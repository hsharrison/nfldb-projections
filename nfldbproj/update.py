"""
Functions for updating the nfldbproj tables.
Currently doesn't use any of nfldb's SQL generation code.

"""
from __future__ import absolute_import, division, print_function

import sys
from itertools import chain

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

from toolz import merge

from nfldb import Tx
from nfldb.update import log

from nfldbproj.db import nfldbproj_tables

_DATA_TABLES_BY_UNIQUE_FIELD = {
    'salary': 'dfs_salary',
    'projected_fp': 'fp_projection',
    'actual_fp': 'fp_score',
    'passing_yds': 'stat_projection',
    'rushing_yds': 'stat_projection',
    'receiving_yds': 'stat_projection',
    'kicking_xpa': 'stat_projection',
    'defense_int': 'stat_projection',
}
DATA_TABLES = list(_DATA_TABLES_BY_UNIQUE_FIELD.keys())

# Order reflects order rows must be added.
METADATA_PRIMARY_KEYS = OrderedDict([
    ('fp_system', ['fpsys_name']),
    ('dfs_site', ['fpsys_name', 'dfs_name']),
    ('projection_source', ['source_name']),
    ('projection_set', ['source_name', 'fpsys_name']),
    # Also set_id, which doesn't need to be inserted as it is SERIAL type.
    # Omitting it here means we can't use these keys to test for the existence of a projection set.
])
METADATA_TABLES = list(METADATA_PRIMARY_KEYS.keys())


def warn(*args, **kwargs):
    log('WARNING:', *args, file=sys.stderr, **kwargs)
    

def error(*args, **kwargs):
    log('ERROR:', *args, file=sys.stderr, **kwargs)


def lock_tables(cursor, tables=frozenset(nfldbproj_tables)):
    log('Locking write access to tables {}...'.format(', '.join(tables)), end='')
    cursor.execute(';\n'.join(
        'LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE'.format(table)
        for table in tables
    ))
    log('done.')


def insert_data(db, metadata, data):
    """
    Given a dataset (as an iterable of dictionaries, all with the same keys)
    and its associated metadata (a dictionary), insert it into the database.
    If any of the metadata items don't exist yet, they will be inserted as well.

    """
    with Tx(db) as c:
        lock_tables(c)
        metadata['set_id'] = _insert_metadata(c, metadata)

        data_iterator = iter(data)
        first_row = next(data_iterator)
        headers = list(first_row.keys())
        _check_headers(c, headers)

        for table in _tables_from_headers(headers):
            _insert_data_rows(c, table, metadata, chain([first_row], data_iterator))


def _insert_data_rows(c, table, metadata, data):
    # This is not performant (one execute per row) but unless it's slow I don't plan to address that.
    columns = _columns(c, table)
    for row in _cleaned_rows(c, table, metadata, data):
        _insert_dict(c, table, row, columns)


def _cleaned_rows(c, table, metadata, data):
    """Combines each row with its metadata fields, then removes any fields that don't need to be stored."""
    columns = _columns(c, table)
    for row in data:
        yield _subdict(merge(metadata, row), columns)


def _insert_metadata(c, metadata):
    """
    Insert new rows into the tables `fp_system`, `dfs_site`, and `projection_source`,
    using a dictionary `metadata` with keys of column names from those tables.

    If a fantasy-point system, DFS site, or projection source specified in `metadata` already exists,
    it is ignored, even if the data conflicts with the existing record (in which case it is NOT updated).

    Returns the `set_id` that was inserted, if any.

    """
    for table in METADATA_TABLES:
        # Special handling for projection_set: don't check primary key existence,
        # because of SERIAL type of set_id.
        if table == 'projection_set':
            # Returning is OK here because projection_set should always be the last metadata table inserted into.
            return _extract_and_insert(c, table, metadata, ignore_if_exists=False, returning='set_id')

        else:
            _extract_and_insert(c, table, metadata, ignore_if_exists=True)


def _check_headers(cursor, headers):
    """Raise an exception if any unrecognized headers are present."""
    all_columns = set(chain.from_iterable(_columns(cursor, table) for table in DATA_TABLES))
    for header in headers:
        if header not in all_columns:
            raise ValueError('column {} not recognized'.format(header))


def _extract_and_insert(cursor, table, data, ignore_if_exists=True, **kwargs):
    """
    Insert row into a metadata table `table`
    using only those elements of dictionary `data` that correspond to columns in `table`.
    Keyword arguments (notably `returning`) are passed to `_insert_dict`.

    """
    if ignore_if_exists:
        return _insert_if_new(cursor, table, _subdict(data, _columns(cursor, table)), **kwargs)
    else:
        return _insert_dict(cursor, table, _subdict(data, _columns(cursor, table)), **kwargs)


def _insert_if_new(cursor, table, data, **kwargs):
    """
    Check if row specified in dictionary `data` exists in table `table`,
    and if it doesn't, insert it.
    Keyword arguments (notably `returning`) are passed to `_insert_dict`.

    """
    pk_only_data = _subdict(data, METADATA_PRIMARY_KEYS[table], enforce_key_presence=True)
    if not _exists(cursor, table, pk_only_data):
        log('inserting new {}...'.format(table), end='')
        result = _insert_dict(cursor, table, data, **kwargs)
        log('done.')
        return result


def _insert_dict(cursor, table, data, returning=None):
    """
    Insert row into `table` as specified by dictionary `data`.
    If `returning` is passed, return specified column from the INSERT statement.

    """
    cols, vals = _query_fields(data)
    returning_clause = 'RETURNING {}'.format(returning) if returning else None
    cursor.execute(
        'INSERT INTO {} ({}) VALUES ({}) {}'.format(table, cols, vals, returning_clause),
        data
    )
    if returning:
        return cursor.fetchone()[returning]


def _exists(cursor, table, data):
    """Check if the row specified by dictionary `data` exists in table `table`."""
    cursor.execute('SELECT 1 FROM {} WHERE ({}) = ({})'.format(table, *_query_fields(data)), data)
    return bool(cursor.fetchone())


def _columns(cursor, table):
    """Return the columns of a table as a list."""
    cursor.execute('''
        SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s
    ''', (table, ))
    return [column['column_name'] for column in cursor.fetchall()]


def _tables_from_headers(headers):
    """
    Given an iterable of column names, return the set of table names that it refers to.
    Works with "data" tables, not "metadata" tables.

    """
    return {table for field, table in _DATA_TABLES_BY_UNIQUE_FIELD.items() if field in headers}


def _query_fields(data):
    """
    Generate the strings needed in a `cursor.execute` call to insert a dictionary.

    Example:
        >>> cols, values = _query_fields({'a': 1, 'b': 2})
        >>> cols
        'a, b'
        >>> values
        '%(a)s, %(b)s'

    """
    keys = data.keys()  # Do this once to avoid any issue with dictionary order.
    column_fields = ', '.join(keys)
    value_fields = ', '.join('%({})s'.format(field) for field in keys)
    return column_fields, value_fields


def _subdict(data, keys, enforce_key_presence=False):
    return {k: data[k] for k in keys if enforce_key_presence or k in data}
