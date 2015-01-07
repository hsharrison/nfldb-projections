from __future__ import absolute_import, division, print_function

import sys
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

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

# Order reflects order rows must be added.
METADATA_PRIMARY_KEYS = OrderedDict([
    ('fp_system', ['fpsys_name']),
    ('dfs_site', ['fpsys_name', 'dfs_name']),
    ('projection_source', ['source_name']),
])


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


def insert_metadata(db, metadata):
    """
    Insert new rows into the tables `fp_system`, `dfs_site`, and `projection_source`,
    using a dictionary `metadata` with keys of column names from those tables.

    If a fantasy-point system, DFS site, or projection source specified in `metadata` already exists,
    it is ignored, even if the data conflicts with the existing record (in which case it is NOT updated).

    """
    with Tx(db) as c:
        metadata_tables = METADATA_PRIMARY_KEYS.keys()
        lock_tables(c, tables=metadata_tables)
        for table in metadata_tables:
            _extract_and_insert(c, table, metadata)


def _extract_and_insert(cursor, table, data):
    if all(pk in data for pk in METADATA_PRIMARY_KEYS[table]):
        _insert_if_new(cursor, table, _subdict(data, _columns(cursor, table)))


def _insert_if_new(cursor, table, data):
    pk_only_data = _subdict(data, METADATA_PRIMARY_KEYS[table])
    if not _exists(cursor, table, pk_only_data):
        log('inserting new {}...'.format(table), end='')
        _insert_dict(cursor, table, data)
        log('done.')


def _insert_dict(cursor, table, data):
    cursor.execute('INSERT INTO {} ({}) VALUES ({})'.format(table, *_query_fields(data)), data)


def _exists(cursor, table, data):
    cursor.execute('SELECT 1 FROM {} WHERE ({}) = ({})'.format(table, *_query_fields(data)), data)
    return bool(cursor.fetchone())


def _columns(cursor, table):
    cursor.execute('''
        SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s
    ''', (table, ))
    return [column['column_name'] for column in cursor.fetchall()]


def _tables_from_headers(headers):
    return {table for field, table in _DATA_TABLES_BY_UNIQUE_FIELD.items() if field in headers}


def _query_fields(data):
    keys = data.keys()  # Do this once to avoid any issue with dictionary order.
    column_fields = ', '.join(keys)
    value_fields = ', '.join('%({})s'.format(field) for field in data.keys())
    return column_fields, value_fields


def _subdict(data, keys):
    return {k: data[k] for k in keys if k in data}
