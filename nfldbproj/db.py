from __future__ import absolute_import, division, print_function

import psycopg2
from nfldb import connect as nfldb_connect, api_version
from nfldb import Tx, set_timezone
from nfldb.db import _db_name

__pdoc__ = {}

nfldbproj_api_version = 1
__pdoc__['nfldbproj_api_version'] = \
    """
    The nfldbproj schema version that this library corresponds to. When the schema
    version of the database is less than this value, `nfldbproj.connect` will
    automatically update the schema to the latest version before doing
    anything else.
    """

nfldb_api_version = 7
__pdoc__['nfldb_api_version'] = \
    """
    The nfldb schema version that this library corresponds to.
    The database will only be updated from this version.
    """

nfldbproj_tables = {
    'nfldbproj_meta',
    'projection_source',
    'fp_system',
    'projection_set',
    'stat_projection',
    'fp_projection',
}


def connect(**kwargs):
    """
    A wrapper around nfldb.connect.

    Returns a `psycopg2._psycopg.connection` object from the
    `psycopg2.connect` function. If database is `None`, then `connect`
    will look for a configuration file using `nfldb.config` with
    `config_path`. Otherwise, the connection will use the parameters
    given.

    If `database` is `None` and no config file can be found, then an
    `IOError` exception is raised.

    This function will also compare the current schema version of the
    database against the API version `nfldb.api_version` and assert
    that they are equivalent. If the schema library version is less
    than the the API version, then the schema will be automatically
    upgraded. If the schema version is newer than the library version,
    then this function will raise an assertion error. An assertion
    error will also be raised if the schema version is 0 and the
    database is not empty.

    In addition, a similar updating will be performed for nfldbproj.

    N.B. The `timezone` parameter should be set to a value that
    PostgreSQL will accept. Select from the `pg_timezone_names` view
    to get a list of valid time zones.
    """
    conn = nfldb_connect(**kwargs)

    # Migration.
    nfldbproj_sversion = nfldbproj_schema_version(conn)
    assert nfldbproj_sversion <= nfldbproj_api_version, \
        'nfldbproj library version {} is older than schema with version {}'.format(
            nfldbproj_api_version, nfldbproj_sversion
        )
    assert api_version == nfldb_api_version, \
        'nfldbproj expects nfldb version {}, encountered nfldb version {}'.format(
            nfldb_api_version, api_version
        )
    assert nfldbproj_sversion > 0 or (nfldbproj_sversion == 0 and _nfldbproj_is_empty(conn)), \
        'nfldbproj schema has version 0 but is not empty'

    set_timezone(conn, 'UTC')
    _migrate_nfldbproj(conn, nfldbproj_api_version)

    if kwargs.get('timezone'):
        set_timezone(conn, kwargs['timezone'])

    return conn


def nfldbproj_schema_version(conn):
    """
    Returns the schema version of the given database. If the version
    is not stored in the database, then `0` is returned.
    """
    with Tx(conn) as c:
        try:
            c.execute('SELECT nfldbproj_version FROM nfldbproj_meta LIMIT 1',
                      ['nfldbproj_version'])
        except psycopg2.ProgrammingError:
            return 0
        if c.rowcount == 0:
            return 0
        return c.fetchone()['nfldbproj_version']


def _nfldbproj_is_empty(conn):
    """
    Returns `True` if and only if none of the nfldbproj tables exist in the database.
    """
    with Tx(conn) as c:
        c.execute('''
            SELECT table_name from information_schema.tables
            WHERE table_catalog = %s AND table_schema='public'
        ''', [_db_name(conn)])
        table_names = {result[0] for result in c.fetchall()}

    return bool(nfldbproj_tables - table_names)


# What follows are the migration functions. They follow the naming
# convention "_migrate_nfldbproj_{VERSION}" where VERSION is an integer that
# corresponds to the version that the nfldbproj schema will be after the
# migration function runs. Each migration function is only responsible
# for running the queries required to update schema. It does not
# need to update the schema version.
#
# The migration functions should accept a cursor as a parameter,
# which is created in the _migrate function. In particular,
# each migration function is run in its own transaction. Commits
# and rollbacks are handled automatically.


def _migrate_nfldbproj(conn, to):
    current = nfldbproj_schema_version(conn)
    assert current <= to

    globs = globals()
    for v in range(current+1, to+1):
        fname = '_migrate_nfldbproj_{}'.format(v)
        with Tx(conn) as c:
            assert fname in globs, 'Migration function {} not defined'.format(v)
            globs[fname](c)
            c.execute("UPDATE meta SET nfldbproj_version = %s", (v,))
