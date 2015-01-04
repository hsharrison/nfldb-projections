from __future__ import absolute_import, division, print_function

import sys

import psycopg2
from nfldb import connect as nfldb_connect, api_version
from nfldb import Tx, set_timezone
from nfldb.db import _db_name, _mogrify, _bind_type
from nfldb.types import _player_categories, _Enum

from nfldbproj.types import ProjEnums

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
    'dfs_site',
    'dfs_salary',
    'name_disambiguation',
    'fp_score',
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

    # Bind SQL -> Python casting functions for additional types.
    _bind_type(conn, 'fantasy_position', _Enum._pg_cast(ProjEnums.fantasy_position))
    _bind_type(conn, 'proj_scope', _Enum._pg_cast(ProjEnums.proj_scope))

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
        table_names = {result['table_name'] for result in c.fetchall()}

    return bool(nfldbproj_tables - table_names)


def _category_sql_field(self):
    """
    Get a modified SQL definition of a statistical category column.
    Unlike in nfldb's tables, we allow NULL statistics,
    in order to differentiate between no projection and a projection of zero.
    """
    return '{} {} NULL'.format(self.category_id, 'real' if self.is_real else 'smallint')

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
            c.execute("UPDATE nfldbproj_meta SET nfldbproj_version = %s", (v,))


def _create_enum(c, enum):
    c.execute('''
        CREATE TYPE {} AS ENUM {}
    '''.format(enum.__name__, _mogrify(c, enum)))


def _migrate_nfldbproj_1(c):
    print('Adding nfldb-projections tables to the database...', file=sys.stderr)

    _create_enum(c, ProjEnums.fantasy_position)
    _create_enum(c, ProjEnums.proj_scope)

    c.execute('''
        CREATE DOMAIN uinteger AS integer
            CHECK (VALUE >= 0)
    ''')

    c.execute('''
        CREATE TABLE nfldbproj_meta (
            nfldbproj_version smallint
        )
    ''')
    c.execute('''
        INSERT INTO nfldbproj_meta (nfldbproj_version) VALUES (0)
    ''')

    c.execute('''
        CREATE TABLE projection_source (
            source_id usmallint NOT NULL,
            source_name character varying (100) NOT NULL,
            source_url character varying (255) NULL,
            source_notes text NULL,
            PRIMARY KEY (source_id)
        )
    ''')

    c.execute('''
        CREATE TABLE fp_system (
            fpsys_id usmallint NOT NULL,
            fpsys_name character varying (100) NOT NULL,
            fpsys_url character varying (255) NULL,
            PRIMARY KEY (fpsys_id)
        )
    ''')
    # Handle stat projections by allowing them to reference a fantasy-point system "None" with id 0.
    c.execute('''
        INSERT INTO fp_system (fpsys_id, fpsys_name) VALUES (0, 'None')
    ''')

    c.execute('''
        CREATE TABLE dfs_site (
            fpsys_id usmallint NOT NULL CHECK (fpsys_id != 0),
            dfs_id usmallint NOT NULL,
            dfs_name character varying (100) NOT NULL,
            dfs_url character varying (255) NOT NULL,
            PRIMARY KEY (fpsys_id, dfs_id),
            FOREIGN KEY (fpsys_id)
                REFERENCES fp_system (fpsys_id)
                ON DELETE RESTRICT
        )
    ''')

    c.execute('''
        CREATE TABLE dfs_salary (
            fpsys_id usmallint NOT NULL,
            dfs_id usmallint NOT NULL,
            player_id character varying (10) NOT NULL,
            season_year usmallint NOT NULL,
            season_type season_phase NOT NULL,
            week usmallint NOT NULL,
            salary uinteger NOT NULL,
            PRIMARY KEY (fpsys_id, dfs_id, player_id, season_year, season_type, week),
            FOREIGN KEY (fpsys_id, dfs_id)
                REFERENCES dfs_site (fpsys_id, dfs_id)
                ON DELETE CASCADE,
            FOREIGN KEY (player_id)
                REFERENCES player (player_id)
                ON DELETE RESTRICT
        )
    ''')

    c.execute('''
        CREATE TABLE projection_set (
            source_id usmallint NOT NULL,
            fpsys_id usmallint NOT NULL,
            set_id usmallint NOT NULL,
            projection_scope proj_scope NOT NULL,
            season_year usmallint NOT NULL,
            season_type season_phase NOT NULL,
            week usmallint NULL,
            date_accessed utctime NOT NULL,
            known_incomplete bool NOT NULL,
            PRIMARY KEY (source_id, fpsys_id, set_id),
            FOREIGN KEY (source_id)
                REFERENCES projection_source (source_id)
                ON DELETE CASCADE,
            FOREIGN KEY (fpsys_id)
                REFERENCES fp_system (fpsys_id)
                ON DELETE CASCADE
        )
    ''')

    c.execute('''
        CREATE INDEX projection_set_in_year_phase_week ON projection_set
            (season_year DESC, season_type DESC, week DESC)
    ''')

    c.execute('''
        CREATE TABLE stat_projection (
            source_id usmallint NOT NULL,
            fpsys_id usmallint NOT NULL CHECK (fpsys_id = 0),
            set_id usmallint NOT NULL,
            player_id character varying (10) NOT NULL,
            gsis_id gameid NULL,
            team character varying (3) NOT NULL,
            fantasy_pos fantasy_position NOT NULL,
            {},
            PRIMARY KEY (source_id, fpsys_id, set_id, player_id),
            FOREIGN KEY (source_id)
                REFERENCES projection_source (source_id)
                ON DELETE CASCADE,
            FOREIGN KEY (source_id, fpsys_id, set_id)
                REFERENCES projection_set (source_id, fpsys_id, set_id)
                ON DELETE CASCADE,
            FOREIGN KEY (fpsys_id)
                REFERENCES fp_system (fpsys_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE,
            FOREIGN KEY (player_id)
                REFERENCES player (player_id)
                ON DELETE RESTRICT,
            FOREIGN KEY (gsis_id)
                REFERENCES game (gsis_id)
                ON DELETE RESTRICT,
            FOREIGN KEY (team)
                REFERENCES team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    '''.format(
        ', '.join(_category_sql_field(cat) for cat in _player_categories.values())
    ))

    c.execute('''
        CREATE TABLE fp_projection (
            source_id usmallint NOT NULL,
            fpsys_id usmallint NOT NULL CHECK (fpsys_id != 0),
            set_id usmallint NOT NULL,
            player_id character varying (10) NOT NULL,
            gsis_id gameid NULL,
            team character varying (3) NOT NULL,
            fantasy_pos fantasy_position NOT NULL,
            projected_fp real NOT NULL,
            fp_variance real NULL CHECK (fp_variance >= 0),
            PRIMARY KEY (source_id, fpsys_id, set_id, player_id),
            FOREIGN KEY (source_id)
                REFERENCES projection_source (source_id)
                ON DELETE CASCADE,
            FOREIGN KEY (source_id, fpsys_id, set_id)
                REFERENCES projection_set (source_id, fpsys_id, set_id)
                ON DELETE CASCADE,
            FOREIGN KEY (fpsys_id)
                REFERENCES fp_system (fpsys_id)
                ON DELETE CASCADE,
            FOREIGN KEY (player_id)
                REFERENCES player (player_id)
                ON DELETE RESTRICT,
            FOREIGN KEY (gsis_id)
                REFERENCES game (gsis_id)
                ON DELETE RESTRICT,
            FOREIGN KEY (team)
                REFERENCES team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    ''')

    c.execute('''
        CREATE TABLE fp_score (
            fpsys_id usmallint NOT NULL CHECK (fpsys_id != 0),
            gsis_id gameid NOT NULL,
            player_id character varying (10) NOT NULL,
            team character varying (3) NOT NULL,
            fantasy_pos fantasy_position NOT NULL,
            actual_fp real NOT NULL,
            PRIMARY KEY (fpsys_id, gsis_id, player_id),
            FOREIGN KEY (fpsys_id)
                REFERENCES fp_system (fpsys_id)
                ON DELETE CASCADE,
            FOREIGN KEY (gsis_id)
                REFERENCES game (gsis_id)
                ON DELETE RESTRICT,
            FOREIGN KEY (player_id)
                REFERENCES player (player_id)
                ON DELETE RESTRICT,
            FOREIGN KEY (team)
                REFERENCES team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    ''')

    c.execute('''
        CREATE TABLE name_disambiguation (
            name_as_scraped character varying (100) NOT NULL,
            player_id character varying (10) NOT NULL,
            PRIMARY KEY (name_as_scraped),
            FOREIGN KEY (player_id)
                REFERENCES player (player_id)
                ON DELETE CASCADE
        )
    ''')
