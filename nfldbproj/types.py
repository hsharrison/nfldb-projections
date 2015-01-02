from __future__ import absolute_import, division, print_function

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

from nfldb import sql, Enums
from nfldb.types import _Enum, _player_categories


player_pos_by_fantasy_pos = OrderedDict([
    ('QB', {'QB'}),
    ('RB', {'RB'}),
    ('WR', {'WR'}),
    ('TE', {'TE'}),
    ('K', {'K'}),
    ('DST', set()),
    ('DL', {'DL', 'DE', 'DT', 'NT'}),
    ('LB', {'LB', 'MLB', 'ILB', 'OLB'}),
    ('DB', {'DB', 'CB', 'SAF', 'FS', 'SS'}),
])
fantasy_pos_by_player_pos = {player_pos: fantasy_pos
                             for fantasy_pos, player_positions in player_pos_by_fantasy_pos.items()
                             for player_pos in player_positions}


class ProjEnums(Enums):
    fantasy_position = _Enum('fantasy_position',
                             list(player_pos_by_fantasy_pos))
    """
    The set of all possible fantasy positions
    (not including composite, i.e. FLEX, positions).
    """

    proj_scope = _Enum('proj_scope',
                       ['week', 'season', 'rest_of_season'])
    """The three different types of projections: single-week, season-long, and rest of season."""


class SQLProjectionSource(sql.Entity):
    __slots__ = []

    _sql_tables = {
        'primary': ['source_id'],
        'managed': ['projection_source'],
        'tables': [
            ('projection_source', ['source_name', 'source_url']),
        ],
        'derived': [],
    }


class SQLFPSystem(sql.Entity):
    __slots__ = []

    _sql_tables = {
        'primary': ['fpsys_id'],
        'managed': ['fp_system'],
        'tables': [
            ('fp_system', ['fpsys_name', 'fpsys_url']),
        ],
        'derived': [],
    }


class SQLProjectionSet(sql.Entity):
    __slots__ = []

    _sql_tables = {
        'primary': ['source_id', 'fpsys_id', 'set_id'],
        'managed': ['projection_set'],
        'tables': [
            ('projection_set', ['projection_scope', 'season_year', 'week', 'date_accessed']),
        ],
        'derived': [],
    }


class SQLStatProjection(sql.Entity):
    __slots__ = []

    _sql_tables = {
        'primary': ['source_id', 'fpsys_id', 'set_id', 'player_id'],
        'managed': ['stat_projection'],
        'tables': [
            ('stat_projection', ['gsis_id', 'team', 'fantasy_pos'] + list(_player_categories))
        ],
        'derived': [],
    }


class SQLFPProjection(sql.Entity):
    __slots__ = []

    _sql_tables = {
        'primary': ['source_id', 'fpsys_id', 'set_id', 'player_id'],
        'managed': ['fp_projection'],
        'tables': [
            ('fp_projection', ['gsis_id', 'team', 'fantasy_pos', 'fantasy_points']),
        ],
        'derived': [],
    }
