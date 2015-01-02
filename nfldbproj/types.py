from __future__ import absolute_import, division, print_function

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

from nfldb import Enums
from nfldb.types import _Enum


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
