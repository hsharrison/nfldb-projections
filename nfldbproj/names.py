"""Player name handling."""
from __future__ import absolute_import, division, print_function

from nfldb import Tx, player_search
from nfldb.update import log

from nfldbproj.update import lock_tables, error

DEFAULT_SEARCH_LIMIT = 5


def add_name_disambiguations(db, ids_by_names):
    """
    Inserts rows to `ambiguous_name`.
    The parameter `ids_by_names` should be a dictionary mapping names to ids.

    """
    log('Writing rows to name_disambiguation...')
    with Tx(db) as c:
        lock_tables(c, ['name_disambiguation'])
        c.execute('INSERT INTO name_disambiguation (name_as_scraped, player_id) VALUES '
                  + ', '.join(c.mogrify('(%s, %s)', item) for item in ids_by_names.items()))
    log('done.')


def name_to_id(db, full_name, **kwargs):
    """
    Find an id for `full_name`,
    checking first the `name_disambiguation` table and then the `player` table.
    Optional keyword arguments are passed to `nfldb.player_search`.

    If not found, a similarity table is printed and `KeyError` raised.
    """
    return disambiguate_from_table(db, full_name) or match_or_raise(db, full_name, **kwargs)


def disambiguate_from_table(db, full_name):
    """
    Lookup `full_name` in `name_disambiguation` table, returning `player_id` if found.
    """
    with Tx(db) as c:
        c.execute('SELECT player_id FROM name_disambiguation WHERE name_as_scraped = %s',
                  (full_name,))
        result = c.fetchone()
        if result:
            return result['player_id']


def match_or_raise(db, full_name, **kwargs):
    """
    Lookup `full_name` in `player` table.
    If not found, print similarity table and raise `KeyError`.
    Otherwise, return the `player_id`.
    Optional keyword arguments are passed to `nfldb.player_search`.
    """
    kwargs['limit'] = kwargs.get('limit', DEFAULT_SEARCH_LIMIT)
    matches = player_search(db, full_name, **kwargs)

    best_match, distance = matches[0]
    if not distance:
        return best_match.player_id

    error("""\
Player "{}" not found. Closest matches:
{}
Use nfldbproj.add_name_disambiguations to insert correct player_id into the database.""".format(
        full_name, _similarity_search_table(matches)
        ))
    raise KeyError('{} (see message above traceback)'.format(full_name))


def _similarity_search_table(matches):
    player_header = 'full_name (team, pos)'
    player_strs = [str(player) for player, _ in matches] + [player_header]
    player_width = max(len(s) for s in player_strs)
    return """\
+-{horiz_rule}-+
| similarity | {player_header:{w}} | player_id  |
+-{horiz_rule}-+
{candidate_list}
+-{horiz_rule}-+""".format(
        horiz_rule='-+-'.join((10*'-', player_width*'-', 10*'-')),
        player_header=player_header,
        w=player_width,
        candidate_list='\n'.join(
            '| {1:10} | {0:{w}} | {0.player_id} |'.format(*match, w=player_width)
            for match in matches
        ))
