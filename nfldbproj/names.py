"""Handling for names that can't be matched."""
from __future__ import absolute_import, division, print_function

from nfldb import Tx, player_search
from nfldb.update import log

from nfldbproj.update import lock_tables


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
