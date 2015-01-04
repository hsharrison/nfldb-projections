from __future__ import absolute_import, division, print_function

from nfldb.update import log

from nfldbproj.db import nfldbproj_tables


def warn(*args, **kwargs):
    log('WARNING:', *args, **kwargs)
    

def error(*args, **kwargs):
    log('ERROR:', *args, **kwargs)


def lock_tables(cursor, tables=frozenset(nfldbproj_tables)):
    log('Locking write access to tables {}...'.format(', '.join(tables)), end='')
    cursor.execute(';\n'.join(
        'LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE'.format(table)
        for table in tables
    ))
    log('done.')
