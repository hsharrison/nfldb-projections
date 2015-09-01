from __future__ import absolute_import, division, print_function

import pandas as pd
import nfldb
from nfldb.update import log
from nfldbproj.names import name_to_id
from nfldbproj import update


def from_dataframe(db, df, metadata, single_week_only=False, season_totals=False,
                   fp_projection=True, stat_projection=True, fp_score=False, dfs_salary=False):
    if 'opp' in df:
        df = drop_byes(df)

    if 'gsis_id' not in df and not season_totals:
        # Not needed for season projections.
        assign_gsis_ids(db, df, metadata)

    fix_dst_names(df)
    if 'fantasy_player_id' not in df:
        assign_player_ids(db, df)

    if fp_projection:
        fp_df = pd.DataFrame(index=df.index)
        for column in ('fantasy_player_id', 'gsis_id', 'team', 'fantasy_pos', 'projected_fp', 'fp_variance', 'week'):
            if column in df:
                fp_df[column] = df[column]

        _from_dataframe_filtered(db, drop_null(fp_df, 'projected_fp'), metadata,
                                 season_totals=season_totals,
                                 single_week_only=single_week_only)

    if stat_projection:
        stat_df = df.copy()
        stat_metadata = metadata.copy()
        stat_metadata['fpsys_name'] = 'None'
        if 'fpsys_url' in stat_metadata:
            del stat_metadata['fpsys_url']
        for column in ('projected_fp', 'fp_variance', 'actual_fp', 'salary'):
            if column in stat_df:
                del stat_df[column]

        _from_dataframe_filtered(db, stat_df, stat_metadata,
                                 season_totals=season_totals,
                                 single_week_only=single_week_only)

    if fp_score:
        results_df = pd.DataFrame(index=df.index)
        for column in ('fantasy_player_id', 'gsis_id', 'team', 'fantasy_pos', 'actual_fp', 'week'):
            if column in df:
                results_df[column] = df[column]

        _from_dataframe_filtered(db, results_df, metadata,
                                 season_totals=season_totals,
                                 single_week_only=single_week_only)

    if dfs_salary:
        salary_df = pd.DataFrame(index=df.index)
        for column in ('fantasy_player_id', 'gsis_id', 'team', 'fantasy_pos', 'salary', 'week'):
            if column in df:
                salary_df[column] = df[column]

        _from_dataframe_filtered(db, drop_null(salary_df, 'salary'), metadata,
                                 season_totals=season_totals,
                                 single_week_only=single_week_only)


def _from_dataframe_filtered(db, df, metadata, season_totals=False, single_week_only=False):
    if season_totals:
        return _from_season_dataframe(db, df, metadata)

    if single_week_only:
        return _from_week_dataframe(db, df, metadata)

    for week, week_df in df.groupby('week'):
        week_metadata = metadata.copy()
        week_metadata['week'] = week
        _from_week_dataframe(db, week_df, week_metadata)


def _from_week_dataframe(db, df, metadata):
    if len(df['week'].unique()) > 1:
        raise ValueError('More than one week in data')
    metadata['week'] = df['week'].iloc[0]
    update.insert_data(db, metadata, _df_to_dicts(df))


def _from_season_dataframe(db, df, metadata):
    pass


def _df_to_dicts(df):
    for _, row in df.iterrows():
        yield dict(row[~row.isnull()])


def drop_byes(df):
    return df.drop(df.index[(df['opp'].isnull()) | (df['opp'] == '-')], axis=0)


def drop_null(df, column):
    return df.drop(df.index[df[column].isnull()], axis=0)


def assign_gsis_ids(db, df, metadata):
    log('finding game ids...', end='')
    for (week, team, home, opp), sub_df in df.groupby(['week', 'team', 'home', 'opp']):
        gsis_id = get_gsis_id(
            db,
            season_year=metadata['season_year'],
            season_type=metadata.get('season_type', 'Regular'),
            week=week,
            home_team=team if home else opp,
        )
        df.loc[(df['week'] == week) & (df['team'] == team), 'gsis_id'] = gsis_id
    log('done')


def get_gsis_id(db, **data):
    q = nfldb.Query(db)
    games = q.game(**data).as_games()
    if not games:
        raise ValueError('Cound not find game matching {}'.format(data))
    if len(games) > 1:
        raise ValueError('Found {} games matching {}'.format(len(games), data))
    return games[0].gsis_id


def assign_player_ids(db, df):
    log('finding player ids...', end='')
    df['fantasy_player_id'] = None
    for name, sub_df in df.groupby('name'):
        df.loc[df['name'] == name, 'fantasy_player_id'] = name_to_id(db, sub_df['name'].iloc[0])
    log('done')


def fix_dst_names(df):
    df.loc[df['fantasy_pos'] == 'DST', 'name'] = df.loc[df['fantasy_pos'] == 'DST', 'team']
