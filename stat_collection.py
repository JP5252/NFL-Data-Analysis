# Dependencies
import pandas as pd
import nfl_data_py as nfl

'''********************************************************************************************
 Author: Justin Pearson

 This file has 4 total functions that implement the fetching of NFL player data for specified 
 years. The data is then converted to exponentially weighted moving averages based on the last
 10 performances. Every function takes a year as input, and can optionally take a data input
 except for get stats, and an offset input which is set to 20 by default. offset determines
 how far back the data should go.
 
 functions in this file:
 - get_stats
 - get_pass_stats
 - get_rush_stats
 - get_rec_stats
 - get_def_stats
 
********************************************************************************************'''

# implements all the stat functions and returns the dataframes
def get_stats(year, offset = 20):
    # import data from nfl_data_py
    data = nfl.import_pbp_data(range(year - offset, year + 1))
    
    pass_df = get_pass_stats(year, data, offset)
    def_df = get_def_stats(year, data, offset)
    
    return pass_df, def_df

# gets the passing stats from a given time frame
def get_pass_stats(year, data = None, offset = 20):
    # Select only the relevant columns
    columns = ['passer_player_name', 'posteam', 'defteam', 'season', 'week', 'home_team', 'away_team', 'play_type', 'air_yards', 
               'yards_after_catch', 'epa', 'complete_pass', 'incomplete_pass', 'interception', 'qb_hit', 'sack', 'pass_touchdown',
               'passing_yards', 'cpoe', 'roof', 'surface']
    
    # if called without data input or incorrect input
    #if data is None or not isinstance(data, pd.DataFrame):
        #data = nfl.import_pbp_data(range(year - offset, year + 1), columns)
    
    # nfl-data-py still loads other columns, so we again need to set our data equal to only the columns we want
    data = data[columns]
    
    # drop all rows that are not a pass
    data = data[data['play_type'] == 'pass']
    
    # drop the play type column
    data = data.drop(columns=['play_type'])

    df = data.groupby(['passer_player_name', 'week', 'season'], as_index=False).agg(
        {'posteam' : 'first',
         'defteam' : 'first',
         'home_team' : 'first',
         'away_team' : 'first',
         'air_yards' : 'sum',
         'yards_after_catch' : 'sum',
         'epa' : 'sum',
         'complete_pass' : 'sum',
         'incomplete_pass' : 'sum',
         'interception' : 'sum',
         'qb_hit' : 'sum',
         'sack' : 'sum',
         'pass_touchdown' : 'sum',
         'passing_yards' : 'sum',
         'cpoe' : 'mean',
         'roof' : 'first',
         'surface' : 'first'
         }
    )

    # Create a new column that is completion percentage
    df['completion_percentage'] = df['complete_pass'] / (df['complete_pass'] + df['incomplete_pass'])
    
    # Create a new column that is the number of pass attempts
    df['pass_attempts'] = df['complete_pass'] + df['incomplete_pass']
    
    # Drop the complete_pass and incomplete_pass columns
    df = df.drop(columns=['complete_pass', 'incomplete_pass'])

    # Create a new column that equals 1 if the passer is the home team and 0 if the passer is the away team
    df['home_flag'] = df['home_team'] == df['posteam']
    
    # Drop the home_team and away_team columns
    df = df.drop(columns=['home_team', 'away_team'])

    # Reorder the columns
    df = df[['passer_player_name', 'posteam', 'defteam', 'season', 'week', 'passing_yards', 'home_flag', 'completion_percentage', 'pass_attempts',
             'air_yards',  'yards_after_catch', 'epa', 'interception', 'qb_hit', 'sack', 'pass_touchdown', 
             'cpoe', 'roof', 'surface']]

    # Calculate the exponentially weighted moving average for each feature
    df['completion_percentage_ewma'] = df.groupby('passer_player_name')['completion_percentage']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['pass_attempts_ewma'] = df.groupby('passer_player_name')['pass_attempts']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['air_yards_ewma'] = df.groupby('passer_player_name')['air_yards']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['yards_after_catch_ewma'] = df.groupby('passer_player_name')['yards_after_catch']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['epa_ewma'] = df.groupby('passer_player_name')['epa']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['interception_ewma'] = df.groupby('passer_player_name')['interception']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['qb_hit_ewma'] = df.groupby('passer_player_name')['qb_hit']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['sack_ewma'] = df.groupby('passer_player_name')['sack']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['pass_touchdown_ewma'] = df.groupby('passer_player_name')['pass_touchdown']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['passing_yards_ewma'] = df.groupby('passer_player_name')['passing_yards']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['cpoe_ewma'] = df.groupby('passer_player_name')['cpoe']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())

    # Drop the non-ewma columns
    df = df.drop(columns=['completion_percentage', 'pass_attempts', 'air_yards', 'yards_after_catch', 'epa', 
                          'interception', 'qb_hit', 'sack', 'pass_touchdown', 'cpoe'])

    return df

# gets the rushing stats from a given time frame
def get_rush_stats(start_year, end_year):
    # Import the NGS data
    df = nfl.import_ngs_data(stat_type='rushing')
    
    # week 0 is full season stats, get rid of everything else
    df = df[df['week'] == 0]
    
    # trim the years we don't want from the dataset
    df = df[df['season'] >= start_year]
    df = df[df['season'] <= end_year]

    df = df.sort_values(by='player_display_name', ascending=True)
    
    return df

# gets the receiving stats from a given year
def get_rec_stats(year):
    # Import the NGS data
    df = nfl.import_ngs_data(stat_type='receiving')
    
    # week 0 is full season stats, get rid of everything else
    df = df[df['week'] == 0]
    
    # trim the years we don't want from the dataset
    df = df[df['season'] >= start_year]
    df = df[df['season'] <= end_year]

    df = df.sort_values(by='player_display_name', ascending=True)
    
    return df

# gets the defence stats from a given time frame
def get_def_stats(year, data = None, offset = 20):
    # Select only the relevant columns
    columns = ['defteam', 'season', 'week', 'home_team', 'away_team', 'play_type', 'air_yards',
               'yards_after_catch', 'epa', 'complete_pass', 'incomplete_pass', 'interception', 'qb_hit', 'sack', 'pass_touchdown',
               'passing_yards', 'cpoe', 'roof', 'surface']

    # if called without data input or incorrect input
    if data is None or not isinstance(data, pd.DataFrame):
        data = nfl.import_pbp_data(range(year - offset, year + 1), columns)
        
    # Drop the play type column
    data = data.drop(columns=['play_type'])

    # Group the data together by passer, week, season and aggregate
    df = data.groupby(['defteam', 'week', 'season'], as_index=False).agg(
        {'home_team': 'first',
         'away_team': 'first',
         'air_yards': 'sum',
         'yards_after_catch': 'sum',
         'epa': 'sum',
         'complete_pass': 'sum',
         'incomplete_pass': 'sum',
         'interception': 'sum',
         'qb_hit': 'sum',
         'sack': 'sum',
         'pass_touchdown': 'sum',
         'passing_yards': 'sum',
         'cpoe': 'mean',
         'roof': 'first',
         'surface': 'first'
         }
    )

    # Create a new column that is completion percentage
    df['completion_percentage'] = df['complete_pass'] / (df['complete_pass'] + df['incomplete_pass'])
    
    # Create a new column that is the number of pass attempts
    df['pass_attempts'] = df['complete_pass'] + df['incomplete_pass']
    
    # Drop the complete_pass and incomplete_pass columns
    df = df.drop(columns=['complete_pass', 'incomplete_pass'])

    # Create a new column that equals 1 if the defense is the home team and 0 if the defense is the away team
    df['home_flag'] = df['home_team'] == df['defteam']
    
    # Drop the home_team and away_team columns
    df = df.drop(columns=['home_team', 'away_team'])

    # Reorder the columns
    df = df[['defteam', 'season', 'week', 'home_flag', 'passing_yards', 'completion_percentage', 'pass_attempts',
             'air_yards',  'yards_after_catch', 'epa', 'interception', 'qb_hit', 'sack', 'pass_touchdown', 
             'cpoe', 'roof', 'surface']]

    # Calculate the exponentially weighted moving average for each feature
    df['completion_percentage_ewma'] = df.groupby('defteam')['completion_percentage']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['pass_attempts_ewma'] = df.groupby('defteam')['pass_attempts']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['air_yards_ewma'] = df.groupby('defteam')['air_yards']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['yards_after_catch_ewma'] = df.groupby('defteam')['yards_after_catch']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['epa_ewma'] = df.groupby('defteam')['epa']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['interception_ewma'] = df.groupby('defteam')['interception']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['qb_hit_ewma'] = df.groupby('defteam')['qb_hit']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['sack_ewma'] = df.groupby('defteam')['sack']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['pass_touchdown_ewma'] = df.groupby('defteam')['pass_touchdown']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['passing_yards_ewma'] = df.groupby('defteam')['passing_yards']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())
    
    df['cpoe_ewma'] = df.groupby('defteam')['cpoe']\
        .transform(lambda x: x.ewm(min_periods=1, span=10).mean())

    # Drop the non-ewma columns
    df = df.drop(columns=['passing_yards','completion_percentage', 'pass_attempts', 'air_yards', 'yards_after_catch', 'epa', 
                          'interception', 'qb_hit', 'sack', 'pass_touchdown', 'cpoe'])

    return df