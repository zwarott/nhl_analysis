import time
from typing import Union

import pandas as pd
from sqlalchemy import select

from src.logging_setup import logger 
from src.session_config import session 
from src.data_models.nhl_teams import teams_dict
from src.data_models.game import Game
from src.data_models.team import TeamStat, TeamStatAdvanced
from src.data_preparation.game_data import scraping_data, scraping_links 

from decorators import timer


def teams() -> pd.DataFrame:
    """Prepare data of NHL teams.

    Retruns
    -------
    pd.DataFrame
        Pandas DataFrame created from dictionary.
    """
    
    # Create DataFrame from the teams_dict and transpose import
    df_all_teams = pd.DataFrame(list(teams_dict.items()), columns=['name', 'abbr'])

    return df_all_teams


@timer
def basic_stats(num_games: Union[int, None] = None) -> pd.DataFrame:
    """Scrape basic team stats from selected games.

    Parameters
    ----------
    num_games: Union[int, None] = None
        An integer, that represents number of games for which will be
        team stats scraped. If values is not specified, default value
        is None -> scrape all missing team stats.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame representing output dataset with basic 
        team stats. Each row corresponds to team's basic stats 
        for a single game.

    """
    
    # Last gid number in Game object
    game_gid = session.scalars(
        select(Game.gid)
        .order_by(Game.gid.desc())
        .limit(1)
    ).first()

    # Last gid number in TeamStat object
    # Last gid number corresponds with index of next gid value
    team_stat_gid = session.scalars(
        select(TeamStat.gid)
        .order_by(TeamStat.gid.desc())
        .limit(1)
    ).first() 
   
    # If num_games is specified and TeamStat object is not empty, modify
    # range of scraped team  stats
    if num_games and team_stat_gid:
        game_gid = team_stat_gid + num_games

    # If num_games is sepcified and TeamStat object is empty, modify range
    # of scraped team stats
    elif num_games and not team_stat_gid:
        game_gid = 0 + num_games
    
    # If TeamStat object is empty, set up value as O (first value from list)
    elif not team_stat_gid:
        team_stat_gid = 0

    # Get basic game info from database
    game_data = scraping_data()[team_stat_gid:game_gid]

    # Scrape game links for further scraping
    game_links = scraping_links()[team_stat_gid:game_gid]

    # Columns of output DataFrame
    columns = ['tid', 'gid', 'g', 'a', 'pts', 'pim', 'evg', 'ppg', 'shg', 'sog', 'sp']

    # Scraped team stats
    scraped_basic_stats = [] 

    # Iterate over each game data and web link
    for idx, (data, link) in enumerate(zip(game_data, game_links), start=1):
        logger.info(f"Scraping {idx}/{len(game_data)} basic team stats...")

        gid, atid, htid = data[0], data[2], data[3]

        # Scrape team stats for each game using pandas + drop nan values
        # 2 index -> atid | 4 index -> htid
        # Team stats are stored in last table row
        atid_team_stats = pd.read_html(link)[2].iloc[-1].dropna() 
        htid_team_stats = pd.read_html(link)[4].iloc[-1].dropna()

        # Create lists 
        # Remove 'TOTAL' string using list slicing
        # g | a | pts | pim | evg | ppg | shg | sog | sp  
        atid_stats = atid_team_stats.tolist()[1:]
        htid_stats = htid_team_stats.tolist()[1:]

        # Insert tid at the first position
        atid_stats.insert(0, atid)
        htid_stats.insert(0, htid)

        # Insert gid at the second position
        atid_stats.insert(1, gid)
        htid_stats.insert(1, gid)
        
        # Append team stats into list
        scraped_basic_stats.append(atid_stats)
        scraped_basic_stats.append(htid_stats)
        
        # Define sleep time to avoid error requests
        time.sleep(5)

    # Output DataFrame
    output_df = pd.DataFrame(scraped_basic_stats, columns=columns)
        
    return output_df 


@timer
def advanced_stats(num_games: Union[int, None] = None) -> pd.DataFrame:
    """Scrape advanced team stats from selected games.

    Parameters
    ----------
    num_games: Union[int, None] = None
        An integer, that represents number of games for which will be
        advanced team stats scraped. If values is not specified, default
        value is None -> scrape all missing advanced team stats.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame representing output dataset with advanced 
        team stats. Each row corresponds to team's advanced stats
        for a single game.

    """
    # Last gid number in Game object
    game_gid = session.scalars(
        select(Game.gid)
        .order_by(Game.gid.desc())
        .limit(1)
    ).first()

    # Last gid number in TeamStatAdvanceed object
    # Last gid number corresponds with index of next gid value
    team_stat_gid = session.scalars(
        select(TeamStatAdvanced.gid)
        .order_by(TeamStatAdvanced.gid.desc())
        .limit(1)
    ).first() 
   
    # If num_games is specified and TeamStatAdvanced object is not empty, modify
    # range of scraped team advanced stats
    if num_games and team_stat_gid:
        game_gid = team_stat_gid + num_games

    # If num_games is sepcified and TeamStatAdvanced object is empty, modify range
    # of scraped team advanced stats
    elif num_games and not team_stat_gid:
        game_gid = 0 + num_games

    # If TeamStatAdvanced object is empty, set up value as O (first value from list)
    elif not team_stat_gid:
        team_stat_gid = 0

    # Get basic game info from database
    game_data = scraping_data()[team_stat_gid:game_gid]

    # Scrape game links for further scraping
    game_links = scraping_links()[team_stat_gid:game_gid]

    # Columns of output DataFrame
    columns = ['tid', 'gid', 'satf', 'sata', 'cfp', 'ozsp', 'hit', 'blk']

    # Scraped team advanced stats
    scraped_advanced_stats = [] 

    # Iterate over each game data and web link
    for idx, (data, link) in enumerate(zip(game_data, game_links), start=1):
        logger.info(f"Scraping {idx}/{len(game_data)} advanced team stats...")

        gid, atid, htid = data[0], data[2], data[3]

        # Scrape team advanced stats for each game using pandas + drop nan values
        # 6 index -> atid | 7 index -> htid
        # Team stats are stored in last table row
        atid_team_stats = pd.read_html(link)[6].iloc[-1].dropna() 
        htid_team_stats = pd.read_html(link)[7].iloc[-1].dropna()

        # Create lists 
        # Remove 'TOTAL' string using list slicing
        # saft | sata | cfp | ozsp | hit | blk 
        atid_stats = atid_team_stats.tolist()[1:]
        htid_stats = htid_team_stats.tolist()[1:]

        # Insert tid at the first position
        atid_stats.insert(0, atid)
        htid_stats.insert(0, htid)

        # Insert gid at the second position
        atid_stats.insert(1, gid)
        htid_stats.insert(1, gid)
        
        # Append team stats into list
        scraped_advanced_stats.append(atid_stats)
        scraped_advanced_stats.append(htid_stats)
        
        # Define sleep time to avoid error requests
        time.sleep(5)

    # Output DataFrame
    output_df = pd.DataFrame(scraped_advanced_stats, columns=columns)
        
    return output_df 
