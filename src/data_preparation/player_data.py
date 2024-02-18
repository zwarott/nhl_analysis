import time
from typing import Union

import pandas as pd
from sqlalchemy import select

from src.logging_setup import logger 
from src.session_config import session

from src.data_models.nhl_teams import team_abbreviations
from src.data_models.game import Game
from src.data_models.team import Team
from src.data_models.player import Player, SkaterStat

from src.data_preparation.game_data import scraping_data, scraping_links


def players():
    """Prepare data of players.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame created from concatened DataFrames.
        Each DataFrame represents data of players from single
        team.
    """
    # List with team abbreviations
    team_abbr = team_abbreviations

    rosters = []
    
    # For each team scrape players data
    for idx, abbr in enumerate(team_abbr, start=1):
        logger.info(f"Scraping {idx}/{len(team_abbr)} roster for {abbr}...")
       
        # Default link for scraping team rosters
        link = "https://www.hockey-reference.com/teams/"

        # Scrape roster tab by specifying tab name
        roster = pd.read_html(f"{link}{abbr}/", match="Scoring Regular Season")
        
        # If there are multiple tables matching the name, select the desired one
        # Assuming the desired table is the first one
        roster = roster[0].iloc[:-1].droplevel(0, axis=1)

        # Useful column from scraped table
        scraped_cols = ["Player", "Pos"]
        
        # Filtered DataFrame
        filtered = roster[scraped_cols]

        # Rename scraped columns for db purposes
        new_cols = ["name", "pos"]
        filtered.columns = new_cols
        
        # Create a new column and insert abbr of specific team
        team_id = session.scalars(
            select(Team.tid)
            .where(Team.abbr == abbr)
        ).first() 
       
        # Make sure, that team_id is not None
        if team_id:
            filtered.insert(2, "tid", team_id)
       
        # Append DataFrame into list
        rosters.append(filtered)
        
        # Define sleep time to avoid error requests
        time.sleep(5)

    # Merge DataFrames from roster list into one DataFrame
    merged_df = pd.concat(rosters)

    # Remove " (C)" pattern using regex - this matches the pattern " (C)" with optional whitespace characters (\s*) 
    # before and after the parentheses and the "C" character
    merged_df["name"] = merged_df["name"].str.replace(r"\s*\(\s*C\s*\)\s*", "", regex=True)

    return merged_df


def basic_skater_stats(num_games: Union[int, None] = None): 
    """

    """
    # Last gid number in Game object
    game_gid = session.scalars(
        select(Game.gid)
        .order_by(Game.gid.desc())
        .limit(1)
    ).first()

    # Last gid number in SkaterStat object
    # Last gid number corresponds with index of next gid value
    skater_stat_gid = session.scalars(
        select(SkaterStat.gid)
        .order_by(SkaterStat.gid.desc())
        .limit(1)
    ).first() 
   
    # If num_games is specified and SkaterStat object is not empty, modify
    # range of scraped skater stats
    if num_games and skater_stat_gid:
        game_gid = skater_stat_gid + num_games

    # If num_games is sepcified and SkaterStat object is empty, modify range
    # of scraped skater stats
    elif num_games and not skater_stat_gid:
        game_gid = num_games
    
    # If TeamStat object is empty, set up value as O (first value from list)
    elif not skater_stat_gid:
        skater_stat_gid = 0

    # Get basic game info from database
    game_data = scraping_data()[skater_stat_gid:game_gid]

    # Scrape game links for further scraping
    game_links = scraping_links()[skater_stat_gid:game_gid]

    # Columns of output DataFrame
    new_columns = [
        'pid', 'tid', 'gid', 
        'g', 'a', 'pts', 'pm', 
        'pim', 'evg', 'ppg', 
        'shg', 'gwg', 'esa', 
        'ppa', 'sha', 'sog', 
        'sp', 'shft', 'toi'
    ]

    # Scraped team stats
    scraped_basic_stats = [] 

    # Iterate over each game data and web link
    for idx, (data, link) in enumerate(zip(game_data, game_links), start=1):
        logger.info(f"Scraping {idx}/{len(game_data)} basic player stats...")

        gid, atid, htid = data[0], data[2], data[3]

        # Scrape skater stats for each game using pandas
        # 2 index -> atid | 4 index -> htid
        # Scrape skater stats without total row (last row) and first 2 rows,
        # that represent headline rows
        # First level columns are removed as well
        atid_player_stats = pd.read_html(link)[2].iloc[:-1, 1:].droplevel(0, axis=1) 
        htid_player_stats = pd.read_html(link)[4].iloc[:-1, 1:].droplevel(0, axis=1)
        
        # Replace NaN values by 0
        atid_player_stats = atid_player_stats.where(pd.notnull(atid_player_stats), 0)
        htid_player_stats = htid_player_stats.where(pd.notnull(htid_player_stats), 0)

        # Player names in list for replacing them by pid values
        atid_players = atid_player_stats["Player"].tolist()
        htid_players = htid_player_stats["Player"].tolist()

        # Prepare pid values for away team
        atid_pid = []
        for player in atid_players:
            player_pid = session.scalars(
                select(Player.pid)
                .where(Player.name == player)
            ).first()
            atid_pid.append(player_pid)
        
        # Prepare pid values for home team
        htid_pid = []
        for player in htid_players:
            player_pid = session.scalars(
                select(Player.pid)
                .where(Player.name == player)
            ).first()
            htid_pid.append(player_pid)
        
        # Relace player names by pid values
        atid_player_stats["Player"] = atid_pid        
        htid_player_stats["Player"] = htid_pid

        # Add tid column with values
        atid_player_stats.insert(1, "tid", atid) 
        htid_player_stats.insert(1, "tid", htid) 

        # Add gid column with values
        atid_player_stats.insert(2, "gid", gid) 
        htid_player_stats.insert(2, "gid", gid) 

        # Rename columns
        atid_player_stats.columns = new_columns
        htid_player_stats.columns = new_columns

        # Append team stats into list
        scraped_basic_stats.append(atid_player_stats)
        scraped_basic_stats.append(htid_player_stats)
        
        # Define sleep time to avoid error requests
        time.sleep(5)

    # Merge DataFrames into one DataFrame
    merged_df = pd.concat(scraped_basic_stats)

    return merged_df
