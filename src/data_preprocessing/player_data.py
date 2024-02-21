import time
from typing import Union

import pandas as pd
from sqlalchemy import select

from src.logging_setup import logger 
from src.session_config import session, Session

from src.data_models.nhl_teams import team_abbreviations
from src.data_models.game import Game
from src.data_models.team import Team
from src.data_models.player import Player, SkaterStat, SkaterStatAdvanced

from src.data_preprocessing.game_data import scraping_data, scraping_links

from decorators import timer


@timer
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


def new_player(tid: int, player_name: str) -> None:
    """Add new player into db table.

    If there are any new player in team, scrape data about him
    and import these data into player table.

    Parameters
    ----------
    tid: int
        An integer representing unique team identifier.
    player_name: str
        A player, for who will be appended into db table.
    """
    # Default link for scraping team rosters
    link = "https://www.hockey-reference.com/teams/"

    player_name = player_name

    team_abbr = session.scalars(
        select(Team.abbr)
        .where(Team.tid == tid)
    ).first()

    # Scrape roster tab by specifying tab name
    roster = pd.read_html(f"{link}{team_abbr}/", match="Scoring Regular Season")
    
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
    
    # Make sure, that team_id is not None
    if tid:
        filtered.insert(2, "tid", tid)

    # Filter row with new player only
    new_player_stat = filtered.query("name == @player_name")
    
    # Convert DataFrame to list of dictionaries
    data = new_player_stat.to_dict(orient="records")
    with Session.begin() as sess:
        print(f"Importing a new player ({player_name}) into Player object...")
        # row is a dictionary containing key-value pairs where the keys correspond to column names
        for row in data:
            # Unpack dictionary with keys matching the attribute names of a class
            # and create an instance of that class with the corresponding values
            record = Player(**row)
            sess.add(record)
        # Number of imported records
        imported_count = len(data)

        print(f"Imported records: {imported_count}")


@timer
def basic_skater_stats(num_games: Union[int, None] = None) -> pd.DataFrame: 
    """Scrape basic skater stats for each team within games played.
    
    Parameters
    ----------
    num_games: Union[int, None] = None
        An integer, that represents number of games for which will be
        skater basic stats scraped. If values is not specified, default
        value is None -> scrape all missing player basic stats.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame representing output dataset with basic 
        skater stats. Each row corresponds to skater's basic stats 
        for a single game.
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
        atid_skater_stats = pd.read_html(link)[2].iloc[:-1, 1:].droplevel(0, axis=1) 
        htid_skater_stats = pd.read_html(link)[4].iloc[:-1, 1:].droplevel(0, axis=1)
        
        # Replace NaN values by 0
        atid_skater_stats = atid_skater_stats.where(pd.notnull(atid_skater_stats), 0)
        htid_skater_stats = htid_skater_stats.where(pd.notnull(htid_skater_stats), 0)

        # Player names in list for replacing them by pid values
        atid_skaters = atid_skater_stats["Player"].tolist()
        htid_skaters = htid_skater_stats["Player"].tolist()
        
        # Prepare pid values for away team
        atid_pid = []
        for skater in atid_skaters:
            # Get a list with all skater's pids
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            skater_pid = session.scalars(
                select(Player.pid)
                .where(Player.name == skater)
            ).all()

            # Get a list with all skater's tids 
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            skater_tid = session.scalars(
                select(Player.tid)
                .where(Player.name == skater)
            ).all()

            # If skater is not in player table yet, add him into player table 
            # and append his pid into list above
            if not skater_pid:
                new_player(atid, skater)
                atid_pid.append(skater_pid[-1])
            
            # If skater changed team and his current tid is not equal to a new
            # team's tid, add him into player table and append his new pid 
            # into list above
            if atid not in skater_tid:
                new_player(atid, skater)
                atid_pid.append(skater_pid[-1])

            # Else add appropriate pid into list above 
            else:
                atid_pid.append(skater_pid[-1])
        
        # Prepare pid values for home team
        htid_pid = []
        for skater in htid_skaters:
            # Get a list with all skater's pids
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            skater_pid = session.scalars(
                select(Player.pid)
                .where(Player.name == skater)
            ).all()

            # Get a list with all skater's tids 
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            skater_tid = session.scalars(
                select(Player.tid)
                .where(Player.name == skater)
            ).all()

            # If skater is not in player table yet, add him into player table 
            # and append his pid into list above
            if not skater_pid:
                new_player(htid, skater)
                htid_pid.append(skater_pid[-1])
            
            # If skater changed team and his current tid is not equal to a new
            # team's tid, add him into player table and append his new pid 
            # into list above
            if htid not in skater_tid:
                new_player(atid, skater)
                htid_pid.append(skater_pid[-1])

            # Else add appropriate pid into list above 
            else:
                htid_pid.append(skater_pid[-1])

        # Relace player names by pid values
        atid_skater_stats["Player"] = atid_pid        
        htid_skater_stats["Player"] = htid_pid

        # Add tid column with values
        atid_skater_stats.insert(1, "tid", atid) 
        htid_skater_stats.insert(1, "tid", htid) 

        # Add gid column with values
        atid_skater_stats.insert(2, "gid", gid) 
        htid_skater_stats.insert(2, "gid", gid) 

        # Rename columns
        atid_skater_stats.columns = new_columns
        htid_skater_stats.columns = new_columns

        # Append team stats into list
        scraped_basic_stats.append(atid_skater_stats)
        scraped_basic_stats.append(htid_skater_stats)

        # Set pandas options for printing whole DataFrame if necessary
        pd.set_option("display.max_rows", None)
        
        # Define sleep time to avoid error requests
        time.sleep(5)

    # Merge DataFrames into one DataFrame
    merged_df = pd.concat(scraped_basic_stats)

    return merged_df


@timer
def advanced_skater_stats(num_games: Union[int, None] = None) -> pd.DataFrame:
    """Scrape advanced skater stats for each team within games played.
    
    Parameters
    ----------
    num_games: Union[int, None] = None
        An integer, that represents number of games for which will be
        skater advanced stats scraped. If values is not specified, default 
        value is None -> scrape all missing player advanced stats.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame representing output dataset with advanced 
        skater stats. Each row corresponds to skater's advanced stats 
        for a single game.
    """
    # Last gid number in Game object
    game_gid = session.scalars(
        select(Game.gid)
        .order_by(Game.gid.desc())
        .limit(1)
    ).first()

    # Last gid number in SkaterStatAdvanced object
    # Last gid number corresponds with index of next gid value
    skater_stat_gid = session.scalars(
        select(SkaterStatAdvanced.gid)
        .order_by(SkaterStatAdvanced.gid.desc())
        .limit(1)
    ).first() 
   
    # If num_games is specified and SkaterStatAdvanced object is not empty, modify
    # range of scraped skater stats
    if num_games and skater_stat_gid:
        game_gid = skater_stat_gid + num_games

    # If num_games is sepcified and SkaterStatAdvanced object is empty, modify range
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
        'icf', 'satf', 'sata', 
        'cfp', 'crel', 'zso', 
        'dzs', 'ozsp', 'hit', 
        'blk', 
    ]

    # Scraped team stats
    scraped_advanced_stats = [] 

    # Iterate over each game data and web link
    for idx, (data, link) in enumerate(zip(game_data, game_links), start=1):
        logger.info(f"Scraping {idx}/{len(game_data)} advanced player stats...")

        gid, atid, htid = data[0], data[2], data[3]

        # Scrape skater stats for each game using pandas
        # 6 index -> atid | 7 index -> htid
        # Scrape skater stats without total row (last row)
        atid_skater_stats = pd.read_html(link)[6].iloc[:-1] 
        htid_skater_stats = pd.read_html(link)[7].iloc[:-1]
        
        # Player names in list for replacing them by pid values
        atid_skaters = atid_skater_stats["Player"].tolist()
        htid_skaters = htid_skater_stats["Player"].tolist()
        
        # Prepare pid values for away team
        atid_pid = []
        for skater in atid_skaters:
            # Get a list with all skater's pids
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            skater_pid = session.scalars(
                select(Player.pid)
                .where(Player.name == skater)
            ).all()

            # Get a list with all skater's tids 
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            skater_tid = session.scalars(
                select(Player.tid)
                .where(Player.name == skater)
            ).all()

            # If skater is not in player table yet, add him into player table 
            # and append his pid into list above
            if not skater_pid:
                new_player(atid, skater)
                atid_pid.append(skater_pid[-1])
            
            # If skater changed team and his current tid is not equal to a new
            # team's tid, add him into player table and append his new pid 
            # into list above
            if atid not in skater_tid:
                new_player(atid, skater)
                atid_pid.append(skater_pid[-1])

            # Else add appropriate pid into list above 
            else:
                atid_pid.append(skater_pid[-1])
        
        # Prepare pid values for home team
        htid_pid = []
        for skater in htid_skaters:
            # Get a list with all skater's pids
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            skater_pid = session.scalars(
                select(Player.pid)
                .where(Player.name == skater)
            ).all()

            # Get a list with all skater's tids 
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            skater_tid = session.scalars(
                select(Player.tid)
                .where(Player.name == skater)
            ).all()

            # If skater is not in player table yet, add him into player table 
            # and append his pid into list above
            if not skater_pid:
                new_player(htid, skater)
                htid_pid.append(skater_pid[-1])
            
            # If skater changed team and his current tid is not equal to a new
            # team's tid, add him into player table and append his new pid 
            # into list above
            if htid not in skater_tid:
                new_player(atid, skater)
                htid_pid.append(skater_pid[-1])

            # Else add appropriate pid into list above 
            else:
                htid_pid.append(skater_pid[-1])

        # Relace player names by pid values
        atid_skater_stats["Player"] = atid_pid        
        htid_skater_stats["Player"] = htid_pid

        # Add tid column with values
        atid_skater_stats.insert(1, "tid", atid) 
        htid_skater_stats.insert(1, "tid", htid) 

        # Add gid column with values
        atid_skater_stats.insert(2, "gid", gid) 
        htid_skater_stats.insert(2, "gid", gid) 

        # Rename columns
        atid_skater_stats.columns = new_columns
        htid_skater_stats.columns = new_columns

        # Append team stats into list
        scraped_advanced_stats.append(atid_skater_stats)
        scraped_advanced_stats.append(htid_skater_stats)
        
        # Define sleep time to avoid error requests
        time.sleep(5)

    # Merge DataFrames into one DataFrame
    merged_df = pd.concat(scraped_advanced_stats)

    return merged_df
