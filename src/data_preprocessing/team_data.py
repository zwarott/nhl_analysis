import time
from typing import Union

import pandas as pd
from sqlalchemy import select, func, desc

from src.logging_setup import logger
from src.session_config import session

from src.data_models.nhl_teams import teams_dict
from src.data_models.game import Game
from src.data_models.team import Team, TeamStat, TeamStatAdvanced

from src.data_preprocessing.game_data import scraping_data, scraping_links

from src.database.decorators import timer


def teams() -> pd.DataFrame:
    """Prepare data of NHL teams.

    Retruns
    -------
    pd.DataFrame
        Pandas DataFrame created from dictionary.
    """
    # Create DataFrame from the teams_dict and transpose import
    df_all_teams = pd.DataFrame(list(teams_dict.items()), columns=["name", "abbr"])

    return df_all_teams


@timer
def basic_team_stats(num_games: Union[int, None] = None) -> pd.DataFrame:
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
    # Subquery to calculate row numbers for Game table
    game_subquery = select(
        Game.gid, func.row_number().over(order_by=Game.gid).label("row_num")
    ).subquery()

    # Last gid number in TeamStat object
    # Last gid number corresponds with index of next gid value
    team_stat_gid = session.scalars(
        select(TeamStat.gid).order_by(TeamStat.gid.desc()).limit(1)
    ).first()

    # Query to get the row number of the last gid in Game table
    game_gid_last_number = session.scalars(
        select(game_subquery.c.row_num).order_by(desc(game_subquery.c.gid)).limit(1)
    ).first()

    # Query to get the row number of the selected gid (last gid in TeamStat object) in Game table
    ts_gid_last_number = session.scalars(
        select(game_subquery.c.row_num).where(game_subquery.c.gid == team_stat_gid)
    ).first()

    # If num_games is specified and TeamStat object is not empty, modify
    # range of scraped team stats
    if num_games and ts_gid_last_number:
        game_gid_last_number = ts_gid_last_number + num_games

    # If num_games is specified and TeamStat object is empty, modify range
    # of scraped team stats
    elif num_games and not ts_gid_last_number:
        game_gid_last_number = num_games

    # If TeamStat object is empty, set up value as 0 (first value from list)
    elif not ts_gid_last_number:
        game_gid_last_number = 0

    # Get basic game info from database
    game_data = scraping_data()[ts_gid_last_number:game_gid_last_number]

    # Scrape game links for further scraping
    game_links = scraping_links()[ts_gid_last_number:game_gid_last_number]

    # Columns of output DataFrame
    columns = ["tid", "gid", "g", "a", "pts", "pim", "evg", "ppg", "shg", "sog", "sp"]

    # Scraped team stats
    scraped_basic_stats = []

    # Iterate over each game data and web link
    for idx, (data, link) in enumerate(zip(game_data, game_links), start=1):
        gid, atid, htid = data[0], data[2], data[3]
        # Get the atid abbr for logging purposes
        atid_abbr = session.scalars(select(Team.abbr).where(Team.tid == atid)).first()
        # Get the htid abbr for logging purposes
        htid_abbr = session.scalars(select(Team.abbr).where(Team.tid == htid)).first()
        logger.info(
            f"Scraping {idx}/{len(game_data)} basic team stats from game ({gid}) | {atid_abbr} x {htid_abbr}..."
        )

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
def advanced_team_stats(num_games: Union[int, None] = None) -> pd.DataFrame:
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
    # Subquery to calculate row numbers for Game table
    game_subquery = select(
        Game.gid, func.row_number().over(order_by=Game.gid).label("row_num")
    ).subquery()

    # Last gid number in TeamStatAdvanced object
    # Last gid number corresponds with index of next gid value
    team_stat_advanced_gid = session.scalars(
        select(TeamStatAdvanced.gid).order_by(TeamStatAdvanced.gid.desc()).limit(1)
    ).first()

    # Query to get the row number of the last gid in Game table
    game_gid_last_number = session.scalars(
        select(game_subquery.c.row_num).order_by(desc(game_subquery.c.gid)).limit(1)
    ).first()

    # Query to get the row number of the selected gid (last gid in TeamStatAdvanced object) in Game table
    tsa_gid_last_number = session.scalars(
        select(game_subquery.c.row_num).where(
            game_subquery.c.gid == team_stat_advanced_gid
        )
    ).first()

    # If num_games is specified and TeamStatAdvanced object is not empty, modify
    # range of scraped team stats
    if num_games and tsa_gid_last_number:
        game_gid_last_number = tsa_gid_last_number + num_games

    # If num_games is specified and TeamStatAdvanced object is empty, modify range
    # of scraped team stats
    elif num_games and not tsa_gid_last_number:
        game_gid_last_number = num_games

    # If TeamStatAdvanced object is empty, set up value as 0 (first value from list)
    elif not tsa_gid_last_number:
        game_gid_last_number = 0

    # Get basic game info from database
    game_data = scraping_data()[tsa_gid_last_number:game_gid_last_number]

    # Scrape game links for further scraping
    game_links = scraping_links()[tsa_gid_last_number:game_gid_last_number]

    # Columns of output DataFrame
    columns = ["tid", "gid", "satf", "sata", "cfp", "ozsp", "hit", "blk"]

    # Scraped team advanced stats
    scraped_advanced_stats = []

    # Iterate over each game data and web link
    for idx, (data, link) in enumerate(zip(game_data, game_links), start=1):
        gid, atid, htid = data[0], data[2], data[3]
        # Get the atid abbr for logging purposes
        atid_abbr = session.scalars(select(Team.abbr).where(Team.tid == atid)).first()
        # Get the htid abbr for logging purposes
        htid_abbr = session.scalars(select(Team.abbr).where(Team.tid == htid)).first()
        logger.info(
            f"Scraping {idx}/{len(game_data)} advanced team stats from game ({gid}) | {atid_abbr} x {htid_abbr}..."
        )

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
