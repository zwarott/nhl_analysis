import time
from typing import Union

import pandas as pd
from sqlalchemy import select, func, desc

from src.logging_setup import logger
from src.session_config import session, Sess

from src.data_models.nhl_teams import team_abbreviations
from src.data_models.game import Game
from src.data_models.team import Team
from src.data_models.player import Player, SkaterStat, SkaterStatAdvanced, GoalieStat

from src.data_preprocessing.game_data import scraping_data, scraping_links

from src.database.decorators import timer


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
        roster = pd.read_html(f"{link}{abbr}/", match="Roster")

        # If there are multiple tables matching the name, select the desired one
        # Assuming the desired table is the first one
        roster = roster[0].iloc[:-1]

        # Useful column from scraped table
        scraped_cols = ["Player", "Pos"]

        # Filtered DataFrame
        filtered = roster[scraped_cols]

        # Rename scraped columns for db purposes
        new_cols = ["name", "pos"]
        filtered.columns = new_cols

        # Create a new column and insert abbr of specific team
        team_id = session.scalars(select(Team.tid).where(Team.abbr == abbr)).first()

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
    merged_df["name"] = merged_df["name"].str.replace(
        r"\s*\(\s*C\s*\)\s*", "", regex=True
    )

    return merged_df


def new_player(tid: int, player_name: str) -> None:
    """Add new player into db table.

    If there is any new player in team, scrape data about him
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

    team_abbr = session.scalars(select(Team.abbr).where(Team.tid == tid)).first()

    # Scrape roster tab by specifying tab nam
    roster = pd.read_html(f"{link}{team_abbr}/", match="Roster")

    # If there are multiple tables matching the name, select the desired one
    # Assuming the desired table is the first one
    roster = roster[0].iloc[:-1]

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
    with Sess.begin() as sess:
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
        skater basic stats scraped. If values are not specified, default
        value is None -> scrape all missing player basic stats.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame representing output dataset with basic
        skater stats. Each row corresponds to skater's basic stats
        for a single game.
    """
    # Subquery to calculate row numbers for Game table
    game_subquery = select(
        Game.gid, func.row_number().over(order_by=Game.gid).label("row_num")
    ).subquery()

    # Last gid number in SkaterStat object
    # Last gid number corresponds with index of next gid value
    skater_stat_gid = session.scalars(
        select(SkaterStat.gid).order_by(SkaterStat.gid.desc()).limit(1)
    ).first()

    # Query to get the row number of the last gid in Game table
    game_gid_last_number = session.scalars(
        select(game_subquery.c.row_num).order_by(desc(game_subquery.c.gid)).limit(1)
    ).first()

    # Query to get the row number of the selected gid (last gid in SkaterStat object) in Game table
    ss_gid_last_number = session.scalars(
        select(game_subquery.c.row_num).where(game_subquery.c.gid == skater_stat_gid)
    ).first()

    # If num_games is specified and SkaterStat object is not empty, modify
    # range of scraped team stats
    if num_games and ss_gid_last_number:
        game_gid_last_number = ss_gid_last_number + num_games

    # If num_games is specified and SkaterStat object is empty, modify range
    # of scraped team stats
    elif num_games and not ss_gid_last_number:
        game_gid_last_number = num_games

    # If SkaterStat object is empty, set up value as 0 (first value from list)
    elif not ss_gid_last_number:
        game_gid_last_number = 0

    # Get basic game info from database
    game_data = scraping_data()[ss_gid_last_number:game_gid_last_number]

    # Scrape game links for further scraping
    game_links = scraping_links()[ss_gid_last_number:game_gid_last_number]

    # Columns of output DataFrame
    new_columns = [
        "pid",
        "tid",
        "gid",
        "g",
        "a",
        "pts",
        "pm",
        "pim",
        "evg",
        "ppg",
        "shg",
        "gwg",
        "esa",
        "ppa",
        "sha",
        "sog",
        "sp",
        "shft",
        "toi",
    ]

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
            f"Scraping {idx}/{len(game_data)} basic skater stats from gid ({gid}) | {atid_abbr} x {htid_abbr}..."
        )

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
                select(Player.pid).where(Player.name == skater)
            ).all()

            # Get a list with all skater's tids
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            skater_tid = session.scalars(
                select(Player.tid).where(Player.name == skater)
            ).all()

            # If skater is not in player table yet, add him into player table
            # and append his pid into list above
            if not skater_pid:
                new_player(atid, skater)
                # Refresh skater_pid after adding the new player
                skater_pid = session.scalars(
                    select(Player.pid).where(Player.name == skater)
                ).all()
                atid_pid.append(skater_pid[-1])

            # If skater changed team and his current tid is not equal to a new
            # team's tid, add him into player table and append his new pid
            # into list above
            elif skater_pid and atid not in skater_tid:
                new_player(atid, skater)
                # Refresh skater_pid after adding the new player
                skater_pid = session.scalars(
                    select(Player.pid).where(Player.name == skater)
                ).all()
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
                select(Player.pid).where(Player.name == skater)
            ).all()

            # Get a list with all skater's tids
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            skater_tid = session.scalars(
                select(Player.tid).where(Player.name == skater)
            ).all()

            # If skater is not in player table yet, add him into player table
            # and append his pid into list above
            if not skater_pid:
                new_player(htid, skater)
                # Refresh skater_pid after adding the new player
                skater_pid = session.scalars(
                    select(Player.pid).where(Player.name == skater)
                ).all()
                htid_pid.append(skater_pid[-1])

            # If skater changed team and his current tid is not equal to a new
            # team's tid, add him into player table and append his new pid
            # into list above
            elif skater_pid and htid not in skater_tid:
                new_player(htid, skater)
                # Refresh skater_pid after adding the new player
                skater_pid = session.scalars(
                    select(Player.pid).where(Player.name == skater)
                ).all()
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
    # Subquery to calculate row numbers for Game table
    game_subquery = select(
        Game.gid, func.row_number().over(order_by=Game.gid).label("row_num")
    ).subquery()

    # Last gid number in SkaterStatAdvanced object
    # Last gid number corresponds with index of next gid value
    skater_stat_advanced_gid = session.scalars(
        select(SkaterStatAdvanced.gid).order_by(SkaterStatAdvanced.gid.desc()).limit(1)
    ).first()

    # Query to get the row number of the last gid in Game table
    game_gid_last_number = session.scalars(
        select(game_subquery.c.row_num).order_by(desc(game_subquery.c.gid)).limit(1)
    ).first()

    # Query to get the row number of the selected gid (last gid in SkaterStatAdvanced object) in Game table
    ssa_gid_last_number = session.scalars(
        select(game_subquery.c.row_num).where(
            game_subquery.c.gid == skater_stat_advanced_gid
        )
    ).first()

    # If num_games is specified and SkaterStatAdvanced object is not empty, modify
    # range of scraped team stats
    if num_games and ssa_gid_last_number:
        game_gid_last_number = ssa_gid_last_number + num_games

    # If num_games is specified and SkaterStatAdvanced object is empty, modify range
    # of scraped team stats
    elif num_games and not ssa_gid_last_number:
        game_gid_last_number = num_games

    # If SkaterStatAdvanced object is empty, set up value as 0 (first value from list)
    elif not ssa_gid_last_number:
        game_gid_last_number = 0

    # Get basic game info from database
    game_data = scraping_data()[ssa_gid_last_number:game_gid_last_number]

    # Scrape game links for further scraping
    game_links = scraping_links()[ssa_gid_last_number:game_gid_last_number]

    # Columns of output DataFrame
    new_columns = [
        "pid",
        "tid",
        "gid",
        "icf",
        "satf",
        "sata",
        "cfp",
        "crel",
        "zso",
        "dzs",
        "ozsp",
        "hit",
        "blk",
    ]

    # Scraped team stats
    scraped_advanced_stats = []

    # Iterate over each game data and web link
    for idx, (data, link) in enumerate(zip(game_data, game_links), start=1):
        gid, atid, htid = data[0], data[2], data[3]
        atid_abbr = session.scalars(select(Team.abbr).where(Team.tid == atid)).first()
        # Get the htid abbr for logging purposes
        htid_abbr = session.scalars(select(Team.abbr).where(Team.tid == htid)).first()
        logger.info(
            f"Scraping {idx}/{len(game_data)} advanced skater stats for gid ({gid}) | {atid_abbr} x {htid_abbr}..."
        )

        # Scrape skater advanced stats for each game using pandas
        # All situations: 6 index -> atid | 13 index -> htid
        # Scrape skater stats without total row (last row)
        atid_skater_stats = pd.read_html(link)[6].iloc[:-1]
        htid_skater_stats = pd.read_html(link)[13].iloc[:-1]

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
                select(Player.pid).where(Player.name == skater)
            ).all()

            # Get a list with all skater's tids
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            skater_tid = session.scalars(
                select(Player.tid).where(Player.name == skater)
            ).all()

            # If skater is not in player table yet, add him into player table
            # and append his pid into list above
            if not skater_pid:
                new_player(atid, skater)
                # Refresh skater_pid after adding the new player
                skater_pid = session.scalars(
                    select(Player.pid).where(Player.name == skater)
                ).all()
                atid_pid.append(skater_pid[-1])

            # If skater changed team and his current tid is not equal to a new
            # team's tid, add him into player table and append his new pid
            # into list above
            elif skater_pid and atid not in skater_tid:
                new_player(atid, skater)
                # Refresh skater_pid after adding the new player
                skater_pid = session.scalars(
                    select(Player.pid).where(Player.name == skater)
                ).all()
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
                select(Player.pid).where(Player.name == skater)
            ).all()

            # Get a list with all skater's tids
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            skater_tid = session.scalars(
                select(Player.tid).where(Player.name == skater)
            ).all()

            # If skater is not in player table yet, add him into player table
            # and append his pid into list above
            if not skater_pid:
                new_player(htid, skater)
                # Refresh skater_pid after adding the new player
                skater_pid = session.scalars(
                    select(Player.pid).where(Player.name == skater)
                ).all()
                htid_pid.append(skater_pid[-1])

            # If skater changed team and his current tid is not equal to a new
            # team's tid, add him into player table and append his new pid
            # into list above
            elif skater_pid and htid not in skater_tid:
                new_player(htid, skater)
                # Refresh skater_pid after adding the new player
                skater_pid = session.scalars(
                    select(Player.pid).where(Player.name == skater)
                ).all()
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


def basic_goalie_stats(num_games: Union[int, None] = None) -> pd.DataFrame:
    """Scrape basic goalie stats for each team within games played.

    Parameters
    ----------
    num_games: Union[int, None] = None
        An integer, that represents number of games for which will be
        goalie stats scraped. If values is not specified, default value
        is None -> scrape all missing goalie stats.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame representing output dataset with basic
        goalie stats. Each row corresponds to goalie's basic stats
        for a single game.
    """
    # Subquery to calculate row numbers for Game table
    game_subquery = select(
        Game.gid, func.row_number().over(order_by=Game.gid).label("row_num")
    ).subquery()

    # Last gid number in GoalieStat object
    # Last gid number corresponds with index of next gid value
    goalie_stat_gid = session.scalars(
        select(GoalieStat.gid).order_by(GoalieStat.gid.desc()).limit(1)
    ).first()

    # Query to get the row number of the last gid in Game table
    game_gid_last_number = session.scalars(
        select(game_subquery.c.row_num).order_by(desc(game_subquery.c.gid)).limit(1)
    ).first()

    # Query to get the row number of the selected gid (last gid in GoalieStat object) in Game table
    gs_gid_last_number = session.scalars(
        select(game_subquery.c.row_num).where(game_subquery.c.gid == goalie_stat_gid)
    ).first()

    # If num_games is specified and GoalieStat object is not empty, modify
    # range of scraped team stats
    if num_games and gs_gid_last_number:
        game_gid_last_number = gs_gid_last_number + num_games

    # If num_games is specified and GoalieStat object is empty, modify range
    # of scraped team stats
    elif num_games and not gs_gid_last_number:
        game_gid_last_number = num_games

    # If GoalieStat object is empty, set up value as 0 (first value from list)
    elif not gs_gid_last_number:
        game_gid_last_number = 0

    # Get basic game info from database
    game_data = scraping_data()[gs_gid_last_number:game_gid_last_number]

    # Scrape game links for further scraping
    game_links = scraping_links()[gs_gid_last_number:game_gid_last_number]

    # Columns of output DataFrame
    new_columns = [
        "pid",
        "tid",
        "gid",
        "dec",
        "ga",
        "sa",
        "sv",
        "svp",
        "so",
        "pim",
        "toi",
        "en",
        "enga",
    ]

    # Scraped goalie stats
    scraped_basic_stats = []

    # Iterate over each game data and web link
    for idx, (data, link) in enumerate(zip(game_data, game_links), start=1):
        gid, atid, htid = data[0], data[2], data[3]
        atid_abbr = session.scalars(select(Team.abbr).where(Team.tid == atid)).first()
        # Get the htid abbr for logging purposes
        htid_abbr = session.scalars(select(Team.abbr).where(Team.tid == htid)).first()
        logger.info(
            f"Scraping {idx}/{len(game_data)} basic goalie stats from gid ({gid}) | {atid_abbr} x {htid_abbr}..."
        )

        # Scrape skater stats for each game using pandas
        # 3 index -> atid | 5 index -> htid
        # Scrape goalie stats without total row (last row) and first 2 rows,
        # that represent headline rows
        # First level columns are removed as well
        atid_goalie_stats = pd.read_html(link)[3].iloc[:, 1:].droplevel(0, axis=1)
        htid_goalie_stats = pd.read_html(link)[5].iloc[:, 1:].droplevel(0, axis=1)

        # Insert EN column and set up default values
        atid_goalie_stats.insert(9, "EN", False)
        htid_goalie_stats.insert(9, "EN", False)

        # Insert ENGA column and set up default values
        atid_goalie_stats.insert(10, "ENGA", 0)
        htid_goalie_stats.insert(10, "ENGA", 0)

        # Remove redundat rows (wrong data on web - e.g. goalie that did not play at the game)
        # If team played with empty net and there is another goalie that played after that, remove him from DataFrame
        if (
            "Empty Net" in atid_goalie_stats["Player"].values
            and atid_goalie_stats["Player"].iloc[-1] != "Empty Net"
        ):
            atid_goalie_stats.drop(atid_goalie_stats.tail(1).index, inplace=True)
        else:
            pass

        # Remove redundat rows (wrong data on web - e.g. goalie that did not play at the game)
        # If team played with empty net and there is another goalie that played after that, remove him from DataFrame
        if (
            "Empty Net" in htid_goalie_stats["Player"].values
            and htid_goalie_stats["Player"].iloc[-1] != "Empty Net"
        ):
            htid_goalie_stats.drop(htid_goalie_stats.tail(1).index, inplace=True)
        else:
            pass

        for i, row in atid_goalie_stats.iterrows():
            if row["Player"] == "Empty Net":
                # Set 'EN' to True in the row before
                atid_goalie_stats.at[i - 1, "EN"] = True

                # Copy 'GA' value to 'ENGA' column in the row before
                atid_goalie_stats.at[i - 1, "ENGA"] = row["GA"]

                # Remove the 'Empty Net' row
                atid_goalie_stats.drop(i, inplace=True)

        for i, row in htid_goalie_stats.iterrows():
            if row["Player"] == "Empty Net":
                # Set 'EN' to True in the row before
                htid_goalie_stats.at[i - 1, "EN"] = True

                # Copy 'GA' value to 'ENGA' column in the row before
                htid_goalie_stats.at[i - 1, "ENGA"] = row["GA"]

                # Remove the 'Empty Net' row
                htid_goalie_stats.drop(i, inplace=True)

        # Reset index after dropping rows
        atid_goalie_stats.reset_index(drop=True, inplace=True)
        htid_goalie_stats.reset_index(drop=True, inplace=True)

        # Replace NaN values by GC (goalie change) and 0 values within DEC and SV% columns
        atid_goalie_stats["DEC"] = atid_goalie_stats["DEC"].where(
            pd.notnull(atid_goalie_stats["DEC"]), "GC"
        )
        atid_goalie_stats["SV%"] = atid_goalie_stats["SV%"].where(
            pd.notnull(atid_goalie_stats["SV%"]), 0
        )
        htid_goalie_stats["DEC"] = htid_goalie_stats["DEC"].where(
            pd.notnull(htid_goalie_stats["DEC"]), "GC"
        )
        htid_goalie_stats["SV%"] = htid_goalie_stats["SV%"].where(
            pd.notnull(htid_goalie_stats["SV%"]), 0
        )

        # Player names in list for replacing them by pid values
        atid_goalies = atid_goalie_stats["Player"].tolist()
        htid_goalies = htid_goalie_stats["Player"].tolist()

        # Prepare pid values for away team
        atid_pid = []
        for goalie in atid_goalies:
            # Get a list with all goalie's pids
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            goalie_pid = session.scalars(
                select(Player.pid).where(Player.name == goalie)
            ).all()

            # Get a list with all goalie's tids
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            goalie_tid = session.scalars(
                select(Player.tid).where(Player.name == goalie)
            ).all()

            # If goalie is not in player table yet, add him into player table
            # and append his pid into list above
            if not goalie_pid:
                new_player(atid, goalie)
                # Refresh skater_pid after adding the new player
                goalie_pid = session.scalars(
                    select(Player.pid).where(Player.name == goalie)
                ).all()
                atid_pid.append(goalie_pid[-1])

            # If goalie changed team and his current tid is not equal to a new
            # team's tid, add him into player table and append his new pid
            # into list above
            elif goalie_pid and atid not in goalie_tid:
                new_player(atid, goalie)
                # Refresh skater_pid after adding the new player
                goalie_pid = session.scalars(
                    select(Player.pid).where(Player.name == goalie)
                ).all()
                atid_pid.append(goalie_pid[-1])

            # Else add appropriate pid into list above
            else:
                atid_pid.append(goalie_pid[-1])

        # Prepare pid values for home team
        htid_pid = []
        for goalie in htid_goalies:
            # Get a list with all goalie's pids
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            goalie_pid = session.scalars(
                select(Player.pid).where(Player.name == goalie)
            ).all()

            # Get a list with all goalie's tids
            # There will be more than 1 if player played for one or more teams and
            # now plays for another one
            goalie_tid = session.scalars(
                select(Player.tid).where(Player.name == goalie)
            ).all()

            # If goalie is not in player table yet, add him into player table
            # and append his pid into list above
            if not goalie_pid:
                new_player(htid, goalie)
                # Refresh skater_pid after adding the new player
                goalie_pid = session.scalars(
                    select(Player.pid).where(Player.name == goalie)
                ).all()
                htid_pid.append(goalie_pid[-1])

            # If goalie changed team and his current tid is not equal to a new
            # team's tid, add him into player table and append his new pid
            # into list above
            elif goalie_pid and htid not in goalie_tid:
                new_player(htid, goalie)
                # Refresh skater_pid after adding the new player
                goalie_pid = session.scalars(
                    select(Player.pid).where(Player.name == goalie)
                ).all()
                htid_pid.append(goalie_pid[-1])

            # Else add appropriate pid into list above
            else:
                htid_pid.append(goalie_pid[-1])

        # Relace player names by pid values
        atid_goalie_stats["Player"] = atid_pid
        htid_goalie_stats["Player"] = htid_pid

        # Add tid column with values
        atid_goalie_stats.insert(1, "tid", atid)
        htid_goalie_stats.insert(1, "tid", htid)

        # Add gid column with values
        atid_goalie_stats.insert(2, "gid", gid)
        htid_goalie_stats.insert(2, "gid", gid)

        # Rename columns
        atid_goalie_stats.columns = new_columns
        htid_goalie_stats.columns = new_columns

        # Append team stats into list
        scraped_basic_stats.append(atid_goalie_stats)
        scraped_basic_stats.append(htid_goalie_stats)

        # Set pandas options for printing whole DataFrame if necessary
        pd.set_option("display.max_rows", None)

        # Define sleep time to avoid error requests
        time.sleep(5)

    # Merge DataFrames into one DataFrame
    merged_df = pd.concat(scraped_basic_stats)

    return merged_df
