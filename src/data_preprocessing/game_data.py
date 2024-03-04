import pandas as pd
import numpy as np

from sqlalchemy import select, desc

from src.session_config import Session

from src.data_models.nhl_teams import teams_dict
from src.data_models.game import Game
from src.data_models.team import Team


def games_played() -> pd.DataFrame:
    """Scrape all games played.

    Scrape all NHL games played in order to populate empty database table.
    After that filter games played only, fill missing data and rename order
    drop selected columns.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame that represents all played NHL games so far.
    """

    # Link from scraping game data
    link = "https://www.hockey-reference.com/leagues/NHL_2024_games.html"
    
    # Create DataFrame with games results (first table -> 0)
    df_all_games = pd.read_html(link)[0]

    # Rename column names
    old_cols = ["Date", "Visitor", "G", "Home", "G.1", "Unnamed: 5"]
    new_cols = ["date", "visitor", "atg", "home", "htg", "end"]
    df_all_games.rename(columns={old_col: new_col for old_col, new_col in zip(old_cols, new_cols)}, inplace=True)

    # Insert empty columns - atid, htid
    df_all_games.insert(1, "atid", np.nan)
    df_all_games.insert(4, "htid", np.nan)

    # Filter played games only with selected columns
    df_filtered = df_all_games[df_all_games["htg"].notna()].iloc[:, :8]

    # Replace team names by team abbreviations
    # Dictionary key is old value and dictinary values is a new value
    df_filtered.loc[:, "home"] = df_filtered.loc[:, "home"].replace(teams_dict)
    df_filtered.loc[:, "visitor"] = df_filtered.loc[:, "visitor"].replace(teams_dict)
    
    # Replace null values in end column with FT (fulltime) 
    df_filtered["end"] = df_filtered["end"].replace(np.nan, "FT")
    
    # Construct Session with begin() method for handling each transaction
    # The transaction is automatically committed or rolled back when exiting the 'with' block
    with Session.begin() as session:
        # Query the database for team names and corresponding primary keys
        stmt = select(Team.abbr, Team.tid) 
        team_data = session.execute(stmt)

        # Create a dictionary mapping team names to primary keys
        team_mapping = {abbr: tid for abbr, tid in team_data}
       
        # Update the DataFrame's foreing key columns based on the team names
        df_filtered["atid"] = df_filtered["visitor"].map(team_mapping)
        df_filtered["htid"] = df_filtered["home"].map(team_mapping)
    
    # Drop team name columns when foreign keys column is populated
    df_filtered.drop(columns=["visitor", "home"], inplace=True)

    return df_filtered 


def games_last(): 
    """Scrape lastest game data that are not within database.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame that represents lastest game data.
    """
    
    # All games played
    all_games_played = games_played()

    # Select last game date from Game object
    with Session.begin() as session:
        stmt = (
            select(
                Game.date
            )
            .order_by(desc("date"))
            .limit(1)
        )
    
        # Prepare above_date variable for query purposes within
        # pandas DataFrame
        above_date = session.execute(stmt).scalars().first()
        above_date = str(above_date)

    # Filter last games played only
    df_last_games = all_games_played.query("date > @above_date")

    return df_last_games 


def scraping_data() -> list:
    """Prepare games data for further scraping.

    Prepare NHL games played data for further scraping such as
    team stats, player stats etc. Game id (gid), game date (date)
    and ids (atid, htid) of both team are required. Need to join
    abbreviations of home teams (abbr) from Team table as well.

    Returns
    -------
    list
        List representing simplified data for further scraping.
    """
    with Session.begin() as session:
        stmt = (
            select(Game.gid, Game.date, Game.atid, Game.htid, Team.abbr)
            .join(Team, Team.tid == Game.htid)
        )    
        
        # Select all games played with joined abbreviations of home teams
        games_played = session.execute(stmt)
        
        # Remove dashes from game dates and each row converts to list and
        # append it to empty list
        modified_data = []
        for row in games_played:
            modified_row = list(row) 
            # Index number 1 is position of date column value
            modified_row[1] = modified_row[1].strftime("%Y%m%d")
            modified_data.append(modified_row)
    
        return modified_data


def scraping_links() -> list:
    """Links of each game played.

    Links of each game played for scraping team and player stats.

    Returns
    -------
    List
        A list representing links of each game played for further
        data scraping such as team and player stats.
    """
    # Indexes: 0-gid, 1-date, 2-atid, 3-htid, 4-htid abbr
    df_all_games = scraping_data()
    
    # List for each game link 
    links = []
    for i in range(len(df_all_games)):
        for df_all_games[i] in df_all_games:
            link = "https://www.hockey-reference.com/boxscores/"
            links.append(f"{link}{df_all_games[i][1]}0{df_all_games[i][4]}.html")

    return links
