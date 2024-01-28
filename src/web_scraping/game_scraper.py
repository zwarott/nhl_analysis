import pandas as pd
import numpy as np

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from config import DATABASE_URL

from src.data_models.nhl_teams import teams_dict
from src.data_models.team import Team


# Check if DATABASE_URL is not None before setting engine 
db_url = []
if DATABASE_URL is not None:
    db_url.append(DATABASE_URL)

engine = create_engine(db_url[0])

# Bind the engine to a session
Session = sessionmaker(bind=engine)


def df_games_played() -> pd.DataFrame:
    """Scrape all games played.

    Scrape all NHL games played in order to populate empty database table.

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


def df_games_last(above_date: str) -> pd.DataFrame:
    """Scrape last games played.

    Scraped last NHL games played in order to append latest game data
    into database table.

    Parameters
    ----------
    above_data: str
        String representing date above that data will be scraped.
        Put date in format YYYY-MM-DD -> 2024-01-01. 

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame that represents last games played.
    """
    
    # All games played
    all_games_played = df_games_played()

    # prepare above_date variable for query purposes
    above_date = above_date

    # Filter last games played only
    df_last_games = all_games_played.query("date > @above_date")

    return df_last_games


def df_for_scraping() -> pd.DataFrame:
    """Prepare games data for further scraping.

    Prepare NHL games played data for further scraping such as
    team stats, player stats etc.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame representing simplified data for further
        scraping.
    """
    
    # DataFrame with all games played
    all_games_played = df_games_played()
    
    # Filter date, visotor team and home team only
    selected_cols = ["date", "atid", "htid"]
    all_games_played = all_games_played[selected_cols]

    # Remove dash symbol for further scraping
    all_games_played["date"] = all_games_played["date"].str.replace("-", "")

    return all_games_played
