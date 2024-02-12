import time

import pandas as pd
from sqlalchemy import select

from src.logging_setup import logger 
from src.session_config import session
from src.data_models.nhl_teams import team_abbreviations
from src.data_models.team import Team


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
        roster = roster[0]

        # Useful column from scraped table
        scraped_cols = ["No.", "Player", "Pos"]
        
        # Filtered DataFrame
        filtered = roster[scraped_cols]

        # Rename scraped columns for db purposes
        new_cols = ["number", "name", "pos"]
        filtered.columns = new_cols
        
        # Create a new column and insert abbr of specific team
        team_id = session.scalars(
            select(Team.tid)
            .where(Team.abbr == abbr)
        ).first() 
       
        # Make sure, that team_id is not None
        if team_id:
            filtered.insert(3, "tid", team_id)
       
        # Append DataFrame into list
        rosters.append(filtered)
        
        # Define sleep time to avoid error requests
        time.sleep(5)

    # Merge DataFrames from roster list into one DataFrame
    merged_df = pd.concat(rosters)

    # Convert "name" column to strings to ensure all values are strings
    merged_df["number"] = merged_df["number"].astype(str)

    # Remove "-NN" pattern from selected rows using regex
    merged_df["number"] = merged_df["number"].str.replace(r"-\d{2}\b", "", regex=True)
    
    # Convert "number" column to integer type
    merged_df["number"] = merged_df["number"].astype(int)
   
    # Remove " (C)" pattern using regex - this matches the pattern " (C)" with optional whitespace characters (\s*) 
    # before and after the parentheses and the "C" character
    merged_df["name"] = merged_df["name"].str.replace(r"\s*\(\s*C\s*\)\s*", "", regex=True)

    return merged_df
