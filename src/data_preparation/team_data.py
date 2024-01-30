import pandas as pd

from src.session_config import Session

from src.data_models.nhl_teams import teams_dict

from src.data_preparation.game_data import df_for_scraping


def df_teams() -> pd.DataFrame:
    """Prepare data of NHL teams.

    Retruns
    -------
    pd.DataFrame
        Pandas DataFrame created from dictionary.
    """
    
    # Create DataFrame from the teams_dict and transpose import
    df_all_teams = pd.DataFrame(list(teams_dict.items()), columns=['name', 'abbr'])

    return df_all_teams


def df_team_stats() -> pd.DataFrame:
    """

    """
    team_stats = []
    
    
    df_all_games = df_for_scraping()
    
    
