from typing import Type

import pandas as pd
from sqlalchemy import select

from src.session_config import Sess
from src.database.decorators import timer

from src.data_models.base import Base
from src.data_models.game import Game
from src.data_models.team import TeamStat, TeamStatAdvanced
from src.data_models.player import SkaterStat, SkaterStatAdvanced, GoalieStat

from src.data_preprocessing.game_data import games_last
from src.data_preprocessing.team_data import basic_team_stats, advanced_team_stats
from src.data_preprocessing.player_data import (
    basic_skater_stats,
    advanced_skater_stats,
    basic_goalie_stats,
)


@timer
def populate_db_table(class_obj: Type[Base], df: pd.DataFrame) -> None:
    """Populate db table.

    Insert/append pandas DataFrame into PostgreSQL database table.

    Parameters
    ----------
    class_obj: Type[Base]
        Class object of SQLAlchemy ORM models derived from the
        Base class.
    df: pandas.DataFrame
        Pandas DataFrame as input data to be imported.

    Returns
    -------
    None

    """
    # Convert DataFrame to list of dictionaries
    data = df.to_dict(orient="records")

    # Construct Session with begin() method for handling each transaction
    # The transaction is automatically committed or rolled back when exiting the 'with' block
    with Sess.begin() as session:
        print(f"Importing data into {class_obj.__name__} object.")
        # row is a dictionary containing key-value pairs where the keys correspond to column names
        for row in data:
            # Unpack dictionary with keys matching the attribute names of a class
            # and create an instance of that class with the corresponding values
            record = class_obj(**row)
            session.add(record)
        # Number of imported records
        imported_count = len(data)
        # Total number of records in db table
        total_count = len(session.scalars(select(class_obj)).all())

        print(
            f"Imported records: {imported_count}",
            f"Total records in db table: {total_count}",
            sep="\n",
        )


@timer
def update_all_tables() -> None:
    """Update all database tables.

    Scrape all missing data and insert them into game,
    team and player tables (skater, goalie).
    """
    # Append last games stats
    populate_db_table(Game, games_last())

    # Append last basic team stats
    populate_db_table(TeamStat, basic_team_stats())

    # Append last advanced team stats
    populate_db_table(TeamStatAdvanced, advanced_team_stats())

    # Append last basic skater stats
    populate_db_table(SkaterStat, basic_skater_stats())

    # Append last advanced skater stats
    populate_db_table(SkaterStatAdvanced, advanced_skater_stats())

    # Append last basic goalie stats
    populate_db_table(GoalieStat, basic_goalie_stats())


if __name__ == "__main__":
    update_all_tables()
