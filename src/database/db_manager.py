from typing import Type

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import DATABASE_URL

from src.data_models.base import Base
from src.data_models.team import Team
from src.data_models.nhl_teams import teams_dict 

from src.web_scraping.team_scraper import df_teams

from src.database.decorators import timer


# Check if DATABASE_URL is not None before setting engine 
db_url = []
if DATABASE_URL is not None:
    db_url.append(DATABASE_URL)

engine = create_engine(db_url[0])


# Set up Session factory
Session = sessionmaker(engine)

@timer
def insert_df(class_obj: Type[Base], df: pd.DataFrame) -> None:
    # Convert DataFrame to list of dictionaries
    data = df.to_dict(orient="records")
    # Construct Session with begin() method for handling each transaction
    # The transaction is automatically committed or rolled back when exiting the 'with' block
    with Session.begin() as session:
        print(f"Importing data into {class_obj.__name__}.")
        for row in data:
            record = class_obj(**row)
            session.add(record)
            # Commits the transaction, closes the session
        # Print number of imported records
        imported_count = session.query(class_obj).count()  
        print(f"Imported records: {imported_count}")


insert_df(Team, df_teams(teams_dict))

