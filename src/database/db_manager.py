from typing import Type

import pandas as pd
from sqlalchemy import select

from src.session_config import Session

from src.data_models.base import Base

from src.database.decorators import timer


@timer
def populate_db_table(class_obj: Type[Base], df: pd.DataFrame) -> None:
    """Populate empty db table.

    Insert pandas DataFrame into emtpy PostgreSQL database table.

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
    with Session.begin() as session:
        print(f"Importing data into {class_obj.__name__} object.")
        # row is a dictionary containing key-value pairs where the keys correspond to column names
        for row in data:
            # Unpack dictionary with keys matching the attribute names of a class
            # and reate an instance of that class with the corresponding values
            record = class_obj(**row)
            session.add(record)
        # Print number of imported records
        imported_count = len(session.scalars(select(class_obj)).all())
        print(f"Imported records: {imported_count}")


# append_data

# delete_data 
