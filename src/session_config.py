from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from config import DATABASE_URL


# Check if DATABASE_URL is not None before setting engine
db_url = []
if DATABASE_URL:
    db_url.append(DATABASE_URL)

# Create engine
# Establishing a connection to the database and provide the interface for issuing SQL statements
engine = create_engine(db_url[0])

# This session is bound to the engine and can be used to interact with the database (querying and committing transactions)
session = Session(engine)

# Bind the engine to a session
# Create a configurable session factory (create a new Session isntances when needed)
Sess = sessionmaker(engine)
