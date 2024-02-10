from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from config import DATABASE_URL


# Check if DATABASE_URL is not None before setting engine 
db_url = []
if DATABASE_URL:
    db_url.append(DATABASE_URL)

# Create engine
engine = create_engine(db_url[0])

session = Session(engine)

# Bind the engine to a session 
Session = sessionmaker(engine)
