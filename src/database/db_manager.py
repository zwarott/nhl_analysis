from sqlalchemy import create_engine

from config import DATABASE_URL

# Create database connecion
engine = create_engine(DATABASE_URL)
