
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from config import DATABASE_URL

# Replace 'your_postgres_url_here' with the actual PostgreSQL database URL
postgres_url = DATABASE_URL 

def check_database_connection():
    try:
        # Create an engine
        engine = create_engine(postgres_url)

        # Try to connect to the database
        with engine.connect():
            print(f"Connection to the database was successful.")

    except SQLAlchemyError as e:
        print(f"Error connecting to the database: {e}")

if __name__ == "__main__":
    check_database_connection()
