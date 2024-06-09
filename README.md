# NHL Analysis

This project provides tools and scripts for scraping, processing, and analyzing NHL data from [**Hockey Reference**](https://www.hockey-reference.com) website. 
The primary goal is to collect and analyze various NHL data to gain insights into game, team and player performances.

## Table of Contents

- [Database Setup](#database_setup)
- [Data Models](#data_models)
- [Data Preprocessing and Import](#data_preprocessing_and_import)
- [Data Analysis](#data_analysis)
- [Machine Learning](#machine_learning)

## Database Setup
This project uses a [**PostgreSQL**](https://www.postgresql.org) database.   


### Database Connection
Database is specified in hidden `.env` file within `DEVELOPLMENT_DATABASE_URL` variable in format `DEVELOPMENT_DATABASE_URL="postgresql+psycopg2://username:password@host:port/database"`.    

`.env` file requires installation of [**python-dotenv**](https://pypi.org/project/python-dotenv/) inside your virtual environment.    

This environment variable is accessed via `DATABASE_URL` varible within [**config.py**](https://github.com/zwarott/nhl_analysis/blob/main/config.py) using `load_dotenv`.

## Data Models
PostgreSQL database structure (schemas, tables, constraints etc.) is defined through [**SQLAlchemy Mapped Classes**](https://docs.sqlalchemy.org/en/20/orm/mapping_styles.html). Project specifies default classes such as `Game`, `Team`, `Player`. Next, there are classes for storing basic stats and advanced stats.

When data models are defined, changes within database is managed by [**alembic migrations**](https://alembic.sqlalchemy.org/en/latest/).


## Data Preprocessing 
Data preprocessing scripts scrape NHL data from the website above using [**Pandas library**](https://pandas.pydata.org/docs/). After that, data structures are modified onto data models architecture. Finally, prepared data are imported into PostgreSQL database at once. Processes are monitored with logs.


## Data Analysis


## Machine Learning
