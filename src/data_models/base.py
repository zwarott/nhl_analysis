import datetime

from typing_extensions import Annotated

from sqlalchemy import MetaData, String, ForeignKey, Table, Column
from sqlalchemy.orm import DeclarativeBase, mapped_column


# Set up default attributes - primary key, foreign key
intpk = Annotated[int, mapped_column(primary_key=True, autoincrement=True)]
intfk_gid = Annotated[int, mapped_column(ForeignKey("game.gid"))] 
intfk_tid = Annotated[int, mapped_column(ForeignKey("team.tid"))] 
intfk_pid = Annotated[int, mapped_column(ForeignKey("player.pid"))] 

# Set up default attributes - strings 
str_2 = Annotated[str, mapped_column(String(2), nullable=False)] 
str_3 = Annotated[str, mapped_column(String(3), nullable=False)] 

# Set up default attributes - date, datetime 
date = Annotated[datetime.date, mapped_column(datetime.date.today().isoformat())] 
timestamp_created = Annotated[
    datetime.datetime, 
    mapped_column(server_default=datetime.datetime.today().isoformat(sep=' ', timespec='seconds'))
]
timestamp_updated = Annotated[
    datetime.datetime, 
    mapped_column(onupdate=datetime.datetime.today().isoformat(sep=' ', timespec='seconds'))
    
]

# Set up constraint naming convention.
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

# Create an instance of MetaData with the custom naming convention.
metadata_obj = MetaData(
    naming_convention=convention,
    # Set up schema name within each class object automatically
    # schema="schema_name"
)


# Define base class allowing to define tables and relationships using class attributes.
class Base(DeclarativeBase):
    # Set up metadata within the declarative base.
    metadata = metadata_obj

# Association tables
# Game-Team Many-to-Many relationship
game_team_join = Table(
    "game_team_join",
    Base.metadata,
    Column("gid", ForeignKey("game.gid"), primary_key=True),
    Column("tid", ForeignKey("team.tid"), primary_key=True)
) 

# Game-Player Many-to-Many relationship
game_player_join = Table(
    "game_player_join",
    Base.metadata,
    Column("gid", ForeignKey("game.gid"), primary_key=True),
    Column("pid", ForeignKey("player.pid"), primary_key=True)
)

