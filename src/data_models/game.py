from __future__ import annotations
from typing import List, TYPE_CHECKING

from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, relationship

from src.data_models.base import Base
from src.data_models.base import intpk, intfk_tid, str_2, date  
from src.data_models.base import timestamp_created, timestamp_updated
from src.data_models.base import game_team_join, game_player_join


# Use TYPE_CHECKING constant to prevent the circular imports
if TYPE_CHECKING:
    from src.data_models.team import Team
    from src.data_models.player import Player 


class Game(Base):
    __tablename__ = "game"
    
    # Basic information
    gid: Mapped[intpk]
    date: Mapped[date]
    
    # Away team 
    atid: Mapped[intfk_tid]
    atg: Mapped[int] # Away team goals

    # Home team
    htid: Mapped[intfk_tid] 
    htg: Mapped[int] # Home team goals

    # Total goals scored.
    @hybrid_property
    def tg(self):
        return self.atg + self.htg

    # Game winner using Python statement
    @hybrid_property
    def winner(self):
        if self.atg > self.htg:
            return self.atid
        else:
            return self.htid

    # How game ended: ft - fulltime | ot - overtime | so - shootout
    end: Mapped[str_2] 
    
    # Many-to-Many relationships between Game and Team, Player class objects
    teams: Mapped[List[Team]] = relationship(secondary=game_team_join, back_populates="games")
    players: Mapped[List[Player]] = relationship(secondary=game_player_join, back_populates="games")
    
    # Record info 
    created: Mapped[timestamp_created]
    updated: Mapped[timestamp_updated]
