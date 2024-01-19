from __future__ import annotations
from typing import List

from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, relationship

from base import Base
from base import intpk, intfk_tid, str_2, date  
from base import timestamp_created, timestamp_updated
from base import game_team_join, game_player_join

from team import Team

from player import Player 


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
    teams: Mapped[List[Team]] = relationship(secondary=game_team_join, back_populates="game")
    players: Mapped[List[Player]] = relationship(secondary=game_player_join, back_populates="game")
    
    # Record information 
    created: Mapped[timestamp_created]
    updated: Mapped[timestamp_updated]
    


