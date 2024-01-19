from __future__ import annotations
from typing import List

from sqlalchemy.orm import Mapped, relationship, relationship

from base import Base
from base import intpk, intfk_tid, intfk_gid, str_3 
from base import timestamp_created, timestamp_updated
from base import game_team_join

from player import Player

from game import Game


class Team(Base):
    __tablename__ = "team" 
    
    # Team info
    tid: Mapped[intpk] 
    name: Mapped[str] # Team name - Buffalo Sabres
    abbr: Mapped[str_3] # Abbreviation - BUF.
    
    # Many-to-Many relationship between Team and Game class objects
    games: Mapped[List[Game]] = relationship(secondary=game_team_join, back_populates="team")

    # One-to-Many relationship between Team and Player objects
    players: Mapped[List[Player]] = relationship(back_populates="team")
    
    # Record info
    created: Mapped[timestamp_created]
    updated: Mapped[timestamp_updated]


class TeamStat(Base):
    __tablename__ = "team_stat"

    # Basic info
    sid: Mapped[intpk]
    tid: Mapped[intfk_tid]
    gid: Mapped[intfk_gid]

    # Team stats in specific game
    g: Mapped[int] # Goals
    a: Mapped[int] # Assists
    pts: Mapped[int] # Points
    pim: Mapped[int] # Penalties in Minutes
    evg: Mapped[int] # Even Strenght Goals
    ppg: Mapped[int] # Power Play Goals
    shg: Mapped[int] # Short-Handed Goals
    sog: Mapped[int] # Shot on Goal
    sp: Mapped[float] # Shooting Percentage

    # Record info 
    created: Mapped[timestamp_created]
    updated: Mapped[timestamp_updated]


class TeamStatAdvanced(Base):
    __tablename__ = "team_stat_advanced"

    #Basic info
    sid: Mapped[intpk]
    tid: Mapped[intfk_tid]
    gid: Mapped[intfk_gid]

    # Advaned team stats in specific game for all situations
    satf: Mapped[int] # one-ice Shots Attempts (Corsi) For Events
    sata: Mapped[int] # one-ice Shots Attempts (Corsi) Against Events
    cfp: Mapped[float] # Corsi For Percentage (% of Corsi For Events vs. opponent while one ice)
    ozsp: Mapped[float] # Offensive Zone start %
    hit: Mapped[int] # Hits
    blk: Mapped[int] # Blocks
    
    # Record info 
    created: Mapped[timestamp_created]
    updated: Mapped[timestamp_updated]

