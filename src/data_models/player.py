from __future__ import annotations
from typing import List, TYPE_CHECKING
from datetime import timedelta

from sqlalchemy.orm import Mapped, relationship

from src.data_models.base import Base
from src.data_models.base import intpk, intfk_gid, intfk_tid, intfk_pid, str_2
from src.data_models.base import timestamp_created, timestamp_updated
from src.data_models.base import game_player_join


# Use TYPE_CHECKING constant to prevent the circular imports
if TYPE_CHECKING:
    from data_models.team import Team
    from data_models.game import Game


class Player(Base):
    __tablename__ = "player"

    # Player info
    pid: Mapped[intpk]
    name: Mapped[str]  # Player name
    pos: Mapped[str_2]  # Player position

    # Team info
    tid: Mapped[intfk_tid]

    # Many-to-One relationship between Player and Team class objects
    team: Mapped[Team] = relationship(back_populates="players")

    # Many-to-Many relationship between Team and Game class objects
    games: Mapped[List[Game]] = relationship(
        secondary=game_player_join, back_populates="players"
    )

    # Record info
    created: Mapped[timestamp_created]
    updated: Mapped[timestamp_updated]


class SkaterStat(Base):
    __tablename__ = "skater_stat"

    # Basic info
    sid: Mapped[intpk]
    pid: Mapped[intfk_pid]
    tid: Mapped[intfk_tid]
    gid: Mapped[intfk_gid]

    # Player stats
    g: Mapped[int]  # Goals
    a: Mapped[int]  # Assists
    pts: Mapped[int]  # Points
    pm: Mapped[int]  # Plus/Minus
    pim: Mapped[int]  # Penalties in Minutes
    evg: Mapped[int]  # Even Strenght Goals
    ppg: Mapped[int]  # Power Play Goals
    shg: Mapped[int]  # Short-Handed Goals
    gwg: Mapped[int]  # Game-Winning Goals
    esa: Mapped[int]  # Even Strenght Assists
    ppa: Mapped[int]  # Power Play Assists
    sha: Mapped[int]  # Short-Handed Assists
    sog: Mapped[int]  # Shot on Goal
    sp: Mapped[float]  # Shooting Percentage
    shft: Mapped[int]  # Shifts
    toi: Mapped[timedelta]  # Time on Ice in format mm:ss

    # Record info
    created: Mapped[timestamp_created]
    updated: Mapped[timestamp_updated]


class GoalieStat(Base):
    __tablename__ = "goalie_stat"

    # Basic info
    sid: Mapped[intpk]
    pid: Mapped[intfk_pid]
    tid: Mapped[intfk_tid]
    gid: Mapped[intfk_gid]

    # Goalie stats
    dec: Mapped[str]  # Decision (W - win, L - loss, O - overtime)
    ga: Mapped[int]  # Goal Against
    sa: Mapped[int]  # Shot Against
    sv: Mapped[int]  # Saves
    svp: Mapped[int]  # Saves Percentage
    so: Mapped[int]  # Shutouts
    pim: Mapped[int]  # Penalties in Minutes
    toi: Mapped[timedelta]  # Time on Ice in format mm:ss
    en: Mapped[bool]  # Empty Net (True/False)
    enga: Mapped[int]  # Empty Net Goal Against

    # Record info
    created: Mapped[timestamp_created]
    updated: Mapped[timestamp_updated]


class SkaterStatAdvanced(Base):
    __tablename__ = "skater_stat_advanced"

    # Basic info
    sid: Mapped[intpk]
    pid: Mapped[intfk_pid]
    tid: Mapped[intfk_tid]
    gid: Mapped[intfk_gid]

    # Advanced player stats
    icf: Mapped[int]  # Individual Corsi For Events
    satf: Mapped[int]  # on-ice Shots Attempts (Corsi) For Events
    sata: Mapped[int]  # on-ice Shots Attempts (Corsi) Against Events
    # Corsi For Percentage (percentage of Corsi For Events vs. opponent while one ice)
    cfp: Mapped[float]
    # Relative Corsi For Percentage for player's team when that player is on-ice vs. when not
    crel: Mapped[float]
    zso: Mapped[int]  # Offensive Zone Starts
    dzs: Mapped[int]  # Defensive Zone Starts
    ozsp: Mapped[float]  # Offensive Zone start %
    hit: Mapped[int]  # Hits
    blk: Mapped[int]  # Blocks

    # Record info
    created: Mapped[timestamp_created]
    updated: Mapped[timestamp_updated]
