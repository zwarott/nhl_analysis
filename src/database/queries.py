from typing import Type

from sqlalchemy import select, func, desc

from src.session_config import Session

from src.data_models.base import Base
from src.data_models.game import Game
from src.data_models.team import Team, TeamStat


def select_all(class_obj: Type[Base]):
    with Session.begin() as session:
        stmt = select(class_obj)
        all_data = session.execute(stmt)
        for row in all_data.scalars():
            print(row)


def select_join():
    with Session.begin() as session:
        stmt = (
            select(Team.abbr)
            .join(Game, Team.tid == Game.atid)
            .where(Game.date == '2023-10-10')
        )
        selected_data = session.execute(stmt)
        for abbr in selected_data.scalars():
            print(abbr)

def select_team():
    with Session.begin() as session:
        stmt = (
            select(Team.tid)
            .where(Team.abbr == 'BUF')
        )
        selected_data = session.execute(stmt)
        for abbr in selected_data.scalars():
            print(abbr)


def avg_team_sog():
    with Session.begin() as session:
        stmt = (
            select(Team.tid, Team.abbr, func.round(func.avg(TeamStat.sog), 2).label("avg_sog"))
            .join(TeamStat, Team.tid == TeamStat.tid)
            .group_by(Team.tid, Team.abbr)
            .order_by(desc("avg_sog"))
        )
        output = session.execute(stmt).all()
        for row in output:
            print(row.abbr, row.avg_sog)
