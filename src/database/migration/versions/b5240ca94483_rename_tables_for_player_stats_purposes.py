"""Rename tables for player stats purposes.

Revision ID: b5240ca94483
Revises: f1416f18d188
Create Date: 2024-02-18 18:34:42.597300

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'b5240ca94483'
down_revision: Union[str, None] = 'f1416f18d188'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('goalie_stat',
    sa.Column('sid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('pid', sa.Integer(), nullable=False),
    sa.Column('tid', sa.Integer(), nullable=False),
    sa.Column('gid', sa.Integer(), nullable=False),
    sa.Column('dec', sa.String(), nullable=False),
    sa.Column('ga', sa.Integer(), nullable=False),
    sa.Column('sa', sa.Integer(), nullable=False),
    sa.Column('sv', sa.Integer(), nullable=False),
    sa.Column('svp', sa.Integer(), nullable=False),
    sa.Column('so', sa.Integer(), nullable=False),
    sa.Column('pim', sa.Integer(), nullable=False),
    sa.Column('en', sa.Boolean(), nullable=False),
    sa.Column('enga', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP(0)'), nullable=False),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['gid'], ['game.gid'], name=op.f('fk_goalie_stat_gid_game')),
    sa.ForeignKeyConstraint(['pid'], ['player.pid'], name=op.f('fk_goalie_stat_pid_player')),
    sa.ForeignKeyConstraint(['tid'], ['team.tid'], name=op.f('fk_goalie_stat_tid_team')),
    sa.PrimaryKeyConstraint('sid', name=op.f('pk_goalie_stat'))
    )
    op.create_table('skater_stat',
    sa.Column('sid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('pid', sa.Integer(), nullable=False),
    sa.Column('tid', sa.Integer(), nullable=False),
    sa.Column('gid', sa.Integer(), nullable=False),
    sa.Column('g', sa.Integer(), nullable=False),
    sa.Column('a', sa.Integer(), nullable=False),
    sa.Column('pts', sa.Integer(), nullable=False),
    sa.Column('pm', sa.Integer(), nullable=False),
    sa.Column('pim', sa.Integer(), nullable=False),
    sa.Column('evg', sa.Integer(), nullable=False),
    sa.Column('ppg', sa.Integer(), nullable=False),
    sa.Column('shg', sa.Integer(), nullable=False),
    sa.Column('gwg', sa.Integer(), nullable=False),
    sa.Column('esa', sa.Integer(), nullable=False),
    sa.Column('ppa', sa.Integer(), nullable=False),
    sa.Column('sha', sa.Integer(), nullable=False),
    sa.Column('sog', sa.Integer(), nullable=False),
    sa.Column('sp', sa.Float(), nullable=False),
    sa.Column('shft', sa.Integer(), nullable=False),
    sa.Column('toi', sa.Interval(), nullable=False),
    sa.Column('created', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP(0)'), nullable=False),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['gid'], ['game.gid'], name=op.f('fk_skater_stat_gid_game')),
    sa.ForeignKeyConstraint(['pid'], ['player.pid'], name=op.f('fk_skater_stat_pid_player')),
    sa.ForeignKeyConstraint(['tid'], ['team.tid'], name=op.f('fk_skater_stat_tid_team')),
    sa.PrimaryKeyConstraint('sid', name=op.f('pk_skater_stat'))
    )
    op.create_table('skater_stat_advanced',
    sa.Column('sid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('pid', sa.Integer(), nullable=False),
    sa.Column('tid', sa.Integer(), nullable=False),
    sa.Column('gid', sa.Integer(), nullable=False),
    sa.Column('icf', sa.Integer(), nullable=False),
    sa.Column('satf', sa.Integer(), nullable=False),
    sa.Column('sata', sa.Integer(), nullable=False),
    sa.Column('cfp', sa.Float(), nullable=False),
    sa.Column('crel', sa.Float(), nullable=False),
    sa.Column('zso', sa.Integer(), nullable=False),
    sa.Column('dzs', sa.Integer(), nullable=False),
    sa.Column('ozsp', sa.Float(), nullable=False),
    sa.Column('hit', sa.Integer(), nullable=False),
    sa.Column('blk', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP(0)'), nullable=False),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['gid'], ['game.gid'], name=op.f('fk_skater_stat_advanced_gid_game')),
    sa.ForeignKeyConstraint(['pid'], ['player.pid'], name=op.f('fk_skater_stat_advanced_pid_player')),
    sa.ForeignKeyConstraint(['tid'], ['team.tid'], name=op.f('fk_skater_stat_advanced_tid_team')),
    sa.PrimaryKeyConstraint('sid', name=op.f('pk_skater_stat_advanced'))
    )
    op.drop_table('player_stat_advanced')
    op.drop_table('player_stat')
    op.drop_table('goalie')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('goalie',
    sa.Column('sid', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('pid', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('tid', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('gid', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('dec', sa.VARCHAR(), autoincrement=False, nullable=False),
    sa.Column('ga', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('sa', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('sv', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('svp', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('so', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('pim', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('en', sa.BOOLEAN(), autoincrement=False, nullable=False),
    sa.Column('enga', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('created', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP(0)'), autoincrement=False, nullable=False),
    sa.Column('updated', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.ForeignKeyConstraint(['gid'], ['game.gid'], name='fk_goalie_gid_game'),
    sa.ForeignKeyConstraint(['pid'], ['player.pid'], name='fk_goalie_pid_player'),
    sa.ForeignKeyConstraint(['tid'], ['team.tid'], name='fk_goalie_tid_team'),
    sa.PrimaryKeyConstraint('sid', name='pk_goalie')
    )
    op.create_table('player_stat',
    sa.Column('sid', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('pid', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('tid', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('gid', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('g', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('a', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('pts', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('pm', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('pim', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('evg', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('ppg', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('shg', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('gwg', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('esa', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('ppa', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('sha', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('sog', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('sp', sa.DOUBLE_PRECISION(precision=53), autoincrement=False, nullable=False),
    sa.Column('shft', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('toi', postgresql.INTERVAL(), autoincrement=False, nullable=False),
    sa.Column('created', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP(0)'), autoincrement=False, nullable=False),
    sa.Column('updated', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.ForeignKeyConstraint(['gid'], ['game.gid'], name='fk_player_stat_gid_game'),
    sa.ForeignKeyConstraint(['pid'], ['player.pid'], name='fk_player_stat_pid_player'),
    sa.ForeignKeyConstraint(['tid'], ['team.tid'], name='fk_player_stat_tid_team'),
    sa.PrimaryKeyConstraint('sid', name='pk_player_stat')
    )
    op.create_table('player_stat_advanced',
    sa.Column('sid', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('pid', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('tid', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('gid', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('icf', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('satf', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('sata', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('cfp', sa.DOUBLE_PRECISION(precision=53), autoincrement=False, nullable=False),
    sa.Column('crel', sa.DOUBLE_PRECISION(precision=53), autoincrement=False, nullable=False),
    sa.Column('zso', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('dzs', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('ozsp', sa.DOUBLE_PRECISION(precision=53), autoincrement=False, nullable=False),
    sa.Column('hit', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('blk', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('created', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP(0)'), autoincrement=False, nullable=False),
    sa.Column('updated', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.ForeignKeyConstraint(['gid'], ['game.gid'], name='fk_player_stat_advanced_gid_game'),
    sa.ForeignKeyConstraint(['pid'], ['player.pid'], name='fk_player_stat_advanced_pid_player'),
    sa.ForeignKeyConstraint(['tid'], ['team.tid'], name='fk_player_stat_advanced_tid_team'),
    sa.PrimaryKeyConstraint('sid', name='pk_player_stat_advanced')
    )
    op.drop_table('skater_stat_advanced')
    op.drop_table('skater_stat')
    op.drop_table('goalie_stat')
    # ### end Alembic commands ###