"""Add snapshot_state table for persistent delta calculation.

Revision ID: 002
Revises: 001
Create Date: 2025-01-03

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "snapshot_state",
        sa.Column("database_id", sa.Integer(), nullable=False),
        sa.Column("queryid", sa.BigInteger(), nullable=False),
        sa.Column("calls", sa.BigInteger(), nullable=False),
        sa.Column("total_exec_time", sa.Double(), nullable=False),
        sa.Column("rows", sa.BigInteger(), nullable=False),
        sa.Column("shared_blks_hit", sa.BigInteger(), nullable=False),
        sa.Column("shared_blks_read", sa.BigInteger(), nullable=False),
        sa.Column("temp_blks_read", sa.BigInteger(), nullable=False),
        sa.Column("temp_blks_written", sa.BigInteger(), nullable=False),
        sa.Column("snapshot_time", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["database_id"], ["monitored_databases.id"]),
        sa.PrimaryKeyConstraint("database_id", "queryid"),
    )


def downgrade() -> None:
    op.drop_table("snapshot_state")
