"""Initial schema.

Revision ID: 001
Revises:
Create Date: 2025-01-03

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Monitored databases registry
    op.create_table(
        "monitored_databases",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("connection_dsn", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )

    # Query text registry
    op.create_table(
        "query_text",
        sa.Column("queryid", sa.BigInteger(), nullable=False),
        sa.Column("database_id", sa.Integer(), nullable=False),
        sa.Column("query", sa.Text(), nullable=False),
        sa.Column("query_type", sa.String(50), nullable=True),
        sa.Column("tables", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column(
            "first_seen",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "last_seen",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["database_id"], ["monitored_databases.id"]),
        sa.PrimaryKeyConstraint("queryid", "database_id"),
    )
    op.create_index("idx_query_text_fingerprint", "query_text", ["fingerprint"])

    # Raw query stats (deltas)
    op.create_table(
        "query_stats_raw",
        sa.Column("snapshot_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("database_id", sa.Integer(), nullable=False),
        sa.Column("queryid", sa.BigInteger(), nullable=False),
        sa.Column("calls_delta", sa.BigInteger(), nullable=True),
        sa.Column("total_exec_time_delta", sa.Double(), nullable=True),
        sa.Column("rows_delta", sa.BigInteger(), nullable=True),
        sa.Column("shared_blks_hit_delta", sa.BigInteger(), nullable=True),
        sa.Column("shared_blks_read_delta", sa.BigInteger(), nullable=True),
        sa.Column("temp_blks_read_delta", sa.BigInteger(), nullable=True),
        sa.Column("temp_blks_written_delta", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(["database_id"], ["monitored_databases.id"]),
        sa.PrimaryKeyConstraint("snapshot_time", "database_id", "queryid"),
    )

    # Hourly rollups
    op.create_table(
        "query_stats_hourly",
        sa.Column("hour", sa.DateTime(timezone=True), nullable=False),
        sa.Column("database_id", sa.Integer(), nullable=False),
        sa.Column("queryid", sa.BigInteger(), nullable=False),
        sa.Column("calls", sa.BigInteger(), nullable=True),
        sa.Column("total_time_ms", sa.Double(), nullable=True),
        sa.Column("mean_time_ms", sa.Double(), nullable=True),
        sa.Column("rows", sa.BigInteger(), nullable=True),
        sa.Column("cache_hit_ratio", sa.Double(), nullable=True),
        sa.Column("temp_blks_total", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(["database_id"], ["monitored_databases.id"]),
        sa.PrimaryKeyConstraint("hour", "database_id", "queryid"),
    )


def downgrade() -> None:
    op.drop_table("query_stats_hourly")
    op.drop_table("query_stats_raw")
    op.drop_index("idx_query_text_fingerprint", table_name="query_text")
    op.drop_table("query_text")
    op.drop_table("monitored_databases")
