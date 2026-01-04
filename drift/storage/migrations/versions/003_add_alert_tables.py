"""Add alert_rules and alert_events tables.

Revision ID: 003
Revises: 002
Create Date: 2025-01-04

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "alert_rules",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("enabled", sa.Boolean(), default=True),
        sa.Column("rule_type", sa.String(50), nullable=False),
        sa.Column("threshold", JSONB(), nullable=False),
        sa.Column("notification", JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "alert_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("rule_id", sa.Integer(), nullable=True),
        sa.Column("database_id", sa.Integer(), nullable=True),
        sa.Column("queryid", sa.BigInteger(), nullable=True),
        sa.Column("triggered_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("details", JSONB(), nullable=True),
        sa.Column("acknowledged", sa.Boolean(), default=False),
        sa.ForeignKeyConstraint(["rule_id"], ["alert_rules.id"]),
        sa.ForeignKeyConstraint(["database_id"], ["monitored_databases.id"]),
    )

    # Index for finding unacknowledged alerts
    op.create_index("idx_alert_events_unack", "alert_events", ["acknowledged", "triggered_at"])


def downgrade() -> None:
    op.drop_index("idx_alert_events_unack")
    op.drop_table("alert_events")
    op.drop_table("alert_rules")
