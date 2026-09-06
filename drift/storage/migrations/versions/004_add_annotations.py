"""Add annotations table for tracking performance changes.

Revision ID: 004
Revises: 003
Create Date: 2025-01-04
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers
revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "annotations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("category", sa.String(50), nullable=False),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("database_id", sa.Integer(), sa.ForeignKey("monitored_databases.id"), nullable=True),
        sa.Column("queryid", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_index("idx_annotations_applied_at", "annotations", ["applied_at"])
    op.create_index("idx_annotations_queryid", "annotations", ["queryid"])


def downgrade() -> None:
    op.drop_index("idx_annotations_queryid")
    op.drop_index("idx_annotations_applied_at")
    op.drop_table("annotations")
