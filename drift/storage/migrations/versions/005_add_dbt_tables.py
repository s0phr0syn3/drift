"""Add dbt integration tables.

Revision ID: 005
Revises: 004
Create Date: 2025-01-04
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY


# revision identifiers
revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # dbt_projects table
    op.create_table(
        "dbt_projects",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(255), unique=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # dbt_models table
    op.create_table(
        "dbt_models",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("project_id", sa.Integer(), sa.ForeignKey("dbt_projects.id"), nullable=False),
        sa.Column("unique_id", sa.String(500), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("schema_name", sa.String(255), nullable=True),
        sa.Column("database_name", sa.String(255), nullable=True),
        sa.Column("alias", sa.String(255), nullable=True),
        sa.Column("relation_name", sa.String(500), nullable=True),
        sa.Column("materialized", sa.String(50), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("compiled_sql", sa.Text(), nullable=True),
        sa.Column("sql_fingerprint", sa.String(64), nullable=True),
        sa.Column("depends_on", ARRAY(sa.Text()), nullable=True),
        sa.Column("tags", ARRAY(sa.Text()), nullable=True),
        sa.Column("correlated_queryid", sa.BigInteger(), nullable=True),
        sa.Column("correlation_type", sa.String(50), nullable=True),
        sa.Column("correlation_confidence", sa.Float(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_index("idx_dbt_models_unique_id", "dbt_models", ["project_id", "unique_id"], unique=True)
    op.create_index("idx_dbt_models_fingerprint", "dbt_models", ["sql_fingerprint"])
    op.create_index("idx_dbt_models_queryid", "dbt_models", ["correlated_queryid"])

    # dbt_runs table
    op.create_table(
        "dbt_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("project_id", sa.Integer(), sa.ForeignKey("dbt_projects.id"), nullable=False),
        sa.Column("invocation_id", sa.String(255), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(50), nullable=True),
        sa.Column("total_execution_time", sa.Float(), nullable=True),
        sa.Column("models_run", sa.Integer(), nullable=True),
        sa.Column("models_success", sa.Integer(), nullable=True),
        sa.Column("models_error", sa.Integer(), nullable=True),
    )

    op.create_index("idx_dbt_runs_started_at", "dbt_runs", ["started_at"])

    # dbt_model_executions table
    op.create_table(
        "dbt_model_executions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.Integer(), sa.ForeignKey("dbt_runs.id"), nullable=False),
        sa.Column("model_id", sa.Integer(), sa.ForeignKey("dbt_models.id"), nullable=False),
        sa.Column("status", sa.String(50), nullable=True),
        sa.Column("execution_time", sa.Float(), nullable=True),
        sa.Column("rows_affected", sa.BigInteger(), nullable=True),
        sa.Column("db_queryid", sa.BigInteger(), nullable=True),
        sa.Column("db_total_time_ms", sa.Float(), nullable=True),
        sa.Column("db_rows", sa.BigInteger(), nullable=True),
        sa.Column("db_cache_hit_ratio", sa.Float(), nullable=True),
        sa.Column("db_temp_blks", sa.BigInteger(), nullable=True),
    )

    op.create_index("idx_dbt_executions_run_model", "dbt_model_executions", ["run_id", "model_id"])


def downgrade() -> None:
    op.drop_index("idx_dbt_executions_run_model")
    op.drop_table("dbt_model_executions")

    op.drop_index("idx_dbt_runs_started_at")
    op.drop_table("dbt_runs")

    op.drop_index("idx_dbt_models_queryid")
    op.drop_index("idx_dbt_models_fingerprint")
    op.drop_index("idx_dbt_models_unique_id")
    op.drop_table("dbt_models")

    op.drop_table("dbt_projects")
