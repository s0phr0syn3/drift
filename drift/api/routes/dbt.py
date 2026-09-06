"""dbt integration API endpoints."""

from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request, UploadFile, File
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from drift.storage.models import (
    get_engine,
    DbtProject,
    DbtModelRecord,
    DbtRun,
    DbtModelExecution,
    QueryText,
    QueryStatsRaw,
)
from drift.dbt.parser import parse_manifest, parse_run_results, DbtModel
from drift.dbt.fingerprint import fingerprint_sql
from drift.dbt.correlate import correlate_model_to_queries, generate_correlation_insights


router = APIRouter()


# Pydantic models
class ProjectResponse(BaseModel):
    id: int
    name: str
    model_count: int
    run_count: int
    created_at: datetime


class ModelResponse(BaseModel):
    id: int
    project_id: int
    unique_id: str
    name: str
    schema_name: str | None
    database_name: str | None
    relation_name: str | None
    materialized: str | None
    description: str | None
    tags: list[str] | None
    correlated_queryid: str | None
    correlation_type: str | None
    correlation_confidence: float | None
    avg_time_ms: float | None
    updated_at: datetime


class ModelDetailResponse(BaseModel):
    id: int
    project_id: int
    unique_id: str
    name: str
    schema_name: str | None
    database_name: str | None
    relation_name: str | None
    materialized: str | None
    description: str | None
    tags: list[str] | None
    correlated_queryid: str | None
    correlation_type: str | None
    correlation_confidence: float | None
    avg_time_ms: float | None
    updated_at: datetime
    compiled_sql: str | None
    depends_on: list[str] | None
    insights: list[dict]
    query_stats: dict | None


class RunResponse(BaseModel):
    id: int
    project_id: int
    invocation_id: str | None
    started_at: datetime
    completed_at: datetime | None
    status: str | None
    total_execution_time: float | None
    models_run: int | None
    models_success: int | None
    models_error: int | None


class RunDetailResponse(RunResponse):
    executions: list[dict]


class IngestRequest(BaseModel):
    project_name: str
    manifest: dict
    run_results: dict | None = None


class IngestResponse(BaseModel):
    project_id: int
    models_ingested: int
    models_correlated: int
    run_id: int | None


def get_db_session(request: Request):
    """Dependency to get database session."""
    config = request.app.state.config
    engine = get_engine(config.storage.dsn)
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@router.get("/dbt/projects", response_model=list[ProjectResponse])
def list_projects(session: Session = Depends(get_db_session)):
    """List all dbt projects."""
    projects = list(session.scalars(select(DbtProject).order_by(DbtProject.name)))

    results = []
    for p in projects:
        model_count = session.scalar(
            select(func.count()).select_from(DbtModelRecord).where(DbtModelRecord.project_id == p.id)
        )
        run_count = session.scalar(
            select(func.count()).select_from(DbtRun).where(DbtRun.project_id == p.id)
        )
        results.append(ProjectResponse(
            id=p.id,
            name=p.name,
            model_count=model_count or 0,
            run_count=run_count or 0,
            created_at=p.created_at,
        ))

    return results


@router.get("/dbt/models", response_model=list[ModelResponse])
def list_models(
    project: str | None = Query(None, description="Filter by project name"),
    project_id: int | None = Query(None, description="Filter by project ID"),
    correlated_only: bool = Query(False, description="Only show models with query correlation"),
    session: Session = Depends(get_db_session),
):
    """List dbt models."""
    from datetime import timedelta

    stmt = select(DbtModelRecord).order_by(DbtModelRecord.name)

    # Filter by project name or ID
    if project:
        proj = session.scalar(select(DbtProject).where(DbtProject.name == project))
        if not proj:
            raise HTTPException(status_code=404, detail=f"Project '{project}' not found")
        stmt = stmt.where(DbtModelRecord.project_id == proj.id)
    elif project_id:
        stmt = stmt.where(DbtModelRecord.project_id == project_id)

    if correlated_only:
        stmt = stmt.where(DbtModelRecord.correlated_queryid.isnot(None))

    models = list(session.scalars(stmt))

    # Calculate avg_time_ms for each model with a correlated query
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    results = []

    for m in models:
        avg_time_ms = None
        if m.correlated_queryid:
            # Get average execution time from recent stats
            stats_stmt = select(
                func.sum(QueryStatsRaw.total_exec_time_delta).label("total_time"),
                func.sum(QueryStatsRaw.calls_delta).label("total_calls"),
            ).where(
                QueryStatsRaw.queryid == m.correlated_queryid,
                QueryStatsRaw.snapshot_time >= since,
            )
            row = session.execute(stats_stmt).first()
            if row and row.total_calls and row.total_calls > 0:
                avg_time_ms = float(row.total_time) / int(row.total_calls)

        results.append(ModelResponse(
            id=m.id,
            project_id=m.project_id,
            unique_id=m.unique_id,
            name=m.name,
            schema_name=m.schema_name,
            database_name=m.database_name,
            relation_name=m.relation_name,
            materialized=m.materialized,
            description=m.description,
            tags=m.tags,
            correlated_queryid=str(m.correlated_queryid) if m.correlated_queryid else None,
            correlation_type=m.correlation_type,
            correlation_confidence=m.correlation_confidence,
            avg_time_ms=avg_time_ms,
            updated_at=m.updated_at,
        ))

    return results


@router.get("/dbt/models/{model_id}", response_model=ModelDetailResponse)
def get_model(
    model_id: int,
    period: str = Query("24h", description="Time period for stats"),
    session: Session = Depends(get_db_session),
):
    """Get detailed information about a dbt model."""
    model = session.get(DbtModelRecord, model_id)
    if not model:
        raise HTTPException(status_code=404, detail=f"Model {model_id} not found")

    # Get query stats if correlated
    query_stats = None
    insights = []

    if model.correlated_queryid:
        # Parse period
        period_hours = 24
        if period.endswith("h"):
            period_hours = int(period[:-1])
        elif period.endswith("d"):
            period_hours = int(period[:-1]) * 24

        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(hours=period_hours)

        # Get aggregated stats
        stats_stmt = select(
            func.sum(QueryStatsRaw.calls_delta).label("total_calls"),
            func.sum(QueryStatsRaw.total_exec_time_delta).label("total_time"),
            func.sum(QueryStatsRaw.rows_delta).label("total_rows"),
            func.sum(QueryStatsRaw.shared_blks_hit_delta).label("blks_hit"),
            func.sum(QueryStatsRaw.shared_blks_read_delta).label("blks_read"),
            func.sum(QueryStatsRaw.temp_blks_read_delta).label("temp_read"),
            func.sum(QueryStatsRaw.temp_blks_written_delta).label("temp_written"),
        ).where(
            QueryStatsRaw.queryid == model.correlated_queryid,
            QueryStatsRaw.snapshot_time >= since,
        )

        row = session.execute(stats_stmt).first()

        if row and row.total_calls:
            total_calls = int(row.total_calls)
            total_time = float(row.total_time) if row.total_time else 0
            blks_hit = int(row.blks_hit) if row.blks_hit else 0
            blks_read = int(row.blks_read) if row.blks_read else 0
            total_blks = blks_hit + blks_read
            temp_read = int(row.temp_read) if row.temp_read else 0
            temp_written = int(row.temp_written) if row.temp_written else 0

            query_stats = {
                "total_calls": total_calls,
                "total_time_ms": total_time,
                "mean_time_ms": total_time / total_calls if total_calls else None,
                "total_rows": int(row.total_rows) if row.total_rows else 0,
                "cache_hit_ratio": (blks_hit / total_blks * 100) if total_blks > 0 else None,
                "temp_blks_read": temp_read,
                "temp_blks_written": temp_written,
            }

            # Generate insights
            dbt_model = DbtModel(
                unique_id=model.unique_id,
                name=model.name,
                schema_name=model.schema_name,
                database=model.database_name,
                alias=model.alias,
                materialized=model.materialized or "view",
                compiled_sql=model.compiled_sql,
                depends_on=model.depends_on or [],
            )
            insights = generate_correlation_insights(dbt_model, query_stats)

    # Calculate avg_time_ms from query_stats
    avg_time_ms = None
    if query_stats and query_stats.get("mean_time_ms"):
        avg_time_ms = query_stats["mean_time_ms"]

    return ModelDetailResponse(
        id=model.id,
        project_id=model.project_id,
        unique_id=model.unique_id,
        name=model.name,
        schema_name=model.schema_name,
        database_name=model.database_name,
        relation_name=model.relation_name,
        materialized=model.materialized,
        description=model.description,
        tags=model.tags,
        correlated_queryid=str(model.correlated_queryid) if model.correlated_queryid else None,
        correlation_type=model.correlation_type,
        correlation_confidence=model.correlation_confidence,
        avg_time_ms=avg_time_ms,
        updated_at=model.updated_at,
        compiled_sql=model.compiled_sql,
        depends_on=model.depends_on,
        insights=insights,
        query_stats=query_stats,
    )


@router.get("/dbt/runs", response_model=list[RunResponse])
def list_runs(
    project: str | None = Query(None, description="Filter by project name"),
    limit: int = Query(20, ge=1, le=100),
    session: Session = Depends(get_db_session),
):
    """List dbt runs."""
    stmt = select(DbtRun).order_by(DbtRun.started_at.desc()).limit(limit)

    if project:
        proj = session.scalar(select(DbtProject).where(DbtProject.name == project))
        if not proj:
            raise HTTPException(status_code=404, detail=f"Project '{project}' not found")
        stmt = stmt.where(DbtRun.project_id == proj.id)

    runs = list(session.scalars(stmt))

    return [
        RunResponse(
            id=r.id,
            project_id=r.project_id,
            invocation_id=r.invocation_id,
            started_at=r.started_at,
            completed_at=r.completed_at,
            status=r.status,
            total_execution_time=r.total_execution_time,
            models_run=r.models_run,
            models_success=r.models_success,
            models_error=r.models_error,
        )
        for r in runs
    ]


@router.get("/dbt/runs/{run_id}", response_model=RunDetailResponse)
def get_run(
    run_id: int,
    session: Session = Depends(get_db_session),
):
    """Get detailed information about a dbt run."""
    run = session.get(DbtRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    # Get executions with model info
    executions = []
    exec_stmt = (
        select(DbtModelExecution)
        .where(DbtModelExecution.run_id == run_id)
        .order_by(DbtModelExecution.execution_time.desc().nulls_last())
    )

    for ex in session.scalars(exec_stmt):
        model = session.get(DbtModelRecord, ex.model_id)
        executions.append({
            "model_id": ex.model_id,
            "model_name": model.name if model else "unknown",
            "model_unique_id": model.unique_id if model else None,
            "status": ex.status,
            "execution_time": ex.execution_time,
            "rows_affected": ex.rows_affected,
            "db_queryid": str(ex.db_queryid) if ex.db_queryid else None,
            "db_total_time_ms": ex.db_total_time_ms,
            "db_cache_hit_ratio": ex.db_cache_hit_ratio,
            "db_temp_blks": ex.db_temp_blks,
        })

    return RunDetailResponse(
        id=run.id,
        project_id=run.project_id,
        invocation_id=run.invocation_id,
        started_at=run.started_at,
        completed_at=run.completed_at,
        status=run.status,
        total_execution_time=run.total_execution_time,
        models_run=run.models_run,
        models_success=run.models_success,
        models_error=run.models_error,
        executions=executions,
    )


@router.post("/dbt/ingest", response_model=IngestResponse)
def ingest_dbt_artifacts(
    data: IngestRequest,
    session: Session = Depends(get_db_session),
):
    """Ingest dbt manifest.json and optionally run_results.json.

    This endpoint:
    1. Creates or updates the project
    2. Parses and stores model definitions
    3. Correlates models with pg_stat_statements queries
    4. If run_results provided, stores run execution data
    """
    # Get or create project
    project = session.scalar(
        select(DbtProject).where(DbtProject.name == data.project_name)
    )
    if not project:
        project = DbtProject(name=data.project_name)
        session.add(project)
        session.flush()

    # Parse manifest (passed as dict)
    models_ingested = 0
    models_correlated = 0

    # Get existing queries for correlation
    queries = []
    for qt in session.scalars(select(QueryText)):
        queries.append({
            "queryid": qt.queryid,
            "query": qt.query,
            "fingerprint": qt.fingerprint,
            "tables": qt.tables,
        })

    # Process nodes from manifest
    nodes = data.manifest.get("nodes", {})
    for unique_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue

        # Extract compiled SQL
        compiled_sql = node.get("compiled_code") or node.get("compiled_sql")

        # Calculate fingerprint
        sql_fp = fingerprint_sql(compiled_sql) if compiled_sql else None

        # Build depends_on list
        depends_on = [
            d for d in node.get("depends_on", {}).get("nodes", [])
            if d.startswith("model.")
        ]

        # Get relation name
        schema = node.get("schema")
        alias = node.get("alias") or node.get("name")
        relation_name = f"{schema}.{alias}" if schema else alias

        # Check if model exists
        existing = session.scalar(
            select(DbtModelRecord).where(
                DbtModelRecord.project_id == project.id,
                DbtModelRecord.unique_id == unique_id,
            )
        )

        if existing:
            # Update existing model
            existing.name = node.get("name", "")
            existing.schema_name = schema
            existing.database_name = node.get("database")
            existing.alias = node.get("alias")
            existing.relation_name = relation_name
            existing.materialized = node.get("config", {}).get("materialized")
            existing.description = node.get("description")
            existing.compiled_sql = compiled_sql
            existing.sql_fingerprint = sql_fp
            existing.depends_on = depends_on
            existing.tags = node.get("tags", [])
            existing.updated_at = datetime.now(timezone.utc)
            model_record = existing
        else:
            # Create new model
            model_record = DbtModelRecord(
                project_id=project.id,
                unique_id=unique_id,
                name=node.get("name", ""),
                schema_name=schema,
                database_name=node.get("database"),
                alias=node.get("alias"),
                relation_name=relation_name,
                materialized=node.get("config", {}).get("materialized"),
                description=node.get("description"),
                compiled_sql=compiled_sql,
                sql_fingerprint=sql_fp,
                depends_on=depends_on,
                tags=node.get("tags", []),
            )
            session.add(model_record)
            session.flush()

        models_ingested += 1

        # Try to correlate with pg_stat_statements
        if compiled_sql and queries:
            dbt_model = DbtModel(
                unique_id=unique_id,
                name=node.get("name", ""),
                schema_name=schema,
                database=node.get("database"),
                alias=node.get("alias"),
                materialized=node.get("config", {}).get("materialized", "view"),
                compiled_sql=compiled_sql,
                depends_on=depends_on,
            )

            correlation = correlate_model_to_queries(dbt_model, queries)
            if correlation:
                model_record.correlated_queryid = correlation.queryid
                model_record.correlation_type = correlation.match_type
                model_record.correlation_confidence = correlation.confidence
                models_correlated += 1

    # Process run results if provided
    run_id = None
    if data.run_results:
        run_results = data.run_results

        # Get metadata
        metadata = run_results.get("metadata", {})
        invocation_id = metadata.get("invocation_id")
        elapsed_time = run_results.get("elapsed_time", 0)

        results = run_results.get("results", [])

        # Count statuses
        success_count = sum(1 for r in results if r.get("status") == "success" and r.get("unique_id", "").startswith("model."))
        error_count = sum(1 for r in results if r.get("status") == "error" and r.get("unique_id", "").startswith("model."))
        model_count = sum(1 for r in results if r.get("unique_id", "").startswith("model."))

        # Determine overall status
        if error_count > 0:
            status = "error"
        elif success_count == model_count:
            status = "success"
        else:
            status = "partial"

        # Create run record
        run = DbtRun(
            project_id=project.id,
            invocation_id=invocation_id,
            started_at=datetime.now(timezone.utc),  # TODO: parse from results
            completed_at=datetime.now(timezone.utc),
            status=status,
            total_execution_time=elapsed_time,
            models_run=model_count,
            models_success=success_count,
            models_error=error_count,
        )
        session.add(run)
        session.flush()
        run_id = run.id

        # Create execution records
        for result in results:
            uid = result.get("unique_id", "")
            if not uid.startswith("model."):
                continue

            # Find model record
            model_record = session.scalar(
                select(DbtModelRecord).where(
                    DbtModelRecord.project_id == project.id,
                    DbtModelRecord.unique_id == uid,
                )
            )
            if not model_record:
                continue

            exec_time = result.get("execution_time", 0)
            adapter_response = result.get("adapter_response", {})
            rows_affected = adapter_response.get("rows_affected")

            execution = DbtModelExecution(
                run_id=run.id,
                model_id=model_record.id,
                status=result.get("status"),
                execution_time=exec_time,
                rows_affected=rows_affected,
                db_queryid=model_record.correlated_queryid,
            )
            session.add(execution)

    return IngestResponse(
        project_id=project.id,
        models_ingested=models_ingested,
        models_correlated=models_correlated,
        run_id=run_id,
    )
