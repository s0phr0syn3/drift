"""Annotations API endpoints for tracking performance changes."""

from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from drift.storage.models import get_engine, Annotation, QueryStatsRaw, QueryText
from drift.storage.queries import get_monitored_database_by_name
from drift.collector.snapshot import extract_query_type


router = APIRouter()


# Pydantic models
class AnnotationCreate(BaseModel):
    title: str
    description: str | None = None
    category: Literal["index", "config", "query_rewrite", "deployment", "vacuum", "other"]
    applied_at: datetime
    database: str | None = None  # Database name (optional)
    queryid: str | None = None   # Query ID (optional)


class AnnotationResponse(BaseModel):
    id: int
    title: str
    description: str | None
    category: str
    applied_at: datetime
    database_id: int | None
    queryid: str | None
    created_at: datetime


class MetricSummary(BaseModel):
    total_calls: int
    total_time_ms: float
    mean_time_ms: float | None
    cache_hit_ratio: float | None
    temp_blks_total: int


class BeforeAfterComparison(BaseModel):
    annotation: AnnotationResponse
    before: MetricSummary
    after: MetricSummary
    improvement: dict  # Percentage changes for each metric


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


@router.get("/annotations", response_model=list[AnnotationResponse])
def list_annotations(
    database: str | None = Query(None, description="Filter by database name"),
    queryid: str | None = Query(None, description="Filter by query ID"),
    category: str | None = Query(None, description="Filter by category"),
    limit: int = Query(50, ge=1, le=500),
    session: Session = Depends(get_db_session),
):
    """List all annotations, optionally filtered."""
    stmt = select(Annotation).order_by(Annotation.applied_at.desc()).limit(limit)

    if database:
        db = get_monitored_database_by_name(session, database)
        if db:
            stmt = stmt.where(Annotation.database_id == db.id)

    if queryid:
        stmt = stmt.where(Annotation.queryid == int(queryid))

    if category:
        stmt = stmt.where(Annotation.category == category)

    annotations = list(session.scalars(stmt))

    return [
        AnnotationResponse(
            id=a.id,
            title=a.title,
            description=a.description,
            category=a.category,
            applied_at=a.applied_at,
            database_id=a.database_id,
            queryid=str(a.queryid) if a.queryid else None,
            created_at=a.created_at,
        )
        for a in annotations
    ]


@router.post("/annotations", response_model=AnnotationResponse, status_code=201)
def create_annotation(
    annotation: AnnotationCreate,
    session: Session = Depends(get_db_session),
):
    """Create a new annotation marking a performance change."""
    database_id = None
    if annotation.database:
        db = get_monitored_database_by_name(session, annotation.database)
        if not db:
            raise HTTPException(status_code=404, detail=f"Database '{annotation.database}' not found")
        database_id = db.id

    queryid = int(annotation.queryid) if annotation.queryid else None

    new_annotation = Annotation(
        title=annotation.title,
        description=annotation.description,
        category=annotation.category,
        applied_at=annotation.applied_at,
        database_id=database_id,
        queryid=queryid,
    )
    session.add(new_annotation)
    session.flush()

    return AnnotationResponse(
        id=new_annotation.id,
        title=new_annotation.title,
        description=new_annotation.description,
        category=new_annotation.category,
        applied_at=new_annotation.applied_at,
        database_id=new_annotation.database_id,
        queryid=str(new_annotation.queryid) if new_annotation.queryid else None,
        created_at=new_annotation.created_at,
    )


@router.get("/annotations/{annotation_id}", response_model=AnnotationResponse)
def get_annotation(
    annotation_id: int,
    session: Session = Depends(get_db_session),
):
    """Get a specific annotation."""
    annotation = session.get(Annotation, annotation_id)
    if not annotation:
        raise HTTPException(status_code=404, detail=f"Annotation {annotation_id} not found")

    return AnnotationResponse(
        id=annotation.id,
        title=annotation.title,
        description=annotation.description,
        category=annotation.category,
        applied_at=annotation.applied_at,
        database_id=annotation.database_id,
        queryid=str(annotation.queryid) if annotation.queryid else None,
        created_at=annotation.created_at,
    )


@router.delete("/annotations/{annotation_id}", status_code=204)
def delete_annotation(
    annotation_id: int,
    session: Session = Depends(get_db_session),
):
    """Delete an annotation."""
    annotation = session.get(Annotation, annotation_id)
    if not annotation:
        raise HTTPException(status_code=404, detail=f"Annotation {annotation_id} not found")
    session.delete(annotation)


def _calculate_metrics(
    session: Session,
    queryid: int | None,
    database_id: int | None,
    start_time: datetime,
    end_time: datetime,
) -> MetricSummary:
    """Calculate aggregated metrics for a time range."""
    stmt = select(
        func.sum(QueryStatsRaw.calls_delta).label("total_calls"),
        func.sum(QueryStatsRaw.total_exec_time_delta).label("total_time"),
        func.sum(QueryStatsRaw.rows_delta).label("total_rows"),
        func.sum(QueryStatsRaw.shared_blks_hit_delta).label("blks_hit"),
        func.sum(QueryStatsRaw.shared_blks_read_delta).label("blks_read"),
        func.sum(QueryStatsRaw.temp_blks_read_delta).label("temp_read"),
        func.sum(QueryStatsRaw.temp_blks_written_delta).label("temp_written"),
    ).where(
        QueryStatsRaw.snapshot_time >= start_time,
        QueryStatsRaw.snapshot_time < end_time,
    )

    if queryid:
        stmt = stmt.where(QueryStatsRaw.queryid == queryid)
    if database_id:
        stmt = stmt.where(QueryStatsRaw.database_id == database_id)

    row = session.execute(stmt).first()

    total_calls = int(row.total_calls) if row.total_calls else 0
    total_time = float(row.total_time) if row.total_time else 0.0
    blks_hit = int(row.blks_hit) if row.blks_hit else 0
    blks_read = int(row.blks_read) if row.blks_read else 0
    total_blks = blks_hit + blks_read
    temp_total = (int(row.temp_read) if row.temp_read else 0) + \
                 (int(row.temp_written) if row.temp_written else 0)

    return MetricSummary(
        total_calls=total_calls,
        total_time_ms=total_time,
        mean_time_ms=total_time / total_calls if total_calls > 0 else None,
        cache_hit_ratio=(blks_hit / total_blks * 100) if total_blks > 0 else None,
        temp_blks_total=temp_total,
    )


def _calculate_improvement(before: MetricSummary, after: MetricSummary) -> dict:
    """Calculate percentage improvement for each metric.

    Positive values mean improvement, negative means regression.
    """
    improvements = {}

    # Mean time: lower is better
    if before.mean_time_ms and after.mean_time_ms:
        change = ((before.mean_time_ms - after.mean_time_ms) / before.mean_time_ms) * 100
        improvements["mean_time_ms"] = round(change, 1)
    else:
        improvements["mean_time_ms"] = None

    # Cache hit ratio: higher is better
    if before.cache_hit_ratio is not None and after.cache_hit_ratio is not None:
        change = after.cache_hit_ratio - before.cache_hit_ratio
        improvements["cache_hit_ratio"] = round(change, 1)
    else:
        improvements["cache_hit_ratio"] = None

    # Temp blocks: lower is better
    if before.temp_blks_total > 0:
        change = ((before.temp_blks_total - after.temp_blks_total) / before.temp_blks_total) * 100
        improvements["temp_blks"] = round(change, 1)
    elif after.temp_blks_total == 0:
        improvements["temp_blks"] = None  # No change, both zero
    else:
        improvements["temp_blks"] = -100  # Went from 0 to something

    # Calls per hour (normalized) - informational, not improvement
    # Calculate hours in each period for normalization
    improvements["calls_change"] = None  # TODO: Implement if needed

    return improvements


@router.get("/annotations/{annotation_id}/compare", response_model=BeforeAfterComparison)
def compare_before_after(
    annotation_id: int,
    hours: int = Query(24, ge=1, le=168, description="Hours before/after to compare"),
    session: Session = Depends(get_db_session),
):
    """Compare metrics before and after an annotation.

    Returns aggregated metrics for the specified hours before the change
    and the same duration after, along with improvement percentages.
    """
    annotation = session.get(Annotation, annotation_id)
    if not annotation:
        raise HTTPException(status_code=404, detail=f"Annotation {annotation_id} not found")

    applied_at = annotation.applied_at
    before_start = applied_at - timedelta(hours=hours)
    before_end = applied_at
    after_start = applied_at
    after_end = applied_at + timedelta(hours=hours)

    before_metrics = _calculate_metrics(
        session,
        annotation.queryid,
        annotation.database_id,
        before_start,
        before_end,
    )

    after_metrics = _calculate_metrics(
        session,
        annotation.queryid,
        annotation.database_id,
        after_start,
        after_end,
    )

    improvement = _calculate_improvement(before_metrics, after_metrics)

    return BeforeAfterComparison(
        annotation=AnnotationResponse(
            id=annotation.id,
            title=annotation.title,
            description=annotation.description,
            category=annotation.category,
            applied_at=annotation.applied_at,
            database_id=annotation.database_id,
            queryid=str(annotation.queryid) if annotation.queryid else None,
            created_at=annotation.created_at,
        ),
        before=before_metrics,
        after=after_metrics,
        improvement=improvement,
    )


@router.get("/annotations/for-query/{queryid}", response_model=list[AnnotationResponse])
def get_annotations_for_query(
    queryid: str,
    database: str | None = Query(None, description="Filter by database name"),
    session: Session = Depends(get_db_session),
):
    """Get all annotations related to a specific query."""
    stmt = (
        select(Annotation)
        .where(Annotation.queryid == int(queryid))
        .order_by(Annotation.applied_at.desc())
    )

    if database:
        db = get_monitored_database_by_name(session, database)
        if db:
            stmt = stmt.where(Annotation.database_id == db.id)

    annotations = list(session.scalars(stmt))

    return [
        AnnotationResponse(
            id=a.id,
            title=a.title,
            description=a.description,
            category=a.category,
            applied_at=a.applied_at,
            database_id=a.database_id,
            queryid=str(a.queryid) if a.queryid else None,
            created_at=a.created_at,
        )
        for a in annotations
    ]
