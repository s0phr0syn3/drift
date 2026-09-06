"""Query stats API endpoints."""

from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from drift.storage.models import get_engine
from drift.storage.queries import (
    get_top_queries,
    get_query_details,
    get_query_trend,
    get_new_queries,
    list_monitored_databases,
    get_monitored_database_by_name,
)
from drift.analysis.recommendations import generate_recommendations
from drift.collector.snapshot import extract_query_type


router = APIRouter()


# Pydantic models for responses
class DatabaseInfo(BaseModel):
    id: int
    name: str
    enabled: bool
    created_at: datetime


class QuerySummary(BaseModel):
    database_id: int
    queryid: str
    total_calls: int
    total_time_ms: float
    total_rows: int
    mean_time_ms: float | None
    query: str | None
    query_type: str | None


class QueryDetails(BaseModel):
    queryid: str
    database_id: int
    query: str
    query_type: str | None
    tables: list[str] | None
    fingerprint: str
    first_seen: datetime | None
    last_seen: datetime | None
    period_hours: int
    total_calls: int
    total_time_ms: float
    mean_time_ms: float | None
    total_rows: int
    cache_hit_ratio: float | None
    temp_blks_read: int
    temp_blks_written: int
    snapshot_count: int


class TrendPoint(BaseModel):
    time: datetime
    calls: int
    total_time_ms: float
    mean_time_ms: float
    rows: int
    cache_hit_ratio: float | None


class NewQuery(BaseModel):
    queryid: str
    database_id: int
    query: str
    query_type: str | None
    tables: list[str] | None
    first_seen: datetime


class RecommendationResponse(BaseModel):
    severity: str  # critical, warning, info, ok
    category: str  # cache, temp_disk, latency, frequency, indexing, config
    title: str
    description: str
    action: str
    metric_value: str


def get_db_session(request: Request):
    """Dependency to get database session."""
    config = request.app.state.config
    engine = get_engine(config.storage.dsn)
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()


def parse_period(period: str) -> int:
    """Parse period string to hours."""
    period = period.lower().strip()
    if period.endswith("h"):
        return int(period[:-1])
    elif period.endswith("d"):
        return int(period[:-1]) * 24
    elif period.endswith("w"):
        return int(period[:-1]) * 24 * 7
    return int(period)


@router.get("/databases", response_model=list[DatabaseInfo])
def list_databases(session: Session = Depends(get_db_session)):
    """List all monitored databases."""
    databases = list_monitored_databases(session)
    return [
        DatabaseInfo(
            id=db.id,
            name=db.name,
            enabled=db.enabled,
            created_at=db.created_at,
        )
        for db in databases
    ]


@router.get("/queries", response_model=list[QuerySummary])
def get_queries(
    database: str | None = Query(None, description="Filter by database name"),
    period: str = Query("24h", description="Time period (e.g., 1h, 24h, 7d)"),
    sort: Literal["total_time", "calls", "mean_time"] = Query("total_time"),
    query_type: str | None = Query(None, description="Filter by query type (SELECT, INSERT, UPDATE, DELETE, DDL, TRANSACTION, UTILITY, DML for all DML types)"),
    limit: int = Query(50, ge=1, le=500),
    session: Session = Depends(get_db_session),
):
    """Get top queries by resource usage."""
    database_id = None
    if database:
        db = get_monitored_database_by_name(session, database)
        if not db:
            raise HTTPException(status_code=404, detail=f"Database '{database}' not found")
        database_id = db.id

    period_hours = parse_period(period)
    # Fetch more results if filtering, so we have enough after filtering
    fetch_limit = limit * 5 if query_type else limit
    queries = get_top_queries(session, database_id, period_hours, sort, fetch_limit)

    # Re-classify query types dynamically for accurate categorization
    results = []
    dml_types = {"SELECT", "INSERT", "UPDATE", "DELETE"}
    filter_types = dml_types if query_type and query_type.upper() == "DML" else {query_type.upper()} if query_type else None

    for q in queries:
        classified_type = extract_query_type(q["query"]) if q.get("query") else q.get("query_type")

        # Apply type filter
        if filter_types and classified_type not in filter_types:
            continue

        results.append(QuerySummary(**{**q, "queryid": str(q["queryid"]), "query_type": classified_type}))

        if len(results) >= limit:
            break

    return results


@router.get("/queries/new", response_model=list[NewQuery])
def get_new_queries_endpoint(
    database: str | None = Query(None, description="Filter by database name"),
    since: str = Query("24h", description="Time window (e.g., 24h, 7d)"),
    limit: int = Query(50, ge=1, le=500),
    session: Session = Depends(get_db_session),
):
    """Get queries that first appeared recently."""
    database_id = None
    if database:
        db = get_monitored_database_by_name(session, database)
        if not db:
            raise HTTPException(status_code=404, detail=f"Database '{database}' not found")
        database_id = db.id

    since_hours = parse_period(since)
    queries = get_new_queries(session, database_id, since_hours, limit)

    # Re-classify query types dynamically
    results = []
    for q in queries:
        query_type = extract_query_type(q["query"]) if q.get("query") else q.get("query_type")
        results.append(NewQuery(**{**q, "queryid": str(q["queryid"]), "query_type": query_type}))
    return results


@router.get("/queries/{queryid}", response_model=QueryDetails)
def get_query(
    queryid: str,
    database: str | None = Query(None, description="Filter by database name"),
    period: str = Query("24h", description="Time period for stats"),
    session: Session = Depends(get_db_session),
):
    """Get detailed information about a specific query."""
    database_id = None
    if database:
        db = get_monitored_database_by_name(session, database)
        if not db:
            raise HTTPException(status_code=404, detail=f"Database '{database}' not found")
        database_id = db.id

    period_hours = parse_period(period)
    queryid_int = int(queryid)
    details = get_query_details(session, queryid_int, database_id, period_hours)

    if not details:
        raise HTTPException(status_code=404, detail=f"Query {queryid} not found")

    # Re-classify query type dynamically
    query_type = extract_query_type(details["query"]) if details.get("query") else details.get("query_type")
    return QueryDetails(**{**details, "queryid": str(details["queryid"]), "query_type": query_type})


@router.get("/queries/{queryid}/trend", response_model=list[TrendPoint])
def get_query_trend_endpoint(
    queryid: str,
    database: str | None = Query(None, description="Filter by database name"),
    period: str = Query("7d", description="Time period to analyze"),
    session: Session = Depends(get_db_session),
):
    """Get query performance trend over time."""
    database_id = None
    if database:
        db = get_monitored_database_by_name(session, database)
        if not db:
            raise HTTPException(status_code=404, detail=f"Database '{database}' not found")
        database_id = db.id

    period_hours = parse_period(period)
    queryid_int = int(queryid)
    trend = get_query_trend(session, queryid_int, database_id, period_hours)

    return [TrendPoint(**t) for t in trend]


@router.get("/queries/{queryid}/recommendations", response_model=list[RecommendationResponse])
def get_query_recommendations(
    queryid: str,
    database: str | None = Query(None, description="Filter by database name"),
    period: str = Query("24h", description="Time period for analysis"),
    session: Session = Depends(get_db_session),
):
    """Get actionable recommendations for a specific query.

    Analyzes query metrics and returns prioritized recommendations
    for optimization, sorted by severity (critical issues first).
    """
    database_id = None
    if database:
        db = get_monitored_database_by_name(session, database)
        if not db:
            raise HTTPException(status_code=404, detail=f"Database '{database}' not found")
        database_id = db.id

    period_hours = parse_period(period)
    queryid_int = int(queryid)
    details = get_query_details(session, queryid_int, database_id, period_hours)

    if not details:
        raise HTTPException(status_code=404, detail=f"Query {queryid} not found")

    # Re-classify query type dynamically
    query_type = extract_query_type(details["query"]) if details.get("query") else details.get("query_type")

    # Generate recommendations based on query metrics
    recommendations = generate_recommendations(
        query=details.get("query", ""),
        query_type=query_type,
        calls=details.get("total_calls", 0),
        total_time_ms=details.get("total_time_ms", 0),
        mean_time_ms=details.get("mean_time_ms"),
        rows=details.get("total_rows", 0),
        shared_blks_hit=details.get("shared_blks_hit", 0),
        shared_blks_read=details.get("shared_blks_read", 0),
        temp_blks_read=details.get("temp_blks_read", 0),
        temp_blks_written=details.get("temp_blks_written", 0),
        cache_hit_ratio=details.get("cache_hit_ratio"),
    )

    return [RecommendationResponse(**r) for r in recommendations]
