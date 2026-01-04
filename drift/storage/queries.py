"""Database query functions for Drift storage."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Iterator

from sqlalchemy import select, delete
from sqlalchemy.orm import Session

from drift.storage.models import MonitoredDatabase, QueryStatsRaw, QueryText, SnapshotState, get_engine


@contextmanager
def get_session(dsn: str) -> Iterator[Session]:
    """Create a database session context manager."""
    engine = get_engine(dsn)
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def add_monitored_database(session: Session, name: str, dsn: str) -> MonitoredDatabase:
    """Add a new database to monitor."""
    db = MonitoredDatabase(name=name, connection_dsn=dsn)
    session.add(db)
    session.flush()  # Get the ID
    return db


def get_monitored_database_by_name(session: Session, name: str) -> MonitoredDatabase | None:
    """Get a monitored database by name."""
    stmt = select(MonitoredDatabase).where(MonitoredDatabase.name == name)
    return session.scalar(stmt)


def list_monitored_databases(session: Session, enabled_only: bool = False) -> list[MonitoredDatabase]:
    """List all monitored databases."""
    stmt = select(MonitoredDatabase)
    if enabled_only:
        stmt = stmt.where(MonitoredDatabase.enabled == True)  # noqa: E712
    return list(session.scalars(stmt))


def remove_monitored_database(session: Session, name: str) -> bool:
    """Remove a monitored database and all its collected data.

    Deletes related records from snapshot_state, query_stats_raw, and query_text
    before removing the database entry.

    Returns True if deleted, False if database not found.
    """
    # Find the database first
    db = get_monitored_database_by_name(session, name)
    if not db:
        return False

    # Delete related data in dependency order
    session.execute(
        delete(SnapshotState).where(SnapshotState.database_id == db.id)
    )
    session.execute(
        delete(QueryStatsRaw).where(QueryStatsRaw.database_id == db.id)
    )
    session.execute(
        delete(QueryText).where(QueryText.database_id == db.id)
    )

    # Now delete the database itself
    session.delete(db)
    return True


def get_latest_snapshot_time(session: Session, database_id: int) -> datetime | None:
    """Get the most recent snapshot time for a database."""
    stmt = (
        select(QueryStatsRaw.snapshot_time)
        .where(QueryStatsRaw.database_id == database_id)
        .order_by(QueryStatsRaw.snapshot_time.desc())
        .limit(1)
    )
    return session.scalar(stmt)


def store_query_stats(
    session: Session,
    database_id: int,
    snapshot_time: datetime,
    stats: list[dict],
) -> int:
    """Store query stats snapshot. Returns number of rows inserted."""
    rows = []
    for stat in stats:
        row = QueryStatsRaw(
            snapshot_time=snapshot_time,
            database_id=database_id,
            queryid=stat["queryid"],
            calls_delta=stat.get("calls_delta"),
            total_exec_time_delta=stat.get("total_exec_time_delta"),
            rows_delta=stat.get("rows_delta"),
            shared_blks_hit_delta=stat.get("shared_blks_hit_delta"),
            shared_blks_read_delta=stat.get("shared_blks_read_delta"),
            temp_blks_read_delta=stat.get("temp_blks_read_delta"),
            temp_blks_written_delta=stat.get("temp_blks_written_delta"),
        )
        rows.append(row)

    session.add_all(rows)
    return len(rows)


def upsert_query_text(
    session: Session,
    database_id: int,
    queryid: int,
    query: str,
    query_type: str | None,
    tables: list[str] | None,
    fingerprint: str,
) -> None:
    """Insert or update query text record."""
    existing = session.get(QueryText, (queryid, database_id))

    if existing:
        existing.last_seen = datetime.now(timezone.utc)
        # Update query text in case it changed (shouldn't happen, but be safe)
        existing.query = query
    else:
        new_record = QueryText(
            queryid=queryid,
            database_id=database_id,
            query=query,
            query_type=query_type,
            tables=tables,
            fingerprint=fingerprint,
        )
        session.add(new_record)


def get_top_queries(
    session: Session,
    database_id: int | None = None,
    period_hours: int = 24,
    sort_by: str = "total_time",
    limit: int = 20,
) -> list[dict]:
    """Get top queries by various metrics.

    Returns a list of dicts with query info and aggregated stats.
    """
    from sqlalchemy import func

    since = datetime.now(timezone.utc) - timedelta(hours=period_hours)

    # Base query: aggregate stats from raw table
    stmt = (
        select(
            QueryStatsRaw.database_id,
            QueryStatsRaw.queryid,
            func.sum(QueryStatsRaw.calls_delta).label("total_calls"),
            func.sum(QueryStatsRaw.total_exec_time_delta).label("total_time"),
            func.sum(QueryStatsRaw.rows_delta).label("total_rows"),
        )
        .where(QueryStatsRaw.snapshot_time >= since)
        .group_by(QueryStatsRaw.database_id, QueryStatsRaw.queryid)
    )

    if database_id is not None:
        stmt = stmt.where(QueryStatsRaw.database_id == database_id)

    # Sort
    if sort_by == "total_time":
        stmt = stmt.order_by(func.sum(QueryStatsRaw.total_exec_time_delta).desc().nulls_last())
    elif sort_by == "calls":
        stmt = stmt.order_by(func.sum(QueryStatsRaw.calls_delta).desc().nulls_last())
    elif sort_by == "mean_time":
        stmt = stmt.order_by(
            (func.sum(QueryStatsRaw.total_exec_time_delta) / func.nullif(func.sum(QueryStatsRaw.calls_delta), 0))
            .desc()
            .nulls_last()
        )

    stmt = stmt.limit(limit)

    results = []
    for row in session.execute(stmt):
        # Get query text
        query_text = session.get(QueryText, (row.queryid, row.database_id))
        total_calls = int(row.total_calls) if row.total_calls else 0
        total_time = float(row.total_time) if row.total_time else 0.0
        total_rows = int(row.total_rows) if row.total_rows else 0
        results.append({
            "database_id": row.database_id,
            "queryid": row.queryid,
            "total_calls": total_calls,
            "total_time_ms": total_time,
            "total_rows": total_rows,
            "mean_time_ms": total_time / total_calls if total_calls else None,
            "query": query_text.query if query_text else None,
            "query_type": query_text.query_type if query_text else None,
        })

    return results


def get_query_details(
    session: Session,
    queryid: int,
    database_id: int | None = None,
    period_hours: int = 24,
) -> dict | None:
    """Get detailed information about a specific query.

    Returns full query text, metadata, and aggregated stats.
    """
    from sqlalchemy import func

    since = datetime.now(timezone.utc) - timedelta(hours=period_hours)

    # Find query text (may exist in multiple databases)
    text_stmt = select(QueryText).where(QueryText.queryid == queryid)
    if database_id is not None:
        text_stmt = text_stmt.where(QueryText.database_id == database_id)

    query_text = session.scalar(text_stmt)
    if not query_text:
        return None

    # Get aggregated stats
    stats_stmt = (
        select(
            func.sum(QueryStatsRaw.calls_delta).label("total_calls"),
            func.sum(QueryStatsRaw.total_exec_time_delta).label("total_time"),
            func.sum(QueryStatsRaw.rows_delta).label("total_rows"),
            func.sum(QueryStatsRaw.shared_blks_hit_delta).label("total_blks_hit"),
            func.sum(QueryStatsRaw.shared_blks_read_delta).label("total_blks_read"),
            func.sum(QueryStatsRaw.temp_blks_read_delta).label("total_temp_read"),
            func.sum(QueryStatsRaw.temp_blks_written_delta).label("total_temp_written"),
            func.min(QueryStatsRaw.snapshot_time).label("first_snapshot"),
            func.max(QueryStatsRaw.snapshot_time).label("last_snapshot"),
            func.count().label("snapshot_count"),
        )
        .where(QueryStatsRaw.queryid == queryid)
        .where(QueryStatsRaw.snapshot_time >= since)
    )
    if database_id is not None:
        stats_stmt = stats_stmt.where(QueryStatsRaw.database_id == database_id)

    stats_row = session.execute(stats_stmt).first()

    total_calls = int(stats_row.total_calls) if stats_row.total_calls else 0
    total_time = float(stats_row.total_time) if stats_row.total_time else 0.0
    total_blks_hit = int(stats_row.total_blks_hit) if stats_row.total_blks_hit else 0
    total_blks_read = int(stats_row.total_blks_read) if stats_row.total_blks_read else 0

    # Calculate cache hit ratio
    total_blks = total_blks_hit + total_blks_read
    cache_hit_ratio = (total_blks_hit / total_blks * 100) if total_blks > 0 else None

    return {
        "queryid": queryid,
        "database_id": query_text.database_id,
        "query": query_text.query,
        "query_type": query_text.query_type,
        "tables": query_text.tables,
        "fingerprint": query_text.fingerprint,
        "first_seen": query_text.first_seen,
        "last_seen": query_text.last_seen,
        "period_hours": period_hours,
        "total_calls": total_calls,
        "total_time_ms": total_time,
        "mean_time_ms": total_time / total_calls if total_calls else None,
        "total_rows": int(stats_row.total_rows) if stats_row.total_rows else 0,
        "cache_hit_ratio": cache_hit_ratio,
        "temp_blks_read": int(stats_row.total_temp_read) if stats_row.total_temp_read else 0,
        "temp_blks_written": int(stats_row.total_temp_written) if stats_row.total_temp_written else 0,
        "snapshot_count": stats_row.snapshot_count or 0,
    }


def get_query_trend(
    session: Session,
    queryid: int,
    database_id: int | None = None,
    period_hours: int = 168,  # 7 days
    bucket_hours: int = 1,
) -> list[dict]:
    """Get time-series performance data for a query.

    Returns hourly (or custom bucket) aggregations for charting.
    """
    from sqlalchemy import func

    since = datetime.now(timezone.utc) - timedelta(hours=period_hours)

    # Use date_trunc to bucket by hour
    time_bucket = func.date_trunc("hour", QueryStatsRaw.snapshot_time)

    stmt = (
        select(
            time_bucket.label("bucket"),
            func.sum(QueryStatsRaw.calls_delta).label("calls"),
            func.sum(QueryStatsRaw.total_exec_time_delta).label("total_time"),
            func.sum(QueryStatsRaw.rows_delta).label("rows"),
            func.sum(QueryStatsRaw.shared_blks_hit_delta).label("blks_hit"),
            func.sum(QueryStatsRaw.shared_blks_read_delta).label("blks_read"),
        )
        .where(QueryStatsRaw.queryid == queryid)
        .where(QueryStatsRaw.snapshot_time >= since)
        .group_by(time_bucket)
        .order_by(time_bucket)
    )

    if database_id is not None:
        stmt = stmt.where(QueryStatsRaw.database_id == database_id)

    results = []
    for row in session.execute(stmt):
        calls = int(row.calls) if row.calls else 0
        total_time = float(row.total_time) if row.total_time else 0.0
        blks_hit = int(row.blks_hit) if row.blks_hit else 0
        blks_read = int(row.blks_read) if row.blks_read else 0
        total_blks = blks_hit + blks_read

        results.append({
            "time": row.bucket,
            "calls": calls,
            "total_time_ms": total_time,
            "mean_time_ms": total_time / calls if calls else 0,
            "rows": int(row.rows) if row.rows else 0,
            "cache_hit_ratio": (blks_hit / total_blks * 100) if total_blks > 0 else None,
        })

    return results


def get_snapshot_state(session: Session, database_id: int) -> dict[int, dict]:
    """Get the last known snapshot state for a database.

    Returns a dict mapping queryid -> {calls, total_exec_time, rows, ...}
    """
    stmt = select(SnapshotState).where(SnapshotState.database_id == database_id)

    result = {}
    for row in session.scalars(stmt):
        result[row.queryid] = {
            "calls": row.calls,
            "total_exec_time": row.total_exec_time,
            "rows": row.rows,
            "shared_blks_hit": row.shared_blks_hit,
            "shared_blks_read": row.shared_blks_read,
            "temp_blks_read": row.temp_blks_read,
            "temp_blks_written": row.temp_blks_written,
        }
    return result


def save_snapshot_state(
    session: Session,
    database_id: int,
    snapshot_time: datetime,
    rows: list[dict],
) -> None:
    """Save current snapshot state for future delta calculation.

    Replaces all existing state for this database with new values.
    """
    # Delete old state for this database
    session.execute(
        delete(SnapshotState).where(SnapshotState.database_id == database_id)
    )

    # Insert new state
    for row in rows:
        state = SnapshotState(
            database_id=database_id,
            queryid=row["queryid"],
            calls=row["calls"],
            total_exec_time=row["total_exec_time"],
            rows=row["rows"],
            shared_blks_hit=row["shared_blks_hit"],
            shared_blks_read=row["shared_blks_read"],
            temp_blks_read=row["temp_blks_read"],
            temp_blks_written=row["temp_blks_written"],
            snapshot_time=snapshot_time,
        )
        session.add(state)


def get_new_queries(
    session: Session,
    database_id: int | None = None,
    since_hours: int = 24,
    limit: int = 50,
) -> list[dict]:
    """Get queries that first appeared within the given time window.

    Args:
        session: Database session
        database_id: Optional filter for specific database
        since_hours: Look for queries first seen in last N hours
        limit: Maximum number of results

    Returns:
        List of dicts with query info, ordered by first_seen desc (newest first)
    """
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours)

    stmt = (
        select(QueryText)
        .where(QueryText.first_seen >= since)
        .order_by(QueryText.first_seen.desc())
        .limit(limit)
    )

    if database_id is not None:
        stmt = stmt.where(QueryText.database_id == database_id)

    results = []
    for qt in session.scalars(stmt):
        results.append({
            "queryid": qt.queryid,
            "database_id": qt.database_id,
            "query": qt.query,
            "query_type": qt.query_type,
            "tables": qt.tables,
            "first_seen": qt.first_seen,
        })

    return results


def rollup_hourly(session: Session, hours_back: int = 24) -> dict[str, int]:
    """Aggregate raw stats into hourly buckets.

    Processes raw data from the last N hours and inserts/updates hourly rollups.
    Safe to run multiple times - uses upsert semantics.

    Args:
        session: Database session
        hours_back: How many hours back to process (default 24)

    Returns:
        Dict with count of hours processed and rows upserted
    """
    from sqlalchemy import func
    from sqlalchemy.dialects.postgresql import insert

    from drift.storage.models import QueryStatsHourly

    since = datetime.now(timezone.utc) - timedelta(hours=hours_back)

    # Aggregate raw data by hour
    time_bucket = func.date_trunc("hour", QueryStatsRaw.snapshot_time)

    stmt = (
        select(
            time_bucket.label("hour"),
            QueryStatsRaw.database_id,
            QueryStatsRaw.queryid,
            func.sum(QueryStatsRaw.calls_delta).label("calls"),
            func.sum(QueryStatsRaw.total_exec_time_delta).label("total_time"),
            func.sum(QueryStatsRaw.rows_delta).label("rows"),
            func.sum(QueryStatsRaw.shared_blks_hit_delta).label("blks_hit"),
            func.sum(QueryStatsRaw.shared_blks_read_delta).label("blks_read"),
            func.sum(QueryStatsRaw.temp_blks_read_delta).label("temp_read"),
            func.sum(QueryStatsRaw.temp_blks_written_delta).label("temp_written"),
        )
        .where(QueryStatsRaw.snapshot_time >= since)
        .group_by(time_bucket, QueryStatsRaw.database_id, QueryStatsRaw.queryid)
    )

    rows_upserted = 0
    hours_seen = set()

    for row in session.execute(stmt):
        hours_seen.add(row.hour)

        calls = int(row.calls) if row.calls else 0
        total_time = float(row.total_time) if row.total_time else 0.0
        blks_hit = int(row.blks_hit) if row.blks_hit else 0
        blks_read = int(row.blks_read) if row.blks_read else 0
        total_blks = blks_hit + blks_read
        temp_total = (int(row.temp_read) if row.temp_read else 0) + \
                     (int(row.temp_written) if row.temp_written else 0)

        # Calculate derived metrics
        mean_time = total_time / calls if calls > 0 else None
        cache_hit_ratio = (blks_hit / total_blks * 100) if total_blks > 0 else None

        # Upsert into hourly table
        insert_stmt = insert(QueryStatsHourly).values(
            hour=row.hour,
            database_id=row.database_id,
            queryid=row.queryid,
            calls=calls,
            total_time_ms=total_time,
            mean_time_ms=mean_time,
            rows=int(row.rows) if row.rows else 0,
            cache_hit_ratio=cache_hit_ratio,
            temp_blks_total=temp_total,
        )

        # On conflict, update with new values
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["hour", "database_id", "queryid"],
            set_={
                "calls": insert_stmt.excluded.calls,
                "total_time_ms": insert_stmt.excluded.total_time_ms,
                "mean_time_ms": insert_stmt.excluded.mean_time_ms,
                "rows": insert_stmt.excluded.rows,
                "cache_hit_ratio": insert_stmt.excluded.cache_hit_ratio,
                "temp_blks_total": insert_stmt.excluded.temp_blks_total,
            },
        )

        session.execute(upsert_stmt)
        rows_upserted += 1

    return {
        "hours_processed": len(hours_seen),
        "rows_upserted": rows_upserted,
    }


def prune_old_data(
    session: Session,
    raw_days: int = 7,
    hourly_days: int = 90,
) -> dict[str, int]:
    """Delete data older than retention period.

    Args:
        session: Database session
        raw_days: Delete raw stats older than this many days
        hourly_days: Delete hourly stats older than this many days

    Returns:
        Dict with counts of deleted rows per table
    """
    raw_cutoff = datetime.now(timezone.utc) - timedelta(days=raw_days)
    hourly_cutoff = datetime.now(timezone.utc) - timedelta(days=hourly_days)

    # Delete old raw stats
    raw_result = session.execute(
        delete(QueryStatsRaw).where(QueryStatsRaw.snapshot_time < raw_cutoff)
    )

    # Delete old hourly stats
    from drift.storage.models import QueryStatsHourly
    hourly_result = session.execute(
        delete(QueryStatsHourly).where(QueryStatsHourly.hour < hourly_cutoff)
    )

    return {
        "query_stats_raw": raw_result.rowcount,
        "query_stats_hourly": hourly_result.rowcount,
    }
