"""Time-series analysis and baseline comparison for query performance."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from drift.storage.models import QueryStatsHourly, QueryStatsRaw


@dataclass
class QueryBaseline:
    """Baseline performance metrics for a query."""

    queryid: int
    database_id: int
    mean_time_ms: float | None
    calls_per_hour: float | None
    cache_hit_ratio: float | None
    temp_blks_per_call: float | None
    sample_hours: int  # Number of hours used to calculate baseline


@dataclass
class Anomaly:
    """Detected performance anomaly."""

    anomaly_type: str  # latency_increase, cache_drop, temp_disk
    queryid: int
    database_id: int
    current_value: float
    baseline_value: float
    percent_change: float
    details: dict


def calculate_baseline(
    session: Session,
    queryid: int,
    database_id: int,
    baseline_days: int = 7,
    exclude_recent_hours: int = 1,
) -> QueryBaseline | None:
    """Calculate baseline metrics using historical data.

    Uses hourly rollup data from the past N days, excluding the most recent
    hours to avoid comparing against the current anomalous period.

    Args:
        session: Database session
        queryid: Query to calculate baseline for
        database_id: Database the query belongs to
        baseline_days: Number of days of history to use (default 7)
        exclude_recent_hours: Skip recent hours (default 1)

    Returns:
        QueryBaseline with average metrics, or None if insufficient data
    """
    now = datetime.now(timezone.utc)
    baseline_end = now - timedelta(hours=exclude_recent_hours)
    baseline_start = baseline_end - timedelta(days=baseline_days)

    stmt = (
        select(
            func.avg(QueryStatsHourly.mean_time_ms).label("avg_mean_time"),
            func.avg(QueryStatsHourly.calls).label("avg_calls"),
            func.avg(QueryStatsHourly.cache_hit_ratio).label("avg_cache_hit"),
            func.sum(QueryStatsHourly.temp_blks_total).label("total_temp"),
            func.sum(QueryStatsHourly.calls).label("total_calls"),
            func.count().label("sample_hours"),
        )
        .where(QueryStatsHourly.queryid == queryid)
        .where(QueryStatsHourly.database_id == database_id)
        .where(QueryStatsHourly.hour >= baseline_start)
        .where(QueryStatsHourly.hour < baseline_end)
    )

    result = session.execute(stmt).first()

    if not result or result.sample_hours < 1:
        return None

    # Calculate temp blocks per call
    temp_per_call = None
    if result.total_calls and result.total_calls > 0 and result.total_temp:
        temp_per_call = result.total_temp / result.total_calls

    return QueryBaseline(
        queryid=queryid,
        database_id=database_id,
        mean_time_ms=float(result.avg_mean_time) if result.avg_mean_time else None,
        calls_per_hour=float(result.avg_calls) if result.avg_calls else None,
        cache_hit_ratio=float(result.avg_cache_hit) if result.avg_cache_hit else None,
        temp_blks_per_call=temp_per_call,
        sample_hours=result.sample_hours,
    )


def get_recent_metrics(
    session: Session,
    queryid: int,
    database_id: int,
    hours: int = 1,
) -> dict | None:
    """Get aggregated metrics for the most recent time period.

    Args:
        session: Database session
        queryid: Query to get metrics for
        database_id: Database the query belongs to
        hours: Number of recent hours to aggregate (default 1)

    Returns:
        Dict with current metrics, or None if no recent data
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    stmt = (
        select(
            func.sum(QueryStatsRaw.calls_delta).label("calls"),
            func.sum(QueryStatsRaw.total_exec_time_delta).label("total_time"),
            func.sum(QueryStatsRaw.shared_blks_hit_delta).label("blks_hit"),
            func.sum(QueryStatsRaw.shared_blks_read_delta).label("blks_read"),
            func.sum(QueryStatsRaw.temp_blks_read_delta).label("temp_read"),
            func.sum(QueryStatsRaw.temp_blks_written_delta).label("temp_written"),
        )
        .where(QueryStatsRaw.queryid == queryid)
        .where(QueryStatsRaw.database_id == database_id)
        .where(QueryStatsRaw.snapshot_time >= since)
    )

    result = session.execute(stmt).first()

    if not result or not result.calls or result.calls == 0:
        return None

    calls = int(result.calls)
    total_time = float(result.total_time) if result.total_time else 0.0
    blks_hit = int(result.blks_hit) if result.blks_hit else 0
    blks_read = int(result.blks_read) if result.blks_read else 0
    total_blks = blks_hit + blks_read
    temp_total = (int(result.temp_read) if result.temp_read else 0) + \
                 (int(result.temp_written) if result.temp_written else 0)

    return {
        "calls": calls,
        "mean_time_ms": total_time / calls if calls > 0 else 0,
        "cache_hit_ratio": (blks_hit / total_blks * 100) if total_blks > 0 else None,
        "temp_blks_per_call": temp_total / calls if calls > 0 else 0,
    }


def detect_latency_anomaly(
    baseline: QueryBaseline,
    current: dict,
    threshold_percent: float = 50.0,
) -> Anomaly | None:
    """Detect if current latency is significantly higher than baseline.

    Args:
        baseline: Historical baseline metrics
        current: Current period metrics
        threshold_percent: Percent increase to trigger alert (default 50%)

    Returns:
        Anomaly if detected, None otherwise
    """
    if not baseline.mean_time_ms or baseline.mean_time_ms <= 0:
        return None

    current_mean = current.get("mean_time_ms", 0)
    if not current_mean:
        return None

    percent_change = ((current_mean - baseline.mean_time_ms) / baseline.mean_time_ms) * 100

    if percent_change >= threshold_percent:
        return Anomaly(
            anomaly_type="latency_increase",
            queryid=baseline.queryid,
            database_id=baseline.database_id,
            current_value=current_mean,
            baseline_value=baseline.mean_time_ms,
            percent_change=percent_change,
            details={
                "threshold_percent": threshold_percent,
                "baseline_hours": baseline.sample_hours,
            },
        )

    return None


def detect_cache_anomaly(
    baseline: QueryBaseline,
    current: dict,
    min_drop_percent: float = 10.0,
    min_baseline_ratio: float = 80.0,
) -> Anomaly | None:
    """Detect if cache hit ratio dropped significantly.

    Args:
        baseline: Historical baseline metrics
        current: Current period metrics
        min_drop_percent: Minimum drop in percentage points to trigger (default 10)
        min_baseline_ratio: Only alert if baseline was above this (default 80%)

    Returns:
        Anomaly if detected, None otherwise
    """
    if not baseline.cache_hit_ratio or baseline.cache_hit_ratio < min_baseline_ratio:
        return None

    current_ratio = current.get("cache_hit_ratio")
    if current_ratio is None:
        return None

    drop = baseline.cache_hit_ratio - current_ratio
    percent_change = (drop / baseline.cache_hit_ratio) * 100

    if drop >= min_drop_percent:
        return Anomaly(
            anomaly_type="cache_drop",
            queryid=baseline.queryid,
            database_id=baseline.database_id,
            current_value=current_ratio,
            baseline_value=baseline.cache_hit_ratio,
            percent_change=-percent_change,  # Negative because it's a drop
            details={
                "drop_points": drop,
                "min_drop_threshold": min_drop_percent,
            },
        )

    return None


def detect_temp_disk_anomaly(
    baseline: QueryBaseline,
    current: dict,
    threshold_percent: float = 100.0,
    min_temp_blocks: int = 100,
) -> Anomaly | None:
    """Detect if temp disk usage increased significantly.

    Args:
        baseline: Historical baseline metrics
        current: Current period metrics
        threshold_percent: Percent increase to trigger (default 100% = doubled)
        min_temp_blocks: Minimum absolute temp blocks to consider (default 100)

    Returns:
        Anomaly if detected, None otherwise
    """
    current_temp = current.get("temp_blks_per_call", 0)
    if not current_temp or current_temp < min_temp_blocks:
        return None

    baseline_temp = baseline.temp_blks_per_call or 0

    # If baseline was zero or very low, any significant temp usage is notable
    if baseline_temp < 1:
        return Anomaly(
            anomaly_type="temp_disk",
            queryid=baseline.queryid,
            database_id=baseline.database_id,
            current_value=current_temp,
            baseline_value=baseline_temp,
            percent_change=float("inf"),
            details={
                "message": "New temp disk usage detected",
                "temp_blks_per_call": current_temp,
            },
        )

    percent_change = ((current_temp - baseline_temp) / baseline_temp) * 100

    if percent_change >= threshold_percent:
        return Anomaly(
            anomaly_type="temp_disk",
            queryid=baseline.queryid,
            database_id=baseline.database_id,
            current_value=current_temp,
            baseline_value=baseline_temp,
            percent_change=percent_change,
            details={
                "threshold_percent": threshold_percent,
                "min_temp_blocks": min_temp_blocks,
            },
        )

    return None


def check_query_for_anomalies(
    session: Session,
    queryid: int,
    database_id: int,
    latency_threshold: float = 50.0,
    cache_drop_threshold: float = 10.0,
    temp_threshold: float = 100.0,
) -> list[Anomaly]:
    """Check a single query for all anomaly types.

    Args:
        session: Database session
        queryid: Query to check
        database_id: Database the query belongs to
        latency_threshold: Percent increase for latency alert
        cache_drop_threshold: Percentage point drop for cache alert
        temp_threshold: Percent increase for temp disk alert

    Returns:
        List of detected anomalies (may be empty)
    """
    baseline = calculate_baseline(session, queryid, database_id)
    if not baseline:
        return []

    current = get_recent_metrics(session, queryid, database_id)
    if not current:
        return []

    anomalies = []

    # Check each anomaly type
    latency = detect_latency_anomaly(baseline, current, latency_threshold)
    if latency:
        anomalies.append(latency)

    cache = detect_cache_anomaly(baseline, current, cache_drop_threshold)
    if cache:
        anomalies.append(cache)

    temp = detect_temp_disk_anomaly(baseline, current, temp_threshold)
    if temp:
        anomalies.append(temp)

    return anomalies
