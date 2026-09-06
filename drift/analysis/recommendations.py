"""Query performance recommendations engine.

Analyzes query metrics and generates actionable recommendations
for database optimization.
"""

from dataclasses import dataclass
from enum import Enum


class Severity(str, Enum):
    """Recommendation severity levels."""

    CRITICAL = "critical"  # Immediate action needed
    WARNING = "warning"    # Should be addressed soon
    INFO = "info"          # Optimization opportunity
    OK = "ok"              # No action needed


class Category(str, Enum):
    """Recommendation categories."""

    CACHE = "cache"           # Cache/memory related
    TEMP_DISK = "temp_disk"   # Temp file usage
    LATENCY = "latency"       # Execution time
    FREQUENCY = "frequency"   # Call patterns
    INDEXING = "indexing"     # Index suggestions
    CONFIG = "config"         # PostgreSQL config


@dataclass
class Recommendation:
    """A single recommendation for a query."""

    severity: Severity
    category: Category
    title: str
    description: str
    action: str  # Specific actionable step
    metric_value: str  # The value that triggered this

    def to_dict(self) -> dict:
        return {
            "severity": self.severity.value,
            "category": self.category.value,
            "title": self.title,
            "description": self.description,
            "action": self.action,
            "metric_value": self.metric_value,
        }


# Thresholds for recommendations
CACHE_HIT_CRITICAL = 0.80  # Below 80% is critical
CACHE_HIT_WARNING = 0.95   # Below 95% is warning
TEMP_BLKS_WARNING = 1000   # More than 1000 temp blocks is warning
TEMP_BLKS_CRITICAL = 10000 # More than 10000 temp blocks is critical
MEAN_TIME_WARNING_MS = 100  # Slower than 100ms is warning
MEAN_TIME_CRITICAL_MS = 1000  # Slower than 1s is critical
HIGH_FREQUENCY_THRESHOLD = 10000  # More than 10k calls in period


def analyze_cache_hit_ratio(
    cache_hit_ratio: float | None,
    shared_blks_hit: int,
    shared_blks_read: int,
) -> Recommendation | None:
    """Analyze cache hit ratio and recommend improvements."""
    if cache_hit_ratio is None:
        return None

    # If there's very little block activity, cache ratio is less meaningful
    total_blks = shared_blks_hit + shared_blks_read
    if total_blks < 100:
        return None

    if cache_hit_ratio < CACHE_HIT_CRITICAL:
        return Recommendation(
            severity=Severity.CRITICAL,
            category=Category.CACHE,
            title="Very Low Cache Hit Ratio",
            description=(
                f"Only {cache_hit_ratio:.1%} of data blocks are being served from cache. "
                "This query is causing excessive disk I/O."
            ),
            action=(
                "1. Check if the tables accessed have appropriate indexes\n"
                "2. Consider increasing shared_buffers if you have available RAM\n"
                "3. Review if this query can be optimized to access fewer rows\n"
                "4. Run EXPLAIN (ANALYZE, BUFFERS) to see which operations cause disk reads"
            ),
            metric_value=f"{cache_hit_ratio:.1%}",
        )

    if cache_hit_ratio < CACHE_HIT_WARNING:
        return Recommendation(
            severity=Severity.WARNING,
            category=Category.CACHE,
            title="Suboptimal Cache Hit Ratio",
            description=(
                f"Cache hit ratio is {cache_hit_ratio:.1%}. While not critical, "
                "there's room for improvement."
            ),
            action=(
                "1. Review table indexes to ensure efficient data access patterns\n"
                "2. Consider if shared_buffers is appropriately sized for your workload\n"
                "3. Check if this query could benefit from more selective WHERE clauses"
            ),
            metric_value=f"{cache_hit_ratio:.1%}",
        )

    return None


def analyze_temp_disk_usage(
    temp_blks_read: int,
    temp_blks_written: int,
    query_type: str | None,
) -> Recommendation | None:
    """Analyze temporary disk usage and recommend improvements."""
    total_temp = temp_blks_read + temp_blks_written

    if total_temp == 0:
        return None

    # Estimate temp disk usage (8KB per block)
    temp_mb = (total_temp * 8) / 1024

    if total_temp >= TEMP_BLKS_CRITICAL:
        return Recommendation(
            severity=Severity.CRITICAL,
            category=Category.TEMP_DISK,
            title="Excessive Temp Disk Usage",
            description=(
                f"This query is using ~{temp_mb:.1f}MB of temporary disk space. "
                "Spilling to disk significantly degrades performance."
            ),
            action=(
                "1. Increase work_mem for this session: SET work_mem = '256MB'\n"
                "2. Review the query for large sorts or hash joins that could be optimized\n"
                "3. Add indexes to avoid large sorts (use ORDER BY columns in index)\n"
                "4. For hash joins, ensure join columns have statistics updated (ANALYZE)"
            ),
            metric_value=f"{temp_mb:.1f}MB temp disk",
        )

    if total_temp >= TEMP_BLKS_WARNING:
        return Recommendation(
            severity=Severity.WARNING,
            category=Category.TEMP_DISK,
            title="Moderate Temp Disk Usage",
            description=(
                f"This query is using ~{temp_mb:.1f}MB of temporary disk space. "
                "Consider tuning to avoid disk spills."
            ),
            action=(
                "1. Try increasing work_mem: SET work_mem = '128MB'\n"
                "2. Check if indexes could eliminate sorting operations\n"
                "3. Run EXPLAIN (ANALYZE, BUFFERS) to identify which operation spills"
            ),
            metric_value=f"{temp_mb:.1f}MB temp disk",
        )

    return None


def analyze_execution_time(
    mean_time_ms: float | None,
    total_time_ms: float,
    calls: int,
    query_type: str | None,
) -> Recommendation | None:
    """Analyze execution time and recommend improvements."""
    if mean_time_ms is None or calls == 0:
        return None

    # Skip analysis for utility commands
    if query_type in ("UTILITY", "TRANSACTION", "DCL"):
        return None

    if mean_time_ms >= MEAN_TIME_CRITICAL_MS:
        return Recommendation(
            severity=Severity.CRITICAL,
            category=Category.LATENCY,
            title="Very Slow Query",
            description=(
                f"Average execution time is {mean_time_ms:.0f}ms. "
                f"Total time spent: {total_time_ms/1000:.1f}s across {calls:,} calls."
            ),
            action=(
                "1. Run EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) on this query\n"
                "2. Look for sequential scans on large tables - add indexes\n"
                "3. Check for missing indexes on JOIN and WHERE columns\n"
                "4. Review if the query can be rewritten to be more efficient\n"
                "5. Consider query result caching if data doesn't change frequently"
            ),
            metric_value=f"{mean_time_ms:.0f}ms avg",
        )

    if mean_time_ms >= MEAN_TIME_WARNING_MS:
        return Recommendation(
            severity=Severity.WARNING,
            category=Category.LATENCY,
            title="Slow Query",
            description=(
                f"Average execution time is {mean_time_ms:.0f}ms. "
                "This may impact application responsiveness."
            ),
            action=(
                "1. Run EXPLAIN ANALYZE to understand the execution plan\n"
                "2. Check for missing indexes on filtered columns\n"
                "3. Ensure table statistics are up to date (run ANALYZE)"
            ),
            metric_value=f"{mean_time_ms:.0f}ms avg",
        )

    return None


def analyze_call_frequency(
    calls: int,
    mean_time_ms: float | None,
    total_time_ms: float,
    query_type: str | None,
) -> Recommendation | None:
    """Analyze call frequency patterns."""
    if calls < HIGH_FREQUENCY_THRESHOLD:
        return None

    # High frequency queries deserve attention
    if mean_time_ms and mean_time_ms > 10:  # More than 10ms per call
        return Recommendation(
            severity=Severity.WARNING,
            category=Category.FREQUENCY,
            title="High-Frequency Query",
            description=(
                f"This query was called {calls:,} times. "
                f"Even small optimizations will have significant impact."
            ),
            action=(
                "1. Consider caching results at the application layer\n"
                "2. Review if multiple calls can be batched into one\n"
                "3. Check if a materialized view could serve this data\n"
                "4. Ensure optimal indexing - every millisecond counts at this volume"
            ),
            metric_value=f"{calls:,} calls",
        )
    elif mean_time_ms and mean_time_ms <= 1:
        # Fast and frequent - this is actually good
        return Recommendation(
            severity=Severity.OK,
            category=Category.FREQUENCY,
            title="Well-Optimized High-Frequency Query",
            description=(
                f"This query runs {calls:,} times with {mean_time_ms:.2f}ms average. "
                "Performance is excellent for the volume."
            ),
            action="No action needed. Continue monitoring for regressions.",
            metric_value=f"{calls:,} calls @ {mean_time_ms:.2f}ms",
        )

    return None


def analyze_query_type(query_type: str | None, query: str) -> Recommendation | None:
    """Provide context-specific recommendations based on query type."""
    if query_type == "DDL":
        return Recommendation(
            severity=Severity.INFO,
            category=Category.CONFIG,
            title="DDL Statement Detected",
            description="Schema modification queries are tracked but typically don't need optimization.",
            action=(
                "DDL statements run infrequently. Focus optimization efforts on DML queries. "
                "However, ensure DDL runs during low-traffic periods to minimize locking."
            ),
            metric_value="DDL",
        )

    if query_type == "TRANSACTION":
        return Recommendation(
            severity=Severity.INFO,
            category=Category.CONFIG,
            title="Transaction Control Statement",
            description="Transaction statements (BEGIN/COMMIT/ROLLBACK) are normal overhead.",
            action=(
                "No optimization needed for transaction control. "
                "If you see many ROLLBACKs, investigate application error handling."
            ),
            metric_value="Transaction",
        )

    return None


def generate_recommendations(
    query: str,
    query_type: str | None,
    calls: int,
    total_time_ms: float,
    mean_time_ms: float | None,
    rows: int,
    shared_blks_hit: int,
    shared_blks_read: int,
    temp_blks_read: int,
    temp_blks_written: int,
    cache_hit_ratio: float | None,
) -> list[dict]:
    """Generate all applicable recommendations for a query.

    Returns recommendations sorted by severity (critical first).
    """
    recommendations = []

    # Run all analyzers
    analyzers = [
        lambda: analyze_cache_hit_ratio(cache_hit_ratio, shared_blks_hit, shared_blks_read),
        lambda: analyze_temp_disk_usage(temp_blks_read, temp_blks_written, query_type),
        lambda: analyze_execution_time(mean_time_ms, total_time_ms, calls, query_type),
        lambda: analyze_call_frequency(calls, mean_time_ms, total_time_ms, query_type),
        lambda: analyze_query_type(query_type, query),
    ]

    for analyzer in analyzers:
        result = analyzer()
        if result:
            recommendations.append(result)

    # If no recommendations, add an "all clear" message
    if not recommendations:
        recommendations.append(Recommendation(
            severity=Severity.OK,
            category=Category.CONFIG,
            title="No Issues Detected",
            description="This query's metrics are within normal ranges.",
            action="Continue monitoring. Set up alerts to catch regressions.",
            metric_value="All metrics OK",
        ))

    # Sort by severity (critical first)
    severity_order = {
        Severity.CRITICAL: 0,
        Severity.WARNING: 1,
        Severity.INFO: 2,
        Severity.OK: 3,
    }
    recommendations.sort(key=lambda r: severity_order[r.severity])

    return [r.to_dict() for r in recommendations]
