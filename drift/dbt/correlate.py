"""Correlate dbt models with pg_stat_statements queries."""

from dataclasses import dataclass

from drift.dbt.fingerprint import fingerprint_sql, normalize_sql
from drift.dbt.parser import DbtModel


@dataclass
class CorrelationResult:
    """Result of correlating a dbt model with a query."""

    model_unique_id: str
    queryid: int
    match_type: str  # "fingerprint", "table_name", "partial"
    confidence: float  # 0.0 to 1.0
    model_fingerprint: str
    query_fingerprint: str


def correlate_model_to_queries(
    model: DbtModel,
    queries: list[dict],  # List of {queryid, query, fingerprint, tables}
) -> CorrelationResult | None:
    """Try to correlate a dbt model to a pg_stat_statements query.

    Correlation strategies (in order of confidence):
    1. Exact fingerprint match - same normalized SQL
    2. Table name match - model's relation appears in query tables
    3. Partial match - significant overlap in normalized tokens

    Args:
        model: Parsed dbt model with compiled SQL
        queries: List of query dicts from query_text table

    Returns:
        CorrelationResult if a match is found, None otherwise
    """
    if not model.compiled_sql:
        return None

    model_fingerprint = fingerprint_sql(model.compiled_sql)
    model_normalized = normalize_sql(model.compiled_sql)

    best_match = None
    best_confidence = 0.0

    for query in queries:
        query_fingerprint = query.get("fingerprint", "")
        queryid = query.get("queryid")

        # Strategy 1: Exact fingerprint match
        if model_fingerprint == query_fingerprint:
            return CorrelationResult(
                model_unique_id=model.unique_id,
                queryid=queryid,
                match_type="fingerprint",
                confidence=1.0,
                model_fingerprint=model_fingerprint,
                query_fingerprint=query_fingerprint,
            )

        # Strategy 2: Table name match
        query_tables = set(t.lower() for t in (query.get("tables") or []))
        model_relation = model.relation_name
        if model_relation:
            model_relation_lower = model_relation.lower()
            model_table = model_relation.split(".")[-1].lower()
            # Match either full relation (schema.table) or just table name
            if model_relation_lower in query_tables or model_table in query_tables:
                confidence = 0.7  # High confidence for table match
                if confidence > best_confidence:
                    best_confidence = confidence
                    best_match = CorrelationResult(
                        model_unique_id=model.unique_id,
                        queryid=queryid,
                        match_type="table_name",
                        confidence=confidence,
                        model_fingerprint=model_fingerprint,
                        query_fingerprint=query_fingerprint,
                    )

        # Strategy 3: Partial token match (for transformed queries)
        query_text = query.get("query", "")
        if query_text:
            query_normalized = normalize_sql(query_text)
            similarity = _calculate_token_similarity(model_normalized, query_normalized)
            if similarity > 0.8 and similarity > best_confidence:  # 80% threshold
                best_confidence = similarity
                best_match = CorrelationResult(
                    model_unique_id=model.unique_id,
                    queryid=queryid,
                    match_type="partial",
                    confidence=similarity,
                    model_fingerprint=model_fingerprint,
                    query_fingerprint=query_fingerprint,
                )

    return best_match


def _calculate_token_similarity(sql1: str, sql2: str) -> float:
    """Calculate similarity between two normalized SQL strings.

    Uses Jaccard similarity on tokens.
    """
    tokens1 = set(sql1.lower().split())
    tokens2 = set(sql2.lower().split())

    if not tokens1 or not tokens2:
        return 0.0

    intersection = tokens1 & tokens2
    union = tokens1 | tokens2

    return len(intersection) / len(union)


def correlate_all_models(
    models: dict[str, DbtModel],
    queries: list[dict],
) -> dict[str, CorrelationResult]:
    """Correlate all dbt models to queries.

    Args:
        models: Dict of unique_id -> DbtModel
        queries: List of query dicts from query_text table

    Returns:
        Dict of model unique_id -> CorrelationResult for matched models
    """
    correlations = {}

    for unique_id, model in models.items():
        result = correlate_model_to_queries(model, queries)
        if result:
            correlations[unique_id] = result

    return correlations


def generate_correlation_insights(
    model: DbtModel,
    query_stats: dict | None,  # Query performance stats
) -> list[dict]:
    """Generate insights for a correlated model based on query performance.

    Args:
        model: The dbt model
        query_stats: Performance stats from pg_stat_statements

    Returns:
        List of insight dicts with severity, message, and recommendation
    """
    if not query_stats:
        return []

    insights = []

    # Check cache hit ratio
    cache_hit = query_stats.get("cache_hit_ratio")
    if cache_hit is not None and cache_hit < 90:
        severity = "critical" if cache_hit < 70 else "warning"
        insights.append({
            "severity": severity,
            "category": "cache",
            "title": f"Low cache hit ratio ({cache_hit:.1f}%)",
            "description": f"Model {model.name} has a {cache_hit:.1f}% cache hit ratio, causing excessive disk I/O.",
            "recommendation": (
                f"Consider adding indexes to tables accessed by {model.name}. "
                f"If this is a large table scan, consider whether all columns are needed "
                f"or if the query can be filtered earlier."
            ),
        })

    # Check temp disk usage
    temp_blks = query_stats.get("temp_blks_read", 0) + query_stats.get("temp_blks_written", 0)
    if temp_blks > 1000:
        temp_mb = (temp_blks * 8) / 1024
        severity = "critical" if temp_blks > 10000 else "warning"
        insights.append({
            "severity": severity,
            "category": "temp_disk",
            "title": f"Spilling to disk ({temp_mb:.1f}MB)",
            "description": f"Model {model.name} is using {temp_mb:.1f}MB of temp disk space.",
            "recommendation": (
                f"For model {model.name}:\n"
                f"1. Increase work_mem in your dbt profile: work_mem: '256MB'\n"
                f"2. Review ORDER BY and GROUP BY - add supporting indexes\n"
                f"3. For incremental models, ensure the merge key is indexed"
            ),
        })

    # Check mean execution time
    mean_time = query_stats.get("mean_time_ms")
    if mean_time and mean_time > 1000:
        severity = "critical" if mean_time > 5000 else "warning"
        insights.append({
            "severity": severity,
            "category": "latency",
            "title": f"Slow execution ({mean_time:.0f}ms avg)",
            "description": f"Model {model.name} averages {mean_time:.0f}ms per execution.",
            "recommendation": (
                f"For model {model.name}:\n"
                f"1. Run EXPLAIN ANALYZE on the compiled SQL\n"
                f"2. Check for sequential scans on large tables\n"
                f"3. Consider changing materialization: "
                f"{'incremental might help' if model.materialized == 'table' else 'table might help' if model.materialized == 'view' else 'review strategy'}"
            ),
        })

    # Materialization-specific insights
    if model.materialized == "view" and mean_time and mean_time > 500:
        insights.append({
            "severity": "info",
            "category": "materialization",
            "title": "Consider materializing as table",
            "description": f"Model {model.name} is a view taking {mean_time:.0f}ms. Views re-execute on each query.",
            "recommendation": (
                f"Change {model.name} to materialized='table' or 'incremental' if:\n"
                f"- It's queried frequently by downstream models or BI tools\n"
                f"- The source data doesn't change often\n"
                f"- Query time is impacting user experience"
            ),
        })

    if model.materialized == "table" and query_stats.get("total_calls", 0) < 10:
        insights.append({
            "severity": "info",
            "category": "materialization",
            "title": "Consider view materialization",
            "description": f"Model {model.name} is a table but rarely queried ({query_stats.get('total_calls', 0)} calls).",
            "recommendation": (
                f"If {model.name} is only used during dbt runs and not queried directly, "
                f"consider changing to materialized='view' to save storage and reduce refresh time."
            ),
        })

    # If no issues found
    if not insights:
        insights.append({
            "severity": "ok",
            "category": "general",
            "title": "Model performing well",
            "description": f"Model {model.name} has healthy performance metrics.",
            "recommendation": "Continue monitoring for regressions.",
        })

    return insights
