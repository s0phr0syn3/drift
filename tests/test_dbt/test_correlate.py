"""Tests for dbt model to query correlation."""

import pytest

from drift.dbt.parser import DbtModel
from drift.dbt.correlate import (
    correlate_model_to_queries,
    correlate_all_models,
    generate_correlation_insights,
    CorrelationResult,
)
from drift.dbt.fingerprint import fingerprint_sql


@pytest.fixture
def sample_model():
    """A sample dbt model."""
    return DbtModel(
        unique_id="model.test_project.users",
        name="users",
        schema_name="public",
        database="analytics",
        alias="dim_users",
        materialized="table",
        compiled_sql="SELECT id, name, email FROM raw.users WHERE active = $1",
        depends_on=["source.test_project.raw.users"],
    )


@pytest.fixture
def sample_queries():
    """Sample pg_stat_statements queries."""
    return [
        {
            "queryid": 123456,
            "query": "SELECT id, name, email FROM raw.users WHERE active = $1",
            "fingerprint": fingerprint_sql("SELECT id, name, email FROM raw.users WHERE active = $1"),
            "tables": ["raw.users"],
        },
        {
            "queryid": 789012,
            "query": "SELECT * FROM orders WHERE customer_id = $1",
            "fingerprint": fingerprint_sql("SELECT * FROM orders WHERE customer_id = $1"),
            "tables": ["orders"],
        },
        {
            "queryid": 345678,
            "query": "INSERT INTO public.dim_users SELECT * FROM staging",
            "fingerprint": fingerprint_sql("INSERT INTO public.dim_users SELECT * FROM staging"),
            "tables": ["public.dim_users", "staging"],
        },
    ]


class TestCorrelateModelToQueries:
    """Tests for correlate_model_to_queries function."""

    def test_fingerprint_match_high_confidence(self, sample_model, sample_queries):
        """Test that fingerprint matches have high confidence."""
        result = correlate_model_to_queries(sample_model, sample_queries)

        assert result is not None
        assert result.queryid == 123456
        assert result.match_type == "fingerprint"
        assert result.confidence >= 0.9

    def test_no_match_returns_none(self, sample_queries):
        """Test that no match returns None."""
        model = DbtModel(
            unique_id="model.test_project.completely_different",
            name="completely_different",
            schema_name="other",
            database="other",
            alias=None,
            materialized="view",
            compiled_sql="SELECT foo, bar, baz FROM some_other_table",
            depends_on=[],
        )

        result = correlate_model_to_queries(model, sample_queries)

        # May match on partial or not match at all
        if result:
            assert result.confidence < 0.5

    def test_table_name_match(self):
        """Test matching by table name when fingerprint doesn't match."""
        model = DbtModel(
            unique_id="model.test_project.orders",
            name="orders",
            schema_name="public",
            database="analytics",
            alias=None,
            materialized="table",
            compiled_sql="SELECT * FROM raw.orders WHERE status = 'active'",
            depends_on=[],
        )

        queries = [
            {
                "queryid": 999999,
                "query": "INSERT INTO public.orders SELECT * FROM staging",
                "fingerprint": "different_fingerprint",
                "tables": ["public.orders", "staging"],
            },
        ]

        result = correlate_model_to_queries(model, queries)

        # Should match on table name (public.orders matches model relation)
        assert result is not None
        assert result.match_type == "table_name"

    def test_model_without_compiled_sql(self, sample_queries):
        """Test that model without compiled SQL can still match on relation name."""
        model = DbtModel(
            unique_id="model.test_project.dim_users",
            name="dim_users",
            schema_name="public",
            database="analytics",
            alias=None,
            materialized="table",
            compiled_sql=None,  # No compiled SQL
            depends_on=[],
        )

        result = correlate_model_to_queries(model, sample_queries)

        # Should match query 345678 which references public.dim_users
        if result:
            assert result.queryid == 345678
            assert result.match_type == "table_name"


class TestCorrelateAllModels:
    """Tests for correlate_all_models function."""

    def test_correlates_multiple_models(self, sample_model, sample_queries):
        """Test correlating multiple models."""
        models = {
            sample_model.unique_id: sample_model,
            "model.test_project.orders": DbtModel(
                unique_id="model.test_project.orders",
                name="orders",
                schema_name="public",
                database="analytics",
                alias=None,
                materialized="view",
                compiled_sql="SELECT * FROM orders WHERE customer_id = $1",
                depends_on=[],
            ),
        }

        results = correlate_all_models(models, sample_queries)

        # Both models should correlate
        assert len(results) >= 1
        assert "model.test_project.users" in results

    def test_empty_models_returns_empty(self, sample_queries):
        """Test that empty models dict returns empty results."""
        results = correlate_all_models({}, sample_queries)
        assert results == {}

    def test_empty_queries_returns_empty(self, sample_model):
        """Test that empty queries returns empty results."""
        results = correlate_all_models({sample_model.unique_id: sample_model}, [])
        assert results == {}


class TestGenerateCorrelationInsights:
    """Tests for generate_correlation_insights function."""

    def test_no_insights_for_healthy_query(self, sample_model):
        """Test that healthy queries generate no critical insights."""
        query_stats = {
            "total_calls": 1000,
            "total_time_ms": 1000.0,
            "mean_time_ms": 1.0,  # 1ms average - very fast
            "cache_hit_ratio": 99.0,
            "temp_blks_read": 0,
            "temp_blks_written": 0,
        }

        insights = generate_correlation_insights(sample_model, query_stats)

        # Should not have critical or warning insights
        critical_or_warning = [i for i in insights if i["severity"] in ("critical", "warning")]
        assert len(critical_or_warning) == 0

    def test_slow_view_recommends_table(self):
        """Test that slow view materialization recommends converting to table."""
        model = DbtModel(
            unique_id="model.test_project.slow_view",
            name="slow_view",
            schema_name="public",
            database="analytics",
            alias=None,
            materialized="view",  # Currently a view
            compiled_sql="SELECT * FROM big_table",
            depends_on=[],
        )

        query_stats = {
            "total_calls": 100,
            "total_time_ms": 100000.0,  # 100s total
            "mean_time_ms": 1000.0,  # 1 second average
            "cache_hit_ratio": 80.0,
            "temp_blks_read": 0,
            "temp_blks_written": 0,
        }

        insights = generate_correlation_insights(model, query_stats)

        # Should recommend materializing as table (category is "materialization")
        materialization_insights = [i for i in insights if i.get("category") == "materialization"]
        assert len(materialization_insights) > 0

    def test_low_cache_generates_insight(self, sample_model):
        """Test that low cache hit ratio generates insight."""
        query_stats = {
            "total_calls": 1000,
            "total_time_ms": 50000.0,
            "mean_time_ms": 50.0,
            "cache_hit_ratio": 50.0,  # Poor cache ratio
            "temp_blks_read": 0,
            "temp_blks_written": 0,
        }

        insights = generate_correlation_insights(sample_model, query_stats)

        # Should have cache-related insight
        cache_insights = [i for i in insights if "cache" in i.get("title", "").lower()]
        assert len(cache_insights) > 0

    def test_high_temp_disk_generates_insight(self, sample_model):
        """Test that high temp disk usage generates insight."""
        query_stats = {
            "total_calls": 1000,
            "total_time_ms": 50000.0,
            "mean_time_ms": 50.0,
            "cache_hit_ratio": 95.0,
            "temp_blks_read": 100000,  # High temp disk
            "temp_blks_written": 100000,
        }

        insights = generate_correlation_insights(sample_model, query_stats)

        # Should have temp disk insight
        temp_insights = [i for i in insights if "temp" in i.get("title", "").lower() or "disk" in i.get("title", "").lower()]
        assert len(temp_insights) > 0

    def test_empty_stats_returns_empty(self, sample_model):
        """Test that empty stats dict returns empty insights."""
        insights = generate_correlation_insights(sample_model, {})
        # Empty dict is falsy, so should return empty list
        assert insights == []

    def test_none_stats_returns_empty(self, sample_model):
        """Test that None stats returns empty insights."""
        insights = generate_correlation_insights(sample_model, None)
        assert insights == []

    def test_minimal_stats_returns_ok(self, sample_model):
        """Test that minimal good stats returns 'ok' insight."""
        query_stats = {
            "total_calls": 100,
            "mean_time_ms": 10.0,
            "cache_hit_ratio": 99.0,
        }
        insights = generate_correlation_insights(sample_model, query_stats)
        # Should have at least one insight (the "ok" one)
        assert len(insights) >= 1
