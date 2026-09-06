"""Tests for the recommendations engine."""

import pytest

from drift.analysis.recommendations import (
    generate_recommendations,
    analyze_cache_hit_ratio,
    analyze_temp_disk_usage,
    analyze_execution_time,
    analyze_call_frequency,
    analyze_query_type,
    Severity,
    Category,
)


class TestAnalyzeCacheHitRatio:
    """Tests for cache hit ratio analysis."""

    def test_no_recommendation_when_none(self):
        result = analyze_cache_hit_ratio(None, 0, 0)
        assert result is None

    def test_no_recommendation_for_low_activity(self):
        # Less than 100 total blocks - not meaningful
        result = analyze_cache_hit_ratio(0.50, 25, 25)
        assert result is None

    def test_critical_for_very_low_ratio(self):
        # 70% cache hit with significant activity
        result = analyze_cache_hit_ratio(0.70, 70, 30)
        assert result is not None
        assert result.severity == Severity.CRITICAL
        assert result.category == Category.CACHE
        assert "70.0%" in result.metric_value

    def test_warning_for_suboptimal_ratio(self):
        # 90% cache hit - good but could be better
        result = analyze_cache_hit_ratio(0.90, 900, 100)
        assert result is not None
        assert result.severity == Severity.WARNING
        assert result.category == Category.CACHE

    def test_no_recommendation_for_good_ratio(self):
        # 99% cache hit - excellent
        result = analyze_cache_hit_ratio(0.99, 9900, 100)
        assert result is None


class TestAnalyzeTempDiskUsage:
    """Tests for temp disk usage analysis."""

    def test_no_recommendation_when_no_temp(self):
        result = analyze_temp_disk_usage(0, 0, "SELECT")
        assert result is None

    def test_warning_for_moderate_temp(self):
        # 2000 blocks = ~16MB
        result = analyze_temp_disk_usage(1000, 1000, "SELECT")
        assert result is not None
        assert result.severity == Severity.WARNING
        assert result.category == Category.TEMP_DISK

    def test_critical_for_high_temp(self):
        # 20000 blocks = ~160MB
        result = analyze_temp_disk_usage(10000, 10000, "SELECT")
        assert result is not None
        assert result.severity == Severity.CRITICAL
        assert result.category == Category.TEMP_DISK
        assert "MB" in result.metric_value


class TestAnalyzeExecutionTime:
    """Tests for execution time analysis."""

    def test_no_recommendation_when_none(self):
        result = analyze_execution_time(None, 0, 0, "SELECT")
        assert result is None

    def test_no_recommendation_for_utility(self):
        result = analyze_execution_time(5000, 5000, 1, "UTILITY")
        assert result is None

    def test_no_recommendation_for_transaction(self):
        result = analyze_execution_time(5000, 5000, 1, "TRANSACTION")
        assert result is None

    def test_critical_for_very_slow(self):
        # 2 second average
        result = analyze_execution_time(2000, 10000, 5, "SELECT")
        assert result is not None
        assert result.severity == Severity.CRITICAL
        assert result.category == Category.LATENCY
        assert "2000ms" in result.metric_value

    def test_warning_for_slow(self):
        # 200ms average
        result = analyze_execution_time(200, 2000, 10, "SELECT")
        assert result is not None
        assert result.severity == Severity.WARNING
        assert result.category == Category.LATENCY

    def test_no_recommendation_for_fast(self):
        # 10ms average
        result = analyze_execution_time(10, 1000, 100, "SELECT")
        assert result is None


class TestAnalyzeCallFrequency:
    """Tests for call frequency analysis."""

    def test_no_recommendation_for_low_frequency(self):
        result = analyze_call_frequency(100, 50, 5000, "SELECT")
        assert result is None

    def test_warning_for_high_frequency_slow(self):
        # 15k calls at 50ms each
        result = analyze_call_frequency(15000, 50, 750000, "SELECT")
        assert result is not None
        assert result.severity == Severity.WARNING
        assert result.category == Category.FREQUENCY
        assert "15,000" in result.metric_value

    def test_ok_for_high_frequency_fast(self):
        # 15k calls at 0.5ms each - well optimized
        result = analyze_call_frequency(15000, 0.5, 7500, "SELECT")
        assert result is not None
        assert result.severity == Severity.OK
        assert "Well-Optimized" in result.title


class TestAnalyzeQueryType:
    """Tests for query type analysis."""

    def test_ddl_info(self):
        result = analyze_query_type("DDL", "CREATE TABLE foo")
        assert result is not None
        assert result.severity == Severity.INFO
        assert "DDL" in result.title

    def test_transaction_info(self):
        result = analyze_query_type("TRANSACTION", "BEGIN")
        assert result is not None
        assert result.severity == Severity.INFO
        assert "Transaction" in result.title

    def test_no_info_for_select(self):
        result = analyze_query_type("SELECT", "SELECT * FROM users")
        assert result is None


class TestGenerateRecommendations:
    """Tests for the main recommendation generator."""

    def test_generates_ok_when_all_good(self):
        recs = generate_recommendations(
            query="SELECT id FROM users WHERE id = 1",
            query_type="SELECT",
            calls=100,
            total_time_ms=50,
            mean_time_ms=0.5,
            rows=100,
            shared_blks_hit=1000,
            shared_blks_read=0,
            temp_blks_read=0,
            temp_blks_written=0,
            cache_hit_ratio=1.0,
        )
        assert len(recs) == 1
        assert recs[0]["severity"] == "ok"
        assert recs[0]["title"] == "No Issues Detected"

    def test_multiple_issues_sorted_by_severity(self):
        # Low cache hit + high temp usage + slow
        recs = generate_recommendations(
            query="SELECT * FROM large_table",
            query_type="SELECT",
            calls=100,
            total_time_ms=200000,
            mean_time_ms=2000,
            rows=100000,
            shared_blks_hit=5000,
            shared_blks_read=5000,
            temp_blks_read=15000,
            temp_blks_written=15000,
            cache_hit_ratio=0.50,
        )
        # Should have at least 3 recommendations (cache, temp, latency)
        assert len(recs) >= 3
        # First should be critical
        assert recs[0]["severity"] == "critical"

    def test_returns_dict_format(self):
        recs = generate_recommendations(
            query="SELECT 1",
            query_type="SELECT",
            calls=1,
            total_time_ms=1,
            mean_time_ms=1,
            rows=1,
            shared_blks_hit=0,
            shared_blks_read=0,
            temp_blks_read=0,
            temp_blks_written=0,
            cache_hit_ratio=None,
        )
        assert isinstance(recs, list)
        for rec in recs:
            assert isinstance(rec, dict)
            assert "severity" in rec
            assert "category" in rec
            assert "title" in rec
            assert "description" in rec
            assert "action" in rec
            assert "metric_value" in rec
