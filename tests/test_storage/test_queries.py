"""Tests for storage query functions."""

import pytest
from datetime import datetime, timezone

from drift.storage.queries import (
    add_monitored_database,
    get_monitored_database_by_name,
    list_monitored_databases,
    remove_monitored_database,
    store_query_stats,
    upsert_query_text,
    get_top_queries,
    get_query_details,
    get_query_trend,
)


class TestMonitoredDatabases:
    """Tests for monitored database CRUD operations."""

    def test_add_database(self, db_session):
        db = add_monitored_database(db_session, "test_db", "postgresql://localhost/test")

        assert db.id is not None
        assert db.name == "test_db"
        assert db.enabled is True

    def test_get_by_name(self, db_session):
        add_monitored_database(db_session, "my_db", "postgresql://localhost/my")

        result = get_monitored_database_by_name(db_session, "my_db")
        assert result is not None
        assert result.name == "my_db"

    def test_get_by_name_not_found(self, db_session):
        result = get_monitored_database_by_name(db_session, "nonexistent")
        assert result is None

    def test_list_databases(self, db_session):
        add_monitored_database(db_session, "db1", "postgresql://localhost/db1")
        add_monitored_database(db_session, "db2", "postgresql://localhost/db2")

        databases = list_monitored_databases(db_session)
        names = [db.name for db in databases]

        assert "db1" in names
        assert "db2" in names

    def test_remove_database(self, db_session):
        add_monitored_database(db_session, "to_remove", "postgresql://localhost/x")

        result = remove_monitored_database(db_session, "to_remove")
        assert result is True

        assert get_monitored_database_by_name(db_session, "to_remove") is None

    def test_remove_nonexistent(self, db_session):
        result = remove_monitored_database(db_session, "doesnt_exist")
        assert result is False


class TestQueryStats:
    """Tests for query stats storage."""

    def test_store_query_stats(self, db_session):
        # First add a database
        db = add_monitored_database(db_session, "stats_test", "postgresql://localhost/x")

        stats = [
            {
                "queryid": 12345,
                "calls_delta": 100,
                "total_exec_time_delta": 500.5,
                "rows_delta": 1000,
                "shared_blks_hit_delta": 50,
                "shared_blks_read_delta": 10,
                "temp_blks_read_delta": 0,
                "temp_blks_written_delta": 0,
            }
        ]

        count = store_query_stats(db_session, db.id, datetime.now(timezone.utc), stats)
        assert count == 1

    def test_upsert_query_text_insert(self, db_session):
        db = add_monitored_database(db_session, "text_test", "postgresql://localhost/x")

        upsert_query_text(
            db_session,
            db.id,
            queryid=99999,
            query="SELECT * FROM users",
            query_type="SELECT",
            tables=["users"],
            fingerprint="abc123",
        )

        # Should be able to insert without error
        db_session.flush()

    def test_upsert_query_text_update(self, db_session):
        db = add_monitored_database(db_session, "text_update", "postgresql://localhost/x")

        # Insert first time
        upsert_query_text(
            db_session,
            db.id,
            queryid=88888,
            query="SELECT * FROM orders",
            query_type="SELECT",
            tables=["orders"],
            fingerprint="def456",
        )
        db_session.flush()

        # Upsert again - should update last_seen
        upsert_query_text(
            db_session,
            db.id,
            queryid=88888,
            query="SELECT * FROM orders",
            query_type="SELECT",
            tables=["orders"],
            fingerprint="def456",
        )
        db_session.flush()
        # No error means success


class TestTopQueries:
    """Tests for top queries retrieval."""

    def test_get_top_queries_empty(self, db_session):
        # Create a fresh database with no stats
        db = add_monitored_database(db_session, "empty_db", "postgresql://localhost/empty")
        db_session.flush()

        queries = get_top_queries(db_session, database_id=db.id)
        assert queries == []

    def test_get_top_queries_with_data(self, db_session):
        db = add_monitored_database(db_session, "top_test", "postgresql://localhost/x")

        # Add some stats
        stats = [
            {
                "queryid": 1,
                "calls_delta": 100,
                "total_exec_time_delta": 1000.0,
                "rows_delta": 500,
                "shared_blks_hit_delta": 50,
                "shared_blks_read_delta": 10,
                "temp_blks_read_delta": 0,
                "temp_blks_written_delta": 0,
            },
            {
                "queryid": 2,
                "calls_delta": 50,
                "total_exec_time_delta": 2000.0,  # Higher total time
                "rows_delta": 100,
                "shared_blks_hit_delta": 20,
                "shared_blks_read_delta": 5,
                "temp_blks_read_delta": 0,
                "temp_blks_written_delta": 0,
            },
        ]
        store_query_stats(db_session, db.id, datetime.now(timezone.utc), stats)

        # Add query text
        upsert_query_text(db_session, db.id, 1, "SELECT * FROM a", "SELECT", ["a"], "fp1")
        upsert_query_text(db_session, db.id, 2, "SELECT * FROM b", "SELECT", ["b"], "fp2")

        db_session.flush()

        queries = get_top_queries(db_session, database_id=db.id, sort_by="total_time")

        assert len(queries) == 2
        # Query 2 should be first (higher total time)
        assert queries[0]["queryid"] == 2
        assert queries[0]["total_time_ms"] == 2000.0


class TestQueryDetails:
    """Tests for query details retrieval."""

    def test_get_query_details(self, db_session):
        db = add_monitored_database(db_session, "details_test", "postgresql://localhost/x")

        # Add stats
        stats = [{
            "queryid": 12345,
            "calls_delta": 100,
            "total_exec_time_delta": 500.0,
            "rows_delta": 1000,
            "shared_blks_hit_delta": 90,
            "shared_blks_read_delta": 10,
            "temp_blks_read_delta": 5,
            "temp_blks_written_delta": 3,
        }]
        store_query_stats(db_session, db.id, datetime.now(timezone.utc), stats)

        # Add query text
        upsert_query_text(
            db_session, db.id, 12345,
            "SELECT * FROM users WHERE id = $1",
            "SELECT", ["users"], "abc123fingerprint"
        )
        db_session.flush()

        details = get_query_details(db_session, 12345, db.id)

        assert details is not None
        assert details["queryid"] == 12345
        assert details["query_type"] == "SELECT"
        assert details["total_calls"] == 100
        assert details["total_time_ms"] == 500.0
        assert details["mean_time_ms"] == 5.0
        assert details["cache_hit_ratio"] == 90.0  # 90 hits / 100 total
        assert details["temp_blks_read"] == 5

    def test_get_query_details_not_found(self, db_session):
        details = get_query_details(db_session, 99999999)
        assert details is None


class TestQueryTrend:
    """Tests for query trend retrieval."""

    def test_get_query_trend(self, db_session):
        db = add_monitored_database(db_session, "trend_test", "postgresql://localhost/x")

        # Add stats at different times
        base_time = datetime.now(timezone.utc)
        for i in range(3):
            stats = [{
                "queryid": 54321,
                "calls_delta": 10 + i * 5,
                "total_exec_time_delta": 100.0 + i * 50,
                "rows_delta": 50,
                "shared_blks_hit_delta": 10,
                "shared_blks_read_delta": 1,
                "temp_blks_read_delta": 0,
                "temp_blks_written_delta": 0,
            }]
            from datetime import timedelta
            snapshot_time = base_time - timedelta(hours=i)
            store_query_stats(db_session, db.id, snapshot_time, stats)

        upsert_query_text(
            db_session, db.id, 54321,
            "SELECT count(*) FROM orders",
            "SELECT", ["orders"], "def456fingerprint"
        )
        db_session.flush()

        trend = get_query_trend(db_session, 54321, db.id, period_hours=24)

        assert len(trend) > 0
        # Each entry should have the expected keys
        for entry in trend:
            assert "time" in entry
            assert "calls" in entry
            assert "mean_time_ms" in entry

    def test_get_query_trend_empty(self, db_session):
        db = add_monitored_database(db_session, "empty_trend", "postgresql://localhost/x")
        db_session.flush()

        trend = get_query_trend(db_session, 99999, db.id)
        assert trend == []
