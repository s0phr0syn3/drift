"""Tests for snapshot collection."""

import pytest

from drift.collector.snapshot import (
    take_snapshot,
    extract_query_type,
    extract_tables,
)


class TestExtractQueryType:
    """Tests for query type extraction."""

    def test_select(self):
        assert extract_query_type("SELECT * FROM users") == "SELECT"
        assert extract_query_type("select id from orders") == "SELECT"

    def test_insert(self):
        assert extract_query_type("INSERT INTO users VALUES (1)") == "INSERT"

    def test_update(self):
        assert extract_query_type("UPDATE users SET name = 'x'") == "UPDATE"

    def test_delete(self):
        assert extract_query_type("DELETE FROM users WHERE id = 1") == "DELETE"

    def test_ddl(self):
        assert extract_query_type("CREATE TABLE foo (id int)") == "DDL"
        assert extract_query_type("DROP TABLE foo") == "DDL"
        assert extract_query_type("ALTER TABLE foo ADD COLUMN bar int") == "DDL"
        assert extract_query_type("TRUNCATE TABLE foo") == "DDL"

    def test_transaction(self):
        assert extract_query_type("BEGIN") == "TRANSACTION"
        assert extract_query_type("COMMIT") == "TRANSACTION"
        assert extract_query_type("ROLLBACK") == "TRANSACTION"

    def test_utility(self):
        assert extract_query_type("SET statement_timeout = '10s'") == "UTILITY"
        assert extract_query_type("VACUUM users") == "UTILITY"

    def test_whitespace(self):
        assert extract_query_type("  SELECT * FROM users") == "SELECT"
        assert extract_query_type("\nSELECT * FROM users") == "SELECT"


class TestExtractTables:
    """Tests for table name extraction."""

    def test_simple_from(self):
        tables = extract_tables("SELECT * FROM users")
        assert "users" in tables

    def test_join(self):
        tables = extract_tables("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
        assert "users" in tables
        assert "orders" in tables

    def test_insert(self):
        tables = extract_tables("INSERT INTO users (name) VALUES ('x')")
        assert "users" in tables

    def test_update(self):
        tables = extract_tables("UPDATE users SET name = 'x'")
        assert "users" in tables

    def test_quoted_table(self):
        tables = extract_tables('SELECT * FROM "Users"')
        assert "users" in tables  # Should be lowercased

    def test_schema_qualified(self):
        tables = extract_tables("SELECT * FROM public.users")
        assert "users" in tables


class TestTakeSnapshot:
    """Integration tests for snapshot collection.

    These tests require the Docker target-db to be running.
    """

    def test_take_snapshot(self, docker_services, target_db_dsn):
        """Test taking a snapshot from the target database."""
        snapshot = take_snapshot(target_db_dsn)

        # Should have some rows (init.sql runs queries)
        assert len(snapshot.rows) > 0

        # Should have a timestamp
        assert snapshot.timestamp is not None

        # Each row should have required fields
        for row in snapshot.rows:
            assert row.queryid is not None
            assert row.query is not None
            assert row.calls >= 0

    def test_snapshot_excludes_own_queries(self, docker_services, target_db_dsn):
        """Snapshot should not include queries about pg_stat_statements itself."""
        snapshot = take_snapshot(target_db_dsn)

        for row in snapshot.rows:
            assert "pg_stat_statements" not in row.query.lower()
