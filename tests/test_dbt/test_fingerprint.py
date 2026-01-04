"""Tests for SQL fingerprinting."""

from drift.dbt.fingerprint import normalize_sql, fingerprint_sql


class TestNormalizeSql:
    """Tests for SQL normalization."""

    def test_basic_select(self):
        sql = "SELECT * FROM users WHERE id = 1"
        normalized = normalize_sql(sql)
        assert "?" in normalized  # Literal replaced
        assert "SELECT" in normalized.upper()

    def test_different_literals_same_result(self):
        sql1 = "SELECT * FROM users WHERE id = 1"
        sql2 = "SELECT * FROM users WHERE id = 42"
        assert normalize_sql(sql1) == normalize_sql(sql2)

    def test_string_literal_replacement(self):
        sql = "SELECT * FROM users WHERE name = 'Alice'"
        normalized = normalize_sql(sql)
        assert "Alice" not in normalized
        assert "?" in normalized

    def test_whitespace_normalization(self):
        sql1 = "SELECT * FROM users"
        sql2 = "SELECT  *   FROM    users"
        sql3 = "SELECT\n*\nFROM\nusers"
        # All should normalize to same form
        assert normalize_sql(sql1) == normalize_sql(sql2)
        assert normalize_sql(sql1) == normalize_sql(sql3)

    def test_comment_removal(self):
        sql1 = "SELECT * FROM users"
        sql2 = "SELECT * FROM users -- get all users"
        sql3 = "SELECT * /* important */ FROM users"
        # Comments should be stripped
        assert normalize_sql(sql1) == normalize_sql(sql2)
        assert normalize_sql(sql1) == normalize_sql(sql3)

    def test_keyword_case_normalization(self):
        sql1 = "SELECT * FROM users"
        sql2 = "select * from users"
        sql3 = "Select * From Users"
        assert normalize_sql(sql1) == normalize_sql(sql2)
        # Note: table names might not be normalized (depends on sqlparse)

    def test_insert_statement(self):
        sql = "INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')"
        normalized = normalize_sql(sql)
        assert "Bob" not in normalized
        assert "bob@example.com" not in normalized
        assert "INSERT" in normalized.upper()

    def test_update_statement(self):
        sql = "UPDATE users SET name = 'Charlie' WHERE id = 5"
        normalized = normalize_sql(sql)
        assert "Charlie" not in normalized
        assert "5" not in normalized or "?" in normalized
        assert "UPDATE" in normalized.upper()


class TestFingerprintSql:
    """Tests for SQL fingerprinting."""

    def test_same_query_same_fingerprint(self):
        sql = "SELECT * FROM users WHERE id = 1"
        fp1 = fingerprint_sql(sql)
        fp2 = fingerprint_sql(sql)
        assert fp1 == fp2

    def test_different_literals_same_fingerprint(self):
        sql1 = "SELECT * FROM users WHERE id = 1"
        sql2 = "SELECT * FROM users WHERE id = 999"
        assert fingerprint_sql(sql1) == fingerprint_sql(sql2)

    def test_different_queries_different_fingerprint(self):
        sql1 = "SELECT * FROM users"
        sql2 = "SELECT * FROM orders"
        assert fingerprint_sql(sql1) != fingerprint_sql(sql2)

    def test_fingerprint_is_hex(self):
        sql = "SELECT 1"
        fp = fingerprint_sql(sql)
        # SHA256 hex is 64 characters, all hex digits
        assert len(fp) == 64
        assert all(c in "0123456789abcdef" for c in fp)
