"""Snapshot collection from pg_stat_statements."""

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg


@dataclass
class PgStatStatementsRow:
    """A single row from pg_stat_statements."""

    queryid: int
    query: str
    calls: int
    total_exec_time: float  # milliseconds
    rows: int
    shared_blks_hit: int
    shared_blks_read: int
    temp_blks_read: int
    temp_blks_written: int


@dataclass
class Snapshot:
    """A complete snapshot of pg_stat_statements at a point in time."""

    timestamp: datetime
    rows: list[PgStatStatementsRow] = field(default_factory=list)


# Regex patterns for extracting info from queries
TABLE_PATTERN = re.compile(
    r'\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+(["\w]+\.)?(["\w]+)',
    re.IGNORECASE,
)


def extract_query_type(query: str) -> str:
    """Extract the query type from a query.

    Returns one of:
    - DML: SELECT, INSERT, UPDATE, DELETE
    - DDL: CREATE, ALTER, DROP, TRUNCATE
    - TRANSACTION: BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE
    - UTILITY: SET, SHOW, EXPLAIN, VACUUM, ANALYZE, REINDEX, CLUSTER
    - DCL: GRANT, REVOKE
    - OTHER: anything else
    """
    query_stripped = query.strip().upper()

    # DML
    for query_type in ("SELECT", "INSERT", "UPDATE", "DELETE"):
        if query_stripped.startswith(query_type):
            return query_type

    # DDL
    for keyword in ("CREATE", "ALTER", "DROP", "TRUNCATE"):
        if query_stripped.startswith(keyword):
            return "DDL"

    # Transaction control
    for keyword in ("BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "START TRANSACTION"):
        if query_stripped.startswith(keyword):
            return "TRANSACTION"

    # Utility commands
    for keyword in ("SET", "SHOW", "EXPLAIN", "VACUUM", "ANALYZE", "REINDEX", "CLUSTER", "COPY"):
        if query_stripped.startswith(keyword):
            return "UTILITY"

    # DCL (Data Control Language)
    for keyword in ("GRANT", "REVOKE"):
        if query_stripped.startswith(keyword):
            return "DCL"

    return "OTHER"


def extract_tables(query: str) -> list[str]:
    """Extract table names from a query.

    This is a simple regex-based approach. It won't catch everything
    (CTEs, subqueries, etc.) but handles the common cases.
    """
    tables = []
    for match in TABLE_PATTERN.finditer(query):
        # Group 2 is the table name (group 1 is optional schema)
        table_name = match.group(2)
        # Remove quotes if present
        table_name = table_name.strip('"')
        if table_name.upper() not in ("SELECT", "FROM", "WHERE", "AND", "OR"):
            tables.append(table_name.lower())
    return list(set(tables))  # Dedupe


def take_snapshot(dsn: str, timeout_seconds: int = 10) -> Snapshot:
    """Take a snapshot of pg_stat_statements from the target database.

    Args:
        dsn: Connection string for the target database
        timeout_seconds: Query timeout

    Returns:
        Snapshot containing all rows from pg_stat_statements
    """
    timestamp = datetime.now(timezone.utc)
    rows = []

    # Query to get stats - we select the columns we care about
    query = """
        SELECT
            queryid,
            query,
            calls,
            total_exec_time,
            rows,
            shared_blks_hit,
            shared_blks_read,
            temp_blks_read,
            temp_blks_written
        FROM pg_stat_statements
        WHERE queryid IS NOT NULL
          AND query NOT LIKE '%pg_stat_statements%'
    """

    with psycopg.connect(dsn, connect_timeout=timeout_seconds) as conn:
        conn.execute(f"SET statement_timeout = '{timeout_seconds}s'")
        with conn.cursor() as cur:
            cur.execute(query)
            for row in cur:
                rows.append(
                    PgStatStatementsRow(
                        queryid=row[0],
                        query=row[1],
                        calls=row[2],
                        total_exec_time=row[3],
                        rows=row[4],
                        shared_blks_hit=row[5],
                        shared_blks_read=row[6],
                        temp_blks_read=row[7],
                        temp_blks_written=row[8],
                    )
                )

    return Snapshot(timestamp=timestamp, rows=rows)
