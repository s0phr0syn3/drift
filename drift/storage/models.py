"""SQLAlchemy models for Drift storage."""

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Double,
    ForeignKey,
    Index,
    String,
    Text,
    create_engine,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class MonitoredDatabase(Base):
    """A PostgreSQL database being monitored by Drift."""

    __tablename__ = "monitored_databases"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    connection_dsn: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utc_now)

    # Relationships
    query_stats: Mapped[list["QueryStatsRaw"]] = relationship(back_populates="database")
    query_texts: Mapped[list["QueryText"]] = relationship(back_populates="database")


class QueryStatsRaw(Base):
    """Raw query statistics from pg_stat_statements.

    Each row represents the delta (change) in stats since the previous snapshot.
    We store deltas rather than absolute values because pg_stat_statements
    counters can reset (e.g., after pg_stat_statements_reset() or server restart).
    """

    __tablename__ = "query_stats_raw"

    snapshot_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True, nullable=False
    )
    database_id: Mapped[int] = mapped_column(
        ForeignKey("monitored_databases.id"), primary_key=True, nullable=False
    )
    queryid: Mapped[int] = mapped_column(BigInteger, primary_key=True, nullable=False)

    # Delta values since last snapshot
    calls_delta: Mapped[int | None] = mapped_column(BigInteger)
    total_exec_time_delta: Mapped[float | None] = mapped_column(Double)
    rows_delta: Mapped[int | None] = mapped_column(BigInteger)
    shared_blks_hit_delta: Mapped[int | None] = mapped_column(BigInteger)
    shared_blks_read_delta: Mapped[int | None] = mapped_column(BigInteger)
    temp_blks_read_delta: Mapped[int | None] = mapped_column(BigInteger)
    temp_blks_written_delta: Mapped[int | None] = mapped_column(BigInteger)

    # Relationships
    database: Mapped["MonitoredDatabase"] = relationship(back_populates="query_stats")


class QueryText(Base):
    """Query text and metadata for each unique queryid.

    pg_stat_statements identifies queries by queryid (a hash of the normalized query).
    We store the query text separately since it doesn't change between snapshots.
    """

    __tablename__ = "query_text"

    queryid: Mapped[int] = mapped_column(BigInteger, primary_key=True, nullable=False)
    database_id: Mapped[int] = mapped_column(
        ForeignKey("monitored_databases.id"), primary_key=True, nullable=False
    )

    query: Mapped[str] = mapped_column(Text, nullable=False)
    query_type: Mapped[str | None] = mapped_column(String(50))  # SELECT, INSERT, UPDATE, DELETE
    tables: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)  # SHA256 hex

    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utc_now)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utc_now)

    # Relationships
    database: Mapped["MonitoredDatabase"] = relationship(back_populates="query_texts")

    __table_args__ = (
        Index("idx_query_text_fingerprint", "fingerprint"),
    )


class QueryStatsHourly(Base):
    """Hourly aggregated query statistics.

    We roll up raw stats into hourly buckets for efficient long-term storage
    and querying. This table is what most analysis queries will use.
    """

    __tablename__ = "query_stats_hourly"

    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    database_id: Mapped[int] = mapped_column(
        ForeignKey("monitored_databases.id"), primary_key=True
    )
    queryid: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    calls: Mapped[int | None] = mapped_column(BigInteger)
    total_time_ms: Mapped[float | None] = mapped_column(Double)
    mean_time_ms: Mapped[float | None] = mapped_column(Double)
    rows: Mapped[int | None] = mapped_column(BigInteger)
    cache_hit_ratio: Mapped[float | None] = mapped_column(Double)
    temp_blks_total: Mapped[int | None] = mapped_column(BigInteger)


class SnapshotState(Base):
    """Persisted snapshot state for delta calculation.

    Stores the last known absolute values from pg_stat_statements so we can
    calculate deltas even after process restart. Updated after each collection.
    """

    __tablename__ = "snapshot_state"

    database_id: Mapped[int] = mapped_column(
        ForeignKey("monitored_databases.id"), primary_key=True
    )
    queryid: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    calls: Mapped[int] = mapped_column(BigInteger, nullable=False)
    total_exec_time: Mapped[float] = mapped_column(Double, nullable=False)
    rows: Mapped[int] = mapped_column(BigInteger, nullable=False)
    shared_blks_hit: Mapped[int] = mapped_column(BigInteger, nullable=False)
    shared_blks_read: Mapped[int] = mapped_column(BigInteger, nullable=False)
    temp_blks_read: Mapped[int] = mapped_column(BigInteger, nullable=False)
    temp_blks_written: Mapped[int] = mapped_column(BigInteger, nullable=False)

    snapshot_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AlertRule(Base):
    """Alert rule configuration.

    Defines conditions that trigger alerts, such as latency increases,
    cache hit ratio drops, or temp disk usage spikes.
    """

    __tablename__ = "alert_rules"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    rule_type: Mapped[str] = mapped_column(String(50), nullable=False)
    # Rule types: latency_increase, cache_drop, temp_disk, new_query
    threshold: Mapped[dict] = mapped_column(JSONB, nullable=False)
    # e.g., {"percent_increase": 50} or {"min_ratio": 90}
    notification: Mapped[dict | None] = mapped_column(JSONB)
    # e.g., {"webhook_url": "https://..."}
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utc_now)

    # Relationships
    events: Mapped[list["AlertEvent"]] = relationship(back_populates="rule")


class AlertEvent(Base):
    """Triggered alert event.

    Records when an alert rule was triggered, with details about what caused it.
    """

    __tablename__ = "alert_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int | None] = mapped_column(ForeignKey("alert_rules.id"))
    database_id: Mapped[int | None] = mapped_column(ForeignKey("monitored_databases.id"))
    queryid: Mapped[int | None] = mapped_column(BigInteger)
    triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utc_now)
    details: Mapped[dict | None] = mapped_column(JSONB)
    # e.g., {"current_value": 150, "baseline_value": 100, "percent_change": 50}
    acknowledged: Mapped[bool] = mapped_column(Boolean, default=False)

    # Relationships
    rule: Mapped["AlertRule"] = relationship(back_populates="events")


def get_engine(dsn: str):
    """Create a SQLAlchemy engine for the given DSN."""
    return create_engine(dsn)
