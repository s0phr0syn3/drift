"""CLI entrypoint for Drift."""

import signal
import sys
import time
from datetime import datetime
from pathlib import Path

import click
from alembic import command
from alembic.config import Config as AlembicConfig

from drift.config import load_config
from drift.storage.queries import (
    add_monitored_database,
    get_session,
    list_monitored_databases,
    remove_monitored_database,
    get_monitored_database_by_name,
    store_query_stats,
    upsert_query_text,
    get_top_queries,
    get_latest_snapshot_time,
    get_query_details,
    get_query_trend,
    get_snapshot_state,
    save_snapshot_state,
    prune_old_data,
)
from drift.collector.snapshot import take_snapshot, extract_query_type, extract_tables
from drift.collector.delta import calculate_deltas_from_state
from drift.dbt.fingerprint import fingerprint_sql


@click.group()
@click.option("--config", "-c", type=click.Path(exists=True), help="Path to config file")
@click.pass_context
def cli(ctx: click.Context, config: str | None) -> None:
    """Drift - PostgreSQL query performance analyzer."""
    ctx.ensure_object(dict)
    config_path = Path(config) if config else None
    ctx.obj["config"] = load_config(config_path)


@cli.group()
def db() -> None:
    """Database management commands."""
    pass


@db.command("init")
@click.pass_context
def db_init(ctx: click.Context) -> None:
    """Initialize the Drift database schema."""
    config = ctx.obj["config"]

    # Find alembic.ini
    migrations_dir = Path(__file__).parent / "storage" / "migrations"
    alembic_ini = migrations_dir / "alembic.ini"

    if not alembic_ini.exists():
        click.echo(f"Error: alembic.ini not found at {alembic_ini}", err=True)
        sys.exit(1)

    alembic_cfg = AlembicConfig(str(alembic_ini))
    alembic_cfg.set_main_option("script_location", str(migrations_dir))

    click.echo(f"Running migrations on {config.storage.dsn.split('@')[-1]}...")
    command.upgrade(alembic_cfg, "head")
    click.echo("Database initialized.")


@db.command("add")
@click.argument("name")
@click.option("--dsn", required=True, help="PostgreSQL connection string")
@click.pass_context
def db_add(ctx: click.Context, name: str, dsn: str) -> None:
    """Add a database to monitor."""
    config = ctx.obj["config"]

    # Test connection first
    click.echo(f"Testing connection to {dsn.split('@')[-1]}...")
    try:
        snapshot = take_snapshot(dsn, timeout_seconds=5)
        click.echo(f"Connection OK. Found {len(snapshot.rows)} queries in pg_stat_statements.")
    except Exception as e:
        click.echo(f"Error: Could not connect - {e}", err=True)
        sys.exit(1)

    # Add to registry
    with get_session(config.storage.dsn) as session:
        existing = get_monitored_database_by_name(session, name)
        if existing:
            click.echo(f"Error: Database '{name}' already exists.", err=True)
            sys.exit(1)

        add_monitored_database(session, name, dsn)
        click.echo(f"Added database '{name}'.")


@db.command("list")
@click.pass_context
def db_list(ctx: click.Context) -> None:
    """List monitored databases."""
    config = ctx.obj["config"]

    with get_session(config.storage.dsn) as session:
        databases = list_monitored_databases(session)

        if not databases:
            click.echo("No databases configured. Use 'drift db add' to add one.")
            return

        click.echo(f"{'NAME':<20} {'ENABLED':<10} {'CREATED'}")
        click.echo("-" * 50)
        for db in databases:
            enabled = "yes" if db.enabled else "no"
            created = db.created_at.strftime("%Y-%m-%d %H:%M")
            click.echo(f"{db.name:<20} {enabled:<10} {created}")


@db.command("remove")
@click.argument("name")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def db_remove(ctx: click.Context, name: str, yes: bool) -> None:
    """Remove a monitored database."""
    config = ctx.obj["config"]

    if not yes:
        click.confirm(f"Remove database '{name}'?", abort=True)

    with get_session(config.storage.dsn) as session:
        if remove_monitored_database(session, name):
            click.echo(f"Removed database '{name}'.")
        else:
            click.echo(f"Error: Database '{name}' not found.", err=True)
            sys.exit(1)


@cli.group()
def collect() -> None:
    """Stats collection commands."""
    pass


def _run_collection(config, database_filter: str | None = None) -> None:
    """Internal function to run a collection cycle."""
    with get_session(config.storage.dsn) as session:
        databases = list_monitored_databases(session, enabled_only=True)

        if database_filter:
            databases = [db for db in databases if db.name == database_filter]

        for db in databases:
            click.echo(f"[{datetime.now().strftime('%H:%M:%S')}] Collecting from '{db.name}'...")

            try:
                snapshot = take_snapshot(db.connection_dsn, config.collector.snapshot_timeout_seconds)
                previous_state = get_snapshot_state(session, db.id)

                deltas = calculate_deltas_from_state(previous_state, snapshot.rows)

                stats_to_store = [
                    {
                        "queryid": d.queryid,
                        "calls_delta": d.calls_delta,
                        "total_exec_time_delta": d.total_exec_time_delta,
                        "rows_delta": d.rows_delta,
                        "shared_blks_hit_delta": d.shared_blks_hit_delta,
                        "shared_blks_read_delta": d.shared_blks_read_delta,
                        "temp_blks_read_delta": d.temp_blks_read_delta,
                        "temp_blks_written_delta": d.temp_blks_written_delta,
                    }
                    for d in deltas
                ]
                store_query_stats(session, db.id, snapshot.timestamp, stats_to_store)

                for d in deltas:
                    upsert_query_text(
                        session,
                        db.id,
                        d.queryid,
                        d.query,
                        extract_query_type(d.query),
                        extract_tables(d.query),
                        fingerprint_sql(d.query),
                    )

                snapshot_rows = [
                    {
                        "queryid": row.queryid,
                        "calls": row.calls,
                        "total_exec_time": row.total_exec_time,
                        "rows": row.rows,
                        "shared_blks_hit": row.shared_blks_hit,
                        "shared_blks_read": row.shared_blks_read,
                        "temp_blks_read": row.temp_blks_read,
                        "temp_blks_written": row.temp_blks_written,
                    }
                    for row in snapshot.rows
                ]
                save_snapshot_state(session, db.id, snapshot.timestamp, snapshot_rows)

                click.echo(f"  Collected {len(deltas)} query deltas")

            except Exception as e:
                click.echo(f"  Error: {e}", err=True)


@collect.command("once")
@click.option("--database", "-d", help="Collect from specific database only")
@click.pass_context
def collect_once(ctx: click.Context, database: str | None) -> None:
    """Run a single collection cycle."""
    config = ctx.obj["config"]

    with get_session(config.storage.dsn) as session:
        databases = list_monitored_databases(session, enabled_only=True)

        if database:
            databases = [db for db in databases if db.name == database]
            if not databases:
                click.echo(f"Error: Database '{database}' not found or not enabled.", err=True)
                sys.exit(1)

        if not databases:
            click.echo("No databases to collect from. Use 'drift db add' first.")
            return

        for db in databases:
            click.echo(f"Collecting from '{db.name}'...")

            try:
                # Take snapshot from target database
                snapshot = take_snapshot(db.connection_dsn, config.collector.snapshot_timeout_seconds)
                click.echo(f"  Got {len(snapshot.rows)} queries from pg_stat_statements")

                # Load previous state from drift database
                previous_state = get_snapshot_state(session, db.id)
                if previous_state:
                    click.echo(f"  Loaded previous state ({len(previous_state)} queries)")
                else:
                    click.echo("  No previous state (first collection)")

                # Calculate deltas
                deltas = calculate_deltas_from_state(previous_state, snapshot.rows)
                click.echo(f"  Calculated {len(deltas)} deltas")

                # Store deltas
                stats_to_store = [
                    {
                        "queryid": d.queryid,
                        "calls_delta": d.calls_delta,
                        "total_exec_time_delta": d.total_exec_time_delta,
                        "rows_delta": d.rows_delta,
                        "shared_blks_hit_delta": d.shared_blks_hit_delta,
                        "shared_blks_read_delta": d.shared_blks_read_delta,
                        "temp_blks_read_delta": d.temp_blks_read_delta,
                        "temp_blks_written_delta": d.temp_blks_written_delta,
                    }
                    for d in deltas
                ]
                stored = store_query_stats(session, db.id, snapshot.timestamp, stats_to_store)
                click.echo(f"  Stored {stored} stat rows")

                # Store/update query text
                for d in deltas:
                    upsert_query_text(
                        session,
                        db.id,
                        d.queryid,
                        d.query,
                        extract_query_type(d.query),
                        extract_tables(d.query),
                        fingerprint_sql(d.query),
                    )

                # Save current snapshot state for next collection
                snapshot_rows = [
                    {
                        "queryid": row.queryid,
                        "calls": row.calls,
                        "total_exec_time": row.total_exec_time,
                        "rows": row.rows,
                        "shared_blks_hit": row.shared_blks_hit,
                        "shared_blks_read": row.shared_blks_read,
                        "temp_blks_read": row.temp_blks_read,
                        "temp_blks_written": row.temp_blks_written,
                    }
                    for row in snapshot.rows
                ]
                save_snapshot_state(session, db.id, snapshot.timestamp, snapshot_rows)
                click.echo("  Saved snapshot state")

            except Exception as e:
                click.echo(f"  Error: {e}", err=True)


@collect.command("start")
@click.option("--database", "-d", help="Collect from specific database only")
@click.option("--interval", "-i", default=None, type=int, help="Collection interval in seconds (default: from config)")
@click.pass_context
def collect_start(ctx: click.Context, database: str | None, interval: int | None) -> None:
    """Start continuous collection daemon."""
    config = ctx.obj["config"]
    interval = interval or config.collector.interval_seconds

    # Verify databases exist
    with get_session(config.storage.dsn) as session:
        databases = list_monitored_databases(session, enabled_only=True)
        if database:
            databases = [db for db in databases if db.name == database]
        if not databases:
            click.echo("No databases to collect from. Use 'drift db add' first.")
            return
        db_names = [db.name for db in databases]

    click.echo(f"Starting collector daemon")
    click.echo(f"  Databases: {', '.join(db_names)}")
    click.echo(f"  Interval: {interval}s")
    click.echo("  Press Ctrl+C to stop")
    click.echo()

    # Handle graceful shutdown
    shutdown_requested = False

    def handle_signal(signum, frame):
        nonlocal shutdown_requested
        shutdown_requested = True
        click.echo("\nShutdown requested, finishing current collection...")

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Main loop
    while not shutdown_requested:
        _run_collection(config, database)

        # Sleep in small increments to allow for quick shutdown
        for _ in range(interval):
            if shutdown_requested:
                break
            time.sleep(1)

    click.echo("Collector stopped.")


@cli.command("top")
@click.option("--database", "-d", help="Filter by database name")
@click.option("--period", "-p", default="24h", help="Time period (e.g., 24h, 7d)")
@click.option("--sort", "-s", default="total_time", type=click.Choice(["total_time", "calls", "mean_time"]))
@click.option("--limit", "-n", default=20, help="Number of results")
@click.pass_context
def top(ctx: click.Context, database: str | None, period: str, sort: str, limit: int) -> None:
    """Show top queries by resource usage."""
    config = ctx.obj["config"]

    # Parse period
    period_hours = _parse_period(period)

    with get_session(config.storage.dsn) as session:
        database_id = None
        if database:
            db = get_monitored_database_by_name(session, database)
            if not db:
                click.echo(f"Error: Database '{database}' not found.", err=True)
                sys.exit(1)
            database_id = db.id

        queries = get_top_queries(session, database_id, period_hours, sort, limit)

        if not queries:
            click.echo("No query data found. Run 'drift collect once' first.")
            return

        # Print results
        click.echo(f"Top {len(queries)} queries by {sort} (last {period}):\n")
        click.echo(f"{'QUERYID':<20} {'CALLS':>10} {'TOTAL MS':>12} {'MEAN MS':>10} {'QUERY'}")
        click.echo("-" * 90)

        for q in queries:
            queryid = q["queryid"]
            calls = q["total_calls"] or 0
            total_ms = q["total_time_ms"] or 0
            mean_ms = q["mean_time_ms"] or 0
            query_preview = (q["query"] or "")[:30].replace("\n", " ")

            click.echo(f"{queryid:<20} {calls:>10} {total_ms:>12.1f} {mean_ms:>10.2f} {query_preview}...")


@cli.command("prune")
@click.option("--raw-days", default=None, type=int, help="Keep raw stats for this many days (default: from config)")
@click.option("--hourly-days", default=None, type=int, help="Keep hourly stats for this many days (default: from config)")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def prune(ctx: click.Context, raw_days: int | None, hourly_days: int | None, yes: bool) -> None:
    """Delete old data based on retention settings."""
    config = ctx.obj["config"]

    # Use config values if not specified
    raw_days = raw_days or config.retention.raw_days
    hourly_days = hourly_days or config.retention.hourly_days

    if not yes:
        click.echo(f"This will delete:")
        click.echo(f"  - Raw stats older than {raw_days} days")
        click.echo(f"  - Hourly stats older than {hourly_days} days")
        click.confirm("Continue?", abort=True)

    with get_session(config.storage.dsn) as session:
        result = prune_old_data(session, raw_days, hourly_days)

        click.echo(f"Deleted {result['query_stats_raw']} raw stat rows")
        click.echo(f"Deleted {result['query_stats_hourly']} hourly stat rows")


def _parse_period(period: str) -> int:
    """Parse a period string like '24h' or '7d' into hours."""
    period = period.lower().strip()

    if period.endswith("h"):
        return int(period[:-1])
    elif period.endswith("d"):
        return int(period[:-1]) * 24
    elif period.endswith("w"):
        return int(period[:-1]) * 24 * 7
    else:
        # Assume hours
        return int(period)


@cli.command("show")
@click.argument("queryid", type=int)
@click.option("--database", "-d", help="Filter by database name")
@click.option("--period", "-p", default="24h", help="Time period for stats (e.g., 24h, 7d)")
@click.pass_context
def show(ctx: click.Context, queryid: int, database: str | None, period: str) -> None:
    """Show detailed information about a specific query."""
    config = ctx.obj["config"]
    period_hours = _parse_period(period)

    with get_session(config.storage.dsn) as session:
        database_id = None
        if database:
            db = get_monitored_database_by_name(session, database)
            if not db:
                click.echo(f"Error: Database '{database}' not found.", err=True)
                sys.exit(1)
            database_id = db.id

        details = get_query_details(session, queryid, database_id, period_hours)

        if not details:
            click.echo(f"Error: Query {queryid} not found.", err=True)
            sys.exit(1)

        # Print query details
        click.echo(f"Query ID: {details['queryid']}")
        click.echo(f"Type: {details['query_type'] or 'Unknown'}")
        click.echo(f"Tables: {', '.join(details['tables']) if details['tables'] else 'Unknown'}")
        click.echo(f"First seen: {details['first_seen'].strftime('%Y-%m-%d %H:%M:%S') if details['first_seen'] else 'N/A'}")
        click.echo(f"Last seen: {details['last_seen'].strftime('%Y-%m-%d %H:%M:%S') if details['last_seen'] else 'N/A'}")
        click.echo()

        click.echo(f"Stats (last {period}):")
        click.echo(f"  Calls: {details['total_calls']:,}")
        click.echo(f"  Total time: {details['total_time_ms']:,.1f} ms")
        if details['mean_time_ms']:
            click.echo(f"  Mean time: {details['mean_time_ms']:.2f} ms")
        click.echo(f"  Rows: {details['total_rows']:,}")
        if details['cache_hit_ratio'] is not None:
            click.echo(f"  Cache hit ratio: {details['cache_hit_ratio']:.1f}%")
        if details['temp_blks_read'] or details['temp_blks_written']:
            click.echo(f"  Temp blocks: {details['temp_blks_read']:,} read, {details['temp_blks_written']:,} written")
        click.echo(f"  Snapshots: {details['snapshot_count']}")
        click.echo()

        click.echo("Query:")
        click.echo("-" * 60)
        click.echo(details['query'])
        click.echo("-" * 60)
        click.echo()
        click.echo(f"Fingerprint: {details['fingerprint']}")


@cli.command("trend")
@click.argument("queryid", type=int)
@click.option("--database", "-d", help="Filter by database name")
@click.option("--period", "-p", default="7d", help="Time period (e.g., 24h, 7d)")
@click.option("--metric", "-m", default="mean_time", type=click.Choice(["calls", "total_time", "mean_time"]))
@click.pass_context
def trend(ctx: click.Context, queryid: int, database: str | None, period: str, metric: str) -> None:
    """Show query performance trend over time."""
    config = ctx.obj["config"]
    period_hours = _parse_period(period)

    with get_session(config.storage.dsn) as session:
        database_id = None
        if database:
            db = get_monitored_database_by_name(session, database)
            if not db:
                click.echo(f"Error: Database '{database}' not found.", err=True)
                sys.exit(1)
            database_id = db.id

        data = get_query_trend(session, queryid, database_id, period_hours)

        if not data:
            click.echo(f"No trend data found for query {queryid}. Need more collection samples.")
            return

        # Map metric name to data key and display label
        metric_map = {
            "calls": ("calls", "Calls"),
            "total_time": ("total_time_ms", "Total Time (ms)"),
            "mean_time": ("mean_time_ms", "Mean Time (ms)"),
        }
        data_key, label = metric_map[metric]

        # Extract values for the chart
        values = [d[data_key] for d in data]
        times = [d["time"] for d in data]

        if not any(values):
            click.echo(f"No {metric} data available for query {queryid}.")
            return

        click.echo(f"Query {queryid} - {label} (last {period})")
        click.echo()

        # Draw ASCII chart
        _draw_ascii_chart(times, values, label)

        # Print summary stats
        non_zero = [v for v in values if v]
        if non_zero:
            click.echo()
            click.echo(f"Min: {min(non_zero):.2f}  Max: {max(non_zero):.2f}  Avg: {sum(non_zero)/len(non_zero):.2f}")


def _draw_ascii_chart(times: list, values: list, label: str, width: int = 60, height: int = 15) -> None:
    """Draw a simple ASCII chart."""
    if not values or not any(values):
        click.echo("No data to display.")
        return

    # Handle None values
    values = [v if v is not None else 0 for v in values]

    max_val = max(values) if values else 0
    min_val = min(values) if values else 0

    if max_val == min_val:
        max_val = min_val + 1  # Avoid division by zero

    # Scale values to chart height
    def scale(v):
        return int((v - min_val) / (max_val - min_val) * (height - 1))

    # Determine how many data points to show (sample if too many)
    if len(values) > width:
        step = len(values) / width
        sampled_values = [values[int(i * step)] for i in range(width)]
        sampled_times = [times[int(i * step)] for i in range(width)]
    else:
        sampled_values = values
        sampled_times = times

    # Draw chart rows (top to bottom)
    for row in range(height - 1, -1, -1):
        # Y-axis label
        if row == height - 1:
            y_label = f"{max_val:>8.1f} |"
        elif row == 0:
            y_label = f"{min_val:>8.1f} |"
        elif row == height // 2:
            mid_val = (max_val + min_val) / 2
            y_label = f"{mid_val:>8.1f} |"
        else:
            y_label = "         |"

        # Build row
        row_chars = []
        for v in sampled_values:
            if scale(v) >= row:
                row_chars.append("█")
            else:
                row_chars.append(" ")

        click.echo(y_label + "".join(row_chars))

    # X-axis
    click.echo("         +" + "-" * len(sampled_values))

    # Time labels (start and end)
    if sampled_times:
        start_label = sampled_times[0].strftime("%m/%d %H:%M")
        end_label = sampled_times[-1].strftime("%m/%d %H:%M")
        padding = len(sampled_values) - len(start_label) - len(end_label)
        click.echo(f"          {start_label}{' ' * max(0, padding)}{end_label}")


if __name__ == "__main__":
    cli()
