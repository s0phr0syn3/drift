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
    rollup_hourly,
    get_new_queries,
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


@cli.command("rollup")
@click.option("--hours", "-h", default=24, help="Process data from last N hours (default: 24)")
@click.pass_context
def rollup(ctx: click.Context, hours: int) -> None:
    """Aggregate raw stats into hourly buckets.

    Processes raw query stats and creates/updates hourly rollups for efficient
    long-term storage and analysis. Safe to run multiple times.
    """
    config = ctx.obj["config"]

    click.echo(f"Rolling up data from last {hours} hours...")

    with get_session(config.storage.dsn) as session:
        result = rollup_hourly(session, hours)

        click.echo(f"Processed {result['hours_processed']} hours")
        click.echo(f"Upserted {result['rows_upserted']} hourly stat rows")


@cli.command("new-queries")
@click.option("--database", "-d", help="Filter by database name")
@click.option("--since", "-s", default="24h", help="Time window (e.g., 24h, 7d)")
@click.option("--limit", "-n", default=50, help="Maximum number of results")
@click.pass_context
def new_queries(ctx: click.Context, database: str | None, since: str, limit: int) -> None:
    """Show queries that first appeared recently.

    Useful for detecting new query patterns, potential issues from deployments,
    or unexpected queries hitting the database.
    """
    config = ctx.obj["config"]
    since_hours = _parse_period(since)

    with get_session(config.storage.dsn) as session:
        database_id = None
        if database:
            db = get_monitored_database_by_name(session, database)
            if not db:
                click.echo(f"Error: Database '{database}' not found.", err=True)
                sys.exit(1)
            database_id = db.id

        queries = get_new_queries(session, database_id, since_hours, limit)

        if not queries:
            click.echo(f"No new queries found in the last {since}.")
            return

        click.echo(f"New queries (first seen in last {since}):\n")
        click.echo(f"{'QUERYID':<20} {'TYPE':<10} {'FIRST SEEN':<20} {'QUERY'}")
        click.echo("-" * 90)

        for q in queries:
            queryid = q["queryid"]
            query_type = q["query_type"] or "?"
            first_seen = q["first_seen"].strftime("%Y-%m-%d %H:%M:%S") if q["first_seen"] else "N/A"
            query_preview = (q["query"] or "")[:35].replace("\n", " ")

            click.echo(f"{queryid:<20} {query_type:<10} {first_seen:<20} {query_preview}...")


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


# Alert commands
@cli.group()
def alerts() -> None:
    """Alert management commands."""
    pass


@alerts.command("rules")
@click.pass_context
def alerts_rules(ctx: click.Context) -> None:
    """List all alert rules."""
    from drift.analysis.alerts import get_alert_rules

    config = ctx.obj["config"]

    with get_session(config.storage.dsn) as session:
        rules = get_alert_rules(session, enabled_only=False)

        if not rules:
            click.echo("No alert rules configured. Use 'drift alerts add' to create one.")
            return

        click.echo(f"{'ID':<5} {'NAME':<25} {'TYPE':<18} {'ENABLED':<8} {'THRESHOLD'}")
        click.echo("-" * 80)

        for rule in rules:
            enabled = "yes" if rule.enabled else "no"
            threshold_str = str(rule.threshold)[:20] + "..." if len(str(rule.threshold)) > 20 else str(rule.threshold)
            click.echo(f"{rule.id:<5} {rule.name:<25} {rule.rule_type:<18} {enabled:<8} {threshold_str}")


@alerts.command("add")
@click.argument("name")
@click.option("--type", "-t", "rule_type", required=True,
              type=click.Choice(["latency_increase", "cache_drop", "temp_disk"]),
              help="Type of anomaly to detect")
@click.option("--threshold", "-T", default=50, help="Threshold value (percent for latency/temp, points for cache)")
@click.option("--webhook", "-w", help="Webhook URL for notifications")
@click.pass_context
def alerts_add(ctx: click.Context, name: str, rule_type: str, threshold: int, webhook: str | None) -> None:
    """Add a new alert rule."""
    from drift.analysis.alerts import create_alert_rule

    config = ctx.obj["config"]

    # Build threshold config based on rule type
    if rule_type == "latency_increase":
        threshold_config = {"percent_increase": threshold}
    elif rule_type == "cache_drop":
        threshold_config = {"min_drop_points": threshold}
    elif rule_type == "temp_disk":
        threshold_config = {"percent_increase": threshold}
    else:
        threshold_config = {"value": threshold}

    notification = {"webhook_url": webhook} if webhook else None

    with get_session(config.storage.dsn) as session:
        rule = create_alert_rule(session, name, rule_type, threshold_config, notification)
        click.echo(f"Created alert rule '{name}' (ID: {rule.id})")


@alerts.command("remove")
@click.argument("rule_id", type=int)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def alerts_remove(ctx: click.Context, rule_id: int, yes: bool) -> None:
    """Remove an alert rule."""
    from drift.storage.models import AlertRule

    config = ctx.obj["config"]

    if not yes:
        click.confirm(f"Remove alert rule {rule_id}?", abort=True)

    with get_session(config.storage.dsn) as session:
        rule = session.get(AlertRule, rule_id)
        if rule:
            session.delete(rule)
            click.echo(f"Removed alert rule '{rule.name}'")
        else:
            click.echo(f"Error: Alert rule {rule_id} not found.", err=True)
            sys.exit(1)


@alerts.command("events")
@click.option("--unack", is_flag=True, help="Show only unacknowledged alerts")
@click.option("--limit", "-n", default=20, help="Number of events to show")
@click.pass_context
def alerts_events(ctx: click.Context, unack: bool, limit: int) -> None:
    """List recent alert events."""
    from drift.analysis.alerts import get_alert_events

    config = ctx.obj["config"]

    with get_session(config.storage.dsn) as session:
        acknowledged = False if unack else None
        events = get_alert_events(session, acknowledged=acknowledged, limit=limit)

        if not events:
            msg = "No unacknowledged alerts." if unack else "No alert events."
            click.echo(msg)
            return

        click.echo(f"{'ID':<6} {'TIME':<20} {'TYPE':<18} {'QUERYID':<20} {'ACK'}")
        click.echo("-" * 80)

        for event in events:
            time_str = event.triggered_at.strftime("%Y-%m-%d %H:%M:%S") if event.triggered_at else "N/A"
            anomaly_type = event.details.get("anomaly_type", "?") if event.details else "?"
            ack = "yes" if event.acknowledged else "no"
            click.echo(f"{event.id:<6} {time_str:<20} {anomaly_type:<18} {event.queryid or 'N/A':<20} {ack}")


@alerts.command("ack")
@click.argument("event_id", type=int)
@click.pass_context
def alerts_ack(ctx: click.Context, event_id: int) -> None:
    """Acknowledge an alert event."""
    from drift.analysis.alerts import acknowledge_alert

    config = ctx.obj["config"]

    with get_session(config.storage.dsn) as session:
        if acknowledge_alert(session, event_id):
            click.echo(f"Acknowledged alert event {event_id}")
        else:
            click.echo(f"Error: Alert event {event_id} not found.", err=True)
            sys.exit(1)


@alerts.command("check")
@click.option("--database", "-d", help="Check specific database only")
@click.option("--no-notify", is_flag=True, help="Skip webhook notifications")
@click.pass_context
def alerts_check(ctx: click.Context, database: str | None, no_notify: bool) -> None:
    """Run anomaly detection and generate alerts.

    Checks all queries for performance anomalies based on configured rules.
    Creates alert events and sends notifications for any triggered rules.
    """
    from drift.analysis.alerts import run_anomaly_check

    config = ctx.obj["config"]

    with get_session(config.storage.dsn) as session:
        database_id = None
        if database:
            db = get_monitored_database_by_name(session, database)
            if not db:
                click.echo(f"Error: Database '{database}' not found.", err=True)
                sys.exit(1)
            database_id = db.id

        click.echo("Running anomaly detection...")
        events = run_anomaly_check(session, database_id, notify=not no_notify)

        if events:
            click.echo(f"Created {len(events)} alert events:")
            for event in events:
                anomaly_type = event.details.get("anomaly_type", "?") if event.details else "?"
                click.echo(f"  - {anomaly_type} for query {event.queryid}")
        else:
            click.echo("No anomalies detected.")


# dbt Integration Commands
@cli.group("dbt")
def dbt_group() -> None:
    """dbt integration commands."""
    pass


@dbt_group.command("ingest")
@click.argument("target_dir", type=click.Path(exists=True))
@click.option("--project", "-p", default=None, help="Project name (default: from manifest)")
@click.pass_context
def dbt_ingest(ctx: click.Context, target_dir: str, project: str | None) -> None:
    """Ingest dbt artifacts from target directory.

    Parses manifest.json and run_results.json (if present) from a dbt target
    directory. Stores model definitions and correlates them with pg_stat_statements
    queries using SQL fingerprinting.

    Example:
        drift dbt ingest ./target/
        drift dbt ingest ./target/ --project my_analytics
    """
    import json
    from drift.dbt.parser import parse_dbt_target
    from drift.dbt.fingerprint import fingerprint_sql
    from drift.dbt.correlate import correlate_model_to_queries
    from drift.storage.models import DbtProject, DbtModelRecord, DbtRun, DbtModelExecution, QueryText
    from sqlalchemy import select

    config = ctx.obj["config"]
    target_path = Path(target_dir)

    click.echo(f"Parsing dbt artifacts from {target_path}...")

    try:
        models, run_results = parse_dbt_target(target_path)
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    click.echo(f"  Found {len(models)} models")
    if run_results:
        click.echo(f"  Found run results with {len(run_results.executions)} model executions")

    # Determine project name
    if not project:
        # Try to get from manifest
        manifest_path = target_path / "manifest.json"
        with open(manifest_path) as f:
            manifest = json.load(f)
        project = manifest.get("metadata", {}).get("project_name", "default")

    click.echo(f"  Project: {project}")

    with get_session(config.storage.dsn) as session:
        # Get or create project
        proj = session.scalar(select(DbtProject).where(DbtProject.name == project))
        if not proj:
            proj = DbtProject(name=project)
            session.add(proj)
            session.flush()
            click.echo(f"  Created project '{project}'")

        # Get existing queries for correlation
        queries = []
        for qt in session.scalars(select(QueryText)):
            queries.append({
                "queryid": qt.queryid,
                "query": qt.query,
                "fingerprint": qt.fingerprint,
                "tables": qt.tables,
            })
        click.echo(f"  Loaded {len(queries)} queries for correlation")

        # Process models
        models_ingested = 0
        models_correlated = 0

        for unique_id, model in models.items():
            # Calculate fingerprint
            sql_fp = fingerprint_sql(model.compiled_sql) if model.compiled_sql else None

            # Check if model exists
            existing = session.scalar(
                select(DbtModelRecord).where(
                    DbtModelRecord.project_id == proj.id,
                    DbtModelRecord.unique_id == unique_id,
                )
            )

            if existing:
                # Update
                existing.name = model.name
                existing.schema_name = model.schema_name
                existing.database_name = model.database
                existing.alias = model.alias
                existing.relation_name = model.relation_name
                existing.materialized = model.materialized
                existing.description = model.description
                existing.compiled_sql = model.compiled_sql
                existing.sql_fingerprint = sql_fp
                existing.depends_on = model.depends_on
                existing.tags = model.tags
                existing.updated_at = datetime.now()
                model_record = existing
            else:
                # Create
                model_record = DbtModelRecord(
                    project_id=proj.id,
                    unique_id=unique_id,
                    name=model.name,
                    schema_name=model.schema_name,
                    database_name=model.database,
                    alias=model.alias,
                    relation_name=model.relation_name,
                    materialized=model.materialized,
                    description=model.description,
                    compiled_sql=model.compiled_sql,
                    sql_fingerprint=sql_fp,
                    depends_on=model.depends_on,
                    tags=model.tags,
                )
                session.add(model_record)
                session.flush()

            models_ingested += 1

            # Correlate with pg_stat_statements
            if model.compiled_sql and queries:
                correlation = correlate_model_to_queries(model, queries)
                if correlation:
                    model_record.correlated_queryid = correlation.queryid
                    model_record.correlation_type = correlation.match_type
                    model_record.correlation_confidence = correlation.confidence
                    models_correlated += 1

        click.echo(f"  Ingested {models_ingested} models")
        click.echo(f"  Correlated {models_correlated} models with queries")

        # Process run results if present
        if run_results:
            # Count stats
            success_count = sum(1 for e in run_results.executions if e.status == "success")
            error_count = sum(1 for e in run_results.executions if e.status == "error")
            total_time = sum(e.execution_time for e in run_results.executions)

            run = DbtRun(
                project_id=proj.id,
                invocation_id=run_results.invocation_id,
                started_at=run_results.started_at,
                completed_at=run_results.completed_at,
                status=run_results.status,
                total_execution_time=total_time,
                models_run=len(run_results.executions),
                models_success=success_count,
                models_error=error_count,
            )
            session.add(run)
            session.flush()

            # Create execution records
            for exec_result in run_results.executions:
                model_record = session.scalar(
                    select(DbtModelRecord).where(
                        DbtModelRecord.project_id == proj.id,
                        DbtModelRecord.unique_id == exec_result.unique_id,
                    )
                )
                if model_record:
                    execution = DbtModelExecution(
                        run_id=run.id,
                        model_id=model_record.id,
                        status=exec_result.status,
                        execution_time=exec_result.execution_time,
                        rows_affected=exec_result.rows_affected,
                        db_queryid=model_record.correlated_queryid,
                    )
                    session.add(execution)

            click.echo(f"  Created run record (ID: {run.id})")
            click.echo(f"    Status: {run_results.status}")
            click.echo(f"    Total time: {total_time:.1f}s")
            click.echo(f"    Success: {success_count}, Errors: {error_count}")

    click.echo("Done!")


@dbt_group.command("models")
@click.option("--project", "-p", help="Filter by project name")
@click.option("--correlated", is_flag=True, help="Only show models with query correlation")
@click.pass_context
def dbt_models(ctx: click.Context, project: str | None, correlated: bool) -> None:
    """List dbt models."""
    from drift.storage.models import DbtProject, DbtModelRecord
    from sqlalchemy import select

    config = ctx.obj["config"]

    with get_session(config.storage.dsn) as session:
        stmt = select(DbtModelRecord).order_by(DbtModelRecord.name)

        if project:
            proj = session.scalar(select(DbtProject).where(DbtProject.name == project))
            if not proj:
                click.echo(f"Error: Project '{project}' not found.", err=True)
                sys.exit(1)
            stmt = stmt.where(DbtModelRecord.project_id == proj.id)

        if correlated:
            stmt = stmt.where(DbtModelRecord.correlated_queryid.isnot(None))

        models = list(session.scalars(stmt))

        if not models:
            click.echo("No dbt models found. Run 'drift dbt ingest' first.")
            return

        click.echo(f"{'NAME':<30} {'MATERIALIZED':<12} {'CORRELATED':<12} {'CONFIDENCE'}")
        click.echo("-" * 75)

        for m in models:
            corr_status = "yes" if m.correlated_queryid else "no"
            confidence = f"{m.correlation_confidence:.0%}" if m.correlation_confidence else "-"
            click.echo(f"{m.name:<30} {m.materialized or 'view':<12} {corr_status:<12} {confidence}")


@dbt_group.command("runs")
@click.option("--project", "-p", help="Filter by project name")
@click.option("--limit", "-n", default=10, help="Number of runs to show")
@click.pass_context
def dbt_runs(ctx: click.Context, project: str | None, limit: int) -> None:
    """List recent dbt runs."""
    from drift.storage.models import DbtProject, DbtRun
    from sqlalchemy import select

    config = ctx.obj["config"]

    with get_session(config.storage.dsn) as session:
        stmt = select(DbtRun).order_by(DbtRun.started_at.desc()).limit(limit)

        if project:
            proj = session.scalar(select(DbtProject).where(DbtProject.name == project))
            if not proj:
                click.echo(f"Error: Project '{project}' not found.", err=True)
                sys.exit(1)
            stmt = stmt.where(DbtRun.project_id == proj.id)

        runs = list(session.scalars(stmt))

        if not runs:
            click.echo("No dbt runs found. Run 'drift dbt ingest' with run_results.json.")
            return

        click.echo(f"{'ID':<6} {'STARTED':<20} {'STATUS':<10} {'MODELS':<8} {'TIME'}")
        click.echo("-" * 60)

        for r in runs:
            started = r.started_at.strftime("%Y-%m-%d %H:%M") if r.started_at else "N/A"
            models_str = f"{r.models_success or 0}/{r.models_run or 0}"
            time_str = f"{r.total_execution_time:.1f}s" if r.total_execution_time else "-"
            click.echo(f"{r.id:<6} {started:<20} {r.status or 'unknown':<10} {models_str:<8} {time_str}")


@cli.command("serve")
@click.option("--host", "-h", default="127.0.0.1", help="Host to bind to")
@click.option("--port", "-p", default=8000, help="Port to bind to")
@click.option("--reload", is_flag=True, help="Enable auto-reload for development")
@click.pass_context
def serve(ctx: click.Context, host: str, port: int, reload: bool) -> None:
    """Start the Drift API server.

    Runs the FastAPI application with uvicorn. The API provides REST endpoints
    for querying performance data and managing alerts.

    API documentation is available at /docs (Swagger) or /redoc.
    """
    import uvicorn

    click.echo(f"Starting Drift API server at http://{host}:{port}")
    click.echo(f"API docs: http://{host}:{port}/docs")
    click.echo("Press Ctrl+C to stop")

    uvicorn.run(
        "drift.api.app:app",
        host=host,
        port=port,
        reload=reload,
    )


if __name__ == "__main__":
    cli()
