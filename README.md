# Drift

PostgreSQL query performance analyzer with dbt integration.

Drift collects query performance metrics from `pg_stat_statements` and correlates them with dbt model definitions to provide unified performance observability.

## Why Drift?

If you're running dbt on PostgreSQL, you're likely missing critical performance insights. Here's what Drift gives you:

| Without Drift | With Drift |
|--------------|------------|
| dbt reports "model took 45s" | See actual Postgres execution time (42s query + 3s dbt overhead) |
| No visibility into cache efficiency | See 73% cache hit ratio → get index recommendations |
| Can't track model performance over time | Historical trends with before/after comparisons |
| "Which model is hurting the database?" | Models ranked by actual DB impact, not just dbt time |
| No connection between dbt and pg_stat_statements | Unified view: model → query → performance metrics |
| Guessing if your optimization worked | Annotations with automatic before/after comparison |

### Example Insight

After correlating your dbt models with database queries, Drift might show:

```
Model: fct_orders
Materialized: incremental
Correlated Query: 8847291034
Confidence: 100% (fingerprint match)

Insights:
⚠️  Low cache hit ratio (67%)
    → Consider adding index on orders(customer_id, created_at)

⚠️  Spilling to disk (128MB temp)
    → Increase work_mem or add index for ORDER BY clause

ℹ️  Mean execution 2,847ms
    → Run EXPLAIN ANALYZE to identify bottlenecks
```

This connects your dbt model directly to what's happening in Postgres—something neither dbt nor your orchestrator can tell you.

## dbt Integration

Drift works with **both dbt Core and dbt Cloud**. It doesn't run dbt—it analyzes the artifacts dbt produces after runs.

### dbt Core

```bash
# After dbt run completes
drift dbt ingest ./target/ --project my_project
```

### dbt Cloud

Fetch artifacts via the dbt Cloud API, then send to Drift:

```python
import httpx

# Fetch from dbt Cloud
manifest = httpx.get(
    f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/manifest.json",
    headers={"Authorization": f"Token {token}"}
).json()

run_results = httpx.get(
    f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/run_results.json",
    headers={"Authorization": f"Token {token}"}
).json()

# Send to Drift
httpx.post("http://localhost:8000/api/v1/dbt/ingest", json={
    "project_name": "analytics",
    "manifest": manifest,
    "run_results": run_results
})
```

### Orchestrator Integration (Dagster, Airflow, etc.)

Add a post-dbt step to ingest artifacts. Example with Dagster:

```python
from dagster import asset
from dagster_dbt import dbt_assets

@asset(deps=[my_dbt_assets])
def ingest_dbt_to_drift():
    """Capture dbt artifacts after each run."""
    import subprocess
    subprocess.run(["drift", "dbt", "ingest", "target/", "--project", "analytics"])
```

## Requirements

- Python 3.11+
- Docker and Docker Compose
- PostgreSQL 14+ (for monitored databases)

## Local Development Setup

### Start the Development Environment

The project uses Docker Compose to provide two PostgreSQL instances for local development:

- **drift-db** (port 5433): Drift's own storage database
- **target-db** (port 5434): A sample monitored database with `pg_stat_statements` enabled

```bash
# Start both databases
docker compose up -d

# Verify containers are healthy
docker compose ps

# View logs
docker compose logs -f

# Stop containers
docker compose down

# Stop and remove all data (fresh start)
docker compose down -v
```

### Connect to Databases

```bash
# Connect to Drift storage database
docker compose exec drift-db psql -U drift -d drift

# Connect to target monitored database
docker compose exec target-db psql -U postgres -d postgres

# Check pg_stat_statements is working on target
docker compose exec target-db psql -U postgres -d postgres -c "SELECT count(*) FROM pg_stat_statements;"
```

### Connection Strings

For local development, use these connection strings:

```
# Drift storage database (uses psycopg3 via SQLAlchemy)
postgresql+psycopg://drift:drift@localhost:5433/drift

# Target monitored database (uses psycopg3 directly)
postgresql://postgres:postgres@localhost:5434/postgres
```

### Install Drift

```bash
# Create virtual environment and install
pip install -e ".[dev]"

# Or using uv
uv pip install -e ".[dev]"
```

### Initialize Drift

```bash
# Run database migrations
drift db init

# Add the target database for monitoring
drift db add target --dsn "postgresql://postgres:postgres@localhost:5434/postgres"

# Verify it was added
drift db list
```

### Collect Query Stats

```bash
# Run a single collection
drift collect once

# View top queries
drift top
```

## Running Tests

Tests use a separate database (`drift_test`) to avoid conflicts with development data.

```bash
# Ensure Docker containers are running
docker compose up -d

# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_collector/test_snapshot.py -v

# Run tests matching a pattern
pytest -k "test_delta" -v
```

**Note:** If you're setting up for the first time or after `docker compose down -v`, the test database is created automatically by the init script.

## CLI Reference

### Database Management

#### `drift db init`

Runs database migrations to create or update Drift's schema.

```bash
drift db init
```

Creates the tables: `monitored_databases`, `query_stats_raw`, `query_text`, and `query_stats_hourly`. Safe to run multiple times - only applies pending migrations.

#### `drift db add <name> --dsn <dsn>`

Registers a PostgreSQL database for monitoring.

```bash
drift db add production --dsn "postgresql://user:pass@host:5432/dbname"
```

Before saving, this command:
1. Tests the connection to verify the DSN is valid
2. Queries `pg_stat_statements` to confirm the extension is enabled
3. Stores the name and DSN in the registry

The `<name>` is a friendly identifier you choose (e.g., "production", "staging").

#### `drift db list`

Displays all registered databases.

```bash
drift db list
```

Shows name, enabled status, and creation date for each database.

#### `drift db remove <name>`

Removes a database from the registry.

```bash
drift db remove production
drift db remove production --yes  # Skip confirmation prompt
```

Also deletes all collected stats data for that database.

### Stats Collection

#### `drift collect once`

Runs a single collection cycle for all enabled databases.

```bash
drift collect once
drift collect once --database production  # Collect from specific database only
```

For each database, this command:
1. Connects and queries `pg_stat_statements`
2. Calculates deltas from the previous snapshot (changes since last collection)
3. Stores deltas in `query_stats_raw` table
4. Stores/updates query text and fingerprints in `query_text` table

The snapshot state is persisted to the database, so delta calculation works correctly across restarts.

#### `drift collect start`

Starts a continuous collection daemon.

```bash
drift collect start
drift collect start --interval 30  # Collect every 30 seconds
drift collect start --database production
```

| Option | Default | Description |
|--------|---------|-------------|
| `--interval`, `-i` | `60` | Collection interval in seconds |
| `--database`, `-d` | all | Collect from specific database only |

Press Ctrl+C to stop the daemon gracefully.

### Query Analysis

#### `drift top`

Shows the most resource-intensive queries over a time period.

```bash
drift top
drift top --period 7d --sort calls --limit 50
drift top --database production
```

| Option | Default | Description |
|--------|---------|-------------|
| `--period`, `-p` | `24h` | Time window: `1h`, `24h`, `7d`, `1w` |
| `--sort`, `-s` | `total_time` | Sort by: `total_time`, `calls`, `mean_time` |
| `--limit`, `-n` | `20` | Number of results to show |
| `--database`, `-d` | all | Filter to specific database |

#### `drift show <queryid>`

Shows detailed information about a specific query.

```bash
drift show 1234567890
drift show 1234567890 --database production --period 7d
```

Displays:
- Query metadata (type, tables, first/last seen)
- Aggregated stats (calls, time, rows, cache hit ratio)
- Full query text
- SQL fingerprint

| Option | Default | Description |
|--------|---------|-------------|
| `--period`, `-p` | `24h` | Time period for stats |
| `--database`, `-d` | all | Filter to specific database |

#### `drift trend <queryid>`

Shows query performance trend over time with an ASCII chart.

```bash
drift trend 1234567890
drift trend 1234567890 --period 24h --metric calls
```

| Option | Default | Description |
|--------|---------|-------------|
| `--period`, `-p` | `7d` | Time period to analyze |
| `--metric`, `-m` | `mean_time` | Metric to chart: `calls`, `total_time`, `mean_time` |
| `--database`, `-d` | all | Filter to specific database |

### Maintenance

#### `drift prune`

Deletes old data based on retention settings.

```bash
drift prune
drift prune --raw-days 14 --hourly-days 180
drift prune --yes  # Skip confirmation
```

| Option | Default | Description |
|--------|---------|-------------|
| `--raw-days` | `7` | Keep raw stats for this many days |
| `--hourly-days` | `90` | Keep hourly stats for this many days |
| `--yes`, `-y` | false | Skip confirmation prompt |

#### `drift rollup`

Aggregates raw stats into hourly buckets for efficient long-term storage.

```bash
drift rollup
drift rollup --hours 48  # Process last 48 hours
```

Safe to run multiple times - uses upsert semantics.

#### `drift new-queries`

Shows queries that first appeared recently.

```bash
drift new-queries
drift new-queries --since 7d --limit 100
```

| Option | Default | Description |
|--------|---------|-------------|
| `--since`, `-s` | `24h` | Time window to check |
| `--limit`, `-n` | `50` | Maximum results |
| `--database`, `-d` | all | Filter to specific database |

### Alerting

#### `drift alerts rules`

Lists all configured alert rules.

```bash
drift alerts rules
```

#### `drift alerts add <name>`

Creates a new alert rule.

```bash
drift alerts add "High Latency" --type latency_increase --threshold 50
drift alerts add "Cache Problem" --type cache_drop --threshold 10 --webhook https://hooks.slack.com/...
```

| Option | Required | Description |
|--------|----------|-------------|
| `--type`, `-t` | yes | Rule type: `latency_increase`, `cache_drop`, `temp_disk` |
| `--threshold`, `-T` | no | Threshold value (default: 50) |
| `--webhook`, `-w` | no | Webhook URL for notifications |

#### `drift alerts remove <rule_id>`

Removes an alert rule.

```bash
drift alerts remove 1
drift alerts remove 1 --yes  # Skip confirmation
```

#### `drift alerts events`

Lists triggered alert events.

```bash
drift alerts events
drift alerts events --unack  # Only unacknowledged
drift alerts events --limit 50
```

#### `drift alerts ack <event_id>`

Acknowledges an alert event.

```bash
drift alerts ack 1
```

#### `drift alerts check`

Runs anomaly detection against configured rules.

```bash
drift alerts check
drift alerts check --database production
drift alerts check --no-notify  # Skip webhook notifications
```

Compares recent query performance against 7-day baseline and creates alert events for any anomalies.

## Data Privacy

Drift collects query performance metrics from `pg_stat_statements`. Here's what gets stored:

**Stored locally in your Drift database:**
- Query text with parameters normalized (e.g., `WHERE id = $1` not `WHERE id = 42`)
- Table and column names referenced in queries
- Performance metrics (execution time, row counts, cache hits)
- Timestamps of when queries were observed

**NOT stored:**
- Actual parameter values (PostgreSQL normalizes these before Drift sees them)
- Query results or row data
- Database credentials

All data stays on your infrastructure. Drift has no telemetry, external API calls, or data collection.

**Data deletion:** Running `drift db remove <name>` permanently deletes all collected data for that database, including query stats, query text, and snapshot state.

**Note:** Query text may reveal schema information and access patterns. This is standard for database monitoring tools and is generally safe for internal infrastructure use.

## Configuration

Drift can be configured via environment variables or a `drift.toml` file:

```toml
[storage]
dsn = "postgresql://drift:drift@localhost:5433/drift"

[collector]
interval_seconds = 60
snapshot_timeout_seconds = 10

[retention]
raw_days = 7
hourly_days = 90
```

Environment variables use the prefix `DRIFT_` with double underscores for nesting:

```bash
export DRIFT_STORAGE__DSN="postgresql://drift:drift@localhost:5433/drift"
export DRIFT_COLLECTOR__INTERVAL_SECONDS=60
```
