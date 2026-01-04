# Drift

PostgreSQL query performance analyzer with dbt integration.

Drift collects query performance metrics from `pg_stat_statements` and correlates them with dbt model definitions to provide unified performance observability.

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

## Project Structure

See `CLAUDE.md` for detailed project structure and implementation phases.
