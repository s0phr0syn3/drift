"""Parse dbt manifest.json and run_results.json artifacts."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class DbtModel:
    """Parsed dbt model from manifest.json."""

    unique_id: str  # e.g., "model.my_project.fct_orders"
    name: str  # e.g., "fct_orders"
    schema_name: str | None  # e.g., "analytics"
    database: str | None  # e.g., "warehouse"
    alias: str | None  # Table/view name if different from model name
    materialized: str  # table, view, incremental, ephemeral
    compiled_sql: str | None  # The actual SQL that runs
    depends_on: list[str] = field(default_factory=list)  # upstream model unique_ids
    tags: list[str] = field(default_factory=list)
    description: str | None = None

    @property
    def relation_name(self) -> str | None:
        """Get the full relation name (schema.table)."""
        table_name = self.alias or self.name
        if self.schema_name:
            return f"{self.schema_name}.{table_name}"
        return table_name


@dataclass
class DbtModelExecution:
    """Parsed model execution from run_results.json."""

    unique_id: str
    status: str  # success, error, skipped
    execution_time: float  # seconds
    rows_affected: int | None
    adapter_response: dict = field(default_factory=dict)


@dataclass
class DbtRunResult:
    """Parsed dbt run results."""

    invocation_id: str
    started_at: datetime
    completed_at: datetime
    status: str  # success, error, partial
    executions: list[DbtModelExecution] = field(default_factory=list)


def parse_manifest(manifest_path: str | Path) -> dict[str, DbtModel]:
    """Parse dbt manifest.json file.

    Args:
        manifest_path: Path to manifest.json

    Returns:
        Dict mapping unique_id to DbtModel
    """
    manifest_path = Path(manifest_path)
    with open(manifest_path) as f:
        manifest = json.load(f)

    models = {}

    # Parse nodes - models are in the nodes dict with resource_type == 'model'
    nodes = manifest.get("nodes", {})
    for unique_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue

        # Extract depends_on model refs (not sources)
        depends_on = []
        for dep in node.get("depends_on", {}).get("nodes", []):
            if dep.startswith("model."):
                depends_on.append(dep)

        model = DbtModel(
            unique_id=unique_id,
            name=node.get("name", ""),
            schema_name=node.get("schema"),
            database=node.get("database"),
            alias=node.get("alias"),
            materialized=node.get("config", {}).get("materialized", "view"),
            compiled_sql=node.get("compiled_code") or node.get("compiled_sql"),
            depends_on=depends_on,
            tags=node.get("tags", []),
            description=node.get("description"),
        )
        models[unique_id] = model

    return models


def parse_run_results(run_results_path: str | Path) -> DbtRunResult:
    """Parse dbt run_results.json file.

    Args:
        run_results_path: Path to run_results.json

    Returns:
        DbtRunResult with execution details
    """
    run_results_path = Path(run_results_path)
    with open(run_results_path) as f:
        results = json.load(f)

    # Parse metadata
    metadata = results.get("metadata", {})
    invocation_id = metadata.get("invocation_id", "")

    # Parse timing
    # dbt uses ISO format timestamps
    elapsed_time = results.get("elapsed_time", 0)

    # Try to get timestamps from args or use current time
    args = results.get("args", {})

    # Parse individual results
    executions = []
    for result in results.get("results", []):
        unique_id = result.get("unique_id", "")

        # Skip non-model results (tests, seeds, etc.)
        if not unique_id.startswith("model."):
            continue

        # Extract rows affected from adapter response
        adapter_response = result.get("adapter_response", {})
        rows_affected = adapter_response.get("rows_affected")

        # Handle different dbt versions - some use 'execution_time', others 'timing'
        exec_time = result.get("execution_time", 0)
        if not exec_time and "timing" in result:
            # Sum up timing entries
            for timing in result.get("timing", []):
                if timing.get("name") == "execute":
                    started = timing.get("started_at", "")
                    completed = timing.get("completed_at", "")
                    if started and completed:
                        try:
                            start_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                            end_dt = datetime.fromisoformat(completed.replace("Z", "+00:00"))
                            exec_time = (end_dt - start_dt).total_seconds()
                        except (ValueError, TypeError):
                            pass

        execution = DbtModelExecution(
            unique_id=unique_id,
            status=result.get("status", "unknown"),
            execution_time=exec_time,
            rows_affected=rows_affected,
            adapter_response=adapter_response,
        )
        executions.append(execution)

    # Determine overall status
    statuses = [e.status for e in executions]
    if all(s == "success" for s in statuses):
        overall_status = "success"
    elif any(s == "error" for s in statuses):
        overall_status = "error"
    else:
        overall_status = "partial"

    # Use current time if we can't parse timestamps
    from datetime import timezone
    now = datetime.now(timezone.utc)

    return DbtRunResult(
        invocation_id=invocation_id,
        started_at=now,  # Will be updated when we have proper timestamps
        completed_at=now,
        status=overall_status,
        executions=executions,
    )


def parse_dbt_target(target_dir: str | Path) -> tuple[dict[str, DbtModel], DbtRunResult | None]:
    """Parse both manifest.json and run_results.json from a dbt target directory.

    Args:
        target_dir: Path to dbt target/ directory

    Returns:
        Tuple of (models dict, run results or None)
    """
    target_dir = Path(target_dir)

    # Parse manifest (required)
    manifest_path = target_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found in {target_dir}")

    models = parse_manifest(manifest_path)

    # Parse run results (optional - may not exist if just compiled)
    run_results_path = target_dir / "run_results.json"
    run_results = None
    if run_results_path.exists():
        run_results = parse_run_results(run_results_path)

    return models, run_results
