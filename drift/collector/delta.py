"""Delta calculation between consecutive snapshots."""

from dataclasses import dataclass

from drift.collector.snapshot import Snapshot, PgStatStatementsRow


@dataclass
class DeltaRow:
    """Calculated delta for a single query between two snapshots."""

    queryid: int
    query: str
    calls_delta: int
    total_exec_time_delta: float
    rows_delta: int
    shared_blks_hit_delta: int
    shared_blks_read_delta: int
    temp_blks_read_delta: int
    temp_blks_written_delta: int


def calculate_deltas(previous: Snapshot | None, current: Snapshot) -> list[DeltaRow]:
    """Calculate deltas between two snapshots.

    If previous is None (first collection), returns current values as deltas.

    Handles counter resets: if a delta would be negative (counter reset due to
    pg_stat_statements_reset() or server restart), we use the current absolute
    value as the delta instead.
    """
    if previous is None:
        # First snapshot - treat current values as deltas
        return [
            DeltaRow(
                queryid=row.queryid,
                query=row.query,
                calls_delta=row.calls,
                total_exec_time_delta=row.total_exec_time,
                rows_delta=row.rows,
                shared_blks_hit_delta=row.shared_blks_hit,
                shared_blks_read_delta=row.shared_blks_read,
                temp_blks_read_delta=row.temp_blks_read,
                temp_blks_written_delta=row.temp_blks_written,
            )
            for row in current.rows
        ]

    # Build lookup of previous snapshot by queryid
    prev_by_id = {row.queryid: row for row in previous.rows}

    deltas = []
    for curr_row in current.rows:
        prev_row = prev_by_id.get(curr_row.queryid)

        if prev_row is None:
            # New query since last snapshot - use current as delta
            deltas.append(
                DeltaRow(
                    queryid=curr_row.queryid,
                    query=curr_row.query,
                    calls_delta=curr_row.calls,
                    total_exec_time_delta=curr_row.total_exec_time,
                    rows_delta=curr_row.rows,
                    shared_blks_hit_delta=curr_row.shared_blks_hit,
                    shared_blks_read_delta=curr_row.shared_blks_read,
                    temp_blks_read_delta=curr_row.temp_blks_read,
                    temp_blks_written_delta=curr_row.temp_blks_written,
                )
            )
        else:
            # Calculate deltas, handling counter resets
            calls_delta = _safe_delta(curr_row.calls, prev_row.calls)
            time_delta = _safe_delta(curr_row.total_exec_time, prev_row.total_exec_time)
            rows_delta = _safe_delta(curr_row.rows, prev_row.rows)
            hit_delta = _safe_delta(curr_row.shared_blks_hit, prev_row.shared_blks_hit)
            read_delta = _safe_delta(curr_row.shared_blks_read, prev_row.shared_blks_read)
            temp_read_delta = _safe_delta(curr_row.temp_blks_read, prev_row.temp_blks_read)
            temp_write_delta = _safe_delta(curr_row.temp_blks_written, prev_row.temp_blks_written)

            # Only include if there was any activity
            if calls_delta > 0:
                deltas.append(
                    DeltaRow(
                        queryid=curr_row.queryid,
                        query=curr_row.query,
                        calls_delta=calls_delta,
                        total_exec_time_delta=time_delta,
                        rows_delta=rows_delta,
                        shared_blks_hit_delta=hit_delta,
                        shared_blks_read_delta=read_delta,
                        temp_blks_read_delta=temp_read_delta,
                        temp_blks_written_delta=temp_write_delta,
                    )
                )

    return deltas


def _safe_delta(current: int | float, previous: int | float) -> int | float:
    """Calculate delta, handling counter resets.

    If delta would be negative (counter was reset), return current value.
    """
    delta = current - previous
    if delta < 0:
        # Counter reset - use current absolute value
        return current
    return delta


def calculate_deltas_from_state(
    previous_state: dict[int, dict],
    current_rows: list[PgStatStatementsRow],
) -> list[DeltaRow]:
    """Calculate deltas using persisted state from database.

    Args:
        previous_state: Dict mapping queryid -> {calls, total_exec_time, ...}
                       from the snapshot_state table
        current_rows: Current snapshot rows from pg_stat_statements

    Returns:
        List of DeltaRow with calculated deltas
    """
    if not previous_state:
        # First snapshot - treat current values as deltas
        return [
            DeltaRow(
                queryid=row.queryid,
                query=row.query,
                calls_delta=row.calls,
                total_exec_time_delta=row.total_exec_time,
                rows_delta=row.rows,
                shared_blks_hit_delta=row.shared_blks_hit,
                shared_blks_read_delta=row.shared_blks_read,
                temp_blks_read_delta=row.temp_blks_read,
                temp_blks_written_delta=row.temp_blks_written,
            )
            for row in current_rows
        ]

    deltas = []
    for curr_row in current_rows:
        prev = previous_state.get(curr_row.queryid)

        if prev is None:
            # New query since last snapshot
            deltas.append(
                DeltaRow(
                    queryid=curr_row.queryid,
                    query=curr_row.query,
                    calls_delta=curr_row.calls,
                    total_exec_time_delta=curr_row.total_exec_time,
                    rows_delta=curr_row.rows,
                    shared_blks_hit_delta=curr_row.shared_blks_hit,
                    shared_blks_read_delta=curr_row.shared_blks_read,
                    temp_blks_read_delta=curr_row.temp_blks_read,
                    temp_blks_written_delta=curr_row.temp_blks_written,
                )
            )
        else:
            # Calculate deltas
            calls_delta = _safe_delta(curr_row.calls, prev["calls"])
            time_delta = _safe_delta(curr_row.total_exec_time, prev["total_exec_time"])
            rows_delta = _safe_delta(curr_row.rows, prev["rows"])
            hit_delta = _safe_delta(curr_row.shared_blks_hit, prev["shared_blks_hit"])
            read_delta = _safe_delta(curr_row.shared_blks_read, prev["shared_blks_read"])
            temp_read_delta = _safe_delta(curr_row.temp_blks_read, prev["temp_blks_read"])
            temp_write_delta = _safe_delta(curr_row.temp_blks_written, prev["temp_blks_written"])

            # Only include if there was any activity
            if calls_delta > 0:
                deltas.append(
                    DeltaRow(
                        queryid=curr_row.queryid,
                        query=curr_row.query,
                        calls_delta=calls_delta,
                        total_exec_time_delta=time_delta,
                        rows_delta=rows_delta,
                        shared_blks_hit_delta=hit_delta,
                        shared_blks_read_delta=read_delta,
                        temp_blks_read_delta=temp_read_delta,
                        temp_blks_written_delta=temp_write_delta,
                    )
                )

    return deltas
