"""Tests for delta calculation."""

import pytest
from datetime import datetime, timezone

from drift.collector.delta import calculate_deltas, _safe_delta
from drift.collector.snapshot import Snapshot, PgStatStatementsRow


def make_row(queryid: int, calls: int, total_time: float = 100.0) -> PgStatStatementsRow:
    """Helper to create a test row."""
    return PgStatStatementsRow(
        queryid=queryid,
        query=f"SELECT * FROM table_{queryid}",
        calls=calls,
        total_exec_time=total_time,
        rows=calls * 10,
        shared_blks_hit=calls * 100,
        shared_blks_read=calls * 10,
        temp_blks_read=0,
        temp_blks_written=0,
    )


class TestSafeDelta:
    """Tests for _safe_delta helper."""

    def test_normal_delta(self):
        assert _safe_delta(100, 50) == 50

    def test_zero_delta(self):
        assert _safe_delta(100, 100) == 0

    def test_counter_reset(self):
        # Current < previous means counter was reset
        # Should return current value
        assert _safe_delta(10, 100) == 10

    def test_float_delta(self):
        assert _safe_delta(100.5, 50.2) == pytest.approx(50.3)


class TestCalculateDeltas:
    """Tests for calculate_deltas function."""

    def test_first_snapshot_no_previous(self):
        """First snapshot with no previous data should use current as delta."""
        current = Snapshot(
            timestamp=datetime.now(timezone.utc),
            rows=[make_row(1, calls=100), make_row(2, calls=50)],
        )

        deltas = calculate_deltas(None, current)

        assert len(deltas) == 2
        assert deltas[0].queryid == 1
        assert deltas[0].calls_delta == 100
        assert deltas[1].queryid == 2
        assert deltas[1].calls_delta == 50

    def test_normal_delta_calculation(self):
        """Normal case: calculate difference between snapshots."""
        previous = Snapshot(
            timestamp=datetime.now(timezone.utc),
            rows=[make_row(1, calls=100)],
        )
        current = Snapshot(
            timestamp=datetime.now(timezone.utc),
            rows=[make_row(1, calls=150)],
        )

        deltas = calculate_deltas(previous, current)

        assert len(deltas) == 1
        assert deltas[0].calls_delta == 50

    def test_new_query(self):
        """Query that appears in current but not previous."""
        previous = Snapshot(
            timestamp=datetime.now(timezone.utc),
            rows=[make_row(1, calls=100)],
        )
        current = Snapshot(
            timestamp=datetime.now(timezone.utc),
            rows=[make_row(1, calls=150), make_row(2, calls=25)],
        )

        deltas = calculate_deltas(previous, current)

        assert len(deltas) == 2
        # Query 1: normal delta
        delta_1 = next(d for d in deltas if d.queryid == 1)
        assert delta_1.calls_delta == 50
        # Query 2: new, uses current as delta
        delta_2 = next(d for d in deltas if d.queryid == 2)
        assert delta_2.calls_delta == 25

    def test_removed_query(self):
        """Query that was in previous but not in current (e.g., evicted)."""
        previous = Snapshot(
            timestamp=datetime.now(timezone.utc),
            rows=[make_row(1, calls=100), make_row(2, calls=50)],
        )
        current = Snapshot(
            timestamp=datetime.now(timezone.utc),
            rows=[make_row(1, calls=150)],  # Query 2 is gone
        )

        deltas = calculate_deltas(previous, current)

        # Query 2 is not in output (we don't track removals)
        assert len(deltas) == 1
        assert deltas[0].queryid == 1

    def test_counter_reset(self):
        """Counter reset: current < previous should use current as delta."""
        previous = Snapshot(
            timestamp=datetime.now(timezone.utc),
            rows=[make_row(1, calls=1000)],
        )
        current = Snapshot(
            timestamp=datetime.now(timezone.utc),
            rows=[make_row(1, calls=50)],  # Counter was reset
        )

        deltas = calculate_deltas(previous, current)

        assert len(deltas) == 1
        assert deltas[0].calls_delta == 50  # Uses current, not negative

    def test_no_activity(self):
        """Query with no new calls should be excluded."""
        previous = Snapshot(
            timestamp=datetime.now(timezone.utc),
            rows=[make_row(1, calls=100)],
        )
        current = Snapshot(
            timestamp=datetime.now(timezone.utc),
            rows=[make_row(1, calls=100)],  # Same as before
        )

        deltas = calculate_deltas(previous, current)

        # No delta because calls_delta == 0
        assert len(deltas) == 0
