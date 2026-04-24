"""Tests for custom_components.history_editor.statistics.

Focuses on:
- The recalc helpers (``recalculate_short_term_stat``, ``recalculate_long_term_stat``).
- The sum cascade for ``total_increasing`` sensors.
- The cross-phase ordering invariant in ``update_statistics_after_state_change``
  and ``recalculate_statistics_sync`` (the bug that motivated the session
  flush/expire between short-term and long-term passes).
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("homeassistant.components.recorder.db_schema")

from homeassistant.components.recorder.db_schema import (  # noqa: E402
    States,
    Statistics,
    StatisticsShortTerm,
)

from custom_components.history_editor.statistics import (  # noqa: E402
    _cascade_sum_adjustment,
    recalculate_long_term_stat,
    recalculate_short_term_stat,
    recalculate_statistics_sync,
    update_statistics_after_state_change,
)


def _add_state(session, metadata_id: int, ts: float, state: str) -> States:
    """Insert a States row at ``ts`` and return it."""
    s = States(
        metadata_id=metadata_id,
        state=state,
        attributes="{}",
        last_updated_ts=ts,
        last_changed_ts=ts,
    )
    session.add(s)
    session.flush()
    return s


def _add_short_term(session, metadata_id: int, start_ts: float, **cols) -> StatisticsShortTerm:
    """Insert a StatisticsShortTerm row and return it."""
    row = StatisticsShortTerm(metadata_id=metadata_id, start_ts=start_ts, **cols)
    session.add(row)
    session.flush()
    return row


def _add_long_term(session, metadata_id: int, start_ts: float, **cols) -> Statistics:
    """Insert a Statistics (hourly) row and return it."""
    row = Statistics(metadata_id=metadata_id, start_ts=start_ts, **cols)
    session.add(row)
    session.flush()
    return row


# --------------------------------------------------------------------------
# recalculate_short_term_stat
# --------------------------------------------------------------------------


class TestRecalculateShortTermStat:
    def test_computes_mean_min_max_from_numeric_states(self, db_session, sample_entity):
        states_meta_id, stat_meta_id, entity_id = sample_entity
        start = 1_700_000_000.0  # aligned-ish; exact alignment not required for this helper
        _add_state(db_session, states_meta_id, start + 10, "1.0")
        _add_state(db_session, states_meta_id, start + 60, "3.0")
        _add_state(db_session, states_meta_id, start + 200, "5.0")
        row = _add_short_term(
            db_session, stat_meta_id, start, mean=99.0, min=99.0, max=99.0, state=99.0,
        )

        updated = recalculate_short_term_stat(db_session, stat_meta_id, entity_id, start)

        assert updated is True
        assert row.mean == pytest.approx((1.0 + 3.0 + 5.0) / 3)
        assert row.min == 1.0
        assert row.max == 5.0
        assert row.state == 5.0  # last value in the window

    def test_ignores_non_numeric_states(self, db_session, sample_entity):
        states_meta_id, stat_meta_id, entity_id = sample_entity
        start = 1_700_000_000.0
        _add_state(db_session, states_meta_id, start + 10, "unavailable")
        _add_state(db_session, states_meta_id, start + 60, "2.0")
        _add_state(db_session, states_meta_id, start + 200, "unknown")
        row = _add_short_term(db_session, stat_meta_id, start, mean=99.0, min=99.0, max=99.0, state=99.0)

        recalculate_short_term_stat(db_session, stat_meta_id, entity_id, start)


        assert row.mean == 2.0
        assert row.state == 2.0

    def test_empty_window_carries_forward_prior_value(self, db_session, sample_entity):
        """No states in this window → hold-last-value from before the window."""
        states_meta_id, stat_meta_id, entity_id = sample_entity
        start = 1_700_000_000.0
        # Only a prior state, none inside [start, start+300)
        _add_state(db_session, states_meta_id, start - 60, "7.5")
        row = _add_short_term(db_session, stat_meta_id, start, mean=99.0, min=99.0, max=99.0, state=99.0)

        recalculate_short_term_stat(db_session, stat_meta_id, entity_id, start)


        assert row.mean == 7.5
        assert row.min == 7.5
        assert row.max == 7.5
        assert row.state == 7.5

    def test_empty_window_no_prior_value_deletes_row(self, db_session, sample_entity):
        """No states in the window AND no prior numeric → row removed."""
        states_meta_id, stat_meta_id, entity_id = sample_entity
        start = 1_700_000_000.0
        row = _add_short_term(db_session, stat_meta_id, start, mean=99.0, state=99.0)
        row_id = row.id

        recalculate_short_term_stat(db_session, stat_meta_id, entity_id, start)
        db_session.flush()

        assert db_session.query(StatisticsShortTerm).filter_by(id=row_id).first() is None

    def test_returns_false_when_stat_row_missing(self, db_session, sample_entity):
        """Helper never creates new rows; returns False when target is absent."""
        states_meta_id, stat_meta_id, entity_id = sample_entity
        _add_state(db_session, states_meta_id, 1_700_000_010.0, "5.0")

        result = recalculate_short_term_stat(db_session, stat_meta_id, entity_id, 1_700_000_000.0)

        assert result is False


# --------------------------------------------------------------------------
# _cascade_sum_adjustment
# --------------------------------------------------------------------------


class TestCascadeSumAdjustment:
    def test_bumps_forward_short_term_rows_only(self, db_session, sample_totaliser):
        """Adjustment at t=T updates rows with start_ts >= T, leaves earlier rows alone."""
        _, stat_meta_id, _ = sample_totaliser
        r_before = _add_short_term(db_session, stat_meta_id, start_ts=100.0, sum=10.0)
        r_at = _add_short_term(db_session, stat_meta_id, start_ts=300.0, sum=20.0)
        r_after = _add_short_term(db_session, stat_meta_id, start_ts=600.0, sum=30.0)

        _cascade_sum_adjustment(db_session, stat_meta_id, 300.0, delta=5.0)
        db_session.expire_all()

        assert db_session.query(StatisticsShortTerm).get(r_before.id).sum == 10.0
        assert db_session.query(StatisticsShortTerm).get(r_at.id).sum == 25.0
        assert db_session.query(StatisticsShortTerm).get(r_after.id).sum == 35.0

    def test_cascades_to_long_term_floored_to_containing_hour(self, db_session, sample_totaliser):
        """A short-term cascade at t=03:05 bumps the hourly row at t=03:00 and onwards."""
        _, stat_meta_id, _ = sample_totaliser
        hour_before = _add_long_term(db_session, stat_meta_id, start_ts=3600.0 * 2, sum=100.0)
        hour_at = _add_long_term(db_session, stat_meta_id, start_ts=3600.0 * 3, sum=200.0)
        hour_after = _add_long_term(db_session, stat_meta_id, start_ts=3600.0 * 4, sum=300.0)

        # Short-term start at 03:05:00 → containing hour is 03:00:00 = 10800
        _cascade_sum_adjustment(db_session, stat_meta_id, 3600.0 * 3 + 300.0, delta=7.0)
        db_session.expire_all()

        assert db_session.query(Statistics).get(hour_before.id).sum == 100.0
        assert db_session.query(Statistics).get(hour_at.id).sum == 207.0
        assert db_session.query(Statistics).get(hour_after.id).sum == 307.0

    def test_zero_delta_is_noop(self, db_session, sample_totaliser):
        _, stat_meta_id, _ = sample_totaliser
        row = _add_short_term(db_session, stat_meta_id, start_ts=300.0, sum=20.0)

        _cascade_sum_adjustment(db_session, stat_meta_id, 300.0, delta=0.0)


        assert row.sum == 20.0


# --------------------------------------------------------------------------
# recalculate_long_term_stat
# --------------------------------------------------------------------------


class TestRecalculateLongTermStat:
    def test_aggregates_twelve_short_term_rows(self, db_session, sample_entity):
        _, stat_meta_id, _ = sample_entity
        hour_start = 3600.0 * 100
        for i in range(12):
            _add_short_term(
                db_session, stat_meta_id,
                start_ts=hour_start + i * 300,
                mean=float(i + 1), min=float(i), max=float(i + 2), state=float(i + 1),
            )
        long_row = _add_long_term(
            db_session, stat_meta_id, hour_start, mean=999.0, min=999.0, max=999.0, state=999.0,
        )

        updated = recalculate_long_term_stat(db_session, stat_meta_id, hour_start)

        assert updated is True

        assert long_row.mean == pytest.approx(sum(range(1, 13)) / 12)
        assert long_row.min == 0.0
        assert long_row.max == 13.0
        assert long_row.state == 12.0  # last short-term state

    def test_mirrors_last_short_term_sum_for_totaliser(self, db_session, sample_totaliser):
        _, stat_meta_id, _ = sample_totaliser
        hour_start = 3600.0 * 100
        _add_short_term(db_session, stat_meta_id, start_ts=hour_start, state=1.0, sum=10.0)
        _add_short_term(db_session, stat_meta_id, start_ts=hour_start + 300, state=2.0, sum=20.0)
        _add_short_term(db_session, stat_meta_id, start_ts=hour_start + 600, state=3.0, sum=35.0)
        long_row = _add_long_term(db_session, stat_meta_id, hour_start, state=0.0, sum=0.0)

        recalculate_long_term_stat(db_session, stat_meta_id, hour_start)


        assert long_row.sum == 35.0  # the LAST short-term sum in the hour, not the average

    def test_empty_hour_carries_forward_prior_short_term(self, db_session, sample_entity):
        _, stat_meta_id, _ = sample_entity
        hour_start = 3600.0 * 100
        # Prior-hour short-term with a real state
        _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start - 300,
            mean=4.0, min=3.0, max=5.0, state=4.2,
        )
        long_row = _add_long_term(
            db_session, stat_meta_id, hour_start, mean=999.0, min=999.0, max=999.0, state=999.0,
        )

        recalculate_long_term_stat(db_session, stat_meta_id, hour_start)


        assert long_row.state == 4.2
        assert long_row.mean == 4.0

    def test_empty_hour_preserves_existing_sum(self, db_session, sample_totaliser):
        """Per the 'sum is intentionally not touched' invariant — hold-last-value
        for mean/min/max/state but leave sum alone."""
        _, stat_meta_id, _ = sample_totaliser
        hour_start = 3600.0 * 100
        _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start - 300,
            mean=4.0, min=3.0, max=5.0, state=4.2, sum=99.0,
        )
        long_row = _add_long_term(
            db_session, stat_meta_id, hour_start, state=0.0, sum=777.0,
        )

        recalculate_long_term_stat(db_session, stat_meta_id, hour_start)


        assert long_row.state == 4.2
        assert long_row.sum == 777.0  # preserved


# --------------------------------------------------------------------------
# Cross-phase ordering — the bug that motivated the session.flush/expire_all
# between short-term and long-term recalculation passes
# --------------------------------------------------------------------------


class TestPhaseBoundaryConsistency:
    """These are the regression tests for the fix applied to
    ``recalculate_statistics_sync`` (flush/expire between phases) and the
    ``update_statistics_after_state_change`` helper.  The scenario:

    1. A totaliser sensor has short-term rows with cascaded sums.
    2. Recalc mutates short-term ``sum`` via the cascade (bulk UPDATE with
       ``synchronize_session=False``, which bypasses the ORM identity map).
    3. If the long-term pass reads ``short_term.sum`` from stale cached ORM
       objects, it writes the wrong value to the hourly row.
    """

    def test_recalculate_sync_keeps_long_term_sum_consistent_with_short_term(
        self, db_session, mock_hass, sample_totaliser,
    ):
        """After recalc, the long-term sum should equal the last short-term sum
        in the hour (the invariant enforced by ``recalculate_long_term_stat``)."""
        _, stat_meta_id, entity_id = sample_totaliser
        hour_start = 3600.0 * 100

        # Seed 12 short-term rows with sums 10, 20, ..., 120 and states 1, 2, ..., 12
        for i in range(12):
            _add_short_term(
                db_session, stat_meta_id,
                start_ts=hour_start + i * 300,
                state=float(i + 1), sum=float((i + 1) * 10),
            )
        # Seed states in the first period that will cause a delta cascade
        # (state at end of first period is 2.0, current short-term state is 1.0 → delta +1)
        _add_state(db_session, mock_hass_states_meta_id(db_session, entity_id),
                   hour_start + 10, "0.5")
        _add_state(db_session, mock_hass_states_meta_id(db_session, entity_id),
                   hour_start + 200, "2.0")

        # Seed hourly row
        long_row = _add_long_term(db_session, stat_meta_id, hour_start, state=0.0, sum=0.0)

        # Call the top-level recalc; it commits internally so we don't need to
        start_time = datetime.fromtimestamp(hour_start, tz=timezone.utc)
        end_time = datetime.fromtimestamp(hour_start + 3600, tz=timezone.utc)
        result = recalculate_statistics_sync(
            mock_hass, entity_id, start_time, end_time, statistic_type="both",
        )

        assert result["success"] is True
        db_session.expire_all()
        long_row = db_session.query(Statistics).filter_by(id=long_row.id).first()

        # The long-term sum must equal the LAST short-term sum in the hour
        # (recalculate_long_term_stat mirrors sums[-1]).  If the bug were still
        # present, the long-term sum would be computed from stale cached
        # short-term rows that predated the cascade and would disagree with the
        # actual last short-term sum.
        last_short_term_sum = (
            db_session.query(StatisticsShortTerm)
            .filter(StatisticsShortTerm.metadata_id == stat_meta_id)
            .filter(StatisticsShortTerm.start_ts >= hour_start)
            .filter(StatisticsShortTerm.start_ts < hour_start + 3600)
            .order_by(StatisticsShortTerm.start_ts.desc())
            .first()
            .sum
        )
        assert long_row.sum == last_short_term_sum

    def test_update_statistics_after_state_change_handles_period_move(
        self, db_session, sample_entity,
    ):
        """When a state record moves between periods, both the old and new
        5-min buckets should be recalculated."""
        states_meta_id, stat_meta_id, entity_id = sample_entity
        old_period = 1_700_000_000.0 - (1_700_000_000.0 % 300)  # align to 5-min boundary
        new_period = old_period + 300.0

        # Seed states: one in the new period
        state = _add_state(db_session, states_meta_id, new_period + 60, "42.0")

        # Both periods have existing stat rows
        old_row = _add_short_term(
            db_session, stat_meta_id, start_ts=old_period,
            mean=5.0, min=5.0, max=5.0, state=5.0,
        )
        new_row = _add_short_term(
            db_session, stat_meta_id, start_ts=new_period,
            mean=0.0, min=0.0, max=0.0, state=0.0,
        )

        # Call helper: state moved from old_period → new_period
        update_statistics_after_state_change(
            db_session, state, old_ts=old_period + 60, new_last_updated=None,
        )
        db_session.flush()
        db_session.expire_all()

        # Old period now empty, no prior value → row deleted
        assert db_session.query(StatisticsShortTerm).filter_by(id=old_row.id).first() is None
        # New period recalculated from the state
        new_row_fresh = db_session.query(StatisticsShortTerm).filter_by(id=new_row.id).first()
        assert new_row_fresh.mean == 42.0
        assert new_row_fresh.state == 42.0


def mock_hass_states_meta_id(session, entity_id):
    """Helper for the cross-phase test — look up the StatesMeta id by entity_id."""
    from homeassistant.components.recorder.db_schema import StatesMeta
    return session.query(StatesMeta).filter_by(entity_id=entity_id).first().metadata_id
