"""CRUD tests for state history records.

Exercises the sync helpers in ``custom_components/history_editor/__init__.py``
(``_get_records_sync``, ``_update_record_sync``, ``_delete_record_sync``,
``_create_record_sync``) against the in-memory SQLite DB provided by conftest.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("homeassistant.components.recorder.db_schema")

from homeassistant.components.recorder.db_schema import (  # noqa: E402
    States,
    StatesMeta,
    Statistics,
    StatisticsShortTerm,
)

from custom_components.history_editor import (  # noqa: E402
    _create_record_sync,
    _delete_record_sync,
    _get_records_sync,
    _update_record_sync,
)


def _add_state(session, metadata_id: int, ts: float, state: str,
               attributes: dict | None = None) -> States:
    s = States(
        metadata_id=metadata_id,
        state=state,
        attributes=json.dumps(attributes or {}),
        last_updated_ts=ts,
        last_changed_ts=ts,
    )
    session.add(s)
    session.flush()
    return s


# --------------------------------------------------------------------------
# _get_records_sync
# --------------------------------------------------------------------------


class TestGetRecordsSync:
    def test_returns_all_records_for_entity_newest_first(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, _, entity_id = sample_entity
        _add_state(db_session, states_meta_id, 1_700_000_000, "1.0")
        _add_state(db_session, states_meta_id, 1_700_000_100, "2.0")
        _add_state(db_session, states_meta_id, 1_700_000_200, "3.0")

        result = _get_records_sync(mock_hass, entity_id, None, None, limit=100)

        assert result["success"] is True
        assert len(result["records"]) == 3
        # Newest-first ordering
        states = [r["state"] for r in result["records"]]
        assert states == ["3.0", "2.0", "1.0"]
        assert result["has_more"] is False

    def test_respects_limit_and_reports_has_more(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, _, entity_id = sample_entity
        for i in range(5):
            _add_state(db_session, states_meta_id, 1_700_000_000 + i, str(i))

        result = _get_records_sync(mock_hass, entity_id, None, None, limit=3)

        assert result["success"] is True
        assert len(result["records"]) == 3
        assert result["has_more"] is True

    def test_filters_by_start_and_end_time(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, _, entity_id = sample_entity
        _add_state(db_session, states_meta_id, 1_700_000_000, "early")
        _add_state(db_session, states_meta_id, 1_700_000_100, "middle")
        _add_state(db_session, states_meta_id, 1_700_000_200, "late")

        start = datetime.fromtimestamp(1_700_000_050, tz=timezone.utc)
        end = datetime.fromtimestamp(1_700_000_150, tz=timezone.utc)
        result = _get_records_sync(mock_hass, entity_id, start, end, limit=100)

        assert result["success"] is True
        assert [r["state"] for r in result["records"]] == ["middle"]

    def test_returns_empty_records_for_unknown_entity(
        self, db_session, mock_hass, sample_entity,
    ):
        # Even though sample_entity fixture creates sensor.test, we query a different one
        result = _get_records_sync(mock_hass, "sensor.does_not_exist", None, None, limit=100)

        assert result["success"] is True
        assert result["records"] == []
        assert result["has_more"] is False

    def test_deserialises_json_attributes(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, _, entity_id = sample_entity
        _add_state(
            db_session, states_meta_id, 1_700_000_000, "1.0",
            attributes={"unit_of_measurement": "°C", "friendly_name": "Temp"},
        )

        result = _get_records_sync(mock_hass, entity_id, None, None, limit=100)

        attrs = result["records"][0]["attributes"]
        assert attrs["unit_of_measurement"] == "°C"
        assert attrs["friendly_name"] == "Temp"

    def test_handles_malformed_attributes_gracefully(
        self, db_session, mock_hass, sample_entity,
    ):
        """Corrupted JSON in attributes should not crash the query."""
        states_meta_id, _, entity_id = sample_entity
        s = States(
            metadata_id=states_meta_id, state="42",
            attributes="{not valid json", last_updated_ts=1.0, last_changed_ts=1.0,
        )
        db_session.add(s)
        db_session.flush()

        result = _get_records_sync(mock_hass, entity_id, None, None, limit=10)

        assert result["success"] is True
        assert result["records"][0]["attributes"] == {}


# --------------------------------------------------------------------------
# _update_record_sync
# --------------------------------------------------------------------------


class TestUpdateRecordSync:
    def test_updates_state_value(self, db_session, mock_hass, sample_entity):
        states_meta_id, _, _ = sample_entity
        s = _add_state(db_session, states_meta_id, 1_700_000_000, "old")

        result = _update_record_sync(mock_hass, s.state_id, "new", None, None, None)

        assert result["success"] is True
        assert result["state_id"] == s.state_id
        assert result["statistics_stale"] is False
        db_session.expire_all()
        assert db_session.get(States, s.state_id).state == "new"

    def test_updates_attributes_as_json_string(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, _, _ = sample_entity
        s = _add_state(db_session, states_meta_id, 1_700_000_000, "5", attributes={"old": 1})

        result = _update_record_sync(
            mock_hass, s.state_id, None, {"new": "value"}, None, None,
        )

        assert result["success"] is True
        db_session.expire_all()
        stored = db_session.get(States, s.state_id).attributes
        assert isinstance(stored, str)
        assert json.loads(stored) == {"new": "value"}

    def test_updates_both_timestamp_columns(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, _, _ = sample_entity
        s = _add_state(db_session, states_meta_id, 1_700_000_000, "1")

        new_ts = datetime.fromtimestamp(1_800_000_000, tz=timezone.utc)
        result = _update_record_sync(
            mock_hass, s.state_id, None, None, new_ts, new_ts,
        )

        assert result["success"] is True
        db_session.expire_all()
        reloaded = db_session.get(States, s.state_id)
        assert reloaded.last_updated_ts == 1_800_000_000
        assert reloaded.last_changed_ts == 1_800_000_000

    def test_returns_error_when_state_not_found(self, db_session, mock_hass):
        result = _update_record_sync(mock_hass, 999_999, "new", None, None, None)

        assert result["success"] is False
        assert "999999" in result["error"]

    def test_leaves_unchanged_fields_alone_when_none_passed(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, _, _ = sample_entity
        s = _add_state(
            db_session, states_meta_id, 1_700_000_000, "original",
            attributes={"kept": True},
        )

        _update_record_sync(mock_hass, s.state_id, None, None, None, None)

        db_session.expire_all()
        reloaded = db_session.get(States, s.state_id)
        assert reloaded.state == "original"
        assert json.loads(reloaded.attributes) == {"kept": True}


# --------------------------------------------------------------------------
# _delete_record_sync
# --------------------------------------------------------------------------


class TestDeleteRecordSync:
    def test_deletes_state_row(self, db_session, mock_hass, sample_entity):
        states_meta_id, _, _ = sample_entity
        s = _add_state(db_session, states_meta_id, 1_700_000_000, "1")
        state_id = s.state_id  # capture before delete to avoid ORM reload

        result = _delete_record_sync(mock_hass, state_id)

        assert result["success"] is True
        assert result["state_id"] == state_id
        db_session.expire_all()
        assert db_session.get(States, state_id) is None

    def test_returns_error_when_state_not_found(self, db_session, mock_hass):
        result = _delete_record_sync(mock_hass, 999_999)

        assert result["success"] is False
        assert "999999" in result["error"]

    def test_delete_succeeds_without_state_id_fk(
        self, db_session, mock_hass, sample_entity,
    ):
        """Deletion should succeed and report statistics_deleted=0 since
        modern HA no longer has a state_id FK on StatisticsShortTerm."""
        states_meta_id, _, _ = sample_entity
        s = _add_state(db_session, states_meta_id, 1_700_000_000, "1.0")
        state_id = s.state_id

        result = _delete_record_sync(mock_hass, state_id)

        assert result["success"] is True
        assert result["statistics_deleted"] == 0

    def test_clears_old_state_id_references_before_delete(
        self, db_session, mock_hass, sample_entity,
    ):
        """Self-referential FK: newer states reference their predecessor via
        old_state_id.  Deleting the predecessor must null out those refs first."""
        states_meta_id, _, _ = sample_entity
        s1 = _add_state(db_session, states_meta_id, 1_700_000_000, "1")
        s2 = States(
            metadata_id=states_meta_id, state="2",
            attributes="{}", last_updated_ts=1_700_000_100, last_changed_ts=1_700_000_100,
            old_state_id=s1.state_id,
        )
        db_session.add(s2)
        db_session.flush()

        result = _delete_record_sync(mock_hass, s1.state_id)

        assert result["success"] is True
        db_session.expire_all()
        reloaded = db_session.get(States, s2.state_id)
        assert reloaded.old_state_id is None


# --------------------------------------------------------------------------
# _create_record_sync
# --------------------------------------------------------------------------


class TestCreateRecordSync:
    def test_creates_state_for_existing_entity(
        self, db_session, mock_hass, sample_entity,
    ):
        _, _, entity_id = sample_entity
        ts = datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)

        result = _create_record_sync(
            mock_hass, entity_id, "42", {"unit": "°C"}, ts, ts,
        )

        assert result["success"] is True
        assert isinstance(result["state_id"], int)
        db_session.expire_all()
        created = db_session.get(States, result["state_id"])
        assert created.state == "42"
        assert json.loads(created.attributes) == {"unit": "°C"}
        assert created.last_updated_ts == 1_700_000_000

    def test_creates_states_meta_for_new_entity(self, db_session, mock_hass):
        """An entity not previously seen should get a StatesMeta row auto-created."""
        entity_id = "sensor.brand_new"
        assert db_session.query(StatesMeta).filter_by(entity_id=entity_id).first() is None

        ts = datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)
        result = _create_record_sync(mock_hass, entity_id, "10", {}, ts, ts)

        assert result["success"] is True
        db_session.expire_all()
        assert db_session.query(StatesMeta).filter_by(entity_id=entity_id).first() is not None

    def test_sets_both_legacy_and_ts_timestamps(
        self, db_session, mock_hass, sample_entity,
    ):
        _, _, entity_id = sample_entity
        ts = datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)

        result = _create_record_sync(mock_hass, entity_id, "1", {}, ts, ts)

        assert result["success"] is True
        db_session.expire_all()
        created = db_session.get(States, result["state_id"])
        assert created.last_updated_ts == 1_700_000_000
        assert created.last_changed_ts == 1_700_000_000

    def test_serialises_attributes_as_json_string(
        self, db_session, mock_hass, sample_entity,
    ):
        _, _, entity_id = sample_entity
        ts = datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)

        result = _create_record_sync(
            mock_hass, entity_id, "1", {"nested": {"a": 1, "b": [2, 3]}}, ts, ts,
        )

        assert result["success"] is True
        db_session.expire_all()
        created = db_session.get(States, result["state_id"])
        assert isinstance(created.attributes, str)
        assert json.loads(created.attributes) == {"nested": {"a": 1, "b": [2, 3]}}

    def test_returns_statistics_stale_flag(
        self, db_session, mock_hass, sample_entity,
    ):
        """Even in the happy path (no stats to update for this entity),
        the response should include the ``statistics_stale`` key."""
        _, _, entity_id = sample_entity
        ts = datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)

        result = _create_record_sync(mock_hass, entity_id, "1", {}, ts, ts)

        assert "statistics_stale" in result
        assert result["statistics_stale"] is False


# --------------------------------------------------------------------------
# End-to-end: deleting a state should recalculate the statistics that
# depended on it (regression for the cascade behaviour, not just the FK
# delete which is a no-op on modern HA).
# --------------------------------------------------------------------------


def _five_min_aligned(ts: float) -> float:
    return float(int(ts // 300) * 300)


def _hour_aligned(ts: float) -> float:
    return float(int(ts // 3600) * 3600)


class TestDeleteStateAffectsStatistics:
    """End-to-end checks that ``_delete_record_sync`` invokes
    ``update_statistics_after_state_change`` so short-term and long-term
    rows reflect the missing state, not just the freed FK link."""

    def test_only_state_in_period_short_term_row_removed_when_no_prior(
        self, db_session, mock_hass, sample_entity,
    ):
        """Sole state in a 5-min bucket with no prior numeric → short-term
        row is removed (nothing left to summarise, nothing to carry forward)."""
        states_meta_id, stat_meta_id, _ = sample_entity
        ts = 1_700_000_000.0
        period = _five_min_aligned(ts)
        s = _add_state(db_session, states_meta_id, ts, "5.0")
        state_id = s.state_id
        short_id = StatisticsShortTerm(
            metadata_id=stat_meta_id, start_ts=period,
            mean=5.0, min=5.0, max=5.0, state=5.0,
        )
        db_session.add(short_id)
        db_session.flush()
        short_row_id = short_id.id

        result = _delete_record_sync(mock_hass, state_id)

        assert result["success"] is True
        db_session.expire_all()
        assert db_session.get(States, state_id) is None
        # Short-term row removed because no states remain and no prior value
        assert db_session.get(StatisticsShortTerm, short_row_id) is None

    def test_only_state_in_period_short_term_holds_last_prior_value(
        self, db_session, mock_hass, sample_entity,
    ):
        """A prior state exists outside the bucket → after delete the
        short-term row carries that prior value forward, not the deleted one."""
        states_meta_id, stat_meta_id, _ = sample_entity
        ts = 1_700_000_000.0
        period = _five_min_aligned(ts)
        # Prior state, BEFORE the affected bucket
        _add_state(db_session, states_meta_id, period - 60, "2.0")
        s = _add_state(db_session, states_meta_id, ts, "5.0")
        state_id = s.state_id
        short_row = StatisticsShortTerm(
            metadata_id=stat_meta_id, start_ts=period,
            mean=5.0, min=5.0, max=5.0, state=5.0,
        )
        db_session.add(short_row)
        db_session.flush()
        short_id = short_row.id

        result = _delete_record_sync(mock_hass, state_id)

        assert result["success"] is True
        db_session.expire_all()
        reloaded = db_session.get(StatisticsShortTerm, short_id)
        assert reloaded is not None
        # All four columns should reflect the prior numeric value, not 5.0
        assert reloaded.state == 2.0
        assert reloaded.mean == 2.0
        assert reloaded.min == 2.0
        assert reloaded.max == 2.0

    def test_other_states_in_period_short_term_row_recomputed(
        self, db_session, mock_hass, sample_entity,
    ):
        """Two states in the same 5-min bucket; deleting the higher one
        should leave the short-term row reflecting only the surviving state."""
        states_meta_id, stat_meta_id, _ = sample_entity
        period = _five_min_aligned(1_700_000_000.0)
        _add_state(db_session, states_meta_id, period + 30, "10.0")    # surviving
        s_high = _add_state(db_session, states_meta_id, period + 120, "20.0")  # deleted
        delete_id = s_high.state_id

        short_row = StatisticsShortTerm(
            metadata_id=stat_meta_id, start_ts=period,
            mean=15.0, min=10.0, max=20.0, state=20.0,
        )
        db_session.add(short_row)
        db_session.flush()
        short_id = short_row.id

        result = _delete_record_sync(mock_hass, delete_id)

        assert result["success"] is True
        db_session.expire_all()
        reloaded = db_session.get(StatisticsShortTerm, short_id)
        assert reloaded is not None
        # After deleting the 20.0, only 10.0 remains
        assert reloaded.mean == 10.0
        assert reloaded.min == 10.0
        assert reloaded.max == 10.0
        assert reloaded.state == 10.0

    def test_long_term_row_recomputed_after_state_delete(
        self, db_session, mock_hass, sample_entity,
    ):
        """Hourly stat row aggregates short-term rows in its hour. After a
        state delete, the affected short-term recomputes, then the hourly
        recomputes from it."""
        states_meta_id, stat_meta_id, _ = sample_entity
        # Pin to an exact hour boundary so 5-min and hour periods align cleanly
        hour_start = 3600.0 * 472_222  # arbitrary fixed hour
        first_period = hour_start

        # One surviving state, one deleted state, both in the same 5-min bucket
        _add_state(db_session, states_meta_id, first_period + 30, "10.0")
        s_doomed = _add_state(db_session, states_meta_id, first_period + 120, "30.0")
        delete_id = s_doomed.state_id

        # Short-term row for that bucket pre-recalc reflects both
        short_row = StatisticsShortTerm(
            metadata_id=stat_meta_id, start_ts=first_period,
            mean=20.0, min=10.0, max=30.0, state=30.0,
        )
        long_row = Statistics(
            metadata_id=stat_meta_id, start_ts=hour_start,
            mean=20.0, min=10.0, max=30.0, state=30.0,
        )
        db_session.add_all([short_row, long_row])
        db_session.flush()
        long_id = long_row.id

        result = _delete_record_sync(mock_hass, delete_id)

        assert result["success"] is True
        db_session.expire_all()
        reloaded_long = db_session.get(Statistics, long_id)
        assert reloaded_long is not None
        # Hourly aggregates the now-recomputed short-term: only 10.0 remains
        assert reloaded_long.mean == 10.0
        assert reloaded_long.min == 10.0
        assert reloaded_long.max == 10.0
        assert reloaded_long.state == 10.0

    def test_totaliser_sum_cascades_forward_after_state_delete(
        self, db_session, mock_hass, sample_totaliser,
    ):
        """Deleting a state in a totaliser should cascade the sum delta
        through this period and all subsequent periods.  This is the
        regression for the energy-dashboard bug class."""
        states_meta_id, stat_meta_id, _ = sample_totaliser
        first_period = _five_min_aligned(1_700_000_000.0)
        next_period = first_period + 300.0
        later_period = first_period + 600.0

        # State sequence: 100.0 (kept) → 200.0 (deleted) inside the same bucket
        _add_state(db_session, states_meta_id, first_period + 30, "100.0")
        s_doomed = _add_state(db_session, states_meta_id, first_period + 120, "200.0")
        delete_id = s_doomed.state_id

        # Pre-existing short-term rows with a running sum reflecting the 200.0 endpoint
        # Period 0: state=200, sum=200; later periods carry that forward
        first = StatisticsShortTerm(
            metadata_id=stat_meta_id, start_ts=first_period,
            state=200.0, sum=200.0,
        )
        nxt = StatisticsShortTerm(
            metadata_id=stat_meta_id, start_ts=next_period,
            state=200.0, sum=200.0,
        )
        later = StatisticsShortTerm(
            metadata_id=stat_meta_id, start_ts=later_period,
            state=200.0, sum=200.0,
        )
        db_session.add_all([first, nxt, later])
        db_session.flush()
        first_id, nxt_id, later_id = first.id, nxt.id, later.id

        result = _delete_record_sync(mock_hass, delete_id)

        assert result["success"] is True
        db_session.expire_all()

        # First period now ends at 100.0, so its state = 100.0; sum delta is -100
        reloaded_first = db_session.get(StatisticsShortTerm, first_id)
        assert reloaded_first.state == 100.0
        assert reloaded_first.sum == 100.0

        # Cascade: subsequent periods got -100 applied to their sum, but state
        # is unchanged because no recalc ran for them
        reloaded_nxt = db_session.get(StatisticsShortTerm, nxt_id)
        assert reloaded_nxt.sum == 100.0
        reloaded_later = db_session.get(StatisticsShortTerm, later_id)
        assert reloaded_later.sum == 100.0

    def test_create_then_delete_round_trips_statistics(
        self, db_session, mock_hass, sample_entity,
    ):
        """Create a state inside a stats-tracked period, then delete it.
        The short-term row should end up as it was before the create
        (or be removed if no prior data exists)."""
        states_meta_id, stat_meta_id, entity_id = sample_entity
        period = _five_min_aligned(1_700_000_000.0)
        # Pre-existing short-term row with a known starting state
        short = StatisticsShortTerm(
            metadata_id=stat_meta_id, start_ts=period,
            mean=7.0, min=7.0, max=7.0, state=7.0,
        )
        db_session.add(short)
        db_session.flush()
        short_id = short.id
        # Prior state so the period has a fallback when emptied
        _add_state(db_session, states_meta_id, period - 60, "7.0")

        # Create a state at period+60 with a different value
        ts = datetime.fromtimestamp(period + 60, tz=timezone.utc)
        create_result = _create_record_sync(mock_hass, entity_id, "99.0", {}, ts, ts)
        assert create_result["success"] is True
        new_state_id = create_result["state_id"]

        # Delete it again
        delete_result = _delete_record_sync(mock_hass, new_state_id)
        assert delete_result["success"] is True

        db_session.expire_all()
        reloaded = db_session.get(StatisticsShortTerm, short_id)
        # After create+delete, the short-term should reflect the prior 7.0,
        # not the temporarily-created 99.0
        assert reloaded is not None
        assert reloaded.state == 7.0
        assert reloaded.mean == 7.0

    def test_edit_state_in_history_recomputes_both_short_and_long_term(
        self, db_session, mock_hass, sample_entity,
    ):
        """Editing the value of a state record should refresh both the
        containing 5-min short-term row and the containing hourly long-term
        row.  Mirror of the user request: 'if I edit a value in the history,
        the short-term and long-term stats get affected'."""
        states_meta_id, stat_meta_id, _ = sample_entity
        # Pin to an exact hour boundary so 5-min and hour periods align cleanly
        hour_start = 3600.0 * 472_222
        period = hour_start  # first 5-min bucket of the hour
        # Single state in the bucket, value 10.0
        s = _add_state(db_session, states_meta_id, period + 60, "10.0")
        state_id = s.state_id

        # Pre-existing stats reflect that 10.0
        short_row = StatisticsShortTerm(
            metadata_id=stat_meta_id, start_ts=period,
            mean=10.0, min=10.0, max=10.0, state=10.0,
        )
        long_row = Statistics(
            metadata_id=stat_meta_id, start_ts=hour_start,
            mean=10.0, min=10.0, max=10.0, state=10.0,
        )
        db_session.add_all([short_row, long_row])
        db_session.flush()
        short_id, long_id = short_row.id, long_row.id

        # User finds the value was wrong (should have been 50.0) and corrects it
        result = _update_record_sync(mock_hass, state_id, "50.0", None, None, None)

        assert result["success"] is True
        assert result["statistics_stale"] is False
        db_session.expire_all()

        # Short-term reflects the new value
        reloaded_short = db_session.get(StatisticsShortTerm, short_id)
        assert reloaded_short.mean == 50.0
        assert reloaded_short.min == 50.0
        assert reloaded_short.max == 50.0
        assert reloaded_short.state == 50.0

        # Long-term re-aggregates from the (single) updated short-term row
        reloaded_long = db_session.get(Statistics, long_id)
        assert reloaded_long.mean == 50.0
        assert reloaded_long.min == 50.0
        assert reloaded_long.max == 50.0
        assert reloaded_long.state == 50.0
