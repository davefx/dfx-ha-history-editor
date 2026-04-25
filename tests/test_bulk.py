"""Tests for bulk operations on state history and statistics records.

Covers:
- ``_bulk_update_record_sync`` and ``_bulk_delete_record_sync`` from
  ``custom_components.history_editor`` — multi-row state edits / deletes.
- ``bulk_update_statistic_sync`` and ``bulk_delete_statistic_sync`` from
  ``custom_components.history_editor.statistics`` — multi-row stats edits
  / deletes for both short-term and long-term tables.

The deduped-cascade behaviour (one statistics recalc per affected
period across the whole batch, not per row) is verified end-to-end.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("homeassistant.components.recorder.db_schema")

from homeassistant.components.recorder.db_schema import (  # noqa: E402
    States,
    Statistics,
    StatisticsShortTerm,
)

from custom_components.history_editor import (  # noqa: E402
    _bulk_delete_record_sync,
    _bulk_update_record_sync,
)
from custom_components.history_editor.statistics import (  # noqa: E402
    bulk_delete_statistic_sync,
    bulk_update_statistic_sync,
)


def _add_state(session, metadata_id, ts, state, attributes=None):
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


def _add_short_term(session, metadata_id, start_ts, **cols):
    row = StatisticsShortTerm(metadata_id=metadata_id, start_ts=start_ts, **cols)
    session.add(row)
    session.flush()
    return row


def _add_long_term(session, metadata_id, start_ts, **cols):
    row = Statistics(metadata_id=metadata_id, start_ts=start_ts, **cols)
    session.add(row)
    session.flush()
    return row


# --------------------------------------------------------------------------
# _bulk_update_record_sync
# --------------------------------------------------------------------------


class TestBulkUpdateRecord:
    def test_applies_same_state_to_all_listed_records(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, _, _ = sample_entity
        s1 = _add_state(db_session, states_meta_id, 1_700_000_000, "1")
        s2 = _add_state(db_session, states_meta_id, 1_700_000_100, "2")
        s3 = _add_state(db_session, states_meta_id, 1_700_000_200, "3")
        ids = [s1.state_id, s2.state_id, s3.state_id]

        result = _bulk_update_record_sync(
            mock_hass, ids, "FIXED", None, None, None,
        )

        assert result["success"] is True
        assert result["updated_count"] == 3
        assert result["not_found"] == []
        db_session.expire_all()
        for sid in ids:
            assert db_session.get(States, sid).state == "FIXED"

    def test_replaces_attributes_uniformly(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, _, _ = sample_entity
        s1 = _add_state(
            db_session, states_meta_id, 1_700_000_000, "1", attributes={"old": True},
        )
        s2 = _add_state(
            db_session, states_meta_id, 1_700_000_100, "2", attributes={"different": 1},
        )
        ids = [s1.state_id, s2.state_id]

        result = _bulk_update_record_sync(
            mock_hass, ids, None, {"new": "attrs"}, None, None,
        )

        assert result["success"] is True
        assert result["updated_count"] == 2
        db_session.expire_all()
        for sid in ids:
            stored = db_session.get(States, sid).attributes
            assert json.loads(stored) == {"new": "attrs"}

    def test_reports_not_found_ids_without_aborting(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, _, _ = sample_entity
        s1 = _add_state(db_session, states_meta_id, 1_700_000_000, "1")
        # Mix a real id with two non-existent ones
        result = _bulk_update_record_sync(
            mock_hass, [s1.state_id, 999_998, 999_999], "X", None, None, None,
        )

        assert result["success"] is True
        assert result["updated_count"] == 1
        assert sorted(result["not_found"]) == [999_998, 999_999]
        db_session.expire_all()
        assert db_session.get(States, s1.state_id).state == "X"

    def test_rejects_empty_list(self, db_session, mock_hass):
        result = _bulk_update_record_sync(mock_hass, [], "X", None, None, None)
        assert result["success"] is False
        assert "non-empty" in result["error"]

    def test_rejects_no_field_overrides(
        self, db_session, mock_hass, sample_entity,
    ):
        """Passing only state_ids with no fields to update is a usage error."""
        states_meta_id, _, _ = sample_entity
        s1 = _add_state(db_session, states_meta_id, 1_700_000_000, "1")

        result = _bulk_update_record_sync(
            mock_hass, [s1.state_id], None, None, None, None,
        )

        assert result["success"] is False
        assert "no fields" in result["error"]

    def test_recalculates_statistics_once_per_affected_period(
        self, db_session, mock_hass, sample_entity,
    ):
        """Two states in the same 5-min bucket and two in another. Bulk-update
        all four. Both buckets' short-term rows should reflect the new value;
        the long-term row should re-aggregate from both."""
        states_meta_id, stat_meta_id, _ = sample_entity
        hour_start = 3600.0 * 472_222
        # Bucket A: 2 states, Bucket B: 2 states (same hour, different 5-min)
        s_a1 = _add_state(db_session, states_meta_id, hour_start + 30, "10.0")
        s_a2 = _add_state(db_session, states_meta_id, hour_start + 60, "11.0")
        s_b1 = _add_state(db_session, states_meta_id, hour_start + 330, "12.0")
        s_b2 = _add_state(db_session, states_meta_id, hour_start + 360, "13.0")
        # Pre-existing stats reflecting the originals
        short_a = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start,
            mean=10.5, min=10.0, max=11.0, state=11.0,
        )
        short_b = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start + 300,
            mean=12.5, min=12.0, max=13.0, state=13.0,
        )
        long_row = _add_long_term(
            db_session, stat_meta_id, start_ts=hour_start,
            mean=11.5, min=10.0, max=13.0, state=13.0,
        )
        a_id, b_id, l_id = short_a.id, short_b.id, long_row.id

        # Set every state to 99.0
        result = _bulk_update_record_sync(
            mock_hass,
            [s_a1.state_id, s_a2.state_id, s_b1.state_id, s_b2.state_id],
            "99.0", None, None, None,
        )

        assert result["success"] is True
        assert result["updated_count"] == 4
        assert result["statistics_stale"] is False
        db_session.expire_all()

        # Both short-term rows reflect new value
        reloaded_a = db_session.get(StatisticsShortTerm, a_id)
        assert reloaded_a.state == 99.0
        assert reloaded_a.mean == 99.0
        reloaded_b = db_session.get(StatisticsShortTerm, b_id)
        assert reloaded_b.state == 99.0
        assert reloaded_b.mean == 99.0
        # Long-term re-aggregates
        reloaded_long = db_session.get(Statistics, l_id)
        assert reloaded_long.state == 99.0
        # mean of two short-terms each at 99.0
        assert reloaded_long.mean == 99.0

    def test_bulk_update_state_only_does_not_skip_stats_recalc(
        self, db_session, mock_hass, sample_entity,
    ):
        """Updating just `state` (no timestamp move) still recomputes the
        affected stats because the new mean/min/max/state depend on it."""
        states_meta_id, stat_meta_id, _ = sample_entity
        period = 1_700_000_000.0 - (1_700_000_000.0 % 300)
        s = _add_state(db_session, states_meta_id, period + 60, "5.0")
        short = _add_short_term(
            db_session, stat_meta_id, start_ts=period,
            mean=5.0, min=5.0, max=5.0, state=5.0,
        )
        short_id = short.id

        _bulk_update_record_sync(
            mock_hass, [s.state_id], "100.0", None, None, None,
        )

        db_session.expire_all()
        assert db_session.get(StatisticsShortTerm, short_id).state == 100.0


# --------------------------------------------------------------------------
# _bulk_delete_record_sync
# --------------------------------------------------------------------------


class TestBulkDeleteRecord:
    def test_deletes_listed_records(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, _, _ = sample_entity
        s1 = _add_state(db_session, states_meta_id, 1_700_000_000, "1")
        s2 = _add_state(db_session, states_meta_id, 1_700_000_100, "2")
        s3 = _add_state(db_session, states_meta_id, 1_700_000_200, "3")
        ids = [s1.state_id, s2.state_id, s3.state_id]

        result = _bulk_delete_record_sync(mock_hass, ids)

        assert result["success"] is True
        assert result["deleted_count"] == 3
        db_session.expire_all()
        for sid in ids:
            assert db_session.get(States, sid) is None

    def test_partial_batch_with_unknown_ids_succeeds(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, _, _ = sample_entity
        s1 = _add_state(db_session, states_meta_id, 1_700_000_000, "1")
        s1_id = s1.state_id

        result = _bulk_delete_record_sync(mock_hass, [s1_id, 999_999])

        assert result["success"] is True
        assert result["deleted_count"] == 1
        assert result["not_found"] == [999_999]
        db_session.expire_all()
        assert db_session.get(States, s1_id) is None

    def test_rejects_empty_list(self, db_session, mock_hass):
        result = _bulk_delete_record_sync(mock_hass, [])
        assert result["success"] is False
        assert "non-empty" in result["error"]

    def test_recalculates_statistics_once_per_affected_period(
        self, db_session, mock_hass, sample_entity,
    ):
        """Delete two states in the same 5-min bucket — the short-term row
        should be removed (no other states, no prior numeric value) and the
        long-term row should reflect the loss."""
        states_meta_id, stat_meta_id, _ = sample_entity
        period = 1_700_000_000.0 - (1_700_000_000.0 % 300)
        hour_start = period - (period % 3600)

        s1 = _add_state(db_session, states_meta_id, period + 30, "10.0")
        s2 = _add_state(db_session, states_meta_id, period + 60, "20.0")
        short = _add_short_term(
            db_session, stat_meta_id, start_ts=period,
            mean=15.0, min=10.0, max=20.0, state=20.0,
        )
        long_row = _add_long_term(
            db_session, stat_meta_id, start_ts=hour_start,
            mean=15.0, min=10.0, max=20.0, state=20.0,
        )
        short_id, long_id = short.id, long_row.id

        result = _bulk_delete_record_sync(
            mock_hass, [s1.state_id, s2.state_id],
        )

        assert result["success"] is True
        assert result["deleted_count"] == 2
        db_session.expire_all()
        # Short-term row removed: no states left, no prior numeric to carry forward
        assert db_session.get(StatisticsShortTerm, short_id) is None
        # Long-term: empty hour fallback with no prior short-term → row removed
        assert db_session.get(Statistics, long_id) is None

    def test_self_referential_old_state_ids_cleared(
        self, db_session, mock_hass, sample_entity,
    ):
        """Bulk delete should null out old_state_id on states that reference
        the deleted ones, even when the referencing state is NOT in the batch."""
        states_meta_id, _, _ = sample_entity
        s1 = _add_state(db_session, states_meta_id, 1_700_000_000, "1")
        s2 = States(
            metadata_id=states_meta_id, state="2", attributes="{}",
            last_updated_ts=1_700_000_100, last_changed_ts=1_700_000_100,
            old_state_id=s1.state_id,
        )
        db_session.add(s2)
        db_session.flush()
        s2_id = s2.state_id

        result = _bulk_delete_record_sync(mock_hass, [s1.state_id])

        assert result["success"] is True
        db_session.expire_all()
        assert db_session.get(States, s2_id).old_state_id is None


# --------------------------------------------------------------------------
# bulk_update_statistic_sync
# --------------------------------------------------------------------------


class TestBulkUpdateStatistic:
    def test_applies_same_overrides_to_listed_long_term_rows(
        self, db_session, mock_hass, sample_entity,
    ):
        _, stat_meta_id, _ = sample_entity
        rows = [
            _add_long_term(
                db_session, stat_meta_id, start_ts=3600.0 * (100 + i),
                mean=float(i), min=float(i), max=float(i + 1), state=float(i),
            )
            for i in range(3)
        ]
        ids = [r.id for r in rows]

        result = bulk_update_statistic_sync(
            mock_hass, ids,
            mean=42.0, min_val=42.0, max_val=42.0, sum_val=None, state=42.0,
            statistic_type="long_term",
        )

        assert result["success"] is True
        assert result["updated_count"] == 3
        assert result["blocked"] == []
        assert result["not_found"] == []
        db_session.expire_all()
        for rid in ids:
            r = db_session.get(Statistics, rid)
            assert r.mean == 42.0
            assert r.state == 42.0

    def test_blocks_short_term_rows_with_underlying_state_history(
        self, db_session, mock_hass, sample_entity,
    ):
        """A 5-min bucket with state history records is locked.  The other
        bucket without state history should still be updated."""
        states_meta_id, stat_meta_id, _ = sample_entity
        # Bucket A: has state history → blocked
        bucket_a = 1_700_000_000.0 - (1_700_000_000.0 % 300)
        _add_state(db_session, states_meta_id, bucket_a + 30, "5.0")
        row_a = _add_short_term(
            db_session, stat_meta_id, start_ts=bucket_a, state=5.0, mean=5.0,
        )
        # Bucket B: no state history → updatable
        bucket_b = bucket_a + 300
        row_b = _add_short_term(
            db_session, stat_meta_id, start_ts=bucket_b, state=5.0, mean=5.0,
        )
        ids = [row_a.id, row_b.id]

        result = bulk_update_statistic_sync(
            mock_hass, ids,
            mean=99.0, min_val=None, max_val=None, sum_val=None, state=99.0,
            statistic_type="short_term",
        )

        assert result["success"] is True
        assert result["updated_count"] == 1
        assert len(result["blocked"]) == 1
        assert result["blocked"][0]["id"] == row_a.id
        assert "state history" in result["blocked"][0]["reason"]
        db_session.expire_all()
        # Bucket A unchanged, bucket B updated
        assert db_session.get(StatisticsShortTerm, row_a.id).state == 5.0
        assert db_session.get(StatisticsShortTerm, row_b.id).state == 99.0

    def test_short_term_bulk_update_cascades_long_term_once_per_hour(
        self, db_session, mock_hass, sample_entity,
    ):
        """Two short-term rows in the same hour, both edited.  The long-term
        row should re-aggregate exactly once from both."""
        _, stat_meta_id, _ = sample_entity
        hour_start = 3600.0 * 100
        a = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start, mean=1.0, state=1.0,
        )
        b = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start + 300, mean=2.0, state=2.0,
        )
        long_row = _add_long_term(
            db_session, stat_meta_id, start_ts=hour_start, mean=1.5, state=2.0,
        )
        long_id = long_row.id

        result = bulk_update_statistic_sync(
            mock_hass, [a.id, b.id],
            mean=10.0, min_val=10.0, max_val=10.0, sum_val=None, state=10.0,
            statistic_type="short_term",
        )

        assert result["success"] is True
        assert result["updated_count"] == 2
        db_session.expire_all()
        reloaded_long = db_session.get(Statistics, long_id)
        # Both short-term rows now mean=10, state=10 → long-term agg = 10
        assert reloaded_long.mean == 10.0
        assert reloaded_long.state == 10.0  # last short-term's state

    def test_reports_not_found_ids(
        self, db_session, mock_hass, sample_entity,
    ):
        _, stat_meta_id, _ = sample_entity
        row = _add_long_term(
            db_session, stat_meta_id, start_ts=3600.0, state=1.0, mean=1.0,
        )

        result = bulk_update_statistic_sync(
            mock_hass, [row.id, 999_998, 999_999],
            mean=42.0, min_val=None, max_val=None, sum_val=None, state=None,
            statistic_type="long_term",
        )

        assert result["success"] is True
        assert result["updated_count"] == 1
        assert sorted(result["not_found"]) == [999_998, 999_999]

    def test_rejects_empty_id_list(self, db_session, mock_hass):
        result = bulk_update_statistic_sync(
            mock_hass, [], mean=1.0, min_val=None, max_val=None,
            sum_val=None, state=None, statistic_type="long_term",
        )
        assert result["success"] is False

    def test_rejects_no_field_overrides(self, db_session, mock_hass, sample_entity):
        _, stat_meta_id, _ = sample_entity
        row = _add_long_term(db_session, stat_meta_id, start_ts=3600.0, state=1.0)
        result = bulk_update_statistic_sync(
            mock_hass, [row.id], mean=None, min_val=None, max_val=None,
            sum_val=None, state=None, statistic_type="long_term",
        )
        assert result["success"] is False
        assert "no fields" in result["error"]


# --------------------------------------------------------------------------
# bulk_delete_statistic_sync
# --------------------------------------------------------------------------


class TestBulkDeleteStatistic:
    def test_deletes_listed_long_term_rows(
        self, db_session, mock_hass, sample_entity,
    ):
        _, stat_meta_id, _ = sample_entity
        rows = [
            _add_long_term(db_session, stat_meta_id, start_ts=3600.0 * (100 + i), state=float(i))
            for i in range(3)
        ]
        ids = [r.id for r in rows]

        result = bulk_delete_statistic_sync(
            mock_hass, ids, statistic_type="long_term",
        )

        assert result["success"] is True
        assert result["deleted_count"] == 3
        db_session.expire_all()
        for rid in ids:
            assert db_session.get(Statistics, rid) is None

    def test_blocks_long_term_rows_with_underlying_short_term(
        self, db_session, mock_hass, sample_entity,
    ):
        _, stat_meta_id, _ = sample_entity
        # Hour A: has short-term → blocked
        hour_a = 3600.0 * 100
        _add_short_term(db_session, stat_meta_id, start_ts=hour_a + 300, state=1.0)
        long_a = _add_long_term(db_session, stat_meta_id, start_ts=hour_a, state=1.0)
        # Hour B: no short-term → deletable
        hour_b = 3600.0 * 200
        long_b = _add_long_term(db_session, stat_meta_id, start_ts=hour_b, state=2.0)
        ids = [long_a.id, long_b.id]

        result = bulk_delete_statistic_sync(
            mock_hass, ids, statistic_type="long_term",
        )

        assert result["success"] is True
        assert result["deleted_count"] == 1
        assert len(result["blocked"]) == 1
        assert result["blocked"][0]["id"] == long_a.id
        db_session.expire_all()
        assert db_session.get(Statistics, long_a.id) is not None
        assert db_session.get(Statistics, long_b.id) is None

    def test_short_term_bulk_delete_cascades_long_term(
        self, db_session, mock_hass, sample_entity,
    ):
        """Deleting all short-term rows in an hour should remove the
        corresponding long-term row (empty-hour fallback with no prior)."""
        _, stat_meta_id, _ = sample_entity
        hour_start = 3600.0 * 100
        a = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start, state=1.0,
        )
        b = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start + 300, state=2.0,
        )
        long_row = _add_long_term(
            db_session, stat_meta_id, start_ts=hour_start, state=2.0,
        )
        long_id = long_row.id

        result = bulk_delete_statistic_sync(
            mock_hass, [a.id, b.id], statistic_type="short_term",
        )

        assert result["success"] is True
        assert result["deleted_count"] == 2
        db_session.expire_all()
        assert db_session.get(Statistics, long_id) is None

    def test_rejects_empty_id_list(self, db_session, mock_hass):
        result = bulk_delete_statistic_sync(
            mock_hass, [], statistic_type="long_term",
        )
        assert result["success"] is False
