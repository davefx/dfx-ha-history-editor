"""CRUD tests for statistics records.

Exercises ``get_statistics_sync``, ``update_statistic_sync`` and
``delete_statistic_sync`` — including the "source-data guard" that blocks
direct edits/deletes when underlying data still exists.
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
    delete_statistic_sync,
    get_statistics_sync,
    update_statistic_sync,
)


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


def _add_state(session, metadata_id, ts, state):
    s = States(
        metadata_id=metadata_id, state=state, attributes="{}",
        last_updated_ts=ts, last_changed_ts=ts,
    )
    session.add(s)
    session.flush()
    return s


# --------------------------------------------------------------------------
# get_statistics_sync
# --------------------------------------------------------------------------


class TestGetStatisticsSync:
    def test_returns_long_term_rows_newest_first(
        self, db_session, mock_hass, sample_entity,
    ):
        _, stat_meta_id, entity_id = sample_entity
        _add_long_term(db_session, stat_meta_id, start_ts=3600.0, state=1.0, mean=1.0)
        _add_long_term(db_session, stat_meta_id, start_ts=7200.0, state=2.0, mean=2.0)
        _add_long_term(db_session, stat_meta_id, start_ts=10800.0, state=3.0, mean=3.0)

        result = get_statistics_sync(
            mock_hass, entity_id, None, None, limit=100, statistic_type="long_term",
        )

        assert result["success"] is True
        states = [r["state"] for r in result["records"]]
        assert states == [3.0, 2.0, 1.0]
        assert result["has_more"] is False

    def test_returns_short_term_rows(self, db_session, mock_hass, sample_entity):
        _, stat_meta_id, entity_id = sample_entity
        _add_short_term(db_session, stat_meta_id, start_ts=0.0, state=0.5, mean=0.5)
        _add_short_term(db_session, stat_meta_id, start_ts=300.0, state=1.5, mean=1.5)

        result = get_statistics_sync(
            mock_hass, entity_id, None, None, limit=100, statistic_type="short_term",
        )

        assert result["success"] is True
        assert len(result["records"]) == 2
        assert all(r["statistic_type"] == "short_term" for r in result["records"])

    def test_respects_limit_and_reports_has_more(
        self, db_session, mock_hass, sample_entity,
    ):
        _, stat_meta_id, entity_id = sample_entity
        for i in range(5):
            _add_long_term(db_session, stat_meta_id, start_ts=3600.0 * i, state=float(i))

        result = get_statistics_sync(
            mock_hass, entity_id, None, None, limit=3, statistic_type="long_term",
        )

        assert result["success"] is True
        assert len(result["records"]) == 3
        assert result["has_more"] is True

    def test_filters_by_start_and_end_time(
        self, db_session, mock_hass, sample_entity,
    ):
        _, stat_meta_id, entity_id = sample_entity
        _add_long_term(db_session, stat_meta_id, start_ts=3600.0, state=1.0)
        _add_long_term(db_session, stat_meta_id, start_ts=7200.0, state=2.0)
        _add_long_term(db_session, stat_meta_id, start_ts=10800.0, state=3.0)

        start = datetime.fromtimestamp(5400, tz=timezone.utc)
        end = datetime.fromtimestamp(9000, tz=timezone.utc)
        result = get_statistics_sync(
            mock_hass, entity_id, start, end, limit=100, statistic_type="long_term",
        )

        assert [r["state"] for r in result["records"]] == [2.0]

    def test_long_term_record_marked_as_locked_when_short_term_exists(
        self, db_session, mock_hass, sample_entity,
    ):
        _, stat_meta_id, entity_id = sample_entity
        hour_start = 3600.0 * 100
        _add_long_term(db_session, stat_meta_id, start_ts=hour_start, state=1.0)
        _add_short_term(db_session, stat_meta_id, start_ts=hour_start + 300, state=1.0)

        result = get_statistics_sync(
            mock_hass, entity_id, None, None, limit=10, statistic_type="long_term",
        )

        assert result["records"][0]["has_source_data"] is True

    def test_long_term_record_unlocked_when_no_short_term(
        self, db_session, mock_hass, sample_entity,
    ):
        _, stat_meta_id, entity_id = sample_entity
        hour_start = 3600.0 * 100
        _add_long_term(db_session, stat_meta_id, start_ts=hour_start, state=1.0)
        # No short-term rows in this hour

        result = get_statistics_sync(
            mock_hass, entity_id, None, None, limit=10, statistic_type="long_term",
        )

        assert result["records"][0]["has_source_data"] is False


# --------------------------------------------------------------------------
# update_statistic_sync
# --------------------------------------------------------------------------


class TestUpdateStatisticSync:
    def test_updates_long_term_fields(self, db_session, mock_hass, sample_entity):
        _, stat_meta_id, _ = sample_entity
        row = _add_long_term(
            db_session, stat_meta_id, start_ts=3600.0,
            mean=1.0, min=0.0, max=2.0, state=1.5, sum=10.0,
        )

        result = update_statistic_sync(
            mock_hass, row.id,
            mean=5.0, min_val=4.0, max_val=6.0, sum_val=50.0, state=5.5,
            start=None, statistic_type="long_term",
        )

        assert result["success"] is True
        assert result["id"] == row.id
        db_session.expire_all()
        reloaded = db_session.get(Statistics, row.id)
        assert reloaded.mean == 5.0
        assert reloaded.min == 4.0
        assert reloaded.max == 6.0
        assert reloaded.sum == 50.0
        assert reloaded.state == 5.5

    def test_returns_error_when_id_not_found(self, db_session, mock_hass):
        result = update_statistic_sync(
            mock_hass, 999_999, 1.0, 1.0, 1.0, 1.0, 1.0, None,
            statistic_type="long_term",
        )

        assert result["success"] is False
        assert "999999" in result["error"]

    def test_blocks_short_term_edit_when_state_history_exists(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, stat_meta_id, _ = sample_entity
        start = 1_700_000_000.0
        row = _add_short_term(db_session, stat_meta_id, start_ts=start, state=1.0, mean=1.0)
        # State within the same 5-min bucket → edit must be blocked
        _add_state(db_session, states_meta_id, start + 60, "5.0")

        result = update_statistic_sync(
            mock_hass, row.id, mean=99.0, min_val=None, max_val=None,
            sum_val=None, state=None, start=None, statistic_type="short_term",
        )

        assert result["success"] is False
        assert "state history" in result["error"].lower()

    def test_blocks_long_term_edit_when_short_term_exists(
        self, db_session, mock_hass, sample_entity,
    ):
        _, stat_meta_id, _ = sample_entity
        hour_start = 3600.0 * 100
        row = _add_long_term(db_session, stat_meta_id, start_ts=hour_start, state=1.0)
        _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start + 300, state=2.0,
        )

        result = update_statistic_sync(
            mock_hass, row.id, mean=99.0, min_val=None, max_val=None,
            sum_val=None, state=None, start=None, statistic_type="long_term",
        )

        assert result["success"] is False
        assert "short-term" in result["error"].lower()

    def test_short_term_update_cascades_to_long_term(
        self, db_session, mock_hass, sample_entity,
    ):
        """Editing a short-term stat (with no underlying state history) should
        refresh the containing hour's long-term row."""
        _, stat_meta_id, _ = sample_entity
        hour_start = 3600.0 * 100
        # One short-term row in the hour, no underlying States → edit is allowed
        short = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start,
            state=1.0, mean=1.0, min=1.0, max=1.0,
        )
        long_row = _add_long_term(
            db_session, stat_meta_id, start_ts=hour_start,
            state=0.0, mean=0.0, min=0.0, max=0.0,
        )

        result = update_statistic_sync(
            mock_hass, short.id,
            mean=7.0, min_val=6.0, max_val=8.0, sum_val=None, state=7.5, start=None,
            statistic_type="short_term",
        )

        assert result["success"] is True
        db_session.expire_all()
        reloaded_long = db_session.get(Statistics, long_row.id)
        # Long-term is re-aggregated from the single (now-edited) short-term row
        assert reloaded_long.mean == 7.0
        assert reloaded_long.state == 7.5

    def test_leaves_none_fields_alone(self, db_session, mock_hass, sample_entity):
        _, stat_meta_id, _ = sample_entity
        row = _add_long_term(
            db_session, stat_meta_id, start_ts=3600.0,
            mean=1.0, min=0.0, max=2.0, sum=10.0, state=1.5,
        )

        # Only update mean; everything else stays put
        update_statistic_sync(
            mock_hass, row.id, mean=99.0, min_val=None, max_val=None,
            sum_val=None, state=None, start=None, statistic_type="long_term",
        )

        db_session.expire_all()
        reloaded = db_session.get(Statistics, row.id)
        assert reloaded.mean == 99.0
        assert reloaded.min == 0.0
        assert reloaded.max == 2.0
        assert reloaded.sum == 10.0
        assert reloaded.state == 1.5


# --------------------------------------------------------------------------
# delete_statistic_sync
# --------------------------------------------------------------------------


class TestDeleteStatisticSync:
    def test_neutralizes_long_term_row_with_prior_values(
        self, db_session, mock_hass, sample_entity,
    ):
        """'Deleting' a long-term row with a prior row should overwrite it
        with the prior's values rather than removing it (HA expects
        continuous hourly rows)."""
        _, stat_meta_id, _ = sample_entity
        _add_long_term(
            db_session, stat_meta_id, start_ts=3600.0,
            mean=5.0, min=4.0, max=6.0, state=5.5, sum=50.0,
        )
        row = _add_long_term(
            db_session, stat_meta_id, start_ts=7200.0,
            mean=999.0, min=999.0, max=999.0, state=999.0, sum=999.0,
        )
        row_id = row.id

        result = delete_statistic_sync(mock_hass, row_id, statistic_type="long_term")

        assert result["success"] is True
        assert result["id"] == row_id
        assert result["action"] == "neutralized"
        db_session.expire_all()
        reloaded = db_session.get(Statistics, row_id)
        assert reloaded is not None
        assert reloaded.mean == 5.0
        assert reloaded.state == 5.5
        assert reloaded.sum == 50.0

    def test_actually_deletes_first_long_term_row_with_no_prior(
        self, db_session, mock_hass, sample_entity,
    ):
        """When the target is the very first row (no prior), there's nothing
        to carry forward — actually remove it."""
        _, stat_meta_id, _ = sample_entity
        row = _add_long_term(db_session, stat_meta_id, start_ts=3600.0, state=1.0)
        row_id = row.id

        result = delete_statistic_sync(mock_hass, row_id, statistic_type="long_term")

        assert result["success"] is True
        assert result["action"] == "deleted"
        db_session.expire_all()
        assert db_session.get(Statistics, row_id) is None

    def test_neutralizes_short_term_row_when_no_source_data(
        self, db_session, mock_hass, sample_entity,
    ):
        _, stat_meta_id, _ = sample_entity
        prior = _add_short_term(
            db_session, stat_meta_id, start_ts=3600.0 * 100 - 300,
            state=2.0, mean=2.0, min=2.0, max=2.0,
        )
        row = _add_short_term(
            db_session, stat_meta_id, start_ts=3600.0 * 100, state=1.0,
        )
        row_id = row.id

        result = delete_statistic_sync(
            mock_hass, row_id, statistic_type="short_term",
        )

        assert result["success"] is True
        assert result["action"] == "neutralized"
        db_session.expire_all()
        reloaded = db_session.get(StatisticsShortTerm, row_id)
        assert reloaded is not None
        assert reloaded.state == 2.0
        assert reloaded.mean == 2.0

    def test_returns_error_when_id_not_found(self, db_session, mock_hass):
        result = delete_statistic_sync(
            mock_hass, 999_999, statistic_type="long_term",
        )

        assert result["success"] is False
        assert "999999" in result["error"]

    def test_blocks_short_term_delete_when_state_history_exists(
        self, db_session, mock_hass, sample_entity,
    ):
        states_meta_id, stat_meta_id, _ = sample_entity
        start = 1_700_000_000.0
        row = _add_short_term(db_session, stat_meta_id, start_ts=start, state=1.0)
        _add_state(db_session, states_meta_id, start + 60, "5.0")

        result = delete_statistic_sync(
            mock_hass, row.id, statistic_type="short_term",
        )

        assert result["success"] is False
        assert "state history" in result["error"].lower()
        # Row still present
        db_session.expire_all()
        assert db_session.get(StatisticsShortTerm, row.id) is not None

    def test_blocks_long_term_delete_when_short_term_exists(
        self, db_session, mock_hass, sample_entity,
    ):
        _, stat_meta_id, _ = sample_entity
        hour_start = 3600.0 * 100
        row = _add_long_term(db_session, stat_meta_id, start_ts=hour_start, state=1.0)
        _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start + 300, state=2.0,
        )

        result = delete_statistic_sync(
            mock_hass, row.id, statistic_type="long_term",
        )

        assert result["success"] is False
        assert "short-term" in result["error"].lower()
        db_session.expire_all()
        assert db_session.get(Statistics, row.id) is not None

    def test_short_term_delete_cascades_to_long_term_no_prior(
        self, db_session, mock_hass, sample_entity,
    ):
        """Neutralizing the only short-term row when there's no prior should
        actually delete it, and the long-term row should be removed too."""
        _, stat_meta_id, _ = sample_entity
        hour_start = 3600.0 * 100
        short = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start, state=5.0, mean=5.0,
        )
        long_row = _add_long_term(
            db_session, stat_meta_id, start_ts=hour_start, state=5.0, mean=5.0,
        )
        long_id = long_row.id

        result = delete_statistic_sync(
            mock_hass, short.id, statistic_type="short_term",
        )

        assert result["success"] is True
        assert result["action"] == "deleted"
        db_session.expire_all()
        # With no prior short-term, the long-term row is removed by the fallback
        assert db_session.get(Statistics, long_id) is None

    def test_short_term_delete_cascades_to_long_term_with_prior(
        self, db_session, mock_hass, sample_entity,
    ):
        """Neutralizing a short-term row that has a prior should keep the row
        with carried-forward values, and the long-term should re-aggregate."""
        _, stat_meta_id, _ = sample_entity
        hour_start = 3600.0 * 100
        prior = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start - 300,
            mean=3.0, min=3.0, max=3.0, state=3.0,
        )
        short = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start,
            mean=999.0, min=999.0, max=999.0, state=999.0,
        )
        long_row = _add_long_term(
            db_session, stat_meta_id, start_ts=hour_start,
            mean=999.0, min=999.0, max=999.0, state=999.0,
        )
        short_id, long_id = short.id, long_row.id

        result = delete_statistic_sync(
            mock_hass, short_id, statistic_type="short_term",
        )

        assert result["success"] is True
        assert result["action"] == "neutralized"
        db_session.expire_all()
        # Short-term row preserved with prior's values
        reloaded = db_session.get(StatisticsShortTerm, short_id)
        assert reloaded is not None
        assert reloaded.mean == 3.0
        assert reloaded.state == 3.0
        # Long-term re-aggregated — now reflects the neutralized value
        reloaded_long = db_session.get(Statistics, long_id)
        assert reloaded_long is not None
        assert reloaded_long.mean == 3.0

    def test_delete_one_short_term_with_siblings_neutralizes_and_recomputes(
        self, db_session, mock_hass, sample_entity,
    ):
        """'Deleting' a wrong short-term row when siblings exist should
        neutralize the bad row (overwrite with the prior row's values),
        then re-aggregate the long-term row from all four (now-corrected)
        short-term rows.

        User flow: 'I see a spike at 999 in 5-min stats; I delete it;
        that row now shows the prior value (10) and the hourly average
        drops from ~270 to ~22.5.'
        """
        _, stat_meta_id, _ = sample_entity
        hour_start = 3600.0 * 100
        # Four short-term rows: 10, 999 (bad), 30, 40
        good_a = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start,
            mean=10.0, min=10.0, max=10.0, state=10.0,
        )
        bad = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start + 300,
            mean=999.0, min=999.0, max=999.0, state=999.0,
        )
        good_b = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start + 600,
            mean=30.0, min=30.0, max=30.0, state=30.0,
        )
        good_c = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start + 900,
            mean=40.0, min=40.0, max=40.0, state=40.0,
        )
        long_row = _add_long_term(
            db_session, stat_meta_id, start_ts=hour_start,
            mean=(10.0 + 999.0 + 30.0 + 40.0) / 4,
            min=10.0, max=999.0, state=40.0,
        )
        good_a_id, bad_id, good_b_id, good_c_id, long_id = (
            good_a.id, bad.id, good_b.id, good_c.id, long_row.id,
        )

        result = delete_statistic_sync(
            mock_hass, bad_id, statistic_type="short_term",
        )

        assert result["success"] is True
        assert result["action"] == "neutralized"
        db_session.expire_all()
        # Bad row still exists but now carries good_a's values
        neutralized = db_session.get(StatisticsShortTerm, bad_id)
        assert neutralized is not None
        assert neutralized.mean == 10.0
        assert neutralized.state == 10.0
        # Siblings untouched
        assert db_session.get(StatisticsShortTerm, good_a_id) is not None
        assert db_session.get(StatisticsShortTerm, good_b_id) is not None
        assert db_session.get(StatisticsShortTerm, good_c_id) is not None
        # Long-term re-aggregated from all 4 rows (bad now = 10)
        # avg(10, 10, 30, 40) = 22.5
        reloaded_long = db_session.get(Statistics, long_id)
        assert reloaded_long is not None
        assert reloaded_long.mean == pytest.approx((10.0 + 10.0 + 30.0 + 40.0) / 4)
        assert reloaded_long.min == 10.0
        assert reloaded_long.max == 40.0
        assert reloaded_long.state == 40.0

    def test_update_one_short_term_with_siblings_recomputes_long_term(
        self, db_session, mock_hass, sample_entity,
    ):
        """Editing one of several short-term rows in an hour should re-aggregate
        the long-term row from all of them, not just the edited one."""
        _, stat_meta_id, _ = sample_entity
        hour_start = 3600.0 * 100
        # Three short-term rows in this hour, average 20.0
        a = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start,
            mean=10.0, min=10.0, max=10.0, state=10.0,
        )
        b = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start + 300,
            mean=20.0, min=20.0, max=20.0, state=20.0,
        )
        c = _add_short_term(
            db_session, stat_meta_id, start_ts=hour_start + 600,
            mean=30.0, min=30.0, max=30.0, state=30.0,
        )
        long_row = _add_long_term(
            db_session, stat_meta_id, start_ts=hour_start,
            mean=20.0, min=10.0, max=30.0, state=30.0,
        )
        a_id, b_id, c_id, long_id = a.id, b.id, c.id, long_row.id

        # Bump the middle one from 20 → 200
        result = update_statistic_sync(
            mock_hass, b_id,
            mean=200.0, min_val=200.0, max_val=200.0, sum_val=None, state=200.0,
            start=None, statistic_type="short_term",
        )

        assert result["success"] is True
        db_session.expire_all()
        # Siblings untouched
        assert db_session.get(StatisticsShortTerm, a_id).state == 10.0
        assert db_session.get(StatisticsShortTerm, c_id).state == 30.0
        # Long-term re-aggregated from all three: average is now (10 + 200 + 30)/3
        reloaded_long = db_session.get(Statistics, long_id)
        assert reloaded_long.mean == pytest.approx((10.0 + 200.0 + 30.0) / 3)
        assert reloaded_long.min == 10.0
        assert reloaded_long.max == 200.0
        # state = last-by-start_ts short-term: c at hour_start+600, value 30.0
        assert reloaded_long.state == 30.0
