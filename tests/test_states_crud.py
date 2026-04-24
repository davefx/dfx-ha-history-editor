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

    def test_cascades_short_term_stats_with_same_state_id(
        self, db_session, mock_hass, sample_entity,
    ):
        """The `state_id` FK column on StatisticsShortTerm was removed in later
        HA releases; skip when absent.  On older schemas the cascade delete
        drops the linked short-term row to avoid FK violations."""
        if not hasattr(StatisticsShortTerm, "state_id"):
            pytest.skip("StatisticsShortTerm.state_id not present in this HA version")

        states_meta_id, stat_meta_id, _ = sample_entity
        s = _add_state(db_session, states_meta_id, 1_700_000_000, "1.0")
        state_id = s.state_id
        short_term = StatisticsShortTerm(
            metadata_id=stat_meta_id, start_ts=1_700_000_000.0,
            state=1.0, state_id=state_id,
        )
        db_session.add(short_term)
        db_session.flush()
        short_id = short_term.id

        result = _delete_record_sync(mock_hass, state_id)

        assert result["success"] is True
        assert result["statistics_deleted"] == 1
        db_session.expire_all()
        assert db_session.get(StatisticsShortTerm, short_id) is None

    def test_delete_no_op_cascade_on_modern_schema(
        self, db_session, mock_hass, sample_entity,
    ):
        """On modern HA (no state_id FK column), deletion should succeed and
        report statistics_deleted=0 without errors."""
        if hasattr(StatisticsShortTerm, "state_id"):
            pytest.skip("state_id FK present; covered by the cascade test above")

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
