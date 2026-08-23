"""Regression tests for issue #76.

Deleting a range of states on a ``total_increasing`` sensor used to apply one
running-sum cascade per 5-minute period.  Across a deleted range those
compounded, driving the sums far negative, and the long-term guard then refused
every repair, so the entity was stuck.
"""
from __future__ import annotations

from homeassistant.components.recorder.db_schema import (
    States, Statistics, StatisticsShortTerm,
)

from custom_components.history_editor import _bulk_delete_record_sync
from custom_components.history_editor.statistics import _check_source_data_blocks_edit

BASE = float(int(1755000000 // 3600) * 3600)
N_5MIN = 36                 # three hours
DEL_FROM, DEL_TO = 12, 24   # delete the middle hour


def _build(db_session, states_meta_id, stat_meta_id):
    """A totaliser rising by 1.0 every 5 minutes, with sum == state."""
    state_ids = []
    for p in range(N_5MIN):
        ts = BASE + p * 300.0
        st = States(metadata_id=states_meta_id, state=str(float(p)),
                    last_updated_ts=ts + 60.0, last_changed_ts=ts + 60.0)
        db_session.add(st)
        db_session.flush()
        state_ids.append(st.state_id)
        db_session.add(StatisticsShortTerm(
            metadata_id=stat_meta_id, start_ts=ts, mean=float(p),
            min=float(p), max=float(p), state=float(p), sum=float(p)))
    for h in range(N_5MIN // 12):
        last = h * 12 + 11
        db_session.add(Statistics(
            metadata_id=stat_meta_id, start_ts=BASE + h * 3600.0,
            mean=float(last), min=float(h * 12), max=float(last),
            state=float(last), sum=float(last)))
    db_session.flush()
    return state_ids


def _sums(db_session, table, stat_meta_id):
    rows = (db_session.query(table)
            .filter(table.metadata_id == stat_meta_id)
            .order_by(table.start_ts).all())
    return [r.sum for r in rows]


def test_range_delete_does_not_compound_the_sum_cascade(
    db_session, sample_totaliser, mock_hass,
):
    states_meta_id, stat_meta_id, _ = sample_totaliser
    state_ids = _build(db_session, states_meta_id, stat_meta_id)

    result = _bulk_delete_record_sync(mock_hass, state_ids[DEL_FROM:DEL_TO])
    assert result["success"] is True
    db_session.expire_all()

    sums = _sums(db_session, StatisticsShortTerm, stat_meta_id)

    # Untouched periods before the window keep their running total.
    assert sums[:DEL_FROM] == [float(p) for p in range(DEL_FROM)]

    # Inside the window every state was deleted, so the last value before it
    # (11.0) is carried forward and the running total holds flat.
    assert sums[DEL_FROM:DEL_TO] == [11.0] * (DEL_TO - DEL_FROM)

    # After the window the series resumes, shifted once by the net delta of the
    # range (11.0 - 23.0 = -12.0).  Previously each of the twelve periods
    # cascaded its own delta, so this tail read -54.0 .. -43.0.
    assert sums[DEL_TO:] == [float(p) - 12.0 for p in range(DEL_TO, N_5MIN)]

    # A running total that only ever rises must not go backwards.
    assert all(b >= a for a, b in zip(sums, sums[1:])), sums

    # The hourly rows the energy dashboard reads mirror their last 5-minute row.
    assert _sums(db_session, Statistics, stat_meta_id) == [11.0, 11.0, 23.0]


def test_hourly_rows_are_editable_once_their_states_are_gone(
    db_session, sample_totaliser, mock_hass,
):
    """The deadlock from the issue: after deleting the bad states, the hourly
    rows must become repairable."""
    states_meta_id, stat_meta_id, _ = sample_totaliser
    state_ids = _build(db_session, states_meta_id, stat_meta_id)

    hours = (db_session.query(Statistics)
             .filter(Statistics.metadata_id == stat_meta_id)
             .order_by(Statistics.start_ts).all())
    # While the state history is there, the hourly row defers to it.
    assert _check_source_data_blocks_edit(db_session, hours[1], "long_term")

    _bulk_delete_record_sync(mock_hass, state_ids[DEL_FROM:DEL_TO])
    db_session.expire_all()

    hours = (db_session.query(Statistics)
             .filter(Statistics.metadata_id == stat_meta_id)
             .order_by(Statistics.start_ts).all())
    # Its states are gone, so the row is now the user's to fix, even though the
    # derived short-term rows for that hour still exist.
    assert (db_session.query(StatisticsShortTerm)
            .filter(StatisticsShortTerm.metadata_id == stat_meta_id)
            .filter(StatisticsShortTerm.start_ts >= hours[1].start_ts)
            .filter(StatisticsShortTerm.start_ts < hours[1].start_ts + 3600.0)
            .count()) == 12
    assert _check_source_data_blocks_edit(db_session, hours[1], "long_term") is None

    # Hours whose states are untouched stay protected.
    assert _check_source_data_blocks_edit(db_session, hours[0], "long_term")
    assert _check_source_data_blocks_edit(db_session, hours[2], "long_term")
