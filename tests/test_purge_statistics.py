"""Tests for the long-term statistics purge (issue #78).

Home Assistant has no age-based trimming of long-term statistics: the
time-based `recorder.purge` leaves the hourly `Statistics` table alone, and
`recorder.purge_entities` never touches statistics at all, so the only way to
remove them is to drop a statistic entirely.  These services fill that gap.

Contract, as settled in the issue:
  - only the hourly `Statistics` table is touched; short-term rows are left
    to the recorder's own purge;
  - `sum` is an absolute running total, so surviving rows keep theirs
    untouched and every remaining pair keeps its correct difference;
  - `StatisticsMeta` survives even when all of a statistic's rows go, because
    the recorder keeps writing new ones;
  - the operation is irreversible, so it reports per-statistic counts and
    supports `dry_run`.
"""
from __future__ import annotations

import pytest
from homeassistant.components.recorder.db_schema import (
    Statistics, StatisticsMeta, StatisticsShortTerm,
)
from homeassistant.util import dt as dt_util

from custom_components.history_editor.statistics import purge_statistics_sync

DAY = 86400.0


@pytest.fixture
def now_ts():
    return dt_util.utcnow().timestamp()


def _add_meta(session, statistic_id, has_sum=True):
    meta = StatisticsMeta(
        statistic_id=statistic_id, source="recorder",
        unit_of_measurement="kWh", has_mean=not has_sum, has_sum=has_sum,
        name=None,
    )
    session.add(meta)
    session.flush()
    return meta


def _add_hours(session, meta_id, now_ts, days_ago_list):
    """One hourly row per entry, `days_ago` before now, sum == index."""
    rows = []
    for i, days_ago in enumerate(days_ago_list):
        row = Statistics(
            metadata_id=meta_id, start_ts=now_ts - days_ago * DAY,
            mean=float(i), min=float(i), max=float(i),
            state=float(i), sum=float(i * 10),
        )
        session.add(row)
        rows.append(row)
    session.flush()
    return rows


def _add_many_hours(session, meta_id, now_ts, count, days_ago=400):
    """`count` consecutive hourly rows, all older than `days_ago`."""
    base = now_ts - days_ago * DAY
    for i in range(count):
        session.add(Statistics(
            metadata_id=meta_id, start_ts=base - i * 3600.0,
            mean=float(i), min=float(i), max=float(i),
            state=float(i), sum=float(i),
        ))
    session.flush()


def _remaining(session, table, meta_id):
    return (session.query(table)
            .filter(table.metadata_id == meta_id)
            .order_by(table.start_ts).all())


# --------------------------------------------------------------------------
# Cutoff
# --------------------------------------------------------------------------

def test_deletes_only_rows_older_than_keep_days(db_session, mock_hass, now_ts):
    meta = _add_meta(db_session, "sensor.energy")
    _add_hours(db_session, meta.id, now_ts, [400, 200, 100, 10, 1])

    result = purge_statistics_sync(mock_hass, keep_days=90)

    assert result["success"] is True
    db_session.expire_all()
    survivors = _remaining(db_session, Statistics, meta.id)
    ages = sorted(round((now_ts - r.start_ts) / DAY) for r in survivors)
    assert ages == [1, 10]


def test_keep_days_zero_purges_everything(db_session, mock_hass, now_ts):
    meta = _add_meta(db_session, "sensor.energy")
    _add_hours(db_session, meta.id, now_ts, [400, 10, 1])

    result = purge_statistics_sync(mock_hass, keep_days=0)

    assert result["success"] is True
    db_session.expire_all()
    assert _remaining(db_session, Statistics, meta.id) == []


def test_negative_keep_days_is_rejected(db_session, mock_hass, now_ts):
    meta = _add_meta(db_session, "sensor.energy")
    _add_hours(db_session, meta.id, now_ts, [400])

    result = purge_statistics_sync(mock_hass, keep_days=-1)

    assert result["success"] is False
    assert "keep_days" in result["error"]
    db_session.expire_all()
    assert len(_remaining(db_session, Statistics, meta.id)) == 1


# --------------------------------------------------------------------------
# What must not be touched
# --------------------------------------------------------------------------

def test_short_term_rows_are_left_alone(db_session, mock_hass, now_ts):
    """The recorder's own purge owns short-term retention."""
    meta = _add_meta(db_session, "sensor.energy")
    _add_hours(db_session, meta.id, now_ts, [400])
    db_session.add(StatisticsShortTerm(
        metadata_id=meta.id, start_ts=now_ts - 400 * DAY,
        mean=1.0, min=1.0, max=1.0, state=1.0, sum=1.0))
    db_session.flush()

    purge_statistics_sync(mock_hass, keep_days=90)

    db_session.expire_all()
    assert len(_remaining(db_session, StatisticsShortTerm, meta.id)) == 1


def test_surviving_sums_are_untouched(db_session, mock_hass, now_ts):
    """`sum` is an absolute running total, so it must not be rebased: every
    surviving pair keeps the difference the dashboards chart."""
    meta = _add_meta(db_session, "sensor.energy")
    _add_hours(db_session, meta.id, now_ts, [400, 200, 10, 5, 1])

    purge_statistics_sync(mock_hass, keep_days=90)

    db_session.expire_all()
    survivors = _remaining(db_session, Statistics, meta.id)
    assert [r.sum for r in survivors] == [20.0, 30.0, 40.0]


def test_metadata_row_survives_a_full_purge(db_session, mock_hass, now_ts):
    """The recorder keeps writing new rows, so dropping the meta would orphan
    the entity."""
    meta = _add_meta(db_session, "sensor.energy")
    _add_hours(db_session, meta.id, now_ts, [400, 300])

    purge_statistics_sync(mock_hass, keep_days=90)

    db_session.expire_all()
    assert _remaining(db_session, Statistics, meta.id) == []
    assert db_session.get(StatisticsMeta, meta.id) is not None


# --------------------------------------------------------------------------
# dry_run
# --------------------------------------------------------------------------

def test_dry_run_reports_without_deleting(db_session, mock_hass, now_ts):
    meta = _add_meta(db_session, "sensor.energy")
    _add_hours(db_session, meta.id, now_ts, [400, 300, 200, 1])

    result = purge_statistics_sync(mock_hass, keep_days=90, dry_run=True)

    assert result["success"] is True
    assert result["dry_run"] is True
    assert result["total_deleted"] == 3
    db_session.expire_all()
    assert len(_remaining(db_session, Statistics, meta.id)) == 4


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

def test_reports_counts_per_statistic_id(db_session, mock_hass, now_ts):
    a = _add_meta(db_session, "sensor.a")
    b = _add_meta(db_session, "sensor.b")
    _add_hours(db_session, a.id, now_ts, [400, 300, 1])
    _add_hours(db_session, b.id, now_ts, [400, 1])

    result = purge_statistics_sync(mock_hass, keep_days=90)

    assert result["total_deleted"] == 3
    by_id = {s["statistic_id"]: s["deleted"] for s in result["statistics"]}
    assert by_id == {"sensor.a": 2, "sensor.b": 1}
    assert result["purge_before"]


def test_statistics_with_nothing_to_purge_are_not_reported(
    db_session, mock_hass, now_ts,
):
    a = _add_meta(db_session, "sensor.a")
    b = _add_meta(db_session, "sensor.b")
    _add_hours(db_session, a.id, now_ts, [400])
    _add_hours(db_session, b.id, now_ts, [1])

    result = purge_statistics_sync(mock_hass, keep_days=90)

    assert [s["statistic_id"] for s in result["statistics"]] == ["sensor.a"]


# --------------------------------------------------------------------------
# Filtering (the purge_entity_statistics service)
# --------------------------------------------------------------------------

def test_filters_by_entity_id(db_session, mock_hass, now_ts):
    a = _add_meta(db_session, "sensor.a")
    b = _add_meta(db_session, "sensor.b")
    _add_hours(db_session, a.id, now_ts, [400])
    _add_hours(db_session, b.id, now_ts, [400])

    purge_statistics_sync(mock_hass, keep_days=90, entity_id=["sensor.a"])

    db_session.expire_all()
    assert _remaining(db_session, Statistics, a.id) == []
    assert len(_remaining(db_session, Statistics, b.id)) == 1


def test_filters_by_entity_glob(db_session, mock_hass, now_ts):
    a = _add_meta(db_session, "sensor.solar_today")
    b = _add_meta(db_session, "sensor.solar_total")
    c = _add_meta(db_session, "sensor.water")
    for meta in (a, b, c):
        _add_hours(db_session, meta.id, now_ts, [400])

    purge_statistics_sync(
        mock_hass, keep_days=90, entity_globs=["sensor.solar_*"],
    )

    db_session.expire_all()
    assert _remaining(db_session, Statistics, a.id) == []
    assert _remaining(db_session, Statistics, b.id) == []
    assert len(_remaining(db_session, Statistics, c.id)) == 1


def test_filters_by_domain(db_session, mock_hass, now_ts):
    a = _add_meta(db_session, "sensor.a")
    b = _add_meta(db_session, "number.b")
    _add_hours(db_session, a.id, now_ts, [400])
    _add_hours(db_session, b.id, now_ts, [400])

    purge_statistics_sync(mock_hass, keep_days=90, domains=["sensor"])

    db_session.expire_all()
    assert _remaining(db_session, Statistics, a.id) == []
    assert len(_remaining(db_session, Statistics, b.id)) == 1


def test_filters_are_a_union(db_session, mock_hass, now_ts):
    a = _add_meta(db_session, "sensor.a")
    b = _add_meta(db_session, "number.b")
    c = _add_meta(db_session, "binary_sensor.c")
    for meta in (a, b, c):
        _add_hours(db_session, meta.id, now_ts, [400])

    purge_statistics_sync(
        mock_hass, keep_days=90, entity_id=["number.b"], domains=["sensor"],
    )

    db_session.expire_all()
    assert _remaining(db_session, Statistics, a.id) == []
    assert _remaining(db_session, Statistics, b.id) == []
    assert len(_remaining(db_session, Statistics, c.id)) == 1


def test_external_statistics_are_matched_by_their_own_prefix(
    db_session, mock_hass, now_ts,
):
    """External statistic ids use `source:name`, not `domain.entity`, so a
    domain filter must not sweep them up."""
    ext = _add_meta(db_session, "energy:solar_production")
    _add_hours(db_session, ext.id, now_ts, [400])

    purge_statistics_sync(mock_hass, keep_days=90, domains=["energy"])

    db_session.expire_all()
    assert len(_remaining(db_session, Statistics, ext.id)) == 1


def test_unfiltered_purge_covers_every_statistic(db_session, mock_hass, now_ts):
    a = _add_meta(db_session, "sensor.a")
    ext = _add_meta(db_session, "energy:solar_production")
    _add_hours(db_session, a.id, now_ts, [400])
    _add_hours(db_session, ext.id, now_ts, [400])

    purge_statistics_sync(mock_hass, keep_days=90)

    db_session.expire_all()
    assert _remaining(db_session, Statistics, a.id) == []
    assert _remaining(db_session, Statistics, ext.id) == []


def test_filter_matching_nothing_deletes_nothing(db_session, mock_hass, now_ts):
    a = _add_meta(db_session, "sensor.a")
    _add_hours(db_session, a.id, now_ts, [400])

    result = purge_statistics_sync(
        mock_hass, keep_days=90, entity_id=["sensor.does_not_exist"],
    )

    assert result["success"] is True
    assert result["total_deleted"] == 0
    db_session.expire_all()
    assert len(_remaining(db_session, Statistics, a.id)) == 1


# --------------------------------------------------------------------------
# Write-lock duration.  Reported on #78 against a production database: the
# recorder's own writes failed with "database is locked" while a large purge
# ran, because the whole delete was one transaction with a single commit at
# the end.  Bulk recalculation already chunks its commits for exactly this
# reason (RECALC_CHUNK_SHORT_TERM); the purge has to as well.
# --------------------------------------------------------------------------

def _count_commits(db_session, monkeypatch):
    commits = []
    original = db_session.commit

    def counting_commit():
        commits.append(1)
        return original()

    monkeypatch.setattr(db_session, "commit", counting_commit)
    return commits


def test_large_purge_commits_in_chunks(db_session, mock_hass, now_ts, monkeypatch):
    from custom_components.history_editor import statistics as stats_mod

    monkeypatch.setattr(stats_mod, "PURGE_CHUNK_SIZE", 50)
    meta = _add_meta(db_session, "sensor.energy")
    _add_many_hours(db_session, meta.id, now_ts, 220)
    commits = _count_commits(db_session, monkeypatch)

    result = purge_statistics_sync(mock_hass, keep_days=90)

    assert result["total_deleted"] == 220
    # 220 rows in chunks of 50 cannot be one long write transaction.
    assert len(commits) >= 4, f"only {len(commits)} commit(s)"
    db_session.expire_all()
    assert _remaining(db_session, Statistics, meta.id) == []


def test_chunking_is_bounded_by_the_recorder_bind_var_limit(
    db_session, mock_hass, now_ts, monkeypatch,
):
    """Deleting by primary key means one bind variable per row, so the chunk
    must never exceed what the database driver accepts."""
    from custom_components.history_editor import statistics as stats_mod

    monkeypatch.setattr(stats_mod, "PURGE_CHUNK_SIZE", 10_000)
    recorder = stats_mod.get_instance(mock_hass)
    monkeypatch.setattr(recorder, "max_bind_vars", 40, raising=False)

    meta = _add_meta(db_session, "sensor.energy")
    _add_many_hours(db_session, meta.id, now_ts, 130)
    commits = _count_commits(db_session, monkeypatch)

    result = purge_statistics_sync(mock_hass, keep_days=90)

    assert result["total_deleted"] == 130
    assert len(commits) >= 4, f"only {len(commits)} commit(s)"


def test_dry_run_takes_no_write_lock(db_session, mock_hass, now_ts, monkeypatch):
    meta = _add_meta(db_session, "sensor.energy")
    _add_many_hours(db_session, meta.id, now_ts, 20)
    commits = _count_commits(db_session, monkeypatch)

    purge_statistics_sync(mock_hass, keep_days=90, dry_run=True)

    assert commits == []
