"""Statistics recalculation helpers for the History Editor component.

This module owns everything that touches HA's ``Statistics`` and
``StatisticsShortTerm`` tables.  It is separated from ``__init__.py`` because
the recalculation logic (sum cascading, 5-min/hourly aggregation, hold-last-
value carry-forward) is subtle and benefits from a focused home.

All DB access is synchronous; callers must dispatch these functions via
``hass.async_add_executor_job`` from async contexts.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.db_schema import States, StatesMeta
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util

from .schema_compat import ensure_schema_current

_LOGGER = logging.getLogger(__name__)


def _check_schema(hass: HomeAssistant) -> dict[str, Any] | None:
    """Return an error response dict if the recorder schema is stale, else None."""
    err = ensure_schema_current(hass)
    if err:
        return {"success": False, "error": err}
    return None

# Try to import statistics tables if available (newer HA versions).
# The module stays importable on older HA releases; HAS_STATISTICS gates all
# public entry points so callers can check capability before dispatching.
try:
    from homeassistant.components.recorder.db_schema import (
        Statistics,
        StatisticsMeta,
        StatisticsShortTerm,
    )
    HAS_STATISTICS = True
    HAS_STATISTICS_SHORT_TERM = True
except ImportError:
    HAS_STATISTICS = False
    HAS_STATISTICS_SHORT_TERM = False
    _LOGGER.debug("Statistics tables not available in this HA version")

# Statistics period durations in seconds
SHORT_TERM_PERIOD_SECONDS = 300    # 5-minute short-term statistics
LONG_TERM_PERIOD_SECONDS = 3600    # 1-hour long-term statistics

# Bulk-recalc chunk size: commit + expire after this many periods to bound
# the duration of the recorder write lock and to refresh the ORM identity
# map.  288 short-term periods = 1 day; 24 long-term periods = 1 day.
RECALC_CHUNK_SHORT_TERM = 288
RECALC_CHUNK_LONG_TERM = 24


def delete_short_term_stats_by_state_id(session, state_id: int) -> int:
    """Delete short-term statistics rows that reference the given state_id.

    Returns the number of rows deleted, or 0 if the schema does not support
    this relationship.  The ``state_id`` FK column existed on older HA
    releases (roughly 2023.x) and was removed in later versions; callers
    should treat a 0 return as "nothing to cascade" rather than an error.
    """
    if not HAS_STATISTICS_SHORT_TERM:
        return 0
    if not hasattr(StatisticsShortTerm, 'state_id'):
        return 0
    return session.query(StatisticsShortTerm).filter(
        StatisticsShortTerm.state_id == state_id
    ).delete(synchronize_session=False)


def _cascade_sum_adjustment(
    session,
    stat_meta_id: int,
    start_ts: float,
    delta: float,
) -> None:
    """Adjust running-total sum columns from the given period onwards.

    For sensors with state_class=total or state_class=total_increasing, the
    ``sum`` column is a running total that must remain consistent across all
    subsequent periods.  When the last-state value of a period changes by
    *delta*, every short-term and long-term statistics row from that period
    onward must receive the same adjustment.

    This mirrors the ``_adjust_sum_statistics`` helper in HA's recorder, which
    is not available on the public API.  We use ``synchronize_session=False``
    for bulk performance; the caller is responsible for flushing/expiring the
    session before reading these rows again.
    """
    if delta == 0:
        return

    session.query(StatisticsShortTerm).filter(
        StatisticsShortTerm.metadata_id == stat_meta_id,
        StatisticsShortTerm.start_ts >= start_ts,
        StatisticsShortTerm.sum.isnot(None),
    ).update(
        {StatisticsShortTerm.sum: StatisticsShortTerm.sum + delta},
        synchronize_session=False,
    )

    # Long-term (hourly) statistics: adjust from the containing hour onwards.
    start_ts_hour = float(int(start_ts // 3600) * 3600)
    session.query(Statistics).filter(
        Statistics.metadata_id == stat_meta_id,
        Statistics.start_ts >= start_ts_hour,
        Statistics.sum.isnot(None),
    ).update(
        {Statistics.sum: Statistics.sum + delta},
        synchronize_session=False,
    )


def recalculate_short_term_stat(
    session, stat_meta_id: int, entity_id: str, start_ts_5min: float,
) -> bool:
    """Recalculate and update a short-term statistics record for a 5-minute period.

    Queries all numeric states in the period, recomputes mean/min/max/state, and
    updates the existing StatisticsShortTerm row. Returns True if a row was updated.

    When no numeric states exist in the period (e.g. all records were deleted), the
    last numeric state value recorded *before* the period is carried forward so the
    statistics show a continuous "hold last value" line rather than a gap or stale data.
    If no prior value exists at all, the stale row is removed.

    For sensors that track a running total (state_class=total or
    state_class=total_increasing), the ``sum`` column is cascaded forward so
    that subsequent statistics periods remain consistent with the new value.
    """
    end_ts = start_ts_5min + 300.0

    states_in_period = (
        session.query(States)
        .join(StatesMeta, States.metadata_id == StatesMeta.metadata_id)
        .filter(StatesMeta.entity_id == entity_id)
        .filter(States.last_updated_ts >= start_ts_5min)
        .filter(States.last_updated_ts < end_ts)
        .order_by(States.last_updated_ts.asc())
        .all()
    )

    numeric_values: list[float] = []
    last_state_id: int | None = None
    last_state_float: float | None = None

    for s in states_in_period:
        try:
            v = float(s.state)
            numeric_values.append(v)
            last_state_id = s.state_id
            last_state_float = v
        except (ValueError, TypeError):
            pass

    short_term = session.query(StatisticsShortTerm).filter(
        StatisticsShortTerm.metadata_id == stat_meta_id,
        StatisticsShortTerm.start_ts == start_ts_5min,
    ).first()

    if short_term is None:
        return False

    # Capture old state value and whether this stat has a running sum before
    # making any modifications.  Used for cascading sum adjustments below.
    old_state: float | None = short_term.state
    has_sum: bool = short_term.sum is not None
    new_state: float | None = None

    if numeric_values:
        short_term.mean = sum(numeric_values) / len(numeric_values)
        short_term.min = min(numeric_values)
        short_term.max = max(numeric_values)
        short_term.state = last_state_float
        new_state = last_state_float
        if hasattr(short_term, 'state_id') and last_state_id is not None:
            short_term.state_id = last_state_id
    else:
        # No numeric states in this period (e.g. all records were deleted).
        # Carry forward the last known numeric value from before this period so the
        # statistics chart shows a continuous line rather than stale or missing data.
        prev_state = (
            session.query(States.state, States.last_updated_ts)
            .join(StatesMeta, States.metadata_id == StatesMeta.metadata_id)
            .filter(StatesMeta.entity_id == entity_id)
            .filter(States.last_updated_ts < start_ts_5min)
            .order_by(States.last_updated_ts.desc())
            .first()
        )
        prev_value: float | None = None
        if prev_state is not None:
            try:
                prev_value = float(prev_state.state)
            except (ValueError, TypeError):
                prev_value = None

        if prev_value is not None:
            short_term.mean = prev_value
            short_term.min = prev_value
            short_term.max = prev_value
            short_term.state = prev_value
            new_state = prev_value
        else:
            # No prior value available either — remove the now-meaningless row.
            session.delete(short_term)

    # For sensors with a running sum (total / total_increasing), cascade the
    # state-change delta to this period and every subsequent one so that the
    # energy dashboard and statistics panels remain accurate.
    # _cascade_sum_adjustment returns early when delta == 0, so no extra guard needed.
    if has_sum and old_state is not None and new_state is not None:
        _cascade_sum_adjustment(session, stat_meta_id, start_ts_5min, new_state - old_state)

    return True


def recalculate_long_term_stat(session, stat_meta_id: int, start_ts_hour: float) -> bool:
    """Recalculate and update a long-term statistics record for an hourly period.

    Aggregates the updated short-term stats in the hour and updates the Statistics row.
    Returns True if a row was updated.

    When no short-term rows remain for the period (e.g. all underlying records were
    deleted), the last short-term row recorded *before* the period is used to carry
    its value forward so the long-term statistics show a continuous line.  If no prior
    short-term row exists, the stale long-term row is removed.
    """
    end_ts = start_ts_hour + 3600.0

    short_terms = (
        session.query(StatisticsShortTerm)
        .filter(
            StatisticsShortTerm.metadata_id == stat_meta_id,
            StatisticsShortTerm.start_ts >= start_ts_hour,
            StatisticsShortTerm.start_ts < end_ts,
        )
        .order_by(StatisticsShortTerm.start_ts.asc())
        .all()
    )

    long_term = session.query(Statistics).filter(
        Statistics.metadata_id == stat_meta_id,
        Statistics.start_ts == start_ts_hour,
    ).first()

    if not short_terms:
        # No short-term stats remain in this period (e.g. all underlying records were
        # deleted). Carry forward the last known short-term value from before this period
        # so the long-term chart shows a continuous line rather than stale or missing data.
        prev_short_term = (
            session.query(
                StatisticsShortTerm.mean,
                StatisticsShortTerm.min,
                StatisticsShortTerm.max,
                StatisticsShortTerm.state,
            )
            .filter(
                StatisticsShortTerm.metadata_id == stat_meta_id,
                StatisticsShortTerm.start_ts < start_ts_hour,
            )
            .order_by(StatisticsShortTerm.start_ts.desc())
            .first()
        )
        if long_term is not None:
            if prev_short_term is not None and prev_short_term.state is not None:
                prev_value = prev_short_term.state
                long_term.mean = prev_short_term.mean if prev_short_term.mean is not None else prev_value
                long_term.min = prev_short_term.min if prev_short_term.min is not None else prev_value
                long_term.max = prev_short_term.max if prev_short_term.max is not None else prev_value
                long_term.state = prev_value
                # Note: ``sum`` is intentionally not touched here.  When short-term
                # rows are gone because the recorder purged them, the existing
                # long-term sum is authoritative and must be preserved.  When
                # short-term rows are gone because the user deleted them via this
                # component, any sum delta was already applied via
                # _cascade_sum_adjustment at deletion time.
            else:
                # No prior value available — remove the now-meaningless row.
                session.delete(long_term)
            return True
        return False

    if long_term is None:
        return False

    means = [s.mean for s in short_terms if s.mean is not None]
    mins = [s.min for s in short_terms if s.min is not None]
    maxs = [s.max for s in short_terms if s.max is not None]
    states = [s.state for s in short_terms if s.state is not None]

    if means:
        # Average the short-term means, consistent with how HA aggregates hourly statistics
        long_term.mean = sum(means) / len(means)
    if mins:
        long_term.min = min(mins)
    if maxs:
        long_term.max = max(maxs)
    if states:
        long_term.state = states[-1]  # last value in the hourly period

    # For total/total_increasing sensors the long-term sum equals the sum of
    # the last short-term row in the hour.  The short-term sums were already
    # cascaded forward by recalculate_short_term_stat; we only need to mirror
    # the last one here so the hourly row stays in sync.
    sums = [s.sum for s in short_terms if s.sum is not None]
    if sums:
        long_term.sum = sums[-1]

    return True


def update_statistics_for_periods(
    session,
    metadata_to_5min: dict[int, set[float]],
    metadata_to_hour: dict[int, set[float]],
) -> tuple[int, int]:
    """Recalculate short-term and long-term stats for a batch of affected
    periods.  Used by the bulk state-mutation paths to dedupe per-period
    work — collect every affected (metadata_id, 5-min start) and
    (metadata_id, hour start) across the whole batch, then call this once.

    Honours the cross-phase ordering invariant: short-term periods are
    processed in chronological order (so sum cascades compound), then the
    session is flushed + expired, then long-term periods are processed.

    Returns ``(short_term_updated, long_term_updated)``.  Silently no-ops
    when the statistics schema is unavailable.
    """
    if not HAS_STATISTICS:
        return 0, 0

    short_updated = 0
    long_updated = 0

    for metadata_id, periods_5min in metadata_to_5min.items():
        meta = session.query(StatesMeta).filter(
            StatesMeta.metadata_id == metadata_id
        ).first()
        if meta is None:
            continue
        stat_meta = session.query(StatisticsMeta).filter(
            StatisticsMeta.statistic_id == meta.entity_id
        ).first()
        if stat_meta is None:
            continue
        stat_meta_id = stat_meta.id

        for ts in sorted(periods_5min):
            if recalculate_short_term_stat(session, stat_meta_id, meta.entity_id, ts):
                short_updated += 1

        # Phase boundary: flush + expire before long-term reads cached short-term sums.
        session.flush()
        session.expire_all()

        for ts in metadata_to_hour.get(metadata_id, set()):
            if recalculate_long_term_stat(session, stat_meta_id, ts):
                long_updated += 1

    return short_updated, long_updated


def update_statistics_after_state_change(
    session,
    state_record,
    old_ts: float | None,
    new_last_updated: datetime | None,
) -> None:
    """Update short-term and long-term statistics affected by a state record change.

    Determines which 5-minute and hourly periods are affected (the period of the
    old timestamp and/or the new timestamp), recalculates statistics for those
    periods from the underlying state data, and persists the results.
    """
    # Collect affected period timestamps
    affected_5min: set[float] = set()
    affected_hour: set[float] = set()

    def _add_periods(ts: float) -> None:
        p5 = float(int(ts // 300) * 300)
        ph = float(int(ts // 3600) * 3600)
        affected_5min.add(p5)
        affected_hour.add(ph)

    if old_ts is not None:
        _add_periods(old_ts)

    # New timestamp after update
    new_ts: float | None = None
    if new_last_updated is not None:
        new_ts = new_last_updated.timestamp()
    elif hasattr(state_record, 'last_updated_ts') and state_record.last_updated_ts is not None:
        new_ts = state_record.last_updated_ts

    if new_ts is not None:
        _add_periods(new_ts)

    if not affected_5min:
        return

    # Resolve entity_id and StatisticsMeta
    meta = session.query(StatesMeta).filter(
        StatesMeta.metadata_id == state_record.metadata_id
    ).first()
    if meta is None:
        return
    entity_id = meta.entity_id

    stat_meta = session.query(StatisticsMeta).filter(
        StatisticsMeta.statistic_id == entity_id
    ).first()
    if stat_meta is None:
        return

    # Capture the primary key before we flush/expire the session so it
    # remains accessible afterwards without an extra DB round-trip.
    stat_meta_id = stat_meta.id

    # Recalculate short-term stats for all affected 5-minute periods.
    # Process in chronological order so earlier sum-cascade adjustments are
    # applied before later periods are recalculated.
    updated_5min = 0
    for start_ts in sorted(affected_5min):
        if recalculate_short_term_stat(session, stat_meta_id, entity_id, start_ts):
            updated_5min += 1

    # Flush pending ORM changes and expire all cached objects so that the
    # long-term recalculation reads the freshly-updated short-term rows
    # (including any sum-cascade bulk UPDATEs that used synchronize_session=False).
    if updated_5min:
        session.flush()
        session.expire_all()

    # Recalculate long-term stats for all affected hourly periods
    updated_hour = 0
    for start_ts in affected_hour:
        if recalculate_long_term_stat(session, stat_meta_id, start_ts):
            updated_hour += 1

    if updated_5min or updated_hour:
        _LOGGER.info(
            "Updated statistics for entity %s: %d short-term and %d long-term period(s)",
            entity_id, updated_5min, updated_hour,
        )


def get_statistics_sync(
    hass: HomeAssistant,
    entity_id: str,
    start_time: datetime | None,
    end_time: datetime | None,
    limit: int,
    statistic_type: str = "long_term",
) -> dict[str, Any]:
    """Get statistics records for an entity (synchronous)."""
    schema_err = _check_schema(hass)
    if schema_err:
        return schema_err
    if not HAS_STATISTICS:
        return {"success": False, "error": "Statistics tables not available in this HA version"}

    recorder = get_instance(hass)
    if recorder is None:
        return {"success": False, "error": "Recorder not available"}

    try:
        table = StatisticsShortTerm if statistic_type == "short_term" else Statistics

        with recorder.get_session() as session:
            query = (
                session.query(table)
                .join(StatisticsMeta, table.metadata_id == StatisticsMeta.id)
                .filter(StatisticsMeta.statistic_id == entity_id)
            )

            if start_time:
                query = query.filter(table.start_ts >= start_time.timestamp())
            if end_time:
                query = query.filter(table.start_ts <= end_time.timestamp())

            # Fetch one extra record to determine whether more records exist
            fetch_limit = limit + 1
            query = query.order_by(table.start_ts.desc()).limit(fetch_limit)
            stats = query.all()
            has_more = len(stats) > limit
            if has_more:
                stats = stats[:limit]

            records = []
            for stat in stats:
                start_iso = None
                if stat.start_ts is not None:
                    start_iso = dt_util.utc_from_timestamp(stat.start_ts).isoformat()
                last_reset_iso = None
                if hasattr(stat, 'last_reset_ts') and stat.last_reset_ts is not None:
                    last_reset_iso = dt_util.utc_from_timestamp(stat.last_reset_ts).isoformat()

                # Determine whether this record is locked by underlying source data
                has_source_data = False
                if stat.start_ts is not None:
                    if statistic_type == "short_term":
                        # Locked when the record falls within the recorder keep period,
                        # since state history exists for that entire period and HA can
                        # recalculate any short-term stat within it.
                        try:
                            keep_days = getattr(recorder, "keep_days", 10)
                            cutoff_ts = (dt_util.utcnow() - timedelta(days=keep_days)).timestamp()
                            has_source_data = stat.start_ts >= cutoff_ts
                        except Exception as check_err:
                            _LOGGER.debug("has_source_data check failed for short-term stat %s: %s", stat.id, check_err)
                    else:
                        # long_term: locked when short-term stats exist in the 1-hour period
                        try:
                            short_term_count = (
                                session.query(StatisticsShortTerm)
                                .filter(StatisticsShortTerm.metadata_id == stat.metadata_id)
                                .filter(StatisticsShortTerm.start_ts >= stat.start_ts)
                                .filter(StatisticsShortTerm.start_ts < stat.start_ts + 3600.0)
                                .count()
                            )
                            has_source_data = short_term_count > 0
                        except Exception as check_err:
                            _LOGGER.debug("has_source_data check failed for long-term stat %s: %s", stat.id, check_err)

                records.append({
                    "id": stat.id,
                    "statistic_id": entity_id,
                    "statistic_type": statistic_type,
                    "start": start_iso,
                    "mean": stat.mean,
                    "min": stat.min,
                    "max": stat.max,
                    "sum": stat.sum,
                    "state": stat.state,
                    "last_reset": last_reset_iso,
                    "has_source_data": has_source_data,
                })

            _LOGGER.debug("Retrieved %d statistics records for entity %s", len(records), entity_id)
            return {"success": True, "records": records, "has_more": has_more}
    except Exception as err:
        _LOGGER.error("Error retrieving statistics: %s", err, exc_info=True)
        return {"success": False, "error": str(err)}


def update_statistic_sync(
    hass: HomeAssistant,
    stat_id: int,
    mean: float | None,
    min_val: float | None,
    max_val: float | None,
    sum_val: float | None,
    state: float | None,
    start: datetime | None,
    statistic_type: str = "long_term",
) -> dict[str, Any]:
    """Update a statistics record (synchronous)."""
    schema_err = _check_schema(hass)
    if schema_err:
        return schema_err
    if not HAS_STATISTICS:
        return {"success": False, "error": "Statistics tables not available in this HA version"}

    recorder = get_instance(hass)
    if recorder is None:
        return {"success": False, "error": "Recorder not available"}

    try:
        table = StatisticsShortTerm if statistic_type == "short_term" else Statistics

        with recorder.get_session() as session:
            stat = session.query(table).filter(table.id == stat_id).first()

            if stat is None:
                return {"success": False, "error": f"Statistic ID {stat_id} not found"}

            # Guard: reject direct edits when underlying source data exists
            if stat.start_ts is not None:
                if statistic_type == "short_term":
                    stat_meta_row = session.query(StatisticsMeta).filter(
                        StatisticsMeta.id == stat.metadata_id
                    ).first()
                    if stat_meta_row is not None:
                        try:
                            state_count = (
                                session.query(States)
                                .join(StatesMeta, States.metadata_id == StatesMeta.metadata_id)
                                .filter(StatesMeta.entity_id == stat_meta_row.statistic_id)
                                .filter(States.last_updated_ts >= stat.start_ts)
                                .filter(States.last_updated_ts < stat.start_ts + 300.0)
                                .count()
                            )
                            if state_count > 0:
                                return {
                                    "success": False,
                                    "error": (
                                        "Cannot edit short-term statistics directly: state history "
                                        "records exist for this 5-minute period. Edit the state "
                                        "history instead, or wait for the recorder to purge those "
                                        "states (default: 10 days), after which this row becomes "
                                        "editable."
                                    ),
                                }
                        except Exception as check_err:
                            _LOGGER.warning("Source-data check failed for short-term stat %s: %s", stat_id, check_err)
                else:
                    try:
                        short_term_count = (
                            session.query(StatisticsShortTerm)
                            .filter(StatisticsShortTerm.metadata_id == stat.metadata_id)
                            .filter(StatisticsShortTerm.start_ts >= stat.start_ts)
                            .filter(StatisticsShortTerm.start_ts < stat.start_ts + 3600.0)
                            .count()
                        )
                        if short_term_count > 0:
                            return {
                                "success": False,
                                "error": (
                                    "Cannot edit long-term statistics directly: short-term statistics "
                                    "records exist for this hour. Edit the short-term statistics "
                                    "instead, or wait for the recorder to purge them (default: 10 "
                                    "days), after which this long-term row becomes editable."
                                ),
                            }
                    except Exception as check_err:
                        _LOGGER.warning("Source-data check failed for long-term stat %s: %s", stat_id, check_err)

            if mean is not None:
                stat.mean = float(mean)
            if min_val is not None:
                stat.min = float(min_val)
            if max_val is not None:
                stat.max = float(max_val)
            if sum_val is not None:
                stat.sum = float(sum_val)
            if state is not None:
                stat.state = float(state)
            if start is not None:
                stat.start_ts = start.timestamp()

            # Flush to make changes visible before cascading to long-term stats
            session.flush()

            # Cascade: after updating a short-term stat, recalculate the corresponding long-term stat
            if statistic_type == "short_term":
                try:
                    effective_ts = start.timestamp() if start is not None else stat.start_ts
                    if effective_ts is not None:
                        start_ts_hour = float(int(effective_ts // 3600) * 3600)
                        recalculate_long_term_stat(session, stat.metadata_id, start_ts_hour)
                except Exception as cascade_err:
                    _LOGGER.warning(
                        "Error updating long-term stat after short-term edit for stat %s: %s",
                        stat_id, cascade_err
                    )

            session.commit()
            _LOGGER.info("Updated statistic record %s", stat_id)
            return {"success": True, "id": stat_id}
    except Exception as err:
        _LOGGER.error("Error updating statistic: %s", err)
        return {"success": False, "error": str(err)}


def delete_statistic_sync(
    hass: HomeAssistant,
    stat_id: int,
    statistic_type: str = "long_term",
) -> dict[str, Any]:
    """Delete a statistics record (synchronous)."""
    schema_err = _check_schema(hass)
    if schema_err:
        return schema_err
    if not HAS_STATISTICS:
        return {"success": False, "error": "Statistics tables not available in this HA version"}

    recorder = get_instance(hass)
    if recorder is None:
        return {"success": False, "error": "Recorder not available"}

    try:
        table = StatisticsShortTerm if statistic_type == "short_term" else Statistics

        with recorder.get_session() as session:
            stat = session.query(table).filter(table.id == stat_id).first()
            if stat is None:
                return {"success": False, "error": f"Statistic ID {stat_id} not found"}

            # Capture values needed for cascade before deletion
            stat_start_ts = stat.start_ts
            stat_metadata_id = stat.metadata_id

            # Guard: reject direct deletes when underlying source data exists
            if stat_start_ts is not None:
                if statistic_type == "short_term":
                    stat_meta_row = session.query(StatisticsMeta).filter(
                        StatisticsMeta.id == stat_metadata_id
                    ).first()
                    if stat_meta_row is not None:
                        try:
                            state_count = (
                                session.query(States)
                                .join(StatesMeta, States.metadata_id == StatesMeta.metadata_id)
                                .filter(StatesMeta.entity_id == stat_meta_row.statistic_id)
                                .filter(States.last_updated_ts >= stat_start_ts)
                                .filter(States.last_updated_ts < stat_start_ts + 300.0)
                                .count()
                            )
                            if state_count > 0:
                                return {
                                    "success": False,
                                    "error": (
                                        "Cannot delete short-term statistics directly: state history "
                                        "records exist for this 5-minute period. Delete the state "
                                        "history instead, or wait for the recorder to purge those "
                                        "states (default: 10 days), after which this row becomes "
                                        "deletable."
                                    ),
                                }
                        except Exception as check_err:
                            _LOGGER.warning("Source-data check failed for short-term stat %s: %s", stat_id, check_err)
                else:
                    try:
                        short_term_count = (
                            session.query(StatisticsShortTerm)
                            .filter(StatisticsShortTerm.metadata_id == stat_metadata_id)
                            .filter(StatisticsShortTerm.start_ts >= stat_start_ts)
                            .filter(StatisticsShortTerm.start_ts < stat_start_ts + 3600.0)
                            .count()
                        )
                        if short_term_count > 0:
                            return {
                                "success": False,
                                "error": (
                                    "Cannot delete long-term statistics directly: short-term statistics "
                                    "records exist for this hour. Delete the short-term statistics "
                                    "instead, or wait for the recorder to purge them (default: 10 "
                                    "days), after which this long-term row becomes deletable."
                                ),
                            }
                    except Exception as check_err:
                        _LOGGER.warning("Source-data check failed for long-term stat %s: %s", stat_id, check_err)

            # Overwrite with the previous period's values so the row
            # becomes "transparent" rather than creating a gap in the
            # continuous statistics that HA expects.
            neutralized = _neutralize_stat_row(session, table, stat)
            if not neutralized:
                # First row ever for this entity — no prior to carry forward;
                # actually remove it since there's no gap to fill.
                session.query(table).filter(table.id == stat_id).delete(
                    synchronize_session=False,
                )

            # Cascade: re-aggregate the containing long-term row from
            # the (now-neutralized or removed) short-term row.
            if statistic_type == "short_term" and stat_start_ts is not None:
                try:
                    session.flush()
                    start_ts_hour = float(int(stat_start_ts // 3600) * 3600)
                    recalculate_long_term_stat(session, stat_metadata_id, start_ts_hour)
                except Exception as cascade_err:
                    _LOGGER.warning(
                        "Error updating long-term stat after short-term delete for stat %s: %s",
                        stat_id, cascade_err
                    )

            session.commit()
            action = "neutralized" if neutralized else "deleted"
            _LOGGER.info("%s statistic record %s (type=%s)", action.capitalize(), stat_id, statistic_type)
            return {"success": True, "id": stat_id, "action": action}
    except Exception as err:
        _LOGGER.error("Error deleting statistic: %s", err)
        return {"success": False, "error": str(err)}


def _neutralize_stat_row(session, table, stat):
    """Overwrite a statistics row with the previous period's values.

    HA expects continuous statistics rows (one every 5 min for short-term,
    one every hour for long-term).  Rather than deleting a row and leaving a
    gap, we make it "transparent" by copying mean/min/max/state/sum from
    the preceding row of the same entity.

    Returns ``True`` if a prior row was found and the overwrite succeeded,
    or ``False`` if this is the very first row for the entity (no prior to
    carry forward) — in which case the caller should fall back to an actual
    delete.
    """
    prev = (
        session.query(table)
        .filter(
            table.metadata_id == stat.metadata_id,
            table.start_ts < stat.start_ts,
        )
        .order_by(table.start_ts.desc())
        .first()
    )
    if prev is None:
        return False

    stat.mean = prev.mean
    stat.min = prev.min
    stat.max = prev.max
    stat.state = prev.state
    stat.sum = prev.sum
    return True


def _check_source_data_blocks_edit(
    session, stat, statistic_type: str,
) -> str | None:
    """Return a human-readable error if direct edits/deletes of ``stat`` are
    blocked by the presence of underlying source data, or ``None`` if the
    operation is allowed.

    For short-term rows: blocked when state history records exist in the same
    5-minute period.  For long-term rows: blocked when short-term records
    exist in the containing hour.  This mirrors the guards in the singular
    update/delete helpers and is reused by the bulk variants.
    """
    if stat.start_ts is None:
        return None

    if statistic_type == "short_term":
        stat_meta_row = session.query(StatisticsMeta).filter(
            StatisticsMeta.id == stat.metadata_id
        ).first()
        if stat_meta_row is None:
            return None
        try:
            state_count = (
                session.query(States)
                .join(StatesMeta, States.metadata_id == StatesMeta.metadata_id)
                .filter(StatesMeta.entity_id == stat_meta_row.statistic_id)
                .filter(States.last_updated_ts >= stat.start_ts)
                .filter(States.last_updated_ts < stat.start_ts + 300.0)
                .count()
            )
        except Exception as check_err:
            _LOGGER.warning("Source-data check failed for short-term stat %s: %s", stat.id, check_err)
            return None
        if state_count > 0:
            return (
                "state history records exist for this 5-minute period; edit "
                "the state history instead, or wait for the recorder to purge "
                "(default: 10 days)"
            )
        return None

    # long_term
    try:
        short_term_count = (
            session.query(StatisticsShortTerm)
            .filter(StatisticsShortTerm.metadata_id == stat.metadata_id)
            .filter(StatisticsShortTerm.start_ts >= stat.start_ts)
            .filter(StatisticsShortTerm.start_ts < stat.start_ts + 3600.0)
            .count()
        )
    except Exception as check_err:
        _LOGGER.warning("Source-data check failed for long-term stat %s: %s", stat.id, check_err)
        return None
    if short_term_count > 0:
        return (
            "short-term statistics records exist for this hour; edit the "
            "short-term statistics instead, or wait for the recorder to purge "
            "(default: 10 days)"
        )
    return None


def bulk_update_statistic_sync(
    hass: HomeAssistant,
    ids: list[int],
    mean: float | None,
    min_val: float | None,
    max_val: float | None,
    sum_val: float | None,
    state: float | None,
    statistic_type: str = "long_term",
) -> dict[str, Any]:
    """Apply the same column overrides to multiple statistics rows.

    Each row is independently subjected to the source-data guard.  Rows that
    are blocked or missing are reported in the result but do not abort the
    rest of the batch.  For short-term updates, the long-term cascade runs
    once per affected hour at the end (deduplicated) instead of per-row.

    Returns a dict with:
      - ``success``: True unless an unrecoverable error occurred
      - ``updated_count``: number of rows successfully updated
      - ``blocked``: list of ``{id, reason}`` for rows blocked by guards
      - ``not_found``: list of ids that did not match any row
    """
    schema_err = _check_schema(hass)
    if schema_err:
        return schema_err
    if not HAS_STATISTICS:
        return {"success": False, "error": "Statistics tables not available in this HA version"}
    if not ids:
        return {"success": False, "error": "ids must be a non-empty list"}
    if all(v is None for v in (mean, min_val, max_val, sum_val, state)):
        return {"success": False, "error": "no fields to update"}

    recorder = get_instance(hass)
    if recorder is None:
        return {"success": False, "error": "Recorder not available"}

    table = StatisticsShortTerm if statistic_type == "short_term" else Statistics

    try:
        with recorder.get_session() as session:
            updated_count = 0
            blocked: list[dict[str, Any]] = []
            not_found: list[int] = []
            # Per-metadata_id, the set of hour starts that need long-term
            # recalculation after this batch.  Only used for short-term updates.
            affected_hours: dict[int, set[float]] = {}

            for stat_id in ids:
                stat = session.query(table).filter(table.id == stat_id).first()
                if stat is None:
                    not_found.append(stat_id)
                    continue

                guard_reason = _check_source_data_blocks_edit(
                    session, stat, statistic_type,
                )
                if guard_reason is not None:
                    blocked.append({"id": stat_id, "reason": guard_reason})
                    continue

                if mean is not None:
                    stat.mean = float(mean)
                if min_val is not None:
                    stat.min = float(min_val)
                if max_val is not None:
                    stat.max = float(max_val)
                if sum_val is not None:
                    stat.sum = float(sum_val)
                if state is not None:
                    stat.state = float(state)
                updated_count += 1

                if statistic_type == "short_term" and stat.start_ts is not None:
                    hour_start = float(int(stat.start_ts // 3600) * 3600)
                    affected_hours.setdefault(stat.metadata_id, set()).add(hour_start)

            session.flush()

            # Cascade: re-aggregate long-term rows for every affected hour.
            # Done once per hour (not per short-term row) to avoid redundant work.
            if statistic_type == "short_term" and affected_hours:
                for metadata_id, hours in affected_hours.items():
                    for hour in hours:
                        try:
                            recalculate_long_term_stat(session, metadata_id, hour)
                        except Exception as cascade_err:
                            _LOGGER.warning(
                                "Error updating long-term stat after bulk short-term "
                                "edit (metadata_id=%s, hour=%s): %s",
                                metadata_id, hour, cascade_err,
                            )

            session.commit()
            _LOGGER.info(
                "Bulk-updated %d statistics rows (type=%s); %d blocked, %d not found",
                updated_count, statistic_type, len(blocked), len(not_found),
            )
            return {
                "success": True,
                "updated_count": updated_count,
                "blocked": blocked,
                "not_found": not_found,
            }
    except Exception as err:
        _LOGGER.error("Error in bulk_update_statistic: %s", err, exc_info=True)
        return {"success": False, "error": str(err)}


def bulk_delete_statistic_sync(
    hass: HomeAssistant,
    ids: list[int],
    statistic_type: str = "long_term",
) -> dict[str, Any]:
    """Neutralize multiple statistics rows by overwriting them with the
    previous period's values (or actually delete if no prior exists).

    Same guard / reporting semantics as ``bulk_update_statistic_sync``.
    For short-term rows, the long-term cascade runs once per affected hour.
    """
    schema_err = _check_schema(hass)
    if schema_err:
        return schema_err
    if not HAS_STATISTICS:
        return {"success": False, "error": "Statistics tables not available in this HA version"}
    if not ids:
        return {"success": False, "error": "ids must be a non-empty list"}

    recorder = get_instance(hass)
    if recorder is None:
        return {"success": False, "error": "Recorder not available"}

    table = StatisticsShortTerm if statistic_type == "short_term" else Statistics

    try:
        with recorder.get_session() as session:
            deleted_count = 0
            blocked: list[dict[str, Any]] = []
            not_found: list[int] = []
            affected_hours: dict[int, set[float]] = {}

            # Load all target rows first, then sort by start_ts ascending.
            # Processing in chronological order is essential: when
            # consecutive rows [A, B, C] are all "deleted", A is
            # neutralized from its prior P, then B copies from the
            # now-neutralized A (= P), then C copies from B (= P).
            # Reverse order would leave C with B's original bad value.
            rows_to_process = []
            for stat_id in ids:
                stat = session.query(table).filter(table.id == stat_id).first()
                if stat is None:
                    not_found.append(stat_id)
                    continue

                guard_reason = _check_source_data_blocks_edit(
                    session, stat, statistic_type,
                )
                if guard_reason is not None:
                    blocked.append({"id": stat_id, "reason": guard_reason})
                    continue

                rows_to_process.append(stat)

            rows_to_process.sort(key=lambda s: s.start_ts if s.start_ts is not None else 0)

            for stat in rows_to_process:
                if statistic_type == "short_term" and stat.start_ts is not None:
                    hour_start = float(int(stat.start_ts // 3600) * 3600)
                    affected_hours.setdefault(stat.metadata_id, set()).add(hour_start)

                neutralized = _neutralize_stat_row(session, table, stat)
                if not neutralized:
                    session.delete(stat)
                deleted_count += 1

            session.flush()

            if statistic_type == "short_term" and affected_hours:
                for metadata_id, hours in affected_hours.items():
                    for hour in hours:
                        try:
                            recalculate_long_term_stat(session, metadata_id, hour)
                        except Exception as cascade_err:
                            _LOGGER.warning(
                                "Error updating long-term stat after bulk short-term "
                                "delete (metadata_id=%s, hour=%s): %s",
                                metadata_id, hour, cascade_err,
                            )

            session.commit()
            _LOGGER.info(
                "Bulk-neutralized %d statistics rows (type=%s); %d blocked, %d not found",
                deleted_count, statistic_type, len(blocked), len(not_found),
            )
            return {
                "success": True,
                "deleted_count": deleted_count,
                "blocked": blocked,
                "not_found": not_found,
            }
    except Exception as err:
        _LOGGER.error("Error in bulk_delete_statistic: %s", err, exc_info=True)
        return {"success": False, "error": str(err)}


def recalculate_statistics_sync(
    hass: HomeAssistant,
    entity_id: str,
    start_time: datetime,
    end_time: datetime,
    statistic_type: str = "both",
) -> dict[str, Any]:
    """Force recalculation of statistics for an entity over a time range (synchronous).

    For ``statistic_type`` values:
    - ``"short_term"`` – recalculates every 5-minute StatisticsShortTerm row whose
      period overlaps [start_time, end_time) from the underlying state history.
    - ``"long_term"`` – recalculates every hourly Statistics row whose period
      overlaps [start_time, end_time) by re-aggregating the short-term rows.
    - ``"both"`` (default) – recalculates short-term first, then long-term.
    """
    schema_err = _check_schema(hass)
    if schema_err:
        return schema_err
    if not HAS_STATISTICS:
        return {"success": False, "error": "Statistics tables not available in this HA version"}

    recorder = get_instance(hass)
    if recorder is None:
        return {"success": False, "error": "Recorder not available"}

    start_ts = start_time.timestamp()
    end_ts = end_time.timestamp()

    if end_ts <= start_ts:
        return {"success": False, "error": "end_time must be after start_time"}

    try:
        with recorder.get_session() as session:
            # Resolve the StatisticsMeta row for this entity
            stat_meta = session.query(StatisticsMeta).filter(
                StatisticsMeta.statistic_id == entity_id
            ).first()
            if stat_meta is None:
                return {
                    "success": False,
                    "error": f"No statistics metadata found for entity '{entity_id}'",
                }
            stat_meta_id = stat_meta.id

            updated_short_term = 0
            updated_long_term = 0

            # Recalculate short-term (5-minute) statistics from state history.
            # Commit + expire in chunks to bound the recorder write-lock duration
            # and to flush any cascade-updated sum columns (which bypassed the
            # ORM via synchronize_session=False) so later iterations read fresh
            # values instead of stale identity-map entries.
            if statistic_type in ("short_term", "both"):
                ts = float(int(start_ts // SHORT_TERM_PERIOD_SECONDS) * SHORT_TERM_PERIOD_SECONDS)
                periods_in_chunk = 0
                while ts < end_ts:
                    if recalculate_short_term_stat(session, stat_meta_id, entity_id, ts):
                        updated_short_term += 1
                    periods_in_chunk += 1
                    ts += SHORT_TERM_PERIOD_SECONDS
                    if periods_in_chunk >= RECALC_CHUNK_SHORT_TERM:
                        session.commit()
                        session.expire_all()
                        periods_in_chunk = 0

            # Phase boundary: commit + expire so the long-term pass reads the
            # freshly-updated short-term rows.  Without this, long-term
            # aggregation computes sums from stale cached short-term.sum
            # values for total / total_increasing sensors.
            session.commit()
            session.expire_all()

            # Recalculate long-term (hourly) statistics from short-term statistics
            if statistic_type in ("long_term", "both"):
                ts = float(int(start_ts // LONG_TERM_PERIOD_SECONDS) * LONG_TERM_PERIOD_SECONDS)
                periods_in_chunk = 0
                while ts < end_ts:
                    if recalculate_long_term_stat(session, stat_meta_id, ts):
                        updated_long_term += 1
                    periods_in_chunk += 1
                    ts += LONG_TERM_PERIOD_SECONDS
                    if periods_in_chunk >= RECALC_CHUNK_LONG_TERM:
                        session.commit()
                        session.expire_all()
                        periods_in_chunk = 0

            session.commit()

            _LOGGER.info(
                "Recalculated statistics for entity %s (%s to %s): "
                "%d short-term and %d long-term period(s) updated",
                entity_id,
                start_time.isoformat(),
                end_time.isoformat(),
                updated_short_term,
                updated_long_term,
            )
            return {
                "success": True,
                "entity_id": entity_id,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "statistic_type": statistic_type,
                "updated_short_term": updated_short_term,
                "updated_long_term": updated_long_term,
            }
    except Exception as err:
        _LOGGER.error("Error recalculating statistics: %s", err, exc_info=True)
        return {"success": False, "error": str(err)}
