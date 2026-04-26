"""Recorder schema compatibility checks.

Probes the actual database columns via SQLAlchemy ``inspect()`` and
compares them against the columns this component reads and writes.
Blocks setup or individual operations when the schema has drifted
(e.g. after a HA upgrade that renames or drops columns).

Two-layer design:
  1. ``validate_schema_sync(hass)`` — called once at ``async_setup`` via
     the executor.  Returns a list of errors (empty = OK).
  2. ``ensure_schema_current(hass)`` — called at the top of every sync
     helper.  Compares ``homeassistant.const.__version__`` against the
     version that was validated at startup; if it changed, re-probes.
     Returns an error string or ``None``.
"""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.db_schema import States, StatesMeta
from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

# Columns we READ or WRITE per table.  If any of these are missing from
# the actual DB, we refuse to operate — a silent mismatch would corrupt data.
# Columns we only *conditionally* use via ``hasattr`` guards (e.g.
# ``last_changed_ts``) are listed under ``optional`` and don't block setup.
REQUIRED_COLUMNS: dict[str, dict[str, list[str]]] = {
    "states": {
        "required": ["state_id", "metadata_id", "state", "attributes"],
        "optional": [
            "last_changed", "last_changed_ts",
            "last_updated", "last_updated_ts",
            "old_state_id",
        ],
    },
    "states_meta": {
        "required": ["metadata_id", "entity_id"],
        "optional": [],
    },
}

# Statistics tables may not exist at all (older HA); that's handled by
# HAS_STATISTICS in statistics.py.  These are only checked when the tables
# *do* exist.
STATISTICS_COLUMNS: dict[str, dict[str, list[str]]] = {
    "statistics": {
        "required": ["id", "metadata_id", "start_ts", "mean", "min", "max", "state"],
        "optional": ["sum", "last_reset_ts"],
    },
    "statistics_meta": {
        "required": ["id", "statistic_id"],
        "optional": ["source", "unit_of_measurement", "has_mean", "has_sum", "name"],
    },
    "statistics_short_term": {
        "required": ["id", "metadata_id", "start_ts", "mean", "min", "max", "state"],
        "optional": ["sum", "state_id", "last_reset_ts"],
    },
}

# Module-level cache: the HA version string we last validated against.
_validated_ha_version: str | None = None
_validation_errors: list[str] = []


def _get_table_columns(inspector, table_name: str) -> set[str] | None:
    """Return the set of column names for ``table_name``, or ``None`` if the
    table does not exist."""
    try:
        cols = inspector.get_columns(table_name)
        return {c["name"] for c in cols}
    except Exception:
        return None


def validate_schema_sync(hass: HomeAssistant) -> list[str]:
    """Inspect the recorder DB and verify every required column exists.

    Returns a list of human-readable error strings.  An empty list means
    the schema is compatible.  Called from the executor at ``async_setup``.
    """
    global _validated_ha_version, _validation_errors

    try:
        from homeassistant.const import __version__ as ha_version
    except ImportError:
        ha_version = "unknown"

    recorder = get_instance(hass)
    if recorder is None:
        return ["Recorder component not available"]

    errors: list[str] = []

    try:
        from sqlalchemy import inspect as sa_inspect

        with recorder.get_session() as session:
            bind = session.get_bind()
            inspector = sa_inspect(bind)

            # Core tables (must exist)
            for table_name, spec in REQUIRED_COLUMNS.items():
                actual = _get_table_columns(inspector, table_name)
                if actual is None:
                    errors.append(
                        f"Table '{table_name}' not found in the recorder database"
                    )
                    continue
                for col in spec["required"]:
                    if col not in actual:
                        errors.append(
                            f"Required column '{table_name}.{col}' is missing "
                            f"(found: {sorted(actual)})"
                        )

            # Statistics tables (optional — absence is fine, HAS_STATISTICS handles it)
            for table_name, spec in STATISTICS_COLUMNS.items():
                actual = _get_table_columns(inspector, table_name)
                if actual is None:
                    _LOGGER.debug(
                        "Statistics table '%s' not present; statistics features "
                        "will be disabled",
                        table_name,
                    )
                    continue
                for col in spec["required"]:
                    if col not in actual:
                        errors.append(
                            f"Required column '{table_name}.{col}' is missing "
                            f"(found: {sorted(actual)})"
                        )

    except Exception as err:
        errors.append(f"Schema inspection failed: {err}")

    _validated_ha_version = ha_version
    _validation_errors = errors

    if errors:
        _LOGGER.error(
            "History Editor schema validation failed (HA %s):\n  %s",
            ha_version,
            "\n  ".join(errors),
        )
    else:
        _LOGGER.info(
            "History Editor schema validation passed (HA %s)", ha_version,
        )

    return errors


def ensure_schema_current(hass: HomeAssistant) -> str | None:
    """Quick check that the HA version hasn't changed since the last
    successful schema probe.

    Called at the top of every sync helper.  Returns an error string to
    surface to the caller, or ``None`` if everything is OK.

    If the version *did* change (e.g. HAOS upgraded the container without
    fully restarting the component), re-runs the full schema probe
    synchronously before allowing the operation to proceed.
    """
    global _validated_ha_version, _validation_errors

    try:
        from homeassistant.const import __version__ as ha_version
    except ImportError:
        ha_version = "unknown"

    if _validated_ha_version == ha_version:
        # Same version as when we last checked — return cached result
        if _validation_errors:
            return (
                "History Editor is disabled due to schema incompatibility "
                f"(HA {ha_version}): {_validation_errors[0]}"
            )
        return None

    # Version changed — re-probe
    _LOGGER.warning(
        "HA version changed (%s → %s); re-validating recorder schema",
        _validated_ha_version,
        ha_version,
    )
    errors = validate_schema_sync(hass)
    if errors:
        return (
            "History Editor detected an incompatible recorder schema after "
            f"HA version change ({_validated_ha_version} → {ha_version}): "
            f"{errors[0]}"
        )
    return None
