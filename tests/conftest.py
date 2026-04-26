"""Pytest fixtures for the History Editor test suite.

These tests exercise the pure-SQLAlchemy helpers in ``statistics.py`` against
an in-memory SQLite database populated with Home Assistant's real recorder
schema.  No ``hass`` instance is spun up — tests that need the ``*_sync``
wrappers (which call ``get_instance(hass)``) use the ``mock_hass`` fixture
that patches ``get_instance`` to return a stub pointing at the test session.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Make the custom_components package importable without requiring a full HA install.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

# Skip the entire suite gracefully if Home Assistant is not available in the
# current environment (running `python test_component.py` does not need it).
pytest.importorskip("homeassistant.components.recorder.db_schema")
pytest.importorskip("sqlalchemy")

from homeassistant.components.recorder.db_schema import (  # noqa: E402
    Base,
    States,
    StatesMeta,
    Statistics,
    StatisticsMeta,
    StatisticsShortTerm,
)
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402


@pytest.fixture
def db_engine():
    """In-memory SQLite engine with the full HA recorder schema created."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(db_engine):
    """Fresh SQLAlchemy session bound to the in-memory DB.  Per-test lifetime."""
    Session = sessionmaker(bind=db_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def sample_entity(db_session):
    """Create a StatesMeta + StatisticsMeta row for ``sensor.test``.

    Returns a tuple ``(states_metadata_id, stat_metadata_id, entity_id)``.
    """
    entity_id = "sensor.test"
    states_meta = StatesMeta(entity_id=entity_id)
    db_session.add(states_meta)
    db_session.flush()

    stat_meta = StatisticsMeta(
        statistic_id=entity_id,
        source="recorder",
        unit_of_measurement="kWh",
        has_mean=True,
        has_sum=False,
        name=None,
    )
    db_session.add(stat_meta)
    db_session.flush()

    return states_meta.metadata_id, stat_meta.id, entity_id


@pytest.fixture
def sample_totaliser(db_session):
    """Create metadata for a ``total_increasing`` sensor (sum-tracking).

    Returns ``(states_metadata_id, stat_metadata_id, entity_id)``.
    """
    entity_id = "sensor.energy"
    states_meta = StatesMeta(entity_id=entity_id)
    db_session.add(states_meta)
    db_session.flush()

    stat_meta = StatisticsMeta(
        statistic_id=entity_id,
        source="recorder",
        unit_of_measurement="kWh",
        has_mean=False,
        has_sum=True,
        name=None,
    )
    db_session.add(stat_meta)
    db_session.flush()

    return states_meta.metadata_id, stat_meta.id, entity_id


@pytest.fixture
def mock_hass(db_session, monkeypatch):
    """A stub ``hass`` whose ``get_instance`` returns the test DB session.

    Use this for testing the ``*_sync`` entry points in ``statistics.py`` that
    call ``get_instance(hass).get_session()``.  The returned object has a
    ``get_session()`` context manager that yields the same ``db_session``
    fixture (commits are intercepted to avoid closing the session mid-test).
    """
    class _SessionCtx:
        def __enter__(self):
            return db_session

        def __exit__(self, *exc):
            return False

    recorder_stub = MagicMock()
    recorder_stub.get_session.side_effect = _SessionCtx
    recorder_stub.keep_days = 10

    from custom_components import history_editor as pkg_module
    from custom_components.history_editor import statistics as stats_module
    from custom_components.history_editor import schema_compat

    monkeypatch.setattr(stats_module, "get_instance", lambda hass: recorder_stub)
    monkeypatch.setattr(pkg_module, "get_instance", lambda hass: recorder_stub)

    # Prime the schema-validation cache so _check_schema() is a no-op in tests.
    # The test DB is known-good (we just created the schema from HA's Base).
    try:
        from homeassistant.const import __version__ as ha_version
    except ImportError:
        ha_version = "test"
    monkeypatch.setattr(schema_compat, "_validated_ha_version", ha_version)
    monkeypatch.setattr(schema_compat, "_validation_errors", [])

    return MagicMock()  # placeholder hass; get_instance is patched out
