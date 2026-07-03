"""Tests for the admin gate on the REST endpoints.

Every ``/api/history_editor/*`` view must reject non-admin (and unauthenticated)
requests before doing any work, and let admin requests through to the regular
request handling.  Rejection is done by raising ``homeassistant.exceptions.
Unauthorized`` (which HA's HTTP middleware turns into ``401 Unauthorized``),
mirroring Home Assistant's own ``require_admin`` behaviour.

These tests exercise only the gate, so they build fake requests and never touch
a real recorder.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from homeassistant.exceptions import Unauthorized

from custom_components.history_editor import (  # noqa: E402
    BulkDeleteRecordView,
    BulkDeleteStatisticView,
    BulkUpdateRecordView,
    BulkUpdateStatisticView,
    CreateRecordView,
    DeleteRecordView,
    DeleteStatisticView,
    GetRecordsView,
    GetStatisticsView,
    UpdateRecordView,
    UpdateStatisticView,
    _require_admin,
)

# Views whose handler is a GET (read endpoints); the rest use POST.
GET_VIEWS = {GetRecordsView, GetStatisticsView}

ALL_VIEWS = [
    GetRecordsView,
    UpdateRecordView,
    DeleteRecordView,
    CreateRecordView,
    GetStatisticsView,
    UpdateStatisticView,
    DeleteStatisticView,
    BulkUpdateRecordView,
    BulkDeleteRecordView,
    BulkUpdateStatisticView,
    BulkDeleteStatisticView,
]

# Endpoints that mutate recorder history / statistics. These are the ones the
# HACS review (hacs/default#7264) flagged: they required authentication but were
# reachable by any authenticated user while the panel UI is admin-only.
MUTATING_VIEWS = [
    UpdateRecordView,
    DeleteRecordView,
    CreateRecordView,
    UpdateStatisticView,
    DeleteStatisticView,
    BulkUpdateRecordView,
    BulkDeleteRecordView,
    BulkUpdateStatisticView,
    BulkDeleteStatisticView,
]


class FakeRequest(dict):
    """Minimal stand-in for an aiohttp request.

    ``dict.get('hass_user')`` returns the stored user (or ``None`` when absent),
    which is exactly what ``_require_admin`` inspects.
    """

    def __init__(self, user=None, json_data=None, query=None):
        super().__init__()
        if user is not None:
            self["hass_user"] = user
        self._json = {} if json_data is None else json_data
        self.query = {} if query is None else query

    async def json(self):
        return self._json


def _call(view, request):
    """Instantiate ``view`` and invoke its request handler synchronously."""
    instance = view(hass=None)
    handler = instance.get if view in GET_VIEWS else instance.post
    return asyncio.run(handler(request))


def test_require_admin_helper():
    # Admin passes silently.
    _require_admin(FakeRequest(SimpleNamespace(is_admin=True)))
    # Non-admin and unauthenticated are rejected.
    with pytest.raises(Unauthorized):
        _require_admin(FakeRequest(SimpleNamespace(is_admin=False)))
    with pytest.raises(Unauthorized):
        _require_admin(FakeRequest(user=None))


@pytest.mark.parametrize("view", ALL_VIEWS, ids=[v.__name__ for v in ALL_VIEWS])
@pytest.mark.parametrize(
    "user",
    [None, SimpleNamespace(is_admin=False)],
    ids=["unauthenticated", "non_admin"],
)
def test_non_admin_is_rejected(view, user):
    with pytest.raises(Unauthorized):
        _call(view, FakeRequest(user=user))


@pytest.mark.parametrize("view", ALL_VIEWS, ids=[v.__name__ for v in ALL_VIEWS])
def test_admin_passes_the_gate(view):
    """Admin requests must get past the gate (no ``Unauthorized`` raised).

    The handlers then proceed into validation / DB work with ``hass=None`` and a
    deliberately empty payload, so they return some non-401 response — the point
    is only that the gate let the admin through.
    """
    resp = _call(view, FakeRequest(user=SimpleNamespace(is_admin=True)))
    assert resp.status != 401


@pytest.mark.parametrize(
    "view", MUTATING_VIEWS, ids=[v.__name__ for v in MUTATING_VIEWS]
)
def test_regression_non_admin_cannot_mutate_recorder(view):
    """Regression for hacs/default#7264.

    A logged-in but non-admin user must not be able to reach any recorder
    mutation through the REST API. Each mutating endpoint must reject the caller
    *before* touching the database, so a realistic payload (which would otherwise
    be acted upon) is supplied to make the test fail loudly if the admin gate is
    ever removed.
    """
    payload = {
        "state_id": 1,
        "state_ids": [1, 2],
        "id": 1,
        "ids": [1, 2],
        "entity_id": "sensor.test",
        "state": "42",
    }
    non_admin = SimpleNamespace(is_admin=False)
    with pytest.raises(Unauthorized):
        _call(view, FakeRequest(user=non_admin, json_data=payload))
