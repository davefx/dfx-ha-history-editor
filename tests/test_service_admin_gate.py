"""Tests for the admin gate on the ``history_editor.*`` services.

Every recorder operation is exposed twice: as a REST endpoint (covered by
``test_admin_gate.py``) and as a Home Assistant service.  Services are callable
by any authenticated user over the websocket ``call_service`` API, so gating
only the REST layer left the same mutations reachable — a non-admin who could
not reach ``/api/history_editor/bulk_delete`` could still call
``history_editor.bulk_delete``.  Regression for hacs/default#7264.

The service handlers are closures defined inside ``async_setup``, which needs a
real recorder to run, so the per-handler check here is structural (AST): every
handler registered with ``hass.services.async_register`` must call the gate as
its first statement.  The gate helper itself is tested directly.
"""
from __future__ import annotations

import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from homeassistant.exceptions import Unauthorized, UnknownUser

from custom_components.history_editor import _require_admin_service  # noqa: E402

_INIT_PY = (
    Path(__file__).resolve().parent.parent
    / "custom_components"
    / "history_editor"
    / "__init__.py"
)
_GATE_NAME = "_require_admin_service"


class FakeHass:
    """Minimal stand-in exposing just ``hass.auth.async_get_user``."""

    def __init__(self, user=None):
        self._user = user
        self.auth = SimpleNamespace(async_get_user=self._async_get_user)
        self.lookups = 0

    async def _async_get_user(self, user_id):
        self.lookups += 1
        return self._user


def _call(context_user_id, user):
    """Run the gate for a service call made by ``user``."""
    hass = FakeHass(user)
    service_call = SimpleNamespace(context=SimpleNamespace(user_id=context_user_id))
    asyncio.run(_require_admin_service(hass, service_call))
    return hass


def _registered_service_handlers() -> list[str]:
    """Return the handler names passed to ``hass.services.async_register``."""
    tree = ast.parse(_INIT_PY.read_text(encoding="utf-8"))
    handlers: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "async_register":
            continue
        # hass.services.async_register(DOMAIN, SERVICE_X, handler, ...)
        if len(node.args) < 3 or not isinstance(node.args[2], ast.Name):
            continue
        handlers.append(node.args[2].id)
    return handlers


def _handler_body(name: str) -> list[ast.stmt]:
    """Return the statements of the service handler called ``name``."""
    tree = ast.parse(_INIT_PY.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return node.body
    raise AssertionError(f"service handler {name!r} not found in __init__.py")


def _is_gate_call(stmt: ast.stmt) -> bool:
    """True if ``stmt`` is exactly ``await _require_admin_service(hass, call)``."""
    if not isinstance(stmt, ast.Expr) or not isinstance(stmt.value, ast.Await):
        return False
    call = stmt.value.value
    return (
        isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == _GATE_NAME
    )


def test_all_services_are_registered_with_a_handler():
    """Sanity check for the AST scan itself, so a rename cannot silently pass."""
    handlers = _registered_service_handlers()
    assert len(handlers) == 9, handlers
    assert set(handlers) == {
        "get_records",
        "update_record",
        "delete_record",
        "create_record",
        "recalculate_statistics",
        "bulk_update_record",
        "bulk_delete_record",
        "bulk_update_statistic",
        "bulk_delete_statistic",
    }


@pytest.mark.parametrize("handler", _registered_service_handlers())
def test_service_handler_gates_on_admin_first(handler):
    """The gate must run before any argument handling or recorder access.

    Any new service added without the gate fails here.
    """
    body = _handler_body(handler)
    statements = [
        stmt
        for stmt in body
        # skip the docstring
        if not (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant))
    ]
    assert statements, f"{handler} has an empty body"
    assert _is_gate_call(statements[0]), (
        f"{handler} does not call {_GATE_NAME} as its first statement"
    )


def test_admin_passes():
    hass = _call("user-1", SimpleNamespace(is_admin=True))
    assert hass.lookups == 1


def test_non_admin_is_rejected():
    with pytest.raises(Unauthorized):
        _call("user-1", SimpleNamespace(is_admin=False))


def test_unknown_user_is_rejected():
    with pytest.raises(UnknownUser):
        _call("user-1", None)


def test_internal_call_without_user_context_is_allowed():
    """Automations, scripts and internal calls carry no ``user_id``.

    This mirrors Home Assistant's own ``async_register_admin_service``: such a
    call is not attributable to a user and is trusted.  No user lookup happens.
    """
    hass = _call(None, None)
    assert hass.lookups == 0
