"""Service-layer tests for the statistics purge (issue #78).

Two services, mirroring the recorder's own shape: an unfiltered
``purge_statistics`` and a targeted ``purge_entity_statistics``.  The handlers
are closures inside ``async_setup``, so these run setup with a fake recorder
and drive the handlers it registered.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
import voluptuous as vol
from homeassistant.exceptions import HomeAssistantError

import custom_components.history_editor as he


class _FakeRecorder:
    def __init__(self, schema_probe):
        self._schema_probe = schema_probe
        self.calls = []

    async def async_add_executor_job(self, func, *args):
        if func is self._schema_probe:
            return []
        self.calls.append((func.__name__, args))
        return {"success": True, "dry_run": False, "purge_before": "x",
                "statistics": [], "total_deleted": 0}


class _FakeHass:
    def __init__(self):
        self.registered = {}
        self.schemas = {}
        self.http = SimpleNamespace(
            register_view=lambda view: None,
            async_register_static_paths=self._noop,
        )
        self.services = SimpleNamespace(async_register=self._register)
        self.bus = SimpleNamespace(async_fire=lambda *a, **kw: None)
        self.config = SimpleNamespace(language="en")
        self.auth = SimpleNamespace(async_get_user=self._admin)

    def _register(self, domain, service, handler, **kwargs):
        self.registered[service] = handler
        self.schemas[service] = kwargs.get("schema")

    async def _admin(self, user_id):
        return SimpleNamespace(is_admin=True, id=user_id)

    async def _noop(self, *args, **kwargs):
        pass

    async def async_add_executor_job(self, func, *args):
        return func(*args)


@pytest.fixture
def hass_with_services(monkeypatch):
    from custom_components.history_editor import panel as panel_mod

    hass = _FakeHass()
    recorder = _FakeRecorder(he.validate_schema_sync)
    monkeypatch.setattr(he, "get_instance", lambda _hass: recorder)

    async def fake_register_panel(_hass, **kwargs):
        pass

    monkeypatch.setattr(panel_mod.panel_custom, "async_register_panel", fake_register_panel)
    assert asyncio.run(he.async_setup(hass, {})) is not False
    return hass, recorder


def _call(hass, service, data):
    handler = hass.registered[service]
    call = SimpleNamespace(data=data, context=SimpleNamespace(user_id="admin"))
    return asyncio.run(handler(call))


def test_both_services_are_registered(hass_with_services):
    hass, _ = hass_with_services
    assert "purge_statistics" in hass.registered
    assert "purge_entity_statistics" in hass.registered


def test_purge_statistics_passes_keep_days_through(hass_with_services):
    hass, recorder = hass_with_services
    _call(hass, "purge_statistics", {"keep_days": 365, "dry_run": False})
    name, args = recorder.calls[-1]
    assert name == "purge_statistics_sync"
    assert 365 in args


def test_purge_statistics_takes_no_selectors(hass_with_services):
    """The unfiltered service must not quietly accept a target and ignore it."""
    hass, _ = hass_with_services
    schema = hass.schemas["purge_statistics"]
    with pytest.raises(vol.Invalid):
        schema({"keep_days": 365, "entity_id": ["sensor.a"]})


def test_purge_entity_statistics_requires_a_selector(hass_with_services):
    """Purging every statistic is what the other service is for; this one must
    not do it by omission."""
    hass, _ = hass_with_services
    schema = hass.schemas["purge_entity_statistics"]
    with pytest.raises(vol.Invalid):
        schema({"keep_days": 365})
    # any one selector is enough
    assert schema({"keep_days": 365, "entity_id": ["sensor.a"]})
    assert schema({"keep_days": 365, "entity_globs": ["sensor.a_*"]})
    assert schema({"keep_days": 365, "domains": ["sensor"]})


def test_purge_entity_statistics_forwards_its_selectors(hass_with_services):
    hass, recorder = hass_with_services
    _call(hass, "purge_entity_statistics", {
        "keep_days": 180,
        "entity_id": ["sensor.a"],
        "entity_globs": ["sensor.b_*"],
        "domains": ["number"],
    })
    name, args = recorder.calls[-1]
    assert name == "purge_statistics_sync"
    assert ["sensor.a"] in args
    assert ["sensor.b_*"] in args
    assert ["number"] in args


def test_dry_run_defaults_to_false(hass_with_services):
    hass, _ = hass_with_services
    schema = hass.schemas["purge_statistics"]
    assert schema({"keep_days": 365})["dry_run"] is False


def test_negative_keep_days_is_rejected_by_the_schema(hass_with_services):
    hass, _ = hass_with_services
    with pytest.raises(vol.Invalid):
        hass.schemas["purge_statistics"]({"keep_days": -1})


def test_failure_is_raised_to_the_caller(hass_with_services, monkeypatch):
    """Automations must see a failure, not a silent no-op."""
    hass, recorder = hass_with_services

    async def failing(func, *args):
        return {"success": False, "error": "boom"}

    monkeypatch.setattr(recorder, "async_add_executor_job", failing)
    with pytest.raises(HomeAssistantError):
        _call(hass, "purge_statistics", {"keep_days": 365, "dry_run": False})


# --------------------------------------------------------------------------
# The three places a service has to be declared must not drift apart.
# --------------------------------------------------------------------------

def test_services_yaml_and_translations_match_the_registered_services(
    hass_with_services,
):
    import json
    from pathlib import Path

    import yaml

    hass, _ = hass_with_services
    registered = set(hass.registered)

    root = Path(__file__).resolve().parent.parent / "custom_components" / "history_editor"
    documented = set(yaml.safe_load((root / "services.yaml").read_text(encoding="utf-8")))
    assert documented == registered

    for name in ("strings.json", "translations/en.json", "translations/es.json"):
        data = json.loads((root / name).read_text(encoding="utf-8"))
        assert set(data["services"]) == registered, name
