"""Tests for the sidebar panel registration.

``_get_sidebar_title`` reads a translation file with a blocking ``open()`` /
``json.load()``.  ``async_register_panel`` runs on the event loop, so the read
must go through ``hass.async_add_executor_job``.  Flagged in the HACS review
(hacs/default#7264).
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

from custom_components.history_editor import panel as panel_mod  # noqa: E402


class FakeHass:
    """Records what gets handed to the executor."""

    def __init__(self):
        self.config = SimpleNamespace(language="en")
        self.http = SimpleNamespace(async_register_static_paths=self._register_paths)
        self.executor_jobs = []
        self.static_paths = []

    async def _register_paths(self, configs):
        self.static_paths.extend(configs)

    async def async_add_executor_job(self, func, *args):
        self.executor_jobs.append(func)
        return func(*args)


def _register(monkeypatch):
    captured = {}

    async def fake_register_panel(hass, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(panel_mod.panel_custom, "async_register_panel", fake_register_panel)
    hass = FakeHass()
    asyncio.run(panel_mod.async_register_panel(hass))
    return hass, captured


def test_translation_file_is_read_in_the_executor(monkeypatch):
    hass, _ = _register(monkeypatch)
    assert panel_mod._get_sidebar_title in hass.executor_jobs


def test_panel_is_registered_admin_only(monkeypatch):
    """The panel must stay admin-only; the REST/service gates assume it."""
    _, captured = _register(monkeypatch)
    assert captured["require_admin"] is True
    assert captured["frontend_url_path"] == "history-editor"
    assert captured["sidebar_title"]
