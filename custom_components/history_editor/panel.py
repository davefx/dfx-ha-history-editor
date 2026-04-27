"""Panel for History Editor."""
import json
import os

from homeassistant.components import panel_custom
from homeassistant.components.http import StaticPathConfig
from homeassistant.core import HomeAssistant

_DEFAULT_TITLE = "History Editor"


def _get_sidebar_title(hass: HomeAssistant) -> str:
    """Return the sidebar title from www/translations/<lang>.json.

    Reads the same translation files the panel JS uses at runtime, so
    there is a single source of truth for all panel strings.  Falls back
    to English if the file or key is missing.
    """
    lang = getattr(hass.config, "language", "en")
    base = lang.split("-")[0]
    translations_dir = os.path.join(os.path.dirname(__file__), "www", "translations")

    for candidate in (lang, base, "en"):
        path = os.path.join(translations_dir, f"{candidate}.json")
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            title = data.get("title")
            if title:
                return title
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            continue

    return _DEFAULT_TITLE


async def async_register_panel(hass: HomeAssistant) -> None:
    """Register the History Editor panel."""
    # Register the static path for our JavaScript file
    await hass.http.async_register_static_paths(
        [
            StaticPathConfig(
                "/history_editor_panel",
                os.path.join(os.path.dirname(__file__), "www"),
                True,
            )
        ]
    )

    # Register the panel
    await panel_custom.async_register_panel(
        hass,
        webcomponent_name="history-editor-panel",
        frontend_url_path="history-editor",
        sidebar_title=_get_sidebar_title(hass),
        sidebar_icon="mdi:database-edit",
        module_url="/history_editor_panel/history-editor-panel.js",
        embed_iframe=False,
        require_admin=True,
    )
