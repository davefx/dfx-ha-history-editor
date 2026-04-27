"""Panel for History Editor."""
import json
import os

from homeassistant.components import panel_custom
from homeassistant.components.http import StaticPathConfig
from homeassistant.core import HomeAssistant

_SIDEBAR_TITLES = {
    "en": "History Editor",
    "es": "Editor de historial",
}
_DEFAULT_TITLE = "History Editor"


def _get_sidebar_title(hass: HomeAssistant) -> str:
    """Return the sidebar title in the HA instance's configured language."""
    lang = getattr(hass.config, "language", "en")
    if lang in _SIDEBAR_TITLES:
        return _SIDEBAR_TITLES[lang]
    # Try base language (e.g. "es" from "es-AR")
    base = lang.split("-")[0]
    return _SIDEBAR_TITLES.get(base, _DEFAULT_TITLE)


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
