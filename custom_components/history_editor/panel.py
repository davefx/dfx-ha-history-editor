"""Panel for History Editor."""
import os

from homeassistant.components import panel_custom
from homeassistant.components.http import StaticPathConfig
from homeassistant.core import HomeAssistant


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
        sidebar_title="History Editor",
        sidebar_icon="mdi:database-edit",
        module_url="/history_editor_panel/history-editor-panel.js",
        embed_iframe=False,
        require_admin=True,
    )
