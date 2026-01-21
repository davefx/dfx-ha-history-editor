"""History Editor component for Home Assistant."""
import logging
from datetime import datetime
from typing import Any

import voluptuous as vol

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.db_schema import States
from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.service import SupportsResponse
from homeassistant.util import dt as dt_util

from .panel import async_register_panel

_LOGGER = logging.getLogger(__name__)

DOMAIN = "history_editor"

# Service names
SERVICE_GET_RECORDS = "get_records"
SERVICE_UPDATE_RECORD = "update_record"
SERVICE_DELETE_RECORD = "delete_record"
SERVICE_CREATE_RECORD = "create_record"

# Service schemas
SERVICE_GET_RECORDS_SCHEMA = vol.Schema({
    vol.Required("entity_id"): cv.entity_id,
    vol.Optional("start_time"): cv.datetime,
    vol.Optional("end_time"): cv.datetime,
    vol.Optional("limit", default=100): cv.positive_int,
})

SERVICE_UPDATE_RECORD_SCHEMA = vol.Schema({
    vol.Required("state_id"): cv.positive_int,
    vol.Optional("state"): cv.string,
    vol.Optional("attributes"): dict,
    vol.Optional("last_changed"): cv.datetime,
    vol.Optional("last_updated"): cv.datetime,
})

SERVICE_DELETE_RECORD_SCHEMA = vol.Schema({
    vol.Required("state_id"): cv.positive_int,
})

SERVICE_CREATE_RECORD_SCHEMA = vol.Schema({
    vol.Required("entity_id"): cv.entity_id,
    vol.Required("state"): cv.string,
    vol.Optional("attributes"): dict,
    vol.Optional("last_changed"): cv.datetime,
    vol.Optional("last_updated"): cv.datetime,
})


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the History Editor component."""
    _LOGGER.info("Setting up History Editor component")

    def _get_records_sync(
        entity_id: str, start_time: datetime | None, end_time: datetime | None, limit: int
    ) -> dict[str, Any]:
        """Get history records for an entity (synchronous)."""
        recorder = get_instance(hass)
        if recorder is None:
            _LOGGER.error("Recorder component not available")
            return {"success": False, "error": "Recorder not available"}

        try:
            with recorder.get_session() as session:
                query = session.query(States).filter(States.entity_id == entity_id)
                
                if start_time:
                    query = query.filter(States.last_updated >= start_time)
                if end_time:
                    query = query.filter(States.last_updated <= end_time)
                
                query = query.order_by(States.last_updated.desc()).limit(limit)
                states = query.all()

                records = []
                for state in states:
                    records.append({
                        "state_id": state.state_id,
                        "entity_id": state.entity_id,
                        "state": state.state,
                        "attributes": state.attributes,
                        "last_changed": state.last_changed.isoformat() if state.last_changed else None,
                        "last_updated": state.last_updated.isoformat() if state.last_updated else None,
                    })

                _LOGGER.debug("Retrieved %d records for entity %s", len(records), entity_id)
                return {"success": True, "records": records}
        except Exception as err:
            _LOGGER.error("Error retrieving records: %s", err)
            return {"success": False, "error": str(err)}

    async def get_records(call: ServiceCall) -> ServiceResponse:
        """Get history records for an entity."""
        entity_id = call.data["entity_id"]
        start_time = call.data.get("start_time")
        end_time = call.data.get("end_time")
        limit = call.data.get("limit", 100)

        result = await hass.async_add_executor_job(
            _get_records_sync, entity_id, start_time, end_time, limit
        )
        return result

    def _update_record_sync(
        state_id: int,
        new_state: str | None,
        new_attributes: dict | None,
        new_last_changed: datetime | None,
        new_last_updated: datetime | None,
    ) -> dict[str, Any]:
        """Update a history record (synchronous)."""
        recorder = get_instance(hass)
        if recorder is None:
            _LOGGER.error("Recorder component not available")
            return {"success": False, "error": "Recorder not available"}

        try:
            with recorder.get_session() as session:
                state = session.query(States).filter(States.state_id == state_id).first()
                
                if state is None:
                    _LOGGER.error("State with ID %s not found", state_id)
                    return {"success": False, "error": f"State ID {state_id} not found"}

                if new_state is not None:
                    state.state = new_state
                if new_attributes is not None:
                    state.attributes = new_attributes
                if new_last_changed is not None:
                    state.last_changed = new_last_changed
                if new_last_updated is not None:
                    state.last_updated = new_last_updated

                session.commit()
                _LOGGER.info("Updated state record %s", state_id)
                return {"success": True, "state_id": state_id}
        except Exception as err:
            _LOGGER.error("Error updating record: %s", err)
            return {"success": False, "error": str(err)}

    async def update_record(call: ServiceCall) -> None:
        """Update a history record."""
        state_id = call.data["state_id"]
        new_state = call.data.get("state")
        new_attributes = call.data.get("attributes")
        new_last_changed = call.data.get("last_changed")
        new_last_updated = call.data.get("last_updated")

        await hass.async_add_executor_job(
            _update_record_sync,
            state_id,
            new_state,
            new_attributes,
            new_last_changed,
            new_last_updated,
        )

    def _delete_record_sync(state_id: int) -> dict[str, Any]:
        """Delete a history record (synchronous)."""
        recorder = get_instance(hass)
        if recorder is None:
            _LOGGER.error("Recorder component not available")
            return {"success": False, "error": "Recorder not available"}

        try:
            with recorder.get_session() as session:
                state = session.query(States).filter(States.state_id == state_id).first()
                
                if state is None:
                    _LOGGER.error("State with ID %s not found", state_id)
                    return {"success": False, "error": f"State ID {state_id} not found"}

                session.delete(state)
                session.commit()
                _LOGGER.info("Deleted state record %s", state_id)
                return {"success": True, "state_id": state_id}
        except Exception as err:
            _LOGGER.error("Error deleting record: %s", err)
            return {"success": False, "error": str(err)}

    async def delete_record(call: ServiceCall) -> None:
        """Delete a history record."""
        state_id = call.data["state_id"]

        await hass.async_add_executor_job(_delete_record_sync, state_id)

    def _create_record_sync(
        entity_id: str,
        state: str,
        attributes: dict,
        last_changed: datetime | None,
        last_updated: datetime | None,
    ) -> dict[str, Any]:
        """Create a new history record (synchronous)."""
        recorder = get_instance(hass)
        if recorder is None:
            _LOGGER.error("Recorder component not available")
            return {"success": False, "error": "Recorder not available"}

        try:
            with recorder.get_session() as session:
                new_state = States(
                    entity_id=entity_id,
                    state=state,
                    attributes=attributes,
                    last_changed=last_changed,
                    last_updated=last_updated,
                )
                session.add(new_state)
                session.commit()
                session.refresh(new_state)
                _LOGGER.info("Created new state record for entity %s with ID %s", entity_id, new_state.state_id)
                return {"success": True, "state_id": new_state.state_id}
        except Exception as err:
            _LOGGER.error("Error creating record: %s", err)
            return {"success": False, "error": str(err)}

    async def create_record(call: ServiceCall) -> None:
        """Create a new history record."""
        entity_id = call.data["entity_id"]
        state = call.data["state"]
        attributes = call.data.get("attributes", {})
        last_changed = call.data.get("last_changed", dt_util.utcnow())
        last_updated = call.data.get("last_updated", dt_util.utcnow())

        await hass.async_add_executor_job(
            _create_record_sync, entity_id, state, attributes, last_changed, last_updated
        )

    # Register services
    hass.services.async_register(
        DOMAIN, SERVICE_GET_RECORDS, get_records, schema=SERVICE_GET_RECORDS_SCHEMA
    )
    hass.services.async_register(
        DOMAIN, SERVICE_UPDATE_RECORD, update_record, schema=SERVICE_UPDATE_RECORD_SCHEMA
    )
    hass.services.async_register(
        DOMAIN, SERVICE_DELETE_RECORD, delete_record, schema=SERVICE_DELETE_RECORD_SCHEMA
    )
    hass.services.async_register(
        DOMAIN, SERVICE_CREATE_RECORD, create_record, schema=SERVICE_CREATE_RECORD_SCHEMA
    )

    # Register the frontend panel
    await async_register_panel(hass)

    _LOGGER.info("History Editor component loaded successfully")
    return True
