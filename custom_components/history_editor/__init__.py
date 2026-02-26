"""History Editor component for Home Assistant."""
import json
import logging
from datetime import datetime
from typing import Any

import voluptuous as vol
from aiohttp import web

from homeassistant.components.http import HomeAssistantView
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.db_schema import States, StatesMeta
from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse, SupportsResponse
from homeassistant.helpers import config_validation as cv
from homeassistant.util import dt as dt_util

from .panel import async_register_panel

_LOGGER = logging.getLogger(__name__)

# Try to import statistics tables if available (newer HA versions)
try:
    from homeassistant.components.recorder.db_schema import Statistics, StatisticsMeta, StatisticsShortTerm
    HAS_STATISTICS = True
    HAS_STATISTICS_SHORT_TERM = True
except ImportError:
    HAS_STATISTICS = False
    HAS_STATISTICS_SHORT_TERM = False
    _LOGGER.debug("Statistics tables not available in this HA version")

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


class GetRecordsView(HomeAssistantView):
    """View to handle getting history records via REST API."""

    url = "/api/history_editor/records"
    name = "api:history_editor:records"
    requires_auth = True

    def __init__(self, hass: HomeAssistant):
        """Initialize the view."""
        self.hass = hass

    async def get(self, request: web.Request) -> web.Response:
        """Get history records for an entity."""
        try:
            entity_id = request.query.get("entity_id")
            if not entity_id:
                return self.json(
                    {"success": False, "error": "entity_id is required"},
                    status_code=400
                )

            # Validate limit parameter
            try:
                limit = int(request.query.get("limit", 100))
                if limit <= 0:
                    return self.json(
                        {"success": False, "error": "limit must be a positive integer"},
                        status_code=400
                    )
            except (ValueError, TypeError):
                return self.json(
                    {"success": False, "error": "Invalid limit parameter"},
                    status_code=400
                )

            start_time_str = request.query.get("start_time")
            end_time_str = request.query.get("end_time")

            start_time = None
            end_time = None

            if start_time_str:
                try:
                    start_time = dt_util.parse_datetime(start_time_str)
                except (ValueError, TypeError):
                    return self.json(
                        {"success": False, "error": "Invalid start_time format"},
                        status_code=400
                    )

            if end_time_str:
                try:
                    end_time = dt_util.parse_datetime(end_time_str)
                except (ValueError, TypeError):
                    return self.json(
                        {"success": False, "error": "Invalid end_time format"},
                        status_code=400
                    )

            # Get the records synchronously in executor
            result = await self.hass.async_add_executor_job(
                _get_records_sync, self.hass, entity_id, start_time, end_time, limit
            )

            return self.json(result)

        except Exception as err:
            _LOGGER.error("Error in GetRecordsView: %s", err)
            return self.json(
                {"success": False, "error": str(err)},
                status_code=500
            )


class UpdateRecordView(HomeAssistantView):
    """View to handle updating history records via REST API."""

    url = "/api/history_editor/update"
    name = "api:history_editor:update"
    requires_auth = True

    def __init__(self, hass: HomeAssistant):
        """Initialize the view."""
        self.hass = hass

    async def post(self, request: web.Request) -> web.Response:
        """Update a history record."""
        try:
            data = await request.json()
            
            state_id = data.get("state_id")
            if not state_id:
                return self.json(
                    {"success": False, "error": "state_id is required"},
                    status_code=400
                )
            
            new_state = data.get("state")
            new_attributes = data.get("attributes")
            
            # Parse datetime strings if provided
            new_last_changed = None
            new_last_updated = None
            
            if "last_changed" in data and data["last_changed"]:
                try:
                    new_last_changed = dt_util.parse_datetime(data["last_changed"])
                except (ValueError, TypeError):
                    return self.json(
                        {"success": False, "error": "Invalid last_changed format"},
                        status_code=400
                    )
            
            if "last_updated" in data and data["last_updated"]:
                try:
                    new_last_updated = dt_util.parse_datetime(data["last_updated"])
                except (ValueError, TypeError):
                    return self.json(
                        {"success": False, "error": "Invalid last_updated format"},
                        status_code=400
                    )
            
            # Update the record synchronously in executor
            result = await self.hass.async_add_executor_job(
                _update_record_sync,
                self.hass,
                int(state_id),
                new_state,
                new_attributes,
                new_last_changed,
                new_last_updated,
            )
            
            return self.json(result)
            
        except Exception as err:
            _LOGGER.error("Error in UpdateRecordView: %s", err)
            return self.json(
                {"success": False, "error": str(err)},
                status_code=500
            )


class DeleteRecordView(HomeAssistantView):
    """View to handle deleting history records via REST API."""

    url = "/api/history_editor/delete"
    name = "api:history_editor:delete"
    requires_auth = True

    def __init__(self, hass: HomeAssistant):
        """Initialize the view."""
        self.hass = hass

    async def post(self, request: web.Request) -> web.Response:
        """Delete a history record."""
        try:
            data = await request.json()
            
            state_id = data.get("state_id")
            if not state_id:
                return self.json(
                    {"success": False, "error": "state_id is required"},
                    status_code=400
                )
            
            # Delete the record synchronously in executor
            result = await self.hass.async_add_executor_job(
                _delete_record_sync, self.hass, int(state_id)
            )
            
            return self.json(result)
            
        except Exception as err:
            _LOGGER.error("Error in DeleteRecordView: %s", err)
            return self.json(
                {"success": False, "error": str(err)},
                status_code=500
            )


class CreateRecordView(HomeAssistantView):
    """View to handle creating history records via REST API."""

    url = "/api/history_editor/create"
    name = "api:history_editor:create"
    requires_auth = True

    def __init__(self, hass: HomeAssistant):
        """Initialize the view."""
        self.hass = hass

    async def post(self, request: web.Request) -> web.Response:
        """Create a new history record."""
        try:
            data = await request.json()
            
            entity_id = data.get("entity_id")
            state = data.get("state")
            
            if not entity_id:
                return self.json(
                    {"success": False, "error": "entity_id is required"},
                    status_code=400
                )
            
            if not state:
                return self.json(
                    {"success": False, "error": "state is required"},
                    status_code=400
                )
            
            attributes = data.get("attributes", {})
            
            # Parse datetime strings if provided, otherwise use current time
            last_changed = dt_util.utcnow()
            last_updated = dt_util.utcnow()
            
            if "last_changed" in data and data["last_changed"]:
                try:
                    last_changed = dt_util.parse_datetime(data["last_changed"])
                except (ValueError, TypeError):
                    return self.json(
                        {"success": False, "error": "Invalid last_changed format"},
                        status_code=400
                    )
            
            if "last_updated" in data and data["last_updated"]:
                try:
                    last_updated = dt_util.parse_datetime(data["last_updated"])
                except (ValueError, TypeError):
                    return self.json(
                        {"success": False, "error": "Invalid last_updated format"},
                        status_code=400
                    )
            
            # Create the record synchronously in executor
            result = await self.hass.async_add_executor_job(
                _create_record_sync,
                self.hass,
                entity_id,
                state,
                attributes,
                last_changed,
                last_updated,
            )
            
            return self.json(result)
            
        except Exception as err:
            _LOGGER.error("Error in CreateRecordView: %s", err)
            return self.json(
                {"success": False, "error": str(err)},
                status_code=500
            )


class GetStatisticsView(HomeAssistantView):
    """View to handle getting statistics records via REST API."""

    url = "/api/history_editor/statistics"
    name = "api:history_editor:statistics"
    requires_auth = True

    def __init__(self, hass: HomeAssistant):
        """Initialize the view."""
        self.hass = hass

    async def get(self, request: web.Request) -> web.Response:
        """Get statistics records for an entity."""
        try:
            entity_id = request.query.get("entity_id")
            if not entity_id:
                return self.json(
                    {"success": False, "error": "entity_id is required"},
                    status_code=400
                )

            try:
                limit = int(request.query.get("limit", 100))
                if limit <= 0:
                    return self.json(
                        {"success": False, "error": "limit must be a positive integer"},
                        status_code=400
                    )
            except (ValueError, TypeError):
                return self.json(
                    {"success": False, "error": "Invalid limit parameter"},
                    status_code=400
                )

            statistic_type = request.query.get("statistic_type", "long_term")
            if statistic_type not in ("long_term", "short_term"):
                return self.json(
                    {"success": False, "error": "statistic_type must be 'long_term' or 'short_term'"},
                    status_code=400
                )

            start_time_str = request.query.get("start_time")
            end_time_str = request.query.get("end_time")
            start_time = None
            end_time = None

            if start_time_str:
                try:
                    start_time = dt_util.parse_datetime(start_time_str)
                except (ValueError, TypeError):
                    return self.json(
                        {"success": False, "error": "Invalid start_time format"},
                        status_code=400
                    )

            if end_time_str:
                try:
                    end_time = dt_util.parse_datetime(end_time_str)
                except (ValueError, TypeError):
                    return self.json(
                        {"success": False, "error": "Invalid end_time format"},
                        status_code=400
                    )

            result = await self.hass.async_add_executor_job(
                _get_statistics_sync, self.hass, entity_id, start_time, end_time, limit, statistic_type
            )
            return self.json(result)

        except Exception as err:
            _LOGGER.error("Error in GetStatisticsView: %s", err)
            return self.json(
                {"success": False, "error": str(err)},
                status_code=500
            )


class UpdateStatisticView(HomeAssistantView):
    """View to handle updating statistics records via REST API."""

    url = "/api/history_editor/statistics/update"
    name = "api:history_editor:statistics:update"
    requires_auth = True

    def __init__(self, hass: HomeAssistant):
        """Initialize the view."""
        self.hass = hass

    async def post(self, request: web.Request) -> web.Response:
        """Update a statistics record."""
        try:
            data = await request.json()

            stat_id = data.get("id")
            if not stat_id:
                return self.json(
                    {"success": False, "error": "id is required"},
                    status_code=400
                )

            statistic_type = data.get("statistic_type", "long_term")
            if statistic_type not in ("long_term", "short_term"):
                return self.json(
                    {"success": False, "error": "statistic_type must be 'long_term' or 'short_term'"},
                    status_code=400
                )

            mean = data.get("mean")
            min_val = data.get("min")
            max_val = data.get("max")
            sum_val = data.get("sum")
            state = data.get("state")

            start = None
            if "start" in data and data["start"]:
                try:
                    start = dt_util.parse_datetime(data["start"])
                except (ValueError, TypeError):
                    return self.json(
                        {"success": False, "error": "Invalid start format"},
                        status_code=400
                    )

            result = await self.hass.async_add_executor_job(
                _update_statistic_sync,
                self.hass,
                int(stat_id),
                mean,
                min_val,
                max_val,
                sum_val,
                state,
                start,
                statistic_type,
            )
            return self.json(result)

        except Exception as err:
            _LOGGER.error("Error in UpdateStatisticView: %s", err)
            return self.json(
                {"success": False, "error": str(err)},
                status_code=500
            )


class DeleteStatisticView(HomeAssistantView):
    """View to handle deleting statistics records via REST API."""

    url = "/api/history_editor/statistics/delete"
    name = "api:history_editor:statistics:delete"
    requires_auth = True

    def __init__(self, hass: HomeAssistant):
        """Initialize the view."""
        self.hass = hass

    async def post(self, request: web.Request) -> web.Response:
        """Delete a statistics record."""
        try:
            data = await request.json()

            stat_id = data.get("id")
            if not stat_id:
                return self.json(
                    {"success": False, "error": "id is required"},
                    status_code=400
                )

            statistic_type = data.get("statistic_type", "long_term")
            if statistic_type not in ("long_term", "short_term"):
                return self.json(
                    {"success": False, "error": "statistic_type must be 'long_term' or 'short_term'"},
                    status_code=400
                )

            result = await self.hass.async_add_executor_job(
                _delete_statistic_sync, self.hass, int(stat_id), statistic_type
            )
            return self.json(result)

        except Exception as err:
            _LOGGER.error("Error in DeleteStatisticView: %s", err)
            return self.json(
                {"success": False, "error": str(err)},
                status_code=500
            )


def _get_records_sync(
    hass: HomeAssistant,
    entity_id: str,
    start_time: datetime | None,
    end_time: datetime | None,
    limit: int
) -> dict[str, Any]:
    """Get history records for an entity (synchronous)."""
    recorder = get_instance(hass)
    if recorder is None:
        _LOGGER.error("Recorder component not available")
        return {"success": False, "error": "Recorder not available"}

    try:
        _LOGGER.info("Querying records for entity_id=%s, start_time=%s, end_time=%s, limit=%d", 
                     entity_id, start_time, end_time, limit)
        
        with recorder.get_session() as session:
            # Join with StatesMeta to filter by entity_id (required for HA 2022.4+)
            query = (
                session.query(States)
                .join(StatesMeta, States.metadata_id == StatesMeta.metadata_id)
                .filter(StatesMeta.entity_id == entity_id)
            )
            
            # Use timestamp fields for filtering (newer schema) with fallback to legacy fields
            if start_time:
                if hasattr(States, 'last_updated_ts'):
                    start_ts = start_time.timestamp()
                    query = query.filter(States.last_updated_ts >= start_ts)
                else:
                    # Fallback to legacy datetime field
                    query = query.filter(States.last_updated >= start_time)
            if end_time:
                if hasattr(States, 'last_updated_ts'):
                    end_ts = end_time.timestamp()
                    query = query.filter(States.last_updated_ts <= end_ts)
                else:
                    # Fallback to legacy datetime field
                    query = query.filter(States.last_updated <= end_time)
            
            # Order by timestamp field (newer schema) with fallback to legacy field
            if hasattr(States, 'last_updated_ts'):
                query = query.order_by(States.last_updated_ts.desc()).limit(limit)
            else:
                # Fallback to legacy datetime field
                query = query.order_by(States.last_updated.desc()).limit(limit)
            
            _LOGGER.info("Executing query: %s", str(query))
            states = query.all()
            _LOGGER.info("Query returned %d states", len(states))

            records = []
            for state in states:
                # Ensure attributes is a dict (it might be a string in some DB backends)
                try:
                    attributes = state.attributes
                    if isinstance(attributes, str):
                        attributes = json.loads(attributes)
                    elif attributes is None:
                        attributes = {}
                except (ValueError, TypeError, json.JSONDecodeError):
                    _LOGGER.warning("Failed to parse attributes for state_id=%s", state.state_id)
                    attributes = {}
                
                # Handle both old (last_changed/last_updated) and new (last_changed_ts/last_updated_ts) schemas
                last_changed_iso = None
                if hasattr(state, 'last_changed_ts') and state.last_changed_ts is not None:
                    last_changed_iso = dt_util.utc_from_timestamp(state.last_changed_ts).isoformat()
                elif hasattr(state, 'last_changed') and state.last_changed is not None:
                    last_changed_iso = state.last_changed.isoformat()
                
                last_updated_iso = None
                if hasattr(state, 'last_updated_ts') and state.last_updated_ts is not None:
                    last_updated_iso = dt_util.utc_from_timestamp(state.last_updated_ts).isoformat()
                elif hasattr(state, 'last_updated') and state.last_updated is not None:
                    last_updated_iso = state.last_updated.isoformat()
                
                records.append({
                    "state_id": state.state_id,
                    "entity_id": entity_id,  # Use the parameter instead of state.entity_id (not in new schema)
                    "state": state.state,
                    "attributes": attributes,
                    "last_changed": last_changed_iso,
                    "last_updated": last_updated_iso,
                })

            _LOGGER.info("Retrieved %d records for entity %s", len(records), entity_id)
            return {"success": True, "records": records}
    except Exception as err:
        _LOGGER.error("Error retrieving records: %s", err, exc_info=True)
        return {"success": False, "error": str(err)}


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the History Editor component."""
    _LOGGER.info("Setting up History Editor component")

    # Register REST API views
    hass.http.register_view(GetRecordsView(hass))
    hass.http.register_view(UpdateRecordView(hass))
    hass.http.register_view(DeleteRecordView(hass))
    hass.http.register_view(CreateRecordView(hass))
    hass.http.register_view(GetStatisticsView(hass))
    hass.http.register_view(UpdateStatisticView(hass))
    hass.http.register_view(DeleteStatisticView(hass))

    async def get_records(call: ServiceCall) -> ServiceResponse:
        """Get history records for an entity."""
        entity_id = call.data["entity_id"]
        start_time = call.data.get("start_time")
        end_time = call.data.get("end_time")
        limit = call.data.get("limit", 100)

        result = await hass.async_add_executor_job(
            _get_records_sync, hass, entity_id, start_time, end_time, limit
        )
        return result

    async def update_record(call: ServiceCall) -> None:
        """Update a history record."""
        state_id = call.data["state_id"]
        new_state = call.data.get("state")
        new_attributes = call.data.get("attributes")
        new_last_changed = call.data.get("last_changed")
        new_last_updated = call.data.get("last_updated")

        await hass.async_add_executor_job(
            _update_record_sync,
            hass,
            state_id,
            new_state,
            new_attributes,
            new_last_changed,
            new_last_updated,
        )

    async def delete_record(call: ServiceCall) -> None:
        """Delete a history record."""
        state_id = call.data["state_id"]

        await hass.async_add_executor_job(_delete_record_sync, hass, state_id)

    async def create_record(call: ServiceCall) -> None:
        """Create a new history record."""
        entity_id = call.data["entity_id"]
        state = call.data["state"]
        attributes = call.data.get("attributes", {})
        last_changed = call.data.get("last_changed", dt_util.utcnow())
        last_updated = call.data.get("last_updated", dt_util.utcnow())

        await hass.async_add_executor_job(
            _create_record_sync, hass, entity_id, state, attributes, last_changed, last_updated
        )

    # Register services
    hass.services.async_register(
        DOMAIN, SERVICE_GET_RECORDS, get_records, schema=SERVICE_GET_RECORDS_SCHEMA,
        supports_response=SupportsResponse.ONLY
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

def _update_record_sync(
    hass: HomeAssistant,
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
                # Serialize dict to JSON string for database storage
                state.attributes = json.dumps(new_attributes)
            if new_last_changed is not None:
                # Set both timestamp and legacy datetime fields for compatibility
                if hasattr(state, 'last_changed_ts'):
                    state.last_changed_ts = new_last_changed.timestamp()
                if hasattr(state, 'last_changed'):
                    state.last_changed = new_last_changed
            if new_last_updated is not None:
                # Set both timestamp and legacy datetime fields for compatibility
                if hasattr(state, 'last_updated_ts'):
                    state.last_updated_ts = new_last_updated.timestamp()
                if hasattr(state, 'last_updated'):
                    state.last_updated = new_last_updated

            session.commit()
            _LOGGER.info("Updated state record %s", state_id)
            return {"success": True, "state_id": state_id}
    except Exception as err:
        _LOGGER.error("Error updating record: %s", err)
        return {"success": False, "error": str(err)}


def _delete_record_sync(hass: HomeAssistant, state_id: int) -> dict[str, Any]:
    """Delete a history record (synchronous)."""
    recorder = get_instance(hass)
    if recorder is None:
        _LOGGER.error("Recorder component not available")
        return {"success": False, "error": "Recorder not available"}

    try:
        with recorder.get_session() as session:
            # First check if the record exists
            state = session.query(States).filter(States.state_id == state_id).first()
            if state is None:
                _LOGGER.error("State with ID %s not found", state_id)
                return {"success": False, "error": f"State ID {state_id} not found"}

            # Nullify old_state_id references in other states to avoid self-referential FK constraint errors
            try:
                refs_updated = (
                    session.query(States)
                    .filter(States.old_state_id == state_id)
                    .update({"old_state_id": None}, synchronize_session=False)
                )
                if refs_updated > 0:
                    _LOGGER.info("Cleared old_state_id reference in %d state(s) for state %s", refs_updated, state_id)
            except AttributeError:
                # old_state_id column not present in this HA version; no self-referential FK to handle
                pass

            # Delete associated statistics before deleting the state to avoid foreign key constraint errors
            stats_deleted = 0
            if HAS_STATISTICS_SHORT_TERM:
                try:
                    stats_deleted = session.query(StatisticsShortTerm).filter(
                        StatisticsShortTerm.state_id == state_id
                    ).delete(synchronize_session=False)
                    if stats_deleted > 0:
                        _LOGGER.info("Deleted %d associated statistics records for state %s", stats_deleted, state_id)
                except Exception as stats_err:
                    _LOGGER.warning("Error deleting statistics for state %s: %s", state_id, stats_err)
                    # Continue with state deletion even if statistics deletion fails

            # Delete using query.delete() which is more efficient than session.delete()
            deleted_count = session.query(States).filter(States.state_id == state_id).delete(synchronize_session=False)
            session.commit()
            
            if deleted_count > 0:
                _LOGGER.info("Deleted state record %s (and %d statistics)", state_id, stats_deleted)
                return {"success": True, "state_id": state_id, "statistics_deleted": stats_deleted}
            else:
                return {"success": False, "error": f"Failed to delete state {state_id}"}
                
    except Exception as err:
        _LOGGER.error("Error deleting record: %s", err)
        # Provide more helpful error message for foreign key constraints
        error_msg = str(err)
        if "FOREIGN KEY constraint failed" in error_msg:
            error_msg = (
                "Cannot delete this record because it is referenced by other records in the database. "
                "This may be a state that has associated statistics or other dependent data. "
                f"Original error: {error_msg}"
            )
        return {"success": False, "error": error_msg}


def _create_record_sync(
    hass: HomeAssistant,
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
            # Get or create StatesMeta for this entity_id (required for HA 2022.4+)
            metadata = session.query(StatesMeta).filter(StatesMeta.entity_id == entity_id).first()
            if metadata is None:
                # Create new metadata if it doesn't exist
                _LOGGER.info("Creating new StatesMeta for entity_id=%s", entity_id)
                try:
                    metadata = StatesMeta(entity_id=entity_id)
                    session.add(metadata)
                    session.flush()  # Ensure metadata_id is generated
                except Exception as metadata_err:
                    # Handle race condition - another process might have created it
                    _LOGGER.debug("Metadata creation failed, retrying query: %s", metadata_err)
                    session.rollback()
                    metadata = session.query(StatesMeta).filter(StatesMeta.entity_id == entity_id).first()
                    if metadata is None:
                        raise  # If still not found, re-raise the original error
            
            # Verify metadata_id is available
            if not hasattr(metadata, 'metadata_id') or metadata.metadata_id is None:
                raise ValueError(f"Failed to get metadata_id for entity_id={entity_id}")
            
            # Create the state with metadata_id and timestamps
            # Use both new timestamp fields and legacy datetime fields for compatibility
            # Serialize attributes dict to JSON string for database storage
            new_state = States(
                metadata_id=metadata.metadata_id,
                state=state,
                attributes=json.dumps(attributes),
            )
            
            # Set timestamp fields (newer schema)
            if hasattr(new_state, 'last_changed_ts'):
                new_state.last_changed_ts = last_changed.timestamp() if last_changed else None
            if hasattr(new_state, 'last_updated_ts'):
                new_state.last_updated_ts = last_updated.timestamp() if last_updated else None
            
            # Set legacy datetime fields (older schema)
            if hasattr(new_state, 'last_changed'):
                new_state.last_changed = last_changed
            if hasattr(new_state, 'last_updated'):
                new_state.last_updated = last_updated
            
            session.add(new_state)
            session.commit()
            session.refresh(new_state)
            _LOGGER.info("Created new state record for entity %s with ID %s", entity_id, new_state.state_id)
            return {"success": True, "state_id": new_state.state_id}
    except Exception as err:
        _LOGGER.error("Error creating record: %s", err, exc_info=True)
        return {"success": False, "error": str(err)}


def _get_statistics_sync(
    hass: HomeAssistant,
    entity_id: str,
    start_time: datetime | None,
    end_time: datetime | None,
    limit: int,
    statistic_type: str = "long_term",
) -> dict[str, Any]:
    """Get statistics records for an entity (synchronous)."""
    if not HAS_STATISTICS:
        return {"success": False, "error": "Statistics tables not available in this HA version"}

    recorder = get_instance(hass)
    if recorder is None:
        return {"success": False, "error": "Recorder not available"}

    try:
        table = StatisticsShortTerm if statistic_type == "short_term" else Statistics

        with recorder.get_session() as session:
            query = (
                session.query(table)
                .join(StatisticsMeta, table.metadata_id == StatisticsMeta.id)
                .filter(StatisticsMeta.statistic_id == entity_id)
            )

            if start_time:
                query = query.filter(table.start_ts >= start_time.timestamp())
            if end_time:
                query = query.filter(table.start_ts <= end_time.timestamp())

            query = query.order_by(table.start_ts.desc()).limit(limit)
            stats = query.all()

            records = []
            for stat in stats:
                start_iso = None
                if stat.start_ts is not None:
                    start_iso = dt_util.utc_from_timestamp(stat.start_ts).isoformat()
                last_reset_iso = None
                if hasattr(stat, 'last_reset_ts') and stat.last_reset_ts is not None:
                    last_reset_iso = dt_util.utc_from_timestamp(stat.last_reset_ts).isoformat()
                records.append({
                    "id": stat.id,
                    "statistic_id": entity_id,
                    "statistic_type": statistic_type,
                    "start": start_iso,
                    "mean": stat.mean,
                    "min": stat.min,
                    "max": stat.max,
                    "sum": stat.sum,
                    "state": stat.state,
                    "last_reset": last_reset_iso,
                })

            _LOGGER.info("Retrieved %d statistics records for entity %s", len(records), entity_id)
            return {"success": True, "records": records}
    except Exception as err:
        _LOGGER.error("Error retrieving statistics: %s", err, exc_info=True)
        return {"success": False, "error": str(err)}


def _update_statistic_sync(
    hass: HomeAssistant,
    stat_id: int,
    mean: float | None,
    min_val: float | None,
    max_val: float | None,
    sum_val: float | None,
    state: float | None,
    start: datetime | None,
    statistic_type: str = "long_term",
) -> dict[str, Any]:
    """Update a statistics record (synchronous)."""
    if not HAS_STATISTICS:
        return {"success": False, "error": "Statistics tables not available in this HA version"}

    recorder = get_instance(hass)
    if recorder is None:
        return {"success": False, "error": "Recorder not available"}

    try:
        table = StatisticsShortTerm if statistic_type == "short_term" else Statistics

        with recorder.get_session() as session:
            stat = session.query(table).filter(table.id == stat_id).first()

            if stat is None:
                return {"success": False, "error": f"Statistic ID {stat_id} not found"}

            if mean is not None:
                stat.mean = float(mean)
            if min_val is not None:
                stat.min = float(min_val)
            if max_val is not None:
                stat.max = float(max_val)
            if sum_val is not None:
                stat.sum = float(sum_val)
            if state is not None:
                stat.state = float(state)
            if start is not None:
                stat.start_ts = start.timestamp()

            session.commit()
            _LOGGER.info("Updated statistic record %s", stat_id)
            return {"success": True, "id": stat_id}
    except Exception as err:
        _LOGGER.error("Error updating statistic: %s", err)
        return {"success": False, "error": str(err)}


def _delete_statistic_sync(
    hass: HomeAssistant,
    stat_id: int,
    statistic_type: str = "long_term",
) -> dict[str, Any]:
    """Delete a statistics record (synchronous)."""
    if not HAS_STATISTICS:
        return {"success": False, "error": "Statistics tables not available in this HA version"}

    recorder = get_instance(hass)
    if recorder is None:
        return {"success": False, "error": "Recorder not available"}

    try:
        table = StatisticsShortTerm if statistic_type == "short_term" else Statistics

        with recorder.get_session() as session:
            deleted_count = session.query(table).filter(table.id == stat_id).delete(synchronize_session=False)
            session.commit()

            if deleted_count > 0:
                _LOGGER.info("Deleted statistic record %s", stat_id)
                return {"success": True, "id": stat_id}
            else:
                return {"success": False, "error": f"Statistic ID {stat_id} not found"}
    except Exception as err:
        _LOGGER.error("Error deleting statistic: %s", err)
        return {"success": False, "error": str(err)}
