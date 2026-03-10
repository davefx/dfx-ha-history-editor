"""History Editor component for Home Assistant."""
import json
import logging
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any

import voluptuous as vol
from aiohttp import web

from homeassistant.components.http import HomeAssistantView
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.db_schema import States, StatesMeta
from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse, SupportsResponse
from homeassistant.helpers import config_validation as cv
from homeassistant.util import dt as dt_util

# Import recorder statistics events for cache invalidation signalling.
# Fall back to the string literals for HA versions that predate the constants.
try:
    from homeassistant.const import (
        EVENT_RECORDER_5MIN_STATISTICS_GENERATED,
        EVENT_RECORDER_HOURLY_STATISTICS_GENERATED,
    )
except ImportError:
    EVENT_RECORDER_5MIN_STATISTICS_GENERATED = "recorder_5min_statistics_generated"
    EVENT_RECORDER_HOURLY_STATISTICS_GENERATED = "recorder_hourly_statistics_generated"

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

# Statistics period durations in seconds
SHORT_TERM_PERIOD_SECONDS = 300    # 5-minute short-term statistics
LONG_TERM_PERIOD_SECONDS = 3600    # 1-hour long-term statistics

# Service names
SERVICE_GET_RECORDS = "get_records"
SERVICE_UPDATE_RECORD = "update_record"
SERVICE_DELETE_RECORD = "delete_record"
SERVICE_CREATE_RECORD = "create_record"
SERVICE_RECALCULATE_STATISTICS = "recalculate_statistics"

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

SERVICE_RECALCULATE_STATISTICS_SCHEMA = vol.Schema({
    vol.Required("entity_id"): cv.entity_id,
    vol.Required("start_time"): cv.datetime,
    vol.Required("end_time"): cv.datetime,
    vol.Optional("statistic_type", default="both"): vol.In(["short_term", "long_term", "both"]),
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

            # Signal the frontend to refresh its statistics cache
            if result.get("success"):
                _fire_statistics_events(self.hass)
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

            # Signal the frontend to refresh its statistics cache
            if result.get("success"):
                _fire_statistics_events(self.hass)

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

            # Signal the frontend to refresh its statistics cache
            if result.get("success"):
                _fire_statistics_events(self.hass)

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

            # Signal the frontend to refresh its statistics cache
            if result.get("success"):
                _fire_statistics_events(self.hass)

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

            # Signal the frontend to refresh its statistics cache
            if result.get("success"):
                _fire_statistics_events(self.hass)

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
            # Fetch one extra record to determine whether more records exist
            fetch_limit = limit + 1
            if hasattr(States, 'last_updated_ts'):
                query = query.order_by(States.last_updated_ts.desc()).limit(fetch_limit)
            else:
                # Fallback to legacy datetime field
                query = query.order_by(States.last_updated.desc()).limit(fetch_limit)
            
            _LOGGER.info("Executing query: %s", str(query))
            states = query.all()
            has_more = len(states) > limit
            if has_more:
                states = states[:limit]
            _LOGGER.info("Query returned %d states (has_more=%s)", len(states), has_more)

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
            return {"success": True, "records": records, "has_more": has_more}
    except Exception as err:
        _LOGGER.error("Error retrieving records: %s", err, exc_info=True)
        return {"success": False, "error": str(err)}


def _fire_statistics_events(hass: HomeAssistant) -> None:
    """Fire recorder statistics-generated events after a direct DB modification.

    These events notify any active WebSocket subscribers (e.g. the energy
    dashboard) that statistics have changed.  Components that poll on a timer
    (e.g. the statistics-graph card) or use a history-stream subscription (e.g.
    the history panel) will only pick up the new data after the user navigates
    away and back, or reloads the browser tab.
    """
    hass.bus.async_fire(EVENT_RECORDER_5MIN_STATISTICS_GENERATED)
    hass.bus.async_fire(EVENT_RECORDER_HOURLY_STATISTICS_GENERATED)


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

        result = await hass.async_add_executor_job(
            _update_record_sync,
            hass,
            state_id,
            new_state,
            new_attributes,
            new_last_changed,
            new_last_updated,
        )
        if result.get("success"):
            _fire_statistics_events(hass)

    async def delete_record(call: ServiceCall) -> None:
        """Delete a history record."""
        state_id = call.data["state_id"]

        result = await hass.async_add_executor_job(_delete_record_sync, hass, state_id)
        if result.get("success"):
            _fire_statistics_events(hass)

    async def create_record(call: ServiceCall) -> None:
        """Create a new history record."""
        entity_id = call.data["entity_id"]
        state = call.data["state"]
        attributes = call.data.get("attributes", {})
        last_changed = call.data.get("last_changed", dt_util.utcnow())
        last_updated = call.data.get("last_updated", dt_util.utcnow())

        result = await hass.async_add_executor_job(
            _create_record_sync, hass, entity_id, state, attributes, last_changed, last_updated
        )
        if result.get("success"):
            _fire_statistics_events(hass)

    async def recalculate_statistics(call: ServiceCall) -> ServiceResponse:
        """Force recalculation of statistics for an entity over a time range."""
        entity_id = call.data["entity_id"]
        start_time = call.data["start_time"]
        end_time = call.data["end_time"]
        statistic_type = call.data.get("statistic_type", "both")

        result = await hass.async_add_executor_job(
            _recalculate_statistics_sync,
            hass,
            entity_id,
            start_time,
            end_time,
            statistic_type,
        )
        if result.get("success"):
            _fire_statistics_events(hass)
        return result

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
    hass.services.async_register(
        DOMAIN, SERVICE_RECALCULATE_STATISTICS, recalculate_statistics,
        schema=SERVICE_RECALCULATE_STATISTICS_SCHEMA,
        supports_response=SupportsResponse.ONLY,
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

            # Capture old timestamp before update (for statistics period calculation)
            old_ts = None
            if hasattr(state, 'last_updated_ts') and state.last_updated_ts is not None:
                old_ts = state.last_updated_ts

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

            # Update affected statistics if available
            if HAS_STATISTICS and (new_state is not None or new_last_updated is not None):
                try:
                    _update_statistics_after_state_change(
                        session, state, old_ts, new_last_updated
                    )
                    session.commit()
                except Exception as stats_err:
                    _LOGGER.warning(
                        "Error updating statistics after state change for state %s: %s",
                        state_id, stats_err
                    )

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

            # Capture state info needed for statistics recalculation before the record is deleted
            state_ts: float | None = None
            state_metadata_id: int | None = None
            if HAS_STATISTICS:
                if hasattr(state, 'last_updated_ts') and state.last_updated_ts is not None:
                    state_ts = state.last_updated_ts
                if hasattr(state, 'metadata_id') and state.metadata_id is not None:
                    state_metadata_id = state.metadata_id

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

                # Recalculate short-term and long-term statistics for the affected time periods.
                # The deleted state may have been the anchor for a short-term stat row (which was
                # removed above via the FK), and the corresponding long-term (hourly) stat must
                # now be recalculated from the remaining short-term data.
                if HAS_STATISTICS and state_ts is not None and state_metadata_id is not None:
                    try:
                        # Build a lightweight proxy so we can reuse the existing helper without
                        # needing the original (now-deleted) state object.
                        # last_updated_ts is set to None so that _update_statistics_after_state_change
                        # does not add a second "new" period — only the period containing the deleted
                        # state (captured in state_ts / old_ts) will be recalculated.
                        state_proxy = SimpleNamespace(
                            metadata_id=state_metadata_id,
                            last_updated_ts=None,
                        )
                        _update_statistics_after_state_change(
                            session, state_proxy, state_ts, None
                        )
                        # Commit statistics updates separately; the primary deletion was already
                        # committed above. A failure here is non-fatal — the state record is
                        # gone and statistics will remain stale rather than rolling back the delete.
                        session.commit()
                    except Exception as stats_err:
                        _LOGGER.warning(
                            "Error recalculating statistics after state deletion for state %s: %s",
                            state_id, stats_err,
                        )

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


def _cascade_sum_adjustment(
    session,
    stat_meta_id: int,
    start_ts: float,
    delta: float,
) -> None:
    """Adjust running-total sum columns from the given period onwards.

    For sensors with state_class=total or state_class=total_increasing, the
    ``sum`` column is a running total that must remain consistent across all
    subsequent periods.  When the last-state value of a period changes by
    *delta*, every short-term and long-term statistics row from that period
    onward must receive the same adjustment.

    This mirrors the ``_adjust_sum_statistics`` helper in HA's recorder, which
    is not available on the public API.  We use ``synchronize_session=False``
    for bulk performance; the caller is responsible for flushing/expiring the
    session before reading these rows again.
    """
    if delta == 0:
        return

    session.query(StatisticsShortTerm).filter(
        StatisticsShortTerm.metadata_id == stat_meta_id,
        StatisticsShortTerm.start_ts >= start_ts,
        StatisticsShortTerm.sum.isnot(None),
    ).update(
        {StatisticsShortTerm.sum: StatisticsShortTerm.sum + delta},
        synchronize_session=False,
    )

    # Long-term (hourly) statistics: adjust from the containing hour onwards.
    start_ts_hour = float(int(start_ts // 3600) * 3600)
    session.query(Statistics).filter(
        Statistics.metadata_id == stat_meta_id,
        Statistics.start_ts >= start_ts_hour,
        Statistics.sum.isnot(None),
    ).update(
        {Statistics.sum: Statistics.sum + delta},
        synchronize_session=False,
    )


def _recalculate_short_term_stat(session, stat_meta_id: int, entity_id: str, start_ts_5min: float) -> bool:
    """Recalculate and update a short-term statistics record for a 5-minute period.

    Queries all numeric states in the period, recomputes mean/min/max/state, and
    updates the existing StatisticsShortTerm row. Returns True if a row was updated.

    When no numeric states exist in the period (e.g. all records were deleted), the
    last numeric state value recorded *before* the period is carried forward so the
    statistics show a continuous "hold last value" line rather than a gap or stale data.
    If no prior value exists at all, the stale row is removed.

    For sensors that track a running total (state_class=total or
    state_class=total_increasing), the ``sum`` column is cascaded forward so
    that subsequent statistics periods remain consistent with the new value.
    """
    end_ts = start_ts_5min + 300.0

    states_in_period = (
        session.query(States)
        .join(StatesMeta, States.metadata_id == StatesMeta.metadata_id)
        .filter(StatesMeta.entity_id == entity_id)
        .filter(States.last_updated_ts >= start_ts_5min)
        .filter(States.last_updated_ts < end_ts)
        .order_by(States.last_updated_ts.asc())
        .all()
    )

    numeric_values: list[float] = []
    last_state_id: int | None = None
    last_state_float: float | None = None

    for s in states_in_period:
        try:
            v = float(s.state)
            numeric_values.append(v)
            last_state_id = s.state_id
            last_state_float = v
        except (ValueError, TypeError):
            pass

    short_term = session.query(StatisticsShortTerm).filter(
        StatisticsShortTerm.metadata_id == stat_meta_id,
        StatisticsShortTerm.start_ts == start_ts_5min,
    ).first()

    if short_term is None:
        return False

    # Capture old state value and whether this stat has a running sum before
    # making any modifications.  Used for cascading sum adjustments below.
    old_state: float | None = short_term.state
    has_sum: bool = short_term.sum is not None
    new_state: float | None = None

    if numeric_values:
        short_term.mean = sum(numeric_values) / len(numeric_values)
        short_term.min = min(numeric_values)
        short_term.max = max(numeric_values)
        short_term.state = last_state_float
        new_state = last_state_float
        if hasattr(short_term, 'state_id') and last_state_id is not None:
            short_term.state_id = last_state_id
    else:
        # No numeric states in this period (e.g. all records were deleted).
        # Carry forward the last known numeric value from before this period so the
        # statistics chart shows a continuous line rather than stale or missing data.
        prev_state = (
            session.query(States.state, States.last_updated_ts)
            .join(StatesMeta, States.metadata_id == StatesMeta.metadata_id)
            .filter(StatesMeta.entity_id == entity_id)
            .filter(States.last_updated_ts < start_ts_5min)
            .order_by(States.last_updated_ts.desc())
            .first()
        )
        prev_value: float | None = None
        if prev_state is not None:
            try:
                prev_value = float(prev_state.state)
            except (ValueError, TypeError):
                prev_value = None

        if prev_value is not None:
            short_term.mean = prev_value
            short_term.min = prev_value
            short_term.max = prev_value
            short_term.state = prev_value
            new_state = prev_value
        else:
            # No prior value available either — remove the now-meaningless row.
            session.delete(short_term)

    # For sensors with a running sum (total / total_increasing), cascade the
    # state-change delta to this period and every subsequent one so that the
    # energy dashboard and statistics panels remain accurate.
    # _cascade_sum_adjustment returns early when delta == 0, so no extra guard needed.
    if has_sum and old_state is not None and new_state is not None:
        _cascade_sum_adjustment(session, stat_meta_id, start_ts_5min, new_state - old_state)

    return True


def _recalculate_long_term_stat(session, stat_meta_id: int, start_ts_hour: float) -> bool:
    """Recalculate and update a long-term statistics record for an hourly period.

    Aggregates the updated short-term stats in the hour and updates the Statistics row.
    Returns True if a row was updated.

    When no short-term rows remain for the period (e.g. all underlying records were
    deleted), the last short-term row recorded *before* the period is used to carry
    its value forward so the long-term statistics show a continuous line.  If no prior
    short-term row exists, the stale long-term row is removed.
    """
    end_ts = start_ts_hour + 3600.0

    short_terms = (
        session.query(StatisticsShortTerm)
        .filter(
            StatisticsShortTerm.metadata_id == stat_meta_id,
            StatisticsShortTerm.start_ts >= start_ts_hour,
            StatisticsShortTerm.start_ts < end_ts,
        )
        .order_by(StatisticsShortTerm.start_ts.asc())
        .all()
    )

    long_term = session.query(Statistics).filter(
        Statistics.metadata_id == stat_meta_id,
        Statistics.start_ts == start_ts_hour,
    ).first()

    if not short_terms:
        # No short-term stats remain in this period (e.g. all underlying records were
        # deleted). Carry forward the last known short-term value from before this period
        # so the long-term chart shows a continuous line rather than stale or missing data.
        prev_short_term = (
            session.query(
                StatisticsShortTerm.mean,
                StatisticsShortTerm.min,
                StatisticsShortTerm.max,
                StatisticsShortTerm.state,
            )
            .filter(
                StatisticsShortTerm.metadata_id == stat_meta_id,
                StatisticsShortTerm.start_ts < start_ts_hour,
            )
            .order_by(StatisticsShortTerm.start_ts.desc())
            .first()
        )
        if long_term is not None:
            if prev_short_term is not None and prev_short_term.state is not None:
                prev_value = prev_short_term.state
                long_term.mean = prev_short_term.mean if prev_short_term.mean is not None else prev_value
                long_term.min = prev_short_term.min if prev_short_term.min is not None else prev_value
                long_term.max = prev_short_term.max if prev_short_term.max is not None else prev_value
                long_term.state = prev_value
            else:
                # No prior value available — remove the now-meaningless row.
                session.delete(long_term)
            return True
        return False

    if long_term is None:
        return False

    means = [s.mean for s in short_terms if s.mean is not None]
    mins = [s.min for s in short_terms if s.min is not None]
    maxs = [s.max for s in short_terms if s.max is not None]
    states = [s.state for s in short_terms if s.state is not None]

    if means:
        # Average the short-term means, consistent with how HA aggregates hourly statistics
        long_term.mean = sum(means) / len(means)
    if mins:
        long_term.min = min(mins)
    if maxs:
        long_term.max = max(maxs)
    if states:
        long_term.state = states[-1]  # last value in the hourly period

    # For total/total_increasing sensors the long-term sum equals the sum of
    # the last short-term row in the hour.  The short-term sums were already
    # cascaded forward by _recalculate_short_term_stat; we only need to mirror
    # the last one here so the hourly row stays in sync.
    sums = [s.sum for s in short_terms if s.sum is not None]
    if sums:
        long_term.sum = sums[-1]

    return True


def _update_statistics_after_state_change(
    session,
    state_record,
    old_ts: float | None,
    new_last_updated: datetime | None,
) -> None:
    """Update short-term and long-term statistics affected by a state record change.

    Determines which 5-minute and hourly periods are affected (the period of the
    old timestamp and/or the new timestamp), recalculates statistics for those
    periods from the underlying state data, and persists the results.
    """
    # Collect affected period timestamps
    affected_5min: set[float] = set()
    affected_hour: set[float] = set()

    def _add_periods(ts: float) -> None:
        p5 = float(int(ts // 300) * 300)
        ph = float(int(ts // 3600) * 3600)
        affected_5min.add(p5)
        affected_hour.add(ph)

    if old_ts is not None:
        _add_periods(old_ts)

    # New timestamp after update
    new_ts: float | None = None
    if new_last_updated is not None:
        new_ts = new_last_updated.timestamp()
    elif hasattr(state_record, 'last_updated_ts') and state_record.last_updated_ts is not None:
        new_ts = state_record.last_updated_ts

    if new_ts is not None:
        _add_periods(new_ts)

    if not affected_5min:
        return

    # Resolve entity_id and StatisticsMeta
    meta = session.query(StatesMeta).filter(
        StatesMeta.metadata_id == state_record.metadata_id
    ).first()
    if meta is None:
        return
    entity_id = meta.entity_id

    stat_meta = session.query(StatisticsMeta).filter(
        StatisticsMeta.statistic_id == entity_id
    ).first()
    if stat_meta is None:
        return

    # Capture the primary key before we flush/expire the session so it
    # remains accessible afterwards without an extra DB round-trip.
    stat_meta_id = stat_meta.id

    # Recalculate short-term stats for all affected 5-minute periods.
    # Process in chronological order so earlier sum-cascade adjustments are
    # applied before later periods are recalculated.
    updated_5min = 0
    for start_ts in sorted(affected_5min):
        if _recalculate_short_term_stat(session, stat_meta_id, entity_id, start_ts):
            updated_5min += 1

    # Flush pending ORM changes and expire all cached objects so that the
    # long-term recalculation reads the freshly-updated short-term rows
    # (including any sum-cascade bulk UPDATEs that used synchronize_session=False).
    if updated_5min:
        session.flush()
        session.expire_all()

    # Recalculate long-term stats for all affected hourly periods
    updated_hour = 0
    for start_ts in affected_hour:
        if _recalculate_long_term_stat(session, stat_meta_id, start_ts):
            updated_hour += 1

    if updated_5min or updated_hour:
        _LOGGER.info(
            "Updated statistics for entity %s: %d short-term and %d long-term period(s)",
            entity_id, updated_5min, updated_hour,
        )


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

            # Fetch one extra record to determine whether more records exist
            fetch_limit = limit + 1
            query = query.order_by(table.start_ts.desc()).limit(fetch_limit)
            stats = query.all()
            has_more = len(stats) > limit
            if has_more:
                stats = stats[:limit]

            records = []
            for stat in stats:
                start_iso = None
                if stat.start_ts is not None:
                    start_iso = dt_util.utc_from_timestamp(stat.start_ts).isoformat()
                last_reset_iso = None
                if hasattr(stat, 'last_reset_ts') and stat.last_reset_ts is not None:
                    last_reset_iso = dt_util.utc_from_timestamp(stat.last_reset_ts).isoformat()

                # Determine whether this record is locked by underlying source data
                has_source_data = False
                if stat.start_ts is not None:
                    if statistic_type == "short_term":
                        # Locked when the record falls within the recorder keep period,
                        # since state history exists for that entire period and HA can
                        # recalculate any short-term stat within it.
                        try:
                            keep_days = getattr(recorder, "keep_days", 10)
                            cutoff_ts = (dt_util.utcnow() - timedelta(days=keep_days)).timestamp()
                            has_source_data = stat.start_ts >= cutoff_ts
                        except Exception as check_err:
                            _LOGGER.debug("has_source_data check failed for short-term stat %s: %s", stat.id, check_err)
                    else:
                        # long_term: locked when short-term stats exist in the 1-hour period
                        try:
                            short_term_count = (
                                session.query(StatisticsShortTerm)
                                .filter(StatisticsShortTerm.metadata_id == stat.metadata_id)
                                .filter(StatisticsShortTerm.start_ts >= stat.start_ts)
                                .filter(StatisticsShortTerm.start_ts < stat.start_ts + 3600.0)
                                .count()
                            )
                            has_source_data = short_term_count > 0
                        except Exception as check_err:
                            _LOGGER.debug("has_source_data check failed for long-term stat %s: %s", stat.id, check_err)

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
                    "has_source_data": has_source_data,
                })

            _LOGGER.info("Retrieved %d statistics records for entity %s", len(records), entity_id)
            return {"success": True, "records": records, "has_more": has_more}
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

            # Guard: reject direct edits when underlying source data exists
            if stat.start_ts is not None:
                if statistic_type == "short_term":
                    stat_meta_row = session.query(StatisticsMeta).filter(
                        StatisticsMeta.id == stat.metadata_id
                    ).first()
                    if stat_meta_row is not None:
                        try:
                            state_count = (
                                session.query(States)
                                .join(StatesMeta, States.metadata_id == StatesMeta.metadata_id)
                                .filter(StatesMeta.entity_id == stat_meta_row.statistic_id)
                                .filter(States.last_updated_ts >= stat.start_ts)
                                .filter(States.last_updated_ts < stat.start_ts + 300.0)
                                .count()
                            )
                            if state_count > 0:
                                return {
                                    "success": False,
                                    "error": (
                                        "Cannot edit short-term statistics directly: state history "
                                        "records exist for this period. Edit the state history instead."
                                    ),
                                }
                        except Exception as check_err:
                            _LOGGER.warning("Source-data check failed for short-term stat %s: %s", stat_id, check_err)
                else:
                    try:
                        short_term_count = (
                            session.query(StatisticsShortTerm)
                            .filter(StatisticsShortTerm.metadata_id == stat.metadata_id)
                            .filter(StatisticsShortTerm.start_ts >= stat.start_ts)
                            .filter(StatisticsShortTerm.start_ts < stat.start_ts + 3600.0)
                            .count()
                        )
                        if short_term_count > 0:
                            return {
                                "success": False,
                                "error": (
                                    "Cannot edit long-term statistics directly: short-term statistics "
                                    "records exist for this period. Edit the short-term statistics instead."
                                ),
                            }
                    except Exception as check_err:
                        _LOGGER.warning("Source-data check failed for long-term stat %s: %s", stat_id, check_err)

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

            # Flush to make changes visible before cascading to long-term stats
            session.flush()

            # Cascade: after updating a short-term stat, recalculate the corresponding long-term stat
            if statistic_type == "short_term":
                try:
                    effective_ts = start.timestamp() if start is not None else stat.start_ts
                    if effective_ts is not None:
                        start_ts_hour = float(int(effective_ts // 3600) * 3600)
                        _recalculate_long_term_stat(session, stat.metadata_id, start_ts_hour)
                except Exception as cascade_err:
                    _LOGGER.warning(
                        "Error updating long-term stat after short-term edit for stat %s: %s",
                        stat_id, cascade_err
                    )

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
            stat = session.query(table).filter(table.id == stat_id).first()
            if stat is None:
                return {"success": False, "error": f"Statistic ID {stat_id} not found"}

            # Capture values needed for cascade before deletion
            stat_start_ts = stat.start_ts
            stat_metadata_id = stat.metadata_id

            # Guard: reject direct deletes when underlying source data exists
            if stat_start_ts is not None:
                if statistic_type == "short_term":
                    stat_meta_row = session.query(StatisticsMeta).filter(
                        StatisticsMeta.id == stat_metadata_id
                    ).first()
                    if stat_meta_row is not None:
                        try:
                            state_count = (
                                session.query(States)
                                .join(StatesMeta, States.metadata_id == StatesMeta.metadata_id)
                                .filter(StatesMeta.entity_id == stat_meta_row.statistic_id)
                                .filter(States.last_updated_ts >= stat_start_ts)
                                .filter(States.last_updated_ts < stat_start_ts + 300.0)
                                .count()
                            )
                            if state_count > 0:
                                return {
                                    "success": False,
                                    "error": (
                                        "Cannot delete short-term statistics directly: state history "
                                        "records exist for this period. Delete the state history instead."
                                    ),
                                }
                        except Exception as check_err:
                            _LOGGER.warning("Source-data check failed for short-term stat %s: %s", stat_id, check_err)
                else:
                    try:
                        short_term_count = (
                            session.query(StatisticsShortTerm)
                            .filter(StatisticsShortTerm.metadata_id == stat_metadata_id)
                            .filter(StatisticsShortTerm.start_ts >= stat_start_ts)
                            .filter(StatisticsShortTerm.start_ts < stat_start_ts + 3600.0)
                            .count()
                        )
                        if short_term_count > 0:
                            return {
                                "success": False,
                                "error": (
                                    "Cannot delete long-term statistics directly: short-term statistics "
                                    "records exist for this period. Delete the short-term statistics instead."
                                ),
                            }
                    except Exception as check_err:
                        _LOGGER.warning("Source-data check failed for long-term stat %s: %s", stat_id, check_err)

            deleted_count = session.query(table).filter(table.id == stat_id).delete(synchronize_session=False)

            # Cascade: after deleting a short-term stat, recalculate the corresponding long-term stat
            if deleted_count > 0 and statistic_type == "short_term" and stat_start_ts is not None:
                try:
                    start_ts_hour = float(int(stat_start_ts // 3600) * 3600)
                    _recalculate_long_term_stat(session, stat_metadata_id, start_ts_hour)
                except Exception as cascade_err:
                    _LOGGER.warning(
                        "Error updating long-term stat after short-term delete for stat %s: %s",
                        stat_id, cascade_err
                    )

            session.commit()

            if deleted_count > 0:
                _LOGGER.info("Deleted statistic record %s", stat_id)
                return {"success": True, "id": stat_id}
            else:
                return {"success": False, "error": f"Statistic ID {stat_id} not found"}
    except Exception as err:
        _LOGGER.error("Error deleting statistic: %s", err)
        return {"success": False, "error": str(err)}


def _recalculate_statistics_sync(
    hass: HomeAssistant,
    entity_id: str,
    start_time: datetime,
    end_time: datetime,
    statistic_type: str = "both",
) -> dict[str, Any]:
    """Force recalculation of statistics for an entity over a time range (synchronous).

    For ``statistic_type`` values:
    - ``"short_term"`` – recalculates every 5-minute StatisticsShortTerm row whose
      period overlaps [start_time, end_time) from the underlying state history.
    - ``"long_term"`` – recalculates every hourly Statistics row whose period
      overlaps [start_time, end_time) by re-aggregating the short-term rows.
    - ``"both"`` (default) – recalculates short-term first, then long-term.
    """
    if not HAS_STATISTICS:
        return {"success": False, "error": "Statistics tables not available in this HA version"}

    recorder = get_instance(hass)
    if recorder is None:
        return {"success": False, "error": "Recorder not available"}

    start_ts = start_time.timestamp()
    end_ts = end_time.timestamp()

    if end_ts <= start_ts:
        return {"success": False, "error": "end_time must be after start_time"}

    try:
        with recorder.get_session() as session:
            # Resolve the StatisticsMeta row for this entity
            stat_meta = session.query(StatisticsMeta).filter(
                StatisticsMeta.statistic_id == entity_id
            ).first()
            if stat_meta is None:
                return {
                    "success": False,
                    "error": f"No statistics metadata found for entity '{entity_id}'",
                }
            stat_meta_id = stat_meta.id

            updated_short_term = 0
            updated_long_term = 0

            # Recalculate short-term (5-minute) statistics from state history
            if statistic_type in ("short_term", "both"):
                ts = float(int(start_ts // SHORT_TERM_PERIOD_SECONDS) * SHORT_TERM_PERIOD_SECONDS)
                while ts < end_ts:
                    if _recalculate_short_term_stat(session, stat_meta_id, entity_id, ts):
                        updated_short_term += 1
                    ts += SHORT_TERM_PERIOD_SECONDS

            # Recalculate long-term (hourly) statistics from short-term statistics
            if statistic_type in ("long_term", "both"):
                ts = float(int(start_ts // LONG_TERM_PERIOD_SECONDS) * LONG_TERM_PERIOD_SECONDS)
                while ts < end_ts:
                    if _recalculate_long_term_stat(session, stat_meta_id, ts):
                        updated_long_term += 1
                    ts += LONG_TERM_PERIOD_SECONDS

            session.commit()

            _LOGGER.info(
                "Recalculated statistics for entity %s (%s to %s): "
                "%d short-term and %d long-term period(s) updated",
                entity_id,
                start_time.isoformat(),
                end_time.isoformat(),
                updated_short_term,
                updated_long_term,
            )
            return {
                "success": True,
                "entity_id": entity_id,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "statistic_type": statistic_type,
                "updated_short_term": updated_short_term,
                "updated_long_term": updated_long_term,
            }
    except Exception as err:
        _LOGGER.error("Error recalculating statistics: %s", err, exc_info=True)
        return {"success": False, "error": str(err)}
