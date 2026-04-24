"""History Editor component for Home Assistant."""
import json
import logging
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import voluptuous as vol
from aiohttp import web

from homeassistant.components.http import HomeAssistantView
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.db_schema import States, StatesMeta
from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse, SupportsResponse
from homeassistant.exceptions import HomeAssistantError
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
from .statistics import (
    HAS_STATISTICS,
    delete_short_term_stats_by_state_id,
    delete_statistic_sync,
    get_statistics_sync,
    recalculate_statistics_sync,
    update_statistic_sync,
    update_statistics_after_state_change,
)

_LOGGER = logging.getLogger(__name__)

DOMAIN = "history_editor"

# Service names
SERVICE_GET_RECORDS = "get_records"
SERVICE_UPDATE_RECORD = "update_record"
SERVICE_DELETE_RECORD = "delete_record"
SERVICE_CREATE_RECORD = "create_record"
SERVICE_RECALCULATE_STATISTICS = "recalculate_statistics"


def _set_state_timestamps(
    state_record,
    last_changed: datetime | None,
    last_updated: datetime | None,
) -> None:
    """Write both the modern ``*_ts`` columns and the legacy datetime columns.

    HA's recorder schema has switched from ``DateTime`` columns to float epoch
    ``*_ts`` columns over time.  We populate whichever exist so the component
    works across versions.  Passing ``None`` leaves the field alone.
    """
    if last_changed is not None:
        if hasattr(state_record, 'last_changed_ts'):
            state_record.last_changed_ts = last_changed.timestamp()
        if hasattr(state_record, 'last_changed'):
            state_record.last_changed = last_changed
    if last_updated is not None:
        if hasattr(state_record, 'last_updated_ts'):
            state_record.last_updated_ts = last_updated.timestamp()
        if hasattr(state_record, 'last_updated'):
            state_record.last_updated = last_updated


def _read_state_timestamps(state_record) -> tuple[str | None, str | None]:
    """Return ISO strings for ``last_changed`` and ``last_updated``.

    Prefers the modern ``*_ts`` columns and falls back to the legacy datetime
    columns.  Returns ``(None, None)`` if neither is set.
    """
    last_changed_iso = None
    if hasattr(state_record, 'last_changed_ts') and state_record.last_changed_ts is not None:
        last_changed_iso = dt_util.utc_from_timestamp(state_record.last_changed_ts).isoformat()
    elif hasattr(state_record, 'last_changed') and state_record.last_changed is not None:
        last_changed_iso = state_record.last_changed.isoformat()

    last_updated_iso = None
    if hasattr(state_record, 'last_updated_ts') and state_record.last_updated_ts is not None:
        last_updated_iso = dt_util.utc_from_timestamp(state_record.last_updated_ts).isoformat()
    elif hasattr(state_record, 'last_updated') and state_record.last_updated is not None:
        last_updated_iso = state_record.last_updated.isoformat()

    return last_changed_iso, last_updated_iso

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
            if state_id is None:
                return self.json(
                    {"success": False, "error": "state_id is required"},
                    status_code=400
                )
            try:
                state_id = int(state_id)
            except (ValueError, TypeError):
                return self.json(
                    {"success": False, "error": "state_id must be an integer"},
                    status_code=400
                )

            new_state = data.get("state")
            if new_state is not None:
                new_state = str(new_state)
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
                state_id,
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
            if state_id is None:
                return self.json(
                    {"success": False, "error": "state_id is required"},
                    status_code=400
                )
            try:
                state_id = int(state_id)
            except (ValueError, TypeError):
                return self.json(
                    {"success": False, "error": "state_id must be an integer"},
                    status_code=400
                )

            # Delete the record synchronously in executor
            result = await self.hass.async_add_executor_job(
                _delete_record_sync, self.hass, state_id
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

            if state is None:
                return self.json(
                    {"success": False, "error": "state is required"},
                    status_code=400
                )
            state = str(state)

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
                get_statistics_sync, self.hass, entity_id, start_time, end_time, limit, statistic_type
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
            if stat_id is None:
                return self.json(
                    {"success": False, "error": "id is required"},
                    status_code=400
                )
            try:
                stat_id = int(stat_id)
            except (ValueError, TypeError):
                return self.json(
                    {"success": False, "error": "id must be an integer"},
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
                update_statistic_sync,
                self.hass,
                stat_id,
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
            if stat_id is None:
                return self.json(
                    {"success": False, "error": "id is required"},
                    status_code=400
                )
            try:
                stat_id = int(stat_id)
            except (ValueError, TypeError):
                return self.json(
                    {"success": False, "error": "id must be an integer"},
                    status_code=400
                )

            statistic_type = data.get("statistic_type", "long_term")
            if statistic_type not in ("long_term", "short_term"):
                return self.json(
                    {"success": False, "error": "statistic_type must be 'long_term' or 'short_term'"},
                    status_code=400
                )

            result = await self.hass.async_add_executor_job(
                delete_statistic_sync, self.hass, stat_id, statistic_type
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
        _LOGGER.debug("Querying records for entity_id=%s, start_time=%s, end_time=%s, limit=%d",
                     entity_id, start_time, end_time, limit)

        with recorder.get_session() as session:
            # Join with StatesMeta to filter by entity_id (required for HA 2022.4+)
            query = (
                session.query(States)
                .join(StatesMeta, States.metadata_id == StatesMeta.metadata_id)
                .filter(StatesMeta.entity_id == entity_id)
            )
            
            # Use timestamp fields for filtering (newer schema) with fallback to legacy fields.
            # Note: this is an inclusive user filter ([start_time, end_time]).  It
            # does NOT match the half-open [start, start+period) convention used
            # by statistics bucket queries elsewhere in this file — a record at
            # exactly end_time is included here but would be excluded by a stats
            # bucket covering the same boundary.
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
            
            states = query.all()
            has_more = len(states) > limit
            if has_more:
                states = states[:limit]
            _LOGGER.debug("Query returned %d states (has_more=%s)", len(states), has_more)

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
                last_changed_iso, last_updated_iso = _read_state_timestamps(state)

                records.append({
                    "state_id": state.state_id,
                    "entity_id": entity_id,  # Use the parameter instead of state.entity_id (not in new schema)
                    "state": state.state,
                    "attributes": attributes,
                    "last_changed": last_changed_iso,
                    "last_updated": last_updated_iso,
                })

            _LOGGER.debug("Retrieved %d records for entity %s", len(records), entity_id)
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
        if not result.get("success"):
            raise HomeAssistantError(result.get("error") or "Failed to update record")
        _fire_statistics_events(hass)

    async def delete_record(call: ServiceCall) -> None:
        """Delete a history record."""
        state_id = call.data["state_id"]

        result = await hass.async_add_executor_job(_delete_record_sync, hass, state_id)
        if not result.get("success"):
            raise HomeAssistantError(result.get("error") or "Failed to delete record")
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
        if not result.get("success"):
            raise HomeAssistantError(result.get("error") or "Failed to create record")
        _fire_statistics_events(hass)

    async def recalculate_statistics(call: ServiceCall) -> ServiceResponse:
        """Force recalculation of statistics for an entity over a time range."""
        entity_id = call.data["entity_id"]
        start_time = call.data["start_time"]
        end_time = call.data["end_time"]
        statistic_type = call.data.get("statistic_type", "both")

        result = await hass.async_add_executor_job(
            recalculate_statistics_sync,
            hass,
            entity_id,
            start_time,
            end_time,
            statistic_type,
        )
        if not result.get("success"):
            raise HomeAssistantError(result.get("error") or "Failed to recalculate statistics")
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
            _set_state_timestamps(state, new_last_changed, new_last_updated)

            session.commit()
            _LOGGER.info("Updated state record %s", state_id)

            # Update affected statistics if available. A failure here is
            # non-fatal — the state update is already committed, so rather
            # than rolling it back we flag statistics_stale so the caller
            # can re-run recalculate_statistics.
            statistics_stale = False
            if HAS_STATISTICS and (new_state is not None or new_last_updated is not None):
                try:
                    update_statistics_after_state_change(
                        session, state, old_ts, new_last_updated
                    )
                    session.commit()
                except Exception as stats_err:
                    statistics_stale = True
                    _LOGGER.warning(
                        "Error updating statistics after state change for state %s: %s "
                        "(state record is updated; statistics may be stale — "
                        "call history_editor.recalculate_statistics to fix)",
                        state_id, stats_err
                    )

            return {
                "success": True,
                "state_id": state_id,
                "statistics_stale": statistics_stale,
            }
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
            try:
                stats_deleted = delete_short_term_stats_by_state_id(session, state_id)
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
                statistics_stale = False
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
                        update_statistics_after_state_change(
                            session, state_proxy, state_ts, None
                        )
                        # Commit statistics updates separately; the primary deletion was already
                        # committed above. A failure here is non-fatal — the state record is
                        # gone and statistics will remain stale rather than rolling back the delete.
                        session.commit()
                    except Exception as stats_err:
                        statistics_stale = True
                        _LOGGER.warning(
                            "Error recalculating statistics after state deletion for state %s: %s "
                            "(state record is deleted; statistics may be stale — "
                            "call history_editor.recalculate_statistics to fix)",
                            state_id, stats_err,
                        )

                return {
                    "success": True,
                    "state_id": state_id,
                    "statistics_deleted": stats_deleted,
                    "statistics_stale": statistics_stale,
                }
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
            _set_state_timestamps(new_state, last_changed, last_updated)

            session.add(new_state)
            session.commit()
            session.refresh(new_state)
            new_state_id = new_state.state_id
            _LOGGER.info("Created new state record for entity %s with ID %s", entity_id, new_state_id)

            # Recalculate short-term and long-term statistics for the period containing
            # the new state so that graphs and the energy dashboard pick up the change.
            # Mirrors the update/delete paths; non-fatal on error.
            statistics_stale = False
            if HAS_STATISTICS:
                try:
                    update_statistics_after_state_change(
                        session, new_state, None, last_updated
                    )
                    session.commit()
                except Exception as stats_err:
                    statistics_stale = True
                    _LOGGER.warning(
                        "Error updating statistics after state creation for state %s: %s "
                        "(state record is created; statistics may be stale — "
                        "call history_editor.recalculate_statistics to fix)",
                        new_state_id, stats_err,
                    )

            return {
                "success": True,
                "state_id": new_state_id,
                "statistics_stale": statistics_stale,
            }
    except Exception as err:
        _LOGGER.error("Error creating record: %s", err, exc_info=True)
        return {"success": False, "error": str(err)}

