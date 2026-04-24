# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repository is

A Home Assistant **custom component** (integration) that provides CRUD operations on the recorder database — state history and short/long-term statistics — plus a custom sidebar panel UI. It is distributed via HACS; there is no build step, no package manager, and no compiled artifact. Files are dropped into `<HA config>/custom_components/history_editor/` and loaded by Home Assistant at startup.

Target Home Assistant version: `2023.1.0+` (see `hacs.json`). Python 3.11+.

## Commands

There is no build system, linter config, or real test suite checked in.

- **Structural/self-check validation** (verifies files, manifest keys, services, JS symbols exist — not unit tests):
  ```bash
  python test_component.py
  ```
  Must be run from the repo root; paths inside are relative.

- **Unit tests** for the sync DB helpers. These exercise `statistics.py` and the state CRUD helpers in `__init__.py` against an in-memory SQLite DB populated with HA's real recorder schema. They require `homeassistant` and `pytest` installed:
  ```bash
  pip install -r requirements-test.txt   # installs homeassistant + pytest
  pytest tests/
  # or a subset:
  pytest tests/test_statistics.py -k totaliser
  ```
  The `mock_hass` fixture (in `tests/conftest.py`) monkey-patches `get_instance` in both `statistics.py` and `__init__.py` so the sync helpers hit the test session instead of a real recorder. Do not use `db_session.refresh(row)` in a test after a helper mutated `row` — the helper's ORM assignments may not have been flushed; read the attribute directly or `session.expire_all()` then re-query.

- **Run against a real Home Assistant** (the only way to exercise behavior):
  ```bash
  mkdir -p config
  cp configuration.yaml.example config/configuration.yaml
  ln -s "$(pwd)/custom_components/history_editor" config/custom_components/history_editor
  python3 -m venv venv && source venv/bin/activate
  pip install homeassistant
  hass -c config
  ```
  Then open `http://localhost:8123/history-editor`. Append `?debug` to enable the in-panel debug status and `[HistoryEditor]` console logs (see `DEBUG_GUIDE.md`).

- **Reload after frontend changes**: the panel JS is served as a static asset (`/history_editor_panel/history-editor-panel.js`). HA and the browser aggressively cache it — hard-reload (Ctrl-Shift-R) or restart HA.

- **Reload after backend changes**: requires `hass` restart. There is no hot-reload; `config_flow` is false and the component is set up via `async_setup` from `configuration.yaml`.

## Architecture

### Two-surface integration

Every data operation is exposed **twice**, and both entry points fan into the same sync helper:

1. **HA service** (callable from automations / Dev Tools) — registered in `async_setup` with a voluptuous schema.
2. **REST view** at `/api/history_editor/*` (consumed by the panel JS) — a `HomeAssistantView` subclass.

Both wrap the same `_*_sync` function via `hass.async_add_executor_job(...)`. When adding or modifying an operation, keep all three in sync: schema + service handler + REST view + sync helper.

| Operation | Service | REST | Sync helper |
|---|---|---|---|
| Read states | `get_records` | `GET /api/history_editor/records` | `_get_records_sync` |
| Update state | `update_record` | `POST /api/history_editor/update` | `_update_record_sync` |
| Delete state | `delete_record` | `POST /api/history_editor/delete` | `_delete_record_sync` |
| Create state | `create_record` | `POST /api/history_editor/create` | `_create_record_sync` |
| Read stats | — | `GET /api/history_editor/statistics` | `_get_statistics_sync` |
| Update stat | — | `POST /api/history_editor/statistics/update` | `_update_statistic_sync` |
| Delete stat | — | `POST /api/history_editor/statistics/delete` | `_delete_statistic_sync` |
| Recalc stats | `recalculate_statistics` | — | `_recalculate_statistics_sync` |

`get_records` and `recalculate_statistics` use `SupportsResponse.ONLY` so their results are visible in Dev Tools; the mutation services return `None` and raise `HomeAssistantError` on failure (so automations see the error). State-mutation responses include a `statistics_stale: bool` flag that is set to `true` if the main DB op succeeded but the follow-up statistics recalc failed — callers can then re-run `history_editor.recalculate_statistics` to fix the drift.

### Module layout

- `__init__.py` — REST views, HA service handlers, `async_setup`, and state-table sync helpers (`_get_records_sync`, `_update_record_sync`, `_delete_record_sync`, `_create_record_sync`).
- `statistics.py` — everything that touches `Statistics`, `StatisticsMeta`, `StatisticsShortTerm`. Owns `HAS_STATISTICS`/`HAS_STATISTICS_SHORT_TERM` flags, the 5-min/hourly recalculation logic, the sum cascade, and the high-level `get_statistics_sync` / `update_statistic_sync` / `delete_statistic_sync` / `recalculate_statistics_sync` entry points. Also exposes `update_statistics_after_state_change` (called from state-mutation paths) and `delete_short_term_stats_by_state_id` (called from the state delete path to drop FK-linked rows).
- `panel.py` — sidebar panel registration (admin-only, `embed_iframe=False`).

### Async/sync boundary

Everything that touches the DB runs in a thread via `async_add_executor_job` because it uses synchronous SQLAlchemy through `recorder.get_instance(hass).get_session()`. **Never call SQLAlchemy directly from async code** — it will block the event loop. The naming convention is strict: async handlers are plain names; sync helpers end in `_sync`.

### Statistics consistency is the hard part

HA keeps three related tables — `States`, `StatisticsShortTerm` (5-min buckets), `Statistics` (hourly buckets) — and the frontend (energy dashboard, history graph, statistics card) trusts them to be coherent. When this component mutates `States`, it must mirror the change into both statistics tables or users see broken graphs. The relevant helpers live in `statistics.py`:

- `update_statistics_after_state_change` — entry point called after create/update/delete. Computes the affected 5-min and hour periods (both old and new timestamps when a record moves) and recalculates each from the raw `States` rows.
- `recalculate_short_term_stat` — rebuilds one 5-min row from the numeric states in that window. If the window is now empty (all records deleted), it carries forward the last prior numeric value rather than leaving a gap.
- `recalculate_long_term_stat` — re-aggregates one hourly row from the twelve 5-min rows inside it. Always run *after* short-term recalculation for the same period (see ordering note below).
- `_cascade_sum_adjustment` — for `state_class=total` / `total_increasing` sensors, the `sum` column is a running total. When the last value in a period changes by Δ, **every** subsequent short-term and long-term row must have Δ added to its `sum`. Uses `synchronize_session=False` bulk updates for speed; the caller must `session.flush()` and `session.expire_all()` (or `session.commit()` + `session.expire_all()`) before reading those rows again.
- `_fire_statistics_events` (in `__init__.py`) — fires `recorder_5min_statistics_generated` and `recorder_hourly_statistics_generated` on the event bus after any successful mutation. This invalidates HA frontend WebSocket caches so energy/graph panels pick up the new values without a page reload.

Ordering invariant in `update_statistics_after_state_change`: short-term periods are processed in chronological order (so sum-cascades compound correctly), then `session.flush()` + `session.expire_all()`, *then* long-term periods. Breaking this order causes long-term rows to be built from stale cached short-term rows.

The same invariant applies to `recalculate_statistics_sync` across the phase boundary: it commits + expires between the short-term and long-term loops, and chunks commits inside each loop (`RECALC_CHUNK_SHORT_TERM = 288`, `RECALC_CHUNK_LONG_TERM = 24`) to bound the recorder write-lock duration on bulk recalcs.

`recalculate_statistics` only **updates existing** statistics rows — it never inserts new ones. HA's recorder is still responsible for creating rows on its normal schedule.

### Frontend

`www/history-editor-panel.js` is a single-file vanilla Web Component (`class HistoryEditorPanel extends HTMLElement`). No framework, no bundler, no TypeScript. Registered via `panel_custom.async_register_panel` in `panel.py` with `require_admin=True`. The file is ~1700 lines and handles: entity picker integration with HA's internal `ha-entity-picker`, infinite-scroll pagination (top and bottom `IntersectionObserver`s), three data sources (states / long-term stats / short-term stats), and edit/create modals.

The panel depends on HA's own custom elements (`ha-entity-picker`, `ha-form`) which load asynchronously. The init flow (`connectedCallback` → `_ensureInitialized` → `_triggerEntityPickerLoad`) waits for `customElements.whenDefined('ha-entity-picker')` with a 10s timeout. A `MutationObserver` watches for HA clearing the panel's DOM (happens on WebSocket reconnects) and re-initializes. See `DEBUG_GUIDE.md` for the expected console log sequence.

### Schema compatibility shims

HA's recorder schema has evolved. The code defensively handles multiple versions:

- `Statistics`, `StatisticsMeta`, `StatisticsShortTerm` are imported in `statistics.py` inside a `try/except ImportError` block; absence sets `HAS_STATISTICS = False` and every public entry point short-circuits with `{"success": False, "error": "Statistics tables not available..."}`.
- `EVENT_RECORDER_{5MIN,HOURLY}_STATISTICS_GENERATED` are imported from `homeassistant.const` with string-literal fallbacks for older HA.
- When reading/writing `States` timestamps, always route through `_set_state_timestamps(state_record, last_changed, last_updated)` and `_read_state_timestamps(state_record)` in `__init__.py`. These encapsulate the dual-write (`last_*` datetime + `last_*_ts` float epoch) pattern with `hasattr` guards. Don't add new `hasattr(state, 'last_updated_ts')` branches inline.

## Conventions specific to this repo

- **Attributes are stored as JSON strings**, not dicts. When writing: `state.attributes = json.dumps(new_attributes)`. When reading: `json.loads(...)` with a try/except fallback.
- **Service schemas use `cv.datetime`, `cv.entity_id`, `cv.positive_int`** from `homeassistant.helpers.config_validation`. Keep this consistent with existing schemas.
- **Errors return `{"success": False, "error": "..."}`**, success returns `{"success": True, ...}`. REST views and sync helpers both follow this; don't raise across the executor boundary.
- **Timestamps are UTC**. Use `homeassistant.util.dt.utcnow()` and `dt_util.parse_datetime(...)`, never `datetime.now()`.
- **Admin-only**: the panel is registered with `require_admin=True`. REST views rely on HA's default auth (`requires_auth = True`) plus this admin gating at the panel layer. Don't weaken either.
- **Version bumps** go in `custom_components/history_editor/manifest.json` (HACS reads this).
