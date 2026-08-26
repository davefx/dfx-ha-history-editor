# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.1] - 2026-08-26

### Fixed

- The statistics purge now deletes in committed chunks instead of one long
  transaction. A purge spanning years of hourly rows held the recorder's write
  lock for its whole duration, and Home Assistant logged
  `database is locked` on its own writes while it ran. Chunk size is 1000 rows,
  clamped by the recorder's `max_bind_vars`. Reported against a production
  database on [#78](https://github.com/davefx/dfx-ha-history-editor/issues/78).

## [1.4.0] - 2026-08-24

### Added

- Two services to purge long-term statistics by age, which Home Assistant
  cannot do on its own: `history_editor.purge_statistics` (every statistic) and
  `history_editor.purge_entity_statistics` (narrowed by the union of
  `entity_id` / `entity_globs` / `domains`). HA's time-based `recorder.purge`
  covers states, events, statistics runs and short-term statistics but leaves
  the hourly `Statistics` table alone, and `recorder.purge_entities` never
  touches statistics at all, so the only built-in alternative drops a statistic
  in full. Both services take `keep_days`, report the rows deleted per
  `statistic_id`, and support `dry_run` for checking an automation before
  trusting it. ([#78](https://github.com/davefx/dfx-ha-history-editor/issues/78))

  Deliberately out of scope, and covered by tests:
  - only the hourly `Statistics` table is touched — short-term rows belong to
    the recorder's own retention;
  - `sum` is an absolute running total and is never rebased, so every surviving
    pair of rows keeps the difference the energy dashboard charts, and exports
    to systems like InfluxDB stay consistent;
  - `StatisticsMeta` survives a full purge, since the recorder keeps writing new
    rows and dropping the metadata would orphan the entity.

## [1.3.5] - 2026-08-23

### Fixed

- Deleting or editing a **range** of state records no longer corrupts the `sum`
  column of `total` / `total_increasing` sensors. The running-sum cascade was
  applied once per 5-minute period, so a range of N recalculated periods shifted
  every later row by the sum of N deltas. Deleting a few hours of an energy
  sensor drove its running totals far negative and left the energy dashboard
  showing nonsense. The cascade now runs **once per range**: each period's row is
  adjusted by its own delta, and only rows after the range are shifted, by the
  delta of the range's last period. Affects the bulk state paths, the single
  create/update/delete paths when a record moves between periods, and
  `history_editor.recalculate_statistics`. ([#76](https://github.com/davefx/dfx-ha-history-editor/issues/76))
- Long-term (hourly) statistics rows are no longer blocked from editing merely
  because short-term rows exist for that hour. Short-term rows are derived data
  and survive for the recorder's full retention, so the old guard blocked every
  hourly row a user could realistically want to repair — including the rows the
  energy dashboard reads, leaving no way to fix an entity after deleting its bad
  state history. A long-term row is now blocked only while actual state history
  exists in that hour, mirroring the short-term guard.
  ([#76](https://github.com/davefx/dfx-ha-history-editor/issues/76))

### Changed

- The singular and bulk statistics paths now share one guard implementation
  (`_check_source_data_blocks_edit`) instead of duplicating it inline.

## [1.3.4] - 2026-08-20

### Security

- Gate the `history_editor.*` services on admin privileges. Every recorder
  operation is exposed both as a REST endpoint and as a service; only the
  endpoints were gated (1.3.1), so a non-admin who could not reach
  `/api/history_editor/bulk_delete` could still call `history_editor.bulk_delete`
  over the websocket `call_service` API and destroy the same history. All nine
  services now resolve `call.context.user_id` through `hass.auth.async_get_user()`
  and raise `Unauthorized` for non-admins, following the semantics of Home
  Assistant's own `async_register_admin_service`: calls with no user in their
  context (automations, scripts, internal calls) are trusted.

### Fixed

- `async_register_panel` no longer reads the translation file with a blocking
  `open()` / `json.load()` on the event loop; the read goes through
  `hass.async_add_executor_job`.

## [1.3.3] - 2026-08-20

### Added

- Dark theme brand icons (`dark_icon.png`, `dark_icon@2x.png`, plus
  `dark_icon.svg` as the source). The navy `#000080` mark is close to invisible
  on Home Assistant's dark background; the dark variant uses `#8080ff`, the same
  hue and saturation lightened from 25% to 75% HSL lightness.

### Changed

- Regenerate `icon.png` / `icon@2x.png` from the SVG at 2048px, trimming the
  transparent margins and re-squaring so the mark fills the canvas, as the
  Home Assistant brands image guidelines ask for. Optimised losslessly.

Since Home Assistant 2026.3, these files are served by the integration itself
through `/api/brands/integration/history_editor/*` and take priority over the
brands CDN.

## [1.3.2] - 2026-07-03

### Changed

- The admin gate on the `/api/history_editor/*` endpoints now raises
  `Unauthorized` (HTTP `401`), matching Home Assistant's own `require_admin`
  behaviour, instead of returning a custom `403` JSON body. Behaviour is
  otherwise unchanged: non-admin and unauthenticated callers are still rejected
  before any recorder access.

## [1.3.1] - 2026-06-24

### Security

- Gate all `/api/history_editor/*` REST endpoints on admin privileges. The
  endpoints previously required authentication but were reachable by any
  authenticated user, while the panel UI itself is admin-only. Non-admin
  requests now receive `403 Forbidden`.

### Added

- HACS install badge in the README.
- Daily `github-repo-stats` workflow.

## [1.3.0] - 2026-04-28

### Added

- Overview chart with a 3-month window and a "load earlier" control.

### Changed

- Read the sidebar panel title from `www/translations/*.json` so it matches the
  Home Assistant language.

### Fixed

- Reset the chart immediately when changing the selected entity.

## [1.2.0] - 2026-04-27

### Added

- Spanish translation and complete service strings.
- Full UI string translation (English + Spanish).

### Changed

- Load translations from external JSON files instead of inlining them.

## [1.1.2] - 2026-04-26

### Fixed

- Route all database operations through the recorder database executor.

## [1.1.1] - 2026-04-26

### Fixed

- Restore the YAML config schema so the panel registers correctly.

## [1.1.0] - 2026-04-26

### Added

- Dragon-warning screen shown on first visit.
- Recorder schema validation at startup and when the Home Assistant version
  changes.

### Fixed

- Statistics recalculation consistency; statistics logic extracted into a
  dedicated module with test coverage.

### Changed

- Regenerated brand/icon PNGs from SVG at the proper sizes.

## [1.0.1] - 2026-04-25

### Fixed

- Various post-release fixes (brand icon folder name, missing icons).

### Added

- HACS and hassfest validation workflows.

## [1.0.0] - 2026-04-25

### Added

- Initial release: CRUD operations on the Home Assistant recorder database
  (state history and short/long-term statistics) via both Home Assistant
  services and REST endpoints.
- Custom admin-only sidebar panel with entity picker, infinite-scroll
  pagination, and edit/create modals.
- Bulk update/delete for state history and statistics records.
- Statistics consistency handling: short-term/long-term recalculation and the
  running-sum cascade for `total` / `total_increasing` sensors.

[1.4.1]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.3.5...v1.4.0
[1.3.5]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.3.4...v1.3.5
[1.3.4]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.3.3...v1.3.4
[1.3.3]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.3.2...v1.3.3
[1.3.2]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.1.2...v1.2.0
[1.1.2]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/davefx/dfx-ha-history-editor/compare/1.0.1...v1.1.0
[1.0.1]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.0.0...1.0.1
[1.0.0]: https://github.com/davefx/dfx-ha-history-editor/releases/tag/v1.0.0
