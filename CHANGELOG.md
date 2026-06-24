# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.3.1]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.1.2...v1.2.0
[1.1.2]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/davefx/dfx-ha-history-editor/compare/1.0.1...v1.1.0
[1.0.1]: https://github.com/davefx/dfx-ha-history-editor/compare/v1.0.0...1.0.1
[1.0.0]: https://github.com/davefx/dfx-ha-history-editor/releases/tag/v1.0.0
