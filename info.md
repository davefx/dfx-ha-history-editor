# History Editor for Home Assistant

Edit, create, and delete history records and statistics directly from your Home Assistant database with a user-friendly sidebar panel.

## Features

**State History (CRUD)**
- Create, read, update, and delete state history records
- Edit state values, attributes, and timestamps
- Infinite-scroll pagination for large datasets

**Statistics Editing**
- Edit and delete both long-term (hourly) and short-term (5-minute) statistics
- Automatic cascade: editing a state recalculates the affected short-term and long-term statistics
- Source-data guards prevent accidental edits when underlying data still exists
- Force-recalculate statistics for any entity and time range

**Bulk Operations**
- Multi-row selection with checkboxes (select all / individual)
- Bulk edit: apply the same value to many rows at once
- Bulk delete: remove many rows in a single transaction
- Works across state history, short-term stats, and long-term stats
- Per-row guard reporting: blocked rows are skipped and reported, never silently dropped

**Panel UI**
- Clean sidebar panel integrated into Home Assistant
- Entity picker with all available entities
- Three data sources: state history, long-term stats, short-term stats
- Go-to-date navigation for jumping to specific time ranges
- Locked-row indicators for statistics with underlying source data

**Services (for automations)**
- `history_editor.get_records` — query state history
- `history_editor.update_record` / `create_record` / `delete_record`
- `history_editor.recalculate_statistics`
- Bulk variants: `bulk_update_record`, `bulk_delete_record`, `bulk_update_statistic`, `bulk_delete_statistic`
- All visible in Developer Tools with response data

**Safe & Secure**
- Admin-only panel access
- Input validation on all endpoints
- Statistics consistency maintained automatically

## Quick Start

1. Install via HACS or manually
2. Add `history_editor:` to `configuration.yaml`
3. Restart Home Assistant
4. Find "History Editor" in your sidebar
5. Select an entity and start editing!

## Use Cases

- Fix incorrect sensor readings that polluted statistics
- Remove wrong spikes from long-term stats (energy dashboard, etc.)
- Fill gaps in historical data
- Clean up erroneous records in bulk
- Repair statistics after database migrations

## Documentation

See the [README](https://github.com/davefx/dfx-ha-history-editor/blob/main/README.md) for detailed service definitions and examples.

## Support

- [Report Issues](https://github.com/davefx/dfx-ha-history-editor/issues)
- [Contributing Guide](https://github.com/davefx/dfx-ha-history-editor/blob/main/CONTRIBUTING.md)

---

This component directly modifies your Home Assistant database. Always backup before making changes!
