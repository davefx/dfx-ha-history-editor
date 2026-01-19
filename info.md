# History Editor for Home Assistant

Edit, create, and delete history records directly from your Home Assistant database with a user-friendly interface.

## Features

✨ **Full CRUD Operations**
- Create new historical records
- Read and view existing records
- Update state values and attributes
- Delete unwanted entries

🎯 **Entity Selector**
- Choose from all available entities
- Configurable record limits (1-1000)
- Easy navigation and filtering

📊 **Modern Data Table**
- Clean, responsive interface
- View state, attributes, and timestamps
- Inline edit and delete actions
- Attribute preview with full view on hover

🔒 **Safe & Secure**
- Admin-only access required
- Direct database integration
- Comprehensive error handling
- Validates all inputs

## Quick Start

1. Install via HACS or manually
2. Add `history_editor:` to `configuration.yaml`
3. Restart Home Assistant
4. Find "History Editor" in your sidebar
5. Select an entity and start editing!

## Use Cases

- Fix incorrect sensor readings
- Fill gaps in historical data
- Import data from other systems
- Clean up erroneous records
- Test dashboards with sample data

## Documentation

For detailed documentation, service definitions, and examples, see the [README](https://github.com/davefx/dfx-ha-history-editor/blob/main/README.md).

## Support

- [Report Issues](https://github.com/davefx/dfx-ha-history-editor/issues)
- [View Documentation](https://github.com/davefx/dfx-ha-history-editor)
- [Contributing Guide](https://github.com/davefx/dfx-ha-history-editor/blob/main/CONTRIBUTING.md)

---

⚠️ **Important**: This component directly modifies your Home Assistant database. Always backup before making changes!
