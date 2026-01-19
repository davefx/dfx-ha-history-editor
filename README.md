# History Editor for Home Assistant

A custom Home Assistant component that provides a powerful interface for editing, creating, and deleting history records in your Home Assistant database.

## Features

- 🔍 **Entity Selector**: Choose any entity from your Home Assistant instance
- 📊 **Data Table View**: View all historical records in an organized table
- ✏️ **Edit Records**: Update state values, attributes, and timestamps
- ➕ **Create Records**: Add new historical data points
- 🗑️ **Delete Records**: Remove unwanted history entries
- 🎨 **Modern UI**: Clean, responsive interface integrated into Home Assistant
- 🔐 **Admin Only**: Requires administrator privileges for safety

## Installation

### HACS (Recommended)

1. Open HACS in your Home Assistant instance
2. Click on "Integrations"
3. Click the three dots in the top right corner
4. Select "Custom repositories"
5. Add this repository URL: `https://github.com/davefx/dfx-ha-history-editor`
6. Select category: "Integration"
7. Click "Add"
8. Find "History Editor" in the integration list and install it
9. Restart Home Assistant

### Manual Installation

1. Copy the `custom_components/history_editor` directory to your Home Assistant's `custom_components` directory
2. Restart Home Assistant

## Configuration

Add the following to your `configuration.yaml`:

```yaml
history_editor:
```

Then restart Home Assistant.

## Usage

### Web Interface

After installation and configuration, you'll find a new "History Editor" menu item in your Home Assistant sidebar.

1. **Select an Entity**: Choose the entity whose history you want to view/edit
2. **Set Record Limit**: Specify how many records to load (default: 100, max: 1000)
3. **Load Records**: Click to fetch the historical data
4. **Edit**: Click the "Edit" button on any record to modify its values
5. **Delete**: Click the "Delete" button to remove a record
6. **Add New**: Click "Add New Record" to create a new historical entry

### Services

The component provides four services that can be called from automations, scripts, or Developer Tools:

#### `history_editor.get_records`

Retrieve historical records for an entity.

```yaml
service: history_editor.get_records
data:
  entity_id: sensor.temperature
  limit: 100
  start_time: "2024-01-01T00:00:00"
  end_time: "2024-01-31T23:59:59"
```

**Parameters:**
- `entity_id` (required): The entity to retrieve records for
- `limit` (optional): Maximum number of records to return (default: 100)
- `start_time` (optional): Filter records after this datetime
- `end_time` (optional): Filter records before this datetime

#### `history_editor.update_record`

Update an existing history record.

```yaml
service: history_editor.update_record
data:
  state_id: 12345
  state: "23.5"
  attributes:
    unit_of_measurement: "°C"
    friendly_name: "Temperature"
  last_changed: "2024-01-15T10:30:00"
  last_updated: "2024-01-15T10:30:00"
```

**Parameters:**
- `state_id` (required): The ID of the state record to update
- `state` (optional): New state value
- `attributes` (optional): New attributes as JSON object
- `last_changed` (optional): New last_changed timestamp
- `last_updated` (optional): New last_updated timestamp

#### `history_editor.delete_record`

Delete a history record.

```yaml
service: history_editor.delete_record
data:
  state_id: 12345
```

**Parameters:**
- `state_id` (required): The ID of the state record to delete

#### `history_editor.create_record`

Create a new history record.

```yaml
service: history_editor.create_record
data:
  entity_id: sensor.temperature
  state: "22.5"
  attributes:
    unit_of_measurement: "°C"
    friendly_name: "Temperature"
  last_changed: "2024-01-15T09:00:00"
  last_updated: "2024-01-15T09:00:00"
```

**Parameters:**
- `entity_id` (required): The entity ID for the new record
- `state` (required): State value
- `attributes` (optional): Attributes as JSON object
- `last_changed` (optional): Timestamp for last_changed (defaults to now)
- `last_updated` (optional): Timestamp for last_updated (defaults to now)

## Use Cases

- **Data Correction**: Fix incorrect sensor readings or state values
- **Gap Filling**: Add missing data points in historical records
- **Testing**: Create sample historical data for testing dashboards and automations
- **Cleanup**: Remove erroneous or duplicate entries from your database
- **Migration**: Import historical data from other systems

## Important Notes

⚠️ **Warning**: This component directly modifies your Home Assistant database. Always backup your database before making changes!

- Changes are immediate and cannot be undone through the component
- The component requires the `recorder` component to be configured
- Only administrators can access the History Editor panel
- Database modifications may affect statistics and long-term data

## Troubleshooting

### Component doesn't load

- Check that `recorder` and `history` components are enabled
- Review Home Assistant logs for errors: `Settings -> System -> Logs`
- Ensure you've restarted Home Assistant after installation

### Panel not appearing

- Verify you're logged in as an administrator
- Clear your browser cache
- Check the component is listed in `Developer Tools -> Info`

### Services not working

- Confirm the component is loaded: Check `Developer Tools -> Services` for `history_editor.*` services
- Verify state_id values are correct (use get_records first)
- Check logs for detailed error messages

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Credits

Created by [@davefx](https://github.com/davefx)

## Disclaimer

This component is not officially associated with or endorsed by Home Assistant. Use at your own risk.
