# History Editor - UI Guide

## Overview

The History Editor provides a user-friendly web interface for managing Home Assistant's historical data. This guide explains the different parts of the UI and how to use them.

## Main Interface

The History Editor panel is accessible from the Home Assistant sidebar and consists of several key areas:

### 1. Header Section

```
🗄️ History Editor
```

The header displays the component name with a database icon, making it easy to identify.

### 2. Control Panel

The control panel includes:

- **Entity Selector**: A dropdown menu showing all available entities in your Home Assistant instance
- **Record Limit**: Input field to specify how many records to load (1-1000, default: 100)
- **Load Records**: Button to fetch historical data for the selected entity
- **Add New Record**: Button to create a new historical entry

### 3. Data Table

The data table displays historical records with the following columns:

| Column | Description |
|--------|-------------|
| ID | Unique identifier for the state record (state_id) |
| State | The value/state of the entity at that time |
| Attributes | JSON object containing entity attributes |
| Last Changed | Timestamp when the state value last changed |
| Last Updated | Timestamp when the record was last updated |
| Actions | Edit and Delete buttons for each record |

### 4. Edit/Create Modal

When clicking "Edit" on a record or "Add New Record", a modal dialog appears with:

- **State ID**: Read-only field showing the record ID (or "NEW" for new records)
- **Entity ID**: The entity identifier (editable for new records)
- **State**: The state value (text input)
- **Attributes**: JSON editor for entity attributes
- **Last Changed**: Date and time picker for the last_changed timestamp
- **Last Updated**: Date and time picker for the last_updated timestamp
- **Save/Cancel**: Action buttons to save or discard changes

## Using the Interface

### Loading Historical Data

1. Click the **Entity Selector** dropdown
2. Choose an entity from the list (e.g., `sensor.temperature`)
3. Optionally adjust the **Record Limit** (default is 100 records)
4. Click **Load Records**
5. The table will populate with historical data

### Editing a Record

1. Find the record you want to edit in the table
2. Click the **Edit** button in the Actions column
3. Modify the desired fields in the modal dialog
4. Click **Save** to apply changes or **Cancel** to discard

### Creating a New Record

1. Click the **Add New Record** button
2. Fill in the required fields:
   - Entity ID (required)
   - State (required)
   - Attributes (optional, must be valid JSON)
   - Timestamps (optional, defaults to current time)
3. Click **Save** to create the record

### Deleting a Record

1. Find the record you want to delete
2. Click the **Delete** button in the Actions column
3. Confirm the deletion in the confirmation dialog
4. The record will be permanently removed from the database

## Visual Features

### Color Coding

- **Primary actions** (Load, Save): Blue/primary color
- **Secondary actions** (Edit, Cancel): Gray/secondary color  
- **Destructive actions** (Delete): Red/error color

### Interactive Elements

- Table rows highlight on hover for better visibility
- Buttons have hover effects showing they're clickable
- Long attribute values are truncated with ellipsis (hover to see full value)
- Modal overlays have a semi-transparent background

### Responsive Design

The interface adapts to different screen sizes:
- On desktop: Full table with all columns visible
- On tablets/mobile: May scroll horizontally for full data view
- Modal dialogs are centered and sized appropriately

## Tips for Effective Use

1. **Start Small**: Load a small number of records first to verify you have the right entity
2. **Use Filters**: Consider adding date filters in future versions to narrow down results
3. **Backup First**: Always backup your Home Assistant database before making bulk changes
4. **Verify Changes**: After editing, reload the records to confirm your changes were applied
5. **JSON Formatting**: When editing attributes, use proper JSON syntax (e.g., `{"key": "value"}`)

## Accessibility

- All interactive elements are keyboard accessible
- Proper ARIA labels for screen readers
- Clear focus indicators for keyboard navigation
- Semantic HTML structure

## Common UI Patterns

### Empty State

When no records are loaded or found, you'll see:
```
📋
No records found
```

### Loading State

While records are being loaded (in future versions):
```
⏳ Loading records...
```

### Error State

If an error occurs:
```
❌ Error: [Error message]
```

## Browser Compatibility

The History Editor panel works with modern browsers that support:
- ES6 JavaScript
- CSS Custom Properties (CSS Variables)
- Custom Elements (Web Components)

Recommended browsers:
- Chrome/Edge 90+
- Firefox 88+
- Safari 14+

## Future UI Enhancements

Planned improvements:
- Search/filter functionality within loaded records
- Bulk edit operations
- Export to CSV/JSON
- Import from CSV/JSON
- Pagination for large datasets
- Sorting by columns
- Date range picker for filtering
- Real-time updates when records are modified
- Undo/redo functionality
- Record comparison view
