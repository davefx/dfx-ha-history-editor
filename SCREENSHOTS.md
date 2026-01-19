# Screenshots and UI Documentation

## History Editor Panel Interface

Since this is a text-based implementation, here's a detailed description of what the UI looks like when rendered:

### Main Panel View

```
┌─────────────────────────────────────────────────────────────────┐
│ 🗄️ History Editor                                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ Select Entity:  [── Choose an entity ──────▼]                   │
│                                                                  │
│ Record Limit:   [100        ]                                   │
│                                                                  │
│ [Load Records]  [Add New Record]                                │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ ╔══════════════════════════════════════════════════════════════╗│
│ ║ ID    │ State │ Attributes    │ Last Changed │ Actions      ║│
│ ╠══════════════════════════════════════════════════════════════╣│
│ ║ 12345 │ 22.5  │ {"unit": "°C"}│ 2024-01-15...│ [Edit][Del] ║│
│ ║ 12344 │ 22.3  │ {"unit": "°C"}│ 2024-01-15...│ [Edit][Del] ║│
│ ║ 12343 │ 22.8  │ {"unit": "°C"}│ 2024-01-15...│ [Edit][Del] ║│
│ ║ ...                                                          ║│
│ ╚══════════════════════════════════════════════════════════════╝│
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Edit Record Modal

```
┌─────────────────────────────────────────────────────────────────┐
│ Edit Record                                              [✕]    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ State ID:                                                        │
│ [12345                                            ] (read-only)  │
│                                                                  │
│ Entity ID:                                                       │
│ [sensor.temperature                               ] (read-only)  │
│                                                                  │
│ State:                                                           │
│ [22.5                                                        ]   │
│                                                                  │
│ Attributes (JSON):                                               │
│ ┌──────────────────────────────────────────────────────────────┐│
│ │ {                                                            ││
│ │   "unit_of_measurement": "°C",                              ││
│ │   "device_class": "temperature"                             ││
│ │ }                                                            ││
│ └──────────────────────────────────────────────────────────────┘│
│                                                                  │
│ Last Changed:                                                    │
│ [2024-01-15T10:30                                            ]   │
│                                                                  │
│ Last Updated:                                                    │
│ [2024-01-15T10:30                                            ]   │
│                                                                  │
│                                           [Cancel]  [Save]       │
└─────────────────────────────────────────────────────────────────┘
```

### Empty State (No Records)

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                  │
│                            📋                                    │
│                                                                  │
│                      No records found                            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Color Scheme

The interface uses Home Assistant's native theming:

- **Primary Actions** (Load, Save): Blue (#03a9f4 or theme primary color)
- **Secondary Actions** (Edit, Cancel): Gray (theme secondary color)
- **Destructive Actions** (Delete): Red (#f44336 or theme error color)
- **Background**: Theme card background color
- **Text**: Theme primary text color
- **Borders**: Theme divider color

## Responsive Design

### Desktop (>768px)
- Full table view with all columns visible
- Side-by-side controls
- Wide modal dialogs

### Tablet (768px)
- Table may scroll horizontally
- Controls stack if needed
- Modal at 90% width

### Mobile (<768px)
- Simplified table view
- Stacked controls
- Full-width modal

## Accessibility Features

- ✅ Keyboard navigation support
- ✅ ARIA labels for screen readers
- ✅ Focus indicators on all interactive elements
- ✅ Semantic HTML structure
- ✅ Color contrast meets WCAG AA standards

## UI Components Used

1. **Entity Selector**: Native HTML `<select>` dropdown
2. **Data Table**: HTML `<table>` with styled rows
3. **Modal Dialogs**: Custom modal with overlay
4. **Form Inputs**: Standard HTML form elements
5. **Buttons**: Styled buttons with hover effects
6. **JSON Editor**: `<textarea>` with monospace font

## User Interactions

### Loading Records
1. Select entity from dropdown
2. (Optional) Adjust record limit
3. Click "Load Records"
4. Records appear in table

### Editing a Record
1. Click "Edit" button on a row
2. Modal opens with pre-filled data
3. Modify desired fields
4. Click "Save" to apply changes
5. Modal closes automatically

### Deleting a Record
1. Click "Delete" button on a row
2. Confirmation dialog appears
3. Confirm deletion
4. Record is removed from database

### Creating a Record
1. Click "Add New Record" button
2. Modal opens with empty form
3. Fill in required fields (entity_id, state)
4. (Optional) Add attributes and timestamps
5. Click "Save" to create record

## Integration with Home Assistant UI

The panel seamlessly integrates with Home Assistant:
- Appears in the sidebar with database icon
- Matches HA's current theme (light/dark mode)
- Uses HA's native UI patterns and conventions
- Requires admin privileges to access
- Responsive to theme changes

## Future UI Enhancements

Potential improvements for future versions:
- [ ] Real-time data loading via WebSocket
- [ ] Pagination controls
- [ ] Column sorting (click headers)
- [ ] Search/filter within loaded records
- [ ] Export table to CSV
- [ ] Import from CSV
- [ ] Bulk selection and operations
- [ ] Confirmation toasts instead of alerts
- [ ] Loading spinners and progress indicators
- [ ] Undo/redo functionality
- [ ] Dark mode optimization
