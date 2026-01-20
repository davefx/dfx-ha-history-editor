# Debugging the Entity Picker

## Overview
The History Editor now includes comprehensive debug logging to help diagnose issues with the entity picker not appearing.

## How to Enable Debug Mode

There are two ways to enable debug mode:

### Method 1: URL Parameter (Temporary)
Add `?debug` to the URL when accessing the History Editor panel:
```
http://your-home-assistant:8123/history-editor?debug
```

### Method 2: LocalStorage (Persistent)
Open your browser's Developer Tools Console and run:
```javascript
localStorage.setItem('history_editor_debug', 'true')
```

Then refresh the page. To disable debug mode:
```javascript
localStorage.removeItem('history_editor_debug')
```

## What Debug Mode Shows

### Visual Debug Panel
When debug mode is enabled, you'll see a debug status panel at the top of the History Editor showing:

1. **Panel Status**: Whether the panel has rendered successfully
2. **Home Assistant Connection**: Whether the hass object is connected
3. **Entity Picker Element**: Whether the DOM element was found and its type
4. **Entity Picker Ready**: The initialization state of the entity picker
5. **Custom Element Defined**: Whether the ha-entity-picker custom element is defined

Each status shows:
- ✓ Green: Success/OK
- ⚠ Orange: In progress/waiting
- ✗ Red: Error/not found

### Console Logging
Open your browser's Developer Tools (F12) and go to the Console tab. Filter by `[HistoryEditor]` to see:

- Component lifecycle events (constructor, connectedCallback)
- Home Assistant connection status
- Entity picker element detection and type
- Custom element definition status
- Initialization progress
- Any errors or warnings

## Common Issues and What to Look For

### Issue: Entity picker not appearing

**Check the debug panel:**
1. If "Panel Status" is not green → The panel didn't render properly
2. If "Home Assistant Connection" is not green → Connection issue with HA
3. If "Entity Picker Element" shows "Not found" → DOM element wasn't created
4. If "Entity Picker Element" shows "HTMLElement" → Custom element not upgraded
5. If "Entity Picker Ready" is not green → Initialization didn't complete

**Check the console logs:**
Look for the sequence of messages. A successful initialization looks like:
```
[HistoryEditor] Constructor called - Debug mode enabled
[HistoryEditor] connectedCallback called
[HistoryEditor] Waiting for home-assistant custom element to be defined...
[HistoryEditor] home-assistant custom element is now defined
[HistoryEditor] _ensureInitialized called, initialized: false
[HistoryEditor] renderPanel called
[HistoryEditor] Panel UI rendered, setting up event listeners
[HistoryEditor] _triggerEntityPickerLoad called
[HistoryEditor] Entity picker element from DOM: found
[HistoryEditor] hass setter called, hass: defined
[HistoryEditor] Starting entity picker initialization
[HistoryEditor] Waiting for ha-entity-picker to be defined...
[HistoryEditor] Triggering ha-entity-picker load...
[HistoryEditor] Attempting to load ha-entity-picker component...
[HistoryEditor] Triggering component load by element creation in DOM
[HistoryEditor] Temporary element added to DOM to trigger load
[HistoryEditor] ha-entity-picker custom element is now defined
[HistoryEditor] Setting hass on newly replaced entity picker
[HistoryEditor] Marking entity picker as ready
```

### Issue: Custom element not defined

If you see:
```
[HistoryEditor] Waiting for ha-entity-picker to be defined...
[HistoryEditor] Timeout promise resolved
```

This means the ha-entity-picker custom element didn't load within 10 seconds. **This issue should now be fixed** with the automatic component loading. If you still see this:
- Try clearing your browser cache completely
- Make sure Home Assistant frontend is fully updated
- Check browser console for any network errors preventing component loading

**Solution:** Refresh the page, clear browser cache, or check browser console for other errors.

### Issue: Element found but wrong type

If "Entity Picker Element" shows `Found (HTMLElement)` instead of `Found (ha-entity-picker)`:

This means the custom element wasn't properly upgraded. The code will try to replace it automatically. Check console for:
```
[HistoryEditor] Replacing uninitialized entity picker with properly initialized element
```

If you don't see this message, there may be an issue with the custom element registration.

## Reporting Issues

When reporting entity picker issues, please include:

1. Screenshot of the debug status panel
2. Full console log output (filtered by `[HistoryEditor]`)
3. Browser version and type
4. Home Assistant version
5. Any other error messages in the console

## Performance Note

Debug mode adds console logging and a visual debug panel. While optimized to minimize performance impact, it's recommended to disable debug mode during normal use once issues are resolved.

## Disabling Debug Mode

### If using URL parameter:
Simply remove `?debug` from the URL

### If using localStorage:
```javascript
localStorage.removeItem('history_editor_debug')
```
Then refresh the page.
