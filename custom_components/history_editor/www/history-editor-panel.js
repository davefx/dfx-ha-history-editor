// Translations are loaded from /history_editor_panel/translations/<lang>.json
// at runtime, with English as the fallback.  The _t() helper on the class
// reads from this cache.  Adding a language is just dropping a JSON file.
const _translationCache = {};

async function _loadTranslation(lang) {
  if (_translationCache[lang]) return _translationCache[lang];
  try {
    const resp = await fetch(`/history_editor_panel/translations/${lang}.json`);
    if (!resp.ok) throw new Error(resp.status);
    _translationCache[lang] = await resp.json();
  } catch {
    _translationCache[lang] = null;
  }
  return _translationCache[lang];
}

// Inline English fallback so the panel works even if the JSON fetch fails
// (e.g. browser offline, static path misconfigured).
const _FALLBACK_EN = {
    title: 'History Editor',
    data_source: 'Data Source',
    states: 'State History',
    statistics_long: 'Statistics (Long-term, hourly)',
    statistics_short: 'Statistics (Short-term, 5 min)',
    record_limit: 'Record Limit',
    go_to_date: 'Go to Date/Time',
    go_to_date_title: 'Jump to records from this date',
    go_to_btn: 'Go to Date',
    clear_date: 'Clear Date',
    add_new: 'Add New Record',
    scroll_top: '⬆ Top',
    scroll_top_title: 'Go to the first (most recent) records',
    scroll_bottom: '⬇ Bottom',
    scroll_bottom_title: 'Go to the last (oldest) records',
    select_date_first: 'Please select a date and time first',
    // Table headers
    th_id: 'ID',
    th_state: 'State',
    th_attributes: 'Attributes',
    th_timestamp: 'Timestamp',
    th_actions: 'Actions',
    th_start_time: 'Start Time',
    th_mean: 'Mean',
    th_min: 'Min',
    th_max: 'Max',
    th_sum: 'Sum',
    select_all: 'Select all visible',
    select_all_unlocked: 'Select all unlocked visible rows',
    // Actions
    edit: 'Edit',
    delete: 'Delete',
    save: 'Save',
    cancel: 'Cancel',
    // Bulk
    n_selected: '{n} selected',
    edit_selected: 'Edit Selected',
    delete_selected: 'Delete Selected',
    clear_selection: 'Clear',
    // Modal
    add_new_title: 'Add New Record',
    edit_record_title: 'Edit Record',
    edit_statistic_title: 'Edit Statistic',
    state_id_label: 'State ID',
    statistic_id_label: 'Statistic ID',
    entity_id_label: 'Entity ID',
    state_label: 'State',
    attributes_label: 'Attributes (JSON)',
    timestamp_label: 'Timestamp',
    start_time_label: 'Start Time',
    mean_label: 'Mean',
    min_label: 'Min',
    max_label: 'Max',
    sum_label: 'Sum',
    state_last_label: 'State (last)',
    // Bulk modal
    bulk_edit_states: 'Bulk Edit ({n} state records)',
    bulk_edit_short: 'Bulk Edit ({n} short-term stats)',
    bulk_edit_long: 'Bulk Edit ({n} long-term stats)',
    bulk_editing: 'Editing {n} selected record(s).',
    bulk_help: 'Leave a field empty to keep the current value on each selected record.',
    bulk_provide_state_or_attrs: 'Provide a new state value or new attributes (or both) before saving.',
    bulk_provide_numeric: 'Provide at least one numeric value (mean, min, max, sum, state) before saving.',
    // Alerts
    invalid_json: 'Invalid JSON in attributes field',
    invalid_json_detail: 'Attributes must be valid JSON: {err}',
    record_created: 'Record created successfully.\n\nTo see the change in HA history graphs and statistics panels, navigate to another page and back, or reload the browser tab.',
    record_updated: 'Record updated successfully.\n\nTo see the change in HA history graphs and statistics panels, navigate to another page and back, or reload the browser tab.',
    record_deleted: 'Record deleted successfully.\n\nTo see the change in HA history graphs and statistics panels, navigate to another page and back, or reload the browser tab.',
    stat_updated: 'Statistic updated successfully.\n\nTo see the change in HA graphs and statistics panels, navigate to another page and back, or reload the browser tab.',
    stat_deleted: 'Statistic deleted successfully.\n\nTo see the change in HA graphs and statistics panels, navigate to another page and back, or reload the browser tab.',
    error_creating: 'Error creating record: {err}',
    error_updating: 'Error updating record: {err}',
    error_deleting: 'Error deleting record: {err}',
    error_saving: 'Error saving record: {err}',
    error_saving_stat: 'Error saving statistic: {err}',
    error_updating_stat: 'Error updating statistic: {err}',
    error_deleting_stat: 'Error deleting statistic: {err}',
    error_loading: 'Error loading records: {err}',
    error_loading_stats: 'Error loading statistics: {err}',
    failed_load: 'Failed to load records: {err}',
    failed_load_stats: 'Failed to load statistics: {err}',
    error_console: 'Error loading records. Please check the console for details.',
    error_console_stats: 'Error loading statistics. Please check the console for details.',
    // Delete confirmation
    confirm_delete: 'Are you sure you want to delete this record?',
    confirm_delete_stat: 'Are you sure you want to delete this statistic?',
    // Bulk delete
    bulk_delete_confirm_states: 'Delete {n} state record(s)?\n\nThis cannot be undone. Affected statistics periods will be recalculated.',
    bulk_delete_confirm_stats: 'Delete {n} statistics row(s)?\n\nThis cannot be undone. Rows blocked by the source-data guard will be skipped and reported.',
    bulk_delete_error: 'Error: {err}',
    bulk_delete_failed: 'Bulk delete failed: {err}',
    bulk_update_error: 'Error: {err}',
    bulk_update_failed: 'Bulk update failed: {err}',
    // Bulk result
    n_records_updated: '{n} record(s) updated.',
    n_records_deleted: '{n} record(s) deleted.',
    blocked_header: '{n} blocked by source-data guard:',
    blocked_item: '  • id={id}: {reason}',
    blocked_more: '  • …and {n} more',
    not_found: '{n} id(s) not found.',
    stats_stale: 'Statistics may be stale — call history_editor.recalculate_statistics to fix.',
    see_change: 'To see the change in HA graphs, navigate away and back, or reload the browser tab.',
    // Empty states
    no_records: 'No records found',
    no_statistics: 'No statistics records found',
    // Pagination
    load_prev: '↑ Scroll up or click to load earlier records',
    loading_more: '⟳ Loading more records...',
    // Locked rows
    locked_state_history: 'State history exists for this period — edit state history instead',
    locked_short_term: 'Short-term statistics exist for this period — edit short-term statistics instead',
    // Warning screen
    warning_icon: '🐉',
    warning_title: 'Here be dragons!',
    warning_text: 'This tool directly edits your Home Assistant database — state history, short-term statistics, and long-term statistics.<br><br>You could easily break your data if you don\'t know what you are doing. <strong>Always back up your database before making changes.</strong>',
    warning_proceed: 'I know what I\'m doing. Proceed.',
    na: 'N/A',
};

class HistoryEditorPanel extends HTMLElement {
  constructor() {
    super();
    // Enable debug mode by checking URL parameter or localStorage
    this._debugMode = new URLSearchParams(window.location.search).has('debug') ||
                      localStorage.getItem('history_editor_debug') === 'true';

    if (this._debugMode) {
      console.log('[HistoryEditor] Constructor called - Debug mode enabled');
    }
    this._hass = null;
    this._lang = 'en';
    this.selectedEntity = null;
    this.records = [];
    this._initialized = false;
    this._entityFormInitialized = false; // Track if ha-form is set up
    this._statusElements = {}; // Cache for status elements
    this.goToDate = null; // Date to load records from
    this.dataSource = 'states'; // 'states', 'statistics', 'statistics_short_term'
    // Infinite scroll pagination state
    this._pageCursors = [];       // end_time cursor used to load each page
    this._domFirst = 0;           // First page index currently in DOM
    this._domLast = -1;           // Last page index currently in DOM
    this._hasMore = false;        // Whether more records exist below current view
    this._loadingMore = false;    // Lock to prevent concurrent page loads
    this._loadingPrev = false;    // Lock to prevent concurrent prev-page loads
    this._isAutoLoading = false;  // Cooldown flag to prevent observer-triggered load loops
    this._autoLoadCooldownTimer = null; // Timer ID for clearing _isAutoLoading cooldown
    this._pageSize = 100;         // Current page size
    this._scrollObserver = null;  // IntersectionObserver for scroll sentinel
    this._topObserver = null;     // IntersectionObserver for load-prev row
    this._mutationObserver = null; // MutationObserver to detect DOM clearing
    // Bulk-selection state.  Set of state_id (states mode) or stat id
    // (statistics mode).  Persists across reloads within the same data
    // source / entity; cleared when those change.
    this._selectedIds = new Set();
    this._editMode = 'single';     // 'single' | 'add' | 'bulk'
  }

  _debugLog(...args) {
    if (this._debugMode) {
      console.log(...args);
    }
  }

  _t(key, params) {
    const dict = _translationCache[this._lang] || _FALLBACK_EN;
    let s = dict[key] ?? _FALLBACK_EN[key] ?? key;
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        s = s.replace(`{${k}}`, v);
      }
    }
    return s;
  }

  async connectedCallback() {
    this._debugLog('[HistoryEditor] connectedCallback called');
    // Watch for the panel's direct children being removed (e.g. HA clearing the DOM
    // during a websocket reconnection while the tab stays visible and hass setter is
    // not being called). Guard against duplicate registration.
    if (!this._mutationObserver) {
      this._mutationObserver = new MutationObserver(() => {
        if (!this.querySelector('#records-display')) {
          this._debugLog('[HistoryEditor] MutationObserver: panel DOM cleared, re-initializing');
          this._ensureInitialized();
        }
      });
      this._mutationObserver.observe(this, { childList: true });
    }
    // Wait for Home Assistant to be fully loaded
    this._debugLog('[HistoryEditor] Waiting for home-assistant custom element to be defined...');
    await customElements.whenDefined('home-assistant');
    this._debugLog('[HistoryEditor] home-assistant custom element is now defined');
    // Guard: element may have been disconnected while awaiting
    if (!this.isConnected) {
      this._debugLog('[HistoryEditor] Element disconnected before initialization, skipping');
      return;
    }
    this._ensureInitialized();
    // Re-establish scroll observers that were torn down in disconnectedCallback.
    // _setupScrollObserver / _setupTopObserver return early if there is nothing to
    // observe, so it is always safe to call them here.
    this._setupScrollObserver();
    this._setupTopObserver();
    // Re-initialize when the browser tab becomes visible again (e.g. after long background period).
    // Guard against duplicate registration in case connectedCallback fires more than once.
    if (!this._visibilityHandler) {
      this._visibilityHandler = () => {
        if (!document.hidden) {
          this._debugLog('[HistoryEditor] Tab became visible, checking panel state');
          this._ensureInitialized();
        }
      };
      document.addEventListener('visibilitychange', this._visibilityHandler);
    }
  }

  disconnectedCallback() {
    this._debugLog('[HistoryEditor] disconnectedCallback called');
    // Clean up visibility listener
    if (this._visibilityHandler) {
      document.removeEventListener('visibilitychange', this._visibilityHandler);
      this._visibilityHandler = null;
    }
    // Clean up mutation observer
    if (this._mutationObserver) {
      this._mutationObserver.disconnect();
      this._mutationObserver = null;
    }
    // Do NOT reset _initialized or _entityFormInitialized here. HA sometimes briefly
    // disconnects and reconnects the element during re-renders. Resetting these flags
    // would cause renderPanel() to be called on reconnection, wiping the loaded records.
    // The #records-display check in _ensureInitialized handles truly cleared DOM, and
    // the constructor handles genuinely fresh instances.
    this._statusElements = {};
    if (this._scrollObserver) {
      this._scrollObserver.disconnect();
      this._scrollObserver = null;
    }
    if (this._topObserver) {
      this._topObserver.disconnect();
      this._topObserver = null;
    }
    if (this._autoLoadCooldownTimer) {
      clearTimeout(this._autoLoadCooldownTimer);
      this._autoLoadCooldownTimer = null;
    }
  }

  set hass(hass) {
    this._debugLog('[HistoryEditor] hass setter called, hass:', hass ? 'defined' : 'null');
    this._hass = hass;
    if (hass && hass.language) {
      const lang = hass.language.split('-')[0];
      if (lang !== this._lang) {
        this._lang = lang;
        this._debugLog('[HistoryEditor] Language set to:', this._lang);
        if (!_translationCache[lang]) {
          _loadTranslation(lang).then((dict) => {
            if (dict && this._initialized) {
              this._debugLog('[HistoryEditor] Translation loaded, re-rendering');
              this._entityFormInitialized = false;
              this.renderPanel();
            }
          });
        }
      }
    }
    this._updateDebugStatus('hass', hass ? 'Connected ✓' : 'Not connected', hass ? 'status-ok' : 'status-error');
    this._ensureInitialized();
    // Set up ha-form when hass becomes available
    if (this._initialized && hass) {
      this._setupEntityForm();
    }
  }

  get hass() {
    return this._hass;
  }

  _setupEntityForm() {
    const entityForm = this.querySelector('#entity-form');
    
    if (!entityForm || !this._hass) {
      this._debugLog('[HistoryEditor] Cannot setup entity form - element or hass missing');
      return;
    }
    
    // Only initialize once to avoid duplicate listeners
    if (this._entityFormInitialized) {
      this._debugLog('[HistoryEditor] Entity form already initialized');
      // Just update hass if needed
      if (entityForm.hass !== this._hass) {
        entityForm.hass = this._hass;
      }
      return;
    }
    
    this._debugLog('[HistoryEditor] Setting up ha-form with entity selector');
    
    // Define the schema for ha-form with entity selector
    const schema = [
      {
        name: 'entity_id',
        required: false,
        selector: {
          entity: {}
        }
      }
    ];
    
    // Set hass and schema on the form
    entityForm.hass = this._hass;
    entityForm.schema = schema;
    entityForm.data = { entity_id: this.selectedEntity || null };
    
    // Listen for value changes (only set up once)
    entityForm.addEventListener('value-changed', (ev) => {
      this._debugLog('[HistoryEditor] Entity form value changed:', ev.detail.value);
      if (ev.detail.value && ev.detail.value.entity_id) {
        const newEntity = ev.detail.value.entity_id;
        if (newEntity !== this.selectedEntity && this._selectedIds && this._selectedIds.size > 0) {
          this._clearSelection();
        }
        this.selectedEntity = newEntity;
        this._debugLog('[HistoryEditor] Selected entity:', this.selectedEntity);
        // Automatically load records when entity is selected
        this.loadRecords();
      } else {
        // Handle case where entity is cleared
        this.selectedEntity = null;
        this.records = [];
        if (this._clearSelection) this._clearSelection();
        this.displayRecords([]);
        this._debugLog('[HistoryEditor] Entity selection cleared');
      }
    });
    
    // Mark as initialized
    this._entityFormInitialized = true;
    
    this._updateDebugStatus('picker-ready', 'ha-form with entity selector ready ✓', 'status-ok');
    this._updateDebugStatus('picker-element', 'Found (ha-form with entity selector)', 'status-ok');
    this._updateDebugStatus('custom-element', 'Using ha-form + selector ✓', 'status-ok');
  }

  _ensureInitialized() {
    this._debugLog('[HistoryEditor] _ensureInitialized called, initialized:', this._initialized);
    if (!this._initialized) {
      this._debugLog('[HistoryEditor] Initializing panel for the first time');
      this._initialized = true;
      if (this._needsWarning()) {
        this._showWarning();
      } else {
        this.renderPanel();
      }
    } else if (!this.querySelector('#records-display') && !this.querySelector('#warning-screen')) {
      // Panel was previously initialized but DOM was cleared (e.g. after HA reconnection)
      this._debugLog('[HistoryEditor] Panel DOM was cleared, re-initializing');
      this._entityFormInitialized = false;
      this.renderPanel();
    }
  }

  _needsWarning() {
    return localStorage.getItem('history_editor_warning_accepted') !== 'true';
  }

  _showWarning() {
    this.style.display = 'flex';
    this.style.flexDirection = 'column';
    this.style.height = '100%';
    this.style.overflow = 'hidden';
    this.style.boxSizing = 'border-box';
    this.style.padding = '16px';
    this.style.background = 'var(--primary-background-color)';
    this.style.color = 'var(--primary-text-color)';

    this.innerHTML = `
      <style>
        .warning-screen {
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          height: 100%;
          text-align: center;
          gap: 24px;
          padding: 32px;
        }
        .warning-icon { font-size: 64px; }
        .warning-title {
          font-size: 22px;
          font-weight: 600;
          color: var(--error-color, #db4437);
        }
        .warning-text {
          font-size: 15px;
          max-width: 520px;
          line-height: 1.6;
          color: var(--secondary-text-color);
        }
        .warning-proceed {
          padding: 12px 32px;
          border-radius: 6px;
          border: none;
          background: var(--primary-color);
          color: var(--text-primary-color, white);
          font-size: 15px;
          font-weight: 500;
          cursor: pointer;
        }
        .warning-proceed:hover {
          opacity: 0.9;
        }
      </style>
      <div id="warning-screen" class="warning-screen">
        <div class="warning-icon">${this._t('warning_icon')}</div>
        <div class="warning-title">${this._t('warning_title')}</div>
        <div class="warning-text">${this._t('warning_text')}</div>
        <button class="warning-proceed" id="warning-accept-btn">${this._t('warning_proceed')}</button>
      </div>
    `;

    this.querySelector('#warning-accept-btn').addEventListener('click', () => {
      localStorage.setItem('history_editor_warning_accepted', 'true');
      this.renderPanel();
    });
  }

  _updateDebugStatus(key, value, statusClass = '') {
    // Only update if debug mode is enabled
    if (!this._debugMode) return;
    
    // Use cached element if available, otherwise query and cache
    let element = this._statusElements[key];
    if (!element) {
      element = this.querySelector(`#status-${key}`);
      if (element) {
        this._statusElements[key] = element;
      }
    }
    
    if (element) {
      element.textContent = value;
      // Remove all status classes
      element.classList.remove('status-ok', 'status-error', 'status-warning');
      // Add the new status class if provided
      if (statusClass) {
        element.classList.add(statusClass);
      }
    }
  }

  renderPanel() {
    this._debugLog('[HistoryEditor] renderPanel called');
    this._statusElements = {}; // Clear cached status elements
    this._entityFormInitialized = false; // Reset form initialization flag

    // Apply layout styles directly to the host element. The :host CSS rule in the
    // inline <style> tag only works inside a shadow DOM, so when HA renders this
    // element in its own shadow DOM context the :host selector does not match this
    // element. Setting the properties here guarantees the flex-column layout and
    // bounded height required for .table-container's overflow:auto to work.
    this.style.display = 'flex';
    this.style.flexDirection = 'column';
    this.style.height = '100%';
    this.style.overflow = 'hidden';
    this.style.boxSizing = 'border-box';
    this.style.padding = '16px';
    this.style.background = 'var(--primary-background-color)';
    this.style.color = 'var(--primary-text-color)';

    this._debugLog('[HistoryEditor] Setting innerHTML to render panel UI');
    this.innerHTML = `
      <style>
        :host {
          display: flex;
          flex-direction: column;
          height: 100%;
          box-sizing: border-box;
          padding: 16px;
          background: var(--primary-background-color);
          color: var(--primary-text-color);
          overflow: hidden;
        }
        .header {
          display: flex;
          align-items: center;
          margin-bottom: 24px;
        }
        .header h1 {
          margin: 0;
          font-size: 24px;
          font-weight: 500;
        }
        .controls {
          display: flex;
          gap: 16px;
          margin-bottom: 24px;
          flex-wrap: wrap;
          align-items: flex-end;
        }
        .control-group {
          display: flex;
          flex-direction: column;
          gap: 4px;
        }
        label {
          font-size: 14px;
          font-weight: 500;
        }
        select, input, button {
          padding: 8px 12px;
          border: 1px solid var(--divider-color);
          background: var(--card-background-color);
          color: var(--primary-text-color);
          border-radius: 4px;
          font-size: 14px;
        }
        select {
          min-width: 250px;
        }
        ha-form {
          display: block;
          width: 100%;
        }
        .entity-form-container {
          flex: 1;
          min-width: 300px;
        }
        button {
          cursor: pointer;
          background: var(--primary-color);
          color: var(--text-primary-color);
          border: none;
          font-weight: 500;
          transition: background 0.2s;
        }
        button:hover {
          background: var(--dark-primary-color);
        }
        button.secondary {
          background: var(--secondary-background-color);
          color: var(--primary-text-color);
          border: 1px solid var(--divider-color);
        }
        button.secondary:hover {
          background: var(--divider-color);
        }
        button.danger {
          background: var(--error-color);
          color: white;
        }
        button.danger:hover {
          background: var(--error-state-color);
        }
        button:disabled {
          opacity: 0.45;
          cursor: not-allowed;
        }
        .table-container {
          background: var(--card-background-color);
          border-radius: 8px;
          overflow: auto;
          box-shadow: 0 2px 4px rgba(0,0,0,0.1);
          flex: 1;
          min-height: 0;
        }
        table {
          width: 100%;
          border-collapse: collapse;
          min-width: 480px;
        }
        th {
          background: var(--secondary-background-color);
          padding: 12px;
          text-align: left;
          font-weight: 500;
          border-bottom: 2px solid var(--divider-color);
        }
        td {
          padding: 12px;
          border-bottom: 1px solid var(--divider-color);
        }
        tr:hover {
          background: var(--secondary-background-color);
        }
        .actions {
          display: flex;
          gap: 8px;
        }
        .actions button {
          padding: 4px 8px;
          font-size: 12px;
        }
        @media (max-width: 600px) {
          .controls {
            flex-direction: column;
            align-items: stretch;
          }
          .control-group, .entity-form-container {
            width: 100%;
            min-width: unset;
          }
          .control-group input {
            width: 100%;
            box-sizing: border-box;
          }
          #go-to-btn, #clear-date-btn, #add-btn, #scroll-top-btn, #scroll-bottom-btn {
            width: 100%;
          }
          table, thead, tbody, th, td, tr {
            display: block;
          }
          thead tr {
            display: none;
          }
          tbody tr {
            margin-bottom: 12px;
            border: 1px solid var(--divider-color);
            border-radius: 4px;
            overflow: hidden;
            min-width: unset;
          }
          td {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 12px;
            border-bottom: 1px solid var(--divider-color);
          }
          td:last-child {
            border-bottom: none;
          }
          td::before {
            content: attr(data-label);
            font-weight: 500;
            margin-right: 8px;
            flex-shrink: 0;
          }
          td.actions {
            justify-content: flex-end;
          }
          td.actions::before {
            display: none;
          }
          .table-container {
            overflow: auto;
          }
        }
        .empty-state {
          text-align: center;
          padding: 48px;
          color: var(--secondary-text-color);
        }
        .empty-state ha-icon {
          font-size: 48px;
          opacity: 0.3;
        }
        .modal {
          display: none;
          position: fixed;
          top: 0;
          left: 0;
          width: 100%;
          height: 100%;
          background: rgba(0,0,0,0.5);
          z-index: 1000;
          align-items: center;
          justify-content: center;
        }
        .modal.show {
          display: flex;
        }
        .modal-content {
          background: var(--card-background-color);
          padding: 24px;
          border-radius: 8px;
          max-width: 600px;
          width: 90%;
          max-height: 80vh;
          overflow-y: auto;
        }
        .modal-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 16px;
        }
        .modal-header h2 {
          margin: 0;
          font-size: 20px;
        }
        .form-field {
          margin-bottom: 16px;
        }
        .form-field label {
          display: block;
          margin-bottom: 4px;
        }
        .form-field input, .form-field textarea {
          width: 100%;
          box-sizing: border-box;
        }
        textarea {
          min-height: 100px;
          font-family: monospace;
          resize: vertical;
        }
        .modal-actions {
          display: flex;
          gap: 8px;
          justify-content: flex-end;
          margin-top: 24px;
        }
        .attribute-preview {
          font-family: monospace;
          font-size: 12px;
          max-height: 60px;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .debug-status {
          margin-bottom: 16px;
          padding: 12px;
          background: var(--card-background-color);
          border: 1px solid var(--divider-color);
          border-radius: 4px;
          font-size: 12px;
          font-family: monospace;
        }
        .debug-status .status-item {
          display: flex;
          gap: 8px;
          margin-bottom: 4px;
        }
        .debug-status .status-label {
          font-weight: bold;
          min-width: 200px;
        }
        .debug-status .status-value {
          color: var(--secondary-text-color);
        }
        .debug-status .status-ok {
          color: var(--success-color, green);
        }
        .debug-status .status-error {
          color: var(--error-color, red);
        }
        .debug-status .status-warning {
          color: var(--warning-color, orange);
        }
        .scroll-sentinel {
          height: 1px;
        }
        .loading-indicator {
          text-align: center;
          padding: 48px;
          color: var(--secondary-text-color);
        }
        .loading-spinner {
          display: inline-block;
          width: 40px;
          height: 40px;
          border: 4px solid var(--divider-color);
          border-top-color: var(--primary-color);
          border-radius: 50%;
          animation: spin 0.8s linear infinite;
          margin-bottom: 12px;
        }
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
        .loading-more-indicator {
          text-align: center;
          padding: 12px 16px;
          color: var(--secondary-text-color);
          font-size: 13px;
        }
        .load-prev-cell {
          text-align: center;
          padding: 12px !important;
          color: var(--primary-color);
          cursor: pointer;
          font-size: 13px;
        }
        .load-prev-cell:hover {
          text-decoration: underline;
        }
        /* Bulk selection */
        .bulk-action-bar {
          display: flex;
          align-items: center;
          gap: 12px;
          padding: 10px 16px;
          margin-bottom: 12px;
          background: var(--primary-color);
          color: var(--text-primary-color, white);
          border-radius: 6px;
          font-size: 14px;
        }
        .bulk-action-bar button {
          background: rgba(255, 255, 255, 0.18);
          color: inherit;
          border: 1px solid rgba(255, 255, 255, 0.4);
        }
        .bulk-action-bar button:hover {
          background: rgba(255, 255, 255, 0.3);
        }
        .bulk-action-bar button.danger {
          background: var(--error-color, #db4437);
          border-color: var(--error-color, #db4437);
        }
        .bulk-selected-count {
          flex: 0 0 auto;
          font-weight: 500;
        }
        .checkbox-cell {
          width: 32px;
          padding: 8px 4px !important;
          text-align: center;
        }
        .row-select:disabled {
          cursor: not-allowed;
          opacity: 0.4;
        }
        .bulk-mode-banner {
          padding: 10px 12px;
          margin-bottom: 12px;
          background: var(--info-color, #4285f4);
          color: var(--text-primary-color, white);
          border-radius: 4px;
          font-size: 13px;
        }
        .form-help {
          font-size: 12px;
          color: var(--secondary-text-color);
          margin-top: -4px;
          margin-bottom: 8px;
        }
      </style>

      <div class="header">
        <h1>🗄️ ${this._t('title')}</h1>
      </div>

      ${this._debugMode ? `<div class="debug-status" id="debug-status">
        <div class="status-item">
          <span class="status-label">Panel Status:</span>
          <span class="status-value" id="status-panel">Initializing...</span>
        </div>
        <div class="status-item">
          <span class="status-label">Home Assistant Connection:</span>
          <span class="status-value" id="status-hass">Waiting...</span>
        </div>
        <div class="status-item">
          <span class="status-label">Entity Picker Element:</span>
          <span class="status-value" id="status-picker-element">Not created</span>
        </div>
        <div class="status-item">
          <span class="status-label">Entity Picker Ready:</span>
          <span class="status-value" id="status-picker-ready">Not ready</span>
        </div>
        <div class="status-item">
          <span class="status-label">Custom Element Defined:</span>
          <span class="status-value" id="status-custom-element">Checking...</span>
        </div>
        <div class="status-item" style="margin-top: 8px; padding-top: 8px; border-top: 1px solid var(--divider-color);">
          <span class="status-label" style="font-size: 11px; font-style: italic;">Debug Mode:</span>
          <span class="status-value status-ok" style="font-size: 11px;">Enabled (Add ?debug to URL or set localStorage.history_editor_debug='true')</span>
        </div>
      </div>` : ''}

      <div class="controls">
        <div class="control-group">
          <label for="data-source">${this._t('data_source')}</label>
          <select id="data-source">
            <option value="states">${this._t('states')}</option>
            <option value="statistics">${this._t('statistics_long')}</option>
            <option value="statistics_short_term">${this._t('statistics_short')}</option>
          </select>
        </div>
        <div class="control-group entity-form-container">
          <ha-form id="entity-form"></ha-form>
        </div>
        <div class="control-group">
          <label for="record-limit">${this._t('record_limit')}</label>
          <input type="number" id="record-limit" value="100" min="1" max="1000">
        </div>
        <div class="control-group">
          <label for="go-to-date">${this._t('go_to_date')}</label>
          <input type="datetime-local" id="go-to-date" title="${this._t('go_to_date_title')}">
        </div>
        <button id="go-to-btn" class="secondary">${this._t('go_to_btn')}</button>
        <button id="clear-date-btn" class="secondary">${this._t('clear_date')}</button>
        <button id="add-btn" class="secondary">${this._t('add_new')}</button>
        <button id="scroll-top-btn" class="secondary" title="${this._t('scroll_top_title')}">${this._t('scroll_top')}</button>
        <button id="scroll-bottom-btn" class="secondary" title="${this._t('scroll_bottom_title')}">${this._t('scroll_bottom')}</button>
      </div>

      <div id="bulk-action-bar" class="bulk-action-bar" style="display:none">
        <span id="bulk-selected-count" class="bulk-selected-count">${this._t('n_selected', {n: 0})}</span>
        <button id="bulk-edit-btn" type="button" class="secondary">${this._t('edit_selected')}</button>
        <button id="bulk-delete-btn" type="button" class="danger">${this._t('delete_selected')}</button>
        <button id="bulk-clear-btn" type="button" class="secondary">${this._t('clear_selection')}</button>
      </div>

      <div class="table-container">
        <div id="records-display"></div>
        <div id="loading-more-indicator" class="loading-more-indicator" style="display:none">${this._t('loading_more')}</div>
        <div id="scroll-sentinel" class="scroll-sentinel"></div>
      </div>

      <div id="edit-modal" class="modal">
        <div class="modal-content">
          <div class="modal-header">
            <h2 id="modal-title">${this._t('edit_record_title')}</h2>
            <button id="modal-close" class="secondary">✕</button>
          </div>
          <form id="edit-form">
            <div id="bulk-mode-banner" class="bulk-mode-banner" style="display:none">
              <span id="bulk-mode-banner-text"></span>
              <div class="form-help">${this._t('bulk_help')}</div>
            </div>
            <div id="single-id-field" class="form-field">
              <label id="id-field-label">${this._t('state_id_label')}</label>
              <input type="text" id="edit-state-id" readonly>
            </div>
            <div id="states-form-fields">
              <div class="form-field">
                <label>${this._t('entity_id_label')}</label>
                <input type="text" id="edit-entity-id">
              </div>
              <div class="form-field">
                <label>${this._t('state_label')}</label>
                <input type="text" id="edit-state">
              </div>
              <div class="form-field">
                <label>${this._t('attributes_label')}</label>
                <textarea id="edit-attributes"></textarea>
              </div>
              <div class="form-field">
                <label>${this._t('timestamp_label')}</label>
                <input type="datetime-local" id="edit-last-updated">
              </div>
            </div>
            <div id="stats-form-fields" style="display:none">
              <div class="form-field">
                <label>${this._t('start_time_label')}</label>
                <input type="datetime-local" id="edit-stat-start">
              </div>
              <div class="form-field">
                <label>${this._t('mean_label')}</label>
                <input type="number" step="any" id="edit-stat-mean" placeholder="null">
              </div>
              <div class="form-field">
                <label>${this._t('min_label')}</label>
                <input type="number" step="any" id="edit-stat-min" placeholder="null">
              </div>
              <div class="form-field">
                <label>${this._t('max_label')}</label>
                <input type="number" step="any" id="edit-stat-max" placeholder="null">
              </div>
              <div class="form-field">
                <label>${this._t('sum_label')}</label>
                <input type="number" step="any" id="edit-stat-sum" placeholder="null">
              </div>
              <div class="form-field">
                <label>${this._t('state_last_label')}</label>
                <input type="number" step="any" id="edit-stat-state-val" placeholder="null">
              </div>
            </div>
            <div class="modal-actions">
              <button type="button" id="modal-cancel" class="secondary">${this._t('cancel')}</button>
              <button type="submit" id="modal-save">${this._t('save')}</button>
            </div>
          </form>
        </div>
      </div>
    `;

    this._debugLog('[HistoryEditor] Panel UI rendered, setting up event listeners');
    this.setupEventListeners();
    // Update debug status
    this._updateDebugStatus('panel', 'Rendered ✓', 'status-ok');
    // Trigger entity picker initialization after rendering
    this._triggerEntityPickerLoad();
  }
  
  _triggerEntityPickerLoad() {
    this._debugLog('[HistoryEditor] _triggerEntityPickerLoad called');
    // Set up ha-form with entity selector
    if (this._hass) {
      this._debugLog('[HistoryEditor] Setting up ha-form with entity selector');
      this._setupEntityForm();
    } else {
      console.warn('[HistoryEditor] hass not available yet in _triggerEntityPickerLoad');
    }
  }

  setupEventListeners() {
    this._debugLog('[HistoryEditor] setupEventListeners called');
    const goToBtn = this.querySelector('#go-to-btn');
    const clearDateBtn = this.querySelector('#clear-date-btn');
    const addBtn = this.querySelector('#add-btn');
    const modalClose = this.querySelector('#modal-close');
    const modalCancel = this.querySelector('#modal-cancel');
    const editForm = this.querySelector('#edit-form');
    const tableContainer = this.querySelector('.table-container');

    this._debugLog('[HistoryEditor] Setting up event listeners on buttons');
    goToBtn.addEventListener('click', () => this.goToDate_onClick());
    clearDateBtn.addEventListener('click', () => this.clearDate_onClick());
    addBtn.addEventListener('click', () => this.showAddModal());
    modalClose.addEventListener('click', () => this.hideModal());
    modalCancel.addEventListener('click', () => this.hideModal());
    editForm.addEventListener('submit', (e) => {
      e.preventDefault();
      this.saveRecord();
    });

    const scrollTopBtn = this.querySelector('#scroll-top-btn');
    const scrollBottomBtn = this.querySelector('#scroll-bottom-btn');
    scrollTopBtn.addEventListener('click', () => this._scrollToTop());
    scrollBottomBtn.addEventListener('click', () => this._scrollToBottom());

    // Handle data source changes
    const dataSourceSelect = this.querySelector('#data-source');
    dataSourceSelect.value = this.dataSource;
    dataSourceSelect.addEventListener('change', (e) => {
      this.dataSource = e.target.value;
      // Hide "Add New Record" button for statistics (stats are computed, not manually created)
      addBtn.style.display = this.dataSource === 'states' ? '' : 'none';
      // Selection is per data source — IDs from short-term and long-term tables
      // collide in numeric space, so clear when switching.
      this._clearSelection();
      if (this.selectedEntity) {
        this.loadRecords();
      }
    });
    
    // Set up event delegation for Edit and Delete buttons in the records table
    tableContainer.addEventListener('click', (e) => {
      const target = e.target;

      // Handle Edit button clicks
      if (target.classList.contains('edit-btn') && !target.disabled) {
        const stateId = parseInt(target.dataset.stateId);
        this.editRecord(stateId);
      }

      // Handle Delete button clicks
      if (target.classList.contains('delete-btn') && !target.disabled) {
        const stateId = parseInt(target.dataset.stateId);
        this.deleteRecord(stateId);
      }

      // Handle Load Previous click
      if (target.classList.contains('load-prev-cell')) {
        this._loadPrevRecords();
      }
    });

    // Selection: per-row checkbox + select-all in header
    tableContainer.addEventListener('change', (e) => {
      const target = e.target;
      if (target.classList && target.classList.contains('row-select')) {
        const id = parseInt(target.dataset.rowId);
        if (target.checked) {
          this._selectedIds.add(id);
        } else {
          this._selectedIds.delete(id);
        }
        this._refreshSelectAllState();
        this._updateBulkActionBar();
      } else if (target.id === 'select-all-rows') {
        const checkboxes = this.querySelectorAll('.row-select:not(:disabled)');
        checkboxes.forEach((cb) => {
          const id = parseInt(cb.dataset.rowId);
          cb.checked = target.checked;
          if (target.checked) {
            this._selectedIds.add(id);
          } else {
            this._selectedIds.delete(id);
          }
        });
        this._updateBulkActionBar();
      }
    });

    // Bulk-action toolbar buttons
    const bulkEditBtn = this.querySelector('#bulk-edit-btn');
    const bulkDeleteBtn = this.querySelector('#bulk-delete-btn');
    const bulkClearBtn = this.querySelector('#bulk-clear-btn');
    if (bulkEditBtn) bulkEditBtn.addEventListener('click', () => this._openBulkEditModal());
    if (bulkDeleteBtn) bulkDeleteBtn.addEventListener('click', () => this._handleBulkDelete());
    if (bulkClearBtn) bulkClearBtn.addEventListener('click', () => this._clearSelection());
    
    // Set up ha-form if hass is available
    if (this._hass) {
      this._debugLog('[HistoryEditor] hass available in setupEventListeners, setting up form');
      this._setupEntityForm();
    } else {
      this._debugLog('[HistoryEditor] In setupEventListeners - hass not available yet');
    }
  }

  goToDate_onClick() {
    const dateInput = this.querySelector('#go-to-date');
    const dateValue = dateInput.value;
    
    if (!dateValue) {
      this.showMessage(this._t('select_date_first'));
      return;
    }
    
    // Store the selected date
    this.goToDate = dateValue;
    
    // Reload records with the new date filter
    this.loadRecords();
  }

  clearDate_onClick() {
    const dateInput = this.querySelector('#go-to-date');
    dateInput.value = '';
    this.goToDate = null;
    
    // Reload records without date filter
    if (this.selectedEntity) {
      this.loadRecords();
    }
  }

  async loadRecords() {
    const limitInput = this.querySelector('#record-limit');
    const entityId = this.selectedEntity;

    if (!entityId) {
      this._debugLog('[HistoryEditor] No entity selected, skipping load');
      return;
    }

    const limit = parseInt(limitInput.value) || 100;

    // Branch based on data source
    if (this.dataSource !== 'states') {
      return this.loadStatistics(entityId, limit);
    }

    this.showLoading();

    try {
      // Build query parameters
      const params = new URLSearchParams({
        entity_id: entityId,
        limit: limit.toString()
      });
      
      // Add date filter if a date is selected
      if (this.goToDate) {
        // Convert datetime-local input to UTC ISO format
        // datetime-local returns 'YYYY-MM-DDTHH:MM' in local timezone
        // We need to preserve the local time intent when converting to UTC
        const localDateTimeStr = this.goToDate;
        
        // Parse as local time (browser's timezone)
        const localDate = new Date(localDateTimeStr);
        
        // Convert to ISO string (UTC) for the API
        params.append('end_time', localDate.toISOString());
      }

      // Use REST API instead of service call to avoid the service call API loop
      // This breaks us out of the problematic callService() return_response parameter issues
      const url = `/api/history_editor/records?${params.toString()}`;
      this._debugLog('[HistoryEditor] Calling API:', url);
      
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${this._hass.auth.data.access_token}`
        }
      });
      
      this._debugLog('[HistoryEditor] API response status:', response.status, response.statusText);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }

      const result = await response.json();
      
      this._debugLog('[HistoryEditor] API response:', result);

      // Check if the API call was successful
      if (result && result.success) {
        const records = result.records || [];
        this._hasMore = result.has_more || false;
        this._pageSize = limit;
        this._loadingMore = false;
        this._loadingPrev = false;
        this._domFirst = 0;
        this._domLast = 0;
        // Store cursor for page 0 so it can be reloaded if pruned from DOM
        // Always add +1ms so that re-fetching with end_time=cursor returns the same records
        const p0Cursor = this.goToDate
          ? new Date(new Date(this.goToDate).getTime() + 1).toISOString()
          : (records.length > 0
            ? new Date(new Date(records[0].last_updated).getTime() + 1).toISOString()
            : new Date().toISOString());
        this._pageCursors = [p0Cursor];
        // Pre-compute cursor for next page (page 1)
        if (records.length > 0) {
          const oldest = records[records.length - 1];
          this._pageCursors.push(new Date(new Date(oldest.last_updated).getTime() - 1).toISOString());
        }
        this.records = records;
        this._debugLog('[HistoryEditor] Loaded', records.length, 'records, has_more:', this._hasMore);
        this.displayRecords(records);
      } else {
        const errorMsg = result?.error || 'Unknown error occurred';
        this._debugLog('[HistoryEditor] API returned error:', errorMsg);
        alert(this._t('error_loading', {err: errorMsg}));
        this.showMessage(this._t('failed_load', {err: errorMsg}));
      }
      
    } catch (error) {
      console.error('Error loading records:', error);
      alert(this._t('error_loading', {err: error.message}));
      this.showMessage(this._t('error_console'));
    }
  }

  showLoading() {
    const display = this.querySelector('#records-display');
    display.innerHTML = `
      <div class="loading-indicator">
        <div class="loading-spinner"></div>
        <p>Loading records…</p>
      </div>
    `;
  }

  showMessage(message) {
    const display = this.querySelector('#records-display');
    display.innerHTML = `
      <div class="empty-state">
        <p>${message}</p>
        <p style="font-size: 12px; margin-top: 8px;">
          Note: Due to Home Assistant service limitations, use the Developer Tools -> Services 
          to call history_editor.get_records and view results.
        </p>
      </div>
    `;
  }

  displayRecords(records) {
    const display = this.querySelector('#records-display');
    
    if (!records || records.length === 0) {
      display.innerHTML = `
        <div class="empty-state">
          <div style="font-size: 48px; opacity: 0.3;">📋</div>
          <p>${this._t('no_records')}</p>
        </div>
      `;
      return;
    }

    let html = `
      <table>
        <thead>
          <tr>
            <th class="checkbox-cell"><input type="checkbox" id="select-all-rows" title="${this._t('select_all')}"></th>
            <th>${this._t('th_id')}</th>
            <th>${this._t('th_state')}</th>
            <th>${this._t('th_attributes')}</th>
            <th>${this._t('th_timestamp')}</th>
            <th>${this._t('th_actions')}</th>
          </tr>
        </thead>
        <tbody id="records-tbody">
          <tr id="load-prev-row" style="display:none">
            <td colspan="6" class="load-prev-cell">${this._t('load_prev')}</td>
          </tr>
    `;

    records.forEach(record => {
      html += `<tr data-page="0">${this._buildStateRowCells(record)}</tr>`;
    });

    html += '</tbody></table>';
    display.innerHTML = html;
    this._setupScrollObserver();
    this._refreshSelectAllState();
    this._updateBulkActionBar();
  }

  escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  showAddModal() {
    const modal = this.querySelector('#edit-modal');
    const title = this.querySelector('#modal-title');
    const form = this.querySelector('#edit-form');

    title.textContent = this._t('add_new_title');
    form.reset();
    
    this.querySelector('#edit-state-id').value = 'NEW';
    this.querySelector('#edit-entity-id').value = this.selectedEntity || '';
    this.querySelector('#edit-entity-id').readOnly = false;

    // Ensure states fields are shown for add mode
    this.querySelector('#id-field-label').textContent = this._t('state_id_label');
    this.querySelector('#states-form-fields').style.display = 'block';
    this.querySelector('#stats-form-fields').style.display = 'none';
    
    modal.classList.add('show');
  }

  editRecord(stateId) {
    const record = this.records.find(r => (r.state_id ?? r.id) === stateId);
    if (!record) return;

    const modal = this.querySelector('#edit-modal');
    const title = this.querySelector('#modal-title');

    if (this.dataSource !== 'states') {
      // Statistics mode
      title.textContent = this._t('edit_statistic_title');
      this.querySelector('#id-field-label').textContent = this._t('statistic_id_label');
      this.querySelector('#edit-state-id').value = record.id;

      this.querySelector('#states-form-fields').style.display = 'none';
      this.querySelector('#stats-form-fields').style.display = 'block';

      if (record.start) {
        this.querySelector('#edit-stat-start').value = this.formatDatetimeLocal(record.start);
      }
      this._setStatField('#edit-stat-mean', record.mean);
      this._setStatField('#edit-stat-min', record.min);
      this._setStatField('#edit-stat-max', record.max);
      this._setStatField('#edit-stat-sum', record.sum);
      this._setStatField('#edit-stat-state-val', record.state);

      modal.classList.add('show');
      return;
    }

    // States mode
    title.textContent = this._t('edit_record_title');
    this.querySelector('#id-field-label').textContent = this._t('state_id_label');
    this.querySelector('#states-form-fields').style.display = 'block';
    this.querySelector('#stats-form-fields').style.display = 'none';

    this.querySelector('#edit-state-id').value = record.state_id;
    this.querySelector('#edit-entity-id').value = record.entity_id;
    this.querySelector('#edit-entity-id').readOnly = true;
    this.querySelector('#edit-state').value = record.state;
    this.querySelector('#edit-attributes').value = JSON.stringify(record.attributes || {}, null, 2);
    
    if (record.last_updated) {
      this.querySelector('#edit-last-updated').value = this.formatDatetimeLocal(record.last_updated);
    }

    modal.classList.add('show');
  }

  _setStatField(selector, value) {
    const field = this.querySelector(selector);
    if (field) {
      field.value = value !== null && value !== undefined ? value : '';
    }
  }

  formatDatetimeLocal(isoString) {
    const date = new Date(isoString);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    return `${year}-${month}-${day}T${hours}:${minutes}`;
  }

  formatDatetimeDisplay(isoString) {
    if (!isoString) return this._t('na');
    try {
      const date = new Date(isoString);
      // Validate that the date is valid
      if (isNaN(date.getTime())) {
        return this._t('na');
      }
      // Format: YYYY-MM-DD HH:MM:SS
      const year = date.getFullYear();
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const day = String(date.getDate()).padStart(2, '0');
      const hours = String(date.getHours()).padStart(2, '0');
      const minutes = String(date.getMinutes()).padStart(2, '0');
      const seconds = String(date.getSeconds()).padStart(2, '0');
      return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    } catch (error) {
      return this._t('na');
    }
  }

  hideModal() {
    const modal = this.querySelector('#edit-modal');
    modal.classList.remove('show');
    // Reset bulk-mode UI so the next single-row open shows everything again
    if (this._editMode === 'bulk') {
      const banner = this.querySelector('#bulk-mode-banner');
      const idField = this.querySelector('#single-id-field');
      if (banner) banner.style.display = 'none';
      if (idField) idField.style.display = '';
      const entityField = this.querySelector('#edit-entity-id').closest('.form-field');
      if (entityField) entityField.style.display = '';
      const tsField = this.querySelector('#edit-last-updated').closest('.form-field');
      if (tsField) tsField.style.display = '';
      const startField = this.querySelector('#edit-stat-start').closest('.form-field');
      if (startField) startField.style.display = '';
    }
    this._editMode = 'single';
  }

  async saveRecord() {
    // Bulk mode dispatches to a different endpoint — never touches the single-row
    // helpers below.
    if (this._editMode === 'bulk') {
      await this._handleBulkSave();
      return;
    }

    const stateId = this.querySelector('#edit-state-id').value;

    // Handle statistics mode
    if (this.dataSource !== 'states') {
      await this._saveStatistic(parseInt(stateId));
      return;
    }

    const entityId = this.querySelector('#edit-entity-id').value;
    const state = this.querySelector('#edit-state').value;
    const attributesText = this.querySelector('#edit-attributes').value;
    const lastUpdated = this.querySelector('#edit-last-updated').value;

    let attributes = {};
    try {
      if (attributesText.trim()) {
        attributes = JSON.parse(attributesText);
      }
    } catch (error) {
      alert(this._t('invalid_json'));
      return;
    }

    try {
      if (stateId === 'NEW') {
        // Create new record
        const data = {
          entity_id: entityId,
          state: state,
          attributes: attributes
        };
        if (lastUpdated) {
          const localDate = new Date(lastUpdated);
          data.last_updated = localDate.toISOString();
          // Set last_changed to the same value as last_updated for consistency
          data.last_changed = localDate.toISOString();
        }

        const response = await fetch('/api/history_editor/create', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${this._hass.auth.data.access_token}`
          },
          body: JSON.stringify(data)
        });

        const result = await response.json();
        
        if (result.success) {
          alert(this._t('record_created'));
          this.hideModal();
          this.loadRecords();
        } else {
          alert(this._t('error_creating', {err: result.error || 'Unknown error'}));
        }
      } else {
        // Update existing record
        const data = {
          state_id: parseInt(stateId),
          state: state,
          attributes: attributes
        };
        if (lastUpdated) {
          const localDate = new Date(lastUpdated);
          data.last_updated = localDate.toISOString();
          // Set last_changed to the same value as last_updated for consistency
          data.last_changed = localDate.toISOString();
        }

        const response = await fetch('/api/history_editor/update', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${this._hass.auth.data.access_token}`
          },
          body: JSON.stringify(data)
        });

        const result = await response.json();
        
        if (result.success) {
          alert(this._t('record_updated'));
          this.hideModal();
          this.loadRecords();
        } else {
          alert(this._t('error_updating', {err: result.error || 'Unknown error'}));
        }
      }
    } catch (error) {
      console.error('Error saving record:', error);
      alert(this._t('error_saving', {err: error.message}));
    }
  }

  async deleteRecord(stateId) {
    if (!confirm(this._t(this.dataSource === 'states' ? 'confirm_delete' : 'confirm_delete_stat'))) {
      return;
    }

    // Handle statistics mode
    if (this.dataSource !== 'states') {
      await this._deleteStatistic(stateId);
      return;
    }

    try {
      const response = await fetch('/api/history_editor/delete', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this._hass.auth.data.access_token}`
        },
        body: JSON.stringify({
          state_id: parseInt(stateId)
        })
      });

      const result = await response.json();
      
      if (result.success) {
        alert(this._t('record_deleted'));
        this.loadRecords();
      } else {
        alert(this._t('error_deleting', {err: result.error || 'Unknown error'}));
      }
    } catch (error) {
      console.error('Error deleting record:', error);
      alert('Error deleting record: ' + error.message);
    }
  }

  async loadStatistics(entityId, limit) {
    this.showLoading();
    try {
      const statisticType = this.dataSource === 'statistics_short_term' ? 'short_term' : 'long_term';
      const params = new URLSearchParams({
        entity_id: entityId,
        limit: limit.toString(),
        statistic_type: statisticType,
      });

      if (this.goToDate) {
        const localDate = new Date(this.goToDate);
        params.append('end_time', localDate.toISOString());
      }

      const url = `/api/history_editor/statistics?${params.toString()}`;
      this._debugLog('[HistoryEditor] Calling statistics API:', url);

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${this._hass.auth.data.access_token}`
        }
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }

      const result = await response.json();
      this._debugLog('[HistoryEditor] Statistics API response:', result);

      if (result && result.success) {
        const records = result.records || [];
        this._hasMore = result.has_more || false;
        this._pageSize = limit;
        this._loadingMore = false;
        this._loadingPrev = false;
        this._domFirst = 0;
        this._domLast = 0;
        // Store cursor for page 0 so it can be reloaded if pruned from DOM
        // Always add +1ms so that re-fetching with end_time=cursor returns the same records
        const p0Cursor = this.goToDate
          ? new Date(new Date(this.goToDate).getTime() + 1).toISOString()
          : (records.length > 0
            ? new Date(new Date(records[0].start).getTime() + 1).toISOString()
            : new Date().toISOString());
        this._pageCursors = [p0Cursor];
        // Pre-compute cursor for next page (page 1)
        if (records.length > 0) {
          const oldest = records[records.length - 1];
          this._pageCursors.push(new Date(new Date(oldest.start).getTime() - 1).toISOString());
        }
        this.records = records;
        this.displayStatistics(records);
      } else {
        const errorMsg = result?.error || 'Unknown error occurred';
        alert(this._t('error_loading_stats', {err: errorMsg}));
        this.showMessage(this._t('failed_load_stats', {err: errorMsg}));
      }
    } catch (error) {
      console.error('Error loading statistics:', error);
      alert(this._t('error_loading_stats', {err: error.message}));
      this.showMessage(this._t('error_console_stats'));
    }
  }

  displayStatistics(records) {
    const display = this.querySelector('#records-display');

    if (!records || records.length === 0) {
      display.innerHTML = `
        <div class="empty-state">
          <div style="font-size: 48px; opacity: 0.3;">📊</div>
          <p>${this._t('no_statistics')}</p>
        </div>
      `;
      return;
    }

    let html = `
      <table>
        <thead>
          <tr>
            <th class="checkbox-cell"><input type="checkbox" id="select-all-rows" title="${this._t('select_all_unlocked')}"></th>
            <th>${this._t('th_id')}</th>
            <th>${this._t('th_start_time')}</th>
            <th>${this._t('th_mean')}</th>
            <th>${this._t('th_min')}</th>
            <th>${this._t('th_max')}</th>
            <th>${this._t('th_sum')}</th>
            <th>${this._t('th_state')}</th>
            <th>${this._t('th_actions')}</th>
          </tr>
        </thead>
        <tbody id="records-tbody">
          <tr id="load-prev-row" style="display:none">
            <td colspan="9" class="load-prev-cell">${this._t('load_prev')}</td>
          </tr>
    `;

    records.forEach(record => {
      html += `<tr data-page="0">${this._buildStatRowCells(record)}</tr>`;
    });

    html += '</tbody></table>';
    display.innerHTML = html;
    this._setupScrollObserver();
    this._refreshSelectAllState();
    this._updateBulkActionBar();
  }

  async _saveStatistic(statId) {
    const statStart = this.querySelector('#edit-stat-start').value;
    const meanVal = this.querySelector('#edit-stat-mean').value;
    const minVal = this.querySelector('#edit-stat-min').value;
    const maxVal = this.querySelector('#edit-stat-max').value;
    const sumVal = this.querySelector('#edit-stat-sum').value;
    const stateVal = this.querySelector('#edit-stat-state-val').value;

    const data = {
      id: statId,
      statistic_type: this.dataSource === 'statistics_short_term' ? 'short_term' : 'long_term',
    };

    if (meanVal !== '') { const n = parseFloat(meanVal); if (!isNaN(n)) data.mean = n; }
    if (minVal !== '') { const n = parseFloat(minVal); if (!isNaN(n)) data.min = n; }
    if (maxVal !== '') { const n = parseFloat(maxVal); if (!isNaN(n)) data.max = n; }
    if (sumVal !== '') { const n = parseFloat(sumVal); if (!isNaN(n)) data.sum = n; }
    if (stateVal !== '') { const n = parseFloat(stateVal); if (!isNaN(n)) data.state = n; }
    if (statStart) {
      data.start = new Date(statStart).toISOString();
    }

    try {
      const response = await fetch('/api/history_editor/statistics/update', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this._hass.auth.data.access_token}`
        },
        body: JSON.stringify(data)
      });

      const result = await response.json();

      if (result.success) {
        alert(this._t('stat_updated'));
        this.hideModal();
        this.loadRecords();
      } else {
        alert(this._t('error_updating_stat', {err: result.error || 'Unknown error'}));
      }
    } catch (error) {
      console.error('Error saving statistic:', error);
      alert(this._t('error_saving_stat', {err: error.message}));
    }
  }

  async _deleteStatistic(statId) {
    try {
      const response = await fetch('/api/history_editor/statistics/delete', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this._hass.auth.data.access_token}`
        },
        body: JSON.stringify({
          id: parseInt(statId),
          statistic_type: this.dataSource === 'statistics_short_term' ? 'short_term' : 'long_term',
        })
      });

      const result = await response.json();

      if (result.success) {
        alert(this._t('stat_deleted'));
        this.loadRecords();
      } else {
        alert(this._t('error_deleting_stat', {err: result.error || 'Unknown error'}));
      }
    } catch (error) {
      console.error('Error deleting statistic:', error);
      alert('Error deleting statistic: ' + error.message);
    }
  }

  // ─── Bulk selection / bulk edit / bulk delete ─────────────────────────────

  _clearSelection() {
    this._selectedIds.clear();
    this.querySelectorAll('.row-select').forEach((cb) => { cb.checked = false; });
    this._refreshSelectAllState();
    this._updateBulkActionBar();
  }

  _refreshSelectAllState() {
    const selectAll = this.querySelector('#select-all-rows');
    if (!selectAll) return;
    const visible = this.querySelectorAll('.row-select:not(:disabled)');
    if (visible.length === 0) {
      selectAll.checked = false;
      selectAll.indeterminate = false;
      return;
    }
    let checked = 0;
    visible.forEach((cb) => { if (cb.checked) checked++; });
    selectAll.checked = (checked === visible.length);
    selectAll.indeterminate = (checked > 0 && checked < visible.length);
  }

  _updateBulkActionBar() {
    const bar = this.querySelector('#bulk-action-bar');
    if (!bar) return;
    const count = this._selectedIds.size;
    if (count === 0) {
      bar.style.display = 'none';
    } else {
      bar.style.display = 'flex';
      const label = this.querySelector('#bulk-selected-count');
      if (label) label.textContent = this._t('n_selected', {n: count});
    }
  }

  _openBulkEditModal() {
    if (this._selectedIds.size === 0) return;
    this._editMode = 'bulk';

    const modal = this.querySelector('#edit-modal');
    const title = this.querySelector('#modal-title');
    const banner = this.querySelector('#bulk-mode-banner');
    const bannerText = this.querySelector('#bulk-mode-banner-text');
    const idField = this.querySelector('#single-id-field');
    const form = this.querySelector('#edit-form');

    form.reset();
    idField.style.display = 'none';
    banner.style.display = 'block';
    const count = this._selectedIds.size;
    bannerText.textContent = this._t('bulk_editing', {n: count});

    if (this.dataSource === 'states') {
      title.textContent = this._t('bulk_edit_states', {n: count});
      this.querySelector('#states-form-fields').style.display = 'block';
      this.querySelector('#stats-form-fields').style.display = 'none';
      // Hide entity_id and timestamp fields in bulk mode — those don't make
      // sense to apply uniformly across many records.
      const entityField = this.querySelector('#edit-entity-id').closest('.form-field');
      if (entityField) entityField.style.display = 'none';
      const tsField = this.querySelector('#edit-last-updated').closest('.form-field');
      if (tsField) tsField.style.display = 'none';
    } else {
      const isShort = this.dataSource === 'statistics_short_term';
      title.textContent = this._t(isShort ? 'bulk_edit_short' : 'bulk_edit_long', {n: count});
      this.querySelector('#states-form-fields').style.display = 'none';
      this.querySelector('#stats-form-fields').style.display = 'block';
      // Hide start_time in bulk mode — different per row.
      const startField = this.querySelector('#edit-stat-start').closest('.form-field');
      if (startField) startField.style.display = 'none';
    }

    modal.classList.add('show');
  }

  async _handleBulkDelete() {
    const count = this._selectedIds.size;
    if (count === 0) return;
    const proceed = confirm(
      this.dataSource === 'states'
        ? this._t('bulk_delete_confirm_states', {n: count})
        : this._t('bulk_delete_confirm_stats', {n: count})
    );
    if (!proceed) return;

    try {
      const ids = Array.from(this._selectedIds);
      const url = this.dataSource === 'states'
        ? '/api/history_editor/bulk_delete'
        : '/api/history_editor/statistics/bulk_delete';
      const body = this.dataSource === 'states'
        ? { state_ids: ids }
        : { ids, statistic_type: this.dataSource === 'statistics_short_term' ? 'short_term' : 'long_term' };

      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this._hass.auth.data.access_token}`,
        },
        body: JSON.stringify(body),
      });
      const result = await response.json();

      if (!result.success) {
        alert(this._t('bulk_delete_error', {err: result.error || 'Bulk delete failed'}));
        return;
      }
      alert(this._formatBulkResult(result, 'deleted'));
      this._clearSelection();
      this.loadRecords();
    } catch (err) {
      console.error('Bulk delete failed:', err);
      alert(this._t('bulk_delete_failed', {err: err.message}));
    }
  }

  async _handleBulkSave() {
    const ids = Array.from(this._selectedIds);
    if (ids.length === 0) return;

    let url;
    let body;
    if (this.dataSource === 'states') {
      const newState = this.querySelector('#edit-state').value;
      const attrText = this.querySelector('#edit-attributes').value;
      body = { state_ids: ids };
      if (newState !== '') body.state = newState;
      if (attrText !== '') {
        try {
          body.attributes = JSON.parse(attrText);
        } catch (err) {
          alert(this._t('invalid_json_detail', {err: err.message}));
          return;
        }
      }
      if (!('state' in body) && !('attributes' in body)) {
        alert(this._t('bulk_provide_state_or_attrs'));
        return;
      }
      url = '/api/history_editor/bulk_update';
    } else {
      const isShort = this.dataSource === 'statistics_short_term';
      const meanV = this.querySelector('#edit-stat-mean').value;
      const minV = this.querySelector('#edit-stat-min').value;
      const maxV = this.querySelector('#edit-stat-max').value;
      const sumV = this.querySelector('#edit-stat-sum').value;
      const stateV = this.querySelector('#edit-stat-state-val').value;

      body = {
        ids,
        statistic_type: isShort ? 'short_term' : 'long_term',
      };
      const setIfNumber = (key, raw) => {
        if (raw === '') return;
        const n = parseFloat(raw);
        if (!isNaN(n)) body[key] = n;
      };
      setIfNumber('mean', meanV);
      setIfNumber('min', minV);
      setIfNumber('max', maxV);
      setIfNumber('sum', sumV);
      setIfNumber('state', stateV);
      if (Object.keys(body).length <= 2) {
        alert(this._t('bulk_provide_numeric'));
        return;
      }
      url = '/api/history_editor/statistics/bulk_update';
    }

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this._hass.auth.data.access_token}`,
        },
        body: JSON.stringify(body),
      });
      const result = await response.json();

      if (!result.success) {
        alert(this._t('bulk_update_error', {err: result.error || 'Bulk update failed'}));
        return;
      }
      alert(this._formatBulkResult(result, 'updated'));
      this.hideModal();
      this._clearSelection();
      this.loadRecords();
    } catch (err) {
      console.error('Bulk update failed:', err);
      alert(this._t('bulk_update_failed', {err: err.message}));
    }
  }

  _formatBulkResult(result, verb) {
    const count = (verb === 'deleted') ? result.deleted_count : result.updated_count;
    const key = (verb === 'deleted') ? 'n_records_deleted' : 'n_records_updated';
    const lines = [this._t(key, {n: count})];
    if (result.blocked && result.blocked.length) {
      lines.push(this._t('blocked_header', {n: result.blocked.length}));
      const sample = result.blocked.slice(0, 5);
      sample.forEach(({ id, reason }) => lines.push(this._t('blocked_item', {id, reason})));
      if (result.blocked.length > 5) {
        lines.push(this._t('blocked_more', {n: result.blocked.length - 5}));
      }
    }
    if (result.not_found && result.not_found.length) {
      lines.push(this._t('not_found', {n: result.not_found.length}));
    }
    if (result.statistics_stale) {
      lines.push('');
      lines.push(this._t('stats_stale'));
    }
    lines.push('');
    lines.push(this._t('see_change'));
    return lines.join('\n');
  }

  // ─── Infinite scroll helpers ──────────────────────────────────────────────

  _getRecordTimestamp(record) {
    // Returns the ISO timestamp string for the record (works for both states and statistics)
    return this.dataSource !== 'states' ? record.start : record.last_updated;
  }

  _setupScrollObserver() {
    if (this._scrollObserver) {
      this._scrollObserver.disconnect();
      this._scrollObserver = null;
    }
    const indicator = this.querySelector('#loading-more-indicator');
    if (!this._hasMore) {
      if (indicator) indicator.style.display = 'none';
      return;
    }
    const sentinel = this.querySelector('#scroll-sentinel');
    if (!sentinel) return;
    const tableContainer = this.querySelector('.table-container');
    this._scrollObserver = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (entry.isIntersecting && this._hasMore && !this._loadingMore && !this._isAutoLoading) {
          this._loadMoreRecords();
        }
      });
    }, { root: tableContainer, rootMargin: '200px 0px' });
    this._scrollObserver.observe(sentinel);
  }

  _setupTopObserver() {
    if (this._topObserver) {
      this._topObserver.disconnect();
      this._topObserver = null;
    }
    const loadPrevRow = this.querySelector('#load-prev-row');
    if (!loadPrevRow || loadPrevRow.style.display === 'none') return;
    const tableContainer = this.querySelector('.table-container');
    this._topObserver = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (entry.isIntersecting && this._domFirst > 0 && !this._loadingPrev && !this._isAutoLoading) {
          this._loadPrevRecords();
        }
      });
    }, { root: tableContainer, rootMargin: '100px 0px' });
    this._topObserver.observe(loadPrevRow);
  }

  _buildStateRowCells(record) {
    const attributes = JSON.stringify(record.attributes || {});
    const attributesPreview = attributes.length > 50
      ? attributes.substring(0, 50) + '...'
      : attributes;
    const checked = this._selectedIds.has(record.state_id) ? 'checked' : '';
    return `
      <td class="checkbox-cell"><input type="checkbox" class="row-select" data-row-id="${record.state_id}" ${checked}></td>
      <td data-label="ID">${record.state_id}</td>
      <td data-label="State">${this.escapeHtml(record.state)}</td>
      <td class="attribute-preview" data-label="Attributes" title="${this.escapeHtml(attributes)}">${this.escapeHtml(attributesPreview)}</td>
      <td data-label="Timestamp">${this.formatDatetimeDisplay(record.last_updated)}</td>
      <td class="actions">
        <button class="secondary edit-btn" data-state-id="${record.state_id}">${this._t('edit')}</button>
        <button class="danger delete-btn" data-state-id="${record.state_id}">${this._t('delete')}</button>
      </td>`;
  }

  _buildStatRowCells(record) {
    const na = this._t('na');
    const fmtNum = (v) => (v !== null && v !== undefined) ? Number(v).toFixed(3) : na;
    const locked = record.has_source_data === true;
    const lockTitle = locked
      ? (this.dataSource === 'statistics_short_term'
        ? this._t('locked_state_history')
        : this._t('locked_short_term'))
      : '';
    const lockIcon = locked ? ' 🔒' : '';
    const disabledAttr = locked ? 'disabled' : '';
    const titleAttr = locked ? `title="${lockTitle}"` : '';
    const checked = this._selectedIds.has(record.id) ? 'checked' : '';
    return `
      <td class="checkbox-cell"><input type="checkbox" class="row-select" data-row-id="${record.id}" ${disabledAttr} ${titleAttr} ${checked}></td>
      <td data-label="ID">${record.id}${lockIcon}</td>
      <td data-label="Start Time">${this.formatDatetimeDisplay(record.start)}</td>
      <td data-label="Mean">${fmtNum(record.mean)}</td>
      <td data-label="Min">${fmtNum(record.min)}</td>
      <td data-label="Max">${fmtNum(record.max)}</td>
      <td data-label="Sum">${fmtNum(record.sum)}</td>
      <td data-label="State">${fmtNum(record.state)}</td>
      <td class="actions">
        <button class="secondary edit-btn" data-state-id="${record.id}" ${disabledAttr} ${titleAttr}>${this._t('edit')}</button>
        <button class="danger delete-btn" data-state-id="${record.id}" ${disabledAttr} ${titleAttr}>${this._t('delete')}</button>
      </td>`;
  }

  _appendToTable(records, pageIdx) {
    const tbody = this.querySelector('#records-tbody');
    if (!tbody) return;
    const buildCells = this.dataSource !== 'states'
      ? (r) => this._buildStatRowCells(r)
      : (r) => this._buildStateRowCells(r);
    records.forEach((record) => {
      const tr = document.createElement('tr');
      tr.dataset.page = pageIdx;
      tr.innerHTML = buildCells(record);
      tbody.appendChild(tr);
    });
    this._refreshSelectAllState();
    this._updateBulkActionBar();
  }

  _prependToTable(records, pageIdx) {
    const tbody = this.querySelector('#records-tbody');
    if (!tbody) return;
    // Insert after the load-prev-row (or at the top if it doesn't exist)
    const loadPrevRow = tbody.querySelector('#load-prev-row');
    const insertBefore = loadPrevRow ? loadPrevRow.nextSibling : tbody.firstChild;
    const fragment = document.createDocumentFragment();
    const buildCells = this.dataSource !== 'states'
      ? (r) => this._buildStatRowCells(r)
      : (r) => this._buildStateRowCells(r);
    records.forEach((record) => {
      const tr = document.createElement('tr');
      tr.dataset.page = pageIdx;
      tr.innerHTML = buildCells(record);
      fragment.appendChild(tr);
    });
    tbody.insertBefore(fragment, insertBefore);
    this._refreshSelectAllState();
    this._updateBulkActionBar();
  }

  _pruneTopPage() {
    const tbody = this.querySelector('#records-tbody');
    if (!tbody) return;
    const pageToRemove = this._domFirst;
    const rows = Array.from(tbody.querySelectorAll(`[data-page="${pageToRemove}"]`));
    // Remove from records array (front)
    this.records = this.records.slice(rows.length);
    rows.forEach(row => row.remove());
    this._domFirst++;
    // Show the load-prev row and set up top observer
    const loadPrevRow = tbody.querySelector('#load-prev-row');
    if (loadPrevRow) {
      loadPrevRow.style.display = '';
      this._setupTopObserver();
    }
  }

  _pruneBottomPage() {
    const tbody = this.querySelector('#records-tbody');
    if (!tbody) return;
    const pageToRemove = this._domLast;
    const rows = Array.from(tbody.querySelectorAll(`[data-page="${pageToRemove}"]`));
    // Remove from records array (back)
    this.records = this.records.slice(0, this.records.length - rows.length);
    rows.forEach(row => row.remove());
    this._domLast--;
    // More pages are known to exist (the removed page's cursor is still in _pageCursors)
    this._hasMore = true;
    this._setupScrollObserver();
  }

  async _loadMoreRecords() {
    if (this._loadingMore || !this._hasMore || !this.selectedEntity) return;
    this._loadingMore = true;
    if (this._autoLoadCooldownTimer) { clearTimeout(this._autoLoadCooldownTimer); this._autoLoadCooldownTimer = null; }
    this._isAutoLoading = true;
    const indicator = this.querySelector('#loading-more-indicator');
    if (indicator) indicator.style.display = 'block';
    try {
      const pageIdx = this._domLast + 1;
      // Use pre-stored cursor when available, otherwise can't proceed
      const cursorEndTime = this._pageCursors.length > pageIdx ? this._pageCursors[pageIdx] : null;
      if (!cursorEndTime) {
        this._hasMore = false;
        if (this._scrollObserver) { this._scrollObserver.disconnect(); this._scrollObserver = null; }
        return;
      }
      const limit = this._pageSize;
      const params = new URLSearchParams({ entity_id: this.selectedEntity, limit: limit.toString(), end_time: cursorEndTime });
      let url;
      if (this.dataSource !== 'states') {
        params.append('statistic_type', this.dataSource === 'statistics_short_term' ? 'short_term' : 'long_term');
        url = `/api/history_editor/statistics?${params.toString()}`;
      } else {
        url = `/api/history_editor/records?${params.toString()}`;
      }
      const response = await fetch(url, { headers: { 'Authorization': `Bearer ${this._hass.auth.data.access_token}` } });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const result = await response.json();
      if (result && result.success && result.records && result.records.length > 0) {
        const newRecords = result.records;
        this._domLast = pageIdx;
        // Pre-compute cursor for the next page and store it
        if (result.has_more && this._pageCursors.length <= this._domLast + 1) {
          const oldest = newRecords[newRecords.length - 1];
          const oldestTs = new Date(this._getRecordTimestamp(oldest)).getTime();
          this._pageCursors.push(new Date(oldestTs - 1).toISOString());
        }
        this._hasMore = result.has_more || false;
        // Append rows and accumulate records
        this._appendToTable(newRecords, pageIdx);
        this.records = this.records.concat(newRecords);
        // Prune top page if the DOM window exceeds 3 pages
        if (this._domLast - this._domFirst + 1 > 3) {
          this._pruneTopPage();
        }
        this._setupScrollObserver();
      } else {
        this._hasMore = false;
        if (this._scrollObserver) { this._scrollObserver.disconnect(); this._scrollObserver = null; }
        if (indicator) indicator.style.display = 'none';
      }
    } catch (error) {
      console.error('[HistoryEditor] Error loading more records:', error);
    } finally {
      this._loadingMore = false;
      // Suppress observer-triggered auto-loads for 300ms to prevent feedback loops
      // after DOM mutations (pruning pages) re-expose sentinels to the viewport.
      this._autoLoadCooldownTimer = setTimeout(() => { this._isAutoLoading = false; this._autoLoadCooldownTimer = null; }, 300);
      const ind = this.querySelector('#loading-more-indicator');
      if (ind) ind.style.display = 'none';
    }
  }

  async _loadPrevRecords() {
    if (this._loadingPrev || this._domFirst === 0) return;
    this._loadingPrev = true;
    if (this._autoLoadCooldownTimer) { clearTimeout(this._autoLoadCooldownTimer); this._autoLoadCooldownTimer = null; }
    this._isAutoLoading = true;
    try {
      const prevPageIdx = this._domFirst - 1;
      const cursorEndTime = this._pageCursors[prevPageIdx];
      const limit = this._pageSize;
      const params = new URLSearchParams({ entity_id: this.selectedEntity, limit: limit.toString() });
      if (cursorEndTime) params.append('end_time', cursorEndTime);
      let url;
      if (this.dataSource !== 'states') {
        params.append('statistic_type', this.dataSource === 'statistics_short_term' ? 'short_term' : 'long_term');
        url = `/api/history_editor/statistics?${params.toString()}`;
      } else {
        url = `/api/history_editor/records?${params.toString()}`;
      }
      const response = await fetch(url, { headers: { 'Authorization': `Bearer ${this._hass.auth.data.access_token}` } });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const result = await response.json();
      if (result && result.success && result.records && result.records.length > 0) {
        const prevRecords = result.records;
        // Prune bottom page if adding this page would exceed 3 pages in DOM
        if (this._domLast - prevPageIdx + 1 > 3) {
          this._pruneBottomPage();
        }
        // Preserve scroll position while prepending (browsers may jump)
        const tableContainer = this.querySelector('.table-container');
        const firstRow = this.querySelector(`#records-tbody [data-page="${this._domFirst}"]`);
        const anchorTop = firstRow ? firstRow.getBoundingClientRect().top : 0;
        this._prependToTable(prevRecords, prevPageIdx);
        this.records = prevRecords.concat(this.records);
        this._domFirst = prevPageIdx;
        // Restore scroll position
        if (firstRow) {
          const newTop = firstRow.getBoundingClientRect().top;
          tableContainer.scrollBy(0, newTop - anchorTop);
        }
        // Hide load-prev row if we're back at the very first page
        if (this._domFirst === 0) {
          const loadPrevRow = this.querySelector('#load-prev-row');
          if (loadPrevRow) loadPrevRow.style.display = 'none';
          if (this._topObserver) { this._topObserver.disconnect(); this._topObserver = null; }
        }
      }
    } catch (error) {
      console.error('[HistoryEditor] Error loading previous records:', error);
    } finally {
      this._loadingPrev = false;
      // Suppress observer-triggered auto-loads for 300ms to prevent feedback loops
      // after DOM mutations (pruning pages) re-expose sentinels to the viewport.
      this._autoLoadCooldownTimer = setTimeout(() => { this._isAutoLoading = false; this._autoLoadCooldownTimer = null; }, 300);
    }
  }

  _scrollToTop() {
    if (this._domFirst === 0) {
      const tableContainer = this.querySelector('.table-container');
      if (tableContainer) tableContainer.scrollTop = 0;
    } else {
      this.loadRecords();
    }
  }

  _scrollToBottom() {
    const tableContainer = this.querySelector('.table-container');
    if (tableContainer) tableContainer.scrollTop = tableContainer.scrollHeight;
  }
}

customElements.define('history-editor-panel', HistoryEditorPanel);
