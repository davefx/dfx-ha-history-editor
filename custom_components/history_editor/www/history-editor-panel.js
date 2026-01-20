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
    this.selectedEntity = null;
    this.records = [];
    this._initialized = false;
    this._entityFormInitialized = false; // Track if ha-form is set up
    this._statusElements = {}; // Cache for status elements
  }

  _debugLog(...args) {
    if (this._debugMode) {
      console.log(...args);
    }
  }

  async connectedCallback() {
    this._debugLog('[HistoryEditor] connectedCallback called');
    // Wait for Home Assistant to be fully loaded
    this._debugLog('[HistoryEditor] Waiting for home-assistant custom element to be defined...');
    await customElements.whenDefined('home-assistant');
    this._debugLog('[HistoryEditor] home-assistant custom element is now defined');
    this._ensureInitialized();
  }

  set hass(hass) {
    this._debugLog('[HistoryEditor] hass setter called, hass:', hass ? 'defined' : 'null');
    this._hass = hass;
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
        this.selectedEntity = ev.detail.value.entity_id;
        this._debugLog('[HistoryEditor] Selected entity:', this.selectedEntity);
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
      this.renderPanel();
    }
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
    
    this._debugLog('[HistoryEditor] Setting innerHTML to render panel UI');
    this.innerHTML = `
      <style>
        :host {
          display: block;
          padding: 16px;
          background: var(--primary-background-color);
          color: var(--primary-text-color);
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
        .table-container {
          background: var(--card-background-color);
          border-radius: 8px;
          overflow: hidden;
          box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        table {
          width: 100%;
          border-collapse: collapse;
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
      </style>

      <div class="header">
        <h1>🗄️ History Editor</h1>
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
        <div class="control-group" style="flex: 1; min-width: 300px;">
          <ha-form id="entity-form"></ha-form>
        </div>
        <div class="control-group">
          <label for="record-limit">Record Limit</label>
          <input type="number" id="record-limit" value="100" min="1" max="1000">
        </div>
        <button id="load-btn">Load Records</button>
        <button id="add-btn" class="secondary">Add New Record</button>
      </div>

      <div class="table-container">
        <div id="records-display"></div>
      </div>

      <div id="edit-modal" class="modal">
        <div class="modal-content">
          <div class="modal-header">
            <h2 id="modal-title">Edit Record</h2>
            <button id="modal-close" class="secondary">✕</button>
          </div>
          <form id="edit-form">
            <div class="form-field">
              <label>State ID</label>
              <input type="text" id="edit-state-id" readonly>
            </div>
            <div class="form-field">
              <label>Entity ID</label>
              <input type="text" id="edit-entity-id">
            </div>
            <div class="form-field">
              <label>State</label>
              <input type="text" id="edit-state" required>
            </div>
            <div class="form-field">
              <label>Attributes (JSON)</label>
              <textarea id="edit-attributes"></textarea>
            </div>
            <div class="form-field">
              <label>Last Changed</label>
              <input type="datetime-local" id="edit-last-changed">
            </div>
            <div class="form-field">
              <label>Last Updated</label>
              <input type="datetime-local" id="edit-last-updated">
            </div>
            <div class="modal-actions">
              <button type="button" id="modal-cancel" class="secondary">Cancel</button>
              <button type="submit" id="modal-save">Save</button>
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
    const loadBtn = this.querySelector('#load-btn');
    const addBtn = this.querySelector('#add-btn');
    const modalClose = this.querySelector('#modal-close');
    const modalCancel = this.querySelector('#modal-cancel');
    const editForm = this.querySelector('#edit-form');

    this._debugLog('[HistoryEditor] Setting up event listeners on buttons');
    loadBtn.addEventListener('click', () => this.loadRecords());
    addBtn.addEventListener('click', () => this.showAddModal());
    modalClose.addEventListener('click', () => this.hideModal());
    modalCancel.addEventListener('click', () => this.hideModal());
    editForm.addEventListener('submit', (e) => {
      e.preventDefault();
      this.saveRecord();
    });
    
    // Set up ha-form if hass is available
    if (this._hass) {
      this._debugLog('[HistoryEditor] hass available in setupEventListeners, setting up form');
      this._setupEntityForm();
    } else {
      this._debugLog('[HistoryEditor] In setupEventListeners - hass not available yet');
    }
  }

  async loadRecords() {
    const limitInput = this.querySelector('#record-limit');
    const entityId = this.selectedEntity;

    if (!entityId) {
      alert('Please select an entity first');
      return;
    }

    const limit = parseInt(limitInput.value) || 100;

    try {
      const result = await this._hass.callService(
        'history_editor', 
        'get_records', 
        {
          entity_id: entityId,
          limit: limit
        },
        {
          return_response: true
        }
      );

      // Check if the service call was successful
      if (result && result.success) {
        this.records = result.records || [];
        this.displayRecords(this.records);
      } else {
        const errorMsg = result?.error || 'Unknown error occurred';
        alert('Error loading records: ' + errorMsg);
        this.showMessage('Failed to load records: ' + errorMsg);
      }
      
    } catch (error) {
      console.error('Error loading records:', error);
      alert('Error loading records: ' + error.message);
      this.showMessage('Error loading records. Please check the console for details.');
    }
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
          <p>No records found</p>
        </div>
      `;
      return;
    }

    let html = `
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>State</th>
            <th>Attributes</th>
            <th>Last Changed</th>
            <th>Last Updated</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
    `;

    records.forEach(record => {
      const attributes = JSON.stringify(record.attributes || {});
      const attributesPreview = attributes.length > 50 
        ? attributes.substring(0, 50) + '...' 
        : attributes;

      html += `
        <tr>
          <td>${record.state_id}</td>
          <td>${this.escapeHtml(record.state)}</td>
          <td class="attribute-preview" title="${this.escapeHtml(attributes)}">${this.escapeHtml(attributesPreview)}</td>
          <td>${record.last_changed || 'N/A'}</td>
          <td>${record.last_updated || 'N/A'}</td>
          <td class="actions">
            <button class="secondary" onclick="document.querySelector('history-editor-panel').editRecord(${record.state_id})">Edit</button>
            <button class="danger" onclick="document.querySelector('history-editor-panel').deleteRecord(${record.state_id})">Delete</button>
          </td>
        </tr>
      `;
    });

    html += '</tbody></table>';
    display.innerHTML = html;
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

    title.textContent = 'Add New Record';
    form.reset();
    
    this.querySelector('#edit-state-id').value = 'NEW';
    this.querySelector('#edit-entity-id').value = this.selectedEntity || '';
    this.querySelector('#edit-entity-id').readOnly = false;
    
    modal.classList.add('show');
  }

  editRecord(stateId) {
    const record = this.records.find(r => r.state_id === stateId);
    if (!record) return;

    const modal = this.querySelector('#edit-modal');
    const title = this.querySelector('#modal-title');

    title.textContent = 'Edit Record';
    
    this.querySelector('#edit-state-id').value = record.state_id;
    this.querySelector('#edit-entity-id').value = record.entity_id;
    this.querySelector('#edit-entity-id').readOnly = true;
    this.querySelector('#edit-state').value = record.state;
    this.querySelector('#edit-attributes').value = JSON.stringify(record.attributes || {}, null, 2);
    
    if (record.last_changed) {
      this.querySelector('#edit-last-changed').value = this.formatDatetimeLocal(record.last_changed);
    }
    if (record.last_updated) {
      this.querySelector('#edit-last-updated').value = this.formatDatetimeLocal(record.last_updated);
    }

    modal.classList.add('show');
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

  hideModal() {
    const modal = this.querySelector('#edit-modal');
    modal.classList.remove('show');
  }

  async saveRecord() {
    const stateId = this.querySelector('#edit-state-id').value;
    const entityId = this.querySelector('#edit-entity-id').value;
    const state = this.querySelector('#edit-state').value;
    const attributesText = this.querySelector('#edit-attributes').value;
    const lastChanged = this.querySelector('#edit-last-changed').value;
    const lastUpdated = this.querySelector('#edit-last-updated').value;

    let attributes = {};
    try {
      if (attributesText.trim()) {
        attributes = JSON.parse(attributesText);
      }
    } catch (error) {
      alert('Invalid JSON in attributes field');
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
        if (lastChanged) data.last_changed = lastChanged;
        if (lastUpdated) data.last_updated = lastUpdated;

        await this._hass.callService('history_editor', 'create_record', data);
        alert('Record created successfully');
      } else {
        // Update existing record
        const data = {
          state_id: parseInt(stateId),
          state: state,
          attributes: attributes
        };
        if (lastChanged) data.last_changed = lastChanged;
        if (lastUpdated) data.last_updated = lastUpdated;

        await this._hass.callService('history_editor', 'update_record', data);
        alert('Record updated successfully');
      }

      this.hideModal();
      this.loadRecords();
    } catch (error) {
      console.error('Error saving record:', error);
      alert('Error saving record: ' + error.message);
    }
  }

  async deleteRecord(stateId) {
    if (!confirm(`Are you sure you want to delete record ${stateId}?`)) {
      return;
    }

    try {
      await this._hass.callService('history_editor', 'delete_record', {
        state_id: stateId
      });
      alert('Record deleted successfully');
      this.loadRecords();
    } catch (error) {
      console.error('Error deleting record:', error);
      alert('Error deleting record: ' + error.message);
    }
  }
}

customElements.define('history-editor-panel', HistoryEditorPanel);
