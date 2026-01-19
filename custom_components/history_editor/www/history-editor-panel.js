class HistoryEditorPanel extends HTMLElement {
  constructor() {
    super();
    this._hass = null;
    this.selectedEntity = null;
    this.records = [];
    this._initialized = false;
    this._entitiesLoaded = false;
  }

  connectedCallback() {
    this._ensureInitialized();
  }

  set hass(hass) {
    const hadHass = this._hass !== null;
    this._hass = hass;
    this._ensureInitialized();
    // Load entities only once when hass becomes available
    if (this._initialized && hass && !this._entitiesLoaded) {
      this._entitiesLoaded = true;
      this.loadEntities();
    }
  }

  get hass() {
    return this._hass;
  }

  _ensureInitialized() {
    if (!this._initialized) {
      this._initialized = true;
      this.renderPanel();
    }
  }

  renderPanel() {
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
      </style>

      <div class="header">
        <h1>🗄️ History Editor</h1>
      </div>

      <div class="controls">
        <div class="control-group">
          <label for="entity-select">Select Entity</label>
          <select id="entity-select">
            <option value="">-- Choose an entity --</option>
          </select>
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

    this.setupEventListeners();
    // Entities will be loaded via the hass setter when hass is available
  }

  setupEventListeners() {
    const loadBtn = this.querySelector('#load-btn');
    const addBtn = this.querySelector('#add-btn');
    const modalClose = this.querySelector('#modal-close');
    const modalCancel = this.querySelector('#modal-cancel');
    const editForm = this.querySelector('#edit-form');

    loadBtn.addEventListener('click', () => this.loadRecords());
    addBtn.addEventListener('click', () => this.showAddModal());
    modalClose.addEventListener('click', () => this.hideModal());
    modalCancel.addEventListener('click', () => this.hideModal());
    editForm.addEventListener('submit', (e) => {
      e.preventDefault();
      this.saveRecord();
    });
  }

  async loadEntities() {
    if (!this._hass) return;

    const entitySelect = this.querySelector('#entity-select');
    const states = this._hass.states;
    const entities = Object.keys(states).sort();

    entitySelect.innerHTML = '<option value="">-- Choose an entity --</option>';
    entities.forEach(entityId => {
      const option = document.createElement('option');
      option.value = entityId;
      option.textContent = entityId;
      entitySelect.appendChild(option);
    });
  }

  async loadRecords() {
    const entitySelect = this.querySelector('#entity-select');
    const limitInput = this.querySelector('#record-limit');
    const entityId = entitySelect.value;

    if (!entityId) {
      alert('Please select an entity first');
      return;
    }

    this.selectedEntity = entityId;
    const limit = parseInt(limitInput.value) || 100;

    try {
      const result = await this._hass.callService('history_editor', 'get_records', {
        entity_id: entityId,
        limit: limit
      });

      // Note: Service calls don't return values directly in HA
      // We need to use a different approach - let's simulate for now
      // In production, this would use WebSocket API to get results
      
      // For now, we'll show a message and user needs to check dev tools
      this.showMessage('Records loaded. Check Home Assistant logs or use Developer Tools -> Services to view results.');
      
    } catch (error) {
      console.error('Error loading records:', error);
      alert('Error loading records: ' + error.message);
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
    const entitySelect = this.querySelector('#entity-select');

    title.textContent = 'Add New Record';
    form.reset();
    
    this.querySelector('#edit-state-id').value = 'NEW';
    this.querySelector('#edit-entity-id').value = entitySelect.value || '';
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
