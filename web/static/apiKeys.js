// API Keys Management Component
const APIKeys = {
    currentFractal: null,
    currentScope: null, // { type: 'fractal'|'prism', id, name }
    currentKeys: [],
    currentTab: 'overview',
    currentPage: 1,

    init() {
        console.log('[APIKeys] Initialized');
        this.setupEventListeners();
    },

    // Returns the API base URL for the current scope
    baseURL() {
        if (!this.currentScope) return '';
        if (this.currentScope.type === 'prism') {
            return `/api/v1/prisms/${this.currentScope.id}/api-keys`;
        }
        return `/api/v1/fractals/${this.currentScope.id}/api-keys`;
    },

    setupEventListeners() {
        // Modal triggers and form handlers
        document.addEventListener('click', (e) => {
            if (e.target.id === 'createAPIKeyBtn') {
                this.switchToTab('create');
            } else if (e.target.id === 'saveAPIKeyBtn') {
                this.createAPIKey();
            } else if (e.target.classList.contains('delete-api-key-btn')) {
                this.confirmDeleteAPIKey(e.target.dataset.keyId);
            } else if (e.target.classList.contains('toggle-api-key-btn')) {
                this.toggleAPIKey(e.target.dataset.keyId);
            } else if (e.target.classList.contains('copy-key-btn')) {
                this.copyToClipboard(e.target.dataset.key);
            }

            // Tab navigation
            else if (e.target.classList.contains('tab-btn')) {
                const tab = e.target.dataset.tab;
                if (tab) {
                    this.switchToTab(tab);
                }
            }

            // Navigation buttons
            else if (e.target.id === 'backToOverviewBtn' || e.target.id === 'cancelCreateBtn') {
                this.switchToTab('overview');
            } else if (e.target.id === 'backToOverviewFromSuccessBtn' || e.target.id === 'confirmAPIKeySavedBtn') {
                this.switchToTab('overview');
            }
        });

        // Expiration toggle functionality
        document.addEventListener('change', (e) => {
            if (e.target.id === 'apiKeyNeverExpires') {
                const expiresAtInput = document.getElementById('apiKeyExpiresAt');
                if (expiresAtInput) {
                    expiresAtInput.disabled = e.target.checked;
                    if (e.target.checked) {
                        expiresAtInput.value = '';
                    }
                }
            }

            // "All Permissions" toggle
            if (e.target.id === 'apiKeyPermAll') {
                const checked = e.target.checked;
                const permIds = ['apiKeyPermQuery', 'apiKeyPermComment', 'apiKeyPermAlertManage', 'apiKeyPermNotebook', 'apiKeyPermDashboard'];
                permIds.forEach(id => {
                    const el = document.getElementById(id);
                    if (el) el.checked = checked;
                });
            }

            // Sync "All" toggle when individual permissions change
            const permIds = ['apiKeyPermQuery', 'apiKeyPermComment', 'apiKeyPermAlertManage', 'apiKeyPermNotebook', 'apiKeyPermDashboard'];
            if (permIds.includes(e.target.id)) {
                const allToggle = document.getElementById('apiKeyPermAll');
                if (allToggle) {
                    allToggle.checked = permIds.every(id => document.getElementById(id)?.checked);
                }
            }
        });

        // Close modal handlers - scoped to the API keys modal only
        document.addEventListener('click', (e) => {
            const modal = document.getElementById('apiKeysModal');
            if (!modal || modal.style.display === 'none') return;
            if (modal.contains(e.target) && (e.target.classList.contains('close-btn') || e.target.classList.contains('modal-backdrop'))) {
                this.closeModal();
            }
        });
    },

    switchToTab(tabName) {
        console.log('[APIKeys] Switching to tab:', tabName);

        // Update current tab
        this.currentTab = tabName;

        // Hide all tab contents
        const tabContents = document.querySelectorAll('.api-keys-tab-content');
        tabContents.forEach(content => {
            content.style.display = 'none';
        });

        // Remove active class from all tab buttons
        const tabButtons = document.querySelectorAll('.tab-btn');
        tabButtons.forEach(btn => {
            btn.classList.remove('active');
        });

        // Show selected tab content
        const selectedContent = document.getElementById(`apiKeys${tabName.charAt(0).toUpperCase() + tabName.slice(1)}Tab`);
        if (selectedContent) {
            selectedContent.style.display = 'block';
        }

        // Add active class to selected tab button
        const selectedButton = document.querySelector(`.tab-btn[data-tab="${tabName}"]`);
        if (selectedButton) {
            selectedButton.classList.add('active');
        }

        // Update breadcrumbs
        this.updateBreadcrumbs(tabName);

        // Tab-specific actions
        if (tabName === 'create') {
            this.resetCreateForm();
        } else if (tabName === 'overview') {
            this.loadAPIKeys(); // Refresh the list when returning to overview
        }
    },

    updateBreadcrumbs(tabName) {
        const breadcrumb = document.getElementById('apiKeysBreadcrumb');
        const currentBreadcrumb = document.getElementById('breadcrumbCurrent');

        if (!breadcrumb || !currentBreadcrumb) return;

        if (tabName === 'overview') {
            breadcrumb.style.display = 'none';
        } else {
            breadcrumb.style.display = 'block';
            const tabNames = {
                'create': 'Create Key',
                'success': 'Key Created'
            };
            currentBreadcrumb.textContent = tabNames[tabName] || tabName;
        }
    },

    resetCreateForm() {
        const form = document.getElementById('createAPIKeyForm');
        if (form) {
            form.reset();
        }

        // Set default expiration to 30 days from now
        const defaultExpiry = new Date();
        defaultExpiry.setDate(defaultExpiry.getDate() + 30);

        const expiresAtInput = document.getElementById('apiKeyExpiresAt');
        if (expiresAtInput) {
            expiresAtInput.value = defaultExpiry.toISOString().slice(0, 16).replace('T', ' ');
            expiresAtInput.disabled = false;
        }

        const neverExpiresCheckbox = document.getElementById('apiKeyNeverExpires');
        if (neverExpiresCheckbox) {
            neverExpiresCheckbox.checked = false;
        }

        // Reset permissions to defaults
        const permDefaults = {
            apiKeyPermAll: false,
            apiKeyPermQuery: true,
            apiKeyPermComment: true,
            apiKeyPermAlertManage: false,
            apiKeyPermNotebook: false,
            apiKeyPermDashboard: false
        };
        Object.entries(permDefaults).forEach(([id, val]) => {
            const el = document.getElementById(id);
            if (el) el.checked = val;
        });
    },

    // Open modal for fractal API keys (existing behavior)
    async showAPIKeysModal() {
        if (!window.FractalContext || !window.FractalContext.currentFractal) {
            if (window.Toast) {
                Toast.error('Error', 'No fractal selected');
            }
            return;
        }

        const fractal = window.FractalContext.currentFractal;
        this.currentFractal = fractal;
        this.currentScope = { type: 'fractal', id: fractal.id, name: fractal.name };

        await this._openModal();
    },

    // Open modal for prism API keys
    async showPrismAPIKeysModal(prism) {
        if (!prism || !prism.id) {
            if (window.Toast) {
                Toast.error('Error', 'No prism provided');
            }
            return;
        }

        this.currentFractal = null;
        this.currentScope = { type: 'prism', id: prism.id, name: prism.name };

        await this._openModal();
    },

    async _openModal() {
        await this.loadAPIKeys();

        const scopeLabel = this.currentScope.type === 'prism' ? 'Prism' : 'Fractal';

        const modalTitle = document.getElementById('apiKeysModalTitle');
        if (modalTitle) {
            modalTitle.textContent = 'API Keys Management';
        }

        const overviewTitle = document.getElementById('apiKeysOverviewTitle');
        if (overviewTitle) {
            overviewTitle.textContent = `API Keys for ${this.currentScope.name}`;
        }

        this.switchToTab('overview');

        const modal = document.getElementById('apiKeysModal');
        if (modal) {
            modal.style.display = 'block';
        } else {
            console.error('API Keys modal not found in DOM');
            if (window.Toast) {
                Toast.error('Error', 'API Keys modal not found');
            }
        }
    },

    async loadAPIKeys() {
        if (!this.currentScope) return;
        try {
            const response = await fetch(this.baseURL(), {
                method: 'GET',
                credentials: 'include'
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            if (data.success) {
                this.currentKeys = data.data?.api_keys || [];
                this.renderAPIKeys();
            } else {
                throw new Error(data.error || 'Failed to load API keys');
            }
        } catch (error) {
            console.error('Failed to load API keys:', error);
            if (window.Toast) {
                Toast.error('Load Failed', error.message);
            }
        }
    },

    renderAPIKeys() {
        const container = document.getElementById('apiKeysList');
        if (!container) {
            console.error('API keys list container not found');
            return;
        }

        const scopeLabel = this.currentScope?.type === 'prism' ? 'prism' : 'fractal';

        if (this.currentKeys.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <p>No API keys configured for this ${scopeLabel}.</p>
                    <button id="createFirstAPIKeyBtn" class="btn-primary" onclick="APIKeys.switchToTab('create')">
                        Create Your First API Key
                    </button>
                </div>
            `;
            return;
        }

        // Calculate pagination
        const pageSize = 10;
        const totalPages = Math.ceil(this.currentKeys.length / pageSize);
        const currentPage = this.currentPage || 1;
        const startIdx = (currentPage - 1) * pageSize;
        const endIdx = startIdx + pageSize;
        const pageKeys = this.currentKeys.slice(startIdx, endIdx);

        let html = `
            <div class="api-keys-table-wrapper">
                <table class="api-keys-table">
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>Key ID</th>
                            <th>Permissions</th>
                            <th>Status</th>
                            <th>Expires</th>
                            <th>Usage</th>
                            <th>Created</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
        `;

        pageKeys.forEach(key => {
            const isExpired = key.expires_at && new Date(key.expires_at) < new Date();
            const statusClass = key.is_active && !isExpired ? 'active' : 'inactive';
            const expiresText = key.expires_at
                ? new Date(key.expires_at).toLocaleDateString()
                : 'Never';
            const lastUsedText = key.last_used_at
                ? new Date(key.last_used_at).toLocaleDateString()
                : 'Never';
            const createdText = new Date(key.created_at).toLocaleDateString();

            html += `
                <tr class="api-key-row ${statusClass}" data-key-id="${key.id}">
                    <td class="key-name-cell">
                        <div class="key-name">${Utils.escapeHtml(key.name)}</div>
                        ${key.description ? `<div class="key-description">${Utils.escapeHtml(key.description)}</div>` : ''}
                    </td>
                    <td class="key-id-cell">
                        <code class="key-id">${key.key_id}</code>
                    </td>
                    <td class="permissions-cell">
                        <div class="perm-badges">
                            ${this.renderPermBadges(key.permissions)}
                        </div>
                    </td>
                    <td class="status-cell">
                        <span class="api-key-status status-${statusClass}">
                            ${isExpired ? 'Expired' : (key.is_active ? 'Active' : 'Inactive')}
                        </span>
                    </td>
                    <td class="expires-cell ${isExpired ? 'expired-text' : ''}">
                        ${expiresText}
                    </td>
                    <td class="usage-cell">
                        <div class="usage-info">
                            <span class="usage-count">${key.usage_count}</span>
                            <span class="last-used">${lastUsedText}</span>
                        </div>
                    </td>
                    <td class="created-cell">
                        <div class="created-info">
                            <span class="created-date">${createdText}</span>
                            <span class="created-by">${Utils.escapeHtml(key.created_by)}</span>
                        </div>
                    </td>
                    <td class="actions-cell">
                        <div class="api-key-actions">
                            <button class="btn-small toggle-api-key-btn ${key.is_active ? 'btn-warning' : 'btn-success'}"
                                    data-key-id="${key.id}">
                                ${key.is_active ? 'Deactivate' : 'Activate'}
                            </button>
                            <button class="btn-small delete-api-key-btn btn-danger"
                                    data-key-id="${key.id}">
                                Delete
                            </button>
                        </div>
                    </td>
                </tr>
            `;
        });

        html += `
                    </tbody>
                </table>
            </div>
        `;

        // Add pagination controls
        if (totalPages > 1) {
            html += `
                <div class="api-keys-pagination">
                    <div class="pagination-info">
                        Showing ${startIdx + 1}-${Math.min(endIdx, this.currentKeys.length)} of ${this.currentKeys.length} API keys
                    </div>
                    <div class="pagination-controls">
                        ${currentPage > 1 ?
                            `<button class="pagination-btn" onclick="APIKeys.goToPage(${currentPage - 1})">← Previous</button>` :
                            `<button class="pagination-btn" disabled>← Previous</button>`
                        }
                        <span class="page-info">Page ${currentPage} of ${totalPages}</span>
                        ${currentPage < totalPages ?
                            `<button class="pagination-btn" onclick="APIKeys.goToPage(${currentPage + 1})">Next →</button>` :
                            `<button class="pagination-btn" disabled>Next →</button>`
                        }
                    </div>
                </div>
            `;
        }

        container.innerHTML = html;
    },

    renderPermBadges(perms) {
        if (!perms) return '<span class="perm-badge perm-off">None</span>';
        const allPerms = [
            { key: 'query', label: 'Query' },
            { key: 'comment', label: 'Comments' },
            { key: 'alert_manage', label: 'Alerts' },
            { key: 'notebook', label: 'Notes' },
            { key: 'dashboard', label: 'Dash' }
        ];
        const allEnabled = allPerms.every(p => perms[p.key]);
        if (allEnabled) return '<span class="perm-badge perm-all">All</span>';
        const enabled = allPerms.filter(p => perms[p.key]);
        if (enabled.length === 0) return '<span class="perm-badge perm-off">None</span>';
        return enabled.map(p => `<span class="perm-badge perm-on">${p.label}</span>`).join('');
    },

    goToPage(page) {
        this.currentPage = page;
        this.renderAPIKeys();
    },

    async createAPIKey() {
        const form = document.getElementById('createAPIKeyForm');
        if (!form) {
            console.error('Create API key form not found');
            return;
        }

        const formData = new FormData(form);
        const neverExpires = formData.get('never_expires') === 'on';
        const expiresAt = neverExpires ? null : formData.get('expires_at');

        const request = {
            name: formData.get('name'),
            description: formData.get('description') || '',
            expires_at: expiresAt ? new Date(expiresAt).toISOString() : null,
            permissions: {
                query: document.getElementById('apiKeyPermQuery')?.checked ?? true,
                comment: document.getElementById('apiKeyPermComment')?.checked ?? true,
                alert_manage: document.getElementById('apiKeyPermAlertManage')?.checked ?? false,
                notebook: document.getElementById('apiKeyPermNotebook')?.checked ?? false,
                dashboard: document.getElementById('apiKeyPermDashboard')?.checked ?? false
            }
        };

        // Validate required fields
        if (!request.name) {
            if (window.Toast) {
                Toast.error('Validation Error', 'API key name is required');
            }
            return;
        }

        const saveBtn = document.getElementById('saveAPIKeyBtn');

        try {
            if (saveBtn) {
                saveBtn.disabled = true;
                saveBtn.textContent = 'Creating...';
            }

            const response = await fetch(this.baseURL(), {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                credentials: 'include',
                body: JSON.stringify(request)
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.error || `HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            if (data.success) {
                // Show the success tab with the API key result
                this.showAPIKeyResult(data.data);

                // Refresh list for when user returns to overview
                await this.loadAPIKeys();

                if (window.Toast) {
                    Toast.success('API Key Created', `API key "${request.name}" created successfully`);
                }
            } else {
                throw new Error(data.error || 'Failed to create API key');
            }
        } catch (error) {
            console.error('Failed to create API key:', error);
            if (window.Toast) {
                Toast.error('Create Failed', error.message);
            }
        } finally {
            if (saveBtn) {
                saveBtn.disabled = false;
                saveBtn.textContent = 'Create API Key';
            }
        }
    },

    showAPIKeyResult(result) {
        // Populate the success tab content
        const keyDisplay = document.getElementById('apiKeyDisplay');
        if (keyDisplay) {
            keyDisplay.value = result.key;
        }

        const keyName = document.getElementById('apiKeyResultName');
        if (keyName) {
            keyName.textContent = result.api_key.name;
        }

        const keyFractal = document.getElementById('apiKeyResultFractal');
        if (keyFractal && this.currentScope) {
            keyFractal.textContent = this.currentScope.name;
        }

        const copyBtn = document.getElementById('copyAPIKeyBtn');
        if (copyBtn) {
            copyBtn.dataset.key = result.key;
        }

        // Switch to success tab
        this.switchToTab('success');

        // Auto-select the key for easy copying
        setTimeout(() => {
            if (keyDisplay) {
                keyDisplay.select();
                keyDisplay.focus();
            }
        }, 100);
    },

    async confirmDeleteAPIKey(keyId) {
        const key = this.currentKeys.find(k => k.id === keyId);
        if (!key) {
            console.error('API key not found:', keyId);
            return;
        }

        const confirmation = confirm(
            `Are you sure you want to delete the API key "${key.name}"?\n\n` +
            `This action cannot be undone and will immediately revoke access for this key.`
        );

        if (confirmation) {
            await this.deleteAPIKey(keyId);
        }
    },

    async deleteAPIKey(keyId) {
        try {
            const response = await fetch(`${this.baseURL()}/${keyId}`, {
                method: 'DELETE',
                credentials: 'include'
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.error || `HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            if (data.success) {
                await this.loadAPIKeys(); // Refresh the list

                if (window.Toast) {
                    Toast.success('API Key Deleted', 'API key has been permanently deleted');
                }
            } else {
                throw new Error(data.error || 'Failed to delete API key');
            }
        } catch (error) {
            console.error('Failed to delete API key:', error);
            if (window.Toast) {
                Toast.error('Delete Failed', error.message);
            }
        }
    },

    async toggleAPIKey(keyId) {
        try {
            const response = await fetch(`${this.baseURL()}/${keyId}/toggle`, {
                method: 'POST',
                credentials: 'include'
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.error || `HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            if (data.success) {
                await this.loadAPIKeys(); // Refresh the list

                const action = data.data.api_key.is_active ? 'activated' : 'deactivated';
                if (window.Toast) {
                    Toast.success('API Key Updated', `API key has been ${action}`);
                }
            } else {
                throw new Error(data.error || 'Failed to toggle API key');
            }
        } catch (error) {
            console.error('Failed to toggle API key:', error);
            if (window.Toast) {
                Toast.error('Toggle Failed', error.message);
            }
        }
    },

    copyToClipboard(text) {
        if (navigator.clipboard && window.isSecureContext) {
            navigator.clipboard.writeText(text).then(() => {
                if (window.Toast) {
                    Toast.success('Copied', 'API key copied to clipboard');
                }
            }).catch(err => {
                console.error('Failed to copy:', err);
                this.fallbackCopyToClipboard(text);
            });
        } else {
            this.fallbackCopyToClipboard(text);
        }
    },

    fallbackCopyToClipboard(text) {
        // Fallback for older browsers or non-HTTPS
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();

        try {
            const successful = document.execCommand('copy');
            if (successful && window.Toast) {
                Toast.success('Copied', 'API key copied to clipboard');
            } else {
                throw new Error('Copy command failed');
            }
        } catch (err) {
            console.error('Fallback copy failed:', err);
            if (window.Toast) {
                Toast.error('Copy Failed', 'Could not copy to clipboard. Please select and copy manually.');
            }
        }

        document.body.removeChild(textArea);
    },

    closeModal() {
        const modal = document.getElementById('apiKeysModal');
        if (modal) {
            modal.style.display = 'none';

            // Clear sensitive data when closing
            const keyDisplay = document.getElementById('apiKeyDisplay');
            if (keyDisplay) {
                keyDisplay.value = '';
            }

            // Reset to overview tab for next time
            this.switchToTab('overview');
        }
    },

};

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    APIKeys.init();
});

// Make globally available
window.APIKeys = APIKeys;
