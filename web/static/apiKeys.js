// API Keys Management Component
const APIKeys = {
    currentScope: null, // { type: 'fractal'|'prism', id, name }
    currentKeys: [],

    init() {
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

    // Returns the DOM ID prefix for inline elements based on current scope
    _inlinePrefix() {
        return this.currentScope?.type === 'prism' ? 'prism' : '';
    },

    // Gets an inline element by base ID, applying the scope prefix
    _inlineEl(baseId) {
        const prefix = this._inlinePrefix();
        const id = prefix ? prefix + baseId.charAt(0).toUpperCase() + baseId.slice(1) : baseId;
        return document.getElementById(id);
    },

    setupEventListeners() {
        // Modal triggers and form handlers
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('delete-api-key-btn')) {
                this.confirmDeleteAPIKey(e.target.dataset.keyId);
            } else if (e.target.classList.contains('toggle-api-key-btn')) {
                this.toggleAPIKey(e.target.dataset.keyId);
            } else if (e.target.id === 'createdKeyCopyBtn' || e.target.closest('#createdKeyCopyBtn')) {
                const input = document.getElementById('createdKeyDisplay');
                if (input) this.copyToClipboard(input.value);
            } else if (e.target.classList.contains('copy-key-btn') || e.target.closest('.copy-key-btn')) {
                const btn = e.target.closest('.copy-key-btn') || e.target;
                this.copyToClipboard(btn.dataset.key);
            }

            // Create key modal (fractal and prism)
            else if (e.target.id === 'createAPIKeyInlineBtn' || e.target.id === 'createPrismAPIKeyInlineBtn') {
                if (e.target.id === 'createPrismAPIKeyInlineBtn' && window.FractalContext?.currentFractal) {
                    this.currentScope = { type: 'prism', id: FractalContext.currentFractal.id, name: FractalContext.currentFractal.name };
                }
                this.showCreateKeyModal();
            } else if (e.target.id === 'cancelInlineCreateBtn' || e.target.id === 'prismCancelInlineCreateBtn') {
                this.hideCreateKeyModal();
            } else if (e.target.id === 'submitInlineCreateBtn' || e.target.id === 'prismSubmitInlineCreateBtn') {
                this.createInlineAPIKey();
            }
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') this.hideCreateKeyModal();
        });

        // Expiration toggle functionality
        document.addEventListener('change', (e) => {
            if (e.target.id === 'inlineApiKeyNeverExpires' || e.target.id === 'prismInlineApiKeyNeverExpires') {
                const prefix = e.target.id.startsWith('prism') ? 'prismInlineApiKeyExpires' : 'inlineApiKeyExpires';
                const expiresAtInput = document.getElementById(prefix);
                if (expiresAtInput) {
                    expiresAtInput.disabled = e.target.checked;
                    if (e.target.checked) expiresAtInput.value = '';
                }
            }
        });
    },

    // A key carries the same role a person would hold on the scope, plus an
    // optional instance-wide grant which is deliberately loud.
    renderGrantBadges(key) {
        const role = key.role || '';
        const out = [];
        if (key.tenant_admin) out.push('<span class="perm-badge perm-tenant">Tenant admin</span>');
        out.push(role
            ? `<span class="perm-badge perm-on">${Utils.escapeHtml(role)}</span>`
            : '<span class="perm-badge perm-off">No access</span>');
        return out.join('');
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
                await this.loadInlineAPIKeys();
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
                await this.loadInlineAPIKeys();
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
            if (successful) {
                // Copied successfully
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

    // Inline API keys rendering for the Access tab
    async loadInlineAPIKeys() {
        if (!this.currentScope) return;
        try {
            const response = await fetch(this.baseURL(), { method: 'GET', credentials: 'include' });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            if (data.success) {
                this.currentKeys = data.data?.api_keys || [];
                this.renderInlineAPIKeys();
            }
        } catch (error) {
            console.error('Failed to load inline API keys:', error);
            const container = this._inlineEl('inlineAPIKeysList');
            if (container) container.innerHTML = '<div class="access-empty-state">Failed to load API keys.</div>';
        }
    },

    renderInlineAPIKeys() {
        const container = this._inlineEl('inlineAPIKeysList');
        if (!container) return;

        if (this.currentKeys.length === 0) {
            container.innerHTML = '<div class="access-empty-state">No API keys created yet.</div>';
            return;
        }

        let html = '<table class="access-table"><thead><tr><th>Name</th><th>Permissions</th><th>Status</th><th>Expires</th><th class="col-actions">Actions</th></tr></thead><tbody>';
        this.currentKeys.forEach(key => {
            const isExpired = key.expires_at && new Date(key.expires_at) < new Date();
            const statusClass = key.is_active && !isExpired ? 'active' : 'inactive';
            const statusText = isExpired ? 'Expired' : (key.is_active ? 'Active' : 'Inactive');
            const expiresText = key.expires_at ? TZ.format(key.expires_at, 'date') : 'Never';

            html += `<tr>
                <td>
                    <div class="key-name">${Utils.escapeHtml(key.name)}</div>
                    ${key.description ? `<div class="key-description">${Utils.escapeHtml(key.description)}</div>` : ''}
                </td>
                <td><div class="perm-badges">${this.renderGrantBadges(key)}</div></td>
                <td><span class="api-key-status status-${statusClass}">${statusText}</span></td>
                <td class="${isExpired ? 'expired-text' : ''}">${expiresText}</td>
                <td class="col-actions">
                    <div class="inline-key-actions">
                        <button class="btn-delete-user btn-sm toggle-api-key-btn" data-key-id="${key.id}">${key.is_active ? 'Deactivate' : 'Activate'}</button>
                        <button class="btn-delete-user btn-sm delete-api-key-btn" data-key-id="${key.id}">Delete</button>
                    </div>
                </td>
            </tr>`;
        });
        html += '</tbody></table>';
        container.innerHTML = html;
    },

    showCreateKeyModal() {
        const modal = this._inlineEl('inlineCreateAPIKeyModal');
        if (modal) {
            modal.style.display = 'flex';
            // Set default expiration
            const defaultExpiry = new Date();
            defaultExpiry.setDate(defaultExpiry.getDate() + 30);
            const expiresInput = this._inlineEl('inlineApiKeyExpires');
            if (expiresInput) {
                expiresInput.value = defaultExpiry.toISOString().slice(0, 16).replace('T', ' ');
                expiresInput.disabled = false;
            }
            const neverExpires = this._inlineEl('inlineApiKeyNeverExpires');
            if (neverExpires) neverExpires.checked = false;
            const nameInput = this._inlineEl('inlineApiKeyName');
            if (nameInput) {
                nameInput.value = '';
                setTimeout(() => nameInput.focus(), 50);
            }
            const roleSelect = this._inlineEl('inlineApiKeyRole');
            if (roleSelect) roleSelect.value = 'analyst';
        }
    },

    hideCreateKeyModal() {
        // Close both scopes: the scope may have changed since the modal opened.
        ['inlineCreateAPIKeyModal', 'prismInlineCreateAPIKeyModal'].forEach(id => {
            const modal = document.getElementById(id);
            if (modal) modal.style.display = 'none';
        });
    },

    async createInlineAPIKey() {
        const name = this._inlineEl('inlineApiKeyName')?.value?.trim();
        if (!name) {
            if (window.Toast) Toast.error('Validation', 'API key name is required');
            return;
        }

        const neverExpires = this._inlineEl('inlineApiKeyNeverExpires')?.checked;
        const expiresAt = neverExpires ? null : this._inlineEl('inlineApiKeyExpires')?.value;

        const request = {
            name,
            description: '',
            expires_at: expiresAt ? new Date(expiresAt).toISOString() : null,
            role: this._inlineEl('inlineApiKeyRole')?.value || 'analyst'
        };

        const btn = this._inlineEl('submitInlineCreateBtn');
        try {
            if (btn) { btn.disabled = true; btn.textContent = 'Creating...'; }

            const response = await fetch(this.baseURL(), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(request)
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.error || `HTTP ${response.status}`);
            }

            const data = await response.json();
            if (data.success) {
                this.hideCreateKeyModal();
                this.showKeyDialog(data.data.key);
                await this.loadInlineAPIKeys();
            } else {
                throw new Error(data.error || 'Failed to create API key');
            }
        } catch (error) {
            console.error('Failed to create inline API key:', error);
            if (window.Toast) Toast.error('Error', error.message);
        } finally {
            if (btn) { btn.disabled = false; btn.textContent = 'Create'; }
        }
    },

    showKeyDialog(key) {
        const dialog = document.getElementById('apiKeyCreatedDialog');
        const input = document.getElementById('createdKeyDisplay');
        if (dialog && input) {
            input.value = key;
            dialog.style.display = 'block';
            setTimeout(() => { input.select(); input.focus(); }, 100);
        }
    },

    dismissKeyDialog() {
        const dialog = document.getElementById('apiKeyCreatedDialog');
        if (dialog) {
            dialog.style.display = 'none';
            const input = document.getElementById('createdKeyDisplay');
            if (input) input.value = '';
        }
    },


};

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    APIKeys.init();
});

// Make globally available
window.APIKeys = APIKeys;
