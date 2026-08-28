// Instance-wide API key administration. Only tenant admins can reach the
// endpoints behind it, so this is the one place every issued key is visible,
// including the instance-wide grants no single fractal page would show.
const APIKeysAdmin = {
    keys: [],
    loaded: false,

    init() {
        const search = document.getElementById('apiKeysAdminSearch');
        if (search) search.addEventListener('input', () => this.render());

        const onlyAdmin = document.getElementById('apiKeysAdminOnlyAdmin');
        if (onlyAdmin) onlyAdmin.addEventListener('change', () => this.render());

        const refresh = document.getElementById('apiKeysAdminRefresh');
        if (refresh) refresh.addEventListener('click', () => this.load());

        const create = document.getElementById('apiKeysAdminNew');
        if (create) create.addEventListener('click', () => this.showCreate());

        const submit = document.getElementById('adminApiKeySubmit');
        if (submit) submit.addEventListener('click', () => this.create());

        // A tenant-admin key must carry an expiry, so the form stops offering
        // "never" rather than letting the server refuse the request.
        const tenantAdmin = document.getElementById('adminApiKeyTenantAdmin');
        if (tenantAdmin) tenantAdmin.addEventListener('change', () => this._syncExpiry());

        const never = document.getElementById('adminApiKeyNeverExpires');
        if (never) never.addEventListener('change', () => this._syncExpiry());
    },

    async showCreate() {
        const modal = document.getElementById('adminCreateAPIKeyModal');
        if (!modal) return;

        const scope = document.getElementById('adminApiKeyScope');
        if (scope && !scope.options.length) {
            try {
                const res = await fetch('/api/v1/fractals', { credentials: 'include' });
                const body = await res.json();
                const data = body.data || {};
                scope.innerHTML = [
                    ...(data.fractals || []).map(f =>
                        `<option value="fractal:${Utils.escapeHtml(f.id)}">fractal &middot; ${Utils.escapeHtml(f.name)}</option>`),
                    ...(data.prisms || []).map(p =>
                        `<option value="prism:${Utils.escapeHtml(p.id)}">prism &middot; ${Utils.escapeHtml(p.name)}</option>`),
                ].join('');
            } catch (_) {
                if (window.Toast) Toast.error('Failed to load scopes', 'Could not list fractals');
                return;
            }
        }

        document.getElementById('adminApiKeyName').value = '';
        document.getElementById('adminApiKeyRole').value = 'analyst';
        document.getElementById('adminApiKeyTenantAdmin').checked = false;
        document.getElementById('adminApiKeyNeverExpires').checked = false;

        const expires = document.getElementById('adminApiKeyExpires');
        const in30Days = new Date(Date.now() + 30 * 86400000);
        expires.value = new Date(in30Days.getTime() - in30Days.getTimezoneOffset() * 60000)
            .toISOString().slice(0, 16);

        this._syncExpiry();
        modal.style.display = 'flex';
        setTimeout(() => document.getElementById('adminApiKeyName')?.focus(), 50);
    },

    hideCreate() {
        const modal = document.getElementById('adminCreateAPIKeyModal');
        if (modal) modal.style.display = 'none';
    },

    _syncExpiry() {
        const tenantAdmin = document.getElementById('adminApiKeyTenantAdmin')?.checked;
        const never = document.getElementById('adminApiKeyNeverExpires');
        const expires = document.getElementById('adminApiKeyExpires');
        if (!never || !expires) return;

        if (tenantAdmin) never.checked = false;
        never.disabled = !!tenantAdmin;
        expires.disabled = never.checked;
    },

    async create() {
        const name = document.getElementById('adminApiKeyName')?.value?.trim();
        if (!name) {
            if (window.Toast) Toast.error('Validation', 'Give the key a name');
            return;
        }
        const scope = document.getElementById('adminApiKeyScope')?.value || '';
        const [kind, id] = scope.split(':');
        if (!id) {
            if (window.Toast) Toast.error('Validation', 'Choose a scope for the key');
            return;
        }

        const never = document.getElementById('adminApiKeyNeverExpires')?.checked;
        const expiresRaw = document.getElementById('adminApiKeyExpires')?.value;
        const request = {
            name,
            description: '',
            role: document.getElementById('adminApiKeyRole')?.value || 'analyst',
            tenant_admin: !!document.getElementById('adminApiKeyTenantAdmin')?.checked,
            expires_at: never || !expiresRaw ? null : new Date(expiresRaw).toISOString(),
        };

        const path = kind === 'prism' ? `/api/v1/prisms/${id}/api-keys` : `/api/v1/fractals/${id}/api-keys`;
        const btn = document.getElementById('adminApiKeySubmit');
        if (btn) btn.disabled = true;
        try {
            const res = await fetch(path, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json', 'X-Bifract-Scope': scope },
                body: JSON.stringify(request),
            });
            if (!res.ok) throw new Error(await Utils.errorMessage(res, 'Failed to create the key'));
            const body = await res.json();
            this.hideCreate();
            // The secret is returned exactly once, so hand it straight to the
            // existing one-time reveal rather than a toast that scrolls away.
            const key = (body.data || {}).key;
            if (key && window.APIKeys?.showKeyDialog) {
                APIKeys.showKeyDialog(key);
            } else if (window.Toast) {
                Toast.success('Key created', 'Copy it now: it is not shown again');
            }
            await this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Failed to create the key', err.message);
        } finally {
            if (btn) btn.disabled = false;
        }
    },

    async show() {
        if (!this.loaded) await this.load();
    },

    async load() {
        const container = document.getElementById('apiKeysAdminList');
        if (container) container.innerHTML = '<div class="loading">Loading API keys...</div>';
        try {
            const res = await fetch('/api/v1/api-keys', { credentials: 'include' });
            if (!res.ok) throw new Error(await Utils.errorMessage(res, 'Failed to load API keys'));
            const data = await res.json();
            this.keys = data.data || [];
            this.loaded = true;
            this.render();
        } catch (err) {
            if (container) {
                container.innerHTML = `<div class="error">${Utils.escapeHtml(err.message)}</div>`;
            }
        }
    },

    filtered() {
        const term = (document.getElementById('apiKeysAdminSearch')?.value || '').toLowerCase().trim();
        const adminOnly = document.getElementById('apiKeysAdminOnlyAdmin')?.checked;
        return this.keys.filter(k => {
            if (adminOnly && !k.tenant_admin) return false;
            if (!term) return true;
            return [k.name, k.fractal_name, k.prism_name, k.created_by, k.key_id]
                .some(v => (v || '').toLowerCase().includes(term));
        });
    },

    // A key is only usable if it is active and unexpired; say which it is, since
    // "not working" is the question an operator actually arrives with.
    statusOf(key) {
        if (!key.is_active) return { label: 'Revoked', cls: 'perm-off' };
        if (key.expires_at && new Date(key.expires_at) < new Date()) {
            return { label: 'Expired', cls: 'perm-off' };
        }
        return { label: 'Active', cls: 'perm-on' };
    },

    scopeOf(key) {
        if (key.prism_id) return `Prism: ${Utils.escapeHtml(key.prism_name || key.prism_id)}`;
        return `Fractal: ${Utils.escapeHtml(key.fractal_name || key.fractal_id)}`;
    },

    render() {
        const container = document.getElementById('apiKeysAdminList');
        if (!container) return;

        const rows = this.filtered();
        if (rows.length === 0) {
            container.innerHTML = '<div class="text-muted" style="padding:0.75rem;">No API keys match.</div>';
            return;
        }

        container.innerHTML = `
            <table class="users-table">
                <thead>
                    <tr>
                        <th>Name</th><th>Scope</th><th>Grants</th><th>Created by</th>
                        <th>Expires</th><th>Last used</th><th>Status</th><th></th>
                    </tr>
                </thead>
                <tbody>
                    ${rows.map(k => this.renderRow(k)).join('')}
                </tbody>
            </table>`;
    },

    renderRow(key) {
        const status = this.statusOf(key);
        const grants = [];
        if (key.tenant_admin) grants.push('<span class="perm-badge perm-tenant">Tenant admin</span>');
        grants.push(key.role
            ? `<span class="perm-badge perm-on">${Utils.escapeHtml(key.role)}</span>`
            : '<span class="perm-badge perm-off">No access</span>');

        const expires = key.expires_at
            ? Utils.formatTimestamp(key.expires_at)
            : (key.tenant_admin ? '<span class="perm-badge perm-off">missing</span>' : 'Never');
        const lastUsed = key.last_used_at ? Utils.formatTimestamp(key.last_used_at) : 'Never';

        return `
            <tr>
                <td>
                    ${Utils.escapeHtml(key.name)}
                    <div class="text-muted" style="font-size:0.8em;">${Utils.escapeHtml(key.key_id)}</div>
                </td>
                <td>${this.scopeOf(key)}</td>
                <td><div class="perm-badges">${grants.join('')}</div></td>
                <td>${Utils.escapeHtml(key.created_by || '')}</td>
                <td>${expires}</td>
                <td>${lastUsed}</td>
                <td><span class="perm-badge ${status.cls}">${status.label}</span></td>
                <td class="kebab-cell" onclick="event.stopPropagation()">
                    <div class="kebab-wrapper">
                        <button class="kebab-btn" onclick="KebabMenu.toggle(event,this)">&vellip;</button>
                        <div class="kebab-menu">
                            <button class="kebab-item" onclick="APIKeysAdmin.copyKeyID('${Utils.escapeJs(key.key_id)}')">Copy key ID</button>
                            ${key.is_active
                                ? `<button class="kebab-item danger" onclick="APIKeysAdmin.revoke('${Utils.escapeJs(key.id)}')">Revoke</button>`
                                : ''}
                        </div>
                    </div>
                </td>
            </tr>`;
    },

    copyKeyID(keyID) {
        navigator.clipboard?.writeText(keyID)
            .then(() => window.Toast && Toast.success('Copied', 'Key ID copied'))
            .catch(() => window.Toast && Toast.error('Copy failed', 'Select and copy manually'));
    },

    async revoke(id) {
        const key = this.keys.find(k => k.id === id);
        if (!key) return;
        const what = key.tenant_admin ? 'instance-wide admin key' : 'API key';
        if (!confirm(`Revoke the ${what} "${key.name}"?\n\nAnything using it stops working immediately.`)) return;

        const scope = key.prism_id ? `prisms/${key.prism_id}` : `fractals/${key.fractal_id}`;
        try {
            const res = await fetch(`/api/v1/${scope}/api-keys/${key.id}/toggle`, {
                method: 'POST',
                credentials: 'include',
            });
            if (!res.ok) throw new Error(await Utils.errorMessage(res, 'Failed to revoke key'));
            if (window.Toast) Toast.success('Revoked', `${key.name} can no longer authenticate`);
            await this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Error', err.message);
        }
    },
};

window.APIKeysAdmin = APIKeysAdmin;
document.addEventListener('DOMContentLoaded', () => APIKeysAdmin.init());
