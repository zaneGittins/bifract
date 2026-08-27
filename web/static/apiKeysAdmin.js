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
                <td style="text-align:right;">
                    ${key.is_active
                        ? `<button class="btn-secondary btn-sm" onclick="APIKeysAdmin.revoke('${Utils.escapeHtml(key.id)}')">Revoke</button>`
                        : ''}
                </td>
            </tr>`;
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
