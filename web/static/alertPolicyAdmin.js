// The Policies tab: the fractal's rule set, and what it would flag across alerts that
// already exist.
//
// A rule is built from pickers rather than typed in a language: pick a field, pick an
// operator that field allows, give a value, and write the message an analyst will read.
const AlertPolicyAdmin = {
    _catalog: null,
    _policies: [],
    _compliance: null,
    _loadingCompliance: false,
    _dirty: false,

    async show() {
        const view = document.getElementById('alertPoliciesView');
        if (!view) return;
        view.innerHTML = '<div class="ap-empty">Loading policies...</div>';

        try {
            const [catalog, policies, gate] = await Promise.all([
                this.fetchJSON('/api/v1/alert-policies/catalog'),
                this.fetchJSON('/api/v1/alert-policies'),
                this.fetchJSON('/api/v1/alert-gate').catch(() => null)
            ]);
            this._catalog = catalog;
            this._policies = policies || [];
            this._gate = gate || { enabled: false, min_approvals: 1, allow_self_approval: true };
            this._compliance = null;
            this._dirty = false;
        } catch (e) {
            view.innerHTML = `<div class="ap-empty">Failed to load policies: ${Utils.escapeHtml(e.message)}</div>`;
            return;
        }

        this.render();
    },

    async fetchJSON(url, options) {
        const res = await fetch(url, { credentials: 'include', ...options });
        const payload = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(payload.error || `HTTP ${res.status}`);
        return payload.data;
    },

    // ---- Rendering ----

    render() {
        const view = document.getElementById('alertPoliciesView');
        if (!view) return;

        view.innerHTML = `
            <section class="ap-admin">
                <div class="ap-admin-head">
                    <h2 class="ap-admin-title">Policies${this._policies.length ? `<span class="ap-admin-count">${this._policies.length}</span>` : ''}</h2>
                    <div class="ap-admin-actions">
                        <button class="btn-secondary btn-sm" onclick="AlertPolicyAdmin.runCompliance()">Check existing alerts</button>
                        <button class="btn-secondary btn-sm" onclick="AlertPolicyAdmin.exportPolicies()">Export</button>
                        <button class="btn-secondary btn-sm" onclick="AlertPolicyAdmin.openImport()">Import</button>
                        <button class="btn-primary btn-sm" onclick="AlertPolicyAdmin.addRule()">Add rule</button>
                    </div>
                </div>

                ${this._policies.length === 0 ? this.renderEmpty() : `
                    <div class="ap-rules">
                        ${this._policies.map((p, i) => this.renderRule(p, i)).join('')}
                    </div>`}

                ${this._dirty ? `
                    <div class="ap-save-bar">
                        <span class="ap-save-hint">Unsaved changes</span>
                        <button class="btn-secondary btn-sm" onclick="AlertPolicyAdmin.show()">Discard</button>
                        <button class="btn-primary btn-sm" onclick="AlertPolicyAdmin.save()">Save rules</button>
                    </div>` : ''}

                ${this.renderGate()}
                ${this.renderCompliance()}
            </section>
        `;
    },

    renderGate() {
        const g = this._gate || {};
        return `
            <div class="ap-gate">
                <div class="ap-gate-head">
                    <span>Review</span>
                    <label class="ap-gate-switch">
                        <input type="checkbox" ${g.enabled ? 'checked' : ''} onchange="AlertPolicyAdmin.updateGate('enabled', this.checked)" />
                        <span>${g.enabled ? 'On' : 'Off'}</span>
                    </label>
                </div>
                ${g.enabled ? `
                    <div class="ap-gate-row">
                        <label class="ap-gate-field">
                            <span>Approvals required</span>
                            <select class="ap-select" onchange="AlertPolicyAdmin.updateGate('min_approvals', parseInt(this.value, 10))">
                                ${[1, 2, 3].map(n => `<option value="${n}"${g.min_approvals === n ? ' selected' : ''}>${n}</option>`).join('')}
                            </select>
                        </label>
                        <label class="ap-gate-field ap-gate-check">
                            <input type="checkbox" ${g.allow_self_approval ? 'checked' : ''} onchange="AlertPolicyAdmin.updateGate('allow_self_approval', this.checked)" />
                            <span>Admins may approve their own</span>
                        </label>
                    </div>` : ''}
            </div>
        `;
    },

    async updateGate(key, value) {
        const next = { ...this._gate, [key]: value };
        try {
            this._gate = await this.fetchJSON('/api/v1/alert-gate', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(next)
            });
            this.render();
        } catch (e) {
            Toast.error('Could not update review settings', e.message);
        }
    },

    renderEmpty() {
        return '<div class="ap-empty">No rules. Alerts are saved unchecked.</div>';
    },

    renderRule(policy, index) {
        const field = this.field(policy.field);
        const type = field ? field.type : 'string';
        const operators = this._catalog?.operators?.[type] || [];
        const needsValue = this._catalog?.operator_needs_value?.[policy.operator] !== false;

        return `
            <div class="ap-rule${policy.enabled ? '' : ' ap-rule-off'}">
                <div class="ap-rule-row">
                    <label class="ap-rule-toggle" title="${policy.enabled ? 'Enabled' : 'Disabled'}">
                        <input type="checkbox" ${policy.enabled ? 'checked' : ''}
                               onchange="AlertPolicyAdmin.update(${index}, 'enabled', this.checked)" />
                    </label>

                    <select class="ap-select" onchange="AlertPolicyAdmin.update(${index}, 'field', this.value)">
                        ${(this._catalog?.fields || []).map(f => `
                            <option value="${Utils.escapeAttr(f.name)}"${f.name === policy.field ? ' selected' : ''}>${Utils.escapeHtml(f.label)}</option>
                        `).join('')}
                    </select>

                    <select class="ap-select" onchange="AlertPolicyAdmin.update(${index}, 'operator', this.value)">
                        ${operators.map(op => `
                            <option value="${Utils.escapeAttr(op)}"${op === policy.operator ? ' selected' : ''}>${Utils.escapeHtml(this._catalog?.operator_labels?.[op] || op)}</option>
                        `).join('')}
                    </select>

                    <input type="text" class="ap-input ap-value" placeholder="value"
                           value="${Utils.escapeAttr(policy.value || '')}" ${needsValue ? '' : 'disabled'}
                           onchange="AlertPolicyAdmin.update(${index}, 'value', this.value)" />

                    <select class="ap-select ap-severity" onchange="AlertPolicyAdmin.update(${index}, 'severity', this.value)">
                        <option value="warn"${policy.severity === 'warn' ? ' selected' : ''}>Warn</option>
                        <option value="block"${policy.severity === 'block' ? ' selected' : ''}>Block</option>
                    </select>

                    <button class="ap-rule-remove" title="Remove rule" onclick="AlertPolicyAdmin.remove(${index})">&times;</button>
                </div>

                <input type="text" class="ap-input ap-message" placeholder="What should the analyst do about it?"
                       value="${Utils.escapeAttr(policy.message || '')}"
                       onchange="AlertPolicyAdmin.update(${index}, 'message', this.value)" />

                ${field?.help ? `<div class="ap-rule-help">${Utils.escapeHtml(field.help)}</div>` : ''}
            </div>
        `;
    },

    renderCompliance() {
        if (this._loadingCompliance) {
            return '<div class="ap-compliance"><div class="ap-empty">Checking...</div></div>';
        }
        if (!this._compliance) return '';

        if (this._compliance.length === 0) {
            return `
                <div class="ap-compliance">
                    <div class="ap-compliance-head">Existing alerts</div>
                    <div class="ap-empty">All clear.</div>
                </div>`;
        }

        const blocking = this._compliance.filter(r => r.blocking > 0).length;
        return `
            <div class="ap-compliance">
                <div class="ap-compliance-head">
                    Existing alerts
                    <span class="ap-compliance-note">${this._compliance.length} flagged${blocking ? ` &middot; ${blocking} blocked on next edit` : ''}</span>
                </div>
                <div class="ap-compliance-rows">
                    ${this._compliance.map(row => `
                        <div class="ap-compliance-row">
                            <div class="ap-compliance-alert">
                                <span class="ap-compliance-name">${Utils.escapeHtml(row.name)}</span>
                                ${row.blocking > 0 ? `<span class="ap-badge ap-block">${row.blocking} blocking</span>` : ''}
                                ${row.warnings > 0 ? `<span class="ap-badge ap-warn">${row.warnings} warning${row.warnings === 1 ? '' : 's'}</span>` : ''}
                            </div>
                            <div class="ap-compliance-why">
                                ${row.violations.map(v => `<span class="ap-compliance-item">${Utils.escapeHtml(v.field)}: ${Utils.escapeHtml(v.message)}</span>`).join('')}
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    },

    field(name) {
        return (this._catalog?.fields || []).find(f => f.name === name) || null;
    },

    // ---- Editing ----

    addRule() {
        const first = this._catalog?.fields?.[0];
        if (!first) return;

        this._policies.push({
            field: first.name,
            operator: (this._catalog.operators[first.type] || ['not_empty'])[0],
            value: '',
            message: '',
            severity: 'warn',
            enabled: true
        });
        this._dirty = true;
        this.render();
    },

    update(index, key, value) {
        const policy = this._policies[index];
        if (!policy) return;

        policy[key] = value;

        // Changing the field can invalidate the operator, since operators are typed.
        if (key === 'field') {
            const type = this.field(value)?.type || 'string';
            const allowed = this._catalog?.operators?.[type] || [];
            if (!allowed.includes(policy.operator)) policy.operator = allowed[0] || 'not_empty';
        }

        this._dirty = true;
        this.render();
    },

    remove(index) {
        this._policies.splice(index, 1);
        this._dirty = true;
        this.render();
    },

    async save() {
        try {
            this._policies = await this.fetchJSON('/api/v1/alert-policies', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ policies: this._policies })
            });
            this._dirty = false;
            Toast.success('Policies saved');
            this.render();
        } catch (e) {
            Toast.error('Could not save policies', e.message);
        }
    },

    async runCompliance() {
        this._loadingCompliance = true;
        this.render();
        try {
            this._compliance = await this.fetchJSON('/api/v1/alert-policies/compliance');
        } catch (e) {
            this._compliance = null;
            Toast.error('Could not check existing alerts', e.message);
        } finally {
            this._loadingCompliance = false;
            this.render();
        }
    },

    async exportPolicies() {
        try {
            const data = await this.fetchJSON('/api/v1/alert-policies/export');
            const view = document.getElementById('alertPoliciesView');
            view?.querySelector('.ap-transfer')?.remove();
            view?.querySelector('.ap-admin')?.insertAdjacentHTML('beforeend', `
                <div class="ap-transfer">
                    <div class="ap-transfer-head">
                        <span>Export</span>
                        <button class="btn-secondary btn-sm" onclick="this.closest('.ap-transfer').remove()">Close</button>
                    </div>
                    <textarea class="ap-transfer-input" readonly spellcheck="false">${Utils.escapeHtml(data.content || '')}</textarea>
                </div>
            `);
        } catch (e) {
            Toast.error('Could not export', e.message);
        }
    },

    openImport() {
        const view = document.getElementById('alertPoliciesView');
        view?.querySelector('.ap-transfer')?.remove();
        view?.querySelector('.ap-admin')?.insertAdjacentHTML('beforeend', `
            <div class="ap-transfer">
                <div class="ap-transfer-head">
                    <span>Import</span>
                    <button class="btn-secondary btn-sm" onclick="this.closest('.ap-transfer').remove()">Cancel</button>
                </div>
                <textarea id="apImportInput" class="ap-transfer-input" spellcheck="false"
                          placeholder="Paste a policy YAML document"></textarea>
                <div class="ap-transfer-actions">
                    <label class="ap-transfer-replace">
                        <input type="checkbox" id="apImportReplace" /> Replace existing rules
                    </label>
                    <button class="btn-primary btn-sm" onclick="AlertPolicyAdmin.importPolicies()">Import</button>
                </div>
            </div>
        `);
        document.getElementById('apImportInput')?.focus();
    },

    async importPolicies() {
        const content = document.getElementById('apImportInput')?.value || '';
        const replace = document.getElementById('apImportReplace')?.checked || false;
        if (!content.trim()) return;

        try {
            this._policies = await this.fetchJSON('/api/v1/alert-policies/import', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ content, replace })
            });
            this._dirty = false;
            Toast.success('Policies imported', `${this._policies.length} rules`);
            this.render();
        } catch (e) {
            Toast.error('Could not import', e.message);
        }
    }
};

window.AlertPolicyAdmin = AlertPolicyAdmin;
