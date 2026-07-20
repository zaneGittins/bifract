// Schema Fields admin tab.
//
// Renders from two endpoints: /admin/schema-fields for the configured set, and
// /admin/schema-fields/insights for the sampled field distribution, capacity,
// and ranked suggestions. Insights is advisory, so a failure there degrades the
// page to the plain configured list rather than breaking it.
const SchemaFields = {
    defaults: [],
    custom: [],
    insights: null,
    _pollTimer: null,

    // Index types the backend accepts. Order is cheapest-first so the select
    // reads as an escalation of cost.
    INDEX_TYPES: [
        { value: 'none', label: 'No index' },
        { value: 'set', label: 'Set' },
        { value: 'bloom_filter', label: 'Bloom filter' },
    ],

    init() {
        document.getElementById('schemaFieldAddBtn')?.addEventListener('click', () => this.showAddForm());
        document.getElementById('schemaFieldCancelBtn')?.addEventListener('click', () => this.hideAddForm());
        document.getElementById('schemaFieldSaveBtn')?.addEventListener('click', () => this.saveField());
        document.getElementById('schemaFieldResetBtn')?.addEventListener('click', () => this.openResetModal());

        document.getElementById('schemaExportBtn')?.addEventListener('click', () => this.exportYaml());
        document.getElementById('schemaImportBtn')?.addEventListener('click', () => document.getElementById('schemaImportInput')?.click());
        document.getElementById('schemaImportInput')?.addEventListener('change', e => this.importYaml(e));

        document.getElementById('schemaFieldName')?.addEventListener('keydown', e => {
            if (e.key === 'Enter') this.saveField();
        });

        // Delegated so re-rendered rows never need re-binding.
        document.getElementById('schemaSuggestList')?.addEventListener('click', e => this._onSuggestClick(e));
        document.getElementById('schemaConfiguredList')?.addEventListener('click', e => this._onConfiguredClick(e));
        document.getElementById('schemaIgnoredList')?.addEventListener('click', e => this._onIgnoredClick(e));
        document.getElementById('schemaConfiguredList')?.addEventListener('change', e => this._onIndexChange(e));

        document.getElementById('schemaResetCancelBtn')?.addEventListener('click', () => this.closeResetModal());
        document.getElementById('schemaResetConfirmInput')?.addEventListener('input', e => this._onResetPhraseInput(e));
        document.getElementById('schemaResetDoBtn')?.addEventListener('click', () => this.executeReset());
        document.getElementById('schemaResetModal')?.addEventListener('click', e => {
            if (e.target === document.getElementById('schemaResetModal')) this.closeResetModal();
        });
    },

    show() {
        this.load();
    },

    async load() {
        const container = document.getElementById('schemaConfiguredList');
        if (container && !this.custom.length && !this.defaults.length) {
            container.innerHTML = '<div class="loading">Loading schema fields...</div>';
        }
        try {
            const data = await HttpUtils.safeFetch('/api/v1/admin/schema-fields');
            this.defaults = data.data.defaults || [];
            this.custom = data.data.custom || [];
        } catch (err) {
            if (window.Toast) Toast.error('Failed to load schema fields', err.message);
            return;
        }

        // Insights is a separate, slower call over ClickHouse. Losing it costs
        // the capacity meter and suggestions but must not hide the field list.
        try {
            const ins = await HttpUtils.safeFetch('/api/v1/admin/schema-fields/insights');
            this.insights = ins.data || null;
        } catch (err) {
            this.insights = null;
            console.warn('Schema insights unavailable:', err.message);
        }

        this.render();
        this._scheduleStatusPoll();
    },

    // Re-fetch while a field is still applying so its status resolves without a
    // manual refresh.
    _scheduleStatusPoll() {
        if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }
        const pending = this.custom.some(f => f.sync_status === 'pending');
        // offsetParent is null while the tab is hidden; stop polling then so we
        // do not hit the API forever in the background. show() resumes it.
        const visible = document.getElementById('mainSchemaTabContent')?.offsetParent !== null;
        if (pending && visible) {
            this._pollTimer = setTimeout(() => this.load(), 2500);
        }
    },

    render() {
        this._renderCapacity();
        this._renderSuggestions();
        this._renderConfigured();
        this._renderIgnored();
    },

    // ---- Capacity -----------------------------------------------------------

    _renderCapacity() {
        const box = document.getElementById('schemaCapacity');
        if (!box) return;
        const cap = this.insights?.capacity;
        if (!cap) { box.hidden = true; return; }
        box.hidden = false;

        const limit = cap.limit || 1024;
        const reserved = cap.reserved || 0;
        const inData = Math.max(cap.in_data || 0, reserved);
        const dynamic = Math.max(inData - reserved, 0);

        document.getElementById('schemaSegHint').style.width = `${Math.min(reserved / limit * 100, 100)}%`;
        document.getElementById('schemaSegDyn').style.width = `${Math.min(dynamic / limit * 100, 100)}%`;

        document.getElementById('schemaCapFig').innerHTML =
            `<b>${inData.toLocaleString()}</b> of ${limit.toLocaleString()} columns in use` +
            ` &nbsp;·&nbsp; <b>${reserved.toLocaleString()}</b> reserved`;

        const note = document.getElementById('schemaCapNote');
        const over = cap.overflowed || [];
        if (over.length) {
            const shown = over.slice(0, 3).map(o => `<span class="mono">${this.escHtml(o.name)}</span>`).join(', ');
            const more = over.length > 3 ? ` and ${over.length - 3} more` : '';
            // Some real log keys (hyphens, dots) are valid in ClickHouse but not
            // accepted as Bifract field names. Offering to fix those would hand
            // the admin a button that always errors, so say so instead.
            const addable = over.filter(o => o.addable).map(o => o.name);
            const blocked = over.length - addable.length;

            let tail = '';
            if (addable.length) {
                tail = ` <button class="btn-secondary btn-sm" id="schemaFixOverflow">Add ${addable.length === 1 ? 'it' : `all ${addable.length}`}</button>`;
            }
            let blockedNote = '';
            if (blocked) {
                blockedNote = ` ${blocked} of these cannot be reserved yet: field names may only contain letters, digits, and underscores.`;
            }

            note.className = 'schema-cap-note warn';
            note.innerHTML = `${shown}${more} no longer ${over.length === 1 ? 'has a column' : 'have columns'} of ` +
                `their own, so queries on them scan every row. Adding them reserves capacity permanently.` +
                blockedNote + tail;
            document.getElementById('schemaFixOverflow')?.addEventListener('click', () => this.addOverflowed(addable));
        } else {
            note.className = 'schema-cap-note';
            note.innerHTML = 'ClickHouse assigns spare columns automatically, first come first served. ' +
                'Reserved fields keep theirs permanently.' + this._checkedSuffix(cap.checked_at);
        }
    },

    _checkedSuffix(checkedAt) {
        if (!checkedAt) return '';
        const d = new Date(checkedAt);
        if (isNaN(d)) return '';
        return ` Capacity last checked ${this._ago(d)}.`;
    },

    _ago(date) {
        const mins = Math.floor((Date.now() - date.getTime()) / 60000);
        if (mins < 1) return 'just now';
        if (mins < 60) return `${mins}m ago`;
        const hrs = Math.floor(mins / 60);
        if (hrs < 24) return `${hrs}h ago`;
        return `${Math.floor(hrs / 24)}d ago`;
    },

    // ---- Suggestions --------------------------------------------------------

    _renderSuggestions() {
        const zone = document.getElementById('schemaSuggestZone');
        const list = document.getElementById('schemaSuggestList');
        const empty = document.getElementById('schemaSuggestEmpty');
        if (!zone || !list || !empty) return;

        const sugs = this.insights?.suggestions || [];
        if (!sugs.length) {
            zone.hidden = true;
            // Only claim "no data yet" when there genuinely is none; otherwise the
            // list is empty because everything worth reserving already is.
            empty.hidden = false;
            const sampled = this.insights?.sample_size || 0;
            document.getElementById('schemaSuggestEmptyText').textContent = sampled
                ? 'Every field seen in your recent logs is already reserved or ignored.'
                : 'Bifract reads your ingested logs to find fields worth reserving. Suggestions appear once enough data has arrived.';
            return;
        }

        empty.hidden = true;
        zone.hidden = false;
        document.getElementById('schemaSuggestCount').textContent = sugs.length;
        list.innerHTML = sugs.map(s => this._suggestRow(s)).join('');
    },

    _suggestRow(s) {
        const name = this.escHtml(s.name);
        const reasons = (s.reasons || []).map(r =>
            r === 'out of capacity'
                ? '<span class="schema-flag">out of capacity</span>'
                : this.escHtml(r)).join(' · ');
        return `<div class="schema-row${s.overflowed ? ' urgent' : ''}" data-field="${name}">
            <div class="schema-fname">${name}</div>
            <div class="schema-why">${reasons}</div>
            ${this._indexSelect(s.recommended_index, s.recommended_index, false)}
            <div class="schema-row-actions">
                <button class="btn-ghost btn-sm" data-act="ignore">Ignore</button>
                <button class="btn-primary btn-sm" data-act="add">Add</button>
            </div>
        </div>`;
    },

    // appearance:none in CSS so the control renders identically in both themes,
    // which native selects do not.
    _indexSelect(selected, recommended, disabled) {
        const opts = this.INDEX_TYPES.map(t => {
            const sel = t.value === selected ? ' selected' : '';
            const rec = recommended && t.value === recommended ? ' - suggested' : '';
            return `<option value="${t.value}"${sel}>${this.escHtml(t.label + rec)}</option>`;
        }).join('');
        const on = selected && selected !== 'none' ? ' on' : '';
        return `<span class="schema-sel${on}">
            <select aria-label="Skip index type"${disabled ? ' disabled' : ''}>${opts}</select></span>`;
    },

    async _onSuggestClick(e) {
        const btn = e.target.closest('button[data-act]');
        if (!btn) return;
        const row = btn.closest('.schema-row');
        const field = row?.dataset.field;
        if (!field) return;

        if (btn.dataset.act === 'ignore') {
            await this.ignoreField(field);
            return;
        }
        const indexType = row.querySelector('select')?.value || 'none';
        await this.addField(field, indexType);
    },

    async addField(name, indexType) {
        try {
            await HttpUtils.safeFetch('/api/v1/admin/schema-fields', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ field_name: name, index_type: indexType }),
            });
            if (window.Toast) {
                Toast.success('Field added', indexType === 'none'
                    ? `"${name}" now has its own column, including for logs you already have.`
                    : `"${name}" now has its own column. Its ${indexType === 'set' ? 'set' : 'bloom filter'} index applies to newly ingested logs.`);
            }
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Could not add field', err.message);
        }
    },

    // Bulk-reserve everything currently overflowing. Sequential rather than
    // parallel: each add triggers a ClickHouse reconcile, and those are
    // serialized server-side anyway.
    async addOverflowed(fields) {
        const btn = document.getElementById('schemaFixOverflow');
        if (btn) { btn.disabled = true; btn.textContent = 'Adding...'; }
        let added = 0;
        for (const f of fields) {
            try {
                await HttpUtils.safeFetch('/api/v1/admin/schema-fields', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ field_name: f, index_type: 'none' }),
                });
                added++;
            } catch (err) {
                console.warn('add overflowed field failed:', f, err.message);
            }
        }
        if (window.Toast) {
            if (added === fields.length) {
                Toast.success('Capacity reserved', `${added} field${added === 1 ? '' : 's'} now ${added === 1 ? 'has its' : 'have their'} own column.`);
            } else {
                Toast.error('Some fields could not be added', `${added} of ${fields.length} succeeded.`);
            }
        }
        this.load();
    },

    async ignoreField(name) {
        try {
            await HttpUtils.safeFetch(`/api/v1/admin/schema-fields/ignore/${encodeURIComponent(name)}`, { method: 'POST' });
            if (window.Toast) Toast.success('Field ignored', `"${name}" will not be suggested again. Restore it from Ignored fields.`);
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Could not ignore field', err.message);
        }
    },

    async unignoreField(name) {
        try {
            await HttpUtils.safeFetch(`/api/v1/admin/schema-fields/ignore/${encodeURIComponent(name)}`, { method: 'DELETE' });
            if (window.Toast) Toast.success('Field restored', `"${name}" can be suggested again.`);
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Could not restore field', err.message);
        }
    },

    // ---- Configured ---------------------------------------------------------

    _renderConfigured() {
        const list = document.getElementById('schemaConfiguredList');
        if (!list) return;
        const all = [
            ...this.defaults.map(f => ({ ...f, builtin: true })),
            ...this.custom.map(f => ({ ...f, builtin: false })),
        ];
        document.getElementById('schemaConfiguredCount').textContent = all.length;

        const header = `<div class="schema-thead">
            <span>Field</span><span>In your logs</span><span>Skip index</span><span>Status</span>
        </div>`;
        list.innerHTML = header + all.map(f => this._configuredRow(f)).join('');
    },

    _configuredRow(f) {
        const name = this.escHtml(f.field_name);
        const stat = this.insights?.stats?.[f.field_name];
        // A reserved field absent from the data is capacity spent on nothing,
        // which is worth showing rather than leaving blank.
        const usage = stat
            ? `in <b>${this._pct(stat.coverage)}</b> of logs · <b>${this._count(stat.cardinality)}</b> distinct`
            : (this.insights ? '<span class="schema-unused">not seen in your logs</span>' : '');

        return `<div class="schema-row" data-field="${name}">
            <div class="schema-fname">${name}${f.builtin ? '<span class="schema-builtin">built in</span>' : ''}</div>
            <div class="schema-why">${usage}</div>
            ${this._indexSelect(f.index_type || 'none', null, f.builtin)}
            <div class="schema-row-actions">
                ${this._statusPill(f)}
                ${f.builtin ? '' : '<button class="btn-ghost btn-sm" data-act="remove">Remove</button>'}
            </div>
        </div>`;
    },

    _statusPill(f) {
        if (f.builtin) return '<span class="schema-pill p-ok">Active</span>';
        switch (f.sync_status) {
            case 'pending':
                return '<span class="schema-pill p-wait">Applying</span>';
            case 'error':
                return `<span class="schema-pill p-err" title="${this.escHtml(f.sync_error || 'Schema update failed')}">Index failed</span>`;
            default:
                return '<span class="schema-pill p-ok">Active</span>';
        }
    },

    _onConfiguredClick(e) {
        const btn = e.target.closest('button[data-act="remove"]');
        if (!btn) return;
        const field = btn.closest('.schema-row')?.dataset.field;
        if (field) this.deleteField(field);
    },

    // Changing a configured field's index re-creates it: the reconcile is
    // additive, so the old index is dropped first or the change would not apply.
    async _onIndexChange(e) {
        const sel = e.target.closest('.schema-sel select');
        if (!sel || sel.disabled) return;
        sel.parentElement.classList.toggle('on', sel.value !== 'none');

        const field = sel.closest('.schema-row')?.dataset.field;
        if (!field) return;
        try {
            await HttpUtils.safeFetch(`/api/v1/admin/schema-fields/${encodeURIComponent(field)}`, { method: 'DELETE' });
            await HttpUtils.safeFetch('/api/v1/admin/schema-fields', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ field_name: field, index_type: sel.value }),
            });
            if (window.Toast) Toast.success('Index updated', `"${field}" now uses ${sel.value === 'none' ? 'no index' : sel.value}.`);
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Could not change index', err.message);
            this.load();
        }
    },

    async deleteField(name) {
        if (!confirm(`Remove custom field "${name}"?\n\nIts skip index is dropped. The column reservation stays until the next schema rebuild, which is harmless and is reused if you re-add the field.`)) return;
        try {
            await HttpUtils.safeFetch(`/api/v1/admin/schema-fields/${encodeURIComponent(name)}`, { method: 'DELETE' });
            if (window.Toast) Toast.success('Field removed', `"${name}" is no longer indexed.`);
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Could not remove field', err.message);
        }
    },

    // ---- Ignored ------------------------------------------------------------

    _renderIgnored() {
        const zone = document.getElementById('schemaIgnoredZone');
        const list = document.getElementById('schemaIgnoredList');
        if (!zone || !list) return;
        const ignored = this.insights?.ignored || [];
        if (!ignored.length) { zone.hidden = true; return; }
        zone.hidden = false;
        document.getElementById('schemaIgnoredCount').textContent = ignored.length;
        list.innerHTML = ignored.map(n => `<div class="schema-ignored-row" data-field="${this.escHtml(n)}">
            <span class="schema-fname">${this.escHtml(n)}</span>
            <button class="btn-ghost btn-sm" data-act="restore">Restore</button>
        </div>`).join('');
    },

    _onIgnoredClick(e) {
        const btn = e.target.closest('button[data-act="restore"]');
        if (!btn) return;
        const field = btn.closest('.schema-ignored-row')?.dataset.field;
        if (field) this.unignoreField(field);
    },

    // ---- Add form -----------------------------------------------------------

    showAddForm() {
        const form = document.getElementById('schemaFieldAddForm');
        const input = document.getElementById('schemaFieldName');
        if (form) form.style.display = 'block';
        if (input) { input.value = ''; input.focus(); }
        const sel = document.getElementById('schemaFieldIndexType');
        if (sel) sel.value = 'none';
    },

    hideAddForm() {
        const form = document.getElementById('schemaFieldAddForm');
        if (form) form.style.display = 'none';
    },

    async saveField() {
        const name = document.getElementById('schemaFieldName')?.value.trim();
        const indexType = document.getElementById('schemaFieldIndexType')?.value;
        if (!name) {
            if (window.Toast) Toast.error('Field name required', 'Enter the field name as it appears in your logs.');
            document.getElementById('schemaFieldName')?.focus();
            return;
        }
        this.hideAddForm();
        await this.addField(name, indexType);
    },

    // ---- Rebuild ------------------------------------------------------------

    openResetModal() {
        const modal = document.getElementById('schemaResetModal');
        const input = document.getElementById('schemaResetConfirmInput');
        const btn = document.getElementById('schemaResetDoBtn');
        if (input) { input.value = ''; input.classList.remove('phrase-match'); }
        if (btn) btn.disabled = true;
        if (modal) modal.style.display = 'flex';
        setTimeout(() => input?.focus(), 50);
    },

    closeResetModal() {
        const modal = document.getElementById('schemaResetModal');
        if (modal) modal.style.display = 'none';
    },

    _onResetPhraseInput(e) {
        const phrase = 'DELETE ALL LOG DATA';
        const match = e.target.value === phrase;
        const btn = document.getElementById('schemaResetDoBtn');
        e.target.classList.toggle('phrase-match', match);
        if (btn) btn.disabled = !match;
    },

    async executeReset() {
        const btn = document.getElementById('schemaResetDoBtn');
        const cancelBtn = document.getElementById('schemaResetCancelBtn');
        if (btn) { btn.disabled = true; btn.textContent = 'Rebuilding...'; }
        if (cancelBtn) cancelBtn.disabled = true;
        try {
            await HttpUtils.safeFetch('/api/v1/admin/schema-fields/reset', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ confirm: 'DELETE ALL LOG DATA' }),
            });
            this.closeResetModal();
            if (window.Toast) Toast.success('Schema rebuilt', 'All log data was deleted and the schema recreated.');
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Rebuild failed', err.message);
        } finally {
            if (btn) { btn.disabled = false; btn.textContent = 'Delete all logs and rebuild'; }
            if (cancelBtn) cancelBtn.disabled = false;
        }
    },

    // ---- Import / export ----------------------------------------------------

    async exportYaml() {
        try {
            const res = await fetch('/api/v1/admin/schema-fields/export', { credentials: 'include' });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const blob = await res.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'schema-fields.yaml';
            document.body.appendChild(a);
            a.click();
            a.remove();
            URL.revokeObjectURL(url);
        } catch (err) {
            if (window.Toast) Toast.error('Export failed', err.message);
        }
    },

    async importYaml(e) {
        const input = e.target;
        const file = input.files && input.files[0];
        if (!file) return;
        const count = this.custom.length;
        if (!confirm(`Import "${file.name}"?\n\nThis replaces all ${count} current custom field(s) with the file's contents. Fields not in the file are removed. Log data is not affected.`)) {
            input.value = '';
            return;
        }
        try {
            const text = await file.text();
            const data = await HttpUtils.safeFetch('/api/v1/admin/schema-fields/import', {
                method: 'POST',
                headers: { 'Content-Type': 'text/yaml' },
                body: text,
            });
            if (window.Toast) Toast.success('Schema imported', data.data?.message || 'Custom fields replaced.');
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Import failed', err.message);
        } finally {
            input.value = '';
        }
    },

    // ---- Helpers ------------------------------------------------------------

    _pct(f) {
        const pct = (f || 0) * 100;
        if (pct > 0 && pct < 1) return '<1%';
        return `${Math.round(pct)}%`;
    },

    _count(n) {
        n = n || 0;
        if (n >= 1000000) return `${(n / 1000000).toFixed(1)}M`;
        if (n >= 1000) return `${Math.round(n / 1000)}k`;
        return String(n);
    },

    escHtml(s) {
        return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    },
};

window.SchemaFields = SchemaFields;
