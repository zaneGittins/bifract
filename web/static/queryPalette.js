// Query rows for the command palette: Recent (history), Saved queries, and the
// Query page's actions. Opens via Cmd/Ctrl+K or the Queries button.
//
// History is sourced from the backend (/api/v1/query-history) so it follows the
// user across devices, with a localStorage cache for instant render. Saved
// queries come from /api/v1/saved-queries and support descriptions, personal/
// shared visibility, favorites, and usage stats.

const QueryPalette = {
    // Rows shown before anything is typed, on the Query page, sized so Actions
    // and Ask AI stay in view.
    IDLE_RECENT: 4,
    IDLE_SAVED: 3,

    init() {
        const btn = document.getElementById('queriesMenuBtn');
        if (btn) btn.addEventListener('click', (e) => { e.stopPropagation(); Palette.toggle({ scope: 'queries' }); });
        this.register();
        this.migrateLegacyRecent();
    },

    register() {
        const onQuery = ctx => ctx.view === 'search';
        const hl = text => (window.SyntaxHighlight ? SyntaxHighlight.highlight(text) : Utils.escapeHtml(text));
        const loadRun = (vars) => [
            { label: 'Load into editor', run: it => this.activate(it.row, vars(it), false) },
            { label: 'Run', run: it => this.activate(it.row, vars(it), true) },
        ];

        Palette.register({
            id: 'recent', group: 'Recent queries', hint: 'this fractal', order: 100, scope: 'queries',
            icon: Palette.ICONS.clock, fresh: true, search: true,
            when: ctx => ctx.hasScope,
            idle: ctx => (onQuery(ctx) ? this.IDLE_RECENT : 0),
            cached: () => this.readCache().map(r => this.historyItem(r, hl)),
            fetch: q => this.loadHistory(q).then(rows => rows.map(r => this.historyItem(r, hl))),
            preview: it => this.historyPreview(it.row, hl),
            actions: () => [
                ...loadRun(() => null),
                { label: 'Save as...', keepOpen: true, run: it => this.openSaveForm(it.row) },
                { label: 'Remove', keepOpen: true, danger: true, run: it => this.deleteHistory(it.row.id) },
            ],
        });

        Palette.register({
            id: 'saved', group: 'Saved', order: 110, scope: 'queries', fresh: true, search: true,
            when: ctx => ctx.hasScope,
            idle: ctx => (onQuery(ctx) ? this.IDLE_SAVED : 0),
            fetch: q => this.loadSaved(q).then(rows => rows.map(r => ({
                key: r.id,
                label: r.name,
                search: `${r.query_text} ${(r.tags || []).join(' ')} ${r.description || ''}`,
                icon: r.favorited ? Palette.ICONS.star : Palette.ICONS.bookmark,
                hint: r.visibility === 'personal' ? 'personal' : 'shared',
                row: r,
            }))),
            preview: it => this.savedPreview(it.row, hl),
            actions: it => [
                ...loadRun(i => i.row.variables),
                { label: 'Edit', keepOpen: true, run: i => this.openEditForm(i.row) },
                { label: it.row.favorited ? 'Unfavorite' : 'Favorite', keepOpen: true, run: i => this.toggleFavorite(i.row) },
                { label: 'Delete', keepOpen: true, danger: true, run: i => this.deleteSaved(i.row.id) },
            ],
        });

        const mod = Utils.isMac ? '⌘' : 'Ctrl';
        const actions = [
            { key: 'save', label: 'Save current query', hintHtml: `<kbd>${mod}</kbd><kbd>S</kbd>`, shortcut: 's', keepOpen: true, run: () => this.openSaveForm(null) },
            { key: 'share', label: 'Copy share link', run: () => window.QueryExecutor && QueryExecutor.generateAndCopyShareLink() },
            { key: 'notebook', label: 'Send query to notebook', run: () => window.NotebookRail && NotebookRail.captureCurrentQuery() },
            { key: 'clear', label: 'Clear query history', keepOpen: true, run: () => this.clearHistory() },
        ];
        Palette.register({
            id: 'query-actions', group: 'Actions', order: 120, scope: 'actions', icon: Palette.ICONS.bolt,
            when: onQuery,
            idle: () => 2,
            items: () => actions.map(a => ({ key: a.key, label: a.label, hintHtml: a.hintHtml, shortcut: a.shortcut, kind: 'Action', act: a })),
            actions: it => [{ label: 'Run action', keepOpen: it.act.keepOpen, run: () => it.act.run() }],
        });
    },

    historyItem(r, hl) {
        return {
            key: r.query_text,
            label: r.query_text,
            labelHtml: hl(r.query_text),
            mono: true,
            hint: this.relTime(r.last_run_at),
            row: r,
        };
    },

    historyPreview(r, hl) {
        const meta = [];
        if (r.last_run_at) meta.push(['Ran', this.relTime(r.last_run_at) + (r.run_count > 1 ? `, ${r.run_count} times` : '')]);
        if (r.time_range) meta.push(['Time range', this.rangeLabel(r)]);
        if (r.result_count != null) {
            meta.push(['Results', `${Number(r.result_count).toLocaleString()} rows${r.duration_ms != null ? ` in ${Number(r.duration_ms).toLocaleString()} ms` : ''}`]);
        }
        if (r.status === 'error') meta.push(['Status', 'Failed']);
        return `<div class="pal-pv-kind">Recent query</div>
            <pre class="pal-code pal-hl">${hl(r.query_text)}</pre>
            ${this.metaList(meta)}`;
    },

    savedPreview(r, hl) {
        const meta = [['Visibility', r.visibility === 'personal' ? 'Personal' : 'Shared']];
        if (r.created_by) meta.push(['Created by', r.created_by]);
        if (r.use_count) meta.push(['Used', `${r.use_count} time${r.use_count === 1 ? '' : 's'}${r.last_used_at ? `, last ${this.relTime(r.last_used_at)}` : ''}`]);
        if (r.time_range) meta.push(['Time range', this.rangeLabel(r)]);
        const tags = (r.tags || []).map(t => `<span class="pal-tag">${Utils.escapeHtml(t)}</span>`).join('');
        return `<div class="pal-pv-kind">Saved query</div>
            <div class="pal-pv-title">${Utils.escapeHtml(r.name)}</div>
            ${r.description ? `<div class="pal-pv-note">${Utils.escapeHtml(r.description)}</div>` : ''}
            <pre class="pal-code pal-hl">${hl(r.query_text)}</pre>
            ${tags ? `<div class="pal-tags">${tags}</div>` : ''}
            ${this.metaList(meta)}`;
    },

    metaList(rows) {
        if (!rows.length) return '';
        return `<dl class="pal-meta">${rows.map(([k, v]) => `<dt>${Utils.escapeHtml(k)}</dt><dd>${Utils.escapeHtml(v)}</dd>`).join('')}</dl>`;
    },

    async loadHistory(search) {
        let url = '/api/v1/query-history';
        if (search) url += '?search=' + encodeURIComponent(search);
        const resp = await fetch(url, { credentials: 'include' });
        const data = await resp.json();
        const rows = (data.success && data.data) || [];
        if (!search) this.writeCache(rows);
        return rows;
    },

    async loadSaved(search) {
        let url = '/api/v1/saved-queries';
        if (search) url += '?search=' + encodeURIComponent(search);
        const resp = await fetch(url, { credentials: 'include' });
        const data = await resp.json();
        return (data.success && data.data) || [];
    },

    reloadSource(id) {
        Palette.invalidate(id);
    },

    // run=true executes immediately; run=false loads into the editor for review.
    activate(row, variables, run) {
        // Both saved queries and history rows carry the time range they were run
        // with; restore it so the query executes against its own range.
        this.applyTimeRange(row);
        this.loadIntoInput(row.query_text, variables, run);
        if (run && row.use_count !== undefined) this.markUsed(row.id);
    },

    // Puts a query in the editor, moving to Query first when the palette was
    // opened on another page.
    loadIntoInput(text, variables, run) {
        if (window.App && !(App.currentViewLevel === 'fractal' && App.currentView === 'search')) {
            App.showFractalView('search');
        }
        const qi = document.getElementById('queryInput');
        if (!qi) return;
        qi.value = text;
        // Seed remembered variable values before dispatching input, so the tray's
        // reconcile picks them up instead of defaulting every @var to "*".
        if (variables && window.QueryExecutor && QueryExecutor.seedVariables) {
            QueryExecutor.seedVariables(variables);
        }
        setTimeout(() => {
            qi.dispatchEvent(new Event('input', { bubbles: true }));
            qi.focus();
            if (run) document.getElementById('executeBtn')?.click();
        }, 0);
    },

    // _variablesForQuery returns the [{name,value}] bindings to persist with a
    // query: the @names actually referenced, valued from prevValues (the editor's
    // current values on create, or the query's saved values on edit), default "*".
    _variablesForQuery(queryText, prevValues) {
        if (!window.VariableManager) return [];
        const prev = VariableManager._asMap(prevValues || []);
        return VariableManager.detectNames(queryText).map(name => ({
            name,
            value: prev.has(name) && prev.get(name) != null ? String(prev.get(name)) : '*'
        }));
    },

    applyTimeRange(it) {
        if (!it || !it.time_range || !window.TimePicker) return;
        TimePicker.applyState({
            type: it.time_range,
            customStart: it.custom_start,
            customEnd: it.custom_end,
            relativeN: it.relative_n,
            relativeUnit: it.relative_unit,
        });
    },

    // ----- Saved query forms -----

    formFields(v = {}) {
        const esc = s => Utils.escapeHtml(s || '');
        return `
            <div class="pal-form-row">
                <input name="name" class="pal-input grow" placeholder="Name" maxlength="255" value="${esc(v.name)}" />
                <input name="tags" class="pal-input" placeholder="Tags, comma-separated" value="${esc((v.tags || []).join(', '))}" />
            </div>
            <input name="description" class="pal-input" placeholder="Description (optional)" value="${esc(v.description)}" />
            ${v.editQuery ? `<textarea name="query" class="pal-input pal-textarea" placeholder="Query">${esc(v.query_text)}</textarea>` : ''}
            <label class="pal-check"><input type="checkbox" name="personal" ${v.visibility === 'personal' ? 'checked' : ''}/> Personal (only visible to you)</label>`;
    },

    readForm(form) {
        const f = name => form.elements[name];
        return {
            name: (f('name')?.value || '').trim(),
            description: (f('description')?.value || '').trim(),
            tags: (f('tags')?.value || '').split(',').map(t => t.trim()).filter(Boolean),
            visibility: f('personal')?.checked ? 'personal' : 'shared',
            query: f('query') ? f('query').value.trim() : null,
        };
    },

    // Saves the editor's query, or a history row's when promoting one.
    openSaveForm(historyRow) {
        const text = (historyRow ? historyRow.query_text : document.getElementById('queryInput')?.value || '').trim();
        if (!text) {
            if (window.Toast) Toast.show('No query to save', 'warning');
            return;
        }
        // Promoting carries the row's range; a normal save snapshots the picker.
        const range = historyRow ? this._rangeFields(historyRow) : (window.TimePicker ? TimePicker.serializeRange() : {});
        const hl = window.SyntaxHighlight ? SyntaxHighlight.highlight(text) : Utils.escapeHtml(text);
        const rangeText = this.rangeLabel(range);
        Palette.showPanel({
            title: 'Save query',
            submitLabel: 'Save',
            html: `<pre class="pal-code pal-hl">${hl}</pre>
                ${rangeText ? this.metaList([['Time range', rangeText]]) : ''}
                ${this.formFields()}`,
            onSubmit: form => this.submitSave(form, text, range, !!historyRow),
        });
    },

    async submitSave(form, queryText, range, promoted) {
        const v = this.readForm(form);
        if (!v.name) { if (window.Toast) Toast.show('Name is required', 'warning'); return false; }
        // A promoted history row's @vars are unrelated to the live editor, so
        // they default to "*"; a normal save reuses the editor's values.
        const prevVals = promoted
            ? []
            : ((window.QueryExecutor && QueryExecutor.varManager) ? QueryExecutor.varManager.serialize() : []);
        const variables = this._variablesForQuery(queryText, prevVals);
        const ok = await this.send('/api/v1/saved-queries', 'POST',
            { name: v.name, query_text: queryText, description: v.description, tags: v.tags, visibility: v.visibility, variables, ...(range || {}) },
            'Query saved', 'Failed to save query');
        if (ok) this.reloadSource('saved');
        return ok;
    },

    openEditForm(row) {
        Palette.showPanel({
            title: 'Edit saved query',
            submitLabel: 'Update',
            html: this.formFields({ ...row, editQuery: true }),
            onSubmit: form => this.submitEdit(form, row),
        });
    },

    async submitEdit(form, row) {
        const v = this.readForm(form);
        if (!v.name) { if (window.Toast) Toast.show('Name is required', 'warning'); return false; }
        if (!v.query) { if (window.Toast) Toast.show('Query text is required', 'warning'); return false; }
        const variables = this._variablesForQuery(v.query, row.variables);
        // Editing does not expose the time range, so the stored one is kept.
        const ok = await this.send(`/api/v1/saved-queries/${encodeURIComponent(row.id)}`, 'PUT',
            { name: v.name, query_text: v.query, description: v.description, tags: v.tags, visibility: v.visibility, variables, ...this._rangeFields(row) },
            'Query updated', 'Failed to update query');
        if (ok) this.reloadSource('saved');
        return ok;
    },

    async send(url, method, body, okMsg, failMsg) {
        try {
            const resp = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: body ? JSON.stringify(body) : undefined,
            });
            const data = await resp.json();
            if (!data.success) { if (window.Toast) Toast.show(data.error || failMsg, 'error'); return false; }
            if (okMsg && window.Toast) Toast.show(okMsg, 'success');
            return true;
        } catch (err) {
            console.error('[QueryPalette]', failMsg, err);
            if (window.Toast) Toast.show(failMsg, 'error');
            return false;
        }
    },

    async deleteSaved(id) {
        if (!confirm('Delete this saved query?')) return;
        if (await this.send(`/api/v1/saved-queries/${encodeURIComponent(id)}`, 'DELETE', null, 'Query deleted', 'Failed to delete query')) {
            this.reloadSource('saved');
        }
    },

    async toggleFavorite(row) {
        try {
            await fetch(`/api/v1/saved-queries/${encodeURIComponent(row.id)}/favorite`, {
                method: row.favorited ? 'DELETE' : 'POST',
                credentials: 'include'
            });
        } catch (err) { /* the reload shows the real state */ }
        // Reload so favorites sort to the top.
        this.reloadSource('saved');
    },

    async markUsed(id) {
        try { await fetch(`/api/v1/saved-queries/${encodeURIComponent(id)}/use`, { method: 'POST', credentials: 'include' }); } catch (err) { /* best-effort */ }
    },

    // ----- History actions / recording -----

    // Called by the query executor on finalize. Persists the run and updates the
    // local cache optimistically so reopening the palette is instant + fresh.
    async recordRun(meta) {
        if (!meta || !meta.query) return;
        const q = meta.query.trim();
        if (!q || q === '*') return;
        const row = {
            query_text: meta.query,
            time_range: meta.timeRange || '',
            custom_start: meta.customStart || '',
            custom_end: meta.customEnd || '',
            relative_n: meta.relativeN != null ? meta.relativeN : null,
            relative_unit: meta.relativeUnit || '',
            result_count: meta.resultCount != null ? meta.resultCount : null,
            duration_ms: meta.durationMs != null ? meta.durationMs : null,
            status: meta.status || 'ok'
        };
        try {
            await fetch('/api/v1/query-history', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(row)
            });
        } catch (err) { /* best-effort; cache still updated below */ }

        const list = this.readCache().filter(h => h.query_text !== meta.query);
        list.unshift({ ...row, id: 'local-' + Date.now(), run_count: 1, last_run_at: new Date().toISOString() });
        this.writeCache(list.slice(0, 50));
        Palette.invalidate('recent');
    },

    async deleteHistory(id) {
        if (!String(id).startsWith('local-')) {
            try { await fetch('/api/v1/query-history/' + encodeURIComponent(id), { method: 'DELETE', credentials: 'include' }); } catch (err) { /* ignore */ }
        }
        this.writeCache(this.readCache().filter(h => h.id !== id));
        this.reloadSource('recent');
    },

    async clearHistory() {
        if (!confirm('Clear all query history for this fractal?')) return;
        try { await fetch('/api/v1/query-history', { method: 'DELETE', credentials: 'include' }); } catch (err) { /* ignore */ }
        this.writeCache([]);
        this.reloadSource('recent');
    },

    // One-time import of the old localStorage recent queries into server history.
    async migrateLegacyRecent() {
        const FLAG = 'bifract_recent_queries_migrated';
        if (localStorage.getItem(FLAG)) return;
        localStorage.setItem(FLAG, '1');
        let legacy = [];
        try { legacy = JSON.parse(localStorage.getItem('bifract_recent_queries')) || []; } catch (e) { legacy = []; }
        if (!Array.isArray(legacy) || legacy.length === 0) return;
        // Record oldest first so the newest keeps the most recent last_run_at.
        for (let i = legacy.length - 1; i >= 0; i--) {
            const q = legacy[i];
            if (typeof q !== 'string' || !q.trim() || q.trim() === '*') continue;
            try {
                await fetch('/api/v1/query-history', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({ query_text: q, status: 'ok' })
                });
            } catch (e) { /* best-effort */ }
        }
        localStorage.removeItem('bifract_recent_queries');
    },

    // ----- Helpers -----

    historyCacheKey() {
        const scope = window.FractalContext?.currentFractal?.id || 'global';
        return 'bifract_query_history_' + scope;
    },
    readCache() {
        try { return JSON.parse(localStorage.getItem(this.historyCacheKey())) || []; } catch (e) { return []; }
    },
    writeCache(list) {
        try { localStorage.setItem(this.historyCacheKey(), JSON.stringify(list)); } catch (e) { /* quota */ }
    },

    relTime(iso) {
        const d = new Date(iso);
        if (isNaN(d.getTime())) return '';
        const s = (new Date().getTime() - d.getTime()) / 1000;
        if (s < 60) return 'just now';
        if (s < 3600) return Math.floor(s / 60) + 'm ago';
        if (s < 86400) return Math.floor(s / 3600) + 'h ago';
        if (s < 604800) return Math.floor(s / 86400) + 'd ago';
        return TZ.format(d, 'date');
    },

    // _rangeFields extracts the persisted snake_case time-range fields from a row
    // (saved query or history entry) into the shape the API expects.
    _rangeFields(src) {
        if (!src || !src.time_range) return {};
        return {
            time_range: src.time_range,
            custom_start: src.custom_start || '',
            custom_end: src.custom_end || '',
            relative_n: src.relative_n != null ? src.relative_n : null,
            relative_unit: src.relative_unit || '',
        };
    },

    // rangeLabel renders a short human label for a stored time range, covering
    // presets, relative (n + unit) and custom (absolute date span).
    rangeLabel(it) {
        const t = it && it.time_range;
        if (!t) return '';
        if (t === 'relative') {
            const u = TimePicker._unitShort[it.relative_unit] || (it.relative_unit ? it.relative_unit[0] : 'h');
            return `Last ${it.relative_n || 1}${u}`;
        }
        if (t === 'custom') {
            const fmt = d => { const p = TZ.parts(d); return p ? `${TZ.MONTHS[p.month - 1]} ${p.day}` : ''; };
            if (it.custom_start && it.custom_end) return `${fmt(it.custom_start)} to ${fmt(it.custom_end)}`;
            return 'Custom';
        }
        if (t === 'all') return 'All time';
        if (t === 'live') return 'Live';
        if (TimePicker.isCalendar(t)) return TimePicker.CALENDAR_LABELS[t];
        return TimePicker.isPreset(t) ? `Last ${t}` : t;
    },
};

window.QueryPalette = QueryPalette;
