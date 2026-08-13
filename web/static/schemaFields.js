// Schema Fields admin tab.
//
// One unified table: configured and unconfigured fields are the same rows,
// distinguished by a Status column and a Recommendation verdict, filtered rather
// than split into sections. That keeps the page a fixed height whether the
// deployment has 30 fields or 1,000.
//
// Everything renders from a single /insights call, which is a pure Postgres read
// of whatever the background schema sweep last measured: coverage, cardinality,
// storage, column capacity, and how often each field is referenced by saved BQL.
// Nothing on this page waits on ClickHouse, so it loads at the same speed on an
// empty install and on a cluster holding billions of rows.
const SchemaFields = {
    fields: [],
    capacity: null,
    sampleSize: 0,
    totalBytes: 0,
    fractals: 0,
    computedAt: null,
    stale: false,
    intervalSecs: 0,
    selected: new Set(),
    page: 0,
    pageSize: 25,
    sortKey: 'verdict',
    sortDir: 1,
    _pollTimer: null,
    _measureTimer: null,
    _measureTries: 0,

    // Closed verdict set, in priority order. Rank doubles as the default sort,
    // so the top of the table is the worklist without a separate section.
    VERDICT: {
        urgent:  { label: 'Reserve now', cls: 'v-urgent',  rank: 0 },
        reserve: { label: 'Reserve',     cls: 'v-reserve', rank: 1 },
        index:   { label: 'Add index',   cls: 'v-index',   rank: 2 },
        unused:  { label: 'Unused',      cls: 'v-unused',  rank: 3 },
        keep:    { label: 'Keep',        cls: 'v-keep',    rank: 4 },
        none:    { label: '—',      cls: 'v-none',    rank: 5 },
    },
    QUERIED: ['Never', 'Rarely', 'Sometimes', 'Often'],

    init() {
        document.getElementById('schemaFieldAddBtn')?.addEventListener('click', () => this.openAddDrawer());
        document.getElementById('schemaFieldResetBtn')?.addEventListener('click', () => this.openResetModal());
        document.getElementById('schemaExportBtn')?.addEventListener('click', () => this.exportYaml());
        document.getElementById('schemaImportBtn')?.addEventListener('click', () => document.getElementById('schemaImportInput')?.click());
        document.getElementById('schemaImportInput')?.addEventListener('change', e => this.importYaml(e));
        ['schemaQ', 'schemaFVerdict', 'schemaFStatus'].forEach(id =>
            document.getElementById(id)?.addEventListener('input', () => { this.page = 0; this.render(); }));

        document.getElementById('schemaTbody')?.addEventListener('click', e => this._onRowClick(e));
        document.getElementById('schemaChkAll')?.addEventListener('change', e => this._onSelectAll(e));
        document.getElementById('schemaBulkClear')?.addEventListener('click', () => { this.selected.clear(); this.render(); });
        document.getElementById('schemaBulkApply')?.addEventListener('click', () => this.applySelected());
        document.getElementById('schemaPgPrev')?.addEventListener('click', () => { this.page--; this.render(); });
        document.getElementById('schemaPgNext')?.addEventListener('click', () => { this.page++; this.render(); });

        document.querySelectorAll('.schema-sortable').forEach(th =>
            th.addEventListener('click', () => this._onSort(th)));

        document.getElementById('schemaScrim')?.addEventListener('click', () => this.closeDrawer());
        document.addEventListener('keydown', e => {
            if (e.key === 'Escape' && document.getElementById('schemaDrawer')?.classList.contains('open')) {
                this.closeDrawer();
            }
        });

        document.getElementById('schemaRefreshBtn')?.addEventListener('click', () => this.refreshMeasurements());

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
        try {
            const res = await HttpUtils.safeFetch('/api/v1/admin/schema-fields/insights');
            const d = res.data || {};
            this.fields = d.fields || [];
            this.capacity = d.capacity || null;
            this.sampleSize = d.sample_size || 0;
            this.totalBytes = d.total_bytes || 0;
            this.fractals = d.fractals || 0;
            this.computedAt = d.computed_at || null;
            this.stale = !!d.stale;
            this.intervalSecs = d.interval_secs || 0;
            if (this._awaitingSince !== undefined && this.computedAt !== this._awaitingSince) {
                this._awaitingSince = undefined;
                this._measureTries = 0;
                if (window.Toast) Toast.success('Schema measured', 'Field statistics and capacity are up to date.');
            }
        } catch (err) {
            const tbody = document.getElementById('schemaTbody');
            if (tbody) {
                tbody.innerHTML = `<tr><td colspan="9"><div class="schema-empty">
                    Could not read field statistics. ${this.escHtml(err.message)}</div></td></tr>`;
            }
            return;
        }
        this.render();
        this._scheduleStatusPoll();
        this._scheduleMeasurePoll();
    },

    _visible() {
        return document.getElementById('mainSchemaTabContent')?.offsetParent !== null;
    },

    // Poll while a field is still applying so its status resolves without a manual
    // refresh. This hits the plain field list, not /insights: sync status is
    // configuration, and re-reading every measurement every few seconds to learn
    // whether one ALTER finished is exactly the kind of load this redesign removes.
    _scheduleStatusPoll() {
        if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }
        if (!this.fields.some(f => f.sync_status === 'pending') || !this._visible()) return;
        this._pollTimer = setTimeout(() => this._pollSyncStatus(), 2500);
    },

    async _pollSyncStatus() {
        try {
            const res = await HttpUtils.safeFetch('/api/v1/admin/schema-fields');
            const custom = new Map((res.data?.custom || []).map(f => [f.field_name, f]));
            let changed = false;
            this.fields.forEach(f => {
                const c = custom.get(f.name);
                if (!c) return;
                if (f.sync_status !== c.sync_status || f.sync_error !== (c.sync_error || '')) {
                    f.sync_status = c.sync_status;
                    f.sync_error = c.sync_error || '';
                    changed = true;
                }
            });
            if (changed) this.render();
        } catch (err) {
            console.warn('schema sync poll failed:', err.message);
        }
        this._scheduleStatusPoll();
    },

    // Wait for a sweep that has been asked for, or has never run. Bounded: after
    // maxTries the page stops asking rather than polling an install whose sweep is
    // failing, and the freshness line still says what it knows.
    _scheduleMeasurePoll() {
        if (this._measureTimer) { clearTimeout(this._measureTimer); this._measureTimer = null; }
        const waiting = !this.computedAt || this._awaitingSince !== undefined;
        if (!waiting || !this._visible() || this._measureTries >= 30) return;
        this._measureTries++;
        this._measureTimer = setTimeout(() => this.load(), 8000);
    },

    // Ask the background sweep to re-measure. The request returns as soon as the
    // sweep is queued; the page then polls for a newer measurement rather than
    // holding a request open for a job that legitimately takes a while.
    async refreshMeasurements() {
        const btn = document.getElementById('schemaRefreshBtn');
        try {
            await HttpUtils.safeFetch('/api/v1/admin/schema-fields/refresh', { method: 'POST' });
        } catch (err) {
            if (window.Toast) Toast.error('Could not start measurement', err.message);
            return;
        }
        this._awaitingSince = this.computedAt;
        this._measureTries = 0;
        if (btn) { btn.disabled = true; btn.textContent = 'Measuring...'; }
        this._renderMeta();
        this._scheduleMeasurePoll();
    },

    // ---- Derived state ------------------------------------------------------

    isReserved(f) { return f.status === 'builtin' || f.status === 'custom'; },
    // Unaddable names are listed (so an overflowing field stays findable) but
    // cannot be acted on, so they are never selectable for a bulk reserve.
    isSelectable(f) { return !this.isReserved(f) && f.status !== 'ignored' && f.addable !== false; },

    filtered() {
        const q = (document.getElementById('schemaQ')?.value || '').trim().toLowerCase();
        const fv = document.getElementById('schemaFVerdict')?.value || '';
        const fs = document.getElementById('schemaFStatus')?.value || '';
        const out = this.fields.filter(f => {
            if (q && !f.name.toLowerCase().includes(q)) return false;
            if (fv) {
                // "reserve" covers the urgent variant: same action, different urgency.
                const v = f.verdict === 'urgent' ? 'reserve' : f.verdict;
                if (v !== fv) return false;
            }
            if (fs === 'reserved' && !this.isReserved(f)) return false;
            if (fs === 'unreserved' && (this.isReserved(f) || f.status === 'ignored')) return false;
            if (fs === 'ignored' && f.status !== 'ignored') return false;
            return true;
        });

        const key = this.sortKey, dir = this.sortDir, V = this.VERDICT;
        out.sort((a, b) => {
            let d;
            if (key === 'name') d = a.name.localeCompare(b.name);
            else if (key === 'verdict') {
                d = (V[a.verdict]?.rank ?? 9) - (V[b.verdict]?.rank ?? 9);
                if (d === 0) d = (b.query_refs - a.query_refs) || (b.coverage - a.coverage);
            } else d = (a[key] || 0) - (b[key] || 0);
            return d * dir;
        });
        return out;
    },

    // ---- Render -------------------------------------------------------------

    render() {
        const list = this.filtered();
        const pages = Math.max(1, Math.ceil(list.length / this.pageSize));
        if (this.page >= pages) this.page = pages - 1;
        if (this.page < 0) this.page = 0;
        const slice = list.slice(this.page * this.pageSize, (this.page + 1) * this.pageSize);

        const tbody = document.getElementById('schemaTbody');
        if (tbody) {
            tbody.innerHTML = slice.length
                ? slice.map(f => this._row(f)).join('')
                : `<tr><td colspan="9"><div class="schema-empty">${
                    this.fields.length ? 'No fields match these filters.'
                        : !this.computedAt ? 'Measuring your schema. Fields appear here once the first measurement completes.'
                        : 'No fields yet. Suggestions appear once Bifract has ingested enough data.'
                  }</div></td></tr>`;
        }

        const rc = document.getElementById('schemaResultCount');
        if (rc) rc.textContent = `${list.length} of ${this.fields.length} fields`;
        const pg = document.getElementById('schemaPgInfo');
        if (pg) {
            pg.textContent = list.length
                ? `${this.page * this.pageSize + 1}–${Math.min((this.page + 1) * this.pageSize, list.length)} of ${list.length}`
                : '0 of 0';
        }
        document.getElementById('schemaPgPrev').disabled = this.page === 0;
        document.getElementById('schemaPgNext').disabled = this.page >= pages - 1;

        // Active filters get an accent border so narrowing is never invisible.
        document.getElementById('schemaWrapVerdict')?.classList
            .toggle('active', !!document.getElementById('schemaFVerdict')?.value);
        document.getElementById('schemaWrapStatus')?.classList
            .toggle('active', !!document.getElementById('schemaFStatus')?.value);

        this._renderBulk();
        this._renderCapacity();
        this._renderMeta();
    },

    // States the page can be in, in one line: never measured, measuring now,
    // measured recently, or overdue. Silence would read as "no fields exist".
    _renderMeta() {
        const el = document.getElementById('schemaMetaText');
        const btn = document.getElementById('schemaRefreshBtn');
        if (!el) return;

        const measuring = this._awaitingSince !== undefined || !this.computedAt;
        if (btn) {
            btn.disabled = measuring;
            btn.textContent = measuring ? 'Measuring...' : 'Refresh';
        }

        if (!this.computedAt) {
            el.classList.remove('stale');
            el.textContent = 'Measuring your schema. This runs in the background and appears here when it finishes.';
            return;
        }

        const parts = [`Measured ${this._ago(this.computedAt)}`];
        if (this.sampleSize) {
            parts.push(`${this.sampleSize.toLocaleString()} logs sampled`
                + (this.fractals > 1 ? ` across ${this.fractals} fractals` : ''));
        }
        if (this.totalBytes) parts.push(`${this._bytes(this.totalBytes)} of fields on disk`);
        el.textContent = parts.join(' · ');
        el.classList.toggle('stale', this.stale);
        if (this.stale) {
            el.title = 'The background measurement has not completed recently. Check the server logs.';
        } else {
            el.removeAttribute('title');
        }
    },

    _ago(iso) {
        const secs = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
        if (secs < 90) return 'just now';
        const mins = Math.round(secs / 60);
        if (mins < 60) return `${mins} minutes ago`;
        const hrs = Math.round(mins / 60);
        if (hrs < 48) return `${hrs} hour${hrs === 1 ? '' : 's'} ago`;
        return `${Math.round(hrs / 24)} days ago`;
    },

    _bytes(n) {
        if (!n) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
        let i = 0;
        while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
        return `${n < 10 && i > 0 ? n.toFixed(1) : Math.round(n)} ${units[i]}`;
    },

    _row(f) {
        const v = this.VERDICT[f.verdict] || this.VERDICT.none;
        const name = this.escHtml(f.name);
        const status = f.status === 'builtin'
            ? '<span class="schema-status-res">Reserved</span><span class="schema-builtin">built in</span>'
            : f.status === 'custom' ? '<span class="schema-status-res">Reserved</span>'
            : f.status === 'ignored' ? '<span class="schema-muted">Ignored</span>'
            : '<span class="schema-muted">Not reserved</span>';

        let idx = '<span class="schema-muted">—</span>';
        if (f.index_type && f.index_type !== 'none') {
            idx = `<span class="mono schema-idx">${f.index_type === 'bloom_filter' ? 'bloom' : this.escHtml(f.index_type)}</span>`;
        }
        // A failed index is the one thing that must not hide behind a dash.
        if (f.sync_status === 'error') {
            idx = `<span class="schema-idxstate s-err" title="${this.escHtml(f.sync_error || '')}">Index failed</span>`;
        } else if (f.sync_status === 'pending') {
            idx = '<span class="schema-idxstate s-wait">Applying</span>';
        }

        const cov = f.coverage
            ? `<span class="schema-minibar"><i style="width:${Math.round(f.coverage * 100)}%"></i></span>${this._pct(f.coverage)}`
            : '<span class="schema-muted">—</span>';

        const box = this.isSelectable(f)
            ? `<label class="schema-box"><input type="checkbox" ${this.selected.has(f.name) ? 'checked' : ''} aria-label="Select ${name}"><span></span></label>`
            : '';

        return `<tr data-n="${name}" class="${this.selected.has(f.name) ? 'sel' : ''}${f.verdict === 'urgent' ? ' urgent' : ''}">
            <td class="schema-chk">${box}</td>
            <td class="schema-f">${name}</td>
            <td class="schema-n">${cov}</td>
            <td class="schema-n">${f.cardinality ? this._count(f.cardinality) : '<span class="schema-muted">—</span>'}</td>
            <td class="schema-size">${f.bytes_on_disk ? this._bytes(f.bytes_on_disk) : '<span class="schema-muted">—</span>'}</td>
            <td>${f.queried ? this.QUERIED[f.queried] : '<span class="schema-muted">Never</span>'}</td>
            <td>${status}</td>
            <td>${idx}</td>
            <td><span class="schema-v ${v.cls}">${v.label}</span></td>
        </tr>`;
    },

    _renderBulk() {
        const bar = document.getElementById('schemaBulkBar');
        if (!bar) return;
        const n = this.selected.size;
        bar.hidden = n === 0;
        if (!n) return;
        document.getElementById('schemaBulkN').textContent = `${n} field${n === 1 ? '' : 's'} selected`;
        const cap = this.capacity;
        const freed = this._freedBySelection();
        document.getElementById('schemaBulkImpact').textContent = cap
            ? `Dynamic columns ${cap.dynamic_used.toLocaleString()} → ${Math.max(cap.dynamic_used - freed, 0).toLocaleString()} of ${cap.limit.toLocaleString()}`
            : '';
    },

    // Reserving a field that is already in the data frees a dynamic slot rather
    // than consuming one: a type-hinted path always gets its own sub-column and
    // sits outside the max_dynamic_paths budget. A selected field absent from the
    // data frees nothing, since it holds no dynamic path today.
    _freedBySelection() {
        let freed = 0;
        this.selected.forEach(name => {
            const f = this.fields.find(x => x.name === name);
            if (f && f.present > 0 && !this.isReserved(f)) freed++;
        });
        return freed;
    },

    _renderCapacity() {
        const box = document.getElementById('schemaCapacity');
        const cap = this.capacity;
        if (!box) return;
        if (!cap) { box.hidden = true; return; }
        box.hidden = false;

        const limit = cap.limit || 1024;
        const used = cap.dynamic_used || 0;
        // The budget governs dynamic paths only. Reserved fields are reported
        // beside the bar rather than inside it, because they are not competing
        // for these slots.
        const freed = Math.min(this._freedBySelection(), used);
        document.getElementById('schemaCapUsed').textContent = used.toLocaleString();
        document.getElementById('schemaCapLimit').textContent = limit.toLocaleString();
        document.getElementById('schemaSegDyn').style.width = `${Math.min((used - freed) / limit * 100, 100)}%`;
        // Ghost segment: the slots the pending selection would give back, so the
        // effect of the action is visible before committing to it.
        const proj = document.getElementById('schemaSegProj');
        // Hidden rather than zero-width: the bar is a gapped flex row, so a
        // zero-width segment still leaves a sliver.
        proj.hidden = freed === 0;
        proj.style.width = `${Math.min(freed / limit * 100, 100)}%`;
        document.getElementById('schemaCapProj').innerHTML = freed
            ? `<span class="schema-cap-proj">-${freed} pending</span>` : '';
        document.getElementById('schemaCapReserved').textContent =
            `${(cap.reserved || 0).toLocaleString()} reserved`;

        // Terse by design: the strip is one line, and the explanation belongs in
        // the drawer for the specific field, not repeated in the chrome.
        const warn = document.getElementById('schemaCapWarn');
        const over = cap.overflowed || [];
        if (over.length) {
            warn.hidden = false;
            warn.innerHTML = `<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" aria-hidden="true">
                <path d="M8 5.2v3.4"></path><circle cx="8" cy="11.2" r=".5" fill="currentColor" stroke="none"></circle>
                <path d="M6.9 2.3 1.4 12.1a1.2 1.2 0 0 0 1.1 1.8h11a1.2 1.2 0 0 0 1.1-1.8L9.1 2.3a1.2 1.2 0 0 0-2.2 0Z"></path>
                </svg> ${over.length} out of capacity`;
            warn.title = 'These fields no longer have a column of their own, so queries on them scan every row.';
        } else {
            warn.hidden = true;
        }
    },

    // ---- Interaction --------------------------------------------------------

    _onRowClick(e) {
        const tr = e.target.closest('tr[data-n]');
        if (!tr) return;
        const name = tr.dataset.n;
        const box = e.target.closest('input[type=checkbox]');
        if (box) {
            box.checked ? this.selected.add(name) : this.selected.delete(name);
            this.render();
            return;
        }
        // Clicking the checkbox label must not also open the drawer.
        if (e.target.closest('.schema-box')) return;
        this.openDrawer(name);
    },

    _onSelectAll(e) {
        const shown = this.filtered()
            .slice(this.page * this.pageSize, (this.page + 1) * this.pageSize)
            .filter(f => this.isSelectable(f));
        shown.forEach(f => e.target.checked ? this.selected.add(f.name) : this.selected.delete(f.name));
        this.render();
    },

    _onSort(th) {
        const k = th.dataset.sort;
        if (this.sortKey === k) this.sortDir *= -1;
        else { this.sortKey = k; this.sortDir = (k === 'name' || k === 'verdict') ? 1 : -1; }
        document.querySelectorAll('.schema-sortable').forEach(t => t.removeAttribute('aria-sort'));
        th.setAttribute('aria-sort', this.sortDir === 1 ? 'ascending' : 'descending');
        this.render();
    },

    // ---- Drawer -------------------------------------------------------------

    openDrawer(name) {
        const f = this.fields.find(x => x.name === name);
        const drawer = document.getElementById('schemaDrawer');
        if (!f || !drawer) return;
        const v = this.VERDICT[f.verdict] || this.VERDICT.none;

        let WHY = {
            urgent: ['warn', 'Out of column capacity, so every query touching this field scans all rows. Reserving it restores a dedicated column permanently.'],
            reserve: ['', 'Queried often or present in most logs. Reserving it keeps queries fast as your data grows, and applies to logs you already have.'],
            index: ['', 'Already reserved and queried often, but it has no skip index. Adding one prunes granules on newly ingested logs.'],
            unused: ['', 'Reserved but never seen in your logs. It costs nothing to keep, but you can free the column if you never expect this field.'],
            keep: ['', 'Configured appropriately for how it is used. No change recommended.'],
            none: ['', 'Not queried and not at risk. No action needed.'],
        }[f.verdict] || ['', ''];

        // A name ClickHouse accepts but Bifract does not. Say so here, where the
        // Reserve button is, rather than in the page chrome.
        if (f.addable === false && !this.isReserved(f)) {
            WHY = ['warn', WHY[1] + ' This name cannot be reserved: Bifract field names may only contain ' +
                'letters, digits, and underscores. Rename it in the normalizer that produces it.'];
        }

        const refs = (f.refs || []).length
            ? `<div class="schema-refs">${f.refs.map(r =>
                `<span class="schema-ref"><span class="kind">${this.escHtml(r.kind)}</span><span class="t">${this.escHtml(r.title)}</span></span>`).join('')}</div>`
            : '<p class="schema-muted schema-drawer-none">Nothing queries this field yet.</p>';

        const maxTop = (f.top || []).reduce((m, t) => Math.max(m, t.count), 0) || 1;
        // An approximate count comes from a bounded-memory estimator, which only
        // happens on fields with too many distinct values for a share to mean
        // much. Marking it beats printing an estimate as though it were counted.
        const top = (f.top || []).length
            ? `<h4>Most common values</h4><div class="schema-topvals">${f.top.map(t =>
                `<span class="schema-tv"><span class="val">${this.escHtml(t.value)}</span>
                 <span class="tvbar"><i style="width:${Math.round(t.count / maxTop * 100)}%"></i></span>
                 <span class="pc">${t.approx ? '~' : ''}${this._pct(f.present ? t.count / f.present : 0)}</span></span>`).join('')}</div>`
            : '';

        const canReserve = this.isSelectable(f);
        drawer.innerHTML = `
            <div class="schema-drawer-head">
                <div>
                    <div class="schema-drawer-name" id="schemaDrawerName">${this.escHtml(f.name)}</div>
                    <span class="schema-v ${v.cls}" style="margin-top:7px">${v.label}</span>
                </div>
                <button class="schema-drawer-close" aria-label="Close">&times;</button>
            </div>
            <p class="schema-why ${WHY[0]}">${WHY[1]}</p>
            <h4>In your logs</h4>
            <dl class="schema-kv">
                <dt>Present in</dt><dd>${f.coverage ? this._pct(f.coverage) + ' of sampled logs' : 'not seen'}</dd>
                <dt>Distinct values</dt><dd>${f.cardinality ? f.cardinality.toLocaleString() : '—'}</dd>
                <dt>Storage</dt><dd>${f.bytes_on_disk
                    ? `${this._bytes(f.bytes_on_disk)}${this.totalBytes ? ` · ${this._pct(f.bytes_on_disk / this.totalBytes)} of fields` : ''}`
                    : (this.isReserved(f) ? '—' : 'measured once reserved')}</dd>
                <dt>Queried</dt><dd>${this.QUERIED[f.queried] || 'Never'}</dd>
                <dt>Skip index</dt><dd>${f.index_type && f.index_type !== 'none' ? this.escHtml(f.index_type) : 'none'}</dd>
            </dl>
            ${top}
            <h4>What references this field</h4>
            ${refs}
            ${f.sync_error ? `<h4>Last error</h4><p class="schema-why warn">${this.escHtml(f.sync_error)}</p>` : ''}
            <div class="schema-drawer-actions">
                ${canReserve ? `<button class="btn-primary btn-sm" data-act="reserve">Reserve field</button>` : ''}
                ${canReserve ? `<button class="btn-secondary btn-sm" data-act="ignore">Ignore</button>` : ''}
                ${f.status === 'ignored' ? `<button class="btn-secondary btn-sm" data-act="restore">Restore</button>` : ''}
                ${f.status === 'custom' ? `<button class="btn-secondary btn-sm" data-act="remove">Remove</button>` : ''}
            </div>`;

        drawer.classList.add('open');
        drawer.setAttribute('aria-hidden', 'false');
        document.getElementById('schemaScrim').classList.add('open');
        drawer.querySelector('.schema-drawer-close')?.addEventListener('click', () => this.closeDrawer());
        drawer.querySelectorAll('.schema-drawer-actions [data-act]').forEach(b =>
            b.addEventListener('click', () => this._drawerAction(f, b.dataset.act)));
    },

    closeDrawer() {
        const d = document.getElementById('schemaDrawer');
        d?.classList.remove('open');
        d?.setAttribute('aria-hidden', 'true');
        document.getElementById('schemaScrim')?.classList.remove('open');
    },

    async _drawerAction(f, act) {
        this.closeDrawer();
        if (act === 'reserve') await this.addField(f.name, f.recommended_index || 'none');
        else if (act === 'ignore') await this.ignoreField(f.name);
        else if (act === 'restore') await this.unignoreField(f.name);
        else if (act === 'remove') await this.deleteField(f.name);
    },

    // ---- Mutations ----------------------------------------------------------

    async addField(name, indexType) {
        try {
            await HttpUtils.safeFetch('/api/v1/admin/schema-fields', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ field_name: name, index_type: indexType || 'none' }),
            });
            if (window.Toast) {
                Toast.success('Field reserved', (!indexType || indexType === 'none')
                    ? `"${name}" now has its own column, including for logs you already have.`
                    : `"${name}" now has its own column. Its ${indexType === 'set' ? 'set' : 'bloom filter'} index applies to newly ingested logs.`);
            }
            this.selected.delete(name);
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Could not reserve field', err.message);
        }
    },

    // Sequential, not parallel: each add triggers a ClickHouse reconcile and
    // those are serialized server-side anyway.
    async applySelected() {
        const names = [...this.selected];
        if (!names.length) return;
        const cap = this.capacity;
        const freed = this._freedBySelection();
        const impact = cap
            ? `\n\nDynamic columns ${cap.dynamic_used.toLocaleString()} → ${Math.max(cap.dynamic_used - freed, 0).toLocaleString()} of ${cap.limit.toLocaleString()}.`
            : '';
        if (!confirm(`Reserve ${names.length} field${names.length === 1 ? '' : 's'}?${impact}\n\nApplies to logs you already have. No data is rewritten.`)) return;

        const btn = document.getElementById('schemaBulkApply');
        if (btn) { btn.disabled = true; btn.textContent = 'Reserving...'; }
        let ok = 0;
        for (const n of names) {
            const f = this.fields.find(x => x.name === n);
            try {
                await HttpUtils.safeFetch('/api/v1/admin/schema-fields', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ field_name: n, index_type: f?.recommended_index || 'none' }),
                });
                ok++;
            } catch (err) {
                console.warn('reserve failed:', n, err.message);
            }
        }
        if (btn) { btn.disabled = false; btn.textContent = 'Reserve selected'; }
        if (window.Toast) {
            if (ok === names.length) Toast.success('Fields reserved', `${ok} field${ok === 1 ? '' : 's'} now ${ok === 1 ? 'has its' : 'have their'} own column.`);
            else Toast.error('Some fields could not be reserved', `${ok} of ${names.length} succeeded.`);
        }
        this.selected.clear();
        this.load();
    },

    async ignoreField(name) {
        try {
            await HttpUtils.safeFetch(`/api/v1/admin/schema-fields/ignore/${encodeURIComponent(name)}`, { method: 'POST' });
            if (window.Toast) Toast.success('Field ignored', `"${name}" will not be recommended again. Filter to Ignored to restore it.`);
            this.selected.delete(name);
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Could not ignore field', err.message);
        }
    },

    async unignoreField(name) {
        try {
            await HttpUtils.safeFetch(`/api/v1/admin/schema-fields/ignore/${encodeURIComponent(name)}`, { method: 'DELETE' });
            if (window.Toast) Toast.success('Field restored', `"${name}" can be recommended again.`);
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Could not restore field', err.message);
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

    // ---- Add field ----------------------------------------------------------
    //
    // Reuses the detail drawer rather than an inline form or a modal: the table
    // is the page, and a second surface for "one more field" would either push
    // the table down on every open or block it entirely. Manual add stays
    // necessary because a field that has not appeared in the logs yet cannot be
    // discovered from the data.

    openAddDrawer() {
        const drawer = document.getElementById('schemaDrawer');
        if (!drawer) return;
        drawer.innerHTML = `
            <div class="schema-drawer-head">
                <div class="schema-drawer-name" id="schemaDrawerName">Add field</div>
                <button class="schema-drawer-close" aria-label="Close">&times;</button>
            </div>
            <p class="schema-why">Reserve a field that has not appeared in your logs yet.
               Fields already present are listed in the table, where you can reserve them directly.</p>
            <div class="schema-form">
                <label for="schemaFieldName">Field name</label>
                <input type="text" id="schemaFieldName" placeholder="e.g. tenant_id" autocomplete="off" spellcheck="false">
                <p class="schema-form-hint">Letters, digits, and underscores. Must start with a letter or underscore.</p>

                <label for="schemaFieldIndexType">Skip index</label>
                <span class="schema-sel schema-sel-block"><select id="schemaFieldIndexType">
                    <option value="none">No index</option>
                    <option value="set">Set: under a few hundred distinct values</option>
                    <option value="bloom_filter">Bloom filter: many distinct values</option>
                </select></span>
                <p class="schema-form-hint">Only speeds up logs ingested from now on. Leave off unless you filter on this field.</p>
            </div>
            <div class="schema-drawer-actions">
                <button class="btn-primary btn-sm" id="schemaFieldSaveBtn">Add field</button>
                <button class="btn-secondary btn-sm" id="schemaFieldCancelBtn">Cancel</button>
            </div>`;
        drawer.classList.add('open');
        drawer.setAttribute('aria-hidden', 'false');
        document.getElementById('schemaScrim').classList.add('open');

        drawer.querySelector('.schema-drawer-close')?.addEventListener('click', () => this.closeDrawer());
        document.getElementById('schemaFieldCancelBtn')?.addEventListener('click', () => this.closeDrawer());
        document.getElementById('schemaFieldSaveBtn')?.addEventListener('click', () => this.saveField());
        const input = document.getElementById('schemaFieldName');
        input?.addEventListener('keydown', e => { if (e.key === 'Enter') this.saveField(); });
        setTimeout(() => input?.focus(), 60);
    },

    async saveField() {
        const name = document.getElementById('schemaFieldName')?.value.trim();
        const indexType = document.getElementById('schemaFieldIndexType')?.value;
        if (!name) {
            if (window.Toast) Toast.error('Field name required', 'Enter the field name as it appears in your logs.');
            document.getElementById('schemaFieldName')?.focus();
            return;
        }
        this.closeDrawer();
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
        const match = e.target.value === 'DELETE ALL LOG DATA';
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
        const count = this.fields.filter(f => f.status === 'custom').length;
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

    _pct(v) {
        const pct = (v || 0) * 100;
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
