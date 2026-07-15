// Recall module for Bifract.
//
// Per-fractal async BQL search over the Iceberg archive (cold storage). The tab
// looks like a normal search but every run is a server-side job: submit, then
// poll the job until it terminates, rendering results inline. Jobs are durable
// server-side, so navigating away (or refreshing) never cancels one; on re-entry
// we reattach to the latest job (or the job id carried in the URL hash).

// Independent ingest-time window controller for Recall. Mirrors the main Query
// TimePicker's UX/behavior but is a SEPARATE instance with Recall-prefixed
// element ids and its OWN state, so selecting a Recall window never touches the
// main Query event-time window. Resolves to UTC ISO8601 from/to. Applying a
// selection does NOT auto-run a search (Recall jobs are heavy, server-side); the
// user runs explicitly with the Recall button.
const RecallTimePicker = {
    state: {
        type: '30d',       // preset key | 'relative' | 'custom' | 'all'
        relN: 30,
        relUnit: 'days',
        absStart: null,    // UTC ISO8601
        absEnd: null,      // UTC ISO8601
    },

    _presetMs: {
        '24h': 24 * 60 * 60 * 1000,
        '7d':  7 * 24 * 60 * 60 * 1000,
        '30d': 30 * 24 * 60 * 60 * 1000,
        '90d': 90 * 24 * 60 * 60 * 1000,
        '1y':  365 * 24 * 60 * 60 * 1000,
    },

    _unitMs: {
        minutes: 60 * 1000,
        hours:   60 * 60 * 1000,
        days:    24 * 60 * 60 * 1000,
        weeks:   7 * 24 * 60 * 60 * 1000,
    },

    // Resolve the current selection to { from, to } UTC ISO8601 strings.
    resolve() {
        const now = Date.now();
        const iso = ms => new Date(ms).toISOString();
        const { type, relN, relUnit, absStart, absEnd } = this.state;
        if (type === 'custom') {
            if (absStart && absEnd) return { from: absStart, to: absEnd };
            return { from: iso(now - this._presetMs['30d']), to: iso(now) };
        }
        if (type === 'relative') {
            const span = relN * (this._unitMs[relUnit] || this._unitMs.days);
            return { from: iso(now - span), to: iso(now) };
        }
        if (type === 'all') {
            return { from: '2000-01-01T00:00:00.000Z', to: iso(now) };
        }
        const span = this._presetMs[type] || this._presetMs['30d'];
        return { from: iso(now - span), to: iso(now) };
    },

    getLabel() {
        const { type, relN, relUnit, absStart, absEnd } = this.state;
        const unitShort = { minutes: 'm', hours: 'h', days: 'd', weeks: 'w' };
        if (type === 'all') return 'Ingest: all';
        if (type === 'relative') return `Ingest: last ${relN}${unitShort[relUnit] || relUnit[0]}`;
        if (type === 'custom') {
            if (absStart && absEnd) {
                const fmt = d => new Date(d).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
                return `Ingest: ${fmt(absStart)} – ${fmt(absEnd)}`;
            }
            return 'Ingest: custom';
        }
        return `Ingest: last ${type}`;
    },

    // Parse "YYYY-MM-DD HH:MM" (optional seconds) as UTC into ISO8601, or null.
    // The backend parses these absolute inputs as UTC, so we match that.
    _parseAbsInput(val) {
        if (!val) return null;
        const m = String(val).trim().match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/);
        if (!m) return null;
        return `${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:${m[6] || '00'}.000Z`;
    },

    // Format a UTC ISO string into the "YYYY-MM-DD HH:MM" absolute input.
    _toAbsInput(iso) {
        const d = new Date(iso);
        if (isNaN(d)) return '';
        const p = n => String(n).padStart(2, '0');
        return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}`;
    },

    setState(newState) {
        Object.assign(this.state, newState);
        this._updateLabel();
        this._updateActivePreset();
    },

    _updateLabel() {
        const el = document.getElementById('recallTimePickerLabel');
        if (el) el.textContent = this.getLabel();
    },

    _updateActivePreset() {
        document.querySelectorAll('#recallTpPresets .tp-preset').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.value === this.state.type);
        });
    },

    open() {
        const panel = document.getElementById('recallTimePickerPanel');
        const backdrop = document.getElementById('recallTimePickerBackdrop');
        const btn = document.getElementById('recallTimePickerBtn');
        if (panel) panel.style.display = 'block';
        if (backdrop) backdrop.style.display = 'block';
        if (btn) btn.classList.add('active');
    },

    close() {
        const panel = document.getElementById('recallTimePickerPanel');
        const backdrop = document.getElementById('recallTimePickerBackdrop');
        const btn = document.getElementById('recallTimePickerBtn');
        if (panel) panel.style.display = 'none';
        if (backdrop) backdrop.style.display = 'none';
        if (btn) btn.classList.remove('active');
    },

    toggle() {
        const panel = document.getElementById('recallTimePickerPanel');
        if (!panel) return;
        if (panel.style.display === 'none' || !panel.style.display) this.open();
        else this.close();
    },

    // Programmatically select an absolute UTC window (used by re-run).
    setCustom(fromISO, toISO) {
        this.setState({ type: 'custom', absStart: fromISO, absEnd: toISO });
        const s = document.getElementById('recallTpAbsStart');
        const e = document.getElementById('recallTpAbsEnd');
        if (s) s.value = this._toAbsInput(fromISO);
        if (e) e.value = this._toAbsInput(toISO);
    },

    init() {
        const btn = document.getElementById('recallTimePickerBtn');
        if (btn) btn.addEventListener('click', (e) => { e.stopPropagation(); this.toggle(); });

        const backdrop = document.getElementById('recallTimePickerBackdrop');
        if (backdrop) backdrop.addEventListener('click', () => this.close());

        document.querySelectorAll('#recallTpPresets .tp-preset').forEach(p => {
            p.addEventListener('click', () => { this.setState({ type: p.dataset.value }); this.close(); });
        });

        const relN = document.getElementById('recallTpRelativeN');
        const relUnit = document.getElementById('recallTpRelativeUnit');
        const relApply = document.getElementById('recallTpRelativeApply');
        const applyRel = () => {
            const n = parseInt(relN?.value || '30', 10);
            const unit = relUnit?.value || 'days';
            if (n > 0) { this.setState({ type: 'relative', relN: n, relUnit: unit }); this.close(); }
        };
        if (relApply) relApply.addEventListener('click', applyRel);
        if (relN) relN.addEventListener('keydown', e => { if (e.key === 'Enter') applyRel(); });

        const absStart = document.getElementById('recallTpAbsStart');
        const absEnd = document.getElementById('recallTpAbsEnd');
        const absApply = document.getElementById('recallTpAbsApply');
        const applyAbs = () => {
            const from = this._parseAbsInput(absStart?.value);
            const to = this._parseAbsInput(absEnd?.value);
            if (from && to) { this.setState({ type: 'custom', absStart: from, absEnd: to }); this.close(); }
        };
        if (absApply) absApply.addEventListener('click', applyAbs);
        if (absStart) absStart.addEventListener('keydown', e => { if (e.key === 'Enter') applyAbs(); });
        if (absEnd) absEnd.addEventListener('keydown', e => { if (e.key === 'Enter') applyAbs(); });

        this._updateLabel();
        this._updateActivePreset();
    },
};

const Recall = {
    POLL_MS: 1000,
    MAX_ROWS: 250, // matches the query page; narrow the range/query rather than scroll
    RECENT_LIMIT: 10,

    isActive: false,
    _initDone: false,
    _running: false,      // a job is in flight (Run button shows Cancel)
    _activeJobId: null,   // job currently shown in the results pane
    _pollTimer: null,
    _elapsedTimer: null,
    _recent: [],
    _visToken: 0,         // guards out-of-order archive-availability checks
    _pager: null,         // scoped client-side pagination for the results table
    _fieldOrder: null,    // column order of the active job's results
    _isAggregated: false, // whether the active job's results are aggregated

    init() {
        if (this._initDone) return;
        this._initDone = true;

        // Dedicated LogDetail host so the per-row drawer targets the Recall table.
        if (window.LogDetail && typeof LogDetail.registerHost === 'function') {
            LogDetail.registerHost('recall', '#recallLogDetailPanel', {
                tableRoot: '#recallResults',
                storageKey: 'recallLogDetailPanelWidth',
            });
        }

        // Client-side pagination over the already-fetched result set (the
        // archive job returns the full set up front, so we page it locally,
        // mirroring the Query tab's page-size behavior).
        if (window.createPagination) {
            this._pager = window.createPagination({
                barId: 'recallPaginationBar',
                numbersId: 'recallPageNumbers',
            });
            this._pager.init((pageRows) => this.renderPage(pageRows));
        }

        const runBtn = document.getElementById('recallRunBtn');
        if (runBtn) runBtn.addEventListener('click', () => this.runOrCancel());

        const input = document.getElementById('recallQueryInput');
        if (input) {
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    if (!this._running) this.runSearch();
                }
            });
            // Wire BQL syntax highlighting (the textarea text is transparent and
            // rendered by the #recallQueryHighlight overlay, same as main Query).
            if (window.SyntaxHighlight) {
                window.SyntaxHighlight.initializeQueryInput('recallQueryInput', 'recallQueryHighlight');
            }
        }

        // Ingest-time picker (independent of the main Query event-time window).
        RecallTimePicker.init();

        // Recent recalls + restore live in a keyboard-driven command palette.
        RecallPalette.init();

        // Escape closes the ingest-time picker (the palette handles its own).
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') RecallTimePicker.close();
        });

        // React to fractal switches: reset state, re-check availability, reload.
        if (window.FractalContext && typeof FractalContext.subscribe === 'function') {
            FractalContext.subscribe('Recall', () => this.onFractalChange());
        }
    },

    // ---- Fractal / availability ----------------------------------------

    fractalId() {
        return (window.FractalContext && FractalContext.currentFractal && !FractalContext.isPrism())
            ? FractalContext.currentFractal.id
            : null;
    },

    onFractalChange() {
        this.stopPolling();
        this._activeJobId = null;
        this.refreshTabVisibility();
        // If the tab is currently showing, reload for the new fractal.
        const view = document.getElementById('recallView');
        if (this.isActive && view && view.style.display !== 'none') {
            this.resetPane();
            this.show('');
        }
    },

    // Toggle the Recall tab button based on archive availability. Uses the
    // auth-only /recall/available endpoint (enabled && provisioned) so the gate
    // is correct for analysts, not just admins.
    async refreshTabVisibility() {
        const btn = document.getElementById('fractalRecallTabBtn');
        if (!btn) return;
        if (!this.fractalId()) { btn.classList.add('rbac-hidden'); return; }

        const token = ++this._visToken;
        const available = await this.checkArchiveAvailable();
        if (token !== this._visToken) return; // superseded by a newer switch
        btn.classList.toggle('rbac-hidden', !available);
    },

    async checkArchiveAvailable() {
        try {
            const res = await fetch('/api/v1/recall/available', { credentials: 'include' });
            if (res.ok) {
                const d = await res.json();
                return !!d.available;
            }
        } catch (_) {}
        return false;
    },

    // ---- Tab lifecycle --------------------------------------------------

    async show(subPath = '') {
        this.isActive = true;
        await this.loadRecent();

        const jobId = (subPath && /^\d+$/.test(String(subPath))) ? String(subPath) : null;
        if (jobId) {
            this.reattach(jobId, false);
        } else if (this._recent.length) {
            // Auto-reattach to the latest job and reflect it in the URL.
            this.reattach(String(this._recent[0].id), true);
        }
    },

    hide() {
        this.isActive = false;
        this.stopPolling();
    },

    stopPolling() {
        if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }
        if (this._elapsedTimer) { clearInterval(this._elapsedTimer); this._elapsedTimer = null; }
    },

    // Convert a UTC ISO8601 string into the archive restore form's datetime-local
    // value ("YYYY-MM-DDTHH:MM:SS", interpreted as UTC there).
    isoToRestoreInput(iso) {
        const d = new Date(iso);
        if (isNaN(d)) return '';
        const p = n => String(n).padStart(2, '0');
        return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}`;
    },

    // ---- Submit + poll --------------------------------------------------

    async runSearch() {
        const fid = this.fractalId();
        if (!fid) { this.setError('Recall is only available on a fractal.'); return; }

        const query = (document.getElementById('recallQueryInput') || {}).value || '';
        if (!query.trim()) { this.setError('Enter a query.'); return; }

        const range = RecallTimePicker.resolve();
        const fromISO = range.from, toISO = range.to;
        if (!fromISO || !toISO) { this.setError('Pick a valid ingest-time range.'); return; }
        if (!(new Date(toISO) > new Date(fromISO))) { this.setError('End must be after start.'); return; }

        this.setError('');
        this.setRunning(true);
        try {
            const res = await fetch(`/api/v1/recall/${encodeURIComponent(fid)}`, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: query, from: fromISO, to: toISO, max_rows: this.MAX_ROWS }),
            });
            if (!res.ok) {
                const msg = (await res.text()) || `Request failed (${res.status})`;
                this.setError(msg.trim());
                this.setRunning(false);
                return;
            }
            const data = await res.json();
            const id = String(data.id);
            await this.loadRecent();
            this.reattach(id, true);
        } catch (err) {
            this.setError('Network error submitting search.');
            this.setRunning(false);
        }
    },

    // Attach the pane to a specific job: render its current state and, if it is
    // still in flight, resume polling. When updateHash is set, reflect the job in
    // the URL so a refresh reattaches to it.
    async reattach(jobId, updateHash) {
        const fid = this.fractalId();
        if (!fid) return;
        this.stopPolling();
        this._activeJobId = jobId;
        if (updateHash && window.App && typeof App.pushSubPath === 'function') {
            App.pushSubPath(jobId);
        }
        this.highlightRecent(jobId);
        await this.pollOnce(jobId);
    },

    async pollOnce(jobId) {
        const fid = this.fractalId();
        if (!fid || this._activeJobId !== jobId) return;
        let job;
        try {
            const res = await fetch(`/api/v1/recall/${encodeURIComponent(fid)}/${jobId}`, { credentials: 'include' });
            if (!res.ok) {
                if (this._activeJobId === jobId) this.setError(`Failed to load job (${res.status}).`);
                this.setRunning(false);
                return;
            }
            job = await res.json();
        } catch (_) {
            // Transient; retry on the next tick if still active.
            if (this._activeJobId === jobId && this.isActive) {
                this._pollTimer = setTimeout(() => this.pollOnce(jobId), this.POLL_MS);
            }
            return;
        }
        if (this._activeJobId !== jobId) return;

        if (job.status === 'pending' || job.status === 'running') {
            this.renderRunning(job);
            this.setRunning(true);
            if (this.isActive) {
                this._pollTimer = setTimeout(() => this.pollOnce(jobId), this.POLL_MS);
            }
        } else {
            this.stopPolling();
            this.setRunning(false);
            this.renderTerminal(job);
            this.loadRecent(); // refresh the strip's chip/time
        }
    },

    async cancelJob(jobId, ev) {
        if (ev) ev.stopPropagation();
        const fid = this.fractalId();
        if (!fid) return;
        try {
            await fetch(`/api/v1/recall/${encodeURIComponent(fid)}/${jobId}/cancel`, {
                method: 'POST', credentials: 'include',
            });
        } catch (_) {}
        await this.loadRecent();
        if (this._activeJobId === String(jobId)) this.pollOnce(String(jobId));
    },

    // ---- Rendering ------------------------------------------------------

    renderRunning(job) {
        this.stopElapsed();
        const startMs = job.started_at ? new Date(job.started_at).getTime()
            : (job.created_at ? new Date(job.created_at).getTime() : Date.now());
        const label = job.status === 'pending' ? 'Queued' : 'Searching archive';
        const paint = () => {
            const secs = Math.max(0, Math.round((Date.now() - startMs) / 1000));
            this.setStatus(
                `<span class="recall-spinner"></span><span>${label}</span>` +
                `<span class="recall-status-elapsed">${secs}s</span>`,
                'running'
            );
        };
        paint();
        this._elapsedTimer = setInterval(paint, 1000);
        // Keep the previous table (if any) but dim it; show a placeholder if empty.
        const pane = document.getElementById('recallResults');
        if (pane && !pane.querySelector('table')) {
            pane.innerHTML = '<div class="no-results">Searching the archive...</div>';
        }
    },

    renderTerminal(job) {
        this.stopElapsed();
        const pane = document.getElementById('recallResults');

        // Any non-success terminal state clears the table; drop the page bar too.
        if (job.status !== 'succeeded' || job.results_expired) {
            if (this._pager) this._pager.reset();
        }

        if (job.status === 'failed') {
            this.setStatus(`<span class="recall-chip recall-chip-failed">Failed</span><span>${this.esc(job.error || 'Search failed.')}</span>`, 'error');
            if (pane) pane.innerHTML = `<div class="no-results">${this.esc(job.error || 'Search failed.')}</div>`;
            return;
        }
        if (job.status === 'canceled') {
            this.setStatus('<span class="recall-chip recall-chip-canceled">Canceled</span><span>Search was canceled.</span>', '');
            if (pane) pane.innerHTML = '<div class="no-results">Search canceled.</div>';
            return;
        }
        // succeeded
        if (job.results_expired) {
            this.setStatus(
                '<span class="recall-chip recall-chip-succeeded">Done</span>' +
                '<span>Results have aged out.</span>' +
                `<button class="recall-rerun-btn" onclick="Recall.rerun('${job.id}')">Re-run</button>`,
                ''
            );
            if (pane) pane.innerHTML = '<div class="no-results">Results are no longer cached. Re-run the search to regenerate them.</div>';
            return;
        }

        const results = Array.isArray(job.results) ? job.results : [];
        const count = job.row_count != null ? job.row_count : results.length;
        let statusHtml = `<span class="recall-chip recall-chip-succeeded">Done</span>` +
            `<span class="recall-status-count">${Number(count).toLocaleString()} rows</span>`;
        if (job.limit_hit) {
            statusHtml += '<span class="recall-limit-note" title="More rows matched than were returned; narrow the range or query.">limit reached</span>';
        }
        this.setStatus(statusHtml, '');

        this._fieldOrder = job.field_order || null;
        this._isAggregated = !!job.is_aggregated;
        if (this._pager) {
            this._pager.setResults(results);
        } else {
            this.renderPage(results);
        }
    },

    // Render a single page of results into the Recall pane. Reused as the
    // pager's render callback so switching pages re-renders the table in place.
    renderPage(rows) {
        const pane = document.getElementById('recallResults');
        if (pane && window.QueryExecutor && typeof QueryExecutor.renderResultsToElement === 'function') {
            QueryExecutor.renderResultsToElement(rows, pane, this._fieldOrder || null, {
                detailHost: 'recall',
                isAggregated: this._isAggregated,
            });
        }
    },

    // Load a prior job's query/window into the form and submit fresh.
    async rerun(jobId) {
        const fid = this.fractalId();
        if (!fid) return;
        try {
            const res = await fetch(`/api/v1/recall/${encodeURIComponent(fid)}/${jobId}`, { credentials: 'include' });
            if (!res.ok) return;
            const job = await res.json();
            const input = document.getElementById('recallQueryInput');
            if (input) input.value = job.query || '';
            if (job.from && job.to) {
                RecallTimePicker.setCustom(new Date(job.from).toISOString(), new Date(job.to).toISOString());
            }
        } catch (_) {}
        this.runSearch();
    },

    // ---- Recent recalls strip ------------------------------------------

    async loadRecent() {
        const fid = this.fractalId();
        if (!fid) { this._recent = []; this.renderRecent(); return; }
        try {
            const res = await fetch(`/api/v1/recall/${encodeURIComponent(fid)}?limit=${this.RECENT_LIMIT}`, { credentials: 'include' });
            if (!res.ok) { this._recent = []; this.renderRecent(); return; }
            const data = await res.json();
            this._recent = Array.isArray(data.jobs) ? data.jobs : [];
        } catch (_) {
            this._recent = [];
        }
        this.renderRecent();
    },

    // The recent list now lives in the command palette; keep it in sync.
    renderRecent() {
        RecallPalette.render();
    },

    // Reflect the currently-attached job in the palette (re-render picks it up).
    highlightRecent(jobId) {
        RecallPalette.render();
    },

    statusLabel(status) {
        switch (status) {
            case 'running': return 'Running';
            case 'succeeded': return 'Done';
            case 'failed': return 'Failed';
            case 'canceled': return 'Canceled';
            default: return 'Pending';
        }
    },

    // ---- Restore handoff (admin only) -----------------------------------

    // Deep-link to the existing System > Archive restore flow, prefilled with the
    // current fractal and the Recall time window. The heavy restore action lives
    // in performance.js; we only open its drawer.
    restoreHandoff() {
        const fid = this.fractalId();
        const range = RecallTimePicker.resolve();
        const fromVal = this.isoToRestoreInput(range.from);
        const toVal = this.isoToRestoreInput(range.to);
        if (window.App && typeof App.showMainView === 'function') {
            App.showMainView('performance', 'archive');
        }
        setTimeout(() => {
            if (!window.Performance || typeof Performance.openRestoreForm !== 'function') return;
            if (fid) Performance._restoreSelected = new Set([fid]);
            Performance.openRestoreForm();
            const f = document.getElementById('restoreFrom');
            const t = document.getElementById('restoreTo');
            if (f && fromVal) f.value = fromVal;
            if (t && toVal) t.value = toVal;
        }, 250);
    },

    // ---- Small helpers --------------------------------------------------

    resetPane() {
        this.stopPolling();
        this.setStatus('', '');
        const bar = document.getElementById('recallStatusBar');
        if (bar) bar.style.display = 'none';
        const pane = document.getElementById('recallResults');
        if (pane) pane.innerHTML = '<div class="no-results">Search the archive for older, cold-tiered logs</div>';
    },

    // Dispatch the single Run/Cancel button: cancel the in-flight job, else search.
    runOrCancel() {
        if (this._running && this._activeJobId) {
            this.cancelJob(this._activeJobId);
        } else {
            this.runSearch();
        }
    },

    setRunning(on) {
        this._running = !!on;
        const btn = document.getElementById('recallRunBtn');
        if (!btn) return;
        // Flip the Run button into a Cancel affordance while a job is in flight
        // (mirrors the Query page). It stays enabled so the click can cancel.
        btn.disabled = false;
        btn.classList.toggle('is-running', !!on);
        const text = btn.querySelector('.btn-text');
        const loader = btn.querySelector('.btn-loader');
        if (text) { text.textContent = on ? 'Cancel' : 'Recall'; text.style.display = ''; }
        if (loader) loader.style.display = 'none';
    },

    // Detach from the attached job and return to a fresh compose state so a new
    // search can be launched (the Run button flips back from Cancel to Recall).
    // Any previously running job keeps running and stays in the palette.
    newSearch() {
        this.resetPane();
        this._activeJobId = null;
        this.setRunning(false);
        if (window.App && typeof App.pushSubPath === 'function') App.pushSubPath('');
        const input = document.getElementById('recallQueryInput');
        if (input) input.focus();
        this.highlightRecent(null);
    },

    setStatus(html, kind) {
        const bar = document.getElementById('recallStatusBar');
        if (!bar) return;
        bar.className = 'recall-status-bar' + (kind ? ' ' + kind : '');
        bar.innerHTML = html || '';
        bar.style.display = html ? 'flex' : 'none';
    },

    setError(msg) {
        const el = document.getElementById('recallError');
        if (!el) return;
        el.textContent = msg || '';
        el.style.display = msg ? 'block' : 'none';
    },

    stopElapsed() {
        if (this._elapsedTimer) { clearInterval(this._elapsedTimer); this._elapsedTimer = null; }
    },

    esc(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    },

    timeAgo(ts) {
        if (!ts) return '';
        const then = new Date(ts).getTime();
        if (isNaN(then)) return '';
        const s = Math.max(0, Math.round((Date.now() - then) / 1000));
        if (s < 60) return `${s}s ago`;
        const m = Math.round(s / 60);
        if (m < 60) return `${m}m ago`;
        const h = Math.round(m / 60);
        if (h < 24) return `${h}h ago`;
        const d = Math.round(h / 24);
        return `${d}d ago`;
    },
};

// Recall command palette: a keyboard-driven surface (Ctrl/Cmd+K when the Recall
// tab is active) that merges the recent-recalls list with the admin-only Restore
// action, modeled on the main Query palette but a fully separate instance.
const RECALL_RESTORE_ICON = '<svg width="13" height="13" viewBox="0 0 16 16" fill="none"><path d="M2.5 8a5.5 5.5 0 1 0 1.7-4M2.5 3v3.5H6" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/></svg>';
const RECALL_NEW_ICON = '<svg width="13" height="13" viewBox="0 0 16 16" fill="none"><path d="M8 3v10M3 8h10" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>';

const RecallPalette = {
    isOpen: false,
    searchTerm: '',
    activeIndex: 0,
    filtered: [],   // recall jobs after the search filter
    _refreshTimer: null, // live-refreshes the jobs list while the palette is open

    init() {
        const btn = document.getElementById('recallPaletteBtn');
        if (btn) btn.addEventListener('click', (e) => { e.stopPropagation(); this.toggle(); });

        const search = document.getElementById('recallPaletteSearch');
        if (search) {
            search.addEventListener('input', (e) => this.handleSearch(e.target.value));
            search.addEventListener('keydown', (e) => this.onSearchKeydown(e));
        }

        const palette = document.getElementById('recallPalette');
        if (palette) palette.addEventListener('click', (e) => e.stopPropagation());
        const backdrop = document.getElementById('recallPaletteBackdrop');
        if (backdrop) backdrop.addEventListener('click', () => this.close());

        // Ctrl/Cmd-K owns the palette only while the Recall tab is active; the
        // main Query palette bails out in that case (see queryPalette.js).
        document.addEventListener('keydown', (e) => {
            if ((e.metaKey || e.ctrlKey) && (e.key === 'k' || e.key === 'K')) {
                if (!document.body.classList.contains('recall-active')) return;
                e.preventDefault();
                this.toggle();
                return;
            }
            if (e.key === 'Escape' && this.isOpen) this.close();
        });

        document.addEventListener('click', (e) => {
            if (this.isOpen && palette && !palette.contains(e.target) && e.target.id !== 'recallPaletteBtn') {
                this.close();
            }
        });
    },

    isAdmin() {
        return !!(window.Auth && typeof Auth.hasFractalRole === 'function' && Auth.hasFractalRole('admin'));
    },

    // Pinned (non-recall) action rows preceding the recall rows: New search
    // (always) plus Restore (admin only).
    pinnedCount() { return 1 + (this.isAdmin() ? 1 : 0); },

    // Total selectable rows currently rendered.
    itemCount() { return this.pinnedCount() + this.filtered.length; },

    toggle() { this.isOpen ? this.close() : this.open(); },

    open() {
        const palette = document.getElementById('recallPalette');
        const backdrop = document.getElementById('recallPaletteBackdrop');
        if (!palette) return;
        if (backdrop) backdrop.style.display = 'block';
        palette.style.display = 'flex';
        this.isOpen = true;
        this.activeIndex = 0;
        this.searchTerm = '';
        const search = document.getElementById('recallPaletteSearch');
        if (search) { search.value = ''; setTimeout(() => search.focus(), 30); }
        const btn = document.getElementById('recallPaletteBtn');
        if (btn) btn.classList.add('active');
        this.render();
        // Refresh the jobs from the server, then re-render. Poll while open so
        // in-flight jobs update their status/spinner live without reopening.
        if (window.Recall) Recall.loadRecent();
        if (this._refreshTimer) clearInterval(this._refreshTimer);
        this._refreshTimer = setInterval(() => {
            if (this.isOpen && window.Recall) Recall.loadRecent();
        }, 2500);
    },

    close() {
        const palette = document.getElementById('recallPalette');
        const backdrop = document.getElementById('recallPaletteBackdrop');
        if (palette) palette.style.display = 'none';
        if (backdrop) backdrop.style.display = 'none';
        this.isOpen = false;
        if (this._refreshTimer) { clearInterval(this._refreshTimer); this._refreshTimer = null; }
        const btn = document.getElementById('recallPaletteBtn');
        if (btn) btn.classList.remove('active');
    },

    handleSearch(value) {
        this.searchTerm = value;
        this.activeIndex = 0;
        this.render();
    },

    onSearchKeydown(e) {
        if (e.key === 'ArrowDown') { e.preventDefault(); this.move(1); }
        else if (e.key === 'ArrowUp') { e.preventDefault(); this.move(-1); }
        else if (e.key === 'Enter') { e.preventDefault(); this.activate(this.activeIndex); }
    },

    move(delta) {
        const n = this.itemCount();
        if (!n) return;
        this.activeIndex = (this.activeIndex + delta + n) % n;
        this.applyHighlight();
    },

    applyHighlight() {
        const list = document.getElementById('recallPaletteList');
        if (!list) return;
        list.querySelectorAll('.palette-row[data-pidx]').forEach(el => {
            const i = parseInt(el.dataset.pidx, 10);
            el.classList.toggle('is-active', i === this.activeIndex);
            if (i === this.activeIndex) el.scrollIntoView({ block: 'nearest' });
        });
    },

    // Activate the palette row at global index i: a pinned action (New search,
    // then Restore) or a recall row (reattach). Always closes the palette after.
    activate(i) {
        const pinned = this.pinnedCount();
        if (i < pinned) {
            this.close();
            if (i === 0) {
                if (window.Recall) Recall.newSearch();
            } else if (window.Recall) {
                Recall.restoreHandoff();
            }
            return;
        }
        const job = this.filtered[i - pinned];
        if (!job) return;
        this.close();
        if (window.Recall) Recall.reattach(String(job.id), true);
    },

    render() {
        const list = document.getElementById('recallPaletteList');
        if (!list) return;

        const jobs = (window.Recall && Array.isArray(Recall._recent)) ? Recall._recent : [];
        const term = this.searchTerm.trim().toLowerCase();
        this.filtered = term
            ? jobs.filter(j => (j.query || '').toLowerCase().includes(term))
            : jobs.slice();

        const total = this.itemCount();
        if (this.activeIndex >= total) this.activeIndex = Math.max(0, total - 1);

        const esc = (s) => (window.Recall ? Recall.esc(s) : String(s == null ? '' : s));
        let html = '';
        let idx = 0;

        // New search: detach and return to a fresh compose state.
        {
            const active = idx === this.activeIndex ? ' is-active' : '';
            html += `
              <div class="palette-row recall-palette-action${active}" data-pidx="${idx}" onclick="RecallPalette.activate(${idx})">
                <span class="recall-palette-action-icon">${RECALL_NEW_ICON}</span>
                <div class="palette-row-main"><div class="palette-query">New search</div></div>
              </div>`;
            idx++;
        }
        if (this.isAdmin()) {
            const active = idx === this.activeIndex ? ' is-active' : '';
            html += `
              <div class="palette-row recall-palette-action${active}" data-pidx="${idx}" onclick="RecallPalette.activate(${idx})">
                <span class="recall-palette-action-icon">${RECALL_RESTORE_ICON}</span>
                <div class="palette-row-main"><div class="palette-query">Restore this window</div></div>
              </div>`;
            idx++;
        }

        if (!this.filtered.length) {
            html += `<div class="palette-empty">${term ? 'No matching recalls' : 'No recalls yet'}</div>`;
        } else {
            const activeId = window.Recall ? String(Recall._activeJobId) : '';
            this.filtered.forEach(j => {
                const active = idx === this.activeIndex ? ' is-active' : '';
                const current = String(j.id) === activeId ? ' recall-palette-current' : '';
                const q = (j.query || '').trim() || '(empty)';
                const status = esc(j.status);
                const label = window.Recall ? Recall.statusLabel(j.status) : status;
                const when = window.Recall ? Recall.timeAgo(j.created_at) : '';
                const inFlight = j.status === 'running' || j.status === 'pending';
                const spinner = inFlight ? '<span class="recall-chip-spinner"></span>' : '';
                const cancel = inFlight
                    ? `<button class="recall-cancel-btn" title="Cancel this search" onclick="Recall.cancelJob('${j.id}', event)">Cancel</button>`
                    : '';
                html += `
                  <div class="palette-row${active}${current}" data-pidx="${idx}" data-id="${j.id}" onclick="RecallPalette.activate(${idx})">
                    <span class="recall-chip recall-chip-${status}">${spinner}${label}</span>
                    <div class="palette-row-main"><div class="palette-query" title="${esc(q)}">${esc(q)}</div></div>
                    <span class="recall-recent-time">${when}</span>
                    ${cancel}
                  </div>`;
                idx++;
            });
        }

        list.innerHTML = html;
    },
};

window.RecallPalette = RecallPalette;
window.Recall = Recall;
