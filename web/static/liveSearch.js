// Live search: the "Live" time range. Plain searches poll /api/v1/query/live for
// newest arrivals; aggregations and charts re-run as a 15 minute search instead.
// The analyst pauses it by reading (scrolled down, past page 1, a log open, a
// press or text selection in the results); afterwards, the rows that arrived
// meanwhile are shown.
const LiveSearch = {
    POLL_MS: 2000,
    RETRY_MS: 5000,
    RERUN_MS: 10000,
    MAX_ROWS: 1000,
    WINDOW_MS: 15 * 60 * 1000,
    OVERLAP_MS: 2 * 60 * 1000, // the server re-reads this far behind the cursor

    active: false,
    mode: 'rows', // 'rows' polls arrivals, 'rerun' repeats the search
    paused: false,
    _gen: 0,
    _timer: null,
    _controller: null,
    _inflight: false,
    _cursor: '',
    _cursorMs: 0, // server clock: the end of the last window read
    _lastPollAt: 0, // browser clock: when that window was read
    _seen: new Map(), // log_id -> arrival ms, while the overlap can still re-read it
    _buckets: new Map(), // bucket start ms -> count
    _bucketSec: 10,
    _pending: [],
    _limited: false,
    _warnings: [],
    _query: '',
    _requestQuery: '',
    _elements: null,
    _scope: '',
    _pressing: false,

    isLiveRange() {
        return !!(window.TimePicker && TimePicker.state.type === 'live');
    },

    // The fractal or prism a Live run belongs to. A switch ends the run, whatever
    // path the switch took, so rows from two scopes never share one table.
    _scopeKey() {
        const ctx = window.FractalContext;
        const cur = ctx && ctx.currentFractal;
        if (!cur) return '';
        return (ctx.isPrism() ? 'p:' : 'f:') + cur.id;
    },

    // start begins a run. query is the display form recorded in history;
    // requestQuery is sent verbatim so server error offsets match the editor.
    start(query, requestQuery, elements) {
        this.stop(true);
        this.active = true;
        this.mode = 'rows';
        this.paused = false;
        this._query = query;
        this._requestQuery = requestQuery;
        this._elements = elements;
        this._scope = this._scopeKey();
        this._resetWindow();
        this._showWaiting();
        if (window.Timeline) Timeline.hide();
        this._bindPauseWatchers();
        this._syncChrome();
        this._poll(this._gen);
    },

    // stop ends polling. quiet skips the chrome reset when a new run replaces this one.
    stop(quiet) {
        this._gen++;
        clearTimeout(this._timer);
        this._timer = null;
        if (this._controller) this._controller.abort();
        this._controller = null;
        this._inflight = false;
        const was = this.active;
        this.active = false;
        this.paused = false;
        this._pending = [];
        if (was && !quiet) {
            QueryExecutor._streamingActive = false;
            this._syncChrome();
            const el = this._elements && this._elements.executionTime;
            if (el) el.textContent = '';
        }
    },

    _resetWindow() {
        this._cursor = '';
        this._cursorMs = 0;
        this._lastPollAt = 0;
        this._seen.clear();
        this._buckets.clear();
        this._pending = [];
        this._limited = false;
        this._warnings = [];
        QueryExecutor.currentResults = [];
        if (window.Pagination) Pagination.setResults([]);
    },

    _showWaiting() {
        const table = this._elements && this._elements.resultsTable;
        if (!table) return;
        table.style.display = 'block';
        table.innerHTML = '<div class="stream-searching"><span>Waiting for new events…</span></div>';
    },

    _schedule(gen, ms) {
        clearTimeout(this._timer);
        this._timer = setTimeout(() => {
            if (gen !== this._gen) return;
            if (this.mode === 'rerun') this._rerun(gen);
            else this._poll(gen);
        }, ms);
    },

    // Nobody is watching a hidden tab or another view, so skip the read.
    _watching() {
        const view = document.getElementById('searchView');
        return !document.hidden && !!(view && view.offsetParent !== null);
    },

    // _stillOurs ends the run once the range or scope it was started for has
    // changed, however that happened.
    _stillOurs() {
        if (this.isLiveRange() && this._scopeKey() === this._scope) return true;
        this.stop();
        return false;
    },

    async _poll(gen) {
        if (gen !== this._gen || this._inflight || !this._stillOurs()) return;
        if (!this._watching()) { this._schedule(gen, this.POLL_MS); return; }
        // Past the window the server reads only its last 15 minutes, which would
        // leave a silent hole under the rows already shown, so start over.
        if (this._lastPollAt && Date.now() - this._lastPollAt > this.WINDOW_MS - this.OVERLAP_MS) {
            this._resetWindow();
            this._showWaiting();
        }

        const body = { query: this._requestQuery, start: this._cursor };
        const vars = QueryExecutor.variablesPayload();
        if (vars) body.variables = vars;
        if (window.FractalContext && FractalContext.currentFractal && !FractalContext.isPrism()) {
            body.fractal_id = FractalContext.currentFractal.id;
        }

        this._controller = new AbortController();
        this._inflight = true;
        let res, data;
        try {
            res = await fetch('/api/v1/query/live', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(body),
                signal: this._controller.signal,
            });
            data = await res.json().catch(() => ({}));
        } catch (e) {
            if (gen !== this._gen) return;
            this._inflight = false;
            if (e.name !== 'AbortError') this._schedule(gen, this.RETRY_MS);
            return;
        }
        if (gen !== this._gen) return;
        this._inflight = false;
        if (!this._stillOurs()) return;

        if (!res.ok || !data.success) {
            // A gateway or overloaded backend is retried; anything else (a bad
            // query, a timeout, an error the server reports) would recur, so it ends Live.
            if ([502, 503, 504].includes(res.status)) { this._schedule(gen, this.RETRY_MS); return; }
            this.stop();
            QueryExecutor._failQuery(data.error || `Live search failed (${res.status})`, data.error_type, data.error_pos);
            return;
        }

        const first = !this._cursor;
        if (first && window.QueryPalette) {
            QueryPalette.recordRun({ query: this._query, timeRange: 'live', resultCount: null, durationMs: data.execution_ms, status: 'ok' });
        }
        if (data.live_unsupported) {
            this.mode = 'rerun';
            this._syncChrome();
            this._rerun(gen);
            return;
        }
        if (first) {
            QueryExecutor._applyQueryMeta({ field_order: data.field_order, is_aggregated: false }, this._elements, true);
        }
        this._cursor = data.cursor || this._cursor;
        this._cursorMs = Date.parse(this._cursor) || this._cursorMs;
        this._lastPollAt = Date.now();
        // The cap only drops new rows when the oldest row it kept is itself new; when
        // it is one already shown, everything cut was an overlap repeat.
        const rows = data.results || [];
        const oldest = rows[rows.length - 1];
        this._limited = !!data.limit_hit && !!oldest && !this._seen.has(oldest.log_id);
        this._warnings = data.warnings || [];
        this._mergeHistogram(data.histogram);
        this._updatePause();
        this._mergeRows(rows, data.histogram, first);
        this._syncChrome();
        this._schedule(gen, this.POLL_MS);
    },

    // Aggregations cannot be appended to, so they re-run the query Live started as a
    // search over the window. A tick is skipped while the analyst is reading, so a
    // re-render never resets the page, sort or chart under them.
    _rerun(gen) {
        if (gen !== this._gen || !this._stillOurs()) return;
        this._updatePause();
        this._syncChrome();
        if (this._watching() && !this.paused && !QueryExecutor.currentRequest) {
            QueryExecutor._liveRerun = true;
            QueryExecutor._liveRerunQuery = this._requestQuery;
            QueryExecutor.execute();
        }
        this._schedule(gen, this.RERUN_MS);
    },

    // ClickHouse renders DateTime64 without a zone; the column is UTC.
    _arrivalMs(row) {
        const v = row._ingest_timestamp;
        return v ? Date.parse(String(v).replace(' ', 'T') + 'Z') : NaN;
    },

    _mergeRows(rows, hist, first) {
        const fresh = [];
        for (const row of rows) {
            const id = row.log_id;
            if (id && this._seen.has(id)) continue;
            if (id) this._seen.set(id, this._arrivalMs(row));
            fresh.push(row);
        }
        // The overlap only re-reads arrivals at or after this window's start, so an
        // id that arrived earlier can never be returned again.
        const readFrom = hist ? Date.parse(hist.bucket_start) : NaN;
        if (Number.isFinite(readFrom)) {
            for (const [id, ms] of this._seen) if (ms < readFrom) this._seen.delete(id);
        }
        if (!fresh.length && !first) return;

        if (this.paused) {
            this._pending = fresh.concat(this._pending).slice(0, this.MAX_ROWS);
            return;
        }
        this._show(fresh, first);
    },

    _show(fresh, first) {
        const all = fresh.concat(QueryExecutor.currentResults || []).slice(0, this.MAX_ROWS);
        QueryExecutor.currentResults = all;
        if (!all.length) { this._showWaiting(); return; }
        QueryExecutor._renderCurrentResults(this._elements);
        if (!first) this._flash(fresh.length);
        if (window.FieldStats && first) FieldStats.onResults();
    },

    // Mark the rows that just arrived; the CSS fades the mark out.
    _flash(n) {
        const rows = document.querySelectorAll('#resultsTable .result-row');
        for (let i = 0; i < Math.min(n, rows.length); i++) rows[i].classList.add('live-new');
    },

    _mergeHistogram(h) {
        if (!h || !Array.isArray(h.buckets)) return;
        this._bucketSec = h.bucket_seconds || 10;
        const step = this._bucketSec * 1000;
        const start = Date.parse(h.bucket_start);
        h.buckets.forEach((n, i) => this._buckets.set(start + i * step, n));
        const floor = this._cursorMs - this.WINDOW_MS - step;
        for (const ms of this._buckets.keys()) if (ms < floor) this._buckets.delete(ms);
        this._renderTimeline();
    },

    // The window ends at the server's cursor, not the browser clock, so a skewed
    // client neither hides the newest buckets nor shows empty future ones.
    _windowBuckets() {
        const step = this._bucketSec * 1000;
        const end = Math.floor((this._cursorMs || Date.now()) / step) * step + step;
        const start = end - this.WINDOW_MS;
        const out = [];
        for (let ms = start; ms < end; ms += step) out.push(this._buckets.get(ms) || 0);
        return { out, start, end };
    },

    _renderTimeline() {
        if (!window.Timeline) return;
        const { out, start, end } = this._windowBuckets();
        Timeline.renderFromHistogram(out, { start: new Date(start).toISOString(), end: new Date(end).toISOString() }, { coveredFrom: 0 });
    },

    // Arrivals per second over the last full minute, or per minute when slower.
    _rate() {
        const step = this._bucketSec * 1000;
        const current = Math.floor(this._cursorMs / step) * step;
        let n = 0;
        for (let ms = current - 60000; ms < current; ms += step) n += this._buckets.get(ms) || 0;
        const perSec = n / 60;
        if (perSec >= 10) return `${Math.round(perSec)}/s`;
        if (perSec >= 1) return `${perSec.toFixed(1)}/s`;
        return `${n}/min`;
    },

    _bindPauseWatchers() {
        if (this._bound) return;
        this._bound = true;
        const check = () => this._checkPause();
        const scroller = document.getElementById('resultsTable');
        if (scroller) scroller.addEventListener('scroll', check, { passive: true });
        // A press holds the table still until release, so a re-render never lands
        // between mousedown and click.
        const section = document.querySelector('#searchView .results-section');
        if (section) section.addEventListener('pointerdown', () => { this._pressing = true; check(); });
        document.addEventListener('pointerup', () => { if (this._pressing) { this._pressing = false; check(); } });
        document.addEventListener('selectionchange', check);
        // Back on the tab: read now instead of waiting out the timer.
        document.addEventListener('visibilitychange', () => {
            if (!document.hidden && this.active && this.mode === 'rows' && !this._inflight) this._schedule(this._gen, 0);
        });
    },

    // Reading pauses Live: scrolled down, past page 1, a log open, a press in the
    // results, or text selected in them.
    _reading() {
        const scroller = document.getElementById('resultsTable');
        const scrolled = scroller && scroller.scrollTop > 40;
        const paged = window.Pagination && Pagination.currentPage > 1;
        const detail = document.getElementById('logDetailPanel');
        const open = detail && detail.classList.contains('open');
        const section = document.querySelector('#searchView .results-section');
        const sel = window.getSelection && window.getSelection();
        const selecting = !!(sel && !sel.isCollapsed && section && section.contains(sel.anchorNode));
        return !!(scrolled || paged || open || this._pressing || selecting);
    },

    // _updatePause tracks whether the analyst is reading; on returning, the rows
    // that arrived meanwhile are shown.
    _updatePause() {
        const reading = this._reading();
        if (reading === this.paused) return;
        this.paused = reading;
        if (!reading && this._pending.length) {
            const pending = this._pending;
            this._pending = [];
            this._show(pending, false);
        }
    },

    _checkPause() {
        if (!this.active) return;
        this._updatePause();
        this._syncChrome();
    },

    // One place renders every Live cue: the picker chip, the Run/Stop button and,
    // for streamed rows, the results header. A re-run's header is the search's own.
    _syncChrome() {
        const btn = document.getElementById('timePickerBtn');
        if (btn) {
            btn.classList.toggle('is-live-running', this.active && !this.paused);
            btn.classList.toggle('is-live-paused', this.active && this.paused);
        }
        const label = document.getElementById('timePickerLabel');
        if (label && this.isLiveRange()) label.textContent = this.active && this.paused ? 'Live, paused' : 'Live';
        QueryExecutor._setRunButtonState(QueryExecutor._queryRunning);

        if (!this.active || this.mode !== 'rows' || !this._elements || !this._cursorMs) return;
        const { out } = this._windowBuckets();
        const total = out.reduce((a, b) => a + b, 0);
        const count = this._elements.resultsCount;
        if (count) {
            count.textContent = `${total.toLocaleString()} events`;
            // Same notice a search shows when it could not cover everything asked for.
            if (this._warnings.length) {
                const note = document.createElement('span');
                note.className = 'results-warning';
                note.style.cssText = 'color:#e0a800;margin-left:8px;cursor:help';
                note.textContent = 'partial coverage';
                note.title = this._warnings.join('\n');
                count.appendChild(note);
            }
        }
        const meta = this._elements.executionTime;
        if (meta) {
            meta.style.display = 'inline';
            const waiting = this._pending.length;
            const capped = this._limited ? ', newest rows only' : '';
            meta.innerHTML = !this.paused
                ? `<span class="live-meta">last 15m, <b>${this._rate()}</b>${capped}</span>`
                : waiting
                    ? `<span class="live-meta"><b>${waiting.toLocaleString()} new</b> waiting while you read</span>`
                    : '<span class="live-meta">paused while you read</span>';
        }
    },
};

window.LiveSearch = LiveSearch;
