// System > Activity: the merged query stream and what surrounds it.
//
// Every row is one ClickHouse query, running or finished, attributed to the
// Bifract activity that issued it (the server tags each query's log_comment).
// Filtering happens server-side, so the search box searches the cluster's history
// rather than a prefetched page of it.

const Activity = {
    // Poll cadences. The stream is cheap and follows the page's refresh rate; the
    // summary aggregates the query log and runs on a slower clock of its own.
    SUMMARY_MIN_MS: 20000,

    initialized: false,
    isActive: false,
    streamTimer: null,
    summaryTimer: null,
    refreshRate: 5000,
    range: '1h',

    mode: 'live',
    state: '',
    klass: '',
    text: '',
    node: '',

    rows: [],
    summary: null,
    hovering: false,
    searchDebounce: null,
    streamAbort: null,
    summaryAbort: null,
    lastSummaryAt: 0,
    selectedId: '',
    latencyData: [],

    // Precise sources collapse into four classes for color and filtering; the row
    // still names the exact feature.
    SOURCE_NAMES: {
        search: 'Search', dashboard: 'Dashboard', notebook: 'Notebook',
        alert: 'Alert', recall: 'Recall', model: 'Model', chat: 'Assistant',
        ingest: 'Ingest', system: 'System'
    },
    CLASS_NAMES: { search: 'Search', alert: 'Alerts', ingest: 'Ingest', system: 'System' },
    STATE_NAMES: {
        running: 'Running', stopping: 'Stopping', finished: 'Finished',
        error: 'Error', killed: 'Killed'
    },

    // init runs on DOMContentLoaded and again from the app's own startup, so it
    // has to be idempotent: bound twice, every chip click toggled its filter on
    // and straight back off, which read as the chips doing nothing.
    init() {
        if (this.initialized) return;
        this.initialized = true;

        const chips = document.getElementById('actChips');
        if (chips) {
            chips.addEventListener('click', (e) => {
                const btn = e.target.closest('.act-chip');
                if (!btn) return;
                const group = btn.dataset.filter;
                const value = btn.dataset.value;
                if (group === 'state') {
                    this.state = (this.state === value) ? '' : value;
                } else {
                    this.klass = (this.klass === value) ? '' : value;
                }
                this.syncChips();
                this.loadStream();
            });
        }

        const modes = document.getElementById('actModes');
        if (modes) {
            modes.addEventListener('click', (e) => {
                const btn = e.target.closest('.act-mode');
                if (btn) this.setMode(btn.dataset.mode);
            });
        }

        const search = document.getElementById('actSearch');
        if (search) {
            search.addEventListener('input', (e) => {
                clearTimeout(this.searchDebounce);
                const value = e.target.value;
                this.searchDebounce = setTimeout(() => {
                    this.text = value;
                    this.loadStream();
                }, 250);
            });
        }

        const node = document.getElementById('actNode');
        if (node) {
            node.addEventListener('change', (e) => {
                this.node = e.target.value;
                this.loadStream();
            });
        }

        // Live tail pauses while the pointer is over the table, so rows stop moving
        // out from under a click.
        const table = document.getElementById('actStreamTable');
        if (table) {
            table.addEventListener('mouseenter', () => { this.hovering = true; this.renderLive(); });
            table.addEventListener('mouseleave', () => { this.hovering = false; this.renderLive(); this.loadStream(); });
            table.addEventListener('click', (e) => {
                const kill = e.target.closest('.act-kill');
                if (kill) {
                    e.stopPropagation();
                    this.kill(kill.dataset.id);
                    return;
                }
                const row = e.target.closest('tr[data-id]');
                if (row) this.openDrawer(row.dataset.id);
            });
        }

        document.getElementById('actFailStrip')?.addEventListener('click', (e) => {
            if (!e.target.closest('.act-failpill')) return;
            this.state = 'error';
            this.syncChips();
            this.loadStream();
        });

        this.setMode('live');

        document.getElementById('actDrawerClose')?.addEventListener('click', () => this.closeDrawer());
        document.getElementById('actDrawerScrim')?.addEventListener('click', () => this.closeDrawer());
        document.getElementById('actDrawerCopy')?.addEventListener('click', () => this.copyQuery());
        document.getElementById('actDrawerKill')?.addEventListener('click', () => {
            if (this.selectedId) this.kill(this.selectedId);
        });
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isDrawerOpen()) this.closeDrawer();
        });
    },

    // ---- lifecycle -------------------------------------------------------

    start(range, refreshRate) {
        this.range = range || this.range;
        this.refreshRate = refreshRate || this.refreshRate;
        this.isActive = true;
        this.renderLive();
        this.loadStream();
        this.loadSummary(true);
        this.schedule();
    },

    stop() {
        this.isActive = false;
        clearInterval(this.streamTimer);
        clearInterval(this.summaryTimer);
        this.streamTimer = this.summaryTimer = null;
        this.streamAbort?.abort();
        this.summaryAbort?.abort();
        this.streamAbort = this.summaryAbort = null;
        this.hideTip();
        this.closeDrawer();
    },

    schedule() {
        clearInterval(this.streamTimer);
        clearInterval(this.summaryTimer);
        this.streamTimer = setInterval(() => this.loadStream(), this.refreshRate);
        this.summaryTimer = setInterval(() => this.loadSummary(), Math.max(this.refreshRate * 4, this.SUMMARY_MIN_MS));
    },

    setRefreshRate(ms) {
        this.refreshRate = ms;
        if (this.isActive) this.schedule();
    },

    setRange(range) {
        this.range = range;
        if (!this.isActive) return;
        this.loadStream();
        this.loadSummary(true);
    },

    // ---- data ------------------------------------------------------------

    // A poll is skipped whenever painting it would be wasted or disruptive: the tab
    // is hidden, the pointer is parked on the table, or the previous request has
    // not landed.
    canPoll() {
        return this.isActive && !document.hidden;
    },

    async loadStream() {
        // Cost patterns shows a different table, so the stream is not just
        // unrendered there, it should not be asked for.
        if (!this.canPoll() || this.hovering || this.mode !== 'live') return;
        this.streamAbort?.abort();
        const abort = new AbortController();
        this.streamAbort = abort;
        const params = new URLSearchParams({ range: this.range, limit: '60' });
        if (this.state) params.set('state', this.state);
        if (this.klass) params.set('class', this.klass);
        if (this.text.trim()) params.set('q', this.text.trim());
        if (this.node) params.set('node', this.node);
        try {
            const res = await fetch('/api/v1/admin/activity?' + params.toString(),
                { credentials: 'include', signal: abort.signal });
            const data = await res.json();
            if (!data.success || abort.signal.aborted) return;
            this.rows = data.rows || [];
            this.renderNodes(data.nodes || []);
            this.renderStream();
        } catch (err) {
            if (err.name !== 'AbortError') console.error('[Activity] stream error:', err);
        }
    },

    async loadSummary(force) {
        if (!this.canPoll()) return;
        const now = Date.now();
        if (!force && now - this.lastSummaryAt < this.SUMMARY_MIN_MS) return;
        this.lastSummaryAt = now;
        this.summaryAbort?.abort();
        const abort = new AbortController();
        this.summaryAbort = abort;
        try {
            const params = new URLSearchParams({ range: this.range });
            if (this.mode === 'patterns') params.set('patterns', '1');
            const res = await fetch('/api/v1/admin/activity/summary?' + params.toString(),
                { credentials: 'include', signal: abort.signal });
            const data = await res.json();
            if (!data.success || abort.signal.aborted) return;
            this.summary = data;
            this.renderSummary();
        } catch (err) {
            if (err.name !== 'AbortError') console.error('[Activity] summary error:', err);
        }
    },

    async kill(queryId) {
        if (!queryId) return;
        try {
            const res = await fetch('/api/v1/admin/kill-query?query_id=' + encodeURIComponent(queryId),
                { method: 'POST', credentials: 'include' });
            if (res.ok) {
                window.Toast?.success('Query', 'Kill signal sent');
                this.loadStream();
            } else {
                window.Toast?.error('Query', await Utils.errorMessage(res, 'Failed to kill query'));
            }
        } catch (err) {
            console.error('[Activity] kill error:', err);
            window.Toast?.error('Query', 'Network error');
        }
    },

    // ---- render: chrome --------------------------------------------------

    syncChips() {
        document.querySelectorAll('#actChips .act-chip').forEach(btn => {
            const value = btn.dataset.value;
            const active = btn.dataset.filter === 'state' ? this.state === value : this.klass === value;
            btn.classList.toggle('active', active);
        });
    },

    renderLive() {
        const badge = document.getElementById('actLive');
        const text = document.getElementById('actLiveText');
        if (!badge || !text) return;
        badge.classList.toggle('paused', this.hovering);
        text.textContent = this.hovering ? 'Paused' : 'Live';
    },

    renderNodes(nodes) {
        const sel = document.getElementById('actNode');
        if (!sel) return;
        if (!nodes.length) { sel.style.display = 'none'; return; }
        const signature = nodes.join('|');
        if (sel.dataset.signature !== signature) {
            sel.dataset.signature = signature;
            sel.innerHTML = '<option value="">All nodes</option>' +
                nodes.map(n => `<option value="${this.esc(n)}">${this.esc(n)}</option>`).join('');
            sel.value = this.node;
        }
        sel.style.display = '';
    },

    // ---- render: stream --------------------------------------------------

    renderStream() {
        if (this.mode !== 'live') return;
        const host = document.getElementById('actStreamTable');
        if (!host) return;
        const count = document.getElementById('actStreamCount');
        if (count) count.textContent = this.rows.length;

        if (!this.rows.length) {
            host.innerHTML = '<div class="empty-state" style="min-height: 120px;"><p>No queries match this filter.</p></div>';
            return;
        }

        let html = '<table class="results-table perf-table act-stream"><thead><tr>' +
            '<th>State</th><th>Age</th><th>Source</th><th>Who / Fractal</th><th>Query</th>' +
            '<th class="act-num">Rows read</th><th class="act-num">Read</th><th class="act-num">Memory</th>' +
            '<th>Node</th><th></th></tr></thead><tbody>';

        for (const row of this.rows) {
            const tag = this.parseTag(row.tag);
            const state = row.state || 'finished';
            const running = state === 'running' || state === 'stopping';
            const age = Number(row.age_sec || 0);
            const ageClass = age > 30 ? 'act-age-critical' : age > 5 ? 'act-age-warn' : '';
            const klass = row.class || 'system';
            html += `<tr data-id="${this.esc(row.query_id)}" class="${this.selectedId === row.query_id ? 'act-row-selected' : ''}">` +
                `<td><span class="act-state act-state-${state}"><i class="act-state-dot"></i>${this.STATE_NAMES[state] || state}</span></td>` +
                `<td><span class="act-age ${ageClass}">${running ? this.duration(age * 1000) : this.duration(age * 1000) + ' ago'}</span></td>` +
                `<td><span class="act-source"><i class="act-dot act-c-${klass}"></i>${this.esc(this.sourceLabel(tag, row))}</span></td>` +
                `<td class="act-who">${this.whoCell(tag, row)}</td>` +
                `<td class="act-query-cell"><span class="act-query">${this.esc(this.rowTitle(tag, row))}</span></td>` +
                `<td class="act-num">${this.number(row.read_rows)}</td>` +
                `<td class="act-num">${this.bytes(row.read_bytes)}</td>` +
                `<td class="act-num">${this.bytes(row.memory)}</td>` +
                `<td class="act-num act-node">${this.esc(row.node || '')}</td>` +
                `<td>${this.actionCell(row, state)}</td></tr>`;
        }
        host.innerHTML = html + '</tbody></table>';
    },

    actionCell(row, state) {
        if (state === 'stopping') return '<span class="act-pending">Stopping</span>';
        if (state !== 'running') return '';
        return `<button type="button" class="act-kill" data-id="${this.esc(row.query_id)}">Kill</button>`;
    },

    // The exact feature, falling back to the class when a query carries no tag
    // (ClickHouse's own background work, or an insert).
    sourceLabel(tag, row) {
        if (tag.src) return this.SOURCE_NAMES[tag.src] || tag.src;
        return this.CLASS_NAMES[row.class] || 'System';
    },

    whoCell(tag, row) {
        // Fall back to the ClickHouse user only when it identifies something: the
        // shared 'default' identity names nobody and is pure noise in the column.
        const chUser = row.user === 'default' ? '' : row.user;
        const who = tag.user || chUser || '';
        const fractal = tag.fractal ? this.fractalName(tag.fractal) : '';
        if (!who) return fractal ? `<span class="act-fractal">${this.esc(fractal)}</span>` : '';
        return `<span class="act-user">${this.esc(who)}</span>` +
            (fractal ? `<span class="act-fractal"> &middot; ${this.esc(fractal)}</span>` : '');
    },

    // A labelled query says what it is ("Endpoint Overview / Top processes")
    // rather than making the reader parse generated SQL out of a table cell.
    rowTitle(tag, row) {
        if (row.state === 'error' && row.exception) return row.exception;
        // A name beats a query, and a query beats the SQL it compiled to.
        return tag.label || tag.bql || this.oneLine(row.query || '');
    },

    fractalName(id) {
        const names = window.Performance?.fractalNames;
        return (names && names[id]) || id.slice(0, 8);
    },

    // ---- render: summary -------------------------------------------------

    // Live and Patterns answer different questions over the same data: what is
    // happening now, and what has cost the most over the range. They share one
    // table rather than stacking two sections.
    setMode(mode) {
        if (mode !== 'live' && mode !== 'patterns') return;
        this.mode = mode;
        document.querySelectorAll('#actModes .act-mode').forEach(b =>
            b.classList.toggle('active', b.dataset.mode === mode));
        const live = mode === 'live';
        this.toggle('actChips', live);
        this.toggle('actLive', live);
        this.toggle('actFailStrip', live && this.failureCount() > 0);
        this.setText('actModeHint', live ? '' : 'grouped by normalized query, over the selected range');
        const search = document.getElementById('actSearch');
        if (search) search.style.display = live ? '' : 'none';
        const node = document.getElementById('actNode');
        if (node) node.style.display = (live && node.dataset.signature) ? '' : 'none';
        this.renderTable();
        if (!this.isActive) return;
        if (live) this.loadStream();
        else this.loadSummary(true);
    },

    toggle(id, on) {
        const el = document.getElementById(id);
        if (el) el.style.display = on ? '' : 'none';
    },

    failureCount() {
        return ((this.summary || {}).failures || []).length;
    },

    renderTable() {
        if (this.mode === 'patterns') this.renderPatterns(((this.summary || {}).patterns) || []);
        else this.renderStream();
    },

    renderSummary() {
        const s = this.summary || {};
        this.renderTiles(s);
        this.renderLatency(s);
        this.renderSparklines(s);
        this.renderFailStrip(s.failures || []);
        if (this.mode === 'patterns') this.renderPatterns(s.patterns || []);
    },

    // Buckets arrive as one row per (time, class). Pivot once here so every
    // consumer below reads a plain series.
    series(buckets, klass) {
        const out = [];
        for (const b of buckets || []) {
            if (b.class !== klass) continue;
            out.push({
                t: Number(b.t || 0), n: Number(b.n || 0),
                p50: Number(b.p50 || 0), p95: Number(b.p95 || 0), p99: Number(b.p99 || 0),
                bytes: Number(b.bytes || 0), failures: Number(b.failures || 0)
            });
        }
        return out.sort((a, b) => a.t - b.t);
    },

    renderTiles(s) {
        const running = s.running || {};
        const buckets = s.buckets || [];
        const search = this.series(buckets, 'search');
        const runCount = Number(running.running || 0);
        const oldest = Number(running.oldest_sec || 0);
        const slow = Number(running.slow || 0);

        this.setText('actTileRunning', runCount);
        this.setText('actTileRunningSub', runCount ? `oldest ${this.duration(oldest * 1000)}${slow ? ` · ${slow} over 5s` : ''}` : 'idle');

        const lastP95 = search.length ? search[search.length - 1].p95 : 0;
        const peakP95 = search.reduce((m, b) => Math.max(m, b.p95), 0);
        this.setText('actTileP95', search.length ? this.duration(lastP95) : '--');
        this.setText('actTileP95Sub', search.length ? `peak ${this.duration(peakP95)}` : 'no searches');

        let failures = 0, reads = 0, queries = 0;
        for (const b of buckets) {
            failures += Number(b.failures || 0);
            reads += Number(b.bytes || 0);
            queries += Number(b.n || 0);
        }
        this.setText('actTileFailed', failures);
        const worst = (s.failures || [])[0];
        this.setText('actTileFailedSub', worst ? `${worst.n}× ${this.esc(worst.message || ('code ' + worst.code))}` : 'none');
        document.getElementById('actTileFailCard')?.classList.toggle('act-tile-attn', failures > 0);

        this.setText('actTileRead', this.bytes(reads));
        this.setText('actTileReadSub', `${this.number(queries)} queries`);

    },

    // Latency is drawn on a log scale: p50 and p99 routinely sit two orders of
    // magnitude apart, and on a linear axis one spike flattens everything else.
    renderLatency(s) {
        const wrap = document.getElementById('actLatencyWrap');
        const legend = document.getElementById('actLatencyLegend');
        if (!wrap) return;
        const data = this.series(s.buckets || [], 'search').filter(b => b.n > 0);
        if (data.length < 2) {
            wrap.innerHTML = '<div class="perf-chart-placeholder">Waiting for samples...</div>';
            if (legend) legend.innerHTML = '';
            return;
        }

        const W = 680, H = 200, L = 46, R = 40, T = 10, B = 22;
        const iw = W - L - R, ih = H - T - B;
        let max = 0;
        for (const b of data) max = Math.max(max, b.p99);
        const lo = Math.log10(10), hi = Math.log10(Math.max(max * 1.4, 100));
        const y = v => T + ih - (Math.log10(Math.max(v, 10)) - lo) / (hi - lo) * ih;
        const x = i => L + (data.length === 1 ? iw / 2 : i / (data.length - 1) * iw);

        let svg = '';
        for (const tick of [10, 100, 1000, 10000, 60000]) {
            if (tick > Math.pow(10, hi)) break;
            svg += `<line x1="${L}" x2="${L + iw}" y1="${y(tick).toFixed(1)}" y2="${y(tick).toFixed(1)}" class="act-grid"/>` +
                `<text x="${L - 8}" y="${(y(tick) + 3.5).toFixed(1)}" text-anchor="end" class="act-axis">${this.duration(tick)}</text>`;
        }
        const lines = [
            { key: 'p50', cls: 'act-q50' },
            { key: 'p95', cls: 'act-q95' },
            { key: 'p99', cls: 'act-q99' }
        ];
        for (const line of lines) {
            const pts = data.map((b, i) => `${x(i).toFixed(1)},${y(b[line.key]).toFixed(1)}`).join(' ');
            const last = data[data.length - 1][line.key];
            svg += `<polyline points="${pts}" class="act-line ${line.cls}"/>` +
                `<circle cx="${x(data.length - 1).toFixed(1)}" cy="${y(last).toFixed(1)}" r="3" class="act-endpoint ${line.cls}"/>` +
                `<text x="${L + iw + 7}" y="${(y(last) + 3.5).toFixed(1)}" class="act-axis ${line.cls}">${line.key}</text>`;
        }
        svg += `<text x="${L}" y="${H - 5}" class="act-axis">${this.clock(data[0].t)}</text>` +
            `<text x="${L + iw}" y="${H - 5}" text-anchor="end" class="act-axis">${this.clock(data[data.length - 1].t)}</text>`;
        for (let i = 0; i < data.length; i++) {
            const half = iw / Math.max(data.length - 1, 1) / 2;
            svg += `<rect x="${(x(i) - half).toFixed(1)}" y="${T}" width="${(half * 2).toFixed(1)}" height="${ih}" ` +
                `fill="transparent" data-i="${i}"/>`;
        }
        wrap.innerHTML = `<svg viewBox="0 0 ${W} ${H}" class="act-svg" role="img" aria-label="Search latency quantiles over the selected range">${svg}</svg>`;
        this.latencyData = data;
        this.bindLatencyHover(wrap);
        if (legend) {
            legend.innerHTML = lines.map(l =>
                `<span title="${this.esc(this.quantileMeaning(l.key))}"><i class="act-dot ${l.cls}"></i>${l.key}</span>`).join('');
        }
    },

    // A quantile is only useful once it is read out loud. "p95" is a label; "95%
    // of searches finished within 2.4s" is the sentence an operator can act on.
    quantileSentence(key, ms) {
        const within = this.duration(ms);
        if (key === 'p50') return `Half of searches finished within ${within}`;
        return `${key.slice(1)}% of searches finished within ${within}`;
    },

    quantileMeaning(key) {
        return key === 'p50'
            ? 'The middle search: half finished faster than this'
            : `${key.slice(1)}% of searches finished within this; the slowest ${100 - Number(key.slice(1))}% took longer`;
    },

    bindLatencyHover(wrap) {
        const svg = wrap.querySelector('svg');
        const tip = document.getElementById('actTip');
        if (!svg || !tip) return;
        svg.addEventListener('mousemove', (e) => {
            const i = e.target && e.target.dataset ? Number(e.target.dataset.i) : NaN;
            const b = this.latencyData[i];
            if (!b) { this.hideTip(); return; }
            tip.innerHTML =
                `<div class="act-tip-head">${this.clock(b.t)} &middot; ${this.number(b.n)} ${b.n === 1 ? 'search' : 'searches'}</div>` +
                ['p50', 'p95', 'p99'].map(k =>
                    `<div class="act-tip-line"><i class="act-dot act-q${k.slice(1)}"></i>${this.esc(this.quantileSentence(k, b[k]))}</div>`).join('');
            tip.setAttribute('aria-hidden', 'false');
            tip.classList.add('open');
            const pad = 14;
            const width = tip.offsetWidth;
            tip.style.left = Math.max(pad, Math.min(e.clientX + pad, window.innerWidth - width - pad)) + 'px';
            tip.style.top = Math.max(pad, e.clientY - tip.offsetHeight - pad) + 'px';
        });
        svg.addEventListener('mouseleave', () => this.hideTip());
    },

    hideTip() {
        const tip = document.getElementById('actTip');
        if (!tip) return;
        tip.classList.remove('open');
        tip.setAttribute('aria-hidden', 'true');
    },

    // Small multiples rather than a stacked bar: ingest runs orders of magnitude
    // above search, so a shared scale would render search as a sub-pixel sliver.
    renderSparklines(s) {
        const host = document.getElementById('actSmalls');
        if (!host) return;
        const buckets = s.buckets || [];
        const seconds = Number(s.bucket_seconds || 150) || 150;
        const classes = ['search', 'alert', 'ingest', 'system'];
        let html = '';
        for (const klass of classes) {
            const data = this.series(buckets, klass);
            const rate = data.length ? data[data.length - 1].n / seconds : 0;
            html += `<div class="act-small"><div class="act-small-head">` +
                `<i class="act-dot act-c-${klass}"></i><span class="act-small-name">${this.CLASS_NAMES[klass]}</span>` +
                `<span class="act-small-rate act-c-${klass}-text">${this.rate(rate)}</span></div>` +
                this.sparkline(data.map(b => b.n), klass) + '</div>';
        }
        host.innerHTML = html;
    },

    sparkline(values, klass) {
        if (values.length < 2) return '<div class="act-spark-empty">no activity</div>';
        const W = 150, H = 48;
        const max = Math.max(...values), min = Math.min(...values);
        // A steady rate is a real answer, so draw it as a level line through the
        // middle rather than pinning it to an edge of an empty box.
        const flat = max === min;
        const y = v => flat ? H / 2 : 3 + (1 - (v - min) / (max - min)) * (H - 8);
        const x = i => i / (values.length - 1) * W;
        const line = values.map((v, i) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(' ');
        return `<svg viewBox="0 0 ${W} ${H}" class="act-spark" preserveAspectRatio="none" aria-hidden="true">` +
            `<polygon points="${line} ${W},${H} 0,${H}" class="act-spark-fill act-c-${klass}-fill"/>` +
            `<polyline points="${line}" class="act-spark-line act-c-${klass}-stroke"/></svg>`;
    },

    renderPatterns(patterns) {
        const host = document.getElementById('actStreamTable');
        if (!host) return;
        const count = document.getElementById('actStreamCount');
        if (count) count.textContent = patterns.length;
        if (!patterns.length) {
            const pending = !(this.summary && 'patterns' in this.summary);
            host.innerHTML = `<div class="empty-state" style="min-height: 80px;"><p>${
                pending ? 'Reading the query log...' : 'No queries in range'}</p></div>`;
            return;
        }
        let html = '<table class="results-table perf-table"><thead><tr>' +
            '<th>Source</th><th>Pattern</th><th class="act-num">Runs</th><th class="act-num">p95</th><th class="act-num">Read</th>' +
            '</tr></thead><tbody>';
        for (const p of patterns) {
            const tag = this.parseTag(p.tag);
            const klass = p.class || 'system';
            html += '<tr>' +
                `<td><span class="act-source"><i class="act-dot act-c-${klass}"></i>${this.esc(this.sourceLabel(tag, p))}</span></td>` +
                `<td class="act-query-cell"><span class="act-query">${this.esc(tag.label || this.oneLine(p.sample || ''))}</span></td>` +
                `<td class="act-num">${this.number(p.runs)}</td>` +
                `<td class="act-num">${this.duration(Number(p.p95 || 0))}</td>` +
                `<td class="act-num">${this.bytes(p.bytes)}</td></tr>`;
        }
        host.innerHTML = html + '</tbody></table>';
    },

    // Grouped failure counts are a different fact from the failed rows themselves
    // ("47 x code 241" versus one error). One line of pills carries it; a whole
    // table did not earn its place next to the stream.
    renderFailStrip(failures) {
        const host = document.getElementById('actFailStrip');
        if (!host) return;
        if (!failures.length || this.mode !== 'live') {
            host.style.display = 'none';
            return;
        }
        host.style.display = '';
        host.innerHTML = '<span class="act-failstrip-label">Failures</span>' +
            failures.map(f => {
                const tag = this.parseTag(f.tag);
                const who = tag.label || tag.user || '';
                return `<button type="button" class="act-failpill" data-code="${this.esc(String(f.code))}" title="${this.esc(who)}">` +
                    `<span class="act-code">${this.esc(String(f.code))}</span>` +
                    `<span>${this.esc(f.message || '')}</span>` +
                    `<span class="act-failpill-n">${this.number(f.n)}</span></button>`;
            }).join('');
    },

    // ---- drawer ----------------------------------------------------------

    isDrawerOpen() {
        return document.getElementById('actDrawer')?.classList.contains('open');
    },

    openDrawer(queryId) {
        const row = this.rows.find(r => r.query_id === queryId);
        if (!row) return;
        this.selectedId = queryId;
        this.drawerQuery = row.query || '';
        const tag = this.parseTag(row.tag);
        this.drawerBql = tag.bql || '';

        const meta = document.getElementById('actDrawerMeta');
        if (meta) {
            const bits = [
                this.STATE_NAMES[row.state] || row.state,
                this.sourceLabel(tag, row),
                tag.user || row.user,
                tag.fractal ? this.fractalName(tag.fractal) : '',
                this.duration(Number(row.age_sec || 0) * 1000),
                `${this.bytes(row.memory)} memory`,
                `${this.number(row.read_rows)} rows read`,
                row.node
            ].filter(Boolean);
            meta.innerHTML = bits.map(b => `<span>${this.esc(String(b))}</span>`).join('');
        }
        // A search's own BQL rides along in the tag, so the drawer can show what
        // the analyst wrote above the SQL it was translated into.
        const bqlBlock = document.getElementById('actDrawerBqlBlock');
        const bql = document.getElementById('actDrawerBql');
        if (bqlBlock && bql) {
            if (tag.bql) {
                bql.innerHTML = window.SyntaxHighlight
                    ? SyntaxHighlight.highlight(tag.bql)
                    : this.esc(tag.bql);
                bqlBlock.style.display = '';
            } else {
                bqlBlock.style.display = 'none';
            }
        }
        this.toggle('actDrawerSqlLabel', !!tag.bql);

        const pre = document.getElementById('actDrawerQuery');
        if (pre) {
            // Same highlighter the Query tab uses, so generated SQL reads the same
            // way wherever it is shown. It escapes its input.
            if (window.QueryExecutor && this.drawerQuery) {
                pre.innerHTML = QueryExecutor.highlightSQL(this.drawerQuery);
            } else {
                pre.textContent = this.drawerQuery;
            }
        }
        const detail = document.getElementById('actDrawerDetail');
        if (detail) {
            detail.innerHTML = row.exception
                ? `<p class="act-drawer-sub">Exception</p><div class="act-exception">${this.esc(row.exception)}</div>`
                : '<p class="act-drawer-sub">Per-shard profile</p><div class="act-drawer-loading">Reading the query log...</div>';
        }
        const kill = document.getElementById('actDrawerKill');
        if (kill) kill.style.display = row.state === 'running' ? '' : 'none';

        document.getElementById('actDrawer')?.classList.add('open');
        document.getElementById('actDrawerScrim')?.classList.add('open');
        this.renderStream();
        this.loadDetail(queryId);
    },

    closeDrawer() {
        document.getElementById('actDrawer')?.classList.remove('open');
        document.getElementById('actDrawerScrim')?.classList.remove('open');
        if (this.selectedId) {
            this.selectedId = '';
            this.renderStream();
        }
    },

    async loadDetail(queryId) {
        try {
            const res = await fetch('/api/v1/admin/activity/detail?query_id=' + encodeURIComponent(queryId),
                { credentials: 'include' });
            const data = await res.json();
            if (!data.success || this.selectedId !== queryId) return;
            this.renderDetail(data.shards || []);
        } catch (err) {
            console.error('[Activity] detail error:', err);
        }
    },

    renderDetail(shards) {
        const host = document.getElementById('actDrawerDetail');
        if (!host) return;
        if (!shards.length) {
            host.innerHTML = '<p class="act-drawer-sub">Per-shard profile</p>' +
                '<div class="act-drawer-loading">Not in the query log yet. A running query is written when it finishes.</div>';
            return;
        }
        let html = '<p class="act-drawer-sub">Per-shard profile</p>' +
            '<div class="act-drawer-table"><table class="results-table perf-table"><thead><tr>' +
            '<th>Node</th><th class="act-num">Duration</th><th class="act-num">Parts</th>' +
            '<th class="act-num">Marks read</th><th class="act-num">Skipped</th><th class="act-num">From disk</th>' +
            '</tr></thead><tbody>';
        let marksRead = 0, marksTotal = 0, rowsRead = 0, resultRows = 0;
        for (const s of shards) {
            const read = Number(s.marks_read || 0), total = Number(s.marks_total || 0);
            marksRead += read; marksTotal += total;
            rowsRead += Number(s.read_rows || 0);
            resultRows = Math.max(resultRows, Number(s.result_rows || 0));
            html += '<tr>' +
                `<td class="act-mono">${this.esc(s.node || '')}${Number(s.coordinator) ? ' <span class="act-muted">(coordinator)</span>' : ''}</td>` +
                `<td class="act-num">${this.duration(Number(s.duration_ms || 0))}</td>` +
                `<td class="act-num">${this.number(s.parts)}</td>` +
                `<td class="act-num">${this.number(read)}</td>` +
                `<td class="act-num">${this.number(Math.max(total - read, 0))}</td>` +
                `<td class="act-num">${this.bytes(s.bytes_from_disk)}</td></tr>`;
        }
        html += '</tbody></table></div>';

        // The two ratios that explain a slow scan: how little the index pruned, and
        // how much was read to return how little.
        const findings = [];
        if (marksTotal > 0) {
            const surviving = marksRead / marksTotal * 100;
            findings.push(['Marks scanned', `${surviving.toFixed(1)}% of the table's marks survived the index`,
                surviving > 40]);
        }
        if (rowsRead > 0) {
            findings.push(['Rows read vs returned',
                `${this.number(rowsRead)} read, ${this.number(resultRows)} returned`,
                resultRows > 0 && rowsRead / resultRows > 100000]);
        }
        if (findings.length) {
            html += '<p class="act-drawer-sub">Why it cost what it did</p><div class="act-drawer-table">' +
                '<table class="results-table perf-table"><tbody>' +
                findings.map(([k, v, warn]) =>
                    `<tr><td class="act-muted act-finding-key">${k}</td>` +
                    `<td class="${warn ? 'act-finding-warn' : ''}">${this.esc(v)}</td></tr>`).join('') +
                '</tbody></table></div>';
        }
        host.innerHTML = html;
    },

    async copyQuery() {
        try {
            await navigator.clipboard.writeText(this.drawerBql || this.drawerQuery || '');
            window.Toast?.success('Query', 'Copied to clipboard');
        } catch (err) {
            console.error('[Activity] copy failed:', err);
        }
    },

    // ---- helpers ---------------------------------------------------------

    parseTag(raw) {
        if (!raw) return {};
        try { return JSON.parse(raw) || {}; } catch (err) { return {}; }
    },

    setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value === undefined || value === null ? '--' : String(value);
    },

    esc(value) {
        if (value === undefined || value === null) return '';
        return String(value)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    },

    oneLine(text) {
        const flat = String(text).replace(/\s+/g, ' ').trim();
        return flat.length > 140 ? flat.slice(0, 140) + '…' : flat;
    },

    number(value) {
        const n = Number(value || 0);
        if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
        if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
        if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
        return String(n);
    },

    bytes(value) {
        const n = Number(value || 0);
        if (!n) return '--';
        const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
        let i = 0, v = n;
        while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
        return `${v >= 100 || i === 0 ? Math.round(v) : v.toFixed(1)} ${units[i]}`;
    },

    duration(ms) {
        const n = Number(ms || 0);
        if (n < 1000) return `${Math.round(n)}ms`;
        if (n < 60000) return `${(n / 1000).toFixed(1)}s`;
        const minutes = Math.floor(n / 60000);
        const seconds = Math.round((n % 60000) / 1000);
        if (minutes < 60) return `${minutes}m ${String(seconds).padStart(2, '0')}s`;
        return `${Math.floor(minutes / 60)}h ${String(minutes % 60).padStart(2, '0')}m`;
    },

    rate(perSecond) {
        if (!perSecond) return '0/s';
        if (perSecond < 1) return `${perSecond.toFixed(2)}/s`;
        if (perSecond < 10) return `${perSecond.toFixed(1)}/s`;
        return `${Math.round(perSecond)}/s`;
    },

    clock(unixSeconds) {
        const d = new Date(Number(unixSeconds || 0) * 1000);
        return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
    }
};

window.Activity = Activity;
