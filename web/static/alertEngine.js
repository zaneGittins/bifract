// System > Alerts: the health of the alert engine.
//
// This page is about the engine, not the rules: is evaluation keeping up, what is
// firing, what has been auto-disabled and why, and are the actions landing. The
// rules themselves live on the Alerts tab.
//
// One shape is deliberately absent. alert_executions records a row only when an
// alert triggers, never on a plain evaluation, so per-alert latency history does
// not exist anywhere. The latency chart is the fleet-wide sample series, and
// per-alert exec time is the most recent evaluation only.

const AlertEngine = {
    // Floor for both polls. Nothing here moves faster than the engine's ticker.
    MIN_POLL_MS: 15000,

    initialized: false,
    isActive: false,
    rowsTimer: null,
    summaryTimer: null,
    refreshRate: 5000,
    range: '1h',

    mode: 'alerts',
    text: '',
    fractal: '',

    rows: [],
    summary: null,
    hovering: false,
    searchDebounce: null,
    rowsAbort: null,
    summaryAbort: null,
    lastSummaryAt: 0,
    pollEvery: 0,
    truncated: false,

    // Severity is an ordinal status, so it draws from the reserved status colours
    // rather than the categorical palette, and always ships with its label.
    SEVERITIES: ['critical', 'high', 'medium', 'low'],
    SEVERITY_NAMES: { critical: 'Critical', high: 'High', medium: 'Medium', low: 'Low' },

    init() {
        if (this.initialized) return;
        this.initialized = true;

        const modes = document.getElementById('aeModes');
        if (modes) {
            modes.addEventListener('click', (e) => {
                const btn = e.target.closest('.act-mode');
                if (btn) this.setMode(btn.dataset.mode);
            });
        }

        const search = document.getElementById('aeSearch');
        if (search) {
            search.addEventListener('input', (e) => {
                clearTimeout(this.searchDebounce);
                const value = e.target.value;
                this.searchDebounce = setTimeout(() => {
                    this.text = value;
                    this.loadRows();
                }, 250);
            });
        }

        const fractal = document.getElementById('aeFractal');
        if (fractal) {
            fractal.addEventListener('change', (e) => {
                this.fractal = e.target.value;
                this.loadRows();
                this.loadSummary(true);
            });
        }

        const table = document.getElementById('aeRowsTable');
        if (table) {
            table.addEventListener('mouseenter', () => { this.hovering = true; });
            table.addEventListener('mouseleave', () => { this.hovering = false; this.loadRows(); });
        }

        // A disabled pill goes to the rule that needs fixing, which is the only
        // useful thing to do with it.
        document.getElementById('aeDisabledStrip')?.addEventListener('click', (e) => {
            const pill = e.target.closest('.act-failpill');
            if (pill && pill.dataset.id) this.openAlert(pill.dataset.id);
        });

        this.setMode('alerts');
    },

    // ---- lifecycle -------------------------------------------------------

    start(range, refreshRate) {
        this.range = range || this.range;
        this.refreshRate = refreshRate || this.refreshRate;
        this.isActive = true;
        this.loadFractalOptions();
        this.loadRows();
        this.loadSummary(true);
        this.schedule();
    },

    stop() {
        this.isActive = false;
        clearInterval(this.rowsTimer);
        clearInterval(this.summaryTimer);
        this.rowsTimer = this.summaryTimer = null;
        this.rowsAbort?.abort();
        this.summaryAbort?.abort();
        this.rowsAbort = this.summaryAbort = null;
    },

    // The engine ticks on its own interval, so nothing on this page changes
    // faster than that: polling at the page's refresh rate would re-run the
    // action-results expansion several times per evaluation for identical data.
    pollInterval() {
        const evalSec = Number((this.summary?.summary || {}).eval_interval_sec || 0);
        return Math.max(this.refreshRate, evalSec * 1000, this.MIN_POLL_MS);
    },

    schedule() {
        clearInterval(this.rowsTimer);
        clearInterval(this.summaryTimer);
        const every = this.pollInterval();
        this.pollEvery = every;
        this.rowsTimer = setInterval(() => this.loadRows(), every);
        this.summaryTimer = setInterval(() => this.loadSummary(), every);
    },

    setRefreshRate(ms) {
        this.refreshRate = ms;
        if (this.isActive) this.schedule();
    },

    setRange(range) {
        this.range = range;
        if (!this.isActive) return;
        this.loadRows();
        this.loadSummary(true);
    },

    setMode(mode) {
        if (mode !== 'alerts' && mode !== 'fires') return;
        this.mode = mode;
        document.querySelectorAll('#aeModes .act-mode').forEach(b =>
            b.classList.toggle('active', b.dataset.mode === mode));
        const hint = mode === 'alerts' ? '' : 'from the execution log, bounded by its retention';
        this.setText('aeModeHint', hint);
        this.toggle('aeModeHint', !!hint);
        this.renderDisabledStrip((this.summary || {}).disabled_alerts || []);
        if (this.isActive) this.loadRows();
    },

    // ---- data ------------------------------------------------------------

    canPoll() {
        return this.isActive && !document.hidden;
    },

    params(extra) {
        const p = new URLSearchParams({ range: this.range });
        if (this.text.trim()) p.set('q', this.text.trim());
        if (this.fractal) p.set('fractal', this.fractal);
        Object.entries(extra || {}).forEach(([k, v]) => p.set(k, v));
        return p;
    },

    async loadRows() {
        if (!this.canPoll() || this.hovering) return;
        this.rowsAbort?.abort();
        const abort = new AbortController();
        this.rowsAbort = abort;
        try {
            const res = await fetch('/api/v1/admin/alert-stats/rows?' + this.params({ mode: this.mode }).toString(),
                { credentials: 'include', signal: abort.signal });
            const data = await res.json();
            if (!data.success || abort.signal.aborted) return;
            // A response for a mode the user has since left would render the wrong
            // columns into the current table.
            if (data.mode !== this.mode) return;
            this.rows = data.rows || [];
            this.truncated = !!data.truncated;
            this.renderRows();
        } catch (err) {
            if (err.name !== 'AbortError') console.error('[AlertEngine] rows error:', err);
        }
    },

    async loadSummary(force) {
        if (!this.canPoll()) return;
        const now = Date.now();
        if (!force && now - this.lastSummaryAt < this.MIN_POLL_MS) return;
        this.lastSummaryAt = now;
        this.summaryAbort?.abort();
        const abort = new AbortController();
        this.summaryAbort = abort;
        try {
            const res = await fetch('/api/v1/admin/alert-stats?range=' + encodeURIComponent(this.range),
                { credentials: 'include', signal: abort.signal });
            const data = await res.json();
            if (!data.success || abort.signal.aborted) return;
            this.summary = data;
            this.renderSummary();
            // The first summary reports the engine's tick, so settle onto it.
            if (this.isActive && this.pollEvery !== this.pollInterval()) this.schedule();
        } catch (err) {
            if (err.name !== 'AbortError') console.error('[AlertEngine] summary error:', err);
        }
    },

    async loadFractalOptions() {
        const sel = document.getElementById('aeFractal');
        if (!sel || sel.dataset.loaded) return;
        try {
            const res = await fetch('/api/v1/fractals', { credentials: 'include' });
            const data = await res.json();
            const fractals = (data.data && data.data.fractals) || data.fractals || [];
            let html = '<option value="">All fractals</option>';
            fractals.forEach(f => {
                if (!f.id) return;
                html += `<option value="${this.esc(f.id)}">${this.esc(f.name || f.id)}</option>`;
            });
            sel.innerHTML = html;
            sel.dataset.loaded = '1';
        } catch (err) {
            console.error('[AlertEngine] fractal options error:', err);
        }
    },

    openAlert(id) {
        if (window.App?.pushPath) window.App.pushPath(`/alerts/${id}`);
        else window.location.hash = `#/alerts/${id}`;
    },

    // ---- render: summary -------------------------------------------------

    renderSummary() {
        const s = this.summary || {};
        const sum = s.summary || {};
        const interval = Number(sum.eval_interval_sec || 60);

        this.setText('aeTileEvaluating', this.number(sum.evaluating));
        this.setText('aeTileEvaluatingSub', `every ${this.interval(interval)}`);

        // Lag is the alerting SLA: the engine is cursor-based, so when it grows
        // alerts fire late. Judge it against the configured tick, not a constant.
        const lag = Number(sum.lag_p95_sec || 0);
        const warnAt = Number(sum.lag_warn_sec || interval * 2);
        this.setText('aeTileLag', Number(sum.evaluating) ? this.duration(lag * 1000) : '--');
        this.setText('aeTileLagSub', sum.lag_max_alert
            ? `worst ${this.duration(Number(sum.lag_max_sec || 0) * 1000)} · ${sum.lag_max_alert}`
            : 'nothing being evaluated');
        this.flag('aeTileLagCard', lag > warnAt);

        this.setText('aeTileFires', this.number(sum.fires));
        const span = this.interval(Number(sum.window_minutes || 60) * 60);
        this.setText('aeTileFiresSub', Number(sum.fires)
            ? `${this.number(sum.firing_alerts)} alerts · ${this.number(sum.logs_matched)} logs matched` +
              (Number(sum.throttled) ? ` · ${this.number(sum.throttled)} throttled` : '')
            : `nothing fired in ${span}`);
        this.setText('aeModeWindow', `last ${span}`);

        this.setText('aeTileDisabled', this.number(sum.disabled));
        // Scan for the oldest rather than trusting the list order: the sub-line
        // would silently report whichever entry happened to land last.
        const disabled = s.disabled_alerts || [];
        const oldest = disabled.reduce((min, d) => {
            const t = Date.parse(d.since);
            return t && (!min || t < min) ? t : min;
        }, 0);
        this.setText('aeTileDisabledSub', oldest ? `oldest ${this.ago(oldest)}` : (disabled.length ? 'reasons recorded' : 'none'));
        this.flag('aeTileDisabledCard', Number(sum.disabled) > 0);

        this.setText('aeTileActions', this.number(sum.action_failures));
        this.setText('aeTileActionsSub', Number(sum.action_failures)
            ? `${this.esc(sum.action_worst || 'action')} · of ${this.number(sum.action_total)} attempts`
            : (Number(sum.action_total) ? `${this.number(sum.action_total)} delivered` : 'no actions in range'));
        this.flag('aeTileActionsCard', Number(sum.action_failures) > 0);

        this.renderLatency(s.exec_history || []);
        this.renderFires(s.fires_history || []);
        this.renderDisabledStrip(disabled);
    },

    flag(id, on) {
        document.getElementById(id)?.classList.toggle('act-tile-attn', !!on);
    },

    toggle(id, on) {
        const el = document.getElementById(id);
        if (el) el.style.display = on ? '' : 'none';
    },

    // Fleet average, the only latency the engine actually persists.
    renderLatency(history) {
        const wrap = document.getElementById('aeLatencyWrap');
        if (!wrap) return;
        const data = (history || []).map(p => ({ t: Number(p.time || 0), v: Number(p.avg_ms || 0) }));
        if (data.length < 2) {
            wrap.innerHTML = '<div class="perf-chart-placeholder">Waiting for samples...</div>';
            return;
        }

        const W = 640, H = 190, L = 46, R = 40, T = 10, B = 22;
        const iw = W - L - R, ih = H - T - B;
        const max = Math.max(...data.map(d => d.v)) * 1.25 || 1;
        const y = v => T + ih - (v / max) * ih;
        const x = i => L + i / (data.length - 1) * iw;

        let svg = '';
        for (const tick of this.ticks(max)) {
            svg += `<line class="act-grid" x1="${L}" x2="${L + iw}" y1="${y(tick).toFixed(1)}" y2="${y(tick).toFixed(1)}"/>` +
                `<text class="act-axis" x="${L - 8}" y="${(y(tick) + 3.5).toFixed(1)}" text-anchor="end">${this.duration(tick)}</text>`;
        }
        const pts = data.map((d, i) => `${x(i).toFixed(1)},${y(d.v).toFixed(1)}`).join(' ');
        svg += `<polygon points="${pts} ${(L + iw).toFixed(1)},${T + ih} ${L},${T + ih}" class="ae-area"/>`;
        svg += `<polyline points="${pts}" class="ae-line"/>`;
        const last = data[data.length - 1];
        svg += `<circle cx="${x(data.length - 1).toFixed(1)}" cy="${y(last.v).toFixed(1)}" r="3" class="ae-endpoint"/>` +
            `<text class="act-axis ae-endlabel" x="${L + iw + 7}" y="${(y(last.v) + 3.5).toFixed(1)}">${this.duration(last.v)}</text>` +
            `<text class="act-axis" x="${L}" y="${H - 5}">${this.clock(data[0].t)}</text>` +
            `<text class="act-axis" x="${L + iw}" y="${H - 5}" text-anchor="end">${this.clock(last.t)}</text>`;
        for (let i = 0; i < data.length; i++) {
            const half = iw / Math.max(data.length - 1, 1) / 2;
            svg += `<rect x="${(x(i) - half).toFixed(1)}" y="${T}" width="${(half * 2).toFixed(1)}" height="${ih}" fill="transparent">` +
                `<title>${this.clock(data[i].t)} — alerts averaged ${this.duration(data[i].v)} per evaluation</title></rect>`;
        }
        wrap.innerHTML = `<svg viewBox="0 0 ${W} ${H}" class="act-svg" role="img" aria-label="Fleet average alert evaluation latency">${svg}</svg>`;
    },

    // Stacked by severity. Severity is ordinal, so the stack order carries meaning
    // and doubles as the secondary encoding beside the legend.
    renderFires(history) {
        const wrap = document.getElementById('aeFiresWrap');
        const legend = document.getElementById('aeFiresLegend');
        if (!wrap) return;

        const byTime = new Map();
        for (const p of history || []) {
            const t = Number(p.time || 0);
            if (!byTime.has(t)) byTime.set(t, { t, critical: 0, high: 0, medium: 0, low: 0 });
            const sev = this.SEVERITIES.includes(p.severity) ? p.severity : 'medium';
            byTime.get(t)[sev] += Number(p.n || 0);
        }
        const data = [...byTime.values()].sort((a, b) => a.t - b.t);
        if (!data.length) {
            wrap.innerHTML = '<div class="perf-chart-placeholder">No fires in range</div>';
            if (legend) legend.innerHTML = '';
            return;
        }

        const W = 640, H = 190, L = 36, R = 14, T = 10, B = 22;
        const iw = W - L - R, ih = H - T - B;
        const totals = data.map(d => this.SEVERITIES.reduce((a, k) => a + d[k], 0));
        const max = Math.max(...totals) * 1.15 || 1;
        const step = iw / data.length;
        const bw = Math.max(step * 0.62, 2);

        let svg = '';
        for (const tick of this.ticks(max, true)) {
            const yy = T + ih - (tick / max) * ih;
            svg += `<line class="act-grid" x1="${L}" x2="${L + iw}" y1="${yy.toFixed(1)}" y2="${yy.toFixed(1)}"/>` +
                `<text class="act-axis" x="${L - 8}" y="${(yy + 3.5).toFixed(1)}" text-anchor="end">${tick}</text>`;
        }
        data.forEach((d, i) => {
            const x0 = L + i * step + (step - bw) / 2;
            let acc = 0;
            for (const sev of this.SEVERITIES) {
                const v = d[sev];
                if (!v) continue;
                const h = (v / max) * ih;
                const top = T + ih - ((acc + v) / max) * ih;
                acc += v;
                // 2px surface gap keeps adjacent segments legible.
                svg += `<rect class="ae-sev-${sev}" x="${x0.toFixed(1)}" y="${top.toFixed(1)}" ` +
                    `width="${bw.toFixed(1)}" height="${Math.max(h - 2, 1).toFixed(1)}" rx="1"/>`;
            }
            const parts = this.SEVERITIES.filter(k => d[k]).map(k => `${this.SEVERITY_NAMES[k]} ${d[k]}`);
            svg += `<rect x="${(L + i * step).toFixed(1)}" y="${T}" width="${step.toFixed(1)}" height="${ih}" fill="transparent">` +
                `<title>${this.clock(d.t)} — ${totals[i]} ${totals[i] === 1 ? 'fire' : 'fires'}\n${parts.join('\n')}</title></rect>`;
        });
        svg += `<text class="act-axis" x="${L}" y="${H - 5}">${this.clock(data[0].t)}</text>` +
            `<text class="act-axis" x="${L + iw}" y="${H - 5}" text-anchor="end">${this.clock(data[data.length - 1].t)}</text>`;
        wrap.innerHTML = `<svg viewBox="0 0 ${W} ${H}" class="act-svg" role="img" aria-label="Alert fires over time, stacked by severity">${svg}</svg>`;

        if (legend) {
            legend.innerHTML = this.SEVERITIES.map(sev =>
                `<span><i class="act-dot ae-sev-${sev}"></i>${this.SEVERITY_NAMES[sev]}</span>`).join('');
        }
    },

    renderDisabledStrip(disabled) {
        const host = document.getElementById('aeDisabledStrip');
        if (!host) return;
        if (!disabled.length || this.mode !== 'alerts') {
            host.style.display = 'none';
            return;
        }
        host.style.display = '';
        host.innerHTML = '<span class="act-failstrip-label">Auto-disabled</span>' +
            disabled.map(d =>
                `<button type="button" class="act-failpill" data-id="${this.esc(d.id)}" title="disabled ${this.esc(this.ago(d.since))}">` +
                `<b class="ae-pill-name">${this.esc(d.name)}</b>` +
                `<span class="ae-pill-why">${this.esc(d.reason)}</span></button>`).join('');
    },

    // ---- render: table ---------------------------------------------------

    renderRows() {
        const host = document.getElementById('aeRowsTable');
        if (!host) return;
        const count = document.getElementById('aeRowCount');
        if (count) count.textContent = this.truncated ? `first ${this.rows.length}` : this.rows.length;

        if (!this.rows.length) {
            host.innerHTML = `<div class="empty-state" style="min-height: 120px;"><p>${
                this.mode === 'alerts' ? 'No alerts match this filter.' : 'Nothing fired in this range.'
            }</p></div>`;
            return;
        }
        host.innerHTML = this.mode === 'alerts' ? this.alertsTable() : this.firesTable();
    },

    alertsTable() {
        const warnAt = Number((this.summary?.summary || {}).lag_warn_sec || 120);
        let html = '<table class="results-table perf-table act-stream"><thead><tr>' +
            '<th>Alert</th><th>Fractal</th><th>Severity</th><th class="act-num">Eval lag</th>' +
            '<th class="act-num">Last exec</th><th class="act-num">Fires</th><th class="act-num">Throttled</th>' +
            '<th>Actions</th></tr></thead><tbody>';
        for (const a of this.rows) {
            const off = !a.enabled;
            const lag = Number(a.lag_sec || 0);
            const lagClass = lag > warnAt * 4 ? 'act-age-critical' : lag > warnAt ? 'act-age-warn' : '';
            html += `<tr class="${off ? 'ae-row-off' : ''}">` +
                `<td class="act-user">${this.esc(a.name)}${off ? ' <span class="ae-badge ae-badge-off">disabled</span>' : ''}</td>` +
                `<td class="act-num act-node">${this.esc(this.scopeName(a))}</td>` +
                `<td>${this.severityCell(a.severity)}</td>` +
                `<td class="act-num">${off ? '<span class="act-muted">&mdash;</span>' : `<span class="act-age ${lagClass}">${this.duration(lag * 1000)}</span>`}</td>` +
                `<td class="act-num">${Number(a.exec_ms) ? this.duration(a.exec_ms) : '<span class="act-muted">&mdash;</span>'}</td>` +
                `<td class="act-num">${this.number(a.fires)}</td>` +
                `<td class="act-num">${Number(a.throttled) ? `<span class="ae-badge ae-badge-thr">${this.number(a.throttled)}</span>` : '<span class="act-muted">0</span>'}</td>` +
                `<td>${this.actionSummary(a, off)}</td></tr>`;
        }
        return html + '</tbody></table>';
    },

    firesTable() {
        let html = '<table class="results-table perf-table act-stream"><thead><tr>' +
            '<th>Time</th><th>Alert</th><th>Fractal</th><th>Severity</th>' +
            '<th class="act-num">Logs</th><th>Outcome</th></tr></thead><tbody>';
        for (const f of this.rows) {
            html += '<tr>' +
                `<td class="act-num act-node">${this.esc(this.clockSeconds(f.time))}</td>` +
                `<td class="act-user">${this.esc(f.name)}</td>` +
                `<td class="act-num act-node">${this.esc(this.scopeName(f))}</td>` +
                `<td>${this.severityCell(f.severity)}</td>` +
                `<td class="act-num">${this.number(f.logs)}</td>` +
                `<td>${this.outcomeCell(f)}</td></tr>`;
        }
        return html + '</tbody></table>';
    },

    severityCell(severity) {
        const sev = this.SEVERITIES.includes(severity) ? severity : 'medium';
        // Never colour alone: the label rides with the swatch.
        return `<span class="act-source"><i class="act-dot ae-sev-${sev}"></i>${this.SEVERITY_NAMES[sev]}</span>`;
    },

    actionSummary(a, off) {
        if (off) return '<span class="ae-act ae-act-none"><i></i>not evaluating</span>';
        const total = Number(a.action_total || 0);
        const failed = Number(a.action_failed || 0);
        if (!total) return '<span class="ae-act ae-act-none"><i></i>none</span>';
        if (failed) {
            return `<span class="ae-act ae-act-bad"><i></i>${this.number(failed)} of ${this.number(total)} failed` +
                (a.action_worst ? ` &middot; ${this.esc(a.action_worst)}` : '') + '</span>';
        }
        return `<span class="ae-act ae-act-ok"><i></i>${this.number(total)} delivered</span>`;
    },

    // The outcome of one fire: what was suppressed, or what each action did.
    outcomeCell(f) {
        if (f.throttled) {
            return '<span class="ae-act ae-act-thr"><i></i>throttled' +
                (f.throttle_key ? ` on ${this.esc(f.throttle_key)}` : '') + '</span>';
        }
        const actions = f.actions || [];
        if (!actions.length) return '<span class="ae-act ae-act-none"><i></i>no actions</span>';
        const failed = actions.filter(a => !a.ok);
        if (failed.length) {
            const first = failed[0];
            return `<span class="ae-act ae-act-bad"><i></i>${this.esc(first.kind)} ${this.esc(first.name)}` +
                (first.detail ? ` &rarr; ${this.esc(first.detail)}` : ' failed') +
                (failed.length > 1 ? ` &middot; +${failed.length - 1} more` : '') + '</span>';
        }
        return `<span class="ae-act ae-act-ok"><i></i>${actions.map(a => this.esc(a.name)).join(', ')}</span>`;
    },

    scopeName(row) {
        const names = window.Performance?.fractalNames;
        if (row.fractal_id) return (names && names[row.fractal_id]) || row.fractal_id.slice(0, 8);
        if (row.prism_id) return 'prism';
        return '—';
    },

    // ---- helpers ---------------------------------------------------------

    // Round axis ticks that never crowd: at most four, always including zero.
    ticks(max, whole) {
        const raw = max / 4;
        const mag = Math.pow(10, Math.floor(Math.log10(Math.max(raw, 1))));
        let step = Math.ceil(raw / mag) * mag;
        if (whole) step = Math.max(1, Math.round(step));
        const out = [];
        for (let v = 0; v <= max && out.length < 6; v += step) out.push(v);
        return out;
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

    number(value) {
        const n = Number(value || 0);
        if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
        if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
        if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
        return String(n);
    },

    duration(ms) {
        const n = Number(ms || 0);
        if (n < 1000) return `${Math.round(n)}ms`;
        if (n < 60000) return `${(n / 1000).toFixed(1)}s`;
        const minutes = Math.floor(n / 60000);
        const seconds = Math.round((n % 60000) / 1000);
        if (minutes < 60) return `${minutes}m ${String(seconds).padStart(2, '0')}s`;
        const hours = Math.floor(minutes / 60);
        if (hours < 24) return `${hours}h ${String(minutes % 60).padStart(2, '0')}m`;
        return `${Math.floor(hours / 24)}d ${hours % 24}h`;
    },

    ago(when) {
        const t = typeof when === 'number' ? when : Date.parse(when);
        if (!t) return 'unknown';
        // Prose, so a zero remainder is noise: "6d", not "6d 0h".
        return this.duration(Date.now() - t).replace(/ 0+[hms]$/, '') + ' ago';
    },

    // Prose form: a whole minute reads "1m", not "1m 00s". Columns keep the
    // padded seconds, which is what makes them scannable.
    interval(seconds) {
        const n = Number(seconds || 0);
        if (n < 60) return `${n}s`;
        if (n % 60 === 0) {
            const m = n / 60;
            return m % 60 === 0 && m >= 60 ? `${m / 60}h` : `${m}m`;
        }
        return this.duration(n * 1000);
    },

    clock(unixSeconds) {
        const d = new Date(Number(unixSeconds || 0) * 1000);
        return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
    },

    clockSeconds(iso) {
        const t = Date.parse(iso);
        if (!t) return '--';
        const d = new Date(t);
        return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}`;
    }
};

window.AlertEngine = AlertEngine;
