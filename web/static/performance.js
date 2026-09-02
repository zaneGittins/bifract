// Performance monitoring module (admin-only)

const Performance = {
    initialized: false,
    isActive: false,
    refreshInterval: null,
    refreshRate: 5000,
    timeRange: '1h',
    cpuChart: null,
    ingestChart: null,
    distQueueChart: null,
    ddlQueueChart: null,
    prevCpuTimes: null,
    subTab: 'overview',
    ingestFractal: '',
    ingestDays: 30,
    ingestMetric: 'raw',
    fractalNames: {},
    _ingestData: [],
    _ingestSeries: null,

    // Called from DOMContentLoaded and again from the app's startup sequence, so
    // it must bind its listeners exactly once.
    init() {
        if (this.initialized) return;
        this.initialized = true;

        const refreshSelect = document.getElementById('perfRefreshRate');
        if (refreshSelect) {
            refreshSelect.addEventListener('change', (e) => {
                this.refreshRate = parseInt(e.target.value, 10);
                window.Activity?.setRefreshRate(this.refreshRate);
                window.AlertEngine?.setRefreshRate(this.refreshRate);
                if (this.isActive) {
                    this.stopUpdates();
                    this.startUpdates();
                }
            });
        }
        const rangeSelect = document.getElementById('perfTimeRange');
        if (rangeSelect) {
            rangeSelect.addEventListener('change', (e) => {
                this.timeRange = e.target.value;
                this.destroyCharts();
                this.prevCpuTimes = null;
                window.Activity?.setRange(this.timeRange);
                window.AlertEngine?.setRange(this.timeRange);
                this.refresh();
            });
        }

        window.Activity?.init();
        window.AlertEngine?.init();

        // Ingest-per-day filters (Storage & Ingest sub-tab)
        const ingestFractalSel = document.getElementById('perfIngestFractal');
        if (ingestFractalSel) {
            ingestFractalSel.addEventListener('change', (e) => {
                this.ingestFractal = e.target.value;
                this.loadIngest();
            });
        }
        const ingestModes = document.getElementById('ingestModes');
        if (ingestModes) {
            ingestModes.addEventListener('click', (e) => {
                const btn = e.target.closest('.act-mode');
                if (!btn || btn.dataset.metric === this.ingestMetric) return;
                this.ingestMetric = btn.dataset.metric;
                ingestModes.querySelectorAll('.act-mode').forEach(b =>
                    b.classList.toggle('active', b.dataset.metric === this.ingestMetric));
                // The axis formatter and dataset both change, so rebuild.
                if (this.ingestChart) { this.ingestChart.destroy(); this.ingestChart = null; }
                this.renderIngestChart();
            });
        }

        const ingestDaysSel = document.getElementById('perfIngestDays');
        if (ingestDaysSel) {
            ingestDaysSel.addEventListener('change', (e) => {
                this.ingestDays = parseInt(e.target.value, 10) || 30;
                this.loadIngest();
            });
        }

        // Restore last-used sub-tab.
        const savedTab = sessionStorage.getItem('perfSubTab');
        if (['overview', 'storage', 'activity', 'alerts', 'archive'].includes(savedTab)) {
            this.subTab = savedTab;
        }

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') this.closeArchiveDrawers();
        });
    },

    switchSubTab(name) {
        window.App?.pushSubPath(name);
        this.subTab = name;
        sessionStorage.setItem('perfSubTab', name);
        this.applySubTab(name);
        this.syncActivity();
        this.refresh();
    },

    // Toggles the active sub-tab button and shows the matching pane.
    applySubTab(name) {
        const bar = document.getElementById('perfSubTabs');
        if (bar) {
            bar.querySelectorAll('.alerts-sub-tab').forEach(b =>
                b.classList.toggle('active', b.dataset.subtab === name));
        }
        const panes = {
            overview: 'perfPaneOverview',
            storage: 'perfPaneStorage',
            activity: 'perfPaneActivity',
            alerts: 'perfPaneAlerts',
            archive: 'perfPaneArchive'
        };
        Object.entries(panes).forEach(([k, id]) => {
            const el = document.getElementById(id);
            if (el) el.style.display = (k === name) ? '' : 'none';
        });
    },

    async show(subPath = '') {
        this.isActive = true;
        this.prevCpuTimes = null;
        if (subPath) {
            this.subTab = subPath;
            sessionStorage.setItem('perfSubTab', subPath);
        }
        this.applySubTab(this.subTab);
        await this.loadFractalOptions();
        await this.refresh();
        this.startUpdates();
        this.syncActivity();
    },

    hide() {
        this.isActive = false;
        this.stopUpdates();
        this.destroyCharts();
        window.Activity?.stop();
        window.AlertEngine?.stop();
    },

    // The Activity tab polls its own endpoints, so it runs only while it is the
    // visible sub-tab.
    syncActivity() {
        if (window.Activity) {
            if (this.isActive && this.subTab === 'activity') {
                if (!Activity.isActive) Activity.start(this.timeRange, this.refreshRate);
            } else if (Activity.isActive) {
                Activity.stop();
            }
        }
        if (window.AlertEngine) {
            if (this.isActive && this.subTab === 'alerts') {
                if (!AlertEngine.isActive) AlertEngine.start(this.timeRange, this.refreshRate);
            } else if (AlertEngine.isActive) {
                AlertEngine.stop();
            }
        }
    },

    startUpdates() {
        this.stopUpdates();
        this.refreshInterval = setInterval(() => {
            if (this.isActive) this.refresh();
        }, this.refreshRate);
    },

    stopUpdates() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
            this.refreshInterval = null;
        }
    },

    async refresh() {
        const tab = this.subTab;
        try {
            // Metrics (server + storage cards, CPU) and pressure are always
            // fetched; everything else is scoped to the sub-tab that shows it.
            // The Activity and Alerts tabs poll their own endpoints on their own
            // cadence.
            const metPromise = fetch(`/api/v1/admin/metrics?range=${this.timeRange}`, { credentials: 'include' });
            const pressurePromise = fetch(`/api/v1/system/pressure?range=${this.timeRange}`, { credentials: 'include' });

            const metData = await (await metPromise).json();
            const pressureData = await (await pressurePromise).json();

            if (metData.success) {
                this.renderMetrics(metData.metrics || {}, metData.async_metrics || {}, metData.log_storage || {}, metData.disk || {}, metData.cluster || null);
                if (tab === 'overview') {
                    this.renderCpuChart(
                        metData.cpu_history || [],
                        metData.cpu_history_nodes || null,
                        metData.memory_history || [],
                        metData.memory_history_nodes || null
                    );
                }
            }

            this.renderPressureBanner(pressureData);
            this.renderClusterHealth(pressureData.distribution_queue || null);
            if (tab === 'overview') {
                this.renderDistQueueChart(pressureData.distribution_queue_history || []);
                this.renderDDLQueueChart(pressureData.ddl_queue_history || []);
            }

            if (tab === 'overview') {
                this.loadBackgroundOps();
            }

            if (tab === 'storage') {
                this.loadIngest();
                this.loadHotTable();
            }

            if (tab === 'archive') {
                this.loadArchive();
            }

        } catch (err) {
            console.error('[Performance] refresh error:', err);
        }
    },

    // Populates the ingest fractal filter dropdown (preserves current selection).
    async loadFractalOptions() {
        const sel = document.getElementById('perfIngestFractal');
        if (!sel) return;
        try {
            const res = await fetch('/api/v1/fractals', { credentials: 'include' });
            const data = await res.json();
            const fractals = (data.data && data.data.fractals) || data.fractals || [];
            const current = sel.value;
            this.fractalNames = {};
            let html = '<option value="">All fractals</option>';
            fractals.forEach(f => {
                if (!f.id) return; // empty id = default fractal, covered by "All"
                this.fractalNames[f.id] = f.name || f.id;
                html += `<option value="${this.escapeHtml(f.id)}">${this.escapeHtml(f.name || f.id)}</option>`;
            });
            sel.innerHTML = html;
            sel.value = current;
        } catch (err) {
            console.error('[Performance] fractal options error:', err);
        }
    },

    // Merges, mutations and the replication backlog. A merge running for hours or
    // a mutation that never finishes is the classic production incident, and the
    // single "Active Merges" count above cannot show either.
    async loadBackgroundOps() {
        try {
            const res = await fetch('/api/v1/admin/background', { credentials: 'include' });
            const data = await res.json();
            if (data.success) this.renderBackgroundOps(data);
        } catch (err) {
            console.error('[Performance] background ops error:', err);
        }
    },

    renderBackgroundOps(bg) {
        const host = document.getElementById('actBackground');
        if (!host) return;
        const merges = bg.merges || [];
        const mutations = bg.mutations || [];
        const summary = document.getElementById('actBgSummary');
        if (summary) {
            const parts = [`${merges.length} merge${merges.length === 1 ? '' : 's'}`,
                `${mutations.length} mutation${mutations.length === 1 ? '' : 's'}`];
            if (bg.replication) parts.push(`${Number(bg.replication.queued || 0)} queued parts`);
            summary.textContent = parts.join(' \u00b7 ');
        }
        if (!merges.length && !mutations.length) {
            host.innerHTML = '<div class="empty-state" style="min-height: 80px;"><p>Nothing running</p></div>';
            return;
        }

        const esc = (v) => this.escapeHtml(v === undefined || v === null ? '' : String(v));
        const age = (seconds, warnAt, dangerAt) => {
            const cls = seconds > dangerAt ? 'act-age-critical' : seconds > warnAt ? 'act-age-warn' : '';
            return `<span class="act-age ${cls}">${this.formatDuration(seconds * 1000)}</span>`;
        };

        let html = '<table class="results-table perf-table"><thead><tr>' +
            '<th>Kind</th><th>Table</th><th>Detail</th><th class="act-num">Elapsed</th><th>Progress</th>' +
            '<th class="act-num">Memory</th><th>Node</th></tr></thead><tbody>';
        for (const m of merges) {
            const pct = Math.max(0, Math.min(100, Math.round(Number(m.progress || 0) * 100)));
            html += '<tr>' +
                `<td>${Number(m.is_mutation) ? 'Mutation merge' : 'Merge'}</td>` +
                `<td class="act-mono">${esc(m.table)}</td>` +
                `<td class="act-mono act-muted">${esc(m.detail)}</td>` +
                `<td class="act-num">${age(Number(m.elapsed_sec || 0), 120, 600)}</td>` +
                `<td><span class="act-bar-track"><span class="act-bar" style="width:${pct}%"></span></span>` +
                `<span class="act-num act-muted act-bar-label">${pct}%</span></td>` +
                `<td class="act-num">${this.formatBytes(m.memory || 0)}</td>` +
                `<td class="act-num act-muted">${esc(m.node)}</td></tr>`;
        }
        for (const m of mutations) {
            const failed = !!String(m.fail_reason || '').trim();
            html += '<tr>' +
                '<td>Mutation</td>' +
                `<td class="act-mono">${esc(m.table)}</td>` +
                `<td class="act-mono act-muted">${esc(m.detail)}</td>` +
                `<td class="act-num">${age(Number(m.elapsed_sec || 0), 0, failed ? 0 : 900)}</td>` +
                `<td>${failed ? `<span class="act-fail">${esc(m.fail_reason)}</span>` : `${this.formatNumber(m.parts_to_do || 0)} parts to do`}</td>` +
                '<td class="act-num">--</td>' +
                `<td class="act-num act-muted">${esc(m.node)}</td></tr>`;
        }
        host.innerHTML = html + '</tbody></table>';
    },

    // logs_hot backs alert evaluation but is storage health, so it lives here
    // rather than on the Alerts tab.
    async loadHotTable() {
        try {
            const res = await fetch('/api/v1/admin/hot-table', { credentials: 'include' });
            const data = await res.json();
            if (data.success) this.renderHotTableStats(data.hot_table || null);
        } catch (err) {
            console.error('[Performance] hot table error:', err);
        }
    },

    async loadIngest() {
        try {
            const url = `/api/v1/admin/ingest-daily?days=${this.ingestDays}&fractal=${encodeURIComponent(this.ingestFractal)}`;
            const res = await fetch(url, { credentials: 'include' });
            const data = await res.json();
            if (data.success) {
                this._ingestData = data.days || [];
                this._ingestSeries = (data.series && data.series.length) ? data.series : null;
                this.clampIngestBuckets();
                this.renderIngestChart();
                this.renderIngestTiles();
            }
        } catch (err) {
            console.error('[Performance] ingest load error:', err);
        }
    },

    async loadArchive() {
        try {
            const res = await fetch('/api/v1/system/archive', { credentials: 'include' });
            if (!res.ok) return;
            this.renderArchive(await res.json());
        } catch (err) {
            console.error('[Performance] archive load error:', err);
        }
        // Restore UI: the jobs table refreshes with the tab's poll cadence; the
        // fractal picker loads lazily when the New-restore drawer opens.
        if (!this._restoreInit) {
            this._restoreInit = true;
            this.restoreMode = 'restore';
            this.restoreTarget = 'existing';
            this._restoreSelected = new Set();
            this.restorePage = 1;
            this.restorePageSize = 20;
            this.restoreStatusFilter = '';
            this.setRestoreMode('restore');
        }
        this.loadRestoreJobs();
    },

    // Request an out-of-schedule maintenance pass. The maintainer polls Postgres
    // for the flag (~10s), so we optimistically show "Queued" and let the next
    // archive refresh reflect the real running/done state.
    async runMaintainNow() {
        const btn = document.getElementById('maintainRunNowBtn');
        if (btn && btn.disabled) return;
        this._maintainRunPending = true;
        if (btn) { btn.disabled = true; btn.textContent = 'Queued…'; }
        try {
            const res = await fetch('/api/v1/system/archive/maintain/run', { method: 'POST', credentials: 'include' });
            if (!res.ok) {
                this._maintainRunPending = false;
                const msg = await Utils.errorMessage(res, 'Failed to request maintenance run');
                if (window.Toast) Toast.error('Maintenance', msg);
                if (btn) { btn.disabled = false; btn.textContent = 'Run now'; }
                return;
            }
            if (window.Toast) Toast.success('Maintenance', 'Run requested; starting shortly.');
            this.loadArchive();
        } catch (err) {
            this._maintainRunPending = false;
            console.error('[Performance] run maintain error:', err);
            if (window.Toast) Toast.error('Maintenance', 'Network error');
            if (btn) { btn.disabled = false; btn.textContent = 'Run now'; }
        }
    },

    renderArchive(d) {
        const dot = document.getElementById('archiveStatusDot');
        const label = document.getElementById('archiveStatusLabel');
        const hint = document.getElementById('archiveHint');
        if (!dot || !label) return;

        // Enabled state: dot + label. Disabled but provisioned = neutral; not
        // provisioned = amber (needs --upgrade).
        if (d.enabled) {
            dot.className = 'status-dot status-enabled';
            label.textContent = 'Archiving enabled';
        } else if (d.provisioned) {
            dot.className = 'status-dot status-disabled';
            label.textContent = 'Archiving disabled';
        } else {
            dot.className = 'status-dot status-auto-disabled';
            label.textContent = 'Not provisioned';
        }

        this.setText('archiveBackend', d.backend || '--');

        const aliveEl = document.getElementById('archiveAlive');
        if (aliveEl) {
            if (!d.provisioned) {
                aliveEl.textContent = 'absent';
            } else if (d.archiver_alive) {
                aliveEl.textContent = 'running';
            } else if (d.enabled) {
                aliveEl.textContent = 'not responding';
            } else {
                aliveEl.textContent = 'idle';
            }
        }

        // Spool usage + pressure.
        const spool = d.spool || {};
        const used = spool.used_bytes || 0;
        const max = spool.max_bytes || 0;
        const pct = max > 0 ? Math.round((used / max) * 100) : 0;
        this.setText('archiveSpoolValue', this.formatBytes(used));
        this.setText('archiveSpoolSub', max > 0 ? `${pct}% of ${this.formatBytes(max)}` : '');
        const spoolValEl = document.getElementById('archiveSpoolValue');
        if (spoolValEl) {
            spoolValEl.className = 'perf-metric-value' +
                (spool.pressure ? ' perf-metric-danger' : pct > 70 ? ' perf-metric-warning' : '');
        }
        const banner = document.getElementById('archiveSpoolBanner');
        if (banner) banner.style.display = spool.pressure ? '' : 'none';

        this.setText('archiveFractals', String(d.fractal_count || 0));
        this.setText('archiveSize', this.formatBytes(d.total_bytes || 0));
        this.setText('archiveRecords', d.total_records ? `${Number(d.total_records).toLocaleString()} records` : '');
        this.setText('archiveLastCommit', d.last_commit_at ? this.timeAgo(d.last_commit_at) : 'never');

        // Maintenance (compaction + snapshot expiry) -- a periodic batch job's
        // last-run summary, distinct from the archiver's always-on heartbeat above.
        // outcome is 'never' until the CronJob has run at least once; on_schedule
        // reflects last_attempt_at (every invocation, including crashes/skips)
        // against the job's known hourly cadence, so a broken or skipped run is
        // visibly distinct from a healthy one that simply had nothing to do.
        const m = d.maintain || {};
        const hasRunOnce = !!m.outcome && m.outcome !== 'never';

        const mDot = document.getElementById('maintainStatusDot');
        const mLabel = document.getElementById('maintainStatusLabel');
        // A 'running' marker only means a pass started; a pass killed mid-flight
        // (OOMKill, eviction) never writes a terminal outcome, so an aged one is
        // checked first. Otherwise it masks every other state indefinitely --
        // including "Overdue" -- and reads as a healthy live pass forever.
        const runningNow = m.outcome === 'running' && !m.stale_running;
        if (mDot && mLabel) {
            if (runningNow) {
                mDot.className = 'status-dot status-enabled';
                mLabel.textContent = 'Running now';
            } else if (m.stale_running) {
                // Ranked above 'Queued': the marker aged out because nothing is left
                // to clear it, so a request sitting behind it will not be serviced
                // either. Showing "Queued" here would promise a pass that never runs.
                mDot.className = 'status-dot status-disabled';
                mLabel.textContent = 'Interrupted';
            } else if (m.run_requested) {
                // Requested from the UI, not yet claimed by the maintainer's poll.
                mDot.className = 'status-dot status-enabled';
                mLabel.textContent = 'Queued';
            } else if (m.outcome === 'interrupted') {
                mDot.className = 'status-dot status-disabled';
                mLabel.textContent = 'Interrupted';
            } else if (m.outcome === 'timeout') {
                mDot.className = 'status-dot status-disabled';
                mLabel.textContent = 'Timed out';
            } else if (!hasRunOnce) {
                mDot.className = 'status-dot status-auto-disabled';
                mLabel.textContent = 'Never run';
            } else if (!m.on_schedule) {
                mDot.className = 'status-dot status-auto-disabled';
                mLabel.textContent = 'Overdue';
            } else if (m.outcome === 'ok') {
                mDot.className = 'status-dot status-enabled';
                mLabel.textContent = 'Healthy';
            } else if (m.outcome === 'error') {
                mDot.className = 'status-dot status-disabled';
                mLabel.textContent = 'Last attempt failed';
            } else {
                mDot.className = 'status-dot status-disabled';
                mLabel.textContent = 'Last attempt skipped';
            }
        }

        // "Run now" button: enabled only when archiving is on and provisioned and
        // no pass is already queued/running. _maintainRunPending covers the brief
        // window between clicking and the maintainer's poll reflecting the request,
        // so the button doesn't flicker back to enabled before the server catches up.
        const runBtn = document.getElementById('maintainRunNowBtn');
        if (runBtn) {
            const serverBusy = runningNow || !!m.run_requested;
            if (serverBusy) this._maintainRunPending = false;
            const busy = serverBusy || this._maintainRunPending;
            runBtn.disabled = !d.enabled || !d.provisioned || busy;
            runBtn.textContent = runningNow ? 'Running…' : (busy ? 'Queued…' : 'Run now');
            runBtn.title = !d.provisioned ? 'Archive not provisioned (run bifract --upgrade)'
                : !d.enabled ? 'Enable archiving to run maintenance'
                : busy ? 'A maintenance pass is in progress'
                : 'Run a compaction + snapshot-expiry pass now';
        }

        this.setText('maintainLastRun', m.last_run_at ? this.timeAgo(m.last_run_at) : 'never');
        this.setText('maintainDuration', m.last_run_at ? `took ${this.formatDuration(m.duration_ms)}` : '');
        this.setText('maintainTables', hasRunOnce ? `${m.compacted || 0} / ${m.tables_seen || 0} compacted` : '--');
        this.setText('maintainExpired', m.expired ? `${m.expired} expired` : '');
        this.setText('maintainGroupsFailed', String(m.groups_failed || 0));
        const groupsFailedEl = document.getElementById('maintainGroupsFailed');
        if (groupsFailedEl) {
            groupsFailedEl.className = 'perf-metric-value' + (m.groups_failed ? ' perf-metric-warning' : '');
        }
        const candidateBytes = m.candidate_bytes || 0;
        const compactedBytes = m.compacted_bytes || 0;
        const hasBacklog = candidateBytes > 0;
        this.setText('maintainBacklog', hasRunOnce
            ? (hasBacklog ? `${this.formatBytes(compactedBytes)} / ${this.formatBytes(candidateBytes)}` : 'caught up')
            : '--');
        this.setText('maintainBacklogSub', hasBacklog
            ? `${Math.round((compactedBytes / candidateBytes) * 100)}% of this pass's backlog`
            : '');

        // Lifecycle: expired archive partitions dropped, and unreferenced files
        // swept. Files rather than bytes -- the orphan sweep reports only counts.
        const retentionFiles = m.retention_files || 0;
        const orphansDeleted = m.orphans_deleted || 0;
        this.setText('maintainReclaimed', hasRunOnce
            ? (retentionFiles || orphansDeleted ? `${retentionFiles + orphansDeleted} files` : 'nothing due')
            : '--');
        const reclaimedParts = [];
        if (retentionFiles) {
            reclaimedParts.push(`${retentionFiles} expired across ${m.retention_tables || 0} fractal(s)`);
        }
        if (orphansDeleted) {
            reclaimedParts.push(`${orphansDeleted} orphaned`);
        }
        this.setText('maintainReclaimedSub', reclaimedParts.join(', '));

        const maintainHint = document.getElementById('maintainHint');
        if (maintainHint) {
            let msg = '';
            if (!hasRunOnce) {
                msg = 'Maintenance has not run yet.';
            } else if (m.stale_running || m.outcome === 'interrupted') {
                msg = 'Last pass was killed before it finished -- usually the maintainer hitting its memory limit. Check the container/pod for OOMKills and raise its memory limit.';
            } else if (m.outcome === 'timeout') {
                msg = 'Last pass was abandoned at its time limit -- object storage is likely unreachable or very slow.';
            } else if (!m.on_schedule) {
                msg = `Last attempt was ${this.timeAgo(m.last_attempt_at)} -- overdue for the hourly schedule. Check the archive-maintain container/pod.`;
            } else if (m.outcome === 'error') {
                msg = `Last attempt failed: ${m.error || 'unknown error'}`;
            } else if (m.outcome === 'skipped_locked') {
                msg = 'Last attempt was skipped -- another maintenance pass was still running.';
            } else if (m.outcome === 'skipped_disabled') {
                msg = 'Last attempt was skipped -- archiving is disabled.';
            } else if (m.groups_failed > 0) {
                msg = `${m.groups_failed} group(s) failed after retries on the last pass -- usually a table under heavy concurrent write load; should clear on a later run.`;
            } else if (hasBacklog) {
                msg = 'Backlog is larger than one pass\'s budget -- still catching up over multiple runs.';
            }
            maintainHint.textContent = msg;
            maintainHint.style.display = msg ? '' : 'none';
        }

        const historyBody = document.getElementById('maintainHistoryBody');
        if (historyBody) {
            const history = m.history || [];
            if (history.length === 0) {
                historyBody.innerHTML = '<tr><td colspan="5" class="restore-empty-cell">No maintenance runs recorded yet.</td></tr>';
            } else {
                historyBody.innerHTML = history.map(h => {
                    const hCandidate = h.candidate_bytes || 0;
                    const hCompacted = h.compacted_bytes || 0;
                    const backlogText = hCandidate > 0
                        ? `${this.formatBytes(hCompacted)} / ${this.formatBytes(hCandidate)}`
                        : (h.outcome === 'ok' ? 'caught up' : '--');
                    return `<tr>
                        <td>${this.timeAgo(h.ran_at)}</td>
                        <td>${this.maintainChip(h.outcome)}</td>
                        <td>${h.outcome === 'ok' ? `${h.compacted || 0} / ${h.tables_seen || 0}` : '--'}</td>
                        <td>${backlogText}</td>
                        <td>${h.outcome === 'ok' ? this.formatDuration(h.duration_ms) : '--'}</td>
                    </tr>`;
                }).join('');
            }
        }

        // Contextual hint.
        if (hint) {
            let msg = '';
            if (!d.provisioned) {
                msg = 'The archiver sidecar is not provisioned. Run bifract --upgrade to add it, then enable archiving in Admin → Settings.';
            } else if (!d.enabled) {
                msg = 'Enable archiving in Admin → Settings to start writing a durable Iceberg copy of all logs.';
            }
            hint.textContent = msg;
            hint.style.display = msg ? '' : 'none';
        }
    },

    // ---- Archive restore -------------------------------------------------

    async loadRestoreFractals() {
        const box = document.getElementById('restoreFractalList');
        if (!box) return;
        try {
            const res = await fetch('/api/v1/fractals', { credentials: 'include' });
            const data = await res.json();
            const fractals = (data.data && data.data.fractals) || data.fractals || [];
            this._restoreFractals = {};
            const withId = fractals.filter(f => f.id);
            if (!withId.length) {
                box.innerHTML = '<div class="restore-empty">No fractals available.</div>';
                return;
            }
            box.innerHTML = withId.map(f => {
                const id = f.id;
                const name = f.name || id;
                this._restoreFractals[id] = name;
                const sel = this._restoreSelected && this._restoreSelected.has(id) ? ' selected' : '';
                return `<button type="button" class="restore-fractal-pill${sel}" data-fid="${this.escapeHtml(id)}" onclick="Performance.toggleRestoreFractal(this)">${this.escapeHtml(name)}</button>`;
            }).join('');
        } catch (err) {
            console.error('[Performance] restore fractals error:', err);
            box.innerHTML = '<div class="restore-empty">Failed to load fractals.</div>';
        }
    },

    toggleRestoreFractal(btn) {
        const fid = btn.getAttribute('data-fid');
        if (!this._restoreSelected) this._restoreSelected = new Set();
        if (this._restoreSelected.has(fid)) {
            this._restoreSelected.delete(fid);
            btn.classList.remove('selected');
        } else {
            this._restoreSelected.add(fid);
            btn.classList.add('selected');
        }
        this.disarmRestore();
    },

    setRestoreMode(mode) {
        this.restoreMode = mode;
        document.querySelectorAll('#restoreModeToggle .restore-mode').forEach(b =>
            b.classList.toggle('active', b.getAttribute('data-mode') === mode));
        const hint = document.getElementById('restoreModeHint');
        const targetField = document.getElementById('restoreTargetField');
        if (mode === 'reconcile') {
            if (hint) hint.textContent = 'Compares hot-store and archive counts, then restores only the rows missing from the hot store (heals a gap).';
            // Reconcile is inherently same-fractal; a new destination is meaningless.
            if (targetField) targetField.style.display = 'none';
            this.setRestoreTarget('existing');
        } else {
            if (hint) hint.textContent = 'Inserts archived rows for the window, skipping any log IDs already present.';
            if (targetField) targetField.style.display = '';
        }
        this.disarmRestore();
    },

    setRestoreTarget(target) {
        this.restoreTarget = target;
        document.querySelectorAll('#restoreTargetToggle .restore-mode').forEach(b =>
            b.classList.toggle('active', b.getAttribute('data-target') === target));
        const hint = document.getElementById('restoreTargetHint');
        const nameRow = document.getElementById('restoreNewFractalRow');
        if (target === 'new') {
            if (hint) hint.textContent = 'Creates a new fractal with no retention and restores the selected source into it. Select a single source fractal.';
            if (nameRow) nameRow.style.display = '';
        } else {
            if (hint) hint.textContent = 'Restores each selected fractal back into itself.';
            if (nameRow) nameRow.style.display = 'none';
        }
        this.disarmRestore();
    },

    restorePreset(days) {
        const to = new Date();
        const from = new Date(to.getTime() - days * 86400000);
        const fromEl = document.getElementById('restoreFrom');
        const toEl = document.getElementById('restoreTo');
        if (fromEl) fromEl.value = this.toInputValue(from);
        if (toEl) toEl.value = this.toInputValue(to);
        this.disarmRestore();
    },

    // The datetime-local fields are wall clock in the user's display zone, which
    // is what the label next to them states.
    toInputValue(d) {
        const t = TZ.format(d.getTime(), 'datetime');
        return t ? t.replace(' ', 'T') : '';
    },

    // Parse a datetime-local value, read as wall clock in the display zone, into
    // a UTC ISO-8601 string, or null.
    inputToUTCISO(v) {
        const ms = TZ.parseWallClock(String(v).trim());
        return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
    },

    setRestoreMsg(text, kind) {
        const el = document.getElementById('restoreFormMsg');
        if (!el) return;
        el.textContent = text || '';
        el.className = 'restore-form-msg' + (kind ? ' ' + kind : '');
    },

    disarmRestore() {
        this._restoreArmed = false;
        clearTimeout(this._restoreArmTimer);
        const btn = document.getElementById('restoreSubmitBtn');
        if (btn) {
            btn.textContent = 'Start restore';
            btn.classList.remove('armed');
        }
    },

    // The server refused because the window predates these fractals' retention.
    // Name them and what they keep, then re-arm so the next confirm carries the
    // acknowledgement. The flag is cleared whenever the form changes, so an
    // override never silently carries over to a different request.
    armRetentionOverride(conflicts) {
        this._restoreAckRetention = true;
        const detail = conflicts.map(c =>
            `${c.fractal_name || c.fractal_id} keeps ${c.retention_days}d`).join(', ');
        this.setRestoreMsg(
            `Retention will delete this data again within the hour (${detail}). ` +
            `Raise retention first, or click Restore anyway.`, 'warn');
        const btn = document.getElementById('restoreSubmitBtn');
        if (btn) { btn.textContent = 'Restore anyway'; btn.classList.add('armed'); }
        this._restoreArmed = true;
        clearTimeout(this._restoreArmTimer);
        this._restoreArmTimer = setTimeout(() => {
            this._restoreAckRetention = false;
            this.disarmRestore();
        }, 12000);
    },

    async submitRestore() {
        const fractals = Array.from(this._restoreSelected || []);
        if (!fractals.length) { this.setRestoreMsg('Select at least one fractal.', 'error'); return; }
        const fromISO = this.inputToUTCISO((document.getElementById('restoreFrom') || {}).value);
        const toISO = this.inputToUTCISO((document.getElementById('restoreTo') || {}).value);
        if (!fromISO || !toISO) { this.setRestoreMsg('Pick a start and end time.', 'error'); return; }
        if (!(new Date(toISO) > new Date(fromISO))) { this.setRestoreMsg('End must be after start.', 'error'); return; }

        const mode = this.restoreMode || 'restore';

        // Destination: same fractal (self-restore) or a new no-retention fractal.
        const targetMode = (mode === 'reconcile') ? 'existing' : (this.restoreTarget || 'existing');
        let newFractalName = '';
        if (targetMode === 'new') {
            if (fractals.length !== 1) { this.setRestoreMsg('Restoring into a new fractal takes a single source fractal.', 'error'); return; }
            newFractalName = ((document.getElementById('restoreNewFractalName') || {}).value || '').trim();
            if (!newFractalName) { this.setRestoreMsg('Name the new fractal.', 'error'); return; }
        }

        // Two-step arm to guard a heavy, hard-to-undo operation without a modal.
        if (!this._restoreArmed) {
            // Fresh confirmation sequence: any previous retention override is
            // void, so a changed window or fractal set is re-checked.
            this._restoreAckRetention = false;
            const confirmMsg = targetMode === 'new'
                ? `Click again to create "${newFractalName}" and restore into it.`
                : `Click again to confirm ${mode} of ${fractals.length} fractal${fractals.length > 1 ? 's' : ''} into the hot store.`;
            this.setRestoreMsg(confirmMsg, 'warn');
            const btn = document.getElementById('restoreSubmitBtn');
            if (btn) { btn.textContent = 'Confirm restore'; btn.classList.add('armed'); }
            this._restoreArmed = true;
            clearTimeout(this._restoreArmTimer);
            this._restoreArmTimer = setTimeout(() => this.disarmRestore(), 6000);
            return;
        }
        this.disarmRestore();

        const btn = document.getElementById('restoreSubmitBtn');
        if (btn) btn.disabled = true;
        this.setRestoreMsg('Enqueuing…', '');
        try {
            const res = await fetch('/api/v1/system/archive/restore', {
                method: 'POST', credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    fractal_ids: fractals, from: fromISO, to: toISO, mode,
                    target_mode: targetMode, new_fractal_name: newFractalName,
                    acknowledge_retention: !!this._restoreAckRetention,
                })
            });
            // The window starts before these fractals' retention horizon, so the
            // hourly retention pass would delete the restored rows again. Surface
            // it and require a second, informed confirmation rather than doing
            // hours of work that gets thrown away.
            if (res.status === 409) {
                const data = await res.json().catch(() => null);
                if (data && data.error === 'retention_conflict') {
                    this.armRetentionOverride(data.conflicts || []);
                    return;
                }
            }
            if (!res.ok) {
                const t = await Utils.errorMessage(res);
                this.setRestoreMsg(t || 'Failed to start restore.', 'error');
                return;
            }
            this._restoreAckRetention = false;
            const data = await res.json();
            const n = (data.job_ids || []).length;
            if (data.target_fractal_name) {
                this.setRestoreMsg(`Created "${data.target_fractal_name}" and queued restore.`, 'ok');
            } else {
                this.setRestoreMsg(`Queued ${n} job${n > 1 ? 's' : ''}.`, 'ok');
            }
            // Reset the destination back to self so the next restore doesn't
            // accidentally reuse the new-fractal choice.
            this.setRestoreTarget('existing');
            const nameEl = document.getElementById('restoreNewFractalName');
            if (nameEl) nameEl.value = '';
            this._restoreSelected = new Set();
            // Reset the view so the new pending jobs are visible, then close.
            this.restoreStatusFilter = '';
            const filterEl = document.getElementById('restoreStatusFilter');
            if (filterEl) filterEl.value = '';
            this.restorePage = 1;
            setTimeout(() => this.closeRestoreForm(), 700);
            this.loadRestoreJobs();
        } catch (err) {
            this.setRestoreMsg('Network error.', 'error');
        } finally {
            if (btn) btn.disabled = false;
        }
    },

    // ---- Drawers ---------------------------------------------------------

    showArchiveDrawer(id) {
        ['restoreFormDrawer', 'restoreDetailDrawer'].forEach(d => {
            const el = document.getElementById(d);
            if (el) el.classList.toggle('open', d === id);
        });
        const scrim = document.getElementById('archiveDrawerScrim');
        if (scrim) scrim.classList.add('show');
    },

    closeArchiveDrawers() {
        ['restoreFormDrawer', 'restoreDetailDrawer'].forEach(d => {
            const el = document.getElementById(d);
            if (el) el.classList.remove('open');
        });
        const scrim = document.getElementById('archiveDrawerScrim');
        if (scrim) scrim.classList.remove('show');
        this._openDetailId = null;
        this.disarmRestore();
    },

    openRestoreForm() {
        this.setRestoreMsg('');
        this.disarmRestore();
        this.loadRestoreFractals();
        this.showArchiveDrawer('restoreFormDrawer');
    },

    closeRestoreForm() { this.closeArchiveDrawers(); },
    closeRestoreDetail() { this.closeArchiveDrawers(); },

    openRestoreDetail(id) {
        const job = this._restoreJobsCache && this._restoreJobsCache[id];
        if (!job) return;
        this._openDetailId = id;
        this.renderRestoreDetail(job);
        this.showArchiveDrawer('restoreDetailDrawer');
    },

    renderRestoreDetail(j) {
        const el = document.getElementById('restoreDetailBody');
        if (!el) return;
        const name = (this._restoreFractals && this._restoreFractals[j.fractal_id]) || j.fractal_id;
        const target = Number(j.target_rows || 0), done = Number(j.rows_restored || 0);
        const chunksTotal = Number(j.chunks_total || 0), chunksDone = Number(j.chunks_done || 0);
        // Chunk completion is the exact progress signal (each ingest-day chunk
        // commits before it is counted); the row target is an estimate, so it is
        // only a fallback when the chunk plan is not yet recorded.
        const pct = chunksTotal > 0
            ? Math.min(100, Math.round((chunksDone / chunksTotal) * 100))
            : (target > 0 ? Math.min(100, Math.round((done / target) * 100)) : (j.status === 'succeeded' ? 100 : 0));
        const row = (k, v) => `<div class="restore-detail-row"><span class="restore-detail-k">${k}</span><span class="restore-detail-v">${v}</span></div>`;

        let html = `<div class="restore-detail-top">${this.statusChip(j.status)}<span class="restore-detail-fractal">${this.escapeHtml(name)}</span></div>`;
        if (j.status === 'running' || j.status === 'succeeded') {
            const detail = chunksTotal > 0 ? `${chunksDone} / ${chunksTotal} days · ` : '';
            html += `<div class="restore-progress"><div class="restore-progress-bar${j.status === 'succeeded' ? ' full' : ''}" style="width:${pct}%"></div></div>
                <div class="restore-detail-stat">${detail}${done.toLocaleString()}${target > 0 ? ' / ~' + target.toLocaleString() : ''} rows${j.status === 'running' ? ' · ' + pct + '%' : ''}</div>`;
        }
        if (j.error) html += `<div class="restore-job-error">${this.escapeHtml(j.error)}</div>`;
        html += '<div class="restore-detail-grid">';
        html += row('Mode', this.escapeHtml(j.mode));
        html += row('Ingested between', `${this.fmtWindow(j.from)} &rarr; ${this.fmtWindow(j.to)} ${TZ.abbrev(j.to)}`);
        if (j.target_fractal_name) html += row('Destination', this.escapeHtml(j.target_fractal_name));
        if (chunksTotal > 0) html += row('Chunks', `${chunksDone} of ${chunksTotal}`);
        if (j.cursor_ts && (j.status === 'failed' || j.status === 'canceled')) {
            html += row('Resumes from', this.fmtStamp(j.cursor_ts));
        }
        html += row('Rows restored', done.toLocaleString() + (target > 0 ? ' of ~' + target.toLocaleString() : ''));
        html += row('Requested by', this.escapeHtml(j.requested_by || '—'));
        html += row('Created', this.fmtStamp(j.created_at));
        if (j.started_at) html += row('Started', this.fmtStamp(j.started_at));
        if (j.finished_at) html += row('Finished', this.fmtStamp(j.finished_at));
        html += row('Job ID', j.id);
        html += row('Batch', `<span class="restore-mono">${this.escapeHtml(j.batch_id || '')}</span>`);
        html += '</div>';
        // Running jobs are cancellable too: the worker kills the in-flight insert
        // on its next heartbeat. Rows already restored stay put, and the cursor
        // means a resume continues from the first unfinished day.
        if (j.status === 'pending' || j.status === 'running') {
            html += `<div class="restore-detail-actions"><button class="restore-cancel" onclick="Performance.cancelRestoreJob(${j.id})">Cancel job</button></div>`;
        } else if (j.status === 'failed' || j.status === 'canceled') {
            html += `<div class="restore-detail-actions"><button class="restore-resume" onclick="Performance.resumeRestoreJob(${j.id})">Resume job</button></div>`;
        }
        el.innerHTML = html;
    },

    // ---- Jobs table ------------------------------------------------------

    async loadRestoreJobs() {
        const body = document.getElementById('restoreJobsBody');
        if (!body) return;
        if (!this.restorePage) this.restorePage = 1;
        if (!this.restorePageSize) this.restorePageSize = 20;
        try {
            const params = new URLSearchParams({ limit: this.restorePageSize, offset: (this.restorePage - 1) * this.restorePageSize });
            if (this.restoreStatusFilter) params.set('status', this.restoreStatusFilter);
            const res = await fetch('/api/v1/system/archive/restore?' + params.toString(), { credentials: 'include' });
            if (!res.ok) return;
            const data = await res.json();
            this.renderRestoreTable(data.data || [], data.page?.total || 0);
            // Keep an open detail drawer live while its job is on the current page.
            if (this._openDetailId != null && this._restoreJobsCache[this._openDetailId]) {
                this.renderRestoreDetail(this._restoreJobsCache[this._openDetailId]);
            }
        } catch (err) {
            console.error('[Performance] restore jobs error:', err);
        }
    },

    renderRestoreTable(jobs, total) {
        const body = document.getElementById('restoreJobsBody');
        if (!body) return;
        this._restoreJobsCache = {};
        if (!jobs.length) {
            const msg = this.restoreStatusFilter ? 'No jobs with this status.' : 'No restore jobs yet.';
            body.innerHTML = `<tr><td colspan="7" class="restore-empty-cell">${msg}</td></tr>`;
        } else {
            body.innerHTML = jobs.map(j => { this._restoreJobsCache[j.id] = j; return this.restoreRow(j); }).join('');
        }
        this.restoreTotal = total || 0;
        this.updatePager();
    },

    restoreRow(j) {
        const srcName = this.escapeHtml((this._restoreFractals && this._restoreFractals[j.fractal_id]) || j.fractal_id);
        // Restore-into-fractal jobs show "source -> destination".
        const name = j.target_fractal_id
            ? `${srcName} <span class="restore-arrow">&rarr;</span> ${this.escapeHtml(j.target_fractal_name || j.target_fractal_id)}`
            : srcName;
        const target = Number(j.target_rows || 0), done = Number(j.rows_restored || 0);
        const chunksTotal = Number(j.chunks_total || 0), chunksDone = Number(j.chunks_done || 0);
        const pct = chunksTotal > 0
            ? Math.min(100, Math.round((chunksDone / chunksTotal) * 100))
            : (target > 0 ? Math.min(100, Math.round((done / target) * 100)) : (j.status === 'succeeded' ? 100 : 0));
        let prog;
        if (j.status === 'running' || j.status === 'succeeded') {
            prog = `<div class="restore-row-progress"><div class="restore-row-bar${j.status === 'succeeded' ? ' full' : ''}" style="width:${pct}%"></div></div>` +
                `<span class="restore-row-progress-txt">${done.toLocaleString()}${target > 0 ? ' / ' + target.toLocaleString() : ''}</span>`;
        } else if (j.status === 'pending') {
            prog = '<span class="restore-muted">queued</span>';
        } else if ((j.status === 'failed' || j.status === 'canceled') && chunksDone > 0) {
            // Partial work survives on the row; show it so the operator can see a
            // resume would not start over.
            prog = `<span class="restore-muted">${chunksDone}${chunksTotal > 0 ? ' / ' + chunksTotal : ''} days done</span>`;
        } else {
            prog = '<span class="restore-muted">&mdash;</span>';
        }
        const action = (j.status === 'pending' || j.status === 'running')
            ? `<button class="restore-row-cancel" onclick="event.stopPropagation();Performance.cancelRestoreJob(${j.id})">Cancel</button>`
            : (j.status === 'failed' || j.status === 'canceled')
                ? `<button class="restore-row-resume" onclick="event.stopPropagation();Performance.resumeRestoreJob(${j.id})">Resume</button>` : '';
        return `<tr class="restore-row" onclick="Performance.openRestoreDetail(${j.id})">
            <td class="restore-td-fractal">${name}</td>
            <td class="restore-td-mode">${this.escapeHtml(j.mode)}</td>
            <td class="restore-td-window">${this.fmtWindow(j.from)} &rarr; ${this.fmtWindow(j.to)}</td>
            <td>${this.statusChip(j.status)}</td>
            <td class="restore-col-progress">${prog}</td>
            <td class="restore-td-by">${this.escapeHtml(j.requested_by || '')}<span class="restore-row-ago">${this.timeAgo(j.created_at)}</span></td>
            <td class="restore-td-action">${action}</td>
        </tr>`;
    },

    statusChip(status) {
        const label = { pending: 'Queued', running: 'Running', succeeded: 'Done', failed: 'Failed', canceled: 'Canceled' }[status] || status;
        return `<span class="restore-chip restore-chip-${status}">${label}</span>`;
    },

    // Reuses the restore-jobs status chip styling (succeeded/failed/pending)
    // for maintain-run outcomes, since there's no dedicated chip palette for
    // this new, smaller set of states.
    maintainChip(outcome) {
        const map = {
            ok: { cls: 'succeeded', label: 'OK' },
            error: { cls: 'failed', label: 'Error' },
            skipped_locked: { cls: 'pending', label: 'Skipped (busy)' },
            skipped_disabled: { cls: 'pending', label: 'Skipped (disabled)' },
            interrupted: { cls: 'failed', label: 'Interrupted' },
            timeout: { cls: 'failed', label: 'Timed out' },
            never: { cls: 'pending', label: 'Never run' },
        };
        const entry = map[outcome] || { cls: 'pending', label: outcome || 'Unknown' };
        return `<span class="restore-chip restore-chip-${entry.cls}">${entry.label}</span>`;
    },

    onRestoreFilterChange() {
        this.restoreStatusFilter = document.getElementById('restoreStatusFilter')?.value || '';
        this.restorePage = 1;
        this.loadRestoreJobs();
    },

    restorePage(delta) {
        const maxPage = Math.max(1, Math.ceil((this.restoreTotal || 0) / this.restorePageSize));
        const next = Math.min(maxPage, Math.max(1, (this.restorePage || 1) + delta));
        if (next === this.restorePage) return;
        this.restorePage = next;
        this.loadRestoreJobs();
    },

    updatePager() {
        const info = document.getElementById('restorePagerInfo');
        const prev = document.getElementById('restorePagerPrev');
        const next = document.getElementById('restorePagerNext');
        const total = this.restoreTotal || 0;
        const size = this.restorePageSize;
        const page = this.restorePage || 1;
        const start = total === 0 ? 0 : (page - 1) * size + 1;
        const end = Math.min(total, page * size);
        if (info) info.textContent = total === 0 ? '0 jobs' : `${start}–${end} of ${total}`;
        if (prev) prev.disabled = page <= 1;
        if (next) next.disabled = end >= total;
    },

    fmtWindow(iso) {
        return TZ.format(iso, 'minute') || '--';
    },

    fmtStamp(iso) {
        const t = TZ.format(iso, 'minute');
        return t ? `${t} ${TZ.abbrev(iso)}` : '—';
    },

    async cancelRestoreJob(id) {
        try {
            await fetch(`/api/v1/system/archive/restore/${id}/cancel`, { method: 'POST', credentials: 'include' });
        } catch (err) {
            console.error('[Performance] cancel restore error:', err);
        }
        this.loadRestoreJobs();
    },

    // Requeue a failed/canceled job. The stored cursor means it continues from the
    // first unfinished ingest day rather than replaying the whole window.
    async resumeRestoreJob(id) {
        try {
            await fetch(`/api/v1/system/archive/restore/${id}/resume`, { method: 'POST', credentials: 'include' });
        } catch (err) {
            console.error('[Performance] resume restore error:', err);
        }
        this.loadRestoreJobs();
    },

    timeAgo(iso) {
        const t = new Date(iso).getTime();
        if (isNaN(t)) return '--';
        const s = Math.max(0, Math.floor((Date.now() - t) / 1000));
        if (s < 60) return `${s}s ago`;
        if (s < 3600) return `${Math.floor(s / 60)}m ago`;
        if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
        return `${Math.floor(s / 86400)}d ago`;
    },

    fractalLabel(id) {
        if (id === '__other__') return 'Other';
        if (!id) return 'Default';
        return this.fractalNames[id] || id;
    },

    // Bounds what the chart is asked to draw. Bucket count is server-side
    // capped, but a mismatched server version returning a huge window would
    // otherwise lock the tab: every bucket is a formatted label plus one bar
    // per fractal series. The window starts at the requested lookback, so the
    // leading buckets are the real data and the tail is future-dated overrun.
    clampIngestBuckets() {
        const max = 400;
        const total = this._ingestData.length;
        if (total <= max) return;
        this._ingestData = this._ingestData.slice(0, max);
        if (this._ingestSeries) {
            this._ingestSeries = this._ingestSeries.map(s => ({
                fractal_id: s.fractal_id,
                raw_bytes: (s.raw_bytes || []).slice(0, max),
                disk_bytes: (s.disk_bytes || []).slice(0, max),
                rows: (s.rows || []).slice(0, max)
            }));
        }
        console.warn(`[Performance] ingest window truncated to ${max} of ${total} days`);
    },

    renderIngestChart() {
        const canvas = document.getElementById('perfIngestChart');
        if (!canvas) return;
        const placeholder = document.getElementById('perfIngestPlaceholder');
        const days = this._ingestData || [];

        if (days.length === 0) {
            if (this.ingestChart) { this.ingestChart.destroy(); this.ingestChart = null; }
            if (placeholder) { placeholder.style.display = ''; placeholder.textContent = 'No ingest data'; }
            return;
        }
        if (placeholder) placeholder.style.display = 'none';

        const cv = window.ThemeManager ? ThemeManager.getCSSVar : (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
        const chartText = cv('--chart-text') || '#e8eaed';
        const chartGrid = cv('--chart-grid') || '#24243e';
        const chartBg = cv('--chart-bg') || '#1a1a2e';
        const chartBorder = cv('--chart-border') || '#24243e';
        const accent = cv('--accent-primary') || '#9c6ade';

        const self = this;
        const labels = days.map(d => this.formatDay(d.day));
        const stacked = Array.isArray(this._ingestSeries) && this._ingestSeries.length > 0;
        const metric = this.ingestMetric || 'raw';
        const seriesKey = metric === 'rows' ? 'rows' : metric === 'disk' ? 'disk_bytes' : 'raw_bytes';
        const isRows = metric === 'rows';
        const fmt = (v) => isRows ? this.formatCount(v) : this.formatBytes(v);

        // The header states the window's totals, which no individual bar does.
        const total = days.reduce((a, d) => a + Number(d[seriesKey] || 0), 0);
        const totalRows = days.reduce((a, d) => a + Number(d.rows || 0), 0);
        this.setText('ingestSummary', total
            ? `${days.length} days \u00b7 ${fmt(total)}${isRows ? '' : ' \u00b7 ' + this.formatCount(totalRows) + ' rows'}`
            : '');

        let datasets;
        if (stacked) {
            datasets = this._ingestSeries.map((s, i) => {
                const color = s.fractal_id === '__other__'
                    ? '#6b7280'
                    : this.nodeColors[i % this.nodeColors.length];
                return {
                    label: this.fractalLabel(s.fractal_id),
                    data: s[seriesKey] || [],
                    backgroundColor: color + 'cc',
                    hoverBackgroundColor: color,
                    borderRadius: 2,
                    maxBarThickness: 40,
                    _disk: s.disk_bytes || [],
                    _rows: s.rows || []
                };
            });
        } else {
            datasets = [{
                label: isRows ? 'Rows' : metric === 'disk' ? 'On disk' : 'Uncompressed',
                data: days.map(d => d[seriesKey]),
                backgroundColor: accent + 'cc',
                hoverBackgroundColor: accent,
                borderRadius: 3,
                maxBarThickness: 40
            }];
        }

        // Toggling between stacked and single changes the dataset shape and axis
        // config; rebuild the chart in that case rather than patching it.
        if (this.ingestChart && (this.ingestChart._stacked !== stacked || this.ingestChart._metric !== metric)) {
            this.ingestChart.destroy();
            this.ingestChart = null;
        }

        if (this.ingestChart) {
            this.ingestChart.data.labels = labels;
            this.ingestChart.data.datasets = datasets;
            this.ingestChart.update('none');
            return;
        }

        const ctx = canvas.getContext('2d');
        this.ingestChart = new Chart(ctx, {
            type: 'bar',
            data: { labels: labels, datasets: datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        display: stacked,
                        position: 'top',
                        labels: {
                            color: chartText,
                            font: { family: 'Inter', size: 11 },
                            boxWidth: 12,
                            padding: 8
                        }
                    },
                    tooltip: {
                        backgroundColor: chartBg,
                        titleColor: chartText,
                        bodyColor: chartText,
                        borderColor: chartBorder,
                        borderWidth: 1,
                        filter: (item) => !stacked || item.parsed.y > 0,
                        callbacks: stacked ? {
                            title: (items) => items.length ? (self._ingestData[items[0].dataIndex] || {}).day || '' : '',
                            label: (ctx) => ctx.dataset.label + ': ' + fmt(ctx.parsed.y || 0),
                            footer: (items) => {
                                const total = items.reduce((sum, it) => sum + (it.parsed.y || 0), 0);
                                return 'Total: ' + fmt(total);
                            }
                        } : {
                            title: (items) => {
                                const row = self._ingestData[items[0].dataIndex];
                                return row ? row.day : '';
                            },
                            label: (ctx) => {
                                const row = self._ingestData[ctx.dataIndex] || {};
                                return [
                                    'Uncompressed: ' + self.formatBytes(row.raw_bytes || 0),
                                    'On disk: ' + self.formatBytes(row.disk_bytes || 0),
                                    'Rows: ' + self.formatCount(row.rows || 0)
                                ];
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        stacked: stacked,
                        grid: { display: false, drawBorder: false },
                        ticks: {
                            color: chartText,
                            font: { family: 'Inter', size: 10 },
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 12
                        }
                    },
                    y: {
                        stacked: stacked,
                        beginAtZero: true,
                        grid: { color: chartGrid, drawBorder: false },
                        ticks: {
                            color: chartText,
                            font: { family: 'Inter', size: 10 },
                            callback: (value) => fmt(value)
                        }
                    }
                }
            }
        });
        // BUG 2: _metric was compared but never stored, so the guard above was
        // always true and the chart was destroyed and rebuilt on every poll
        // instead of updating in place.
        this.ingestChart._stacked = stacked;
        this.ingestChart._metric = metric;
    },

    renderPressureBanner(data) {
        const existing = document.getElementById('perfPressureBanner');
        if (!data || !data.alerts_deferred) {
            if (existing) existing.remove();
            return;
        }
        if (existing) return; // already showing

        const banner = document.createElement('div');
        banner.id = 'perfPressureBanner';
        banner.className = 'system-pressure-banner';
        banner.innerHTML = `
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>
                <line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>
            </svg>
            Alert evaluation is temporarily deferred due to high ingestion load. Cursors are preserved and alerts will catch up automatically.
        `;

        const header = document.querySelector('.performance-header');
        if (header && header.parentNode) {
            header.parentNode.insertBefore(banner, header.nextSibling);
        }
    },

    renderClusterHealth(dq) {
        const section = document.getElementById('clusterSection');
        if (!section) return;
        section.style.display = dq ? '' : 'none';
    },

    // Renders the four SERVER overview cards. In cluster mode the node-local
    // system.metrics gauges only reflect one node, so the backend supplies a
    // `cluster` object with fanned-out aggregates: queries and merges are summed
    // cluster-wide, memory becomes the worst node's utilization, and Uptime is
    // repurposed as shard health. Single-node keeps the original node-local view.
    renderServerCards(metrics, asyncMetrics, cluster) {
        const memLabel = document.getElementById('metricMemoryLabel');
        const memSub = document.getElementById('metricMemorySub');
        const upLabel = document.getElementById('metricUptimeLabel');
        const upSub = document.getElementById('metricUptimeSub');

        if (cluster) {
            this.setText('metricActiveQueries', cluster.active_queries || 0);
            this.setText('metricMerges', cluster.active_merges || 0);

            // Card 2: worst-node memory utilization.
            const pct = typeof cluster.mem_peak_pct === 'number' ? cluster.mem_peak_pct : 0;
            if (memLabel) memLabel.textContent = 'Peak Memory';
            const memEl = document.getElementById('metricMemory');
            if (memEl) {
                memEl.textContent = pct.toFixed(0) + '%';
                memEl.className = 'perf-metric-value' +
                    (pct > 85 ? ' perf-metric-danger' : pct > 70 ? ' perf-metric-warning' : '');
            }
            if (memSub) memSub.textContent = cluster.mem_peak_node ? 'on ' + cluster.mem_peak_node : '';

            // Card 4: shard health, repurposed from Uptime. A shard is healthy when
            // at least one of its replicas is reachable (data stays available).
            const total = cluster.nodes_total || 0;
            const healthy = cluster.nodes_healthy || 0;
            if (upLabel) upLabel.textContent = 'Shards';
            const upEl = document.getElementById('metricUptime');
            if (upEl) {
                upEl.textContent = healthy + ' / ' + total;
                upEl.className = 'perf-metric-value' +
                    (total > 0 && healthy < total ? ' perf-metric-danger' : '');
            }
            if (upSub) upSub.textContent = (total > 0 && healthy < total)
                ? (total - healthy) + ' degraded' : 'all healthy';
            return;
        }

        // Single-node: node-local gauges.
        if (memLabel) memLabel.textContent = 'Memory Usage';
        if (memSub) memSub.textContent = '';
        if (upLabel) upLabel.textContent = 'Uptime';
        if (upSub) upSub.textContent = '';

        const memEl = document.getElementById('metricMemory');
        if (memEl) memEl.className = 'perf-metric-value';
        const upEl = document.getElementById('metricUptime');
        if (upEl) upEl.className = 'perf-metric-value';

        this.setText('metricActiveQueries', metrics['Query'] || 0);
        this.setText('metricMemory', this.formatBytes(metrics['MemoryTracking'] || 0));
        this.setText('metricMerges', metrics['Merge'] || 0);
        this.setText('metricUptime', this.formatUptime(asyncMetrics['Uptime'] || 0));
    },

    renderMetrics(metrics, asyncMetrics, logStorage, disk, cluster) {
        this.renderServerCards(metrics, asyncMetrics, cluster);

        // Rows and compression ride the storage tile's sub-line rather than taking
        // a card each: they qualify the number above them, they are not separate
        // questions.
        const logRows = logStorage['log_rows'] || 0;
        const compressedBytes = logStorage['compressed_bytes'] || 0;
        const uncompressedBytes = logStorage['uncompressed_bytes'] || 0;

        this.setText('metricLogStorage', this.formatBytes(compressedBytes));
        const parts = [];
        if (logRows) parts.push(this.formatCount(logRows) + ' rows');
        if (compressedBytes > 0 && uncompressedBytes > 0) {
            parts.push(((1 - compressedBytes / uncompressedBytes) * 100).toFixed(0) + '% compressed');
        }
        this.setText('metricLogStorageSub', parts.join(' \u00b7 '));

        const diskPct = typeof disk['used_pct'] === 'number' ? disk['used_pct'] : 0;
        const diskEl = document.getElementById('metricDiskUsage');
        if (diskEl) {
            diskEl.textContent = diskPct + '%';
            diskEl.className = 'perf-metric-value' +
                (diskPct > 85 ? ' perf-metric-danger' : diskPct > 70 ? ' perf-metric-warning' : '');
        }
        this.setText('metricDiskFree', disk['free_space'] ? disk['free_space'] + ' free' : '');
        document.getElementById('metricDiskCard')?.classList.toggle('act-tile-attn', diskPct > 85);

        this._diskFreeBytes = Number(disk['free_bytes'] || 0);
        this.renderIngestTiles();
    },

    // Ingest rate and disk runway are derived from the ingest series, so they are
    // recomputed whenever either the series or the disk reading changes.
    renderIngestTiles() {
        const days = this._ingestData || [];
        // The newest day is still filling, so it would drag any average down.
        const complete = days.length > 1 ? days.slice(0, -1) : days;
        const sample = complete.slice(-7);
        if (!sample.length) {
            this.setText('metricIngestRate', '--');
            this.setText('metricIngestRateSub', 'no ingest in window');
            this.setText('metricRunway', '--');
            this.setText('metricRunwaySub', '');
            return;
        }

        const avgRaw = sample.reduce((a, d) => a + Number(d.raw_bytes || 0), 0) / sample.length;
        const avgDisk = sample.reduce((a, d) => a + Number(d.disk_bytes || 0), 0) / sample.length;
        const avgRows = sample.reduce((a, d) => a + Number(d.rows || 0), 0) / sample.length;
        this.setText('metricIngestRate', this.formatBytes(avgRaw));
        this.setText('metricIngestRateSub',
            `${this.formatCount(avgRows)} rows/day \u00b7 ${sample.length}d average`);

        // Runway is deliberately conservative: it divides free space by what is
        // being written and ignores retention and tiering, both of which give
        // space back. A number that errs early is the useful direction for a
        // disk warning.
        const free = Number(this._diskFreeBytes || 0);
        const runwayEl = document.getElementById('metricRunway');
        if (!free || avgDisk <= 0) {
            this.setText('metricRunway', '--');
            this.setText('metricRunwaySub', avgDisk > 0 ? 'disk size unavailable' : 'nothing being written');
            document.getElementById('metricRunwayCard')?.classList.remove('act-tile-attn');
            if (runwayEl) runwayEl.className = 'perf-metric-value';
            return;
        }
        const days_ = free / avgDisk;
        if (runwayEl) {
            runwayEl.textContent = days_ >= 365 ? '> 1y'
                : days_ >= 1 ? Math.round(days_) + 'd'
                : '< 1d';
            runwayEl.className = 'perf-metric-value' +
                (days_ < 14 ? ' perf-metric-danger' : days_ < 45 ? ' perf-metric-warning' : '');
        }
        this.setText('metricRunwaySub', `at ${this.formatBytes(avgDisk)}/day on disk, before retention`);
        document.getElementById('metricRunwayCard')?.classList.toggle('act-tile-attn', days_ < 14);
    },

    // Per-node color palette for multi-node CPU charts.
    nodeColors: [
        '#9c6ade', '#4ecdc4', '#ff6b6b', '#ffd93d',
        '#6bcb77', '#4d96ff', '#ff8fab', '#c9b1ff'
    ],

    renderCpuChart(cpuHistory, cpuHistoryNodes, memHistory, memHistoryNodes) {
        const canvas = document.getElementById('perfCpuChart');
        if (!canvas) return;

        const placeholder = document.getElementById('perfCpuPlaceholder');
        const isMultiNode = cpuHistoryNodes && Object.keys(cpuHistoryNodes).length > 0;
        const hasSingle = cpuHistory && cpuHistory.length > 0;
        const isMultiMem = memHistoryNodes && Object.keys(memHistoryNodes).length > 0;
        const hasSingleMem = memHistory && memHistory.length > 0;
        const hasMem = isMultiMem || hasSingleMem;

        if (!isMultiNode && !hasSingle) {
            if (placeholder) placeholder.style.display = '';
            return;
        }
        if (placeholder) placeholder.style.display = 'none';

        const cv = window.ThemeManager ? ThemeManager.getCSSVar : (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
        const chartText = cv('--chart-text') || '#e8eaed';
        const chartGrid = cv('--chart-grid') || '#24243e';
        const chartBg = cv('--chart-bg') || '#1a1a2e';
        const chartBorder = cv('--chart-border') || '#24243e';
        const accentColor = cv('--accent-primary') || '#9c6ade';
        const memColor = '#4ecdc4';

        const longRange = this.timeRange === '7d' || this.timeRange === '30d';
        const showDate = longRange || this.timeRange === '8h' || this.timeRange === '24h';
        const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

        const extractLabel = (rawTime) => {
            const d = new Date(Number(rawTime) * 1000);
            if (isNaN(d.getTime())) return String(rawTime || '');
            const hhmm = d.toTimeString().slice(0, 5);
            if (!showDate) return hhmm;
            return `${months[d.getMonth()]} ${d.getDate()} ${hhmm}`;
        };

        let labels, datasets;

        if (isMultiNode) {
            // Unified time axis across all CPU and memory node series.
            const timeSet = new Set();
            for (const points of Object.values(cpuHistoryNodes)) {
                for (const p of points) timeSet.add(String(p.time || ''));
            }
            if (isMultiMem) {
                for (const points of Object.values(memHistoryNodes)) {
                    for (const p of points) timeSet.add(String(p.time || ''));
                }
            }
            const sortedTimes = Array.from(timeSet).sort((a, b) => Number(a) - Number(b));
            labels = sortedTimes.map(t => extractLabel(t));

            const nodes = Object.keys(cpuHistoryNodes).sort();
            datasets = [];
            nodes.forEach((node, i) => {
                const color = this.nodeColors[i % this.nodeColors.length];
                const timeMap = {};
                for (const p of cpuHistoryNodes[node]) timeMap[String(p.time || '')] = p.value;
                datasets.push({
                    label: hasMem ? node + ' cpu' : node,
                    data: sortedTimes.map(t => timeMap[t] !== undefined ? timeMap[t] : null),
                    borderColor: color,
                    backgroundColor: color + '1a',
                    borderWidth: 2,
                    fill: false,
                    tension: 0.3,
                    pointRadius: sortedTimes.length > 60 ? 0 : 2,
                    pointHoverRadius: 4,
                    pointBackgroundColor: color,
                    spanGaps: true
                });
            });
            // Memory overlay per node — same color as CPU but dashed.
            if (isMultiMem) {
                const memNodes = Object.keys(memHistoryNodes).sort();
                memNodes.forEach((node) => {
                    const i = nodes.indexOf(node);
                    const color = this.nodeColors[(i >= 0 ? i : 0) % this.nodeColors.length];
                    const timeMap = {};
                    for (const p of memHistoryNodes[node]) timeMap[String(p.time || '')] = p.value;
                    datasets.push({
                        label: node + ' mem',
                        data: sortedTimes.map(t => timeMap[t] !== undefined ? timeMap[t] : null),
                        borderColor: color,
                        backgroundColor: 'transparent',
                        borderWidth: 1.5,
                        borderDash: [4, 4],
                        fill: false,
                        tension: 0.3,
                        pointRadius: 0,
                        pointHoverRadius: 3,
                        pointBackgroundColor: color,
                        spanGaps: true
                    });
                });
            }
        } else {
            labels = cpuHistory.map(p => extractLabel(p.time));
            datasets = [{
                label: 'CPU %',
                data: cpuHistory.map(p => p.value),
                borderColor: accentColor,
                backgroundColor: accentColor + '1a',
                borderWidth: 2,
                fill: true,
                tension: 0.3,
                pointRadius: cpuHistory.length > 60 ? 0 : 2,
                pointHoverRadius: 4,
                pointBackgroundColor: accentColor,
                spanGaps: true
            }];
            // Memory overlay for single-node — teal solid line, no fill.
            if (hasSingleMem) {
                // Merge time labels from both series.
                const timeSet = new Set(cpuHistory.map(p => String(p.time || '')));
                for (const p of memHistory) timeSet.add(String(p.time || ''));
                const sortedTimes = Array.from(timeSet).sort((a, b) => Number(a) - Number(b));
                labels = sortedTimes.map(t => extractLabel(t));

                const cpuMap = {};
                for (const p of cpuHistory) cpuMap[String(p.time || '')] = p.value;
                const memMap = {};
                for (const p of memHistory) memMap[String(p.time || '')] = p.value;

                datasets[0].data = sortedTimes.map(t => cpuMap[t] !== undefined ? cpuMap[t] : null);
                datasets.push({
                    label: 'Memory %',
                    data: sortedTimes.map(t => memMap[t] !== undefined ? memMap[t] : null),
                    borderColor: memColor,
                    backgroundColor: 'transparent',
                    borderWidth: 2,
                    fill: false,
                    tension: 0.3,
                    pointRadius: sortedTimes.length > 60 ? 0 : 2,
                    pointHoverRadius: 4,
                    pointBackgroundColor: memColor,
                    spanGaps: true
                });
            }
        }

        const showLegend = isMultiNode || hasMem;

        if (this.cpuChart) {
            this.cpuChart.data.labels = labels;
            this.cpuChart.data.datasets = datasets;
            this.cpuChart.options.plugins.legend.display = showLegend;
            this.cpuChart.update('none');
            return;
        }

        const ctx = canvas.getContext('2d');
        this.cpuChart = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    legend: {
                        display: showLegend,
                        labels: {
                            color: chartText,
                            font: { family: 'Inter', size: 11 },
                            boxWidth: 12,
                            padding: 8
                        }
                    },
                    tooltip: {
                        backgroundColor: chartBg,
                        titleColor: chartText,
                        bodyColor: chartText,
                        borderColor: chartBorder,
                        borderWidth: 1,
                        callbacks: {
                            label: (ctx) => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(1) + '%'
                        }
                    }
                },
                scales: {
                    x: {
                        display: true,
                        grid: { color: chartGrid, drawBorder: false },
                        ticks: {
                            color: chartText,
                            font: { family: 'Inter', size: 10 },
                            maxTicksLimit: longRange ? 10 : 8,
                            maxRotation: 0
                        }
                    },
                    y: {
                        display: true,
                        min: 0,
                        suggestedMax: 10,
                        grid: { color: chartGrid, drawBorder: false },
                        ticks: {
                            color: chartText,
                            font: { family: 'Inter', size: 10 },
                            callback: (value) => value + '%'
                        }
                    }
                }
            }
        });
    },

    // logs_hot only needs attention when its coverage window drifts, so it is one
    // line rather than four cards.
    renderHotTableStats(hot) {
        const el = document.getElementById('hotStrip');
        if (!el) return;
        if (!hot) {
            el.textContent = 'unavailable';
            el.className = 'perf-section-hint perf-hint-value';
            return;
        }
        const bits = [
            this.formatCount(hot.partition_count || 0) + ' partitions',
            this.formatCount(hot.row_count || 0) + ' rows',
            this.formatBytes(hot.disk_bytes || 0)
        ];
        const mins = hot.coverage_minutes;
        let drifted = false;
        if (mins != null) {
            const h = Math.floor(mins / 60);
            const m = Math.floor(mins % 60);
            bits.push('covering ' + (h > 0 ? `${h}h ${m}m` : `${m}m`));
            // Coverage should stay near the alert lookback; well past it means
            // parts are not ageing out.
            drifted = mins > 150;
        }
        el.textContent = bits.join(' \u00b7 ');
        el.className = 'perf-section-hint perf-hint-value' + (drifted ? ' perf-metric-warning' : '');
    },

    epochLabel(unixSec) {
        const d = new Date(Number(unixSec) * 1000);
        if (isNaN(d.getTime())) return '';
        const hhmm = d.toTimeString().slice(0, 5);
        if (this.timeRange === '1h') return hhmm;
        const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        return `${months[d.getMonth()]} ${d.getDate()} ${hhmm}`;
    },

    renderDistQueueChart(history) {
        const canvas = document.getElementById('perfDistQueueChart');
        const placeholder = document.getElementById('perfDistQueuePlaceholder');
        if (!canvas) return;

        if (!history || history.length < 2) {
            if (this.distQueueChart) { this.distQueueChart.destroy(); this.distQueueChart = null; }
            if (placeholder) placeholder.style.display = '';
            canvas.style.display = 'none';
            return;
        }
        if (placeholder) placeholder.style.display = 'none';
        canvas.style.display = '';

        const cv = window.ThemeManager ? ThemeManager.getCSSVar : (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
        const chartText   = cv('--chart-text')   || '#e8eaed';
        const chartGrid   = cv('--chart-grid')   || '#24243e';
        const chartBg     = cv('--chart-bg')     || '#1a1a2e';
        const chartBorder = cv('--chart-border') || '#24243e';
        const color = cv('--accent-primary') || '#9c6ade';

        const labels = history.map(s => this.epochLabel(s.time));
        const values = history.map(s => s.data_files);

        if (this.distQueueChart) {
            this.distQueueChart.data.labels = labels;
            this.distQueueChart.data.datasets[0].data = values;
            this.distQueueChart.update('none');
            return;
        }

        const ctx = canvas.getContext('2d');
        this.distQueueChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    label: 'Files',
                    data: values,
                    borderColor: color,
                    backgroundColor: color + '22',
                    borderWidth: 2,
                    pointRadius: 2,
                    pointHoverRadius: 4,
                    fill: true,
                    tension: 0.3
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: chartBg,
                        titleColor: chartText,
                        bodyColor: chartText,
                        borderColor: chartBorder,
                        borderWidth: 1,
                        callbacks: { label: (ctx) => ctx.parsed.y.toLocaleString() + ' files' }
                    }
                },
                scales: {
                    x: {
                        grid: { color: chartGrid, drawBorder: false },
                        ticks: { color: chartText, font: { family: 'Inter', size: 10 }, maxTicksLimit: 8 }
                    },
                    y: {
                        beginAtZero: true,
                        grid: { color: chartGrid, drawBorder: false },
                        ticks: {
                            color: chartText,
                            font: { family: 'Inter', size: 10 },
                            callback: (v) => v.toLocaleString()
                        }
                    }
                }
            }
        });
    },

    renderDDLQueueChart(history) {
        const canvas = document.getElementById('perfDDLQueueChart');
        const placeholder = document.getElementById('perfDDLQueuePlaceholder');
        if (!canvas) return;

        if (!history || history.length < 2) {
            if (this.ddlQueueChart) { this.ddlQueueChart.destroy(); this.ddlQueueChart = null; }
            if (placeholder) placeholder.style.display = '';
            canvas.style.display = 'none';
            return;
        }
        if (placeholder) placeholder.style.display = 'none';
        canvas.style.display = '';

        const cv = window.ThemeManager ? ThemeManager.getCSSVar : (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
        const chartText   = cv('--chart-text')   || '#e8eaed';
        const chartGrid   = cv('--chart-grid')   || '#24243e';
        const chartBg     = cv('--chart-bg')     || '#1a1a2e';
        const chartBorder = cv('--chart-border') || '#24243e';
        const color = '#ffd93d';

        const labels = history.map(s => this.epochLabel(s.time));
        const values = history.map(s => s.pending);

        if (this.ddlQueueChart) {
            this.ddlQueueChart.data.labels = labels;
            this.ddlQueueChart.data.datasets[0].data = values;
            this.ddlQueueChart.update('none');
            return;
        }

        const ctx = canvas.getContext('2d');
        this.ddlQueueChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    label: 'Tasks',
                    data: values,
                    borderColor: color,
                    backgroundColor: color + '22',
                    borderWidth: 2,
                    pointRadius: 2,
                    pointHoverRadius: 4,
                    fill: true,
                    tension: 0.3
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: chartBg,
                        titleColor: chartText,
                        bodyColor: chartText,
                        borderColor: chartBorder,
                        borderWidth: 1,
                        callbacks: { label: (ctx) => ctx.parsed.y.toLocaleString() + ' tasks' }
                    }
                },
                scales: {
                    x: {
                        grid: { color: chartGrid, drawBorder: false },
                        ticks: { color: chartText, font: { family: 'Inter', size: 10 }, maxTicksLimit: 8 }
                    },
                    y: {
                        beginAtZero: true,
                        grid: { color: chartGrid, drawBorder: false },
                        ticks: {
                            color: chartText,
                            font: { family: 'Inter', size: 10 },
                            precision: 0,
                            callback: (v) => v.toLocaleString()
                        }
                    }
                }
            }
        });
    },

    destroyCharts() {
        if (this.cpuChart) {
            this.cpuChart.destroy();
            this.cpuChart = null;
        }
        if (this.ingestChart) {
            this.ingestChart.destroy();
            this.ingestChart = null;
        }
        if (this.distQueueChart) {
            this.distQueueChart.destroy();
            this.distQueueChart = null;
        }
        if (this.ddlQueueChart) {
            this.ddlQueueChart.destroy();
            this.ddlQueueChart = null;
        }
    },

    async killQuery(queryId) {
        if (!confirm('Kill this query? The user running it will receive an error.')) return;

        try {
            const res = await fetch(`/api/v1/admin/kill-query?query_id=${encodeURIComponent(queryId)}`, {
                method: 'POST',
                credentials: 'include'
            });
            const data = await res.json();

            if (data.success) {
                if (window.Toast) Toast.success('Query Killed', 'Kill signal sent successfully');
                setTimeout(() => this.refresh(), 500);
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to kill query');
            }
        } catch (err) {
            console.error('[Performance] kill query error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    // Utility methods
    setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    },

    formatBytes(bytes) {
        if (bytes === 0 || bytes == null) return '0 B';
        const neg = bytes < 0;
        bytes = Math.abs(bytes);
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        const val = parseFloat((bytes / Math.pow(k, i)).toFixed(1));
        return (neg ? '-' : '') + val + ' ' + sizes[i];
    },

    formatNumber(n) {
        if (n == null) return '0';
        return Number(n).toLocaleString();
    },

    // Compact form for counts that run to billions, where every digit is noise.
    // formatNumber stays exact for the tables that want it.
    formatCount(n) {
        const v = Number(n || 0);
        if (v >= 1e12) return (v / 1e12).toFixed(1) + 'T';
        if (v >= 1e9) return (v / 1e9).toFixed(1) + 'B';
        if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
        if (v >= 1e3) return (v / 1e3).toFixed(1) + 'K';
        return String(Math.round(v));
    },

    formatUptime(seconds) {
        if (!seconds) return '--';
        seconds = Math.floor(seconds);
        const days = Math.floor(seconds / 86400);
        const hours = Math.floor((seconds % 86400) / 3600);
        const mins = Math.floor((seconds % 3600) / 60);
        if (days > 0) return `${days}d ${hours}h`;
        if (hours > 0) return `${hours}h ${mins}m`;
        return `${mins}m`;
    },

    formatDuration(ms) {
        if (ms == null) return '--';
        if (ms < 1) return '<1ms';
        if (ms < 1000) return ms + 'ms';
        if (ms < 60000) return (ms / 1000).toFixed(1) + 's';
        // Past a minute, seconds stop being readable: a merge running for
        // "1084.0s" is the same fact as "18m 04s" and much harder to judge.
        const minutes = Math.floor(ms / 60000);
        const seconds = Math.round((ms % 60000) / 1000);
        if (minutes < 60) return `${minutes}m ${String(seconds).padStart(2, '0')}s`;
        return `${Math.floor(minutes / 60)}h ${String(minutes % 60).padStart(2, '0')}m`;
    },

    // Deliberately UTC. The value is a whole calendar day produced by a
    // server-side aggregate over UTC, so shifting the label into the display
    // zone would name a day whose counts it does not hold.
    formatDay(d) {
        const parts = String(d).split('-');
        if (parts.length !== 3) return d;
        const date = new Date(Date.UTC(+parts[0], +parts[1] - 1, +parts[2]));
        if (isNaN(date.getTime())) return d;
        return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
    },

    escapeHtml(str) {
        if (!str) return '';
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }
};

window.Performance = Performance;

document.addEventListener('DOMContentLoaded', () => {
    Performance.init();
});

window.addEventListener('beforeunload', () => {
    Performance.stopUpdates();
});
