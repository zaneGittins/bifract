// Performance monitoring module (admin-only)

const Performance = {
    isActive: false,
    refreshInterval: null,
    refreshRate: 5000,
    timeRange: '1h',
    durationChart: null,
    memoryChart: null,
    cpuChart: null,
    ingestChart: null,
    prevCpuTimes: null,
    subTab: 'overview',
    ingestFractal: '',
    ingestDays: 30,
    fractalNames: {},
    _ingestData: [],
    _ingestSeries: null,
    _lastProcesses: [],
    _lastRecentQueries: [],
    _shownProcesses: [],
    _shownRecentQueries: [],
    _drawerQuery: '',

    init() {
        const refreshSelect = document.getElementById('perfRefreshRate');
        if (refreshSelect) {
            refreshSelect.addEventListener('change', (e) => {
                this.refreshRate = parseInt(e.target.value, 10);
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
                this.refresh();
            });
        }

        const procSearch = document.getElementById('perfProcessSearch');
        if (procSearch) {
            procSearch.addEventListener('input', () => this.filterProcesses());
        }
        const recentSearch = document.getElementById('perfRecentSearch');
        if (recentSearch) {
            recentSearch.addEventListener('input', () => this.filterRecentQueries());
        }

        // Ingest-per-day filters (Storage & Ingest sub-tab)
        const ingestFractalSel = document.getElementById('perfIngestFractal');
        if (ingestFractalSel) {
            ingestFractalSel.addEventListener('change', (e) => {
                this.ingestFractal = e.target.value;
                this.loadIngest();
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
        if (savedTab === 'overview' || savedTab === 'storage' || savedTab === 'activity') {
            this.subTab = savedTab;
        }

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') this.closeDrawer();
        });
    },

    switchSubTab(name) {
        this.subTab = name;
        sessionStorage.setItem('perfSubTab', name);
        this.applySubTab(name);
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
            activity: 'perfPaneActivity'
        };
        Object.entries(panes).forEach(([k, id]) => {
            const el = document.getElementById(id);
            if (el) el.style.display = (k === name) ? '' : 'none';
        });
    },

    async show() {
        this.isActive = true;
        this.prevCpuTimes = null;
        this.applySubTab(this.subTab);
        this.loadFractalOptions();
        await this.refresh();
        this.startUpdates();
    },

    hide() {
        this.isActive = false;
        this.stopUpdates();
        this.destroyCharts();
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
            // Metrics (server + storage cards, CPU, recent queries) and pressure
            // are always fetched; processes only for Activity; ingest only for
            // Storage. This keeps each poll scoped to what the active tab shows.
            const metPromise = fetch(`/api/v1/admin/metrics?range=${this.timeRange}`, { credentials: 'include' });
            const pressurePromise = fetch('/api/v1/system/pressure', { credentials: 'include' });
            const procPromise = tab === 'activity'
                ? fetch('/api/v1/admin/processes', { credentials: 'include' })
                : null;

            const metData = await (await metPromise).json();
            const pressureData = await (await pressurePromise).json();

            if (metData.success) {
                this.renderMetrics(metData.metrics || {}, metData.async_metrics || {}, metData.log_storage || {}, metData.disk || {});
                if (tab === 'overview') {
                    this.renderCpuChart(metData.cpu_history || [], metData.cpu_history_nodes || null);
                }
                if (tab === 'activity') {
                    this.renderRecentQueries(metData.recent_queries || []);
                    this.updateCharts(metData.recent_queries || []);
                }
            }

            this.renderPressureBanner(pressureData);
            this.renderClusterHealth(pressureData.distribution_queue || null);

            if (procPromise) {
                const procData = await procPromise.json();
                if (procData.success) this.renderProcesses(procData.processes || []);
            }

            if (tab === 'storage') {
                this.loadIngest();
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

    async loadIngest() {
        try {
            const url = `/api/v1/admin/ingest-daily?days=${this.ingestDays}&fractal=${encodeURIComponent(this.ingestFractal)}`;
            const res = await fetch(url, { credentials: 'include' });
            const data = await res.json();
            if (data.success) {
                this._ingestData = data.days || [];
                this._ingestSeries = (data.series && data.series.length) ? data.series : null;
                this.renderIngestChart();
            }
        } catch (err) {
            console.error('[Performance] ingest load error:', err);
        }
    },

    fractalLabel(id) {
        if (id === '__other__') return 'Other';
        if (!id) return 'Default';
        return this.fractalNames[id] || id;
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

        let datasets;
        if (stacked) {
            datasets = this._ingestSeries.map((s, i) => {
                const color = s.fractal_id === '__other__'
                    ? '#6b7280'
                    : this.nodeColors[i % this.nodeColors.length];
                return {
                    label: this.fractalLabel(s.fractal_id),
                    data: s.raw_bytes || [],
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
                label: 'Uncompressed',
                data: days.map(d => d.raw_bytes),
                backgroundColor: accent + 'cc',
                hoverBackgroundColor: accent,
                borderRadius: 3,
                maxBarThickness: 40
            }];
        }

        // Toggling between stacked and single changes the dataset shape and axis
        // config; rebuild the chart in that case rather than patching it.
        if (this.ingestChart && this.ingestChart._stacked !== stacked) {
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
                            label: (ctx) => ctx.dataset.label + ': ' + self.formatBytes(ctx.parsed.y || 0),
                            footer: (items) => {
                                const total = items.reduce((sum, it) => sum + (it.parsed.y || 0), 0);
                                return 'Total: ' + self.formatBytes(total);
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
                                    'Rows: ' + self.formatNumber(row.rows || 0)
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
                            callback: (value) => self.formatBytes(value)
                        }
                    }
                }
            }
        });
        this.ingestChart._stacked = stacked;
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
        if (!dq) { section.style.display = 'none'; return; }
        section.style.display = '';

        const el = document.getElementById('metricDistQueue');
        const sub = document.getElementById('metricDistQueueSub');
        if (!el) return;

        if (dq.broken_data_files > 0) {
            el.textContent = 'Critical';
            el.className = 'perf-metric-value perf-metric-danger';
            sub.textContent = dq.broken_data_files.toLocaleString() + ' broken files';
        } else if (!dq.healthy) {
            el.textContent = 'Degraded';
            el.className = 'perf-metric-value perf-metric-warning';
            sub.textContent = dq.data_files.toLocaleString() + ' files pending';
        } else if (dq.data_files > 0) {
            el.textContent = 'Draining';
            el.className = 'perf-metric-value';
            sub.textContent = dq.data_files.toLocaleString() + ' files';
        } else {
            el.textContent = 'Healthy';
            el.className = 'perf-metric-value';
            sub.textContent = '';
        }
    },

    renderMetrics(metrics, asyncMetrics, logStorage, disk) {
        const activeQueries = metrics['Query'] || 0;
        const merges = metrics['Merge'] || 0;
        const memTracking = metrics['MemoryTracking'] || 0;
        const uptime = asyncMetrics['Uptime'] || 0;

        this.setText('metricActiveQueries', activeQueries);
        this.setText('metricMemory', this.formatBytes(memTracking));
        this.setText('metricMerges', merges);
        this.setText('metricUptime', this.formatUptime(uptime));

        // Log-specific storage metrics
        const logRows = logStorage['log_rows'] || 0;
        const compressedBytes = logStorage['compressed_bytes'] || 0;
        const uncompressedBytes = logStorage['uncompressed_bytes'] || 0;

        this.setText('metricLogRows', this.formatNumber(logRows));
        this.setText('metricLogStorage', this.formatBytes(compressedBytes));

        // Disk usage with color coding
        const diskPct = typeof disk['used_pct'] === 'number' ? disk['used_pct'] : 0;
        const diskFree = disk['free_space'] || '';
        const diskEl = document.getElementById('metricDiskUsage');
        if (diskEl) {
            diskEl.textContent = diskPct + '%';
            diskEl.className = 'perf-metric-value' +
                (diskPct > 85 ? ' perf-metric-danger' : diskPct > 70 ? ' perf-metric-warning' : '');
        }
        this.setText('metricDiskFree', diskFree ? diskFree + ' free' : '');

        // Compression: space saved as a percentage
        if (compressedBytes > 0 && uncompressedBytes > 0) {
            const saved = (1 - compressedBytes / uncompressedBytes) * 100;
            this.setText('metricCompression', saved.toFixed(1) + '% saved');
            this.setText('metricUncompressed', this.formatBytes(uncompressedBytes) + ' raw');
        } else {
            this.setText('metricCompression', '--');
            this.setText('metricUncompressed', '');
        }
    },

    // Per-node color palette for multi-node CPU charts.
    nodeColors: [
        '#9c6ade', '#4ecdc4', '#ff6b6b', '#ffd93d',
        '#6bcb77', '#4d96ff', '#ff8fab', '#c9b1ff'
    ],

    renderCpuChart(cpuHistory, cpuHistoryNodes) {
        const canvas = document.getElementById('perfCpuChart');
        if (!canvas) return;

        const placeholder = document.getElementById('perfCpuPlaceholder');
        const isMultiNode = cpuHistoryNodes && Object.keys(cpuHistoryNodes).length > 0;
        const hasSingle = cpuHistory && cpuHistory.length > 0;

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

        const longRange = this.timeRange === '7d' || this.timeRange === '30d';
        const showDate = longRange || this.timeRange === '8h' || this.timeRange === '24h';
        const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

        const extractLabel = (rawTime) => {
            const t = String(rawTime || '');
            const parts = t.split(' ');
            if (!showDate) {
                return (parts.length > 1 ? parts[1] : parts[0]).substring(0, 5);
            }
            if (parts.length < 2) return t;
            const dp = parts[0].split('-');
            const mon = dp.length >= 2 ? (months[parseInt(dp[1], 10) - 1] || dp[1]) : dp[0];
            const day = dp.length >= 3 ? parseInt(dp[2], 10) : '';
            const hhmm = parts[1].substring(0, 5);
            return longRange ? `${mon} ${day} ${hhmm}` : `${mon} ${day} ${hhmm}`;
        };

        const extractTime = (p) => extractLabel(p.time);

        let labels, datasets;

        if (isMultiNode) {
            // Build unified time labels from all nodes.
            const timeSet = new Set();
            for (const points of Object.values(cpuHistoryNodes)) {
                for (const p of points) timeSet.add(String(p.time || ''));
            }
            const sortedTimes = Array.from(timeSet).sort();
            labels = sortedTimes.map(t => extractLabel(t));

            const nodes = Object.keys(cpuHistoryNodes).sort();
            datasets = nodes.map((node, i) => {
                const color = this.nodeColors[i % this.nodeColors.length];
                const timeMap = {};
                for (const p of cpuHistoryNodes[node]) {
                    timeMap[String(p.time || '')] = p.value;
                }
                const data = sortedTimes.map(t => timeMap[t] !== undefined ? timeMap[t] : null);
                return {
                    label: node,
                    data: data,
                    borderColor: color,
                    backgroundColor: color + '1a',
                    borderWidth: 2,
                    fill: false,
                    tension: 0.3,
                    pointRadius: data.length > 60 ? 0 : 2,
                    pointHoverRadius: 4,
                    pointBackgroundColor: color,
                    pointHoverBackgroundColor: color,
                    pointHoverBorderColor: color,
                    spanGaps: true
                };
            });
        } else {
            labels = cpuHistory.map(extractTime);
            const data = cpuHistory.map(p => p.value);
            datasets = [{
                label: 'CPU %',
                data: data,
                borderColor: accentColor,
                backgroundColor: accentColor + '1a',
                borderWidth: 2,
                fill: true,
                tension: 0.3,
                pointRadius: data.length > 60 ? 0 : 2,
                pointHoverRadius: 4,
                pointBackgroundColor: accentColor,
                pointHoverBackgroundColor: accentColor,
                pointHoverBorderColor: accentColor
            }];
        }

        if (this.cpuChart) {
            this.cpuChart.data.labels = labels;
            this.cpuChart.data.datasets = datasets;
            this.cpuChart.options.plugins.legend.display = isMultiNode;
            this.cpuChart.update('none');
            return;
        }

        const ctx = canvas.getContext('2d');
        this.cpuChart = new Chart(ctx, {
            type: 'line',
            data: { labels: labels, datasets: datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                plugins: {
                    legend: {
                        display: isMultiNode,
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
                            label: (ctx) => {
                                const name = ctx.dataset.label || 'CPU';
                                return name + ': ' + ctx.parsed.y.toFixed(1) + '%';
                            }
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

    renderProcesses(processes) {
        this._lastProcesses = processes.filter(p => {
            const q = (p.query || '').toLowerCase();
            return !q.includes('system.processes') &&
                   !q.includes('system.metrics') &&
                   !q.includes('system.asynchronous_metrics') &&
                   !q.includes('system.query_log');
        });
        this.filterProcesses();
    },

    filterProcesses() {
        const input = document.getElementById('perfProcessSearch');
        const term = input ? input.value.toLowerCase().trim() : '';
        this._shownProcesses = term
            ? this._lastProcesses.filter(p =>
                (p.query || '').toLowerCase().includes(term) ||
                (p.user || '').toLowerCase().includes(term))
            : this._lastProcesses;
        const countEl = document.getElementById('perfProcessCount');
        if (countEl) countEl.textContent = this._shownProcesses.length;
        this._renderProcessTable();
    },

    _renderProcessTable() {
        const container = document.getElementById('perfProcessesTable');
        if (!container) return;
        const data = this._shownProcesses;

        if (data.length === 0) {
            container.innerHTML = '<div class="empty-state" style="min-height: 120px;"><p>No active queries</p></div>';
            return;
        }

        let html = '<table class="results-table perf-table"><thead><tr>';
        html += '<th>Elapsed</th><th>User</th><th>Query</th><th>Rows Read</th><th>Memory</th><th></th>';
        html += '</tr></thead><tbody>';

        data.forEach((p, idx) => {
            const elapsed = parseFloat(p.elapsed || 0);
            const elapsedClass = elapsed > 30 ? 'perf-danger' : elapsed > 10 ? 'perf-warning' : '';
            const queryText = this.truncateQuery(p.query || '', 120);
            const memReadable = p.memory_readable || this.formatBytes(p.memory_usage || 0);
            const readRows = this.formatNumber(p.read_rows || 0);
            const queryId = this.escapeHtml(p.query_id || '');

            html += `<tr class="${elapsedClass} perf-row-clickable" onclick="Performance.openQueryDrawer('proc',${idx})">`;
            html += `<td class="perf-elapsed">${elapsed.toFixed(1)}s</td>`;
            html += `<td>${this.escapeHtml(p.user || '')}</td>`;
            html += `<td class="perf-query-cell">${this.escapeHtml(queryText)}</td>`;
            html += `<td>${readRows}</td>`;
            html += `<td>${memReadable}</td>`;
            html += `<td><button class="btn-kill-query" onclick="event.stopPropagation();Performance.killQuery('${queryId}')">Kill</button></td>`;
            html += '</tr>';
        });

        html += '</tbody></table>';
        container.innerHTML = html;
    },

    renderRecentQueries(queries) {
        this._lastRecentQueries = queries.filter(q => (q.query_kind || '') !== '');
        this.filterRecentQueries();
    },

    filterRecentQueries() {
        const input = document.getElementById('perfRecentSearch');
        const term = input ? input.value.toLowerCase().trim() : '';
        if (term) {
            this._shownRecentQueries = this._lastRecentQueries.filter(q =>
                (q.query || '').toLowerCase().includes(term) ||
                (q.query_kind || '').toLowerCase().includes(term));
        } else {
            this._shownRecentQueries = this._lastRecentQueries.slice(0, 50);
        }
        const countEl = document.getElementById('perfRecentCount');
        if (countEl) countEl.textContent = this._shownRecentQueries.length;
        this._renderRecentTable();
    },

    _renderRecentTable() {
        const container = document.getElementById('perfRecentTable');
        if (!container) return;
        const data = this._shownRecentQueries;

        if (data.length === 0) {
            container.innerHTML = '<div class="empty-state" style="min-height: 120px;"><p>No recent queries</p></div>';
            return;
        }

        let html = '<table class="results-table perf-table"><thead><tr>';
        html += '<th>Time</th><th>Type</th><th>Query</th><th>Duration</th><th>Rows Read</th><th>Memory</th><th>Status</th>';
        html += '</tr></thead><tbody>';

        data.forEach((q, idx) => {
            const duration = q.query_duration_ms || 0;
            const durationClass = duration > 30000 ? 'perf-danger' : duration > 5000 ? 'perf-warning' : '';
            const isError = (q.type || '') === 'ExceptionWhileProcessing';
            const statusClass = isError ? 'perf-status-error' : 'perf-status-ok';
            const statusText = isError ? 'Error' : 'OK';
            const timeStr = this.formatEventTime(q.event_time);

            html += `<tr class="${durationClass} perf-row-clickable" onclick="Performance.openQueryDrawer('recent',${idx})">`;
            html += `<td class="perf-time">${timeStr}</td>`;
            html += `<td>${this.escapeHtml(q.query_kind || '--')}</td>`;
            html += `<td class="perf-query-cell">${this.escapeHtml(this.truncateQuery(q.query || '', 120))}</td>`;
            html += `<td>${this.formatDuration(duration)}</td>`;
            html += `<td>${this.formatNumber(q.read_rows || 0)}</td>`;
            html += `<td>${this.formatBytes(q.memory_usage || 0)}</td>`;
            html += `<td><span class="${statusClass}">${statusText}</span></td>`;
            html += '</tr>';
        });

        html += '</tbody></table>';
        container.innerHTML = html;
    },

    openQueryDrawer(source, idx) {
        const item = source === 'proc' ? this._shownProcesses[idx] : this._shownRecentQueries[idx];
        if (!item) return;

        this._drawerQuery = item.query || '';

        let metaHtml = '';
        if (source === 'proc') {
            const elapsed = parseFloat(item.elapsed || 0);
            metaHtml = [
                item.user ? `<span>${this.escapeHtml(item.user)}</span>` : '',
                `<span>${elapsed.toFixed(1)}s elapsed</span>`,
                `<span>${this.formatBytes(item.memory_usage || 0)} memory</span>`,
                `<span>${this.formatNumber(item.read_rows || 0)} rows read</span>`,
            ].filter(Boolean).join('');
        } else {
            const isError = (item.type || '') === 'ExceptionWhileProcessing';
            metaHtml = [
                `<span>${this.formatEventTime(item.event_time)}</span>`,
                `<span>${this.escapeHtml(item.query_kind || '--')}</span>`,
                `<span>${this.formatDuration(item.query_duration_ms || 0)}</span>`,
                `<span>${this.formatBytes(item.memory_usage || 0)}</span>`,
                `<span class="${isError ? 'perf-status-error' : 'perf-status-ok'}">${isError ? 'Error' : 'OK'}</span>`,
            ].join('');
        }

        const pre = document.getElementById('perfDrawerQuery');
        const metaEl = document.getElementById('perfDrawerMeta');
        const drawer = document.getElementById('perfQueryDrawer');
        if (pre) pre.textContent = this._drawerQuery;
        if (metaEl) metaEl.innerHTML = metaHtml;
        if (drawer) drawer.classList.add('open');
    },

    closeDrawer() {
        const drawer = document.getElementById('perfQueryDrawer');
        if (drawer) drawer.classList.remove('open');
    },

    async copyDrawerQuery() {
        try {
            await navigator.clipboard.writeText(this._drawerQuery);
            const btn = document.getElementById('perfDrawerCopy');
            if (btn) {
                const orig = btn.innerHTML;
                btn.textContent = 'Copied!';
                setTimeout(() => { btn.innerHTML = orig; }, 1500);
            }
        } catch (e) {
            console.error('[Performance] clipboard copy failed:', e);
        }
    },

    updateCharts(recentQueries) {
        if (!recentQueries || recentQueries.length === 0) return;

        const cv = window.ThemeManager ? ThemeManager.getCSSVar : (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();

        // Build scatter data from recent queries
        const durationData = [];
        const memoryData = [];

        // Reverse so oldest is first (left-to-right chronological)
        [...recentQueries].reverse().forEach(q => {
            if (q.type === 'ExceptionWhileProcessing') return;
            const time = q.event_time;
            const duration = q.query_duration_ms || 0;
            const mem = q.memory_usage || 0;

            durationData.push({ x: time, y: duration });
            memoryData.push({ x: time, y: mem });
        });

        // Duration chart
        this.renderScatterChart(
            'perfQueryDurationChart',
            'durationChart',
            durationData,
            'Duration (ms)',
            cv('--accent-primary') || '#9c6ade',
            (val) => val > 1000 ? (val / 1000).toFixed(1) + 's' : val + 'ms'
        );

        // Memory chart
        this.renderScatterChart(
            'perfMemoryChart',
            'memoryChart',
            memoryData,
            'Memory',
            cv('--info') || '#60a5fa',
            (val) => this.formatBytes(val)
        );
    },

    renderScatterChart(canvasId, chartProp, data, label, color, tooltipFormatter) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;

        if (this[chartProp]) {
            this[chartProp].data.datasets[0].data = data;
            this[chartProp].update('none');
            return;
        }

        const cv = window.ThemeManager ? ThemeManager.getCSSVar : (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
        const chartBg = cv('--chart-bg') || '#1a1a2e';
        const chartText = cv('--chart-text') || '#e8eaed';
        const chartGrid = cv('--chart-grid') || '#24243e';
        const chartBorder = cv('--chart-border') || '#24243e';

        const ctx = canvas.getContext('2d');
        this[chartProp] = new Chart(ctx, {
            type: 'scatter',
            data: {
                datasets: [{
                    label: label,
                    data: data,
                    backgroundColor: color + '99',
                    borderColor: color,
                    borderWidth: 1,
                    pointRadius: 3,
                    pointHoverRadius: 5
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: chartBg,
                        titleColor: chartText,
                        bodyColor: chartText,
                        borderColor: chartBorder,
                        borderWidth: 1,
                        callbacks: {
                            label: (ctx) => tooltipFormatter(ctx.parsed.y)
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'category',
                        display: true,
                        grid: { color: chartGrid, drawBorder: false },
                        ticks: {
                            color: chartText,
                            font: { family: 'Inter', size: 10 },
                            maxTicksLimit: 6,
                            callback: function(value, index) {
                                const label = this.getLabelForValue(value);
                                if (!label) return '';
                                // Show only time portion
                                const parts = String(label).split(' ');
                                return parts.length > 1 ? parts[1] : parts[0];
                            }
                        }
                    },
                    y: {
                        display: true,
                        grid: { color: chartGrid, drawBorder: false },
                        ticks: {
                            color: chartText,
                            font: { family: 'Inter', size: 10 },
                            callback: (value) => tooltipFormatter(value)
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
        if (this.durationChart) {
            this.durationChart.destroy();
            this.durationChart = null;
        }
        if (this.memoryChart) {
            this.memoryChart.destroy();
            this.memoryChart = null;
        }
        if (this.ingestChart) {
            this.ingestChart.destroy();
            this.ingestChart = null;
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
        return (ms / 1000).toFixed(1) + 's';
    },

    formatDay(d) {
        const parts = String(d).split('-');
        if (parts.length !== 3) return d;
        const date = new Date(Date.UTC(+parts[0], +parts[1] - 1, +parts[2]));
        if (isNaN(date.getTime())) return d;
        return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
    },

    formatEventTime(t) {
        if (!t) return '--';
        // ClickHouse returns time strings; extract time portion
        const str = String(t);
        const parts = str.split(' ');
        return parts.length > 1 ? parts[1] : str;
    },

    truncateQuery(q, maxLen) {
        if (q.length <= maxLen) return q;
        return q.substring(0, maxLen) + '...';
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
