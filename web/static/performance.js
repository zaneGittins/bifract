// Performance monitoring module (admin-only)

const Performance = {
    isActive: false,
    refreshInterval: null,
    refreshRate: 5000,
    timeRange: '1h',
    durationChart: null,
    memoryChart: null,
    cpuChart: null,
    prevCpuTimes: null,

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
    },

    async show() {
        this.isActive = true;
        this.prevCpuTimes = null;
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
        try {
            const [procRes, metRes, pressureRes] = await Promise.all([
                fetch('/api/v1/admin/processes', { credentials: 'include' }),
                fetch(`/api/v1/admin/metrics?range=${this.timeRange}`, { credentials: 'include' }),
                fetch('/api/v1/system/pressure', { credentials: 'include' })
            ]);

            const procData = await procRes.json();
            const metData = await metRes.json();
            const pressureData = await pressureRes.json();

            if (procData.success) {
                this.renderProcesses(procData.processes || []);
            }

            if (metData.success) {
                this.renderMetrics(metData.metrics || {}, metData.async_metrics || {});
                this.renderRecentQueries(metData.recent_queries || []);
                this.updateCharts(metData.recent_queries || []);
                this.renderCpuChart(metData.cpu_history || [], metData.cpu_history_nodes || null);
            }

            this.renderPressureBanner(pressureData);
        } catch (err) {
            console.error('[Performance] refresh error:', err);
        }
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

    renderMetrics(metrics, asyncMetrics) {
        const activeQueries = metrics['Query'] || 0;
        const merges = metrics['Merge'] || 0;
        const memTracking = metrics['MemoryTracking'] || 0;
        const uptime = asyncMetrics['Uptime'] || 0;
        const tables = asyncMetrics['NumberOfTables'] || 0;
        const totalRows = asyncMetrics['TotalRowsOfMergeTreeTables'] || 0;

        this.setText('metricActiveQueries', activeQueries);
        this.setText('metricMemory', this.formatBytes(memTracking));
        this.setText('metricMerges', merges);
        this.setText('metricUptime', this.formatUptime(uptime));
        this.setText('metricTables', tables);
        this.setText('metricTotalRows', this.formatNumber(totalRows));
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

        const extractTime = (p) => {
            const t = String(p.time || '');
            const parts = t.split(' ');
            const timePart = parts.length > 1 ? parts[1] : parts[0];
            return timePart.substring(0, 8);
        };

        let labels, datasets;

        if (isMultiNode) {
            // Build unified time labels from all nodes.
            const timeSet = new Set();
            for (const points of Object.values(cpuHistoryNodes)) {
                for (const p of points) timeSet.add(String(p.time || ''));
            }
            const sortedTimes = Array.from(timeSet).sort();
            labels = sortedTimes.map(t => {
                const parts = t.split(' ');
                return (parts.length > 1 ? parts[1] : parts[0]).substring(0, 8);
            });

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
                            maxTicksLimit: 6,
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
        const container = document.getElementById('perfProcessesTable');
        const countEl = document.getElementById('perfProcessCount');
        if (!container) return;

        // Filter out our own monitoring queries
        const filtered = processes.filter(p => {
            const q = (p.query || '').toLowerCase();
            return !q.includes('system.processes') &&
                   !q.includes('system.metrics') &&
                   !q.includes('system.asynchronous_metrics') &&
                   !q.includes('system.query_log');
        });

        if (countEl) countEl.textContent = filtered.length;

        if (filtered.length === 0) {
            container.innerHTML = '<div class="empty-state" style="min-height: 120px;"><p>No active queries</p></div>';
            return;
        }

        let html = '<table class="results-table perf-table"><thead><tr>';
        html += '<th>Elapsed</th><th>User</th><th>Query</th><th>Rows Read</th><th>Memory</th><th></th>';
        html += '</tr></thead><tbody>';

        filtered.forEach(p => {
            const elapsed = parseFloat(p.elapsed || 0);
            const elapsedClass = elapsed > 10 ? 'perf-warning' : elapsed > 30 ? 'perf-danger' : '';
            const queryText = this.truncateQuery(p.query || '', 120);
            const memReadable = p.memory_readable || this.formatBytes(p.memory_usage || 0);
            const readRows = this.formatNumber(p.read_rows || 0);
            const queryId = this.escapeHtml(p.query_id || '');

            html += `<tr class="${elapsedClass}">`;
            html += `<td class="perf-elapsed">${elapsed.toFixed(1)}s</td>`;
            html += `<td>${this.escapeHtml(p.user || '')}</td>`;
            html += `<td class="perf-query-cell" title="${this.escapeHtml(p.query || '')}">${this.escapeHtml(queryText)}</td>`;
            html += `<td>${readRows}</td>`;
            html += `<td>${memReadable}</td>`;
            html += `<td><button class="btn-kill-query" onclick="Performance.killQuery('${queryId}')">Kill</button></td>`;
            html += '</tr>';
        });

        html += '</tbody></table>';
        container.innerHTML = html;
    },

    renderRecentQueries(queries) {
        const container = document.getElementById('perfRecentTable');
        const countEl = document.getElementById('perfRecentCount');
        if (!container) return;

        // Filter out monitoring queries
        const filtered = queries.filter(q => {
            const text = (q.query_kind || '').toLowerCase();
            return text !== '';
        }).slice(0, 50);

        if (countEl) countEl.textContent = filtered.length;

        if (filtered.length === 0) {
            container.innerHTML = '<div class="empty-state" style="min-height: 120px;"><p>No recent queries</p></div>';
            return;
        }

        let html = '<table class="results-table perf-table"><thead><tr>';
        html += '<th>Time</th><th>Type</th><th>Duration</th><th>Rows Read</th><th>Memory</th><th>Status</th>';
        html += '</tr></thead><tbody>';

        filtered.forEach(q => {
            const duration = q.query_duration_ms || 0;
            const durationClass = duration > 5000 ? 'perf-warning' : duration > 30000 ? 'perf-danger' : '';
            const isError = (q.type || '') === 'ExceptionWhileProcessing';
            const statusClass = isError ? 'perf-status-error' : 'perf-status-ok';
            const statusText = isError ? 'Error' : 'OK';
            const timeStr = this.formatEventTime(q.event_time);

            html += `<tr class="${durationClass}">`;
            html += `<td class="perf-time">${timeStr}</td>`;
            html += `<td>${this.escapeHtml(q.query_kind || '--')}</td>`;
            html += `<td>${this.formatDuration(duration)}</td>`;
            html += `<td>${this.formatNumber(q.read_rows || 0)}</td>`;
            html += `<td>${this.formatBytes(q.memory_usage || 0)}</td>`;
            html += `<td><span class="${statusClass}">${statusText}</span></td>`;
            html += '</tr>';
        });

        html += '</tbody></table>';
        container.innerHTML = html;
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
