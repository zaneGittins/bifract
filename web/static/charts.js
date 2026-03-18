// BifractCharts - Shared chart rendering module
// All chart types are defined once here and called from search, notebooks, dashboards, and chat.
window.BifractCharts = {

    PIE_COLORS: [
        '#9c6ade', '#b794f6', '#8b5fbf', '#a855f7', '#7c3aed',
        '#6366f1', '#3b82f6', '#06b6d4', '#10b981', '#f59e0b',
        '#ef4444', '#f97316', '#84cc16', '#22c55e', '#14b8a6'
    ],

    SERIES_COLORS: [
        '#9c6ade', '#3b82f6', '#10b981', '#f59e0b', '#ef4444',
        '#06b6d4', '#8b5fbf', '#f97316', '#84cc16', '#14b8a6'
    ],

    // ---- Shared formatters ----

    formatSingleValue(num) {
        if (num === 0) return '0';
        const abs = Math.abs(num);
        if (abs >= 1e9) return (num / 1e9).toFixed(1) + 'B';
        if (abs >= 1e6) return (num / 1e6).toFixed(1) + 'M';
        if (abs >= 1e4) return (num / 1e3).toFixed(1) + 'K';
        if (Number.isInteger(num)) return num.toLocaleString();
        return num.toFixed(2);
    },

    formatBinEdge(num) {
        if (Number.isInteger(num)) return num.toLocaleString();
        if (Math.abs(num) >= 1000) return num.toFixed(0);
        if (Math.abs(num) >= 1) return num.toFixed(2);
        return num.toPrecision(3);
    },

    formatHeatmapValue(num) {
        if (num === 0) return '0';
        const abs = Math.abs(num);
        if (abs >= 1e6) return (num / 1e6).toFixed(1) + 'M';
        if (abs >= 1e4) return (num / 1e3).toFixed(1) + 'K';
        if (Number.isInteger(num)) return num.toLocaleString();
        return num.toFixed(1);
    },

    heatmapNiceTicks(min, max, count) {
        if (max <= 0) return [0];
        const range = max - min;
        const rawStep = range / count;
        const magnitude = Math.pow(10, Math.floor(Math.log10(rawStep)));
        const residual = rawStep / magnitude;
        let niceStep;
        if (residual <= 1.5) niceStep = magnitude;
        else if (residual <= 3) niceStep = 2 * magnitude;
        else if (residual <= 7) niceStep = 5 * magnitude;
        else niceStep = 10 * magnitude;
        const ticks = [];
        for (let v = 0; v <= max; v += niceStep) {
            ticks.push(Math.round(v * 1e6) / 1e6);
        }
        if (ticks[ticks.length - 1] < max) ticks.push(Math.ceil(max / niceStep) * niceStep);
        return ticks;
    },

    hexToRGB(hex) {
        hex = (hex || '').replace('#', '');
        if (hex.length === 3) hex = hex[0]+hex[0]+hex[1]+hex[1]+hex[2]+hex[2];
        return {
            r: parseInt(hex.substring(0, 2), 16) || 156,
            g: parseInt(hex.substring(2, 4), 16) || 106,
            b: parseInt(hex.substring(4, 6), 16) || 222
        };
    },

    // ---- Shared Chart.js theme helpers ----

    _cv() {
        return window.ThemeManager ? ThemeManager.getCSSVar : () => '';
    },

    _themedTooltip() {
        const cv = this._cv();
        return {
            backgroundColor: cv('--chart-bg'),
            titleColor: cv('--chart-text'),
            bodyColor: cv('--chart-text-secondary'),
            borderColor: cv('--chart-accent'),
            borderWidth: 1
        };
    },

    _themedLegend(position, extra) {
        const cv = this._cv();
        return Object.assign({
            position: position || 'top',
            labels: {
                color: cv('--chart-text'),
                font: { family: 'Inter', size: 12 }
            }
        }, extra || {});
    },

    _themedScales(opts) {
        const cv = this._cv();
        const scales = {};
        if (opts.y !== false) {
            scales.y = Object.assign({
                beginAtZero: true,
                ticks: { color: cv('--chart-text-secondary'), font: { family: 'Inter', size: 11 } },
                grid: { color: cv('--chart-grid') }
            }, opts.y || {});
        }
        if (opts.x !== false) {
            scales.x = Object.assign({
                ticks: {
                    color: cv('--chart-text-secondary'),
                    font: { family: 'Inter', size: 11 },
                    maxRotation: 45,
                    minRotation: 0
                },
                grid: { color: cv('--chart-grid') }
            }, opts.x || {});
        }
        return scales;
    },

    _detectFields(data, fields) {
        if (!fields) fields = Object.keys(data[0] || {});
        const labelField = fields[0];
        const valueField = fields.find(f =>
            f === '_count' || f.includes('count') || f === 'sum' || f === 'avg'
        ) || fields[1];
        return { labelField, valueField, fields };
    },

    // ---- Pie Chart ----

    renderPieChart(container, opts) {
        const data = opts.data;
        if (!data || data.length === 0) return null;

        const { labelField, valueField } = this._detectFields(data, opts.fields);
        const limit = (opts.config && opts.config.limit) || 10;
        const cv = this._cv();

        let chartData = data.map(row => ({
            label: String(row[labelField] || 'Unknown'),
            value: parseFloat(row[valueField]) || 0
        }));
        chartData.sort((a, b) => b.value - a.value);

        const topItems = chartData.slice(0, limit);
        const remaining = chartData.slice(limit);

        const labels = topItems.map(i => i.label);
        const values = topItems.map(i => i.value);

        if (remaining.length > 0) {
            labels.push(`Others (${remaining.length})`);
            values.push(remaining.reduce((s, i) => s + i.value, 0));
        }

        // Create wrapper with fresh canvas (Chart.js pie needs this)
        const wrapper = document.createElement('div');
        wrapper.className = 'pie-chart-wrapper';
        wrapper.style.cssText = `position:relative;width:100%;height:${opts.height || '400px'};`;
        container.appendChild(wrapper);

        const canvas = document.createElement('canvas');
        wrapper.appendChild(canvas);

        const chart = new Chart(canvas, {
            type: 'pie',
            data: {
                labels,
                datasets: [{
                    data: values,
                    backgroundColor: this.PIE_COLORS.slice(0, values.length),
                    borderColor: cv('--chart-border'),
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: this._themedLegend('bottom', { labels: { color: cv('--chart-text'), font: { family: 'Inter', size: 12 }, padding: 20 } }),
                    tooltip: this._themedTooltip()
                },
                layout: { padding: 20 }
            }
        });

        return { chart, wrapper };
    },

    // ---- Bar Chart ----

    renderBarChart(canvas, opts) {
        const data = opts.data;
        if (!data || data.length === 0) return null;

        const { labelField, valueField } = this._detectFields(data, opts.fields);
        const limit = (opts.config && opts.config.limit) || 10;
        const cv = this._cv();

        let chartData = data.map(row => ({
            label: String(row[labelField] || 'Unknown'),
            value: parseFloat(row[valueField]) || 0
        }));
        chartData.sort((a, b) => b.value - a.value);
        const topItems = chartData.slice(0, limit);

        const labels = topItems.map(i => i.label);
        const values = topItems.map(i => i.value);

        const chart = new Chart(canvas, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    label: valueField.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase()),
                    data: values,
                    backgroundColor: cv('--chart-accent'),
                    borderColor: cv('--chart-accent-dark'),
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: opts.maintainAspectRatio !== false,
                plugins: {
                    legend: this._themedLegend('top'),
                    tooltip: this._themedTooltip()
                },
                scales: this._themedScales({
                    x: { ticks: { color: cv('--chart-text-secondary'), font: { family: 'Inter', size: 11 }, maxRotation: 45, minRotation: 0 } }
                }),
                layout: { padding: 20 }
            }
        });

        return { chart };
    },

    // ---- Time Chart ----

    renderTimeChart(canvas, opts) {
        const data = opts.data;
        if (!data || data.length === 0) return null;

        const fields = opts.fields || Object.keys(data[0] || {});
        const timeField = fields.find(f => f === 'time_bucket') || fields[0];
        const valueFields = fields.filter(f =>
            f !== timeField && f !== 'time_bucket' &&
            (f === '_count' || f === 'count' || f.startsWith('sum_') ||
             f.startsWith('avg_') || f.startsWith('min_') || f.startsWith('max_') ||
             f.startsWith('bucket_') || f.startsWith('stddev_'))
        );
        const groupFields = fields.filter(f => f !== timeField && !valueFields.includes(f));
        const cv = this._cv();
        const valueField = valueFields[0] || fields[1];

        let datasets, labels;

        if (groupFields.length > 0) {
            const groupField = groupFields[0];
            const groups = {};
            data.forEach(row => {
                const key = String(row[groupField] || 'Unknown');
                if (!groups[key]) groups[key] = [];
                groups[key].push(row);
            });

            datasets = Object.entries(groups).map(([key, rows], idx) => ({
                label: key,
                data: rows.map(r => parseFloat(r[valueField]) || 0),
                borderColor: this.SERIES_COLORS[idx % this.SERIES_COLORS.length],
                backgroundColor: this.SERIES_COLORS[idx % this.SERIES_COLORS.length] + '20',
                fill: true,
                tension: 0.3,
                pointRadius: 2,
                pointHoverRadius: 5,
                borderWidth: 2
            }));

            labels = Object.values(groups)[0].map(r => String(r[timeField] || ''));
        } else {
            labels = data.map(r => String(r[timeField] || ''));
            datasets = [{
                label: valueField.replace(/_/g, ' '),
                data: data.map(r => parseFloat(r[valueField]) || 0),
                borderColor: cv('--chart-accent'),
                backgroundColor: cv('--chart-accent') + '20',
                fill: true,
                tension: 0.3,
                pointRadius: 2,
                pointHoverRadius: 5,
                borderWidth: 2
            }];
        }

        const chart = new Chart(canvas, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: opts.maintainAspectRatio !== false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: Object.assign(this._themedLegend('top'), {
                        display: datasets.length > 1,
                        labels: {
                            color: cv('--chart-text'),
                            font: { family: 'Inter', size: 12 },
                            usePointStyle: true,
                            pointStyle: 'circle'
                        }
                    }),
                    tooltip: this._themedTooltip()
                },
                scales: this._themedScales({
                    x: {
                        ticks: {
                            color: cv('--chart-text-secondary'),
                            font: { family: 'Inter', size: 10 },
                            maxRotation: 45, minRotation: 0, maxTicksLimit: 20
                        }
                    }
                }),
                layout: { padding: 10 }
            }
        });

        return { chart };
    },

    // ---- Histogram ----

    renderHistogram(canvas, opts) {
        const data = opts.data;
        if (!data || data.length === 0) return null;

        const labels = [];
        const values = [];

        data.forEach(row => {
            const lower = parseFloat(row._bin_lower);
            const upper = parseFloat(row._bin_upper);
            const count = parseFloat(row._bin_count) || 0;
            if (!isNaN(lower) && !isNaN(upper)) {
                labels.push(`${this.formatBinEdge(lower)} - ${this.formatBinEdge(upper)}`);
                values.push(count);
            }
        });

        const fieldName = (opts.config && opts.config.field) || 'value';
        const cv = this._cv();

        const chart = new Chart(canvas, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    label: `Distribution of ${fieldName}`,
                    data: values,
                    backgroundColor: cv('--chart-accent') + 'B0',
                    borderColor: cv('--chart-accent'),
                    borderWidth: 1,
                    barPercentage: 1.0,
                    categoryPercentage: 1.0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: opts.maintainAspectRatio !== false,
                plugins: {
                    legend: this._themedLegend('top'),
                    tooltip: Object.assign(this._themedTooltip(), {
                        callbacks: {
                            label: (context) => `Count: ${context.raw.toLocaleString()}`
                        }
                    })
                },
                scales: this._themedScales({
                    y: {
                        beginAtZero: true,
                        title: { display: true, text: 'Count', color: cv('--chart-text-secondary'), font: { family: 'Inter', size: 11 } },
                        ticks: { color: cv('--chart-text-secondary'), font: { family: 'Inter', size: 11 } }
                    },
                    x: {
                        title: { display: true, text: fieldName, color: cv('--chart-text-secondary'), font: { family: 'Inter', size: 11 } },
                        ticks: { color: cv('--chart-text-secondary'), font: { family: 'Inter', size: 10 }, maxRotation: 45, minRotation: 0 }
                    }
                }),
                layout: { padding: 10 }
            }
        });

        return { chart };
    },

    // ---- Single Value ----

    renderSingleVal(container, opts) {
        const data = opts.data;
        const fields = opts.fields || Object.keys((data && data[0]) || {});

        let rawValue = '--';
        let label = '';
        let valueField = '';

        if (data && data.length > 0) {
            valueField = fields.find(f =>
                f === '_count' || f === 'count' || f.startsWith('sum_') ||
                f.startsWith('avg_') || f.startsWith('min_') || f.startsWith('max_') ||
                f.startsWith('stddev_')
            ) || fields[0];

            const val = data[0][valueField];
            const numValue = parseFloat(val);
            rawValue = isNaN(numValue) ? String(val) : this.formatSingleValue(numValue);
            label = (opts.config && opts.config.label) || valueField.replace(/_/g, ' ');
        }

        // Conditional formatting
        let valueStyle = '';
        const rules = opts.coloringRules || [];
        for (const rule of rules) {
            if (!rule.column) continue;
            if (rule.column === valueField && data && data.length > 0) {
                const cellVal = data[0][valueField];
                if (this._evaluateRule(cellVal, rule)) {
                    valueStyle = `color: ${rule.color || '#8b5cf6'};`;
                    break;
                }
            }
        }

        const html = `
            <div class="singleval-display">
                <div class="singleval-value" style="${valueStyle}">${Utils.escapeHtml(rawValue)}</div>
                <div class="singleval-label">${Utils.escapeHtml(label)}</div>
            </div>
        `;

        if (opts.returnHtml) return html;

        const el = document.createElement('div');
        el.innerHTML = html;
        if (container) container.appendChild(el.firstElementChild);
        return el.firstElementChild;
    },

    _evaluateRule(cellVal, rule) {
        if (cellVal === undefined || cellVal === null) return false;
        const op = rule.operator || '=';
        const ruleVal = rule.value;
        if (op === 'contains') return String(cellVal).toLowerCase().includes(String(ruleVal).toLowerCase());
        if (op === '>' || op === '>=' || op === '<' || op === '<=') {
            const numCell = parseFloat(cellVal);
            const numRule = parseFloat(ruleVal);
            if (isNaN(numCell) || isNaN(numRule)) return false;
            if (op === '>') return numCell > numRule;
            if (op === '>=') return numCell >= numRule;
            if (op === '<') return numCell < numRule;
            return numCell <= numRule;
        }
        return String(cellVal) === String(ruleVal);
    },

    // ---- Heatmap ----

    renderHeatmap(container, opts) {
        const data = opts.data;
        if (!data || data.length === 0) return null;

        const config = opts.config || {};
        const xField = config.xField || '_heatmap_x';
        const yField = config.yField || '_heatmap_y';

        const xVals = [];
        const yVals = [];
        const xSet = new Set();
        const ySet = new Set();
        const valueMap = {};
        let maxVal = 0;

        const xKey = data.length > 0 && data[0]._heatmap_x !== undefined ? '_heatmap_x' : xField;
        const yKey = data.length > 0 && data[0]._heatmap_y !== undefined ? '_heatmap_y' : yField;
        const vKey = data.length > 0 && data[0]._heatmap_value !== undefined ? '_heatmap_value' : '_count';

        data.forEach(row => {
            const x = String(row[xKey] || '');
            const y = String(row[yKey] || '');
            const v = parseFloat(row[vKey]) || 0;
            if (!xSet.has(x)) { xSet.add(x); xVals.push(x); }
            if (!ySet.has(y)) { ySet.add(y); yVals.push(y); }
            valueMap[`${x}||${y}`] = v;
            if (v > maxVal) maxVal = v;
        });

        const smartSort = (a, b) => {
            const na = parseFloat(a), nb = parseFloat(b);
            if (!isNaN(na) && !isNaN(nb)) return na - nb;
            return a.localeCompare(b);
        };
        xVals.sort(smartSort);
        yVals.sort(smartSort);

        const cv = this._cv();
        const isLight = document.documentElement.getAttribute('data-theme') === 'light';
        const textColor = cv('--chart-text-secondary') || '#8b8fa3';
        const textPrimary = cv('--chart-text') || '#e0e0e0';
        const borderColor = cv('--border-color') || (isLight ? '#d0d0d0' : '#333');

        const colorStops = isLight ? [
            { r: 198, g: 219, b: 239 },
            { r: 107, g: 174, b: 214 },
            { r: 49, g: 130, b: 189 },
            { r: 8, g: 81, b: 156 },
            { r: 8, g: 48, b: 107 },
        ] : [
            { r: 68, g: 1, b: 84 },
            { r: 59, g: 82, b: 139 },
            { r: 33, g: 145, b: 140 },
            { r: 94, g: 201, b: 98 },
            { r: 253, g: 231, b: 37 },
        ];
        const emptyCellColor = isLight ? '#f0f0f0' : 'rgba(255, 255, 255, 0.04)';
        const cellBorderColor = isLight ? 'rgba(0, 0, 0, 0.10)' : 'rgba(255, 255, 255, 0.06)';

        const interpolateColor = (t) => {
            t = Math.max(0, Math.min(1, t));
            const segment = t * (colorStops.length - 1);
            const i = Math.min(Math.floor(segment), colorStops.length - 2);
            const frac = segment - i;
            const a = colorStops[i], b = colorStops[i + 1];
            return {
                r: Math.round(a.r + (b.r - a.r) * frac),
                g: Math.round(a.g + (b.g - a.g) * frac),
                b: Math.round(a.b + (b.b - a.b) * frac),
            };
        };

        const cellPad = 2;
        const xLabelHeight = 50;
        const axisTitleSize = 28;
        const legendBarWidth = 14;
        const legendLabelWidth = 50;
        const legendGap = 20;

        const measureCanvas = document.createElement('canvas');
        const measureCtx = measureCanvas.getContext('2d');
        measureCtx.font = '11px Inter, sans-serif';
        const maxYLabelPx = yVals.reduce((max, y) => {
            const label = y.length > 20 ? y.substring(0, 20) + '\u2026' : y;
            return Math.max(max, measureCtx.measureText(label).width);
        }, 0);
        const yLabelWidth = Math.max(40, Math.min(200, Math.ceil(maxYLabelPx) + 24));

        const availableWidth = (container.clientWidth || 800) - 24;
        const gridAvailWidth = availableWidth - yLabelWidth - legendGap - legendBarWidth - legendLabelWidth;
        const cellW = Math.max(20, Math.floor(gridAvailWidth / Math.max(xVals.length, 1)));
        const cellH = Math.max(28, Math.min(60, Math.floor(400 / Math.max(yVals.length, 1))));
        const gridWidth = xVals.length * cellW;
        const gridHeight = yVals.length * cellH;
        const totalWidth = yLabelWidth + gridWidth + legendGap + legendBarWidth + legendLabelWidth;
        const totalHeight = xLabelHeight + gridHeight + axisTitleSize;

        const heatmapDiv = document.createElement('div');
        heatmapDiv.className = 'heatmap-container';
        heatmapDiv.style.cssText = 'overflow:auto;position:relative;';

        const canvas = document.createElement('canvas');
        const dpr = window.devicePixelRatio || 1;
        canvas.width = totalWidth * dpr;
        canvas.height = totalHeight * dpr;
        canvas.style.cssText = `display:block;width:${totalWidth}px;height:${totalHeight}px;image-rendering:auto;margin:0 auto;`;
        heatmapDiv.appendChild(canvas);

        const overlayCanvas = document.createElement('canvas');
        overlayCanvas.width = totalWidth * dpr;
        overlayCanvas.height = totalHeight * dpr;
        overlayCanvas.style.cssText = `display:block;width:${totalWidth}px;height:${totalHeight}px;position:absolute;top:0;left:50%;transform:translateX(-50%);pointer-events:none;`;
        heatmapDiv.appendChild(overlayCanvas);
        const overlayCtx = overlayCanvas.getContext('2d');
        overlayCtx.setTransform(dpr, 0, 0, dpr, 0, 0);

        container.appendChild(heatmapDiv);

        const ctx = canvas.getContext('2d');
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.imageSmoothingEnabled = true;
        ctx.imageSmoothingQuality = 'high';
        ctx.clearRect(0, 0, totalWidth, totalHeight);

        // Y-axis title
        ctx.save();
        ctx.font = '12px Inter, sans-serif';
        ctx.fillStyle = textPrimary;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.translate(12, xLabelHeight + gridHeight / 2);
        ctx.rotate(-Math.PI / 2);
        ctx.fillText(yField, 0, 0);
        ctx.restore();

        // X-axis title
        ctx.font = '12px Inter, sans-serif';
        ctx.fillStyle = textPrimary;
        ctx.textAlign = 'center';
        ctx.fillText(xField, yLabelWidth + gridWidth / 2, xLabelHeight + gridHeight + axisTitleSize - 6);

        // Column headers
        ctx.save();
        ctx.font = '11px Inter, sans-serif';
        ctx.fillStyle = textColor;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'bottom';
        xVals.forEach((x, i) => {
            const cx = yLabelWidth + i * cellW + cellW / 2;
            const label = x.length > 12 ? x.substring(0, 12) + '\u2026' : x;
            ctx.fillText(label, cx, xLabelHeight - 6);
        });
        ctx.restore();

        // Row labels
        ctx.font = '11px Inter, sans-serif';
        ctx.fillStyle = textColor;
        ctx.textAlign = 'right';
        ctx.textBaseline = 'middle';
        yVals.forEach((y, j) => {
            const cy = xLabelHeight + j * cellH + cellH / 2;
            const label = y.length > 20 ? y.substring(0, 20) + '\u2026' : y;
            ctx.fillText(label, yLabelWidth - 8, cy);
        });

        // Grid cells
        yVals.forEach((y, j) => {
            xVals.forEach((x, i) => {
                const key = `${x}||${y}`;
                const hasData = key in valueMap;
                const v = hasData ? valueMap[key] : 0;
                const cx = yLabelWidth + i * cellW + cellPad;
                const cy = xLabelHeight + j * cellH + cellPad;
                const cw = cellW - cellPad * 2;
                const ch = cellH - cellPad * 2;
                const radius = 3;

                if (!hasData || v === 0) {
                    ctx.fillStyle = emptyCellColor;
                } else {
                    const intensity = maxVal > 0 ? v / maxVal : 0;
                    const c = interpolateColor(intensity);
                    ctx.fillStyle = `rgb(${c.r}, ${c.g}, ${c.b})`;
                }

                ctx.beginPath();
                ctx.moveTo(cx + radius, cy);
                ctx.lineTo(cx + cw - radius, cy);
                ctx.quadraticCurveTo(cx + cw, cy, cx + cw, cy + radius);
                ctx.lineTo(cx + cw, cy + ch - radius);
                ctx.quadraticCurveTo(cx + cw, cy + ch, cx + cw - radius, cy + ch);
                ctx.lineTo(cx + radius, cy + ch);
                ctx.quadraticCurveTo(cx, cy + ch, cx, cy + ch - radius);
                ctx.lineTo(cx, cy + radius);
                ctx.quadraticCurveTo(cx, cy, cx + radius, cy);
                ctx.closePath();
                ctx.fill();

                ctx.strokeStyle = cellBorderColor;
                ctx.lineWidth = 1;
                ctx.stroke();

                if (cellW >= 36 && cellH >= 28 && v > 0) {
                    const intensity = maxVal > 0 ? v / maxVal : 0;
                    const useWhiteText = isLight ? intensity > 0.4 : intensity < 0.7;
                    ctx.fillStyle = useWhiteText ? '#ffffff' : 'rgba(0,0,0,0.75)';
                    ctx.font = 'bold 10px Inter, sans-serif';
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'middle';
                    ctx.fillText(this.formatHeatmapValue(v), cx + cw / 2, cy + ch / 2);
                }
            });
        });

        // Legend
        const legendX = yLabelWidth + gridWidth + legendGap;
        const legendY = xLabelHeight;
        const legendH = gridHeight;
        const scaleSteps = 100;
        const stepH = legendH / scaleSteps;
        const legendRadius = 4;

        ctx.save();
        ctx.beginPath();
        ctx.moveTo(legendX + legendRadius, legendY);
        ctx.lineTo(legendX + legendBarWidth - legendRadius, legendY);
        ctx.quadraticCurveTo(legendX + legendBarWidth, legendY, legendX + legendBarWidth, legendY + legendRadius);
        ctx.lineTo(legendX + legendBarWidth, legendY + legendH - legendRadius);
        ctx.quadraticCurveTo(legendX + legendBarWidth, legendY + legendH, legendX + legendBarWidth - legendRadius, legendY + legendH);
        ctx.lineTo(legendX + legendRadius, legendY + legendH);
        ctx.quadraticCurveTo(legendX, legendY + legendH, legendX, legendY + legendH - legendRadius);
        ctx.lineTo(legendX, legendY + legendRadius);
        ctx.quadraticCurveTo(legendX, legendY, legendX + legendRadius, legendY);
        ctx.closePath();
        ctx.clip();

        for (let s = 0; s < scaleSteps; s++) {
            const t = 1 - s / scaleSteps;
            const c = interpolateColor(t);
            ctx.fillStyle = `rgb(${c.r}, ${c.g}, ${c.b})`;
            ctx.fillRect(legendX, legendY + s * stepH, legendBarWidth, stepH + 1);
        }
        ctx.restore();

        // Legend border
        ctx.beginPath();
        ctx.moveTo(legendX + legendRadius, legendY);
        ctx.lineTo(legendX + legendBarWidth - legendRadius, legendY);
        ctx.quadraticCurveTo(legendX + legendBarWidth, legendY, legendX + legendBarWidth, legendY + legendRadius);
        ctx.lineTo(legendX + legendBarWidth, legendY + legendH - legendRadius);
        ctx.quadraticCurveTo(legendX + legendBarWidth, legendY + legendH, legendX + legendBarWidth - legendRadius, legendY + legendH);
        ctx.lineTo(legendX + legendRadius, legendY + legendH);
        ctx.quadraticCurveTo(legendX, legendY + legendH, legendX, legendY + legendH - legendRadius);
        ctx.lineTo(legendX, legendY + legendRadius);
        ctx.quadraticCurveTo(legendX, legendY, legendX + legendRadius, legendY);
        ctx.closePath();
        ctx.strokeStyle = borderColor;
        ctx.lineWidth = 1;
        ctx.stroke();

        // Legend ticks
        const niceTickValues = this.heatmapNiceTicks(0, maxVal, 5);
        ctx.font = '10px Inter, sans-serif';
        ctx.fillStyle = textColor;
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        niceTickValues.forEach(val => {
            const frac = maxVal > 0 ? 1 - val / maxVal : 0;
            const ty = legendY + frac * legendH;
            ctx.fillRect(legendX + legendBarWidth, ty - 0.5, 4, 1);
            ctx.fillText(this.formatHeatmapValue(val), legendX + legendBarWidth + 6, ty);
        });

        // Tooltip
        const tooltip = document.createElement('div');
        tooltip.className = 'heatmap-tooltip';
        tooltip.style.cssText = `display:none;position:fixed;padding:8px 12px;border-radius:6px;font-size:12px;pointer-events:none;z-index:1000;background:${cv('--bg-tertiary') || '#2a2a3e'};color:${cv('--chart-text') || '#e0e0e0'};border:1px solid ${borderColor};font-family:Inter,sans-serif;box-shadow:0 4px 12px rgba(0,0,0,0.3);`;
        document.body.appendChild(tooltip);

        let lastHoverXi = -1, lastHoverYi = -1;

        canvas.addEventListener('mousemove', (e) => {
            const rect = canvas.getBoundingClientRect();
            const scaleX = totalWidth / rect.width;
            const scaleY = totalHeight / rect.height;
            const mx = (e.clientX - rect.left) * scaleX;
            const my = (e.clientY - rect.top) * scaleY;
            const xi = Math.floor((mx - yLabelWidth) / cellW);
            const yi = Math.floor((my - xLabelHeight) / cellH);

            if (xi >= 0 && xi < xVals.length && yi >= 0 && yi < yVals.length) {
                const x = xVals[xi];
                const y = yVals[yi];
                const v = valueMap[`${x}||${y}`] || 0;
                tooltip.innerHTML = `<strong>${Utils.escapeHtml(xField)}</strong>: ${Utils.escapeHtml(x)}<br><strong>${Utils.escapeHtml(yField)}</strong>: ${Utils.escapeHtml(y)}<br><strong>Value</strong>: ${v.toLocaleString()}`;
                tooltip.style.display = 'block';
                tooltip.style.left = (e.clientX + 14) + 'px';
                tooltip.style.top = (e.clientY + 14) + 'px';

                if (xi !== lastHoverXi || yi !== lastHoverYi) {
                    lastHoverXi = xi;
                    lastHoverYi = yi;
                    overlayCtx.clearRect(0, 0, totalWidth, totalHeight);
                    const hlColor = isLight ? 'rgba(0, 0, 0, 0.06)' : 'rgba(255, 255, 255, 0.06)';
                    overlayCtx.fillStyle = hlColor;
                    overlayCtx.fillRect(yLabelWidth, xLabelHeight + yi * cellH, gridWidth, cellH);
                    overlayCtx.fillRect(yLabelWidth + xi * cellW, xLabelHeight, cellW, gridHeight);
                    const cellHlColor = isLight ? 'rgba(0, 0, 0, 0.08)' : 'rgba(255, 255, 255, 0.10)';
                    overlayCtx.fillStyle = cellHlColor;
                    overlayCtx.fillRect(yLabelWidth + xi * cellW, xLabelHeight + yi * cellH, cellW, cellH);
                }
            } else {
                tooltip.style.display = 'none';
                if (lastHoverXi !== -1) {
                    lastHoverXi = -1;
                    lastHoverYi = -1;
                    overlayCtx.clearRect(0, 0, totalWidth, totalHeight);
                }
            }
        });

        canvas.addEventListener('mouseleave', () => {
            tooltip.style.display = 'none';
            lastHoverXi = -1;
            lastHoverYi = -1;
            overlayCtx.clearRect(0, 0, totalWidth, totalHeight);
        });

        return { tooltip, container: heatmapDiv };
    },

    // ---- Graph (simple - for notebooks/dashboards) ----

    renderGraphSimple(container, opts) {
        if (!container || !window.vis) return null;

        const config = opts.config || {};
        const childField = config.childField;
        const parentField = config.parentField;
        if (!childField || !parentField) return null;

        const data = opts.data || [];
        const fields = opts.fields || Object.keys(data[0] || {});
        const limit = config.limit || 100;
        const limitedResults = data.slice(0, limit);
        const specifiedLabels = config.labels || [];
        const labelFields = specifiedLabels.length > 0
            ? specifiedLabels
            : fields.filter(f => f !== childField && f !== parentField);

        const cv = this._cv();
        const nodes = new vis.DataSet();
        const edges = new vis.DataSet();
        const nodeDetails = new Map();

        limitedResults.forEach((result) => {
            const childId = result[childField];
            const parentId = result[parentField];
            if (childId && !nodeDetails.has(childId)) {
                const details = {};
                labelFields.forEach(f => { if (result[f] != null && result[f] !== '') details[f] = result[f]; });
                nodeDetails.set(childId, details);
            }
            if (parentId && !nodeDetails.has(parentId)) {
                nodeDetails.set(parentId, {});
            }
        });

        const parentIds = new Set(limitedResults.map(r => r[parentField]).filter(Boolean));
        nodeDetails.forEach((details, id) => {
            const isParent = parentIds.has(id);
            let shortLabel = id;
            if (specifiedLabels.length > 0) {
                const parts = specifiedLabels.map(f => details[f]).filter(v => v != null && v !== '');
                if (parts.length > 0) {
                    const joined = parts.join(' | ');
                    shortLabel = joined.length > 30 ? joined.substring(0, 30) + '\u2026' : joined;
                } else {
                    shortLabel = id.length > 12 ? id.substring(0, 12) + '\u2026' : id;
                }
            } else {
                shortLabel = id.length > 12 ? id.substring(0, 12) + '\u2026' : id;
            }
            nodes.add({
                id, label: shortLabel,
                size: 16,
                color: {
                    background: isParent ? (cv('--graph-node-parent') || '#3498db') : (cv('--graph-node-child') || '#555'),
                    border: isParent ? (cv('--graph-node-parent') || '#3498db') : (cv('--graph-node-child') || '#555'),
                },
                font: { color: cv('--graph-label') || '#eee', size: 11, face: 'Inter', vadjust: -4, strokeWidth: 3, strokeColor: cv('--graph-label-stroke') || 'rgba(0,0,0,0.5)' },
                shape: 'dot',
            });
        });

        limitedResults.forEach((result) => {
            const childId = result[childField];
            const parentId = result[parentField];
            if (childId && parentId) {
                edges.add({
                    from: parentId, to: childId,
                    arrows: { to: { enabled: true, scaleFactor: 0.6, type: 'arrow' } },
                    color: { color: cv('--graph-edge') || '#555', opacity: 0.5 },
                    width: 1.5,
                    smooth: { enabled: true, type: 'cubicBezier', forceDirection: 'vertical', roundness: 0.4 },
                });
            }
        });

        return new vis.Network(container, { nodes, edges }, {
            layout: { hierarchical: { direction: 'UD', sortMethod: 'directed', levelSeparation: 100, nodeSpacing: 150, treeSpacing: 200 } },
            physics: { enabled: false },
            interaction: { hover: true, zoomView: true, dragView: true, dragNodes: true, zoomSpeed: 1.0 }
        });
    },

    // ---- Chat pre-processed format ----

    renderFromPreprocessed(canvas, args) {
        if (!args || !args.labels || !args.datasets) return null;

        const cv = this._cv();
        const chartType = args.chart_type || 'bar';
        let config;

        if (chartType === 'pie') {
            const ds = args.datasets[0] || { label: '', data: [] };
            config = {
                type: 'pie',
                data: {
                    labels: args.labels,
                    datasets: [{
                        data: ds.data,
                        backgroundColor: this.PIE_COLORS.slice(0, ds.data.length),
                        borderColor: cv('--chart-border'),
                        borderWidth: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: this._themedLegend('bottom', { labels: { color: cv('--chart-text'), font: { family: 'Inter', size: 12 }, padding: 20 } }),
                        tooltip: this._themedTooltip()
                    },
                    layout: { padding: 20 }
                }
            };
        } else if (chartType === 'line') {
            const datasets = args.datasets.map((ds, idx) => ({
                label: ds.label,
                data: ds.data,
                borderColor: args.datasets.length === 1 ? cv('--chart-accent') : this.SERIES_COLORS[idx % this.SERIES_COLORS.length],
                backgroundColor: (args.datasets.length === 1 ? cv('--chart-accent') : this.SERIES_COLORS[idx % this.SERIES_COLORS.length]) + '20',
                fill: true,
                tension: 0.3,
                pointRadius: 2,
                pointHoverRadius: 5,
                borderWidth: 2
            }));
            config = {
                type: 'line',
                data: { labels: args.labels, datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: {
                        legend: Object.assign(this._themedLegend('top'), {
                            display: datasets.length > 1,
                            labels: { color: cv('--chart-text'), font: { family: 'Inter', size: 12 }, usePointStyle: true, pointStyle: 'circle' }
                        }),
                        tooltip: this._themedTooltip()
                    },
                    scales: this._themedScales({
                        x: { ticks: { color: cv('--chart-text-secondary'), font: { family: 'Inter', size: 10 }, maxRotation: 45, minRotation: 0, maxTicksLimit: 20 } }
                    }),
                    layout: { padding: 10 }
                }
            };
        } else {
            const ds = args.datasets[0] || { label: '', data: [] };
            config = {
                type: 'bar',
                data: {
                    labels: args.labels,
                    datasets: [{
                        label: ds.label,
                        data: ds.data,
                        backgroundColor: cv('--chart-accent'),
                        borderColor: cv('--chart-accent-dark'),
                        borderWidth: 1
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: this._themedLegend('top'),
                        tooltip: this._themedTooltip()
                    },
                    scales: this._themedScales({
                        x: { ticks: { color: cv('--chart-text-secondary'), font: { family: 'Inter', size: 11 }, maxRotation: 45, minRotation: 0 } }
                    }),
                    layout: { padding: 20 }
                }
            };
        }

        return new Chart(canvas, config);
    },

    // ---- Dispatcher for notebooks/dashboards ----

    renderOnCanvas(canvas, chartType, opts) {
        const existingChart = Chart.getChart(canvas);
        if (existingChart) existingChart.destroy();

        if (chartType === 'piechart') {
            // Pie needs a wrapper; hide the original canvas
            canvas.style.display = 'none';
            const parent = canvas.parentElement;
            const oldPie = parent.querySelector('.pie-chart-wrapper');
            if (oldPie) oldPie.remove();
            return this.renderPieChart(parent, opts);
        } else if (chartType === 'barchart') {
            return this.renderBarChart(canvas, opts);
        } else if (chartType === 'timechart') {
            return this.renderTimeChart(canvas, opts);
        } else if (chartType === 'histogram') {
            return this.renderHistogram(canvas, opts);
        }
        return null;
    }
};
