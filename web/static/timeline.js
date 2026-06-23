// Timeline rendering - compact inline histogram with time ruler
const Timeline = {
    isSelecting: false,
    selectionStart: null,
    selectionEnd: null,
    selectionStartTime: null,
    selectionEndTime: null,
    _resizeObserver: null,
    _lastRenderData: null,
    _currentTotal: null,
    _currentPeak: null,

    async fetch(timeRange) {
        const queryInput = document.getElementById('queryInput');
        if (!queryInput) return;

        try {
            let baseQuery = queryInput.value.trim();
            baseQuery = baseQuery.split('|')[0].trim();
            if (!baseQuery) baseQuery = '*';

            let requestBody = {
                query: baseQuery,
                start: timeRange.start,
                end: timeRange.end
            };

            if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
                requestBody.fractal_id = window.FractalContext.currentFractal.id;
            }

            const response = await fetch('/api/v1/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody)
            });

            const data = await response.json();
            if (data.success && data.results) {
                this.render(data.results, timeRange);
            }
        } catch (error) {
            // Silently fail
        }
    },

    // Render from pre-bucketed histogram data (used by default recent logs view)
    renderFromHistogram(buckets, timeRange) {
        const timelineSection = document.getElementById('timelineSection');
        const canvas = document.getElementById('timeline');
        const timelineStats = document.getElementById('timelineStats');

        const total = buckets.reduce((a, b) => a + b, 0);
        if (total === 0) {
            if (timelineSection) timelineSection.style.display = 'none';
            return;
        }

        if (timelineSection) timelineSection.style.display = 'block';
        if (timelineStats) timelineStats.style.display = 'none';

        this.currentTimeRange = timeRange;
        this._currentTotal = total;
        this._currentPeak = Math.max(...buckets);
        this._lastRenderData = { buckets, timeRange, isHistogram: true };

        setTimeout(() => {
            if (!canvas) return;
            canvas.style.width = '100%';
            canvas.style.display = 'block';

            if (canvas.offsetWidth === 0 || canvas.offsetHeight === 0) return;

            this._drawBars(canvas, buckets);

            const start = new Date(timeRange.start);
            const duration = new Date(timeRange.end) - start;
            const bucketSize = duration / buckets.length;

            this.setupInteraction(canvas, buckets, canvas.offsetWidth / buckets.length, start, bucketSize, duration);
            this._setupResizeObserver(canvas);
        }, 100);
    },

    render(results, timeRange) {
        const timelineSection = document.getElementById('timelineSection');
        const canvas = document.getElementById('timeline');
        const timelineStats = document.getElementById('timelineStats');

        if (!results || results.length === 0) {
            if (timelineSection) timelineSection.style.display = 'none';
            return;
        }

        const hasTimestamp = results.length > 0 &&
            (results[0].hasOwnProperty('timestamp') || results[0].timestamp !== undefined);

        if (!hasTimestamp) {
            if (timelineSection) timelineSection.style.display = 'none';
            return;
        }

        if (timelineSection) timelineSection.style.display = 'block';
        if (timelineStats) timelineStats.style.display = 'none';

        this.currentTimeRange = timeRange;
        this._lastRenderData = { results, timeRange };

        setTimeout(() => {
            if (!canvas) return;

            canvas.style.width = '100%';
            canvas.style.display = 'block';

            if (canvas.offsetWidth === 0 || canvas.offsetHeight === 0) return;

            const validTimestamps = [];
            results.forEach(result => {
                if (result.timestamp) {
                    let tsString = result.timestamp;
                    if (typeof tsString === 'string' && tsString.includes(' ')) {
                        tsString = tsString.replace(' ', 'T') + 'Z';
                    }
                    const ts = new Date(tsString);
                    if (!isNaN(ts.getTime())) validTimestamps.push(ts);
                }
            });

            const start = new Date(timeRange.start);
            const end = new Date(timeRange.end);
            const duration = end - start;

            let bucketSize, bucketCount;
            if (duration <= 3600000) {
                bucketSize = 30000;
                bucketCount = 120;
            } else if (duration <= 86400000) {
                bucketSize = 900000;
                bucketCount = 96;
            } else if (duration <= 604800000) {
                bucketSize = 3600000;
                bucketCount = Math.ceil(duration / 3600000);
            } else {
                bucketSize = 10800000;
                bucketCount = Math.ceil(duration / bucketSize);
            }

            const buckets = new Array(bucketCount).fill(0);
            validTimestamps.forEach(ts => {
                const idx = Math.floor((ts - start) / bucketSize);
                if (idx >= 0 && idx < bucketCount) buckets[idx]++;
            });

            this._currentTotal = buckets.reduce((a, b) => a + b, 0);
            this._currentPeak = Math.max(...buckets);

            this._drawBars(canvas, buckets);

            this.setupInteraction(canvas, buckets, canvas.offsetWidth / bucketCount, start, bucketSize, duration);
            this._setupResizeObserver(canvas);
        }, 100);
    },

    _drawBars(canvas, buckets) {
        const ctx = canvas.getContext('2d');
        const dpr = window.devicePixelRatio || 1;

        canvas.width = canvas.offsetWidth * dpr;
        canvas.height = canvas.offsetHeight * dpr;
        ctx.scale(dpr, dpr);

        const width = canvas.offsetWidth;
        const totalHeight = canvas.offsetHeight;
        const BAR_AREA_H = 44;
        const RULER_H = totalHeight - BAR_AREA_H;

        const barWidth = width / buckets.length;
        const maxCount = Math.max(...buckets, 1);
        const barGap = buckets.length > 80 ? 0 : 1;
        const barRadius = barWidth > 4 ? 1.5 : 0;

        ctx.clearRect(0, 0, width, totalHeight);

        const barStart   = ThemeManager.getCSSVar('--timeline-bar-start');
        const barEnd     = ThemeManager.getCSSVar('--timeline-bar-end');
        const textColor  = ThemeManager.getCSSVar('--timeline-text');
        const borderColor = ThemeManager.getCSSVar('--border-color');
        const bgPrimary  = ThemeManager.getCSSVar('--bg-primary');
        const fontFamily = ThemeManager.getCSSVar('--font-mono') || 'monospace';
        const rulerFont  = `10px ${fontFamily}`;

        // Compute ruler ticks — must happen before bar drawing so gridlines render under bars
        ctx.font = rulerFont;
        const ticks = this.currentTimeRange
            ? this._computeRulerTicks(
                new Date(this.currentTimeRange.start).getTime(),
                new Date(this.currentTimeRange.end).getTime(),
                width, ctx)
            : [];

        // Gridlines in bar area — drawn first so bars render on top
        if (ticks.length > 0) {
            ctx.save();
            ctx.strokeStyle = borderColor;
            ctx.globalAlpha = 0.2;
            ctx.lineWidth = 0.5;
            ticks.forEach(({ x }) => {
                const px = Math.round(x) + 0.5;
                ctx.beginPath();
                ctx.moveTo(px, 0);
                ctx.lineTo(px, BAR_AREA_H);
                ctx.stroke();
            });
            ctx.restore();
        }

        // Bars
        buckets.forEach((count, i) => {
            if (count === 0) return;
            const barH = Math.max((count / maxCount) * (BAR_AREA_H - 4), 2);
            const x = i * barWidth;
            const y = BAR_AREA_H - barH;

            const gradient = ctx.createLinearGradient(0, y, 0, BAR_AREA_H);
            gradient.addColorStop(0, barStart);
            gradient.addColorStop(1, barEnd);
            ctx.fillStyle = gradient;

            if (barRadius > 0) {
                ctx.beginPath();
                ctx.roundRect(x, y, barWidth - barGap, barH, [barRadius, barRadius, 0, 0]);
                ctx.fill();
            } else {
                ctx.fillRect(x, y, barWidth - barGap, barH);
            }
        });

        // Separator line between bar area and ruler
        ctx.save();
        ctx.strokeStyle = borderColor;
        ctx.globalAlpha = 0.35;
        ctx.lineWidth = 0.5;
        ctx.beginPath();
        ctx.moveTo(0, BAR_AREA_H + 0.5);
        ctx.lineTo(width, BAR_AREA_H + 0.5);
        ctx.stroke();
        ctx.restore();

        // Ruler: tick marks + labels
        if (ticks.length > 0 && RULER_H > 0) {
            const tickTop = BAR_AREA_H + 1;
            const labelY  = BAR_AREA_H + RULER_H / 2 + 1;

            ctx.font = rulerFont;
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';

            ticks.forEach(({ x, label, labelWidth }) => {
                const px = Math.round(x) + 0.5;

                // Tick mark
                ctx.save();
                ctx.strokeStyle = borderColor;
                ctx.globalAlpha = 0.4;
                ctx.lineWidth = 0.5;
                ctx.beginPath();
                ctx.moveTo(px, tickTop);
                ctx.lineTo(px, tickTop + 4);
                ctx.stroke();
                ctx.restore();

                // Label — clamped so it never clips outside canvas edges
                const halfW  = labelWidth / 2;
                const labelX = Math.max(halfW + 3, Math.min(width - halfW - 3, x));
                ctx.fillStyle = textColor;
                ctx.globalAlpha = 0.8;
                ctx.fillText(label, labelX, labelY);
                ctx.globalAlpha = 1;
            });
        }

        // Stat badge — top-right of bar area, drawn last so it sits above bars
        const total = this._currentTotal;
        const peak  = this._currentPeak;
        if (total != null && peak != null && total > 0) {
            const badgeText = `Total ${total.toLocaleString()} · Peak ${peak.toLocaleString()}`;
            ctx.font = rulerFont;
            ctx.textAlign = 'left';
            ctx.textBaseline = 'middle';
            const textW = ctx.measureText(badgeText).width;

            const PAD_H = 5, PAD_V = 3;
            const badgeW = textW + PAD_H * 2;
            const badgeH = 10 + PAD_V * 2;   // 16px total
            const badgeX = width - badgeW - 6;
            const badgeY = 5;

            ctx.save();
            ctx.globalAlpha = 0.85;
            ctx.fillStyle = bgPrimary;
            ctx.beginPath();
            ctx.roundRect(badgeX, badgeY, badgeW, badgeH, 3);
            ctx.fill();
            ctx.restore();

            ctx.save();
            ctx.globalAlpha = 0.18;
            ctx.strokeStyle = borderColor;
            ctx.lineWidth = 0.5;
            ctx.beginPath();
            ctx.roundRect(badgeX, badgeY, badgeW, badgeH, 3);
            ctx.stroke();
            ctx.restore();

            ctx.font = rulerFont;
            ctx.fillStyle = textColor;
            ctx.globalAlpha = 0.9;
            ctx.fillText(badgeText, badgeX + PAD_H, badgeY + badgeH / 2);
            ctx.globalAlpha = 1;
        }

        this._currentBuckets = buckets;
    },

    // Returns the first tick time >= startMs that aligns to a clean boundary for intervalMs.
    _firstTickAfter(startMs, intervalMs) {
        if (intervalMs < 60000) {
            // 30 s — snap to :00 or :30 second mark
            const d = new Date(startMs);
            const s = d.getSeconds();
            if (s === 0 || s === 30) return d.getTime() >= startMs ? d.getTime() : d.getTime() + intervalMs;
            d.setSeconds(s < 30 ? 30 : 60, 0); // setSeconds(60) rolls over to next minute
            return d.getTime();
        }
        if (intervalMs < 3600000) {
            // Minute-level — snap to interval-aligned minute from the hour
            const d = new Date(startMs);
            d.setSeconds(0, 0);
            const intervalMins = intervalMs / 60000;
            const snapped = Math.ceil(d.getMinutes() / intervalMins) * intervalMins;
            d.setMinutes(snapped, 0, 0); // handles overflow (e.g. 60 → next hour)
            return d.getTime() >= startMs ? d.getTime() : d.getTime() + intervalMs;
        }
        if (intervalMs < 86400000) {
            // Hour-level — snap to interval-aligned hour from local midnight
            const d = new Date(startMs);
            d.setMinutes(0, 0, 0);
            const intervalHours = intervalMs / 3600000;
            const snapped = Math.ceil(d.getHours() / intervalHours) * intervalHours;
            d.setHours(snapped, 0, 0, 0); // setHours handles >=24 (rolls into next day)
            return d.getTime() >= startMs ? d.getTime() : d.getTime() + intervalMs;
        }
        // Day-level — snap to local midnight, advance by day multiples
        const d = new Date(startMs);
        d.setHours(0, 0, 0, 0);
        const days = intervalMs / 86400000;
        while (d.getTime() < startMs) d.setDate(d.getDate() + days);
        return d.getTime();
    },

    // Finds the finest interval where no two adjacent labels collide.
    // Returns array of { x, label, labelWidth } in canvas-pixel coordinates.
    _computeRulerTicks(startMs, endMs, canvasWidth, ctx) {
        const duration = endMs - startMs;
        const MIN_GAP  = 10; // minimum px gap between adjacent label edges

        const INTERVALS = [
            30000, 60000, 300000, 900000, 1800000,
            3600000, 7200000, 14400000, 21600000, 43200000,
            86400000, 172800000, 604800000
        ];

        const startDate = new Date(startMs);
        const endDate   = new Date(endMs);

        for (const interval of INTERVALS) {
            const firstTick = this._firstTickAfter(startMs, interval);
            if (firstTick > endMs) continue;

            const ticks = [];
            for (let t = firstTick; t <= endMs; t += interval) {
                const x = (t - startMs) / duration * canvasWidth;
                const label = this._formatRulerLabel(new Date(t), interval, startDate, endDate);
                const labelWidth = ctx.measureText(label).width;
                ticks.push({ x, label, labelWidth });
            }

            if (ticks.length === 0) continue;

            // Reject this interval if any adjacent pair of labels would overlap
            let ok = true;
            for (let i = 1; i < ticks.length; i++) {
                const gap = ticks[i].x - ticks[i - 1].x
                    - (ticks[i - 1].labelWidth + ticks[i].labelWidth) / 2;
                if (gap < MIN_GAP) { ok = false; break; }
            }

            if (ok) return ticks;
        }

        return [];
    },

    // Formats a tick label adaptively based on interval length and range span.
    _formatRulerLabel(date, intervalMs, rangeStart, rangeEnd) {
        const h   = date.getHours();
        const m   = date.getMinutes();
        const s   = date.getSeconds();
        const D   = date.getDate();
        const Mon = date.getMonth();
        const Y   = date.getFullYear();

        const DAY_NAMES = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
        const MON_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

        const crossesYear = rangeStart.getFullYear() !== rangeEnd.getFullYear();
        const rangeDays   = (rangeEnd - rangeStart) / 86400000;

        if (intervalMs >= 86400000) {
            if (crossesYear) return `${MON_NAMES[Mon]} ${D} ’${String(Y).slice(2)}`;
            if (rangeDays > 14) return `${MON_NAMES[Mon]} ${D}`;
            return `${DAY_NAMES[date.getDay()]} ${D}`;
        }

        if (intervalMs >= 3600000) {
            // Midnight tick: show the day name instead of 00:00
            if (h === 0 && m === 0) {
                if (crossesYear) return `${MON_NAMES[Mon]} ${D}`;
                return `${DAY_NAMES[date.getDay()]} ${D}`;
            }
            return `${String(h).padStart(2, '0')}:00`;
        }

        if (intervalMs >= 60000) {
            return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
        }

        // 30 s
        return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
    },

    _setupResizeObserver(canvas) {
        if (this._resizeObserver) this._resizeObserver.disconnect();

        this._resizeObserver = new ResizeObserver(() => {
            if (this._currentBuckets) {
                this._drawBars(canvas, this._currentBuckets);
            }
        });
        this._resizeObserver.observe(canvas.parentElement);
    },

    formatDateLabel(date) {
        const month   = String(date.getMonth() + 1).padStart(2, '0');
        const day     = String(date.getDate()).padStart(2, '0');
        const hours   = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        return `${month}/${day} ${hours}:${minutes}`;
    },

    setupInteraction(canvas, buckets, barWidth, startDate, bucketSize, duration) {
        if (this.mouseDownHandler) {
            canvas.removeEventListener('mousedown', this.mouseDownHandler);
            canvas.removeEventListener('mousemove', this.mouseMoveHandler);
            canvas.removeEventListener('mouseup', this.mouseUpHandler);
            canvas.removeEventListener('mouseleave', this.leaveHandler);
        }

        this.mouseDownHandler = (e) => {
            if (e.button !== 0) return;
            e.preventDefault();
            const rect = canvas.getBoundingClientRect();
            const x = e.clientX - rect.left;
            this.isSelecting = true;
            this.selectionStart = x;
            this.selectionEnd = x;
            this.selectionStartTime = new Date(startDate.getTime() + (x / canvas.offsetWidth) * duration);
            this.hideTooltip();
        };

        this.mouseMoveHandler = (e) => {
            const rect = canvas.getBoundingClientRect();
            const x = e.clientX - rect.left;

            if (this.isSelecting) {
                this.selectionEnd = x;
                this.selectionEndTime = new Date(startDate.getTime() + (x / canvas.offsetWidth) * duration);
                this._redrawWithSelection(canvas, buckets, barWidth);
            } else {
                const barIndex = Math.floor(x / barWidth);
                if (barIndex >= 0 && barIndex < buckets.length) {
                    const bucketTime = new Date(startDate.getTime() + barIndex * bucketSize);
                    this.showTooltip(e.clientX, e.clientY, buckets[barIndex], bucketTime);
                } else {
                    this.hideTooltip();
                }
            }
        };

        this.mouseUpHandler = (e) => {
            if (!this.isSelecting) return;
            this.isSelecting = false;

            if (Math.abs(this.selectionEnd - this.selectionStart) > 5) {
                const startTime = new Date(Math.min(this.selectionStartTime.getTime(), this.selectionEndTime.getTime()));
                const endTime   = new Date(Math.max(this.selectionStartTime.getTime(), this.selectionEndTime.getTime()));
                this.applyTimeRangeSelection(startTime, endTime);
            }

            this.selectionStart = null;
            this.selectionEnd = null;
            this.selectionStartTime = null;
            this.selectionEndTime = null;
            this._drawBars(canvas, buckets);
        };

        this.leaveHandler = () => {
            this.hideTooltip();
        };

        canvas.addEventListener('mousedown', this.mouseDownHandler);
        canvas.addEventListener('mousemove', this.mouseMoveHandler);
        canvas.addEventListener('mouseup', this.mouseUpHandler);
        canvas.addEventListener('mouseleave', this.leaveHandler);
    },

    _redrawWithSelection(canvas, buckets, barWidth) {
        this._drawBars(canvas, buckets);

        if (this.isSelecting && this.selectionStart !== null && this.selectionEnd !== null) {
            const ctx = canvas.getContext('2d');
            const height = canvas.offsetHeight;
            const selStart = Math.min(this.selectionStart, this.selectionEnd);
            const selEnd   = Math.max(this.selectionStart, this.selectionEnd);
            const selWidth = selEnd - selStart;

            ctx.fillStyle = ThemeManager.getCSSVar('--timeline-selection');
            ctx.fillRect(selStart, 0, selWidth, height);

            ctx.strokeStyle = ThemeManager.getCSSVar('--timeline-selection-border');
            ctx.lineWidth = 1;
            ctx.strokeRect(selStart, 0, selWidth, height);
        }
    },

    showTooltip(x, y, count, time) {
        let tooltip = document.getElementById('timelineTooltip');
        if (!tooltip) {
            tooltip = document.createElement('div');
            tooltip.id = 'timelineTooltip';
            tooltip.className = 'timeline-tooltip';
            document.body.appendChild(tooltip);
        }

        const timeStr = this.formatDateLabel(time);
        tooltip.innerHTML = `<strong>${count.toLocaleString()}</strong> events &middot; ${timeStr}`;
        tooltip.style.display = 'block';
        tooltip.style.left = (x + 10) + 'px';
        tooltip.style.top  = (y - 30) + 'px';
    },

    hideTooltip() {
        const tooltip = document.getElementById('timelineTooltip');
        if (tooltip) tooltip.style.display = 'none';
    },

    hide() {
        const timelineSection = document.getElementById('timelineSection');
        if (timelineSection) timelineSection.style.display = 'none';
    },

    applyTimeRangeSelection(startTime, endTime) {
        const customStart = startTime.toISOString();
        const customEnd   = endTime.toISOString();

        if (window.TimePicker) {
            const absStart = document.getElementById('tpAbsStart');
            const absEnd   = document.getElementById('tpAbsEnd');
            if (absStart) absStart.value = TimePicker._toDatetimeLocal(customStart);
            if (absEnd)   absEnd.value   = TimePicker._toDatetimeLocal(customEnd);
            TimePicker._applyAndExecute({ type: 'custom', customStart, customEnd });
        }

        const alertTimeRange      = document.getElementById('alertTimeRange');
        const alertCustomStart    = document.getElementById('alertCustomStart');
        const alertCustomEnd      = document.getElementById('alertCustomEnd');
        const alertCustomTimeInputs = document.getElementById('alertCustomTimeInputs');

        if (alertTimeRange && alertCustomStart && alertCustomEnd && alertCustomTimeInputs) {
            const pad = n => String(n).padStart(2, '0');
            const fmt = d => `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
            alertTimeRange.value = 'custom';
            alertCustomTimeInputs.style.display = 'flex';
            alertCustomStart.value = fmt(startTime);
            alertCustomEnd.value   = fmt(endTime);
        }
    }
};

window.Timeline = Timeline;
