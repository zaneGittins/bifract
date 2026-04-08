// Timeline rendering - compact inline histogram
const Timeline = {
    isSelecting: false,
    selectionStart: null,
    selectionEnd: null,
    selectionStartTime: null,
    selectionEndTime: null,
    _resizeObserver: null,
    _lastRenderData: null,

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
        this.currentTimeRange = timeRange;
        this._lastRenderData = { buckets, timeRange, isHistogram: true };

        setTimeout(() => {
            if (!canvas) return;
            canvas.style.width = '100%';
            canvas.style.display = 'block';

            const actualWidth = canvas.offsetWidth;
            const actualHeight = canvas.offsetHeight;
            if (actualWidth === 0 || actualHeight === 0) return;

            this._drawBars(canvas, buckets);

            const start = new Date(timeRange.start);
            const end = new Date(timeRange.end);
            const duration = end - start;
            const bucketCount = buckets.length;
            const bucketSize = duration / bucketCount;
            const avg = (total / bucketCount).toFixed(1);
            const peak = Math.max(...buckets);
            const startLabel = this.formatDateLabel(start);
            const endLabel = this.formatDateLabel(end);

            if (timelineStats) {
                timelineStats.innerHTML =
                    `<span>${startLabel}</span>` +
                    `<span>Total: ${total} &middot; Avg: ${avg}/bucket &middot; Peak: ${peak}</span>` +
                    `<span>${endLabel}</span>`;
            }

            this.setupInteraction(canvas, buckets, canvas.offsetWidth / bucketCount, start, bucketSize, duration);
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

        this.currentTimeRange = timeRange;

        // Store data for resize redraws
        this._lastRenderData = { results, timeRange };

        setTimeout(() => {
            if (!canvas) return;

            canvas.style.width = '100%';
            canvas.style.display = 'block';

            const actualWidth = canvas.offsetWidth;
            const actualHeight = canvas.offsetHeight;
            if (actualWidth === 0 || actualHeight === 0) return;

            // Parse timestamps
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
                const bucketIndex = Math.floor((ts - start) / bucketSize);
                if (bucketIndex >= 0 && bucketIndex < bucketCount) {
                    buckets[bucketIndex]++;
                }
            });

            // Draw
            this._drawBars(canvas, buckets);

            // Stats below canvas
            const total = buckets.reduce((a, b) => a + b, 0);
            const avg = (total / bucketCount).toFixed(1);
            const peak = Math.max(...buckets);
            const startLabel = this.formatDateLabel(start);
            const endLabel = this.formatDateLabel(end);

            if (timelineStats) {
                timelineStats.innerHTML =
                    `<span>${startLabel}</span>` +
                    `<span>Total: ${total} &middot; Avg: ${avg}/bucket &middot; Peak: ${peak}</span>` +
                    `<span>${endLabel}</span>`;
            }

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
        const height = canvas.offsetHeight;
        const barWidth = width / buckets.length;
        const maxCount = Math.max(...buckets, 1);
        const barGap = buckets.length > 80 ? 0 : 1;
        const barRadius = barWidth > 4 ? 1.5 : 0;

        ctx.clearRect(0, 0, width, height);

        const barStart = ThemeManager.getCSSVar('--timeline-bar-start');
        const barEnd = ThemeManager.getCSSVar('--timeline-bar-end');

        buckets.forEach((count, i) => {
            if (count === 0) return;
            const barHeight = Math.max((count / maxCount) * (height - 4), 2);
            const x = i * barWidth;
            const y = height - barHeight;

            const gradient = ctx.createLinearGradient(0, y, 0, height);
            gradient.addColorStop(0, barStart);
            gradient.addColorStop(1, barEnd);
            ctx.fillStyle = gradient;

            if (barRadius > 0) {
                ctx.beginPath();
                ctx.roundRect(x, y, barWidth - barGap, barHeight, [barRadius, barRadius, 0, 0]);
                ctx.fill();
            } else {
                ctx.fillRect(x, y, barWidth - barGap, barHeight);
            }
        });

        // Store for selection redraws
        this._currentBuckets = buckets;
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
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hours = String(date.getHours()).padStart(2, '0');
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
                    const count = buckets[barIndex];
                    const bucketTime = new Date(startDate.getTime() + (barIndex * bucketSize));
                    this.showTooltip(e.clientX, e.clientY, count, bucketTime);
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
                const endTime = new Date(Math.max(this.selectionStartTime.getTime(), this.selectionEndTime.getTime()));
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
            const selEnd = Math.max(this.selectionStart, this.selectionEnd);
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
        tooltip.innerHTML = `<strong>${count}</strong> events &middot; ${timeStr}`;
        tooltip.style.display = 'block';
        tooltip.style.left = (x + 10) + 'px';
        tooltip.style.top = (y - 30) + 'px';
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
        const formatForInput = (date) => {
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            const hours = String(date.getHours()).padStart(2, '0');
            const minutes = String(date.getMinutes()).padStart(2, '0');
            return `${year}-${month}-${day} ${hours}:${minutes}`;
        };

        const timeRangeSelect = document.getElementById('timeRange');
        const customTimeInputs = document.getElementById('customTimeInputs');
        const customStart = document.getElementById('customStart');
        const customEnd = document.getElementById('customEnd');

        if (timeRangeSelect && customTimeInputs && customStart && customEnd) {
            timeRangeSelect.value = 'custom';
            customTimeInputs.style.display = 'flex';
            customStart.value = formatForInput(startTime);
            customEnd.value = formatForInput(endTime);

            if (window.QueryExecutor) {
                setTimeout(() => QueryExecutor.execute(), 100);
            }
        }

        const alertTimeRange = document.getElementById('alertTimeRange');
        const alertCustomStart = document.getElementById('alertCustomStart');
        const alertCustomEnd = document.getElementById('alertCustomEnd');
        const alertCustomTimeInputs = document.getElementById('alertCustomTimeInputs');

        if (alertTimeRange && alertCustomStart && alertCustomEnd && alertCustomTimeInputs) {
            alertTimeRange.value = 'custom';
            alertCustomTimeInputs.style.display = 'flex';
            alertCustomStart.value = formatForInput(startTime);
            alertCustomEnd.value = formatForInput(endTime);
        }
    }
};

window.Timeline = Timeline;
