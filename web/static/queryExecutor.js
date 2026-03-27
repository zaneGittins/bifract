// Query execution and results rendering
const QueryExecutor = {
    currentResults: [],
    currentTimeRange: null,
    sortColumn: null,
    sortDirection: null,
    columnWidths: {},
    columnOrder: null,
    isAggregated: false,
    chartType: '',
    chartConfig: {},
    currentChart: null, // Store current chart instance
    currentRequest: null, // Track current request for cancellation
    currentFractalId: null, // Track current fractal to validate responses
    hasLoadedShareLink: false, // Track if we've already loaded shared link on first fractal change
    pendingShareData: null, // Store shared link data waiting for fractal switch
    deferredShareLink: null, // Store share link data waiting for fractals to load
    deferredPollingInterval: null, // Interval for polling fractal availability
    isProcessingSharedQuery: false, // Flag to prevent clearing state during shared query processing

    // Default element configuration for main search view
    elementConfig: {
        queryInput: 'queryInput',
        resultsTable: 'resultsTable',
        errorDiv: 'error',
        sqlOutput: 'sqlOutput',
        resultsCount: 'resultsCount',
        executionTime: 'executionTime',
        pageSizeSelect: 'pageSizeSelect',
        paginationControls: 'paginationControls',
        prevPageBtn: 'prevPageBtn',
        nextPageBtn: 'nextPageBtn',
        pageInfo: 'pageInfo',
        timeRange: 'timeRange',
        customStart: 'customStart',
        customEnd: 'customEnd',
        customTimeInputs: 'customTimeInputs'
    },

    // Get DOM elements based on current configuration
    getElements(config = null) {
        const elementConfig = config || this.elementConfig;
        const elements = {};
        for (const [key, id] of Object.entries(elementConfig)) {
            elements[key] = document.getElementById(id);
        }
        return elements;
    },

    // Strip comment lines (lines starting with //) from query
    stripComments(query) {
        return query
            .split('\n')
            .filter(line => !line.trim().startsWith('//'))
            .join('\n')
            .trim();
    },

    // Load recent logs sample for initial fractal exploration
    async loadRecentLogsSample(config = null) {
        console.log('[QueryExecutor] loadRecentLogsSample called, current fractal:', window.FractalContext?.currentFractal?.name || 'none');
        const elements = this.getElements(config);

        // Cancel any previous request
        if (this.currentRequest) {
            console.log('[QueryExecutor] Cancelling previous request for recent logs');
            this.currentRequest.abort();
        }

        // Create new abort controller for this request
        this.currentRequest = new AbortController();

        // Store current fractal ID to validate response
        this.currentFractalId = window.FractalContext?.currentFractal?.id || null;

        // Show loading state in results table
        if (elements.resultsTable) {
            elements.resultsTable.innerHTML = '<div class="loading-spinner"><span class="spinner"></span></div>';
        }

        // Hide error div
        if (elements.errorDiv) elements.errorDiv.style.display = 'none';

        try {
            const queryStart = performance.now();

            // Use the safer HttpUtils for better error handling with cache-busting
            const data = await HttpUtils.safeFetch(`/api/v1/logs/recent?t=${Date.now()}`, {
                method: 'GET',
                credentials: 'include',
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                },
                signal: this.currentRequest.signal
            });

            const executionTime = Math.round(performance.now() - queryStart);

            // Validate that we're still on the same fractal (prevent race conditions)
            const currentFractalId = window.FractalContext?.currentFractal?.id || null;
            if (this.currentFractalId !== currentFractalId) {
                console.log('[QueryExecutor] Fractal changed during recent logs fetch, discarding results');
                return;
            }

            if (!data.success) {
                this.showError(data.error || 'Failed to load recent logs');
                if (elements.resultsTable) elements.resultsTable.innerHTML = '';
                return;
            }

            this.currentResults = data.results || [];
            this.fieldOrder = data.field_order || ["timestamp", "fields", "log_id"];
            this.isAggregated = false;
            this.chartType = '';
            this.chartConfig = {};

            // Reset sort state
            this.sortColumn = null;
            this.sortDirection = null;

            // Update results count for recent logs
            if (elements.resultsCount) {
                elements.resultsCount.textContent = `${this.currentResults.length} recent logs (last 24h)`;
            }

            // Display execution time
            if (elements.executionTime) {
                elements.executionTime.textContent = `(${executionTime}ms)`;
                elements.executionTime.style.display = 'inline';
            }

            // Render results immediately, fetch comments in background
            if (window.Pagination) {
                Pagination.setResults(this.currentResults);
                this.renderPage(Pagination.getCurrentPageResults());
            } else {
                this.renderResults(this.currentResults);
            }

            // Update field statistics sidebar
            if (window.FieldStats) FieldStats.refresh();

            // Use the server-provided time range (last 24h)
            this.currentTimeRange = {
                start: data.time_start || new Date(Date.now() - 86400000).toISOString(),
                end: data.time_end || new Date().toISOString()
            };

            // Defer timeline to next frame so the table paints first
            if (window.Timeline && data.histogram) {
                requestAnimationFrame(() => {
                    Timeline.renderFromHistogram(data.histogram, this.currentTimeRange);
                });
            }

            // Fetch commented log IDs in background and update row highlights
            if (window.Comments) {
                Comments.fetchCommentedLogIds().then(() => {
                    this.updateCommentHighlights();
                });
            }

            console.log(`[QueryExecutor] Loaded ${this.currentResults.length} recent logs (last 24h)`);

        } catch (error) {
            // Don't show error if request was cancelled (fractal switch)
            if (error.name === 'AbortError') {
                console.log('[QueryExecutor] Recent logs request cancelled due to fractal switch');
                return;
            }

            console.error('Failed to load recent logs:', error);
            this.showError('Failed to load recent logs: ' + error.message);
            if (elements.resultsTable) elements.resultsTable.innerHTML = '';
        } finally {
            // Clear the current request reference
            this.currentRequest = null;
        }
    },

    async execute(config = null) {
        const elements = this.getElements(config);

        if (!elements.queryInput) return;

        const rawQuery = elements.queryInput.value.trim();
        if (!rawQuery) return;

        // Strip comment lines (lines starting with //)
        const query = this.stripComments(rawQuery);
        if (!query) return;

        // Clear shared query state when user runs their own query (but not during shared query processing)
        if (!this.isProcessingSharedQuery) {
            this.clearSharedQueryState();
        }

        // Cancel any previous request
        if (this.currentRequest) {
            console.log('[QueryExecutor] Cancelling previous request');
            this.currentRequest.abort();
        }

        // Create new abort controller for this request
        this.currentRequest = new AbortController();

        // Store current fractal ID to validate response
        this.currentFractalId = window.FractalContext?.currentFractal?.id || null;

        // Add to recent queries
        if (window.RecentQueries) {
            RecentQueries.add(query);
        }

        // Get time range
        this.currentTimeRange = this.getTimeRange();

        // Hide previous results and show loading
        if (elements.errorDiv) elements.errorDiv.style.display = 'none';

        // Reset chart/graph container so loading spinner is visible
        const chartContainer = document.getElementById('chartContainer');
        if (chartContainer) chartContainer.style.display = 'none';
        const fieldsDrawerReset = document.getElementById('fieldStatsDrawer');
        if (fieldsDrawerReset) fieldsDrawerReset.style.display = '';
        if (elements.resultsTable) {
            elements.resultsTable.style.display = 'block';
            elements.resultsTable.innerHTML = '<div class="loading-spinner"><span class="spinner"></span><button class="cancel-query-btn" onclick="QueryExecutor.cancelQuery()">Cancel</button></div>';
        }

        try {
            // Get currently selected fractal for context
            let requestBody = {
                query: query,
                start: this.currentTimeRange.start,
                end: this.currentTimeRange.end
            };

            // Include fractal context if FractalContext is available (skip for prisms - server uses session)
            if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
                requestBody.fractal_id = window.FractalContext.currentFractal.id;
            }

            // Use the safer HttpUtils for better error handling
            const data = await HttpUtils.safeFetch('/api/v1/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody),
                signal: this.currentRequest.signal
            });

            if (data.sql && elements.sqlOutput) {
                elements.sqlOutput.innerHTML = this.highlightSQL(data.sql);
                const sqlPreview = document.querySelector('.sql-preview');
                if (sqlPreview && window.UserPrefs && UserPrefs.showSQL()) {
                    sqlPreview.style.display = 'block';
                }
            }

            // Validate that we're still on the same fractal (prevent race conditions)
            const currentFractalId = window.FractalContext?.currentFractal?.id || null;
            if (this.currentFractalId !== currentFractalId) {
                console.log('[QueryExecutor] Fractal changed during query, discarding results');
                return;
            }

            if (!data.success) {
                this.showError(data.error || 'Query failed');
                if (elements.resultsTable) elements.resultsTable.innerHTML = '';
                return;
            }

            this.currentResults = data.results || [];
            this.fieldOrder = data.field_order || null;
            this.isAggregated = data.is_aggregated || false;
            this.chartType = data.chart_type || '';
            this.chartConfig = data.chart_config || {};

            // Debug: (removed debug logging)

            // Reset sort state for new query
            this.sortColumn = null;
            this.sortDirection = null;

            // Show results count with limit information and styling
            if (elements.resultsCount) {
                const resultsLength = this.currentResults.length;

                if (data.limit_hit) {
                    // Show clear message when hitting limits
                    const countSpan = `<span style="color: #e74c3c; font-weight: 500;">${resultsLength}</span>`;

                    switch (data.limit_hit) {
                        case 'bloom':
                            elements.resultsCount.innerHTML = `${countSpan} results (limit reached)`;
                            break;
                        case 'search':
                            elements.resultsCount.innerHTML = `${countSpan} results (limit reached)`;
                            break;
                        case 'truncated':
                            elements.resultsCount.innerHTML = `${countSpan} results (truncated due to large response)`;
                            break;
                        default:
                            elements.resultsCount.textContent = `${resultsLength} results`;
                    }
                } else {
                    // Normal case - show total results
                    elements.resultsCount.textContent = `${resultsLength.toLocaleString()} results`;
                }
            }

            // Display execution time
            if (elements.executionTime && data.execution_ms !== undefined) {
                elements.executionTime.textContent = `(${data.execution_ms}ms)`;
                elements.executionTime.style.display = 'inline';
            }

            // Show export CSV button when results are available
            const exportBtn = document.getElementById('exportCsvBtn');
            if (exportBtn && this.currentResults && this.currentResults.length > 0) {
                exportBtn.style.display = 'inline-block';
            }

            // Render results immediately, fetch comments in background
            const paginationEl = document.getElementById('paginationControls');
            const pageSizeEl = document.getElementById('pageSizeSelect');
            if (this.chartType && this.chartType !== '') {
                if (paginationEl) paginationEl.style.display = 'none';
                if (pageSizeEl) pageSizeEl.style.display = 'none';
                if (elements.resultsTable) elements.resultsTable.style.display = 'none';
                this.renderResults(this.currentResults);
            } else {
                if (paginationEl) paginationEl.style.display = '';
                if (pageSizeEl) pageSizeEl.style.display = '';
                if (window.Pagination) {
                    Pagination.setResults(this.currentResults);
                    this.renderPage(Pagination.getCurrentPageResults());
                } else {
                    this.renderResults(this.currentResults);
                }
            }

            // Update field statistics sidebar
            if (window.FieldStats) FieldStats.refresh();

            // Defer timeline to next frame so the table paints first
            const shouldShowTimeline = !this.fieldOrder || this.fieldOrder.includes('timestamp');
            if (window.Timeline) {
                if (shouldShowTimeline) {
                    requestAnimationFrame(() => {
                        Timeline.render(this.currentResults, this.currentTimeRange);
                    });
                } else {
                    Timeline.hide();
                }
            }

            // Fetch commented log IDs in background and update row highlights
            if (window.Comments) {
                Comments.fetchCommentedLogIds().then(() => {
                    this.updateCommentHighlights();
                });
            }

        } catch (error) {
            // Don't show error if request was cancelled (fractal switch)
            if (error.name === 'AbortError') {
                console.log('[QueryExecutor] Request cancelled due to fractal switch');
                return;
            }

            this.showError(error.message);
            if (elements.resultsTable) elements.resultsTable.innerHTML = '';
        } finally {
            // Clear the current request reference
            this.currentRequest = null;
        }
    },

    cancelQuery() {
        if (this.currentRequest) {
            this.currentRequest.abort();
            this.currentRequest = null;
            const elements = this.getElements();
            if (elements.resultsTable) elements.resultsTable.innerHTML = '';
            if (elements.resultsCount) elements.resultsCount.textContent = 'Query cancelled';
            if (window.Toast) Toast.show('Query cancelled', 'info');
        }
    },

    getTimeRange() {
        const timeRangeSelect = document.getElementById('timeRange');
        const customStart = document.getElementById('customStart');
        const customEnd = document.getElementById('customEnd');

        if (!timeRangeSelect) {
            return {
                start: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
                end: new Date().toISOString()
            };
        }

        const value = timeRangeSelect.value;
        const now = new Date();
        let start, end = now;

        switch (value) {
            case '5m':
                start = new Date(now - 5 * 60 * 1000);
                break;
            case '15m':
                start = new Date(now - 15 * 60 * 1000);
                break;
            case '1h':
                start = new Date(now - 60 * 60 * 1000);
                break;
            case '24h':
                start = new Date(now - 24 * 60 * 60 * 1000);
                break;
            case '7d':
                start = new Date(now - 7 * 24 * 60 * 60 * 1000);
                break;
            case '30d':
                start = new Date(now - 30 * 24 * 60 * 60 * 1000);
                break;
            case 'all':
                start = new Date('2000-01-01T00:00:00Z');
                break;
            case 'custom':
                if (customStart && customEnd && customStart.value && customEnd.value) {
                    const timeRange = {
                        start: new Date(customStart.value).toISOString(),
                        end: new Date(customEnd.value).toISOString()
                    };
                    // Store custom time range in localStorage
                    this.saveTimeRangeToStorage('custom', timeRange);
                    return timeRange;
                }
                start = new Date(now - 24 * 60 * 60 * 1000);
                break;
            default:
                start = new Date(now - 24 * 60 * 60 * 1000);
        }

        const timeRange = {
            start: start.toISOString(),
            end: end.toISOString()
        };

        // Store time range in localStorage
        this.saveTimeRangeToStorage(value, timeRange);

        return timeRange;
    },

    _getTimeRangeStorageKey(suffix) {
        // FractalContext is the canonical source (updated by both listing and selector paths)
        const fractalId = window.FractalContext?.currentFractal?.id
            || window.FractalSelector?.getCurrentFractalId?.()
            || 'default';
        return `bifract_${suffix}_${fractalId}`;
    },

    saveTimeRangeToStorage(rangeType, timeRange) {
        try {
            localStorage.setItem(this._getTimeRangeStorageKey('time_range_type'), rangeType);
            if (rangeType === 'custom') {
                localStorage.setItem(this._getTimeRangeStorageKey('custom_time_range'), JSON.stringify(timeRange));
            }
        } catch (e) {
            console.error('Failed to save time range to localStorage:', e);
        }
    },

    restoreTimeRangeFromStorage() {
        try {
            const timeRangeSelect = document.getElementById('timeRange');
            const customStart = document.getElementById('customStart');
            const customEnd = document.getElementById('customEnd');
            const customTimeInputs = document.getElementById('customTimeInputs');
            if (!timeRangeSelect) return;

            const savedRangeType = localStorage.getItem(this._getTimeRangeStorageKey('time_range_type'));

            if (savedRangeType) {
                timeRangeSelect.value = savedRangeType;

                if (savedRangeType === 'custom') {
                    const savedCustomRange = localStorage.getItem(this._getTimeRangeStorageKey('custom_time_range'));
                    if (savedCustomRange) {
                        const range = JSON.parse(savedCustomRange);
                        if (customStart && customEnd) {
                            customStart.value = new Date(range.start).toISOString().slice(0, 16).replace('T', ' ');
                            customEnd.value = new Date(range.end).toISOString().slice(0, 16).replace('T', ' ');
                        }
                    }
                    if (customTimeInputs) {
                        customTimeInputs.style.display = 'flex';
                    }
                } else {
                    if (customTimeInputs) {
                        customTimeInputs.style.display = 'none';
                    }
                }
            } else {
                // No saved range for this fractal -- reset to default
                // to prevent the previous fractal's range from leaking
                timeRangeSelect.value = '24h';
                if (customTimeInputs) {
                    customTimeInputs.style.display = 'none';
                }
            }
        } catch (e) {
            console.error('Failed to restore time range from localStorage:', e);
        }
    },

    renderPage(pageResults) {
        this.renderResults(pageResults);
    },

    renderResults(results) {
        const resultsTable = document.getElementById('resultsTable');
        const chartContainer = document.getElementById('chartContainer');
        if (!resultsTable) return;

        if (!results || results.length === 0) {
            resultsTable.innerHTML = '<div class="no-results">No results found</div>';
            if (chartContainer) chartContainer.style.display = 'none';
            return;
        }

        // Check if we should render as a chart instead of a table
        if (this.chartType && this.chartType !== '') {
            this.renderChart(results);
            return;
        }

        // Hide chart container and show table for normal results
        if (chartContainer) chartContainer.style.display = 'none';
        resultsTable.style.display = 'block';

        // Use field order from backend if available (to overcome ClickHouse JSON alphabetization)
        // Otherwise fall back to Object.keys() order
        let fields = [];
        if (this.fieldOrder && this.fieldOrder.length > 0) {
            // Use the field order provided by the backend
            fields = this.fieldOrder.filter(f => f !== 'fractal_id');
            console.log('Using backend field order:', fields);
        } else if (results.length > 0) {
            // Fall back to extracting from first result
            const firstResult = results[0];
            console.log('First result keys order:', Object.keys(firstResult));
            for (const key of Object.keys(firstResult)) {
                if (key !== 'raw_log' && key !== '_all_fields' && key !== 'fractal_id') {
                    fields.push(key);
                }
            }
            console.log('Fields array after filtering:', fields);
        }

        // Apply custom column order if available
        if (this.columnOrder && this.columnOrder.length > 0) {
            const orderedFields = [];
            this.columnOrder.forEach(colField => {
                if (fields.includes(colField)) {
                    orderedFields.push(colField);
                }
            });
            // Add any new fields that weren't in the saved order
            fields.forEach(field => {
                if (!orderedFields.includes(field)) {
                    orderedFields.push(field);
                }
            });
            fields = orderedFields;
        }

        // Build table with sortable headers
        let html = '<table class="results-table"><thead><tr>';
        fields.forEach(field => {
            const displayName = field;
            const sortIcon = this.sortColumn === field
                ? (this.sortDirection === 'asc' ? ' ▲' : ' ▼')
                : '';
            const width = this.columnWidths[field] ? `style="width: ${this.columnWidths[field]}px"` : '';
            html += `<th class="sortable" data-field="${Utils.escapeAttr(field)}" ${width} draggable="true">${Utils.escapeHtml(displayName)}${sortIcon}<div class="column-resizer"></div></th>`;
        });
        html += '</tr></thead><tbody>';

        results.forEach((result, index) => {
            // Check if this log has comments
            const hasComments = window.Comments && Comments.hasComments(result);
            const rowClass = hasComments ? 'result-row has-comments' : 'result-row';

            html += '<tr class="' + rowClass + '" data-index="' + index + '">';
            fields.forEach(field => {
                let value = result[field];
                let cellClass = field === 'timestamp' ? 'timestamp-cell' : '';

                if (typeof value === 'object' && value !== null) {
                    const jsonStr = JSON.stringify(value);
                    value = `<span class="json-value json-unhighlighted">${Utils.escapeHtml(jsonStr)}</span>`;
                    cellClass += ' json-cell';
                } else if (value === undefined || value === null) {
                    value = '-';
                    cellClass += ' null-cell';
                } else {
                    // Escape HTML for safety
                    value = Utils.escapeHtml(String(value));
                }

                html += `<td class="${cellClass}">${value}</td>`;
            });
            html += '</tr>';
        });

        html += '</tbody></table>';
        resultsTable.innerHTML = html;

        // Lazy-highlight JSON cells as they scroll into view
        this.lazyHighlightJSON(resultsTable);

        // Event delegation for row clicks
        const tbody = resultsTable.querySelector('tbody');
        if (tbody) {
            tbody.addEventListener('click', (e) => {
                if (e.target.classList.contains('column-resizer')) return;
                const row = e.target.closest('.result-row');
                if (!row) return;

                const index = parseInt(row.dataset.index);
                const logData = results[index];

                let detailData = logData;
                if (logData._all_fields && typeof logData._all_fields === 'object') {
                    detailData = {
                        ...logData._all_fields,
                        timestamp: logData.timestamp,
                        log_id: logData.log_id
                    };
                }

                if (window.LogDetail) {
                    LogDetail.show(detailData, this.isAggregated);
                }
            });
        }

        // Event delegation for sortable header clicks
        const thead = resultsTable.querySelector('thead');
        if (thead) {
            thead.addEventListener('click', (e) => {
                const header = e.target.closest('th.sortable');
                if (!header) return;
                if (e.target.classList.contains('column-resizer') || header.classList.contains('dragging')) return;
                this.sortByColumn(header.dataset.field);
            });
        }

        // Add column resizing handlers
        this.setupColumnResizing();

        // Add column reordering handlers
        this.setupColumnReordering();

        // Add relative time tooltips
        if (window.TimeBar) {
            TimeBar.addRelativeTimeTooltips();
        }
    },

    // Update comment highlighting on already-rendered rows
    updateCommentHighlights() {
        if (!window.Comments) return;
        const resultsTable = document.getElementById('resultsTable');
        if (!resultsTable) return;
        const rows = resultsTable.querySelectorAll('.result-row');
        const pageResults = window.Pagination ? Pagination.getCurrentPageResults() : this.currentResults;
        rows.forEach(row => {
            const index = parseInt(row.dataset.index);
            const logData = pageResults[index];
            if (!logData) return;
            if (Comments.hasComments(logData)) {
                row.classList.add('has-comments');
            }
        });
    },

    setupColumnResizing() {
        const resizers = document.querySelectorAll('.column-resizer');
        resizers.forEach(resizer => {
            resizer.addEventListener('mousedown', (e) => {
                e.stopPropagation();
                const th = resizer.parentElement;
                const field = th.dataset.field;
                const startX = e.pageX;
                const startWidth = th.offsetWidth;

                th.classList.add('resizing');

                const onMouseMove = (e) => {
                    const newWidth = startWidth + (e.pageX - startX);
                    if (newWidth > 50) { // Minimum column width
                        th.style.width = newWidth + 'px';
                        this.columnWidths[field] = newWidth;
                    }
                };

                const onMouseUp = () => {
                    th.classList.remove('resizing');
                    document.removeEventListener('mousemove', onMouseMove);
                    document.removeEventListener('mouseup', onMouseUp);
                };

                document.addEventListener('mousemove', onMouseMove);
                document.addEventListener('mouseup', onMouseUp);
            });
        });
    },

    setupColumnReordering() {
        const headers = document.querySelectorAll('.results-table th.sortable');
        let draggedElement = null;
        let draggedField = null;

        headers.forEach(header => {
            header.addEventListener('dragstart', (e) => {
                // Only allow drag from the header text area, not the resizer
                if (e.target.classList.contains('column-resizer')) {
                    e.preventDefault();
                    return;
                }

                draggedElement = header;
                draggedField = header.dataset.field;
                header.classList.add('dragging');
                e.dataTransfer.effectAllowed = 'move';
            });

            header.addEventListener('dragover', (e) => {
                if (e.target === draggedElement) return;
                e.preventDefault();
                e.dataTransfer.dropEffect = 'move';

                const targetHeader = e.target.closest('th');
                if (targetHeader && targetHeader !== draggedElement) {
                    targetHeader.classList.add('drag-over');
                }
            });

            header.addEventListener('dragleave', (e) => {
                const targetHeader = e.target.closest('th');
                if (targetHeader) {
                    targetHeader.classList.remove('drag-over');
                }
            });

            header.addEventListener('drop', (e) => {
                e.preventDefault();
                const targetHeader = e.target.closest('th');
                if (!targetHeader || targetHeader === draggedElement) return;

                targetHeader.classList.remove('drag-over');

                const targetField = targetHeader.dataset.field;

                // Get current field order
                const thead = targetHeader.parentElement;
                const allHeaders = Array.from(thead.querySelectorAll('th.sortable'));
                let fields = allHeaders.map(h => h.dataset.field);

                // Remove dragged field and insert at target position
                const draggedIndex = fields.indexOf(draggedField);
                const targetIndex = fields.indexOf(targetField);

                fields.splice(draggedIndex, 1);
                fields.splice(targetIndex, 0, draggedField);

                // Save new order
                this.columnOrder = fields;

                // Re-render with new order
                if (window.Pagination) {
                    this.renderPage(Pagination.getCurrentPageResults());
                } else {
                    this.renderResults(this.currentResults);
                }
            });

            header.addEventListener('dragend', (e) => {
                header.classList.remove('dragging');
                // Remove all drag-over classes
                headers.forEach(h => h.classList.remove('drag-over'));
            });
        });
    },

    sortByColumn(field) {
        // Determine sort direction
        if (this.sortColumn === field) {
            // Toggle direction
            this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            // New column - check if it's numeric or a timestamp
            const firstValue = this.currentResults[0]?.[field];
            const isNumeric = !isNaN(parseFloat(firstValue)) && isFinite(firstValue);
            const isTimestamp = !isNaN(Date.parse(firstValue)) && /\d{4}-\d{2}-\d{2}/.test(firstValue);

            // Default to desc for numeric/timestamp fields, asc for text
            this.sortDirection = (isNumeric || isTimestamp) ? 'desc' : 'asc';
            this.sortColumn = field;
        }

        // Sort the results
        const sorted = [...this.currentResults].sort((a, b) => {
            let aVal = a[field];
            let bVal = b[field];

            // Handle undefined/null
            if (aVal === undefined || aVal === null) return 1;
            if (bVal === undefined || bVal === null) return -1;

            // Try timestamp comparison (e.g. "2026-03-07 12:34:56")
            const aDate = Date.parse(aVal);
            const bDate = Date.parse(bVal);
            if (!isNaN(aDate) && !isNaN(bDate) && /\d{4}-\d{2}-\d{2}/.test(aVal)) {
                return this.sortDirection === 'asc' ? aDate - bDate : bDate - aDate;
            }

            // Try numeric comparison
            const aNum = parseFloat(aVal);
            const bNum = parseFloat(bVal);

            if (!isNaN(aNum) && !isNaN(bNum) && isFinite(aVal) && isFinite(bVal)) {
                return this.sortDirection === 'asc' ? aNum - bNum : bNum - aNum;
            } else {
                // String comparison
                const aStr = String(aVal).toLowerCase();
                const bStr = String(bVal).toLowerCase();

                if (this.sortDirection === 'asc') {
                    return aStr < bStr ? -1 : aStr > bStr ? 1 : 0;
                } else {
                    return bStr < aStr ? -1 : bStr > aStr ? 1 : 0;
                }
            }
        });

        // Update pagination with sorted results
        if (window.Pagination) {
            Pagination.setResults(sorted);
            this.renderPage(Pagination.getCurrentPageResults());
        } else {
            this.renderResults(sorted);
        }
    },

    showError(message) {
        // Use toast notifications if available, fallback to error div
        if (window.Toast) {
            Toast.error('Query Error', message);
        } else {
            const errorDiv = document.getElementById('error');
            if (errorDiv) {
                errorDiv.textContent = message;
                errorDiv.style.display = 'block';
            }
        }

        // Hide export CSV button when there's an error
        const exportBtn = document.getElementById('exportCsvBtn');
        if (exportBtn) {
            exportBtn.style.display = 'none';
        }
    },

    highlightSQL(sql) {
        if (!sql) return '';

        const keywords = [
            'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'ORDER BY', 'GROUP BY',
            'LIMIT', 'AS', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'DISTINCT',
            'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'ON', 'IN', 'LIKE',
            'BETWEEN', 'IS', 'NULL', 'ASC', 'DESC'
        ];

        const functions = [
            'formatDateTime', 'toString', 'toDate', 'position', 'match',
            'positionCaseInsensitive', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN',
            'groupBy', 'table', 'sort', 'limit'
        ];

        let highlighted = sql;

        // Escape HTML
        highlighted = highlighted.replace(/&/g, '&amp;')
                                 .replace(/</g, '&lt;')
                                 .replace(/>/g, '&gt;');

        // Highlight strings
        highlighted = highlighted.replace(/('(?:[^'\\]|\\.)*')/g, '<span class="sql-string">$1</span>');

        // Highlight numbers
        highlighted = highlighted.replace(/\b(\d+)\b/g, '<span class="sql-number">$1</span>');

        // Highlight keywords
        keywords.forEach(keyword => {
            const regex = new RegExp(`\\b(${keyword})\\b`, 'gi');
            highlighted = highlighted.replace(regex, '<span class="sql-keyword">$1</span>');
        });

        // Highlight functions
        functions.forEach(func => {
            const regex = new RegExp(`\\b(${func})\\s*\\(`, 'gi');
            highlighted = highlighted.replace(regex, '<span class="sql-function">$1</span>(');
        });

        return highlighted;
    },

    // ============================
    // Alert Editor Shared Methods
    // ============================

    // Execute query for alert editor with specific element configuration
    async executeForAlertEditor(query, timeRange, elementsConfig) {
        const elements = this.getElements(elementsConfig);

        if (!elements.resultsTable) return;

        // Show loading
        elements.resultsTable.innerHTML = '<div class="loading-spinner"><span class="spinner"></span></div>';
        if (elements.resultsCount) elements.resultsCount.textContent = 'Testing...';

        try {
            // Get currently selected fractal for context
            let requestBody = {
                query: query,
                start: timeRange.start,
                end: timeRange.end
            };

            // Include fractal context if FractalContext is available (skip for prisms - server uses session)
            if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
                requestBody.fractal_id = window.FractalContext.currentFractal.id;
            }

            // Use the safer HttpUtils for better error handling
            const data = await HttpUtils.safeFetch('/api/v1/query', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestBody)
            });

            // Display SQL output
            if (data.sql && elements.sqlOutput) {
                elements.sqlOutput.innerHTML = this.highlightSQL(data.sql);
                const sqlPreview = document.querySelector('.sql-preview');
                if (sqlPreview && window.UserPrefs && UserPrefs.showSQL()) {
                    sqlPreview.style.display = 'block';
                }
            }

            if (!data.success) {
                throw new Error(data.error || 'Query failed');
            }

            // Store results
            const results = data.results || [];
            const fieldOrder = data.field_order || null;
            const isAggregated = data.is_aggregated || false;

            if (elements.resultsCount) {
                elements.resultsCount.textContent = `${results.length} results`;
            }

            return {
                results,
                fieldOrder,
                isAggregated,
                success: true
            };

        } catch (error) {
            if (elements.resultsTable) {
                elements.resultsTable.innerHTML = `<div class="query-error"><p>Query Error: ${Utils.escapeHtml(error.message)}</p></div>`;
            }
            if (elements.resultsCount) {
                elements.resultsCount.textContent = 'Error';
            }

            return {
                success: false,
                error: error.message
            };
        }
    },

    // Render results for alert editor (reuses main rendering logic)
    renderForAlertEditor(results, fieldOrder, elementsConfig) {
        const elements = this.getElements(elementsConfig);

        if (!elements.resultsTable || !results || results.length === 0) {
            if (elements.resultsTable) {
                elements.resultsTable.innerHTML = '<div class="no-results">No results found</div>';
            }
            return;
        }

        // Store field order for rendering
        const originalFieldOrder = this.fieldOrder;
        this.fieldOrder = fieldOrder;

        // Use existing renderResults logic but with different target element
        const originalResultsTable = document.getElementById(this.elementConfig.resultsTable);
        const alertResultsTable = elements.resultsTable;

        // Temporarily replace the target element ID
        const originalId = alertResultsTable.id;
        const tempId = this.elementConfig.resultsTable + '_temp';
        alertResultsTable.id = tempId;

        // Store original element config and temporarily use alert config
        const originalConfig = this.elementConfig;
        this.elementConfig = elementsConfig;

        // Call existing renderResults method
        this.renderResults(results);

        // Restore original configuration
        this.elementConfig = originalConfig;
        alertResultsTable.id = originalId;
        this.fieldOrder = originalFieldOrder;
    },

    // Simple method to render results to any target element
    renderResultsToElement(results, targetElement, fieldOrder = null, options = {}) {
        if (!targetElement || !results || results.length === 0) {
            if (targetElement) {
                targetElement.innerHTML = '<div class="no-results">No results found</div>';
            }
            return;
        }

        // Use field order from backend if available
        let fields = [];
        if (fieldOrder && fieldOrder.length > 0) {
            fields = fieldOrder.filter(f => f !== 'fractal_id');
        } else if (results.length > 0) {
            const firstResult = results[0];
            for (const key of Object.keys(firstResult)) {
                if (key !== 'raw_log' && key !== '_all_fields' && key !== 'fractal_id') {
                    fields.push(key);
                }
            }
        }

        // Build table with sortable headers
        let html = '<table class="results-table"><thead><tr>';
        fields.forEach(field => {
            html += `<th class="sortable" data-field="${Utils.escapeAttr(field)}" draggable="true">${Utils.escapeHtml(field)}<div class="column-resizer"></div></th>`;
        });
        html += '</tr></thead><tbody>';

        results.forEach((result, index) => {
            const hasComments = window.Comments && Comments.hasComments(result);
            const rowClass = hasComments ? 'result-row has-comments' : 'result-row';

            html += '<tr class="' + rowClass + '" data-index="' + index + '">';
            fields.forEach(field => {
                let value = result[field];
                let cellClass = field === 'timestamp' ? 'timestamp-cell' : '';

                if (typeof value === 'object' && value !== null) {
                    const jsonStr = JSON.stringify(value);
                    value = `<span class="json-value json-unhighlighted">${Utils.escapeHtml(jsonStr)}</span>`;
                    cellClass += ' json-cell';
                } else if (value === undefined || value === null) {
                    value = '-';
                    cellClass += ' null-cell';
                } else {
                    value = Utils.escapeHtml(String(value));
                }

                html += `<td class="${cellClass}">${value}</td>`;
            });
            html += '</tr>';
        });

        html += '</tbody></table>';
        targetElement.innerHTML = html;

        // Lazy-highlight JSON cells
        this.lazyHighlightJSON(targetElement);

        // Event delegation for row clicks (only if not disabled)
        if (!options.disableDetailView) {
            const tbody = targetElement.querySelector('tbody');
            if (tbody) {
                tbody.addEventListener('click', (e) => {
                    if (e.target.classList.contains('column-resizer')) return;
                    const row = e.target.closest('.result-row');
                    if (!row) return;

                    const index = parseInt(row.dataset.index);
                    const logData = results[index];

                    if (window.LogDetail) {
                        let detailData = logData;
                        if (logData._all_fields && typeof logData._all_fields === 'object') {
                            detailData = {
                                ...logData._all_fields,
                                timestamp: logData.timestamp,
                                log_id: logData.log_id
                            };
                        }
                        LogDetail.show(detailData, options.isAggregated || false);
                    }
                });
            }
        }

        return targetElement;
    },

    highlightJSON(json) {
        if (!json) return '';

        // Escape HTML first
        let highlighted = json.replace(/&/g, '&amp;')
                             .replace(/</g, '&lt;')
                             .replace(/>/g, '&gt;');

        // Highlight strings (values in quotes)
        highlighted = highlighted.replace(/("(?:[^"\\]|\\.)*")\s*:/g, '<span class="json-key">$1</span>:');

        // Highlight string values
        highlighted = highlighted.replace(/:\s*("(?:[^"\\]|\\.)*")/g, ': <span class="json-string">$1</span>');

        // Highlight numbers
        highlighted = highlighted.replace(/:\s*(-?\d+\.?\d*)/g, ': <span class="json-number">$1</span>');

        // Highlight booleans and null
        highlighted = highlighted.replace(/:\s*(true|false|null)/g, ': <span class="json-boolean">$1</span>');

        // Highlight brackets and braces
        highlighted = highlighted.replace(/([{}\[\]])/g, '<span class="json-bracket">$1</span>');

        return highlighted;
    },

    // Lazily highlight JSON cells using IntersectionObserver
    lazyHighlightJSON(container) {
        const cells = container.querySelectorAll('.json-unhighlighted');
        if (cells.length === 0) return;

        // Disconnect any previous observer
        if (this._jsonObserver) {
            this._jsonObserver.disconnect();
        }

        this._jsonObserver = new IntersectionObserver((entries) => {
            for (const entry of entries) {
                if (entry.isIntersecting) {
                    const el = entry.target;
                    el.innerHTML = this.highlightJSON(el.textContent);
                    el.classList.remove('json-unhighlighted');
                    this._jsonObserver.unobserve(el);
                }
            }
        }, { rootMargin: '200px' });

        cells.forEach(cell => this._jsonObserver.observe(cell));
    },

    // ============================
    // Fractal Context Management
    // ============================

    // Re-execute current query when fractal context changes
    onFractalChange(retryCount = 0) {
        // Add small delay to ensure fractal context is fully updated
        setTimeout(() => {
            // Restore the per-fractal time range selection
            this.restoreTimeRangeFromStorage();

            const elements = this.getElements();
            console.log('[QueryExecutor] onFractalChange - elements check:', {
                queryInput: !!elements.queryInput,
                resultsTable: !!elements.resultsTable,
                currentView: window.App?.currentView || 'unknown'
            });

            // If we have pending share data from fractal switch, process it now
            if (this.pendingShareData) {
                console.log('[Share] Processing pending share data after fractal switch');
                this.loadShareDataIntoUI(this.pendingShareData);
                this.pendingShareData = null;
                return;
            }

            // Check for deferred share links now that fractals are loaded
            if (this.deferredShareLink) {
                console.log('[Share] Checking if fractals are available for deferred processing');

                let hasFractals = false;
                let selectorCount = 0;
                let listingCount = 0;

                try {
                    // Safe checking with comprehensive null protection
                    if (window.FractalSelector &&
                        window.FractalSelector.availableFractals &&
                        Array.isArray(window.FractalSelector.availableFractals)) {
                        selectorCount = window.FractalSelector.availableFractals.length;
                    }

                    if (window.FractalListing &&
                        window.FractalListing.fractals &&
                        Array.isArray(window.FractalListing.fractals)) {
                        listingCount = window.FractalListing.fractals.length;
                    }

                    hasFractals = selectorCount > 0 || listingCount > 0;
                    console.log('[Share] Fractal availability check - Selector:', selectorCount, 'Listing:', listingCount, 'HasFractals:', hasFractals);
                } catch (checkError) {
                    console.warn('[Share] Error checking deferred fractals:', checkError);
                    hasFractals = false;
                }

                if (hasFractals) {
                    console.log('[Share] Processing deferred share link now that fractals are loaded');
                    this.processDeferredShareLink();
                    return;
                } else {
                    console.log('[Share] Fractals still not available, keeping deferred');
                }
            }

            // On first fractal change, check for shared links
            if (!this.hasLoadedShareLink) {
                this.hasLoadedShareLink = true;
                console.log('[QueryExecutor] First fractal change, checking for share link');
                const loadedShareLink = this.loadFromShareLink();
                console.log('[QueryExecutor] Share link check result:', loadedShareLink);
                if (loadedShareLink) {
                    console.log('[QueryExecutor] Share link was processed, stopping here');
                    return; // Share link was processed, stop here
                }
                console.log('[QueryExecutor] No share link found, continuing with default behavior');
            }

            // Only attempt to execute or load logs if we're in the search view
            const searchView = document.getElementById('searchView');
            if (!searchView || searchView.style.display === 'none') {
                console.log('[QueryExecutor] Not in search view, skipping query execution');
                return;
            }

            // Check if we're in the search view and elements are available
            if (!elements.queryInput || !elements.resultsTable) {
                if (retryCount < 5) { // Max 5 retries (1 second total)
                    console.log(`[QueryExecutor] Search view elements not available yet, will retry (${retryCount + 1}/5)`);
                    // Retry after view has had time to initialize
                    setTimeout(() => this.onFractalChange(retryCount + 1), 200);
                    return;
                } else {
                    console.log('[QueryExecutor] Search view elements still not available after retries, giving up');
                    return;
                }
            }

            // If we have a current query, re-execute it for the new fractal
            if (elements.queryInput && elements.queryInput.value.trim()) {
                console.log('[QueryExecutor] Fractal changed, re-executing current query');
                this.execute();
            } else if (!this.isProcessingSharedQuery) {
                // If no query is present and no shared query is loading, load recent logs sample
                console.log('[QueryExecutor] Fractal changed, no query present, loading recent logs sample');
                this.loadRecentLogsSample();
            } else {
                console.log('[QueryExecutor] Skipping recent logs, shared query is being processed');
            }
        }, 100);
    },

    // Get current fractal context information for display
    getCurrentFractalContext() {
        if (window.FractalContext && window.FractalContext.currentFractal) {
            return {
                id: window.FractalContext.currentFractal.id,
                name: window.FractalContext.currentFractal.name
            };
        }
        return null;
    },

    exportToCsv() {
        if (!this.currentResults || this.currentResults.length === 0) {
            Toast.show('No results to export', 'warning');
            return;
        }

        try {
            // Get field order from the last query or use the first result's keys
            const fields = this.fieldOrder || Object.keys(this.currentResults[0]);

            // Create CSV header
            let csvContent = fields.map(field => `"${field}"`).join(',') + '\n';

            // Add CSV rows
            this.currentResults.forEach(row => {
                const values = fields.map(field => {
                    let value = row[field];

                    // Handle different data types
                    if (value === null || value === undefined) {
                        return '""';
                    } else if (typeof value === 'object') {
                        return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
                    } else {
                        return `"${String(value).replace(/"/g, '""')}"`;
                    }
                });
                csvContent += values.join(',') + '\n';
            });

            // Create and download file
            const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
            const link = document.createElement('a');
            const timestamp = new Date().toISOString().slice(0, 19).replace(/[:]/g, '-');
            const filename = `bifract-results-${timestamp}.csv`;

            if (link.download !== undefined) {
                const url = URL.createObjectURL(blob);
                link.setAttribute('href', url);
                link.setAttribute('download', filename);
                link.style.visibility = 'hidden';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
                URL.revokeObjectURL(url);

                Toast.show(`Exported ${this.currentResults.length} results to ${filename}`, 'success');
            } else {
                Toast.show('CSV export not supported in this browser', 'error');
            }

        } catch (error) {
            console.error('Export error:', error);
            Toast.show('Failed to export CSV: ' + error.message, 'error');
        }
    },

    renderChart(results) {
        const chartContainer = document.getElementById('chartContainer');
        const resultsTable = document.getElementById('resultsTable');
        let chartCanvas = document.getElementById('resultsChart');
        let networkDiv = document.getElementById('networkGraph');

        if (!chartContainer) return;

        // Hide table and fields drawer, show chart container
        if (resultsTable) resultsTable.style.display = 'none';
        chartContainer.style.display = 'block';
        const fieldsDrawer = document.getElementById('fieldStatsDrawer');
        if (fieldsDrawer) fieldsDrawer.style.display = 'none';

        // Remove any singleval overlay from a previous render
        const oldSingleval = chartContainer.querySelector('.singleval-display');
        if (oldSingleval) oldSingleval.remove();

        // Remove any heatmap from a previous render
        const oldHeatmap = chartContainer.querySelector('.heatmap-container');
        if (oldHeatmap) oldHeatmap.remove();

        // Remove any worldmap from a previous render
        const oldWorldmap = chartContainer.querySelector('.worldmap-container');
        if (oldWorldmap) {
            if (this._worldmapInstance) {
                this._worldmapInstance.remove();
                this._worldmapInstance = null;
            }
            oldWorldmap.remove();
        }
        if (this._heatmapTooltip) {
            this._heatmapTooltip.remove();
            this._heatmapTooltip = null;
        }

        // Restore canvas/network divs if destroyed by a previous singleval render
        if (!chartCanvas) {
            chartCanvas = document.createElement('canvas');
            chartCanvas.id = 'resultsChart';
            chartContainer.prepend(chartCanvas);
        }
        if (!networkDiv) {
            networkDiv = document.createElement('div');
            networkDiv.id = 'networkGraph';
            networkDiv.style.width = '100%';
            networkDiv.style.height = '400px';
            chartContainer.appendChild(networkDiv);
        }

        // Hide both chart elements initially
        chartCanvas.style.display = 'none';
        networkDiv.style.display = 'none';

        // Destroy existing chart if it exists
        if (this.currentChart) {
            this.currentChart.destroy();
            this.currentChart = null;
        }

        // Remove graph-specific elements left over from a previous graph() render
        const oldToolbar = chartContainer.querySelector('.graph-toolbar');
        if (oldToolbar) oldToolbar.remove();
        const oldDetail = chartContainer.querySelector('.graph-detail-panel');
        if (oldDetail) oldDetail.remove();

        // Remove pie chart wrapper from a previous piechart render
        const oldPie = chartContainer.querySelector('.pie-chart-wrapper');
        if (oldPie) oldPie.remove();


        if (this.chartType === 'piechart') {
            this.renderPieChart(results);
        } else if (this.chartType === 'barchart') {
            this.renderBarChart(results);
        } else if (this.chartType === 'graph') {
            this.renderGraph(results);
        } else if (this.chartType === 'singleval') {
            this.renderSingleVal(results);
        } else if (this.chartType === 'timechart') {
            this.renderTimeChart(results);
        } else if (this.chartType === 'histogram') {
            this.renderHistogram(results);
        } else if (this.chartType === 'heatmap') {
            this.renderHeatmap(results);
        } else if (this.chartType === 'worldmap') {
            this.renderWorldMap(results);
        }
    },

    renderPieChart(results) {
        const chartContainer = document.getElementById('chartContainer');
        const chartCanvas = document.getElementById('resultsChart');
        if (!chartContainer) return;
        if (chartCanvas) chartCanvas.style.display = 'none';

        const oldPie = chartContainer.querySelector('.pie-chart-wrapper');
        if (oldPie) oldPie.remove();

        const result = BifractCharts.renderPieChart(chartContainer, {
            data: results,
            fields: this.fieldOrder,
            config: this.chartConfig
        });
        if (result && result.chart) this.currentChart = result.chart;
    },

    renderBarChart(results) {
        const chartCanvas = document.getElementById('resultsChart');
        if (!chartCanvas) return;
        chartCanvas.style.display = 'block';

        const result = BifractCharts.renderBarChart(chartCanvas, {
            data: results,
            fields: this.fieldOrder,
            config: this.chartConfig
        });
        if (result && result.chart) this.currentChart = result.chart;
    },

    renderGraph(results) {
        const chartCanvas = document.getElementById('resultsChart');
        const networkDiv = document.getElementById('networkGraph');
        if (!networkDiv) return;

        if (chartCanvas) chartCanvas.style.display = 'none';
        networkDiv.style.display = 'block';

        if (this.currentChart) {
            this.currentChart.destroy();
            this.currentChart = null;
        }

        if (!this.chartConfig || !this.chartConfig.childField || !this.chartConfig.parentField) {
            return;
        }

        const childField = this.chartConfig.childField;
        const parentField = this.chartConfig.parentField;
        const limit = this.chartConfig.limit || 100;
        const cv = ThemeManager.getCSSVar;

        const nodes = new vis.DataSet();
        const edges = new vis.DataSet();
        const fields = this.fieldOrder || Object.keys(results[0] || {});
        const specifiedLabels = this.chartConfig.labels || [];
        const labelFields = specifiedLabels.length > 0
            ? specifiedLabels
            : fields.filter(f => f !== childField && f !== parentField);
        const limitedResults = results.slice(0, limit);
        const nodeDetails = new Map();

        // First pass: collect unique nodes and their details
        limitedResults.forEach((result) => {
            const childId = result[childField];
            const parentId = result[parentField];

            if (childId && !nodeDetails.has(childId)) {
                const details = {};
                labelFields.forEach(f => {
                    if (result[f] !== undefined && result[f] !== null && result[f] !== '') {
                        details[f] = result[f];
                    }
                });
                nodeDetails.set(childId, details);
            }

            if (parentId && parentId !== '' && parentId !== 'null' && !nodeDetails.has(parentId)) {
                nodeDetails.set(parentId, {});
            }
        });

        // Build a set of parent IDs for fast lookup
        const parentIds = new Set();
        limitedResults.forEach(r => {
            const pid = r[parentField];
            if (pid && pid !== '' && pid !== 'null') parentIds.add(pid);
        });

        // Create nodes with improved labels and HTML tooltips
        nodeDetails.forEach((details, nodeId) => {
            let shortLabel = nodeId;
            if (specifiedLabels.length > 0) {
                // Use specified label fields for node display
                const parts = specifiedLabels
                    .map(f => details[f])
                    .filter(v => v !== undefined && v !== null && v !== '');
                if (parts.length > 0) {
                    const joined = parts.join(' | ');
                    shortLabel = joined.length > 30 ? joined.substring(0, 30) + '\u2026' : joined;
                } else {
                    shortLabel = nodeId.length > 12 ? nodeId.substring(0, 12) + '\u2026' : nodeId;
                }
            } else if (details.image) {
                const parts = details.image.split(/[/\\]/);
                shortLabel = parts.pop() || nodeId.substring(0, 12);
            } else {
                shortLabel = nodeId.length > 12 ? nodeId.substring(0, 12) + '\u2026' : nodeId;
            }

            const isParent = parentIds.has(nodeId);

            // Build HTML tooltip
            const tooltipLines = Object.entries(details)
                .map(([k, v]) => `<div class="graph-tooltip-row"><span class="graph-tooltip-key">${Utils.escapeHtml(k)}</span><span class="graph-tooltip-val">${Utils.escapeHtml(String(v))}</span></div>`)
                .join('');
            const titleEl = document.createElement('div');
            titleEl.innerHTML = `<div class="graph-tooltip"><div class="graph-tooltip-header">${Utils.escapeHtml(nodeId)}</div>${tooltipLines || '<div class="graph-tooltip-empty">No additional fields</div>'}</div>`;

            nodes.add({
                id: nodeId,
                label: shortLabel,
                title: titleEl,
                size: 16,
                mass: 1,
                color: {
                    background: isParent ? cv('--graph-node-parent') : cv('--graph-node-child'),
                    border: isParent ? cv('--graph-node-parent') : cv('--graph-node-child'),
                    highlight: {
                        background: isParent ? cv('--graph-node-parent-hover') : cv('--graph-node-child-hover'),
                        border: cv('--accent-primary')
                    },
                    hover: {
                        background: isParent ? cv('--graph-node-parent-hover') : cv('--graph-node-child-hover'),
                        border: cv('--accent-primary')
                    }
                }
            });
        });

        // Create edges
        limitedResults.forEach((result) => {
            const childId = result[childField];
            const parentId = result[parentField];
            if (childId && parentId && parentId !== '' && parentId !== 'null') {
                edges.add({ from: parentId, to: childId });
            }
        });

        // Dynamic container height based on node count
        const nodeCount = nodeDetails.size;
        const dynamicHeight = Math.min(Math.max(400, nodeCount * 30), 800);
        networkDiv.style.height = dynamicHeight + 'px';

        // -- Toolbar --
        let graphToolbar = networkDiv.parentElement.querySelector('.graph-toolbar');
        if (graphToolbar) graphToolbar.remove();
        graphToolbar = document.createElement('div');
        graphToolbar.className = 'graph-toolbar';
        graphToolbar.innerHTML = `
            <div class="graph-stats">
                <span class="graph-stat-item"><span class="graph-stat-count" id="graphNodeCount">${nodes.length}</span> nodes</span>
                <span class="graph-stat-separator"></span>
                <span class="graph-stat-item"><span class="graph-stat-count" id="graphEdgeCount">${edges.length}</span> edges</span>
            </div>
            <div class="graph-legend">
                <span class="graph-legend-item"><span class="graph-legend-dot graph-legend-parent"></span>Parent</span>
                <span class="graph-legend-item"><span class="graph-legend-dot graph-legend-child"></span>Child</span>
            </div>
            <div class="graph-search">
                <input type="text" id="graphNodeSearch" class="graph-search-input" placeholder="Search nodes...">
            </div>
            <div class="graph-controls">
                <button class="toolbar-icon-btn" id="graphFitBtn" title="Fit to view">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"/></svg>
                </button>
                <button class="toolbar-icon-btn" id="graphZoomInBtn" title="Zoom in">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/><path d="M11 8v6"/><path d="M8 11h6"/></svg>
                </button>
                <button class="toolbar-icon-btn" id="graphZoomOutBtn" title="Zoom out">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/><path d="M8 11h6"/></svg>
                </button>
                <span class="graph-toolbar-sep"></span>
                <button class="toolbar-icon-btn" id="graphExportBtn" title="Export as PNG">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                </button>
            </div>
        `;
        networkDiv.parentElement.insertBefore(graphToolbar, networkDiv);

        // -- Detail Panel --
        let detailPanel = networkDiv.parentElement.querySelector('.graph-detail-panel');
        if (detailPanel) detailPanel.remove();
        detailPanel = document.createElement('div');
        detailPanel.className = 'graph-detail-panel';
        detailPanel.innerHTML = `
            <div class="graph-detail-header">
                <span class="graph-detail-title">Node Details</span>
                <button class="graph-detail-close">&times;</button>
            </div>
            <div class="graph-detail-body"></div>
        `;
        networkDiv.parentElement.appendChild(detailPanel);

        // -- Create Network --
        const data = { nodes, edges };
        const options = {
            layout: {
                hierarchical: {
                    enabled: true,
                    direction: 'UD',
                    sortMethod: 'directed',
                    nodeSpacing: 150,
                    levelSeparation: 100,
                    treeSpacing: 200
                }
            },
            physics: { enabled: false },
            interaction: {
                dragNodes: true,
                dragView: true,
                zoomView: true,
                zoomSpeed: 1.0,
                hover: true,
                selectConnectedEdges: true,
                multiselect: false,
                keyboard: { enabled: false },
                navigationButtons: false,
                tooltipDelay: 200,
                hideEdgesOnDrag: false,
                hideEdgesOnZoom: false,
                hideNodesOnDrag: false,
                zoomExtentOnStabilize: false
            },
            nodes: {
                shape: 'dot',
                size: 16,
                borderWidth: 2,
                chosen: true,
                font: {
                    size: 11,
                    color: cv('--graph-label'),
                    face: 'Inter',
                    vadjust: -4,
                    strokeWidth: 3,
                    strokeColor: cv('--graph-label-stroke')
                }
            },
            edges: {
                color: {
                    color: cv('--graph-edge'),
                    opacity: 0.5,
                    highlight: cv('--accent-primary'),
                    hover: cv('--graph-edge')
                },
                arrows: {
                    to: { enabled: true, scaleFactor: 0.6, type: 'arrow' }
                },
                width: 1.5,
                hoverWidth: 0.3,
                smooth: {
                    enabled: true,
                    type: 'cubicBezier',
                    forceDirection: 'vertical',
                    roundness: 0.4
                },
                chosen: true
            },
            configure: { enabled: false }
        };

        // Ensure pointer/touch events are set before vis.Network captures its canvas
        networkDiv.style.pointerEvents = 'auto';
        networkDiv.style.touchAction = 'auto';

        this.currentChart = new vis.Network(networkDiv, data, options);

        // Fit view
        setTimeout(() => {
            if (nodes.length < 10) {
                this.currentChart.moveTo({
                    position: { x: 0, y: 0 },
                    scale: 0.8,
                    animation: { duration: 400, easingFunction: 'easeInOutQuad' }
                });
            } else {
                this.currentChart.fit({
                    animation: { duration: 400, easingFunction: 'easeInOutQuad' },
                    padding: 80
                });
            }
        }, 200);

        // -- Toolbar handlers --
        document.getElementById('graphFitBtn')?.addEventListener('click', () => {
            this.currentChart.fit({ animation: { duration: 400, easingFunction: 'easeInOutQuad' }, padding: 60 });
        });
        document.getElementById('graphZoomInBtn')?.addEventListener('click', () => {
            this.currentChart.moveTo({ scale: this.currentChart.getScale() * 1.3, animation: { duration: 200 } });
        });
        document.getElementById('graphZoomOutBtn')?.addEventListener('click', () => {
            this.currentChart.moveTo({ scale: this.currentChart.getScale() / 1.3, animation: { duration: 200 } });
        });
        document.getElementById('graphExportBtn')?.addEventListener('click', () => {
            const canvas = networkDiv.querySelector('canvas');
            if (!canvas) return;
            const link = document.createElement('a');
            link.download = 'bifract-graph.png';
            link.href = canvas.toDataURL('image/png');
            link.click();
            Toast.show('Graph exported as PNG', 'success');
        });

        // -- Node search --
        const searchInput = document.getElementById('graphNodeSearch');
        if (searchInput) {
            searchInput.addEventListener('input', Utils.debounce((e) => {
                const term = e.target.value.toLowerCase().trim();
                if (!term) {
                    const updates = [];
                    nodeDetails.forEach((_, nodeId) => {
                        updates.push({ id: nodeId, opacity: 1.0, font: { color: cv('--graph-label') } });
                    });
                    nodes.update(updates);
                    return;
                }
                const updates = [];
                nodeDetails.forEach((details, nodeId) => {
                    const matches = nodeId.toLowerCase().includes(term) ||
                        Object.values(details).some(v => String(v).toLowerCase().includes(term));
                    updates.push({
                        id: nodeId,
                        opacity: matches ? 1.0 : 0.15,
                        font: { color: matches ? cv('--graph-label') : 'transparent' }
                    });
                });
                nodes.update(updates);
            }, 200));
        }

        // -- Node click: detail panel --
        const closePanel = () => {
            detailPanel.classList.remove('open');
            this.currentChart.unselectAll();
        };
        detailPanel.querySelector('.graph-detail-close').addEventListener('click', closePanel);

        this.currentChart.on('selectNode', (params) => {
            const nodeId = params.nodes[0];
            if (!nodeId) return;
            const details = nodeDetails.get(nodeId);
            const isParent = parentIds.has(nodeId);
            const roleLabel = isParent ? 'Parent' : 'Child';
            const body = detailPanel.querySelector('.graph-detail-body');

            let html = `
                <div class="graph-detail-id">
                    <span class="graph-detail-id-label">ID</span>
                    <span class="graph-detail-id-value">${Utils.escapeHtml(nodeId)}</span>
                    <button class="graph-detail-copy" title="Copy node ID">
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
                    </button>
                </div>
                <div class="graph-detail-role ${isParent ? 'parent' : 'child'}">${roleLabel}</div>
            `;

            if (details && Object.keys(details).length > 0) {
                html += '<div class="graph-detail-fields">';
                for (const [key, val] of Object.entries(details)) {
                    html += `<div class="graph-detail-field">
                        <div class="graph-detail-field-name">${Utils.escapeHtml(key)}</div>
                        <div class="graph-detail-field-value">${Utils.escapeHtml(String(val))}</div>
                    </div>`;
                }
                html += '</div>';
            } else {
                html += '<div class="graph-detail-empty">No additional fields available</div>';
            }

            body.innerHTML = html;
            detailPanel.classList.add('open');

            body.querySelector('.graph-detail-copy')?.addEventListener('click', () => {
                navigator.clipboard.writeText(nodeId).then(() => Toast.show('Copied', 'success'));
            });
        });

        this.currentChart.on('deselectNode', () => {
            detailPanel.classList.remove('open');
        });

        // -- Double-click to focus neighborhood --
        this.currentChart.on('doubleClick', (params) => {
            if (params.nodes.length > 0) {
                const nodeId = params.nodes[0];
                const connected = this.currentChart.getConnectedNodes(nodeId);
                this.currentChart.fit({
                    nodes: [nodeId, ...connected],
                    animation: { duration: 400, easingFunction: 'easeInOutQuad' },
                    padding: 80
                });
            }
        });

        // -- Right-click context menu --
        networkDiv.addEventListener('contextmenu', (e) => e.preventDefault());

        this.currentChart.on('oncontext', (params) => {
            params.event.preventDefault();
            const nodeId = this.currentChart.getNodeAt(params.pointer.DOM);
            if (!nodeId) return;

            const oldMenu = document.querySelector('.graph-context-menu');
            if (oldMenu) oldMenu.remove();

            const menu = document.createElement('div');
            menu.className = 'graph-context-menu';
            menu.style.left = params.event.pageX + 'px';
            menu.style.top = params.event.pageY + 'px';
            menu.innerHTML = `
                <button class="graph-ctx-item" data-action="focus">Focus neighborhood</button>
                <button class="graph-ctx-item" data-action="copy">Copy node ID</button>
            `;
            document.body.appendChild(menu);

            menu.addEventListener('click', (e) => {
                const action = e.target.dataset.action;
                if (action === 'focus') {
                    const connected = this.currentChart.getConnectedNodes(nodeId);
                    this.currentChart.fit({ nodes: [nodeId, ...connected], animation: { duration: 400 }, padding: 80 });
                } else if (action === 'copy') {
                    navigator.clipboard.writeText(nodeId).then(() => Toast.show('Copied', 'success'));
                }
                menu.remove();
            });

            const closeMenu = () => { menu.remove(); document.removeEventListener('click', closeMenu); };
            setTimeout(() => document.addEventListener('click', closeMenu), 0);
        });

    },

    renderSingleVal(results) {
        const chartContainer = document.getElementById('chartContainer');
        if (!chartContainer) return;
        BifractCharts.renderSingleVal(chartContainer, {
            data: results,
            fields: this.fieldOrder,
            config: this.chartConfig
        });
    },

    renderTimeChart(results) {
        const chartCanvas = document.getElementById('resultsChart');
        const networkDiv = document.getElementById('networkGraph');
        if (!chartCanvas) return;
        chartCanvas.style.display = 'block';
        if (networkDiv) networkDiv.style.display = 'none';
        if (this.currentChart) { this.currentChart.destroy(); this.currentChart = null; }

        const result = BifractCharts.renderTimeChart(chartCanvas, {
            data: results,
            fields: this.fieldOrder,
            config: this.chartConfig
        });
        if (result && result.chart) this.currentChart = result.chart;
    },

    renderHistogram(results) {
        const chartCanvas = document.getElementById('resultsChart');
        const networkDiv = document.getElementById('networkGraph');
        if (!chartCanvas) return;
        chartCanvas.style.display = 'block';
        if (networkDiv) networkDiv.style.display = 'none';
        if (this.currentChart) { this.currentChart.destroy(); this.currentChart = null; }

        const result = BifractCharts.renderHistogram(chartCanvas, {
            data: results,
            config: this.chartConfig
        });
        if (result && result.chart) this.currentChart = result.chart;
    },

    renderHeatmap(results) {
        const chartCanvas = document.getElementById('resultsChart');
        const networkDiv = document.getElementById('networkGraph');
        if (chartCanvas) chartCanvas.style.display = 'none';
        if (networkDiv) networkDiv.style.display = 'none';

        const chartContainer = document.getElementById('chartContainer');
        if (!chartContainer || !results || results.length === 0) return;

        const result = BifractCharts.renderHeatmap(chartContainer, {
            data: results,
            config: this.chartConfig
        });
        if (result && result.tooltip) this._heatmapTooltip = result.tooltip;
    },

    renderWorldMap(results) {
        const chartCanvas = document.getElementById('resultsChart');
        const networkDiv = document.getElementById('networkGraph');
        if (chartCanvas) chartCanvas.style.display = 'none';
        if (networkDiv) networkDiv.style.display = 'none';
        if (this.currentChart) { this.currentChart.destroy(); this.currentChart = null; }

        const chartContainer = document.getElementById('chartContainer');
        if (!chartContainer || !results || results.length === 0) return;
        if (typeof L === 'undefined') return;

        const latField = (this.chartConfig && this.chartConfig.latField) || 'latitude';
        const lonField = (this.chartConfig && this.chartConfig.lonField) || 'longitude';
        const labelField = (this.chartConfig && this.chartConfig.labelField) || null;
        const limit = (this.chartConfig && this.chartConfig.limit) || 5000;

        const container = document.createElement('div');
        container.className = 'worldmap-container';
        chartContainer.appendChild(container);

        const mapDiv = document.createElement('div');
        mapDiv.style.cssText = 'width:100%;height:100%;';
        container.appendChild(mapDiv);

        BifractWorldMap.render(mapDiv, results.slice(0, limit), { latField, lonField, labelField });
        this._worldmapInstance = BifractWorldMap._lastMap;
    },

    hexToRGB(hex) { return BifractCharts.hexToRGB(hex); },
    formatBinEdge(num) { return BifractCharts.formatBinEdge(num); },
    formatHeatmapValue(num) { return BifractCharts.formatHeatmapValue(num); },
    formatSingleValue(num) { return BifractCharts.formatSingleValue(num); },

    // ============================
    // Share Query Functionality
    // ============================

    // Generate shareable URL and copy to clipboard
    async generateAndCopyShareLink() {
        try {
            const elements = this.getElements();
            if (!elements.queryInput) return;

            const rawQuery = elements.queryInput.value.trim();
            if (!rawQuery) return;

            // Get current time range
            const timeRange = this.getTimeRange();
            const timeRangeSelect = document.getElementById('timeRange');
            const timeRangeValue = timeRangeSelect ? timeRangeSelect.value : '24h';

            // Get current fractal ID
            const fractalId = window.FractalContext?.currentFractal?.id;
            if (!fractalId) {
                console.error('[Share] No fractal selected');
                return;
            }

            // Build URL parameters
            const urlParams = new URLSearchParams();
            urlParams.set('q', btoa(encodeURIComponent(rawQuery))); // Base64 encode the query
            urlParams.set('tr', timeRangeValue); // Time range type
            urlParams.set('f', fractalId); // Fractal ID

            // Add custom time range if applicable
            if (timeRangeValue === 'custom' && elements.customStart && elements.customEnd) {
                if (elements.customStart.value && elements.customEnd.value) {
                    urlParams.set('ts', new Date(elements.customStart.value).toISOString());
                    urlParams.set('te', new Date(elements.customEnd.value).toISOString());
                }
            }

            // Generate full URL
            const shareUrl = `${window.location.origin}${window.location.pathname}?${urlParams.toString()}`;

            // Copy to clipboard
            await navigator.clipboard.writeText(shareUrl);
            console.log('[Share] Link copied to clipboard:', shareUrl);

        } catch (error) {
            console.error('[Share] Failed to generate/copy link:', error);

            // Fallback for older browsers
            try {
                const textArea = document.createElement('textarea');
                textArea.value = shareUrl;
                document.body.appendChild(textArea);
                textArea.select();
                document.execCommand('copy');
                document.body.removeChild(textArea);
            } catch (fallbackError) {
                console.error('[Share] Fallback copy also failed:', fallbackError);

                // Show error toast when both methods fail
                if (window.Toast) {
                    Toast.error('Copy Failed', 'Could not copy query link to clipboard');
                }
            }
        }
    },

    // Load query from URL parameters on page load
    loadFromShareLink() {
        console.log('[Share] loadFromShareLink called, checking URL:', window.location.search);
        // First, check if we even have a search string to avoid unnecessary processing
        if (!window.location.search) {
            console.log('[Share] No URL parameters found, skipping share link check');
            return false;
        }

        const urlParams = new URLSearchParams(window.location.search);

        // Check if we have share parameters - be very explicit about this check
        const hasQuery = urlParams.has('q');
        const hasTimeRange = urlParams.has('tr');
        const hasFractal = urlParams.has('f');

        if (!hasQuery || !hasTimeRange || !hasFractal) {
            console.log('[Share] No complete share parameters found (q:', hasQuery, ', tr:', hasTimeRange, ', f:', hasFractal, '), skipping');
            return false; // No share parameters found
        }

        console.log('[Share] Found complete share parameters, processing...');

        // Set flag to prevent clearing shared state during processing
        this.isProcessingSharedQuery = true;

        try {
            const encodedQuery = urlParams.get('q');
            const timeRangeValue = urlParams.get('tr');
            const fractalId = urlParams.get('f');

            // Validate the parameters before proceeding
            if (!encodedQuery || !timeRangeValue || !fractalId) {
                console.log('[Share] Share parameters are empty, skipping');
                return false;
            }

            // Decode query
            let query;
            try {
                query = decodeURIComponent(atob(encodedQuery));
            } catch (decodeError) {
                console.error('[Share] Failed to decode shared query:', decodeError);
                this.showError('Invalid shared link: malformed query');
                return false;
            }

            // Safely check if fractals are loaded with comprehensive null checking
            let hasFractals = false;
            let selectorFractals = 0;
            let listingFractals = 0;

            try {
                // Check FractalSelector availability
                if (window.FractalSelector &&
                    window.FractalSelector.availableFractals &&
                    Array.isArray(window.FractalSelector.availableFractals)) {
                    selectorFractals = window.FractalSelector.availableFractals.length;
                }

                // Check FractalListing availability
                if (window.FractalListing &&
                    window.FractalListing.fractals &&
                    Array.isArray(window.FractalListing.fractals)) {
                    listingFractals = window.FractalListing.fractals.length;
                }

                hasFractals = selectorFractals > 0 || listingFractals > 0;
                console.log('[Share] Fractal availability check - Selector:', selectorFractals, 'Listing:', listingFractals, 'HasFractals:', hasFractals);
            } catch (selectorError) {
                console.warn('[Share] Error checking fractal availability:', selectorError);
                hasFractals = false;
            }

            if (!hasFractals) {
                console.log('[Share] Fractals not loaded yet, deferring share link processing');
                // Store the share link data to be processed when fractals are loaded
                this.deferredShareLink = { encodedQuery, timeRangeValue, fractalId };

                // Start periodic check for fractals loading
                this.startDeferredShareLinkPolling();
                return true; // We are processing a share link, just deferred
            }

            // Check if user has access to the shared fractal with comprehensive null checking
            console.log('[Share] Checking access for fractal:', fractalId);

            let hasAccess = null;
            try {
                // Check FractalSelector first
                if (window.FractalSelector &&
                    window.FractalSelector.availableFractals &&
                    Array.isArray(window.FractalSelector.availableFractals) &&
                    window.FractalSelector.availableFractals.length > 0) {

                    console.log('[Share] Available fractals from FractalSelector:',
                               window.FractalSelector.availableFractals.map(f => f?.id || 'unknown'));
                    hasAccess = window.FractalSelector.availableFractals.find(f => f && f.id === fractalId);
                }

                // Also check FractalListing if FractalSelector doesn't have fractals
                if (!hasAccess &&
                    window.FractalListing &&
                    window.FractalListing.fractals &&
                    Array.isArray(window.FractalListing.fractals) &&
                    window.FractalListing.fractals.length > 0) {

                    console.log('[Share] Available fractals from FractalListing:',
                               window.FractalListing.fractals.map(f => f?.id || 'unknown'));
                    hasAccess = window.FractalListing.fractals.find(f => f && f.id === fractalId);
                }
            } catch (accessError) {
                console.error('[Share] Error checking fractal access:', accessError);
                this.showError('Failed to verify fractal access: ' + accessError.message);
                return false;
            }

            if (!hasAccess) {
                console.error('[Share] User does not have access to shared fractal:', fractalId);
                this.showError('Access denied: You do not have permission to view this shared query');
                return false;
            }

            // Switch to the shared fractal if it's not current
            if (!window.FractalContext?.currentFractal || window.FractalContext.currentFractal.id !== fractalId) {
                console.log('[Share] Switching to shared fractal:', hasAccess.name || 'Unknown');

                // Store the shared link data to be processed after fractal switch
                this.pendingShareData = {
                    query,
                    timeRangeValue,
                    customStart: urlParams.get('ts'),
                    customEnd: urlParams.get('te')
                };

                // Try different fractal selection methods in order of preference
                if (window.FractalContext && typeof window.FractalContext.setCurrentFractal === 'function') {
                    console.log('[Share] Using FractalContext.setCurrentFractal');
                    window.FractalContext.setCurrentFractal(hasAccess);
                    return true;
                } else if (window.FractalSelector && typeof window.FractalSelector.setCurrentFractal === 'function') {
                    console.log('[Share] Using FractalSelector.setCurrentFractal');
                    window.FractalSelector.setCurrentFractal(hasAccess);
                    return true;
                } else if (window.FractalSelector && typeof window.FractalSelector.selectFractal === 'function') {
                    console.log('[Share] Using FractalSelector.selectFractal');
                    window.FractalSelector.selectFractal(fractalId, hasAccess.name);
                    return true;
                } else if (window.FractalContext && typeof window.FractalContext.selectFractalOnServer === 'function') {
                    console.log('[Share] Using FractalContext.selectFractalOnServer');
                    // Manually set current fractal first, then sync to server
                    window.FractalContext.currentFractal = hasAccess;
                    window.FractalContext.selectFractalOnServer(fractalId);
                    return true;
                } else {
                    console.error('[Share] No fractal selection method available');
                    this.showError('Unable to switch to shared fractal: no selection methods available');
                    return false;
                }
            }

            console.log('[Share] Loading shared query:', { query, timeRangeValue, fractalId });

            // Load the shared data into UI
            this.loadShareDataIntoUI({
                query,
                timeRangeValue,
                customStart: urlParams.get('ts'),
                customEnd: urlParams.get('te')
            });

        } catch (error) {
            console.error('[Share] Failed to load shared query:', error);
            this.isProcessingSharedQuery = false; // Clear flag on error
            this.showError('Failed to load shared query: ' + (error.message || 'Unknown error'));
            return false;
        }

        // Successfully processed share link data
        return true;
    },

    // Load shared query data into the UI and execute
    loadShareDataIntoUI(shareData) {
        const { query, timeRangeValue, customStart, customEnd } = shareData;

        // Set flag to prevent clearing shared state during processing
        this.isProcessingSharedQuery = true;
        console.log('[Share] Starting shared query processing');

        // Navigate to the search view within the fractal
        if (window.App && typeof window.App.showFractalView === 'function') {
            console.log('[Share] Navigating to fractal search view');
            window.App.showFractalView('search');
        }

        // Small delay to ensure view has switched before setting values
        setTimeout(() => {
            const elements = this.getElements();

            // Set query in input
            if (elements.queryInput) {
                elements.queryInput.value = query;
            }

            // Set time range
            if (elements.timeRange) {
                elements.timeRange.value = timeRangeValue;

                // Handle custom time range
                if (timeRangeValue === 'custom') {
                    if (customStart && customEnd && elements.customStart && elements.customEnd) {
                        // Convert ISO strings to datetime-local format
                        const startDate = new Date(customStart);
                        const endDate = new Date(customEnd);

                        elements.customStart.value = this.formatDateTimeLocal(startDate);
                        elements.customEnd.value = this.formatDateTimeLocal(endDate);

                        // Show custom inputs
                        if (elements.customTimeInputs) {
                            elements.customTimeInputs.style.display = 'flex';
                        }
                    }
                }
            }

            // Trigger syntax highlighting if available
            if (window.SyntaxHighlight) {
                SyntaxHighlight.update();
            }

            // Auto-execute the shared query
            setTimeout(() => {
                this.execute();
                // Clear processing flag after execution starts
                setTimeout(() => {
                    this.isProcessingSharedQuery = false;
                    console.log('[Share] Finished shared query processing');
                    // Now clear URL parameters after everything is loaded
                    if (window.location.search) {
                        const urlParams = new URLSearchParams(window.location.search);
                        if (urlParams.has('q') || urlParams.has('tr') || urlParams.has('f')) {
                            const cleanUrl = `${window.location.origin}${window.location.pathname}`;
                            window.history.replaceState({}, document.title, cleanUrl);
                            console.log('[Share] Cleared URL parameters after successful load');
                        }
                    }
                }, 1000); // Wait a second for the query to fully execute
            }, 100);
        }, 200);
    },

    // Start polling for fractal availability
    startDeferredShareLinkPolling() {
        if (this.deferredPollingInterval) {
            clearInterval(this.deferredPollingInterval);
        }

        let attempts = 0;
        const maxAttempts = 20; // Try for 10 seconds (500ms intervals)

        this.deferredPollingInterval = setInterval(() => {
            attempts++;
            console.log(`[Share] Polling for fractals (attempt ${attempts}/${maxAttempts})`);

            let hasFractals = false;
            try {
                // Comprehensive null checking for fractal availability
                const selectorFractals = (window.FractalSelector &&
                                        window.FractalSelector.availableFractals &&
                                        Array.isArray(window.FractalSelector.availableFractals))
                                        ? window.FractalSelector.availableFractals.length : 0;

                const listingFractals = (window.FractalListing &&
                                       window.FractalListing.fractals &&
                                       Array.isArray(window.FractalListing.fractals))
                                       ? window.FractalListing.fractals.length : 0;

                hasFractals = selectorFractals > 0 || listingFractals > 0;
                console.log(`[Share] Poll check - Selector: ${selectorFractals}, Listing: ${listingFractals}, Total: ${hasFractals}`);
            } catch (checkError) {
                console.warn('[Share] Error during polling check:', checkError);
                hasFractals = false;
            }

            if (hasFractals) {
                console.log('[Share] Fractals now available, processing deferred share link');
                clearInterval(this.deferredPollingInterval);
                this.deferredPollingInterval = null;
                this.processDeferredShareLink();
            } else if (attempts >= maxAttempts) {
                console.error('[Share] Timeout waiting for fractals to load');
                clearInterval(this.deferredPollingInterval);
                this.deferredPollingInterval = null;
                this.deferredShareLink = null; // Clear the deferred data
                this.showError('Failed to load shared query: timeout waiting for fractal data');
            }
        }, 500);
    },

    // Check for deferred share links when fractals are loaded
    checkDeferredShareLink() {
        if (this.deferredShareLink) {
            console.log('[Share] Checking deferred share link after fractal load');
            const hasFractals = (window.FractalSelector?.availableFractals?.length > 0) ||
                              (window.FractalListing?.fractals?.length > 0);

            if (hasFractals) {
                console.log('[Share] Fractals now available, processing deferred share link');
                this.processDeferredShareLink();
            } else {
                console.log('[Share] Fractals still not available');
            }
        }
    },

    // Process deferred share link once fractals are loaded
    processDeferredShareLink() {
        if (!this.deferredShareLink) {
            console.log('[Share] No deferred share link to process');
            return;
        }

        console.log('[Share] Processing deferred share link');
        const { encodedQuery, timeRangeValue, fractalId } = this.deferredShareLink;

        // Clear the deferred data and polling
        this.deferredShareLink = null;
        if (this.deferredPollingInterval) {
            clearInterval(this.deferredPollingInterval);
            this.deferredPollingInterval = null;
        }

        try {
            // Validate that we have the required data
            if (!encodedQuery || !timeRangeValue || !fractalId) {
                console.error('[Share] Invalid deferred share data:', { encodedQuery, timeRangeValue, fractalId });
                this.showError('Invalid shared link data');
                return;
            }

            let query;
            try {
                query = decodeURIComponent(atob(encodedQuery));
            } catch (decodeError) {
                console.error('[Share] Failed to decode deferred query:', decodeError);
                this.showError('Invalid shared link: malformed query');
                return;
            }

            // Now fractals should be loaded, check access again with comprehensive null checking
            let hasAccess = null;
            try {
                // Check FractalSelector first
                if (window.FractalSelector &&
                    window.FractalSelector.availableFractals &&
                    Array.isArray(window.FractalSelector.availableFractals)) {
                    hasAccess = window.FractalSelector.availableFractals.find(f => f && f.id === fractalId);
                }

                // Also check FractalListing if FractalSelector doesn't have fractals
                if (!hasAccess &&
                    window.FractalListing &&
                    window.FractalListing.fractals &&
                    Array.isArray(window.FractalListing.fractals)) {
                    hasAccess = window.FractalListing.fractals.find(f => f && f.id === fractalId);
                }
            } catch (accessCheckError) {
                console.error('[Share] Error checking deferred fractal access:', accessCheckError);
                this.showError('Failed to verify fractal access: ' + accessCheckError.message);
                return;
            }

            console.log('[Share] Deferred access check result:', !!hasAccess, 'for fractal:', fractalId);
            if (!hasAccess) {
                console.error('[Share] User does not have access to deferred fractal:', fractalId);
                this.showError('Access denied: You do not have permission to view this shared query');
                return;
            }

            // Switch to the shared fractal if it's not current
            if (!window.FractalContext?.currentFractal || window.FractalContext.currentFractal.id !== fractalId) {
                console.log('[Share] Switching to deferred shared fractal:', hasAccess.name || 'Unknown');

                // Store the shared link data to be processed after fractal switch
                this.pendingShareData = {
                    query,
                    timeRangeValue,
                    customStart: null, // We don't have URL params in deferred processing
                    customEnd: null
                };

                // Try different fractal selection methods in order of preference
                if (window.FractalContext && typeof window.FractalContext.setCurrentFractal === 'function') {
                    console.log('[Share] Using FractalContext.setCurrentFractal for deferred');
                    window.FractalContext.setCurrentFractal(hasAccess);
                    return;
                } else if (window.FractalSelector && typeof window.FractalSelector.setCurrentFractal === 'function') {
                    console.log('[Share] Using FractalSelector.setCurrentFractal for deferred');
                    window.FractalSelector.setCurrentFractal(hasAccess);
                    return;
                } else if (window.FractalSelector && typeof window.FractalSelector.selectFractal === 'function') {
                    console.log('[Share] Using FractalSelector for deferred');
                    window.FractalSelector.selectFractal(fractalId, hasAccess.name);
                    return;
                } else if (window.FractalContext && typeof window.FractalContext.selectFractalOnServer === 'function') {
                    console.log('[Share] Using FractalContext.selectFractalOnServer for deferred');
                    // Manually set current fractal first, then sync to server
                    window.FractalContext.currentFractal = hasAccess;
                    window.FractalContext.selectFractalOnServer(fractalId);
                    return;
                } else if (window.FractalListing && typeof window.FractalListing.selectFractal === 'function') {
                    console.log('[Share] Using FractalListing for deferred');
                    window.FractalListing.selectFractal(fractalId);
                    return;
                } else {
                    console.error('[Share] No fractal selection method available for deferred processing');
                    this.showError('Unable to switch to shared fractal: no selection methods available');
                    return;
                }
            }

            // Load directly if already in correct fractal
            console.log('[Share] Already in correct fractal, loading query directly');
            this.loadShareDataIntoUI({
                query,
                timeRangeValue,
                customStart: null, // We don't have custom time data in deferred processing
                customEnd: null
            });

        } catch (error) {
            console.error('[Share] Failed to process deferred share link:', error);
            this.isProcessingSharedQuery = false; // Clear flag on error
            this.showError('Failed to load shared query: ' + (error.message || 'Unknown error'));
        }
    },

    // Clear shared query state and URL parameters
    clearSharedQueryState() {
        console.log('[Share] Clearing shared query state');

        // Clear pending and deferred share data
        this.pendingShareData = null;
        this.deferredShareLink = null;
        this.isProcessingSharedQuery = false;

        // Clear polling interval if active
        if (this.deferredPollingInterval) {
            clearInterval(this.deferredPollingInterval);
            this.deferredPollingInterval = null;
        }

        // Clear URL parameters if they exist
        if (window.location.search) {
            const urlParams = new URLSearchParams(window.location.search);
            if (urlParams.has('q') || urlParams.has('tr') || urlParams.has('f')) {
                const cleanUrl = `${window.location.origin}${window.location.pathname}`;
                window.history.replaceState({}, document.title, cleanUrl);
                console.log('[Share] Cleared URL parameters');
            }
        }
    },

    // Helper function to format date for datetime-local input
    formatDateTimeLocal(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        return `${year}-${month}-${day} ${hours}:${minutes}`;
    }
};

// Make it globally available
window.QueryExecutor = QueryExecutor;
