// Query execution and results rendering
const QueryExecutor = {
    currentResults: [],
    currentTimeRange: null,
    sortColumn: null,
    sortDirection: null,
    columnWidths: {},
    columnOrder: null,
    isAggregated: false,
    limitHit: null,
    chartType: '',
    chartConfig: {},
    currentChart: null, // Store current chart instance
    currentRequest: null, // Track current request for cancellation
    currentHistRequest: null, // Track histogram request for cancellation
    currentFractalId: null, // Track current fractal to validate responses
    currentCursor: null, // Cursor token for next-page fetch
    currentQuery: '', // Last executed query text (needed by loadMore)
    loadingMore: false, // Guard against concurrent load-more clicks
    hasLoadedShareLink: false, // Track if we've already loaded shared link on first fractal change
    pendingShareData: null, // Store shared link data waiting for fractal switch
    deferredShareLink: null, // Store share link data waiting for fractals to load
    deferredPollingInterval: null, // Interval for polling fractal availability
    isProcessingSharedQuery: false, // Flag to prevent clearing state during shared query processing
    pendingActiveDays: null,        // Set before execute() to pass pre-computed days to the query handler (skips preflight)

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
        timeRange: null,
        customStart: null,
        customEnd: null,
        customTimeInputs: null
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

    // Load recent logs sample for initial fractal exploration.
    // Fires logs and histogram fetches independently so each renders as it arrives.
    loadRecentLogsSample(config = null) {
        const elements = this.getElements(config);

        // Cancel any in-flight requests from a previous fractal
        if (this.currentRequest) this.currentRequest.abort();
        if (this.currentHistRequest) this.currentHistRequest.abort();

        // Clear any deferred streaming indicator left over from a superseded execute().
        // execute()'s finally guard (currentRequest === myController) is false by the
        // time we get here, so it won't clean up after itself.
        this._clearLoadingTimer();
        this._endLoadingIndicator();

        this.currentRequest = new AbortController();
        this.currentHistRequest = new AbortController();
        this.currentFractalId = window.FractalContext?.currentFractal?.id || null;

        if (elements.resultsTable) {
            elements.resultsTable.innerHTML = '<div class="loading-spinner"><span class="spinner"></span></div>';
        }
        if (elements.errorDiv) elements.errorDiv.style.display = 'none';
        this._updateLoadMoreButton(false);

        this._fetchRecentLogs(elements);
        this._fetchRecentHistogram();
    },

    async _fetchRecentLogs(elements) {
        try {
            const queryStart = performance.now();
            const data = await HttpUtils.safeFetch(`/api/v1/logs/recent?t=${Date.now()}`, {
                method: 'GET',
                credentials: 'include',
                headers: { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' },
                signal: this.currentRequest.signal
            });
            const executionTime = Math.round(performance.now() - queryStart);

            if (this.currentFractalId !== (window.FractalContext?.currentFractal?.id || null)) return;

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
            this.sortColumn = null;
            this.sortDirection = null;

            if (elements.resultsCount) {
                elements.resultsCount.textContent = `${this.currentResults.length} recent logs (last 24h)`;
            }
            if (elements.executionTime) {
                elements.executionTime.textContent = `(${executionTime}ms)`;
                elements.executionTime.style.display = 'inline';
            }

            if (window.Pagination) {
                Pagination.setResults(this.currentResults);
                this.renderPage(Pagination.getCurrentPageResults());
            } else {
                this.renderResults(this.currentResults);
            }

            if (window.FieldStats) FieldStats.refresh();

            this.currentTimeRange = {
                start: data.time_start || new Date(Date.now() - 86400000).toISOString(),
                end: data.time_end || new Date().toISOString()
            };

            if (window.Comments) {
                Comments.fetchCommentedLogIds().then(() => this.updateCommentHighlights());
            }
        } catch (error) {
            if (error.name === 'AbortError') return;
            console.error('Failed to load recent logs:', error);
            this.showError('Failed to load recent logs: ' + error.message);
            if (elements.resultsTable) elements.resultsTable.innerHTML = '';
        } finally {
            this.currentRequest = null;
        }
    },

    async _fetchRecentHistogram() {
        try {
            const data = await HttpUtils.safeFetch(`/api/v1/logs/histogram?t=${Date.now()}`, {
                method: 'GET',
                credentials: 'include',
                headers: { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' },
                signal: this.currentHistRequest.signal
            });

            if (this.currentFractalId !== (window.FractalContext?.currentFractal?.id || null)) return;
            if (!data.success || !data.histogram) return;

            const timeRange = {
                start: data.time_start || new Date(Date.now() - 86400000).toISOString(),
                end: data.time_end || new Date().toISOString()
            };

            if (window.Timeline) {
                requestAnimationFrame(() => Timeline.renderFromHistogram(data.histogram, timeRange));
            }
        } catch (error) {
            if (error.name === 'AbortError') return;
            console.warn('Failed to load histogram:', error);
        } finally {
            this.currentHistRequest = null;
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

        // Store for use by loadMore()
        this.currentQuery = query;
        this.currentCursor = null;

        // Clear shared query state when user runs their own query (but not during shared query processing)
        if (!this.isProcessingSharedQuery) {
            this.clearSharedQueryState();
        }

        // Cancel any previous request
        if (this.currentRequest) {
            this.currentRequest.abort();
        }

        // Create new abort controller for this request. Capture it so the finally
        // block only tears down loading state if this run is still the active one
        // (a rapid re-run supersedes us and owns the indicator/timer).
        this.currentRequest = new AbortController();
        const myController = this.currentRequest;

        // Store current fractal ID to validate response
        this.currentFractalId = window.FractalContext?.currentFractal?.id || null;

        // Get time range
        this.currentTimeRange = this.getTimeRange();

        // Capture run metadata; the history entry is recorded on finalize once
        // result count and duration are known (see _finalizeQuery).
        const trToken = window.TimePicker?.state?.type || '24h';
        this._pendingHistory = {
            query: query,
            timeRange: trToken,
            customStart: trToken === 'custom' ? (this.currentTimeRange?.start || '') : '',
            customEnd: trToken === 'custom' ? (this.currentTimeRange?.end || '') : ''
        };

        // Hide previous results and show loading
        if (elements.errorDiv) elements.errorDiv.style.display = 'none';
        this.clearQueryError();
        const profilePanel = document.getElementById('profilePanel');
        if (profilePanel) { profilePanel.style.display = 'none'; profilePanel.innerHTML = ''; }

        // Reset chart/graph container so loading spinner is visible
        const chartContainer = document.getElementById('chartContainer');
        if (chartContainer) chartContainer.style.display = 'none';
        const fieldsDrawerReset = document.getElementById('fieldStatsDrawer');
        if (fieldsDrawerReset) fieldsDrawerReset.style.display = '';

        // Profiling collects per-shard execution stats over the full result,
        // which the progressive stream does not produce. When the SQL/profile
        // panel is enabled, use the buffered endpoint so the profile renders;
        // otherwise stream for newest-first progressive results. This also picks
        // the loading style: a blocking spinner (buffered) vs the deferred bar.
        const wantsProfile = !!(window.UserPrefs && UserPrefs.showSQL());

        // A superseded run's finally is guarded out and cannot clear its own
        // deferred timer, so clear any pending one here before starting fresh.
        this._clearLoadingTimer();

        if (elements.resultsTable) elements.resultsTable.style.display = 'block';
        if (wantsProfile) {
            // Buffered path: show the blocking spinner immediately.
            if (elements.resultsTable) {
                elements.resultsTable.innerHTML = '<div class="loading-spinner"><span class="spinner"></span><button class="cancel-query-btn" onclick="QueryExecutor.cancelQuery()">Cancel</button></div>';
            }
            this._loadingMode = 'spinner';
            this._loadingShown = true;
            this._setRunButtonState(true);
        } else {
            // Streaming path: defer the bar + Cancel flip so fast (sub-threshold)
            // searches don't flash chrome. Prior results stay until rows arrive.
            this._beginLoadingIndicator(elements);
        }

        try {
            // Get currently selected fractal for context
            let requestBody = {
                query: query,
                start: this.currentTimeRange.start,
                end: this.currentTimeRange.end
            };
            if (this.currentTimeRange.selective) requestBody.selective = true;
            if (this.pendingActiveDays && this.pendingActiveDays.length) {
                requestBody.active_days = this.pendingActiveDays;
                this.pendingActiveDays = null;
            }

            // Include fractal context if FractalContext is available (skip for prisms - server uses session)
            if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
                requestBody.fractal_id = window.FractalContext.currentFractal.id;
            }

            if (wantsProfile) {
                requestBody.profile = true;
            }
            const endpoint = wantsProfile ? '/api/v1/query' : '/api/v1/query/stream';

            const res = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(requestBody),
                signal: this.currentRequest.signal
            });

            // Validate that we're still on the same fractal (prevent race conditions).
            // Abort so the abandoned stream body doesn't keep the server scanning.
            if (this.currentFractalId !== (window.FractalContext?.currentFractal?.id || null)) {
                myController.abort();
                return;
            }

            const contentType = res.headers.get('Content-Type') || '';
            if (!contentType.includes('application/x-ndjson')) {
                // prepareQuery short-circuited with a single JSON response (auth /
                // parse / translate error, or an empty prism result).
                let data = {};
                try { data = await res.json(); } catch (e) {}
                if (!res.ok || !data.success) {
                    const msg = data.error || `Query failed (${res.status})`;
                    this.showError(msg, data.error_type);
                    this.renderTableError(msg);
                    return;
                }
                this._applyQueryMeta(data, elements, false);
                this.currentResults = data.results || [];
                this._renderCurrentResults(elements);
                this._finalizeQuery(data, elements);
                return;
            }

            await this._consumeQueryStream(res, elements);
        } catch (error) {
            // Don't show error if request was cancelled (fractal switch)
            if (error.name === 'AbortError') {
                return;
            }

            this.showError(error.message);
            this.renderTableError(error.message);
        } finally {
            // Only tear down if a newer run hasn't superseded us (which would own
            // the request, timer, and loading indicator).
            if (this.currentRequest === myController) {
                this._streamingActive = false;
                this.currentRequest = null;
                this._endLoadingIndicator();
            }
        }
    },

    // Dispatch the Run/Cancel button: cancel an in-flight query, else run a new one.
    runOrCancel() {
        if (this._queryRunning) {
            this.cancelQuery();
        } else {
            this.execute();
        }
    },

    // Toggle the search button between "Run" and a themed "Cancel" while running.
    _setRunButtonState(running) {
        this._queryRunning = running;
        const btn = document.getElementById('executeBtn');
        if (!btn) return;
        const text = btn.querySelector('.btn-text');
        const shortcut = btn.querySelector('.btn-shortcut');
        if (running) {
            btn.classList.add('is-running');
            if (text) text.textContent = 'Cancel';
            if (shortcut) shortcut.style.display = 'none';
        } else {
            btn.classList.remove('is-running');
            if (text) text.textContent = 'Run';
            if (shortcut) shortcut.style.display = '';
        }
    },

    // Delay (ms) before the streaming loading chrome (bar + Cancel flip + searching
    // line) appears. Searches that finish faster show no chrome at all, avoiding a
    // flicker. Raise toward 1000 to keep sub-second searches fully silent.
    LOADING_INDICATOR_DELAY_MS: 500,

    // Schedule the streaming loading chrome. If the query finishes before the delay
    // (_endLoadingIndicator clears the timer), nothing is shown.
    _beginLoadingIndicator(elements) {
        this._clearLoadingTimer();
        this._loadingBar('hide'); // clear any leftover bar from a superseded run
        this._loadingShown = false;
        this._loadingMode = 'spinner'; // until the meta frame says it streams
        this._loadingGotRows = false;
        this._loadingTimer = setTimeout(() => {
            this._loadingTimer = null;
            this._loadingShown = true;
            this._queryHadError = false;
            this._outputTypeStatus('loading');
            this._setRunButtonState(true);
            if (this._loadingMode === 'bar') {
                this._loadingBar('show');
                if (!this._loadingGotRows && elements.resultsTable) {
                    elements.resultsTable.innerHTML =
                        '<div class="stream-searching"><span>Searching, newest first…</span>' +
                        '<button class="cancel-query-btn" onclick="QueryExecutor.cancelQuery()">Cancel</button></div>';
                }
            } else if (elements.resultsTable) {
                elements.resultsTable.innerHTML =
                    '<div class="loading-spinner"><span class="spinner"></span>' +
                    '<button class="cancel-query-btn" onclick="QueryExecutor.cancelQuery()">Cancel</button></div>';
            }
        }, this.LOADING_INDICATOR_DELAY_MS);
    },

    // Tear down the loading chrome: cancel a pending show, finish the bar, reset the button.
    _endLoadingIndicator() {
        this._clearLoadingTimer();
        if (this._loadingShown && this._loadingMode === 'bar') {
            this._loadingBar('done');
        } else {
            this._loadingBar('hide');
        }
        if (this._loadingShown) {
            if (!this._queryHadError) this._outputTypeStatus('done');
        } else {
            this._outputTypeStatus('reset');
        }
        this._loadingShown = false;
        this._setRunButtonState(false);
    },

    _clearLoadingTimer() {
        if (this._loadingTimer) {
            clearTimeout(this._loadingTimer);
            this._loadingTimer = null;
        }
    },

    // Consume an NDJSON stream of query result frames, rendering rows newest-first
    // as they arrive. Frames: meta, histogram, rows, progress, error, done.
    async _consumeQueryStream(res, elements) {
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        let pendingRender = false;
        let timeRange = this.currentTimeRange;
        let histogram = null;

        let lastPageKey = '';
        const scheduleRender = () => {
            if (pendingRender) return;
            pendingRender = true;
            requestAnimationFrame(() => {
                pendingRender = false;
                // Only rebuild the table when the current page's rows have actually
                // changed. After page 1 fills up, streaming rows go to later pages
                // and the visible content is stable — avoid needless innerHTML churn.
                const pg = window.Pagination ? Pagination.currentPage : 1;
                const ps = window.Pagination ? Pagination.pageSize : 50;
                const start = (pg - 1) * ps;
                const end = Math.min(start + ps, this.currentResults.length);
                const pageKey = `${pg}:${start}-${end}`;
                if (pageKey !== lastPageKey) {
                    lastPageKey = pageKey;
                    this._renderCurrentResults(elements, { preservePage: true });
                } else {
                    // Page content unchanged — just refresh pagination controls and count
                    if (window.Pagination) {
                        Pagination.allResults = this.currentResults;
                        Pagination.totalResults = this.currentResults.length;
                        const bar = document.getElementById('paginationBar');
                        const pageNums = document.getElementById('pageNumbers');
                        const total = Pagination.getTotalPages();
                        if (bar && pageNums) {
                            if (total > 1) {
                                bar.style.display = 'grid';
                                pageNums.innerHTML = Pagination._renderPageNumbers(total);
                            } else {
                                bar.style.display = 'none';
                            }
                        }
                    }
                }
                if (elements.resultsCount) {
                    elements.resultsCount.textContent = `${this.currentResults.length.toLocaleString()} results`;
                }
            });
        };

        const handleFrame = (frame) => {
            switch (frame.type) {
                case 'meta':
                    this.currentResults = [];
                    // Tell the deferred indicator which style this query uses: the
                    // thin progress bar for a real stream, a spinner otherwise.
                    this._loadingMode = frame.streaming ? 'bar' : 'spinner';
                    this._loadingGotRows = false;
                    // New query starts at page 1; subsequent streamed batches
                    // preserve whatever page the user is viewing.
                    if (window.Pagination) {
                        Pagination.currentPage = 1;
                        Pagination.allResults = [];
                        Pagination.totalResults = 0;
                    }
                    timeRange = {
                        start: frame.time_start || this.currentTimeRange.start,
                        end: frame.time_end || this.currentTimeRange.end
                    };
                    this._applyQueryMeta({
                        sql: frame.sql,
                        field_order: frame.field_order,
                        is_aggregated: frame.is_aggregated,
                        chart_type: frame.chart_type,
                        chart_config: frame.chart_config
                    }, elements, !!frame.streaming);
                    break;
                case 'histogram':
                    histogram = frame.buckets || null;
                    break;
                case 'rows':
                    if (frame.data && frame.data.length) {
                        this._loadingGotRows = true;
                        for (const row of frame.data) this.currentResults.push(row);
                        scheduleRender();
                    }
                    break;
                case 'progress':
                    // Only drive the bar once the deferred indicator has shown it.
                    if (this._loadingShown && this._loadingMode === 'bar') {
                        this._loadingBar('set', typeof frame.ratio === 'number' ? frame.ratio : 0);
                    }
                    break;
                case 'error':
                    this.showError(frame.error || 'Query failed', frame.error_type);
                    this.renderTableError(frame.error || 'Query failed');
                    break;
                case 'done':
                    this._streamingActive = false;
                    this._renderCurrentResults(elements, { preservePage: true });
                    this._finalizeQuery({
                        has_more: frame.has_more,
                        next_cursor: frame.next_cursor,
                        limit_hit: frame.limit_hit,
                        execution_ms: frame.execution_ms,
                        histogram: histogram,
                        time_start: timeRange.start,
                        time_end: timeRange.end
                    }, elements);
                    break;
            }
        };

        for (;;) {
            const { value, done } = await reader.read();
            if (done) break;
            buf += decoder.decode(value, { stream: true });
            let nl;
            while ((nl = buf.indexOf('\n')) >= 0) {
                const line = buf.slice(0, nl).trim();
                buf = buf.slice(nl + 1);
                if (!line) continue;
                let frame;
                try { frame = JSON.parse(line); } catch (e) { continue; }
                handleFrame(frame);
            }
        }
    },

    // Apply query metadata (SQL display, output type, container setup) shared by
    // the buffered and streaming paths. `streaming` selects the loading indicator.
    _applyQueryMeta(data, elements, streaming) {
        if (data.sql && elements.sqlOutput) {
            elements.sqlOutput.innerHTML = this.highlightSQL(data.sql);
            const sqlPreview = document.querySelector('.sql-preview');
            if (sqlPreview && window.UserPrefs && UserPrefs.showSQL()) {
                sqlPreview.style.display = 'block';
                elements.sqlOutput.style.display = 'block';
                const toggleBtn = document.getElementById('toggleSqlBtn');
                if (toggleBtn) toggleBtn.textContent = 'Hide SQL';
            }
        }

        this.fieldOrder = data.field_order || null;
        this.isAggregated = data.is_aggregated || false;
        this.chartType = data.chart_type || '';
        this.chartConfig = data.chart_config || {};
        this.sortColumn = null;
        this.sortDirection = null;

        const outputTypeLabels = {
            piechart: 'Pie Chart', barchart: 'Bar Chart', graph: 'Network Graph',
            singleval: 'Single Value', timechart: 'Time Chart', histogram: 'Histogram',
            heatmap: 'Heat Map', worldmap: 'World Map',
        };
        const outputLabel = document.getElementById('outputTypeLabel');
        if (outputLabel) outputLabel.textContent = outputTypeLabels[this.chartType] || 'Table';

        // Rows arrive pre-sorted (timestamp DESC) during a stream; block column
        // sorting until done so a mid-stream re-sort can't scramble partial data.
        // The loading chrome itself is owned by the deferred indicator, not here.
        this._streamingActive = streaming;
    },

    // Render this.currentResults as a chart or a paginated table.
    // opts.preservePage keeps the user's current page (used for incremental
    // streaming updates) instead of resetting to page 1 as a new query would.
    _renderCurrentResults(elements, opts = {}) {
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
            if (elements.resultsTable) elements.resultsTable.style.display = 'block';
            if (window.Pagination) {
                if (opts.preservePage) {
                    // Incremental, page-preserving update (mirrors loadMore) so
                    // streaming batches don't bounce the user back to page 1.
                    Pagination.allResults = this.currentResults;
                    Pagination.totalResults = this.currentResults.length;
                    Pagination.updateDisplay();
                } else {
                    Pagination.setResults(this.currentResults);
                    this.renderPage(Pagination.getCurrentPageResults());
                }
            } else {
                this.renderResults(this.currentResults);
            }
        }
    },

    // Finalize a completed query: counts, cursor, timeline, comments, profile.
    // The loading chrome is torn down separately by _endLoadingIndicator.
    _finalizeQuery(data, elements) {
        this.limitHit = data.limit_hit || null;
        this.currentCursor = data.next_cursor || null;

        // Record the run into query history now that we know counts + timing.
        if (window.QueryPalette && this._pendingHistory) {
            QueryPalette.recordRun({
                ...this._pendingHistory,
                resultCount: this.currentResults ? this.currentResults.length : null,
                durationMs: data.execution_ms != null ? data.execution_ms : null,
                status: data.limit_hit ? 'limit_hit' : 'ok'
            });
            this._pendingHistory = null;
        }

        if (elements.resultsCount) {
            const resultsLength = this.currentResults.length;
            if (data.limit_hit) {
                const countSpan = `<span style="color: #e74c3c; font-weight: 500;">${resultsLength}</span>`;
                switch (data.limit_hit) {
                    case 'bloom':
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
                const suffix = data.has_more ? '+' : '';
                elements.resultsCount.textContent = `${resultsLength.toLocaleString()}${suffix} results`;
            }
        }

        if (elements.executionTime && data.execution_ms !== undefined) {
            elements.executionTime.textContent = `(${data.execution_ms}ms)`;
            elements.executionTime.style.display = 'inline';
        }

        const exportBtn = document.getElementById('exportCsvBtn');
        if (exportBtn && this.currentResults && this.currentResults.length > 0) {
            exportBtn.style.display = 'inline-block';
        }

        const wrapBtn = document.getElementById('wrapToggleBtn');
        if (wrapBtn && this.currentResults && this.currentResults.length > 0) {
            wrapBtn.style.display = 'inline-block';
            wrapBtn.classList.add('active');
            const resultsTableEl = document.getElementById('resultsTable');
            if (resultsTableEl) resultsTableEl.classList.add('table-wrap');
        }

        this._updateLoadMoreButton(data.has_more);

        if (window.FieldStats) FieldStats.refresh();

        const shouldShowTimeline = !this.fieldOrder || this.fieldOrder.includes('timestamp');
        if (window.Timeline) {
            if (shouldShowTimeline && data.histogram) {
                const histTimeRange = {
                    start: data.time_start || this.currentTimeRange.start,
                    end: data.time_end || this.currentTimeRange.end
                };
                requestAnimationFrame(() => Timeline.renderFromHistogram(data.histogram, histTimeRange));
            } else if (shouldShowTimeline) {
                requestAnimationFrame(() => Timeline.render(this.currentResults, this.currentTimeRange));
            } else {
                Timeline.hide();
            }
        }

        if (window.Comments) {
            Comments.fetchCommentedLogIds().then(() => this.updateCommentHighlights());
        }

        if (data.profile) {
            this.renderProfilePanel(data.profile);
        }
    },

    // Drive the subtle determinate loading bar above the results.
    // state: 'show' | 'set' (ratio 0..1) | 'done' | 'hide'.
    _loadingBar(state, ratio) {
        let bar = document.getElementById('queryLoadingBar');
        const elements = this.getElements();
        if (!bar) {
            if (state === 'hide' || state === 'done') return;
            const anchor = elements.resultsTable;
            if (!anchor || !anchor.parentNode) return;
            bar = document.createElement('div');
            bar.id = 'queryLoadingBar';
            bar.className = 'query-loading-bar';
            bar.innerHTML = '<div class="query-loading-bar-fill"></div>';
            anchor.parentNode.insertBefore(bar, anchor);
        }
        const fill = bar.firstElementChild;
        switch (state) {
            case 'show':
                bar.classList.remove('is-done');
                bar.style.display = 'block';
                this._loadingProgress = 4;
                if (fill) fill.style.width = '4%';
                break;
            case 'set': {
                // ClickHouse revises its total-rows estimate upward mid-scan, so the
                // raw read/total ratio is non-monotonic. Only ever advance the bar
                // (never slide back), and hold below 100% until 'done' completes it.
                bar.style.display = 'block';
                const target = Math.min(95, Math.max(4, (ratio || 0) * 100));
                this._loadingProgress = Math.max(this._loadingProgress || 4, target);
                if (fill) fill.style.width = `${this._loadingProgress}%`;
                break;
            }
            case 'done':
                this._loadingProgress = 100;
                if (fill) fill.style.width = '100%';
                bar.classList.add('is-done');
                setTimeout(() => { if (bar) bar.style.display = 'none'; }, 280);
                break;
            case 'hide':
                this._loadingProgress = 4;
                bar.style.display = 'none';
                break;
        }
    },

    _outputTypeStatus(state) {
        const spinner = document.getElementById('outputTypeSpinner');
        const check = document.getElementById('outputTypeCheck');
        const error = document.getElementById('outputTypeError');
        if (!spinner || !check || !error) return;
        const hideAll = () => {
            spinner.classList.remove('is-active');
            check.classList.remove('is-visible'); check.style.display = 'none';
            error.classList.remove('is-visible'); error.style.display = 'none';
        };
        const show = (el) => {
            el.style.display = 'block';
            el.offsetWidth; // force reflow so transition fires
            el.classList.add('is-visible');
        };
        if (state === 'loading') {
            hideAll();
            spinner.classList.add('is-active');
        } else if (state === 'done') {
            hideAll();
            show(check);
        } else if (state === 'error') {
            hideAll();
            show(error);
        } else {
            hideAll();
        }
    },

    cancelQuery() {
        if (this.currentHistRequest) {
            this.currentHistRequest.abort();
            this.currentHistRequest = null;
        }
        if (this.currentRequest) {
            this.currentRequest.abort();
            this.currentRequest = null;
            this.currentCursor = null;
            this._streamingActive = false;
            this._endLoadingIndicator();
            this._updateLoadMoreButton(false);
            const elements = this.getElements();
            if (elements.resultsTable) elements.resultsTable.innerHTML = '';
            if (elements.resultsCount) elements.resultsCount.textContent = 'Query cancelled';
            if (window.Toast) Toast.show('Query cancelled', 'info');
        } else {
            // No in-flight request (button raced an already-finished query): just reset.
            this._endLoadingIndicator();
        }
    },

    _updateLoadMoreButton(hasMore) {
        let container = document.getElementById('loadMoreContainer');
        if (!container) {
            const anchor = document.getElementById('paginationControls');
            if (!anchor) return;
            container = document.createElement('div');
            container.id = 'loadMoreContainer';
            anchor.insertAdjacentElement('afterend', container);
        }
        if (hasMore) {
            container.innerHTML = '<button class="load-more-btn" onclick="QueryExecutor.loadMore()">Load more</button>';
            container.style.display = 'block';
        } else {
            container.style.display = 'none';
            container.innerHTML = '';
        }
    },

    async loadMore() {
        if (!this.currentCursor || this.loadingMore) return;
        this.loadingMore = true;

        const btn = document.querySelector('#loadMoreContainer .load-more-btn');
        if (btn) { btn.textContent = 'Loading…'; btn.disabled = true; }

        const fractalIdAtStart = window.FractalContext?.currentFractal?.id || null;

        try {
            const requestBody = {
                query: this.currentQuery,
                start: this.currentTimeRange.start,
                end: this.currentTimeRange.end,
                cursor: this.currentCursor
            };
            if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
                requestBody.fractal_id = window.FractalContext.currentFractal.id;
            }

            const data = await HttpUtils.safeFetch('/api/v1/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody)
            });

            if (fractalIdAtStart !== (window.FractalContext?.currentFractal?.id || null)) return;

            if (!data.success) {
                if (btn) { btn.textContent = 'Load more'; btn.disabled = false; }
                return;
            }

            const newResults = data.results || [];
            this.currentResults = [...this.currentResults, ...newResults];
            this.currentCursor = data.next_cursor || null;

            if (window.Pagination) {
                Pagination.allResults = this.currentResults;
                Pagination.totalResults = this.currentResults.length;
                Pagination.updateDisplay();
            }

            const elements = this.getElements();
            if (elements.resultsCount) {
                const suffix = data.has_more ? '+' : '';
                elements.resultsCount.textContent = `${this.currentResults.length.toLocaleString()}${suffix} results`;
            }

            this._updateLoadMoreButton(data.has_more);

            if (window.FieldStats) FieldStats.refresh();

        } catch (error) {
            if (error.name === 'AbortError') return;
            console.error('Load more failed:', error);
            const b = document.querySelector('#loadMoreContainer .load-more-btn');
            if (b) { b.textContent = 'Load more'; b.disabled = false; }
        } finally {
            this.loadingMore = false;
        }
    },

    _buildProfileText(profile) {
        // Normalise coordinator field to a plain boolean for clean JSON output.
        const clean = {
            query_id: profile.query_id,
            shards: (profile.shards || []).map(r => ({
                shard:          r.shard,
                coordinator:    r.coordinator == 1 || r.coordinator === '1',
                duration_ms:    Number(r.duration_ms),
                read_bytes:     r.read_bytes,
                read_rows:      Number(r.read_rows),
                parts_scanned:  Number(r.parts_scanned),
                marks_selected: Number(r.marks_selected),
                marks_skipped:  Number(r.marks_skipped),
                rows_surviving: Number(r.rows_surviving),
                file_opens:     Number(r.file_opens),
                disk_ms:        Number(r.disk_ms),
                net_wait_ms:    Number(r.net_wait_ms),
                bytes_from_disk: r.bytes_from_disk,
            })),
        };
        if (profile.skip_index && profile.skip_index.length > 0) {
            clean.skip_index = profile.skip_index.map(r => ({
                shard:               r.shard,
                marks_read:          Number(r.marks_read),
                marks_skipped:       Number(r.marks_skipped),
                total_marks:         Number(r.total_marks),
                pct_marks_surviving: r.pct_marks_surviving != null ? Number(Number(r.pct_marks_surviving).toFixed(1)) : 0,
            }));
        }
        return JSON.stringify(clean, null, 2);
    },

    renderProfilePanel(profile) {
        const panel = document.getElementById('profilePanel');
        if (!panel) return;

        const fmtNum = n => (n !== undefined && n !== null) ? Number(n).toLocaleString() : '—';
        const esc = s => String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;');

        // Shorten long pod/hostname to last segment for readability
        const shortHost = h => {
            const s = String(h || '');
            return s.length > 32 ? '…' + s.slice(-32) : s;
        };

        panel._profileData = profile;

        let html = `<div class="profile-header">
  <div class="profile-section-label">Per-shard profile &nbsp;<span class="profile-query-id">${esc(profile.query_id)}</span></div>
  <button class="toggle-sql-btn profile-copy-btn" id="profileCopyBtn">Copy</button>
</div>`;

        if (!profile.shards || profile.shards.length === 0) {
            html += `<p class="profile-empty">No shard data found in query_log (logging may be disabled or entry not yet flushed).</p>`;
        } else {
            html += `<table class="profile-table">
<thead><tr>
  <th>Shard</th><th>Coord</th><th>Duration</th><th>Read</th><th>Rows</th>
  <th>Parts</th><th>Marks✓</th><th>Marks✗</th><th>Rows Out</th>
  <th>Files</th><th>Disk ms</th><th>Net ms</th><th>Disk Bytes</th>
</tr></thead><tbody>`;
            for (const r of profile.shards) {
                const coord = r.coordinator == 1 || r.coordinator === '1';
                html += `<tr class="${coord ? 'profile-coordinator' : ''}">
  <td title="${esc(r.shard)}">${esc(shortHost(r.shard))}</td>
  <td>${coord ? '✓' : ''}</td>
  <td>${fmtNum(r.duration_ms)}</td>
  <td>${esc(r.read_bytes)}</td>
  <td>${fmtNum(r.read_rows)}</td>
  <td>${fmtNum(r.parts_scanned)}</td>
  <td>${fmtNum(r.marks_selected)}</td>
  <td>${fmtNum(r.marks_skipped)}</td>
  <td>${fmtNum(r.rows_surviving)}</td>
  <td>${fmtNum(r.file_opens)}</td>
  <td>${fmtNum(r.disk_ms)}</td>
  <td>${fmtNum(r.net_wait_ms)}</td>
  <td>${esc(r.bytes_from_disk)}</td>
</tr>`;
            }
            html += '</tbody></table>';
        }

        if (profile.skip_index && profile.skip_index.length > 0) {
            html += `<div class="profile-section-label" style="margin-top:0.75rem;">Skip index effectiveness</div>
<table class="profile-table">
<thead><tr><th>Shard</th><th>Marks Read</th><th>Marks Skipped</th><th>Total</th><th>% Surviving</th></tr></thead>
<tbody>`;
            for (const r of profile.skip_index) {
                html += `<tr>
  <td title="${esc(r.shard)}">${esc(shortHost(r.shard))}</td>
  <td>${fmtNum(r.marks_read)}</td>
  <td>${fmtNum(r.marks_skipped)}</td>
  <td>${fmtNum(r.total_marks)}</td>
  <td>${r.pct_marks_surviving != null ? Number(r.pct_marks_surviving).toFixed(1) + '%' : '—'}</td>
</tr>`;
            }
            html += '</tbody></table>';
        }

        panel.innerHTML = html;
        panel.style.display = 'block';

        const copyBtn = document.getElementById('profileCopyBtn');
        if (copyBtn) {
            copyBtn.addEventListener('click', () => this._copyProfile(copyBtn));
        }
    },

    _copyProfile(btn) {
        const panel = document.getElementById('profilePanel');
        if (!panel || !panel._profileData) return;
        const text = this._buildProfileText(panel._profileData);
        navigator.clipboard.writeText(text).then(() => {
            const orig = btn.textContent;
            btn.textContent = 'Copied!';
            setTimeout(() => { btn.textContent = orig; }, 1500);
        }).catch(() => {
            if (window.Toast) Toast.show('Copy failed — clipboard unavailable', 'error');
        });
    },

    getTimeRange() {
        if (window.TimePicker) return TimePicker.getTimeRange();
        return {
            start: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
            end: new Date().toISOString()
        };
    },

    _getTimeRangeStorageKey(suffix) {
        // FractalContext is the canonical source (updated by both listing and selector paths)
        const fractalId = window.FractalContext?.currentFractal?.id
            || window.FractalSelector?.getCurrentFractalId?.()
            || 'default';
        return `bifract_${suffix}_${fractalId}`;
    },

    saveTimeRangeToStorage(rangeType, timeRange) {
        if (window.TimePicker) TimePicker.saveToStorage();
    },

    restoreTimeRangeFromStorage() {
        if (window.TimePicker) TimePicker.restoreFromStorage();
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
        } else if (results.length > 0) {
            // Fall back to extracting from first result
            const firstResult = results[0];
            for (const key of Object.keys(firstResult)) {
                if (key !== '_all_fields' && key !== 'fractal_id') {
                    fields.push(key);
                }
            }
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
        } else if (!this.isAggregated) {
            // Default source-event column order: timestamp, log_id, raw_log, then rest
            const priority = ['timestamp', 'log_id', 'raw_log'];
            const prioritized = priority.filter(f => fields.includes(f));
            const rest = fields.filter(f => !priority.includes(f));
            fields = [...prioritized, ...rest];
        }

        const hasRawLog = fields.includes('raw_log');

        // Hide log_id from the display when raw_log is present (default source-event view).
        // log_id stays in the row data so the detail pane can still fetch by it.
        if (hasRawLog) fields = fields.filter(f => f !== 'log_id');

        // Build table with sortable headers
        const numericFields = new Set(fields.filter(field =>
            results.length > 0 && results.every(r => {
                const v = r[field];
                return v !== undefined && v !== null && v !== '' && !isNaN(Number(v));
            })
        ));

        let html = '<table class="results-table"><thead><tr>';
        fields.forEach(field => {
            const displayName = field;
            const sortIcon = this.sortColumn === field
                ? (this.sortDirection === 'asc' ? ' ▲' : ' ▼')
                : '';
            const width = this.columnWidths[field] ? `style="width: ${this.columnWidths[field]}px"` : '';
            const numClass = numericFields.has(field) ? ' numeric-col' : (field === 'raw_log' ? ' raw-log-col' : '');
            html += `<th class="sortable${numClass}" data-field="${Utils.escapeAttr(field)}" ${width}>${Utils.escapeHtml(displayName)}${sortIcon}<div class="column-resizer"></div></th>`;
        });
        html += (hasRawLog ? '' : '<th class="filler-col"></th>') + '</tr></thead><tbody>';

        results.forEach((result, index) => {
            // Check if this log has comments
            const hasComments = window.Comments && Comments.hasComments(result);
            const rowClass = hasComments ? 'result-row has-comments' : 'result-row';

            html += '<tr class="' + rowClass + '" data-index="' + index + '">';
            fields.forEach(field => {
                let value = result[field];
                let cellClass = field === 'timestamp' ? 'timestamp-cell'
                    : field === 'raw_log' ? 'raw-log-col'
                    : (numericFields.has(field) ? 'numeric-col' : '');

                if (typeof value === 'object' && value !== null) {
                    const jsonStr = JSON.stringify(value);
                    value = `<span class="json-value json-unhighlighted">${Utils.escapeHtml(jsonStr)}</span>`;
                    cellClass += ' json-cell';
                } else if (value === undefined || value === null) {
                    value = '-';
                    cellClass += ' null-cell';
                } else if (typeof value === 'string' && (value.startsWith('{') || value.startsWith('['))) {
                    value = `<span class="json-value json-unhighlighted">${Utils.escapeHtml(value)}</span>`;
                    cellClass += ' json-cell';
                } else {
                    value = Utils.escapeHtml(String(value));
                }

                html += `<td class="${cellClass}">${value}</td>`;
            });
            html += (hasRawLog ? '' : '<td class="filler-col"></td>') + '</tr>';
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
                        log_id: logData.log_id,
                        fractal_id: logData.fractal_id,
                        _shard_num: logData._shard_num
                    };
                }

                if (window.LogDetail) {
                    document.querySelectorAll('.result-row.selected').forEach(r => r.classList.remove('selected'));
                    row.classList.add('selected');
                    LogDetail.setContext(results, index, this.isAggregated);
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
                if (e.target.classList.contains('column-resizer')) return;
                this.sortByColumn(header.dataset.field);
            });
        }

        // Add column resizing handlers
        this.setupColumnResizing();


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

    sortByColumn(field) {
        // Rows are still streaming in (pre-sorted newest-first); defer sorting
        // until the result set is complete to avoid scrambling partial data.
        if (this._streamingActive) {
            if (window.Toast) Toast.show('Results still loading, sort available when complete', 'info');
            return;
        }
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

    renderTableError(message) {
        const resultsTable = document.getElementById('resultsTable');
        if (!resultsTable) return;
        this._queryHadError = true;
        this._outputTypeStatus('error');
        const safe = message ? Utils.escapeHtml(String(message)) : 'Query failed';
        resultsTable.innerHTML = `<div class="results-error"><span class="results-error-icon">⚠</span><span>${safe}</span></div>`;
        const chartContainer = document.getElementById('chartContainer');
        if (chartContainer) chartContainer.style.display = 'none';
    },

    showError(message, errorType) {
        // Parse/translate errors point at a position in the BQL the user just
        // typed, so render them persistently under the editor rather than in an
        // auto-dismissing toast they'd lose while fixing the query. Execution and
        // timeout errors are not tied to the cursor, so a toast is fine.
        if (errorType === 'parse' || errorType === 'translate') {
            this.showQueryError(message);
        } else if (window.Toast) {
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
        const wrapBtn = document.getElementById('wrapToggleBtn');
        if (wrapBtn) {
            wrapBtn.style.display = 'none';
        }
    },

    showQueryError(message) {
        const el = document.getElementById('queryError');
        if (!el) {
            // Fall back to a toast if the inline element is missing.
            if (window.Toast) Toast.error('Query Error', message);
            return;
        }
        el.innerHTML = '';
        const text = document.createElement('span');
        text.className = 'query-error-text';
        text.textContent = message;
        const dismiss = document.createElement('button');
        dismiss.className = 'query-error-dismiss';
        dismiss.type = 'button';
        dismiss.setAttribute('aria-label', 'Dismiss error');
        dismiss.textContent = '×';
        dismiss.onclick = () => this.clearQueryError();
        el.appendChild(text);
        el.appendChild(dismiss);
        el.style.display = 'flex';
    },

    clearQueryError() {
        const el = document.getElementById('queryError');
        if (el) {
            el.style.display = 'none';
            el.innerHTML = '';
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
                if (key !== '_all_fields' && key !== 'fractal_id') {
                    fields.push(key);
                }
            }
        }

        // Build table with sortable headers
        const numericFields = new Set(fields.filter(field =>
            results.length > 0 && results.every(r => {
                const v = r[field];
                return v !== undefined && v !== null && v !== '' && !isNaN(Number(v));
            })
        ));

        let html = '<table class="results-table"><thead><tr>';
        fields.forEach(field => {
            const numClass = numericFields.has(field) ? ' numeric-col' : '';
            html += `<th class="sortable${numClass}" data-field="${Utils.escapeAttr(field)}">${Utils.escapeHtml(field)}<div class="column-resizer"></div></th>`;
        });
        html += '<th class="filler-col"></th></tr></thead><tbody>';

        results.forEach((result, index) => {
            const hasComments = window.Comments && Comments.hasComments(result);
            const rowClass = hasComments ? 'result-row has-comments' : 'result-row';

            html += '<tr class="' + rowClass + '" data-index="' + index + '">';
            fields.forEach(field => {
                let value = result[field];
                let cellClass = field === 'timestamp' ? 'timestamp-cell' : (numericFields.has(field) ? 'numeric-col' : '');

                if (typeof value === 'object' && value !== null) {
                    const jsonStr = JSON.stringify(value);
                    value = `<span class="json-value json-unhighlighted">${Utils.escapeHtml(jsonStr)}</span>`;
                    cellClass += ' json-cell';
                } else if (value === undefined || value === null) {
                    value = '-';
                    cellClass += ' null-cell';
                } else if (typeof value === 'string' && (value.startsWith('{') || value.startsWith('['))) {
                    value = `<span class="json-value json-unhighlighted">${Utils.escapeHtml(value)}</span>`;
                    cellClass += ' json-cell';
                } else {
                    value = Utils.escapeHtml(String(value));
                }

                html += `<td class="${cellClass}">${value}</td>`;
            });
            html += '<td class="filler-col"></td></tr>';
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
                                log_id: logData.log_id,
                                fractal_id: logData.fractal_id,
                                _shard_num: logData._shard_num
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

        // Highlight keys
        highlighted = highlighted.replace(/("(?:[^"\\]|\\.)*")\s*:/g, '<span class="json-key">$1</span><span class="json-punct">:</span>');

        // Highlight string values
        highlighted = highlighted.replace(/(<span class="json-punct">:<\/span>)\s*("(?:[^"\\]|\\.)*")/g, '$1 <span class="json-string">$2</span>');

        // Highlight numbers
        highlighted = highlighted.replace(/(<span class="json-punct">:<\/span>)\s*(-?\d+\.?\d*)/g, '$1 <span class="json-number">$2</span>');

        // Highlight booleans and null
        highlighted = highlighted.replace(/(<span class="json-punct">:<\/span>)\s*(true|false|null)/g, '$1 <span class="json-boolean">$2</span>');

        // Mute commas and brackets
        highlighted = highlighted.replace(/,/g, '<span class="json-punct">,</span>');
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

            // If we have pending share data from fractal switch, process it now
            if (this.pendingShareData) {
                this.loadShareDataIntoUI(this.pendingShareData);
                this.pendingShareData = null;
                return;
            }

            // Check for deferred share links now that data may be loaded
            if (this.deferredShareLink) {
                let hasData;
                if (this.deferredShareLink.isPrismShare) {
                    hasData = (window.FractalSelector?.availablePrisms?.length > 0) ||
                              (window.FractalListing?.prisms?.length > 0);
                } else {
                    hasData = (window.FractalSelector?.availableFractals?.length > 0) ||
                              (window.FractalListing?.fractals?.length > 0);
                }

                if (hasData) {
                    this.processDeferredShareLink();
                    return;
                }
            }

            // Check for shared links on fractal change if URL has share params
            const urlHasShareParams = window.location.search && new URLSearchParams(window.location.search).has('q');
            if (!this.hasLoadedShareLink || urlHasShareParams) {
                this.hasLoadedShareLink = true;
                const loadedShareLink = this.loadFromShareLink();
                if (loadedShareLink) {
                    return; // Share link was processed, stop here
                }
            }

            // Only attempt to execute or load logs if we're in the search view
            const searchView = document.getElementById('searchView');
            if (!searchView || searchView.style.display === 'none') {
                return;
            }

            // Check if we're in the search view and elements are available
            if (!elements.queryInput || !elements.resultsTable) {
                if (retryCount < 5) { // Max 5 retries (1 second total)
                    // Retry after view has had time to initialize
                    setTimeout(() => this.onFractalChange(retryCount + 1), 200);
                    return;
                } else {
                    return;
                }
            }

            // If we have a current query, re-execute it for the new fractal
            if (elements.queryInput && elements.queryInput.value.trim()) {
                this.execute();
            } else if (!this.isProcessingSharedQuery) {
                // If no query is present and no shared query is loading, load recent logs sample
                this.loadRecentLogsSample();
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

    toggleWrap() {
        const container = document.getElementById('resultsTable');
        const btn = document.getElementById('wrapToggleBtn');
        if (!container || !btn) return;
        const active = container.classList.toggle('table-wrap');
        btn.classList.toggle('active', active);
    },

    toggleFullscreen() {
        const isFs = document.body.classList.toggle('results-fullscreen');
        const btn = document.getElementById('fullscreenBtn');
        if (btn) {
            btn.querySelector('.fs-expand-icon').style.display = isFs ? 'none' : '';
            btn.querySelector('.fs-compress-icon').style.display = isFs ? '' : 'none';
        }
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
            const trState = window.TimePicker?.state || { type: '24h' };
            const timeRangeValue = trState.type;

            // Get current fractal/prism ID
            const ctx = window.FractalContext;
            const contextId = ctx?.currentFractal?.id;
            if (!contextId) {
                console.error('[Share] No fractal or prism selected');
                return;
            }

            // Build URL parameters
            const urlParams = new URLSearchParams();
            urlParams.set('q', btoa(encodeURIComponent(rawQuery)));
            urlParams.set('tr', timeRangeValue);
            if (ctx.isPrism()) {
                urlParams.set('p', contextId);
            } else {
                urlParams.set('f', contextId);
            }

            if (timeRangeValue === 'custom' && trState.customStart && trState.customEnd) {
                urlParams.set('ts', trState.customStart);
                urlParams.set('te', trState.customEnd);
            } else if (timeRangeValue === 'relative') {
                urlParams.set('rn', String(trState.relativeN || 4));
                urlParams.set('ru', trState.relativeUnit || 'hours');
            }

            // Generate full URL
            const shareUrl = `${window.location.origin}${window.location.pathname}?${urlParams.toString()}`;

            // Copy to clipboard
            await navigator.clipboard.writeText(shareUrl);

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
        // First, check if we even have a search string to avoid unnecessary processing
        if (!window.location.search) {
            return false;
        }

        const urlParams = new URLSearchParams(window.location.search);

        // Check if we have share parameters - be very explicit about this check
        const hasQuery = urlParams.has('q');
        const hasTimeRange = urlParams.has('tr');
        const hasFractal = urlParams.has('f');
        const hasPrism = urlParams.has('p');

        if (!hasQuery || !hasTimeRange || (!hasFractal && !hasPrism)) {
            return false; // No share parameters found
        }


        // Set flag to prevent clearing shared state during processing
        this.isProcessingSharedQuery = true;

        try {
            const encodedQuery = urlParams.get('q');
            const timeRangeValue = urlParams.get('tr');
            const fractalId = urlParams.get('f');
            const prismId = urlParams.get('p');
            const contextId = fractalId || prismId;
            const isPrismShare = !!prismId;

            // Validate the parameters before proceeding
            if (!encodedQuery || !timeRangeValue || !contextId) {
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

            // Check if the relevant data source (fractals or prisms) is loaded
            let hasData = false;
            try {
                if (isPrismShare) {
                    const selectorPrisms = window.FractalSelector?.availablePrisms?.length || 0;
                    const listingPrisms = window.FractalListing?.prisms?.length || 0;
                    hasData = selectorPrisms > 0 || listingPrisms > 0;
                } else {
                    const selectorFractals = window.FractalSelector?.availableFractals?.length || 0;
                    const listingFractals = window.FractalListing?.fractals?.length || 0;
                    hasData = selectorFractals > 0 || listingFractals > 0;
                }
            } catch (selectorError) {
                console.warn('[Share] Error checking availability:', selectorError);
                hasData = false;
            }

            if (!hasData) {
                // Store the share link data to be processed when fractals are loaded
                this.deferredShareLink = { encodedQuery, timeRangeValue, fractalId: contextId, isPrismShare, relativeN: urlParams.get('rn'), relativeUnit: urlParams.get('ru') };

                // Start periodic check for fractals loading
                this.startDeferredShareLinkPolling();
                return true; // We are processing a share link, just deferred
            }

            // Check if user has access to the shared fractal/prism

            let hasAccess = null;
            try {
                if (isPrismShare) {
                    // Check prism access in FractalSelector and FractalListing
                    if (window.FractalSelector?.availablePrisms?.length > 0) {
                        hasAccess = window.FractalSelector.availablePrisms.find(p => p && p.id === contextId);
                    }
                    if (!hasAccess && window.FractalListing?.prisms?.length > 0) {
                        hasAccess = window.FractalListing.prisms.find(p => p && p.id === contextId);
                    }
                } else {
                    // Check fractal access in FractalSelector and FractalListing
                    if (window.FractalSelector?.availableFractals?.length > 0) {
                        hasAccess = window.FractalSelector.availableFractals.find(f => f && f.id === contextId);
                    }
                    if (!hasAccess && window.FractalListing?.fractals?.length > 0) {
                        hasAccess = window.FractalListing.fractals.find(f => f && f.id === contextId);
                    }
                }
            } catch (accessError) {
                console.error('[Share] Error checking access:', accessError);
                this.showError('Failed to verify access: ' + accessError.message);
                return false;
            }

            if (!hasAccess) {
                console.error('[Share] User does not have access to shared', isPrismShare ? 'prism' : 'fractal', ':', contextId);
                this.showError('Access denied: You do not have permission to view this shared query');
                return false;
            }

            // Switch to the shared fractal/prism if it's not current
            if (!window.FractalContext?.currentFractal || window.FractalContext.currentFractal.id !== contextId) {

                // Store the shared link data to be processed after context switch
                this.pendingShareData = {
                    query,
                    timeRangeValue,
                    customStart: urlParams.get('ts'),
                    customEnd: urlParams.get('te'),
                    relativeN: urlParams.get('rn'),
                    relativeUnit: urlParams.get('ru')
                };

                if (isPrismShare) {
                    // Use prism selection methods
                    if (window.FractalContext && typeof window.FractalContext.setCurrentPrism === 'function') {
                        window.FractalContext.setCurrentPrism(hasAccess);
                        return true;
                    } else if (window.FractalSelector && typeof window.FractalSelector.selectPrism === 'function') {
                        window.FractalSelector.selectPrism(contextId);
                        return true;
                    } else {
                        console.error('[Share] No prism selection method available');
                        this.showError('Unable to switch to shared prism');
                        return false;
                    }
                }

                // Fractal selection methods
                if (window.FractalContext && typeof window.FractalContext.setCurrentFractal === 'function') {
                    window.FractalContext.setCurrentFractal(hasAccess);
                    return true;
                } else if (window.FractalSelector && typeof window.FractalSelector.setCurrentFractal === 'function') {
                    window.FractalSelector.setCurrentFractal(hasAccess);
                    return true;
                } else if (window.FractalSelector && typeof window.FractalSelector.selectFractal === 'function') {
                    window.FractalSelector.selectFractal(contextId, hasAccess.name);
                    return true;
                } else if (window.FractalContext && typeof window.FractalContext.selectFractalOnServer === 'function') {
                    window.FractalContext.currentFractal = hasAccess;
                    window.FractalContext.selectFractalOnServer(contextId);
                    return true;
                } else {
                    console.error('[Share] No fractal selection method available');
                    this.showError('Unable to switch to shared fractal');
                    return false;
                }
            }


            // Load the shared data into UI
            this.loadShareDataIntoUI({
                query,
                timeRangeValue,
                customStart: urlParams.get('ts'),
                customEnd: urlParams.get('te'),
                relativeN: urlParams.get('rn'),
                relativeUnit: urlParams.get('ru')
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
        const { query, timeRangeValue, customStart, customEnd, relativeN, relativeUnit } = shareData;

        // Set flag to prevent clearing shared state during processing
        this.isProcessingSharedQuery = true;

        // Navigate to the search view within the fractal
        if (window.App && typeof window.App.showFractalView === 'function') {
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
            if (window.TimePicker) {
                const newState = { type: timeRangeValue };
                if (timeRangeValue === 'custom' && customStart && customEnd) {
                    newState.customStart = customStart;
                    newState.customEnd = customEnd;
                    const absStart = document.getElementById('tpAbsStart');
                    const absEnd = document.getElementById('tpAbsEnd');
                    if (absStart) absStart.value = TimePicker._toDatetimeLocal(customStart);
                    if (absEnd) absEnd.value = TimePicker._toDatetimeLocal(customEnd);
                } else if (timeRangeValue === 'relative' && relativeN) {
                    newState.relativeN = parseInt(relativeN, 10);
                    newState.relativeUnit = relativeUnit || 'hours';
                    const nEl = document.getElementById('tpRelativeN');
                    const unitEl = document.getElementById('tpRelativeUnit');
                    if (nEl) nEl.value = newState.relativeN;
                    if (unitEl) unitEl.value = newState.relativeUnit;
                }
                TimePicker.setState(newState, true);
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
                    // Now clear URL parameters after everything is loaded
                    if (window.location.search) {
                        const urlParams = new URLSearchParams(window.location.search);
                        if (urlParams.has('q') || urlParams.has('tr') || urlParams.has('f') || urlParams.has('p')) {
                            const cleanUrl = `${window.location.origin}${window.location.pathname}`;
                            window.history.replaceState({}, document.title, cleanUrl);
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
            const isPrism = this.deferredShareLink?.isPrismShare;

            let hasData = false;
            try {
                if (isPrism) {
                    const selectorPrisms = window.FractalSelector?.availablePrisms?.length || 0;
                    const listingPrisms = window.FractalListing?.prisms?.length || 0;
                    hasData = selectorPrisms > 0 || listingPrisms > 0;
                } else {
                    const selectorFractals = window.FractalSelector?.availableFractals?.length || 0;
                    const listingFractals = window.FractalListing?.fractals?.length || 0;
                    hasData = selectorFractals > 0 || listingFractals > 0;
                }
            } catch (checkError) {
                console.warn('[Share] Error during polling check:', checkError);
                hasData = false;
            }

            if (hasData) {
                clearInterval(this.deferredPollingInterval);
                this.deferredPollingInterval = null;
                this.processDeferredShareLink();
            } else if (attempts >= maxAttempts) {
                console.error('[Share] Timeout waiting for data to load');
                clearInterval(this.deferredPollingInterval);
                this.deferredPollingInterval = null;
                this.deferredShareLink = null;
                this.showError('Failed to load shared query: timeout waiting for data');
            }
        }, 500);
    },

    // Check for deferred share links when fractals are loaded
    checkDeferredShareLink() {
        if (this.deferredShareLink) {
            let hasData;
            if (this.deferredShareLink.isPrismShare) {
                hasData = (window.FractalSelector?.availablePrisms?.length > 0) ||
                          (window.FractalListing?.prisms?.length > 0);
            } else {
                hasData = (window.FractalSelector?.availableFractals?.length > 0) ||
                          (window.FractalListing?.fractals?.length > 0);
            }

            if (hasData) {
                this.processDeferredShareLink();
            }
        }
    },

    // Process deferred share link once fractals are loaded
    processDeferredShareLink() {
        if (!this.deferredShareLink) {
            return;
        }

        const { encodedQuery, timeRangeValue, fractalId, isPrismShare, relativeN, relativeUnit } = this.deferredShareLink;

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

            // Now fractals/prisms should be loaded, check access
            let hasAccess = null;
            try {
                if (isPrismShare) {
                    if (window.FractalSelector?.availablePrisms?.length > 0) {
                        hasAccess = window.FractalSelector.availablePrisms.find(p => p && p.id === fractalId);
                    }
                    if (!hasAccess && window.FractalListing?.prisms?.length > 0) {
                        hasAccess = window.FractalListing.prisms.find(p => p && p.id === fractalId);
                    }
                } else {
                    if (window.FractalSelector?.availableFractals?.length > 0) {
                        hasAccess = window.FractalSelector.availableFractals.find(f => f && f.id === fractalId);
                    }
                    if (!hasAccess && window.FractalListing?.fractals?.length > 0) {
                        hasAccess = window.FractalListing.fractals.find(f => f && f.id === fractalId);
                    }
                }
            } catch (accessCheckError) {
                console.error('[Share] Error checking deferred access:', accessCheckError);
                this.showError('Failed to verify access: ' + accessCheckError.message);
                return;
            }

            if (!hasAccess) {
                console.error('[Share] User does not have access to deferred', isPrismShare ? 'prism' : 'fractal', ':', fractalId);
                this.showError('Access denied: You do not have permission to view this shared query');
                return;
            }

            // Switch to the shared fractal/prism if it's not current
            if (!window.FractalContext?.currentFractal || window.FractalContext.currentFractal.id !== fractalId) {

                // Store the shared link data to be processed after context switch
                this.pendingShareData = {
                    query,
                    timeRangeValue,
                    customStart: null,
                    customEnd: null,
                    relativeN: relativeN || null,
                    relativeUnit: relativeUnit || null
                };

                if (isPrismShare) {
                    if (window.FractalContext && typeof window.FractalContext.setCurrentPrism === 'function') {
                        window.FractalContext.setCurrentPrism(hasAccess);
                        return;
                    } else if (window.FractalSelector && typeof window.FractalSelector.selectPrism === 'function') {
                        window.FractalSelector.selectPrism(fractalId);
                        return;
                    } else {
                        console.error('[Share] No prism selection method available for deferred processing');
                        this.showError('Unable to switch to shared prism');
                        return;
                    }
                }

                if (window.FractalContext && typeof window.FractalContext.setCurrentFractal === 'function') {
                    window.FractalContext.setCurrentFractal(hasAccess);
                    return;
                } else if (window.FractalSelector && typeof window.FractalSelector.setCurrentFractal === 'function') {
                    window.FractalSelector.setCurrentFractal(hasAccess);
                    return;
                } else if (window.FractalSelector && typeof window.FractalSelector.selectFractal === 'function') {
                    window.FractalSelector.selectFractal(fractalId, hasAccess.name);
                    return;
                } else if (window.FractalContext && typeof window.FractalContext.selectFractalOnServer === 'function') {
                    window.FractalContext.currentFractal = hasAccess;
                    window.FractalContext.selectFractalOnServer(fractalId);
                    return;
                } else if (window.FractalListing && typeof window.FractalListing.selectFractal === 'function') {
                    window.FractalListing.selectFractal(fractalId);
                    return;
                } else {
                    console.error('[Share] No fractal selection method available for deferred processing');
                    this.showError('Unable to switch to shared fractal');
                    return;
                }
            }

            // Load directly if already in correct context
            this.loadShareDataIntoUI({
                query,
                timeRangeValue,
                customStart: null,
                customEnd: null,
                relativeN: relativeN || null,
                relativeUnit: relativeUnit || null
            });

        } catch (error) {
            console.error('[Share] Failed to process deferred share link:', error);
            this.isProcessingSharedQuery = false; // Clear flag on error
            this.showError('Failed to load shared query: ' + (error.message || 'Unknown error'));
        }
    },

    // Clear shared query state and URL parameters
    clearSharedQueryState() {

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
            if (urlParams.has('q') || urlParams.has('tr') || urlParams.has('f') || urlParams.has('p')) {
                const cleanUrl = `${window.location.origin}${window.location.pathname}`;
                window.history.replaceState({}, document.title, cleanUrl);
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
