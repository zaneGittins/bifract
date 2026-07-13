// Query execution and results rendering
const QueryExecutor = {
    currentResults: [],
    currentTimeRange: null,
    sortColumn: null,
    sortDirection: null,
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

    varManager: null,           // VariableManager for the search editor's @vars
    _pendingUrlVars: null,      // values from a share link, applied as vars appear

    // initVariables wires the auto-detected @variable tray under the search box:
    // scanning the query on every edit, seeding values from a share link, and
    // mirroring the current values into the URL so a copied link carries them.
    initVariables() {
        if (this.varManager || !window.VariableManager) return;
        const container = document.getElementById('searchVariables');
        this.varManager = new VariableManager({
            container,
            onChange: () => this._syncVarsToUrl(),
            onVarsChanged: () => this._syncVarsToUrl(),
        });
        this._pendingUrlVars = this._readVarsFromUrl();
        const qi = document.getElementById('queryInput');
        if (qi) {
            qi.addEventListener('input', () => this.syncSearchVariables());
            this.syncSearchVariables();
        }
    },

    // syncSearchVariables reconciles the tray against the current query text and
    // applies any pending share-link values to newly-surfaced variables.
    syncSearchVariables() {
        if (!this.varManager) return;
        const qi = document.getElementById('queryInput');
        // Detect against the executed text (comments stripped), so a @var that
        // only appears in a // comment line never becomes a phantom pill.
        this.varManager.syncFromText(this.stripComments(qi ? qi.value : ''));
        this._applyPendingUrlVars();
    },

    // Applies staged values (share link / saved query) to matching vars, once.
    // Pending values are single-shot: leftover entries are discarded so a stale
    // value cannot resurrect when the same @name is retyped later.
    _applyPendingUrlVars() {
        if (!this._pendingUrlVars || !this.varManager) return;
        let applied = false;
        for (const [name, val] of this._pendingUrlVars.entries()) {
            if (this.varManager.values.has(name)) {
                this.varManager.setValue(name, val);
                applied = true;
            }
        }
        this._pendingUrlVars = null;
        if (applied) { this.varManager.render(); this._syncVarsToUrl(); }
    },

    // seedVariables stages remembered values (e.g. from a saved query) so the
    // next tray reconcile applies them to matching @vars instead of "*".
    seedVariables(arr) {
        const map = new Map();
        if (Array.isArray(arr)) {
            for (const v of arr) if (v && v.name) map.set(v.name, v.value == null ? '*' : String(v.value));
        }
        this._pendingUrlVars = map;
    },

    // variablesPayload returns the [{name,value}] bindings for a request body, or
    // undefined when there are no variables (keeps payloads clean).
    variablesPayload() {
        if (!this.varManager || this.varManager.isEmpty()) return undefined;
        return this.varManager.serialize();
    },

    _readVarsFromUrl() {
        return this._decodeVars(new URLSearchParams(window.location.search).get('vars'));
    },

    // _decodeVars turns a base64(encodeURIComponent(JSON)) vars param into a
    // name->value Map. Malformed input yields an empty Map.
    _decodeVars(raw) {
        const map = new Map();
        try {
            if (raw) {
                const arr = JSON.parse(decodeURIComponent(atob(raw)));
                if (Array.isArray(arr)) {
                    for (const v of arr) if (v && v.name) map.set(v.name, v.value == null ? '*' : String(v.value));
                }
            }
        } catch (e) { /* malformed vars param: ignore */ }
        return map;
    },

    _encodeVars(arr) { return btoa(encodeURIComponent(JSON.stringify(arr))); },

    _syncVarsToUrl() {
        if (!this.varManager) return;
        try {
            const params = new URLSearchParams(window.location.search);
            const arr = this.varManager.serialize();
            // Only encode when the user has set a non-default value; an all-"*"
            // set carries no information and would needlessly pollute the URL.
            const meaningful = arr.some(v => v.value !== '*' && v.value !== '');
            if (meaningful) params.set('vars', this._encodeVars(arr));
            else params.delete('vars');
            const qs = params.toString();
            const url = window.location.origin + window.location.pathname + (qs ? '?' + qs : '') + window.location.hash;
            history.replaceState(history.state, '', url);
        } catch (e) { /* best-effort URL sync */ }
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

            if (window.FieldStats) FieldStats.onResults();

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

        if (window.LogDetail) LogDetail.close();

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
        const trState = window.TimePicker?.state || { type: '24h' };
        const trToken = trState.type || '24h';
        this._pendingHistory = {
            query: query,
            timeRange: trToken,
            customStart: trToken === 'custom' ? (this.currentTimeRange?.start || '') : '',
            customEnd: trToken === 'custom' ? (this.currentTimeRange?.end || '') : '',
            relativeN: trToken === 'relative' ? trState.relativeN : null,
            relativeUnit: trToken === 'relative' ? (trState.relativeUnit || '') : ''
        };

        // Hide previous results and show loading
        if (elements.errorDiv) elements.errorDiv.style.display = 'none';
        this.clearQueryError();
        const profilePanel = document.getElementById('profilePanel');
        if (profilePanel) { profilePanel.style.display = 'none'; profilePanel.innerHTML = ''; }

        // Reset chart/graph container so loading spinner is visible
        const chartContainer = document.getElementById('chartContainer');
        if (chartContainer) chartContainer.style.display = 'none';

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
            const _vars = this.variablesPayload();
            if (_vars) requestBody.variables = _vars;

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
                    this.showError(msg, data.error_type, data.error_pos);
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
                    this.showError(frame.error || 'Query failed', frame.error_type, frame.error_pos);
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
            piechart: 'Pie Chart', barchart: 'Bar Chart', graph: 'Graph', mesh: 'Mesh Network',
            pgraph: 'Provenance Graph',
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
        // The real pagination element is #paginationBar (page numbers + page-size buttons).
        const paginationEl = document.getElementById('paginationBar');
        if (this.chartType && this.chartType !== '') {
            // Charts/graphs (incl. pgraph) never paginate. Hide the bar that a prior table/log
            // query may have left visible; Pagination is not part of this render path.
            if (paginationEl) paginationEl.style.display = 'none';
            if (elements.resultsTable) elements.resultsTable.style.display = 'none';
            this.renderResults(this.currentResults);
        } else {
            if (elements.resultsTable) elements.resultsTable.style.display = 'block';
            // #paginationBar visibility is owned by Pagination.updateDisplay (grid when >1 page,
            // none otherwise) -- don't force it here or single-page results show an empty bar.
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

        if (window.FieldStats) FieldStats.onResults();

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
            const _vars = this.variablesPayload();
            if (_vars) requestBody.variables = _vars;
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

            if (window.FieldStats) FieldStats.onResults();

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

        // Layout key for this fractal + query shape (order-independent), used
        // for both persisted widths and persisted column order.
        const fractalId = this.currentFractalId || 'default';
        const sizingSig = ColumnSizing.signature(fields);

        // Hydrate any saved column order for this layout (once per result set;
        // cleared on fractal switch). A share-link/explicit order takes priority.
        if (this.columnOrder === null || this.columnOrder === undefined) {
            const savedOrder = ColumnSizing.loadOrder(fractalId, sizingSig);
            if (savedOrder && savedOrder.length) this.columnOrder = savedOrder;
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
            // Default source-event column order: timestamp, log_id, norm_log, then rest
            const priority = ['timestamp', 'log_id', 'norm_log'];
            const prioritized = priority.filter(f => fields.includes(f));
            const rest = fields.filter(f => !priority.includes(f));
            fields = [...prioritized, ...rest];
        }

        const hasNormLog = fields.includes('norm_log');

        // Hide log_id from the display when norm_log is present (default source-event view).
        // log_id stays in the row data so the detail pane can still fetch by it.
        if (hasNormLog) fields = fields.filter(f => f !== 'log_id');

        this._sizingFractalId = fractalId;
        this._sizingSig = sizingSig;
        this._displayFields = fields.slice();

        // Build via the shared core renderer with the full interaction set.
        const built = this.buildResultsTable(fields, results, {
            sizingKey: { fractalId, sig: sizingSig },
            features: { resize: true, reorder: true, sort: true },
            sortColumn: this.sortColumn,
            sortDirection: this.sortDirection,
            rowClass: (row) => (window.Comments && Comments.hasComments(row)) ? 'has-comments' : '',
            onSort: (field) => this.sortByColumn(field),
            onReorder: (field, targetIndex) => this._applyReorder(field, targetIndex),
            onRowClick: (row, index, rowEl) => {
                let detailData = row;
                if (row._all_fields && typeof row._all_fields === 'object') {
                    detailData = {
                        ...row._all_fields,
                        timestamp: row.timestamp,
                        log_id: row.log_id,
                        fractal_id: row.fractal_id,
                        _shard_num: row._shard_num
                    };
                }
                if (window.LogDetail) {
                    document.querySelectorAll('.result-row.selected').forEach(r => r.classList.remove('selected'));
                    rowEl.classList.add('selected');
                    LogDetail.setContext(results, index, this.isAggregated, 'search');
                    LogDetail.show(detailData, this.isAggregated, 'search');
                }
            },
        });

        resultsTable.innerHTML = built.html;
        built.mount(resultsTable);
    },

    // ---- Shared results-table renderer -------------------------------------
    // Single source of truth for every results table (main search, alert/model
    // editors, notebooks, dashboards). Given a final ordered `fields` list and
    // `results`, returns { html, mount(root) }: insert `html` wherever, then call
    // mount(root) on the inserted container to wire sizing/resize/reorder/sort/
    // row-click and lazy JSON highlighting. Surface-specific behaviour is
    // supplied through hooks rather than forked implementations.
    _tableSeq: 0,

    _computeNumericFields(fields, results) {
        return new Set(fields.filter(field =>
            results.length > 0 && results.every(r => {
                const v = r[field];
                return v !== undefined && v !== null && v !== '' && !isNaN(Number(v));
            })
        ));
    },

    _defaultCellHTML(field, value, numericFields) {
        // norm_log is the serialized normalized fields; drop empty values so the scan
        // column stays readable (the detail grid does the precise type-hint filtering).
        if (field === 'norm_log' && typeof value === 'string' && value.charCodeAt(0) === 123) {
            try {
                const o = JSON.parse(value);
                for (const k of Object.keys(o)) { if (o[k] === '' || o[k] === null) delete o[k]; }
                value = JSON.stringify(o);
            } catch (e) { /* leave as-is on parse failure */ }
        }
        let cellClass = field === 'timestamp' ? 'timestamp-cell'
            : field === 'norm_log' ? 'raw-log-col'
            : (numericFields.has(field) ? 'numeric-col' : '');
        let html;
        if (typeof value === 'object' && value !== null) {
            html = `<span class="json-value json-unhighlighted">${Utils.escapeHtml(JSON.stringify(value))}</span>`;
            cellClass += ' json-cell';
        } else if (value === undefined || value === null) {
            html = '-';
            cellClass += ' null-cell';
        } else if (typeof value === 'string' && (value.startsWith('{') || value.startsWith('['))) {
            html = `<span class="json-value json-unhighlighted">${Utils.escapeHtml(value)}</span>`;
            cellClass += ' json-cell';
        } else {
            html = Utils.escapeHtml(String(value));
        }
        return { html, cellClass };
    },

    buildResultsTable(fields, results, opts = {}) {
        const features = Object.assign({ resize: true, reorder: false, sort: false }, opts.features || {});
        const rows = (opts.maxRows && results.length > opts.maxRows) ? results.slice(0, opts.maxRows) : results;
        // Detect numeric columns and sample widths over the rows we actually
        // render (not the full result set), so capped tables stay cheap.
        const numericFields = opts.numeric === false ? new Set() : this._computeNumericFields(fields, rows);
        const fractalId = (opts.sizingKey && opts.sizingKey.fractalId) || 'default';
        const sig = (opts.sizingKey && opts.sizingKey.sig) || ColumnSizing.signature(fields);
        const sizing = ColumnSizing.resolve(fractalId, fields, rows, numericFields, sig);

        const seq = ++this._tableSeq;

        let tableClass = 'results-table is-fixed';
        if (features.reorder) tableClass += ' col-reorderable';

        // Stamp the persistence key so the globally-delegated resize/autofit
        // handler can save widths without any per-table wiring.
        const sizeAttrs = features.resize
            ? ` data-colsize-fractal="${Utils.escapeAttr(fractalId)}" data-colsize-sig="${Utils.escapeAttr(sig)}"`
            : '';

        let html = `<table class="${tableClass}" data-table-id="${seq}"${sizeAttrs}>` + ColumnSizing.buildColgroup(fields, sizing) + '<thead><tr>';
        fields.forEach(field => {
            const sortable = features.sort ? ' sortable' : '';
            const sortIcon = (features.sort && opts.sortColumn === field)
                ? (opts.sortDirection === 'asc' ? ' ▲' : ' ▼') : '';
            const numClass = numericFields.has(field) ? ' numeric-col' : (field === 'raw_log' ? ' raw-log-col' : '');
            const resizer = features.resize ? '<div class="column-resizer"></div>' : '';
            html += `<th class="${sortable.trim()}${numClass}" data-field="${Utils.escapeAttr(field)}">${Utils.escapeHtml(field)}${sortIcon}${resizer}</th>`;
        });
        html += (sizing.hasFiller ? '<th class="filler-col"></th>' : '') + '</tr></thead><tbody>';

        rows.forEach((result, index) => {
            const extra = opts.rowClass ? (opts.rowClass(result, index) || '') : '';
            const rowStyle = opts.rowStyle ? (opts.rowStyle(result, index) || '') : '';
            html += `<tr class="result-row${extra ? ' ' + extra : ''}" data-index="${index}"${rowStyle ? ` style="${rowStyle}"` : ''}>`;
            fields.forEach(field => {
                const value = result[field];
                let cellHtml, cellClass;
                const custom = opts.cellRender ? opts.cellRender(field, value, result) : null;
                if (custom !== null && custom !== undefined) {
                    cellHtml = custom;
                    cellClass = field === 'timestamp' ? 'timestamp-cell' : (numericFields.has(field) ? 'numeric-col' : '');
                } else {
                    const d = this._defaultCellHTML(field, value, numericFields);
                    cellHtml = d.html;
                    cellClass = d.cellClass;
                }
                const cellStyle = opts.cellStyle ? (opts.cellStyle(field, result) || '') : '';
                html += `<td class="${cellClass}"${cellStyle ? ` style="${cellStyle}"` : ''}>${cellHtml}</td>`;
            });
            html += (sizing.hasFiller ? '<td class="filler-col"></td>' : '') + '</tr>';
        });

        html += '</tbody></table>';
        if (opts.truncatedNote && results.length > rows.length) html += opts.truncatedNote;

        const self = this;
        const mount = (root) => {
            if (!root) return null;
            const table = root.querySelector(`table[data-table-id="${seq}"]`);
            if (!table) return null;

            if (opts.lazyJson !== false) self.lazyHighlightJSON(root);

            // Resize + autofit are handled by ColumnSizing's global delegation
            // (keyed off the data-colsize-* attributes), so no wiring here.

            if (features.reorder && opts.onReorder) {
                ColumnSizing.attachReordering(table, opts.onReorder);
            }

            if (features.sort && opts.onSort) {
                const thead = table.querySelector('thead');
                if (thead) thead.addEventListener('click', (e) => {
                    const header = e.target.closest('th[data-field]');
                    if (!header) return;
                    if (e.target.classList.contains('column-resizer')) return;
                    // Swallow the click that trails a drag-to-reorder so it doesn't sort.
                    const now = (window.performance ? performance.now() : Date.now());
                    if (self._lastReorderTs && (now - self._lastReorderTs) < 300) return;
                    opts.onSort(header.dataset.field);
                });
            }

            if (opts.onRowClick) {
                const tbody = table.querySelector('tbody');
                if (tbody) tbody.addEventListener('click', (e) => {
                    if (e.target.classList.contains('column-resizer')) return;
                    const rowEl = e.target.closest('.result-row');
                    if (!rowEl) return;
                    const index = parseInt(rowEl.dataset.index);
                    opts.onRowClick(rows[index], index, rowEl, e);
                });
            }

            // Right-click a cell -> caller opens a context menu (copy / interactions).
            // Left-click stays native so text selection + copy keep working.
            if (opts.onCellContextMenu) {
                const tbody = table.querySelector('tbody');
                if (tbody) tbody.addEventListener('contextmenu', (e) => {
                    const rowEl = e.target.closest('.result-row');
                    const td = e.target.closest('td');
                    if (!rowEl || !td) return;
                    const cellIndex = Array.prototype.indexOf.call(rowEl.children, td);
                    const field = fields[cellIndex];
                    const index = parseInt(rowEl.dataset.index);
                    const row = rows[index];
                    e.preventDefault();
                    opts.onCellContextMenu(row, field, field != null ? (row ? row[field] : undefined) : undefined, e);
                });
            }

            if (opts.afterMount) opts.afterMount(root, table);
            return table;
        };

        return { html, mount, sizing, numericFields, seq };
    },

    // Splice a dragged column into the persisted display order and re-render.
    _applyReorder(field, targetIndex) {
        const order = (this._displayFields || []).slice();
        const from = order.indexOf(field);
        if (from === -1) return;
        order.splice(from, 1);
        let to = (from < targetIndex) ? targetIndex - 1 : targetIndex;
        to = Math.max(0, Math.min(order.length, to));
        if (to === from) return;
        order.splice(to, 0, field);

        this.columnOrder = order;
        if (this._sizingSig) ColumnSizing.saveOrder(this._sizingFractalId || 'default', this._sizingSig, order);
        this._lastReorderTs = (window.performance ? performance.now() : Date.now());

        const page = window.Pagination ? Pagination.getCurrentPageResults() : this.currentResults;
        this.renderResults(page);
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

    showError(message, errorType, errorPos) {
        // Parse/translate errors point at a position in the BQL the user just
        // typed, so render them persistently under the editor rather than in an
        // auto-dismissing toast they'd lose while fixing the query. Execution and
        // timeout errors are not tied to the cursor, so a toast is fine.
        if (errorType === 'parse' || errorType === 'translate') {
            this.showQueryError(message, errorPos);
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

    showQueryError(message, errorPos) {
        // Underline the offending span in the editor when the backend pinpointed it.
        if (window.QueryValidate) {
            QueryValidate.applyResult('queryInput', 'queryHighlight', { error_pos: errorPos, error: message });
        }
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
        if (window.QueryValidate) QueryValidate.applyResult('queryInput', 'queryHighlight', {});
        else if (window.SyntaxHighlight) SyntaxHighlight.clearError('queryInput', 'queryHighlight');
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

    // Render results to any target element (alert editor, model search, ...).
    // Thin wrapper over the shared core: resize + autofit + persistence, no sort
    // or reorder, optional detail-view on row click.
    renderResultsToElement(results, targetElement, fieldOrder = null, options = {}) {
        if (!targetElement || !results || results.length === 0) {
            if (targetElement) {
                targetElement.innerHTML = '<div class="no-results">No results found</div>';
            }
            return;
        }

        let fields = [];
        if (fieldOrder && fieldOrder.length > 0) {
            fields = fieldOrder.filter(f => f !== 'fractal_id');
        } else {
            for (const key of Object.keys(results[0])) {
                if (key !== '_all_fields' && key !== 'fractal_id') fields.push(key);
            }
        }

        const embedFractalId = (window.FractalContext && FractalContext.currentFractal && FractalContext.currentFractal.id) || 'embed';

        const built = this.buildResultsTable(fields, results, {
            sizingKey: { fractalId: embedFractalId, sig: ColumnSizing.signature(fields) },
            features: { resize: true, reorder: false, sort: false },
            rowClass: (row) => (window.Comments && Comments.hasComments(row)) ? 'has-comments' : '',
            onRowClick: options.disableDetailView ? null : (row, index, rowEl) => {
                if (!window.LogDetail) return;
                let detailData = row;
                if (row._all_fields && typeof row._all_fields === 'object') {
                    detailData = {
                        ...row._all_fields,
                        timestamp: row.timestamp,
                        log_id: row.log_id,
                        fractal_id: row.fractal_id,
                        _shard_num: row._shard_num
                    };
                }
                const hostRef = options.detailHost || 'search';
                if (rowEl) {
                    const tbody = rowEl.parentElement;
                    if (tbody) tbody.querySelectorAll('.result-row.selected').forEach(r => r.classList.remove('selected'));
                    rowEl.classList.add('selected');
                }
                LogDetail.setContext(results, index, options.isAggregated || false, hostRef);
                LogDetail.show(detailData, options.isAggregated || false, hostRef);
            },
        });

        targetElement.innerHTML = built.html;
        built.mount(targetElement);
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

        // Hide table, show chart container. The Fields rail is a push panel; when
        // it is open over a chart it self-renders an "applies to raw events" note.
        if (resultsTable) resultsTable.style.display = 'none';
        chartContainer.style.display = 'block';

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

        // Hide graph-only chrome (toolbar + docked stage); renderGraph re-shows it.
        const graphHostHide = networkDiv.closest('.chart-container');
        if (graphHostHide) {
            graphHostHide.querySelector('.graph-toolbar')?.style.setProperty('display', 'none');
            const st = graphHostHide.querySelector('.graph-stage');
            if (st) st.style.display = 'none';
        }

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
        } else if (this.chartType === 'pgraph') {
            this.renderProvenanceGraph(results);
        } else if (this.chartType === 'mesh') {
            this.renderMesh(results);
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

    // Size a graph/mesh container so it fits the visible viewport without forcing
    // the user to scroll. The height still grows with node count (denser graphs
    // want more room) but is capped at the space actually available below the
    // graph, measured live so it adapts to window size. Call this AFTER the
    // toolbar/stage are in the DOM so the measured offset is accurate.
    fitGraphHeight(networkDiv, nodeCount, perNode) {
        const top = networkDiv.getBoundingClientRect().top;
        // Leave a small breathing margin at the bottom of the viewport.
        const viewportCap = Math.max(400, window.innerHeight - top - 24);
        const desired = Math.max(400, nodeCount * perNode);
        return Math.round(Math.min(desired, viewportCap));
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

        // Child -> parent map for ancestry walks, and the set of nodes that have
        // an incoming edge (everything that is NOT a root of the forest).
        const parentMap = new Map();
        const hasIncoming = new Set();
        limitedResults.forEach(r => {
            const c = r[childField];
            const p = r[parentField];
            if (c && p && p !== '' && p !== 'null') {
                if (!parentMap.has(c)) parentMap.set(c, p);
                hasIncoming.add(c);
            }
        });
        const rootSet = new Set();
        nodeDetails.forEach((_, id) => { if (!hasIncoming.has(id)) rootSet.add(id); });

        // Color nodes by process (image basename), capped to the most frequent
        // few so the palette stays meaningful instead of turning into a rainbow.
        const colorKeyOf = (id) => {
            const d = nodeDetails.get(id) || {};
            if (d.image) {
                const parts = String(d.image).split(/[/\\]/);
                const base = (parts.pop() || '').toLowerCase();
                if (base) return base;
            }
            for (const f of labelFields) {
                if (d[f] !== undefined && d[f] !== null && d[f] !== '') return String(d[f]).toLowerCase();
            }
            return null;
        };
        const keyFreq = new Map();
        nodeDetails.forEach((_, id) => {
            const k = colorKeyOf(id);
            if (k) keyFreq.set(k, (keyFreq.get(k) || 0) + 1);
        });
        const MAX_COLORS = 8;
        const topKeys = [...keyFreq.entries()].sort((a, b) => b[1] - a[1]).slice(0, MAX_COLORS).map(e => e[0]);
        const topSet = new Set(topKeys);
        const neutralColor = cv('--graph-node-neutral');
        const colorForKey = (k) => (k && topSet.has(k)) ? Utils.tagColorFor(k) : neutralColor;

        // Resting/dim colors (always solid, never transparent) for highlight states.
        const dimNode = cv('--graph-node-dim');
        const labelColor = cv('--graph-label');
        const labelDim = cv('--graph-label-dim');
        const edgeBase = cv('--graph-edge');
        const accentColor = cv('--accent-primary');

        // Create nodes with improved labels and HTML tooltips
        const baseColors = new Map();
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

            const fill = colorForKey(colorKeyOf(nodeId));
            const isRoot = rootSet.has(nodeId);
            const border = isRoot ? accentColor : fill;
            baseColors.set(nodeId, { background: fill, border });

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
                size: isRoot ? 24 : 16,
                borderWidth: isRoot ? 3 : 2,
                mass: 1,
                color: {
                    background: fill,
                    border: border,
                    highlight: { background: fill, border: accentColor },
                    hover: { background: fill, border: accentColor }
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

        const nodeCount = nodeDetails.size;

        // -- Toolbar --
        const graphHost = networkDiv.closest('.chart-container') || networkDiv.parentElement;
        let graphToolbar = graphHost.querySelector('.graph-toolbar');
        if (graphToolbar) graphToolbar.remove();

        // Dynamic legend: one swatch per top process color, plus overflow + root.
        const truncKey = (k) => k.length > 16 ? k.substring(0, 15) + '…' : k;
        let legendHtml = topKeys.map(k =>
            `<span class="graph-legend-item" title="${Utils.escapeHtml(k)}"><span class="graph-legend-dot" style="background:${Utils.tagColorFor(k)}"></span>${Utils.escapeHtml(truncKey(k))}</span>`
        ).join('');
        if (keyFreq.size > topKeys.length) {
            legendHtml += `<span class="graph-legend-item"><span class="graph-legend-dot" style="background:${neutralColor}"></span>other</span>`;
        }
        if (rootSet.size > 0) {
            legendHtml += `<span class="graph-legend-item"><span class="graph-legend-dot graph-legend-root"></span>root</span>`;
        }

        graphToolbar = document.createElement('div');
        graphToolbar.className = 'graph-toolbar';
        graphToolbar.innerHTML = `
            <div class="graph-stats">
                <span class="graph-stat-item"><span class="graph-stat-count" id="graphNodeCount">${nodes.length}</span> nodes</span>
                <span class="graph-stat-separator"></span>
                <span class="graph-stat-item"><span class="graph-stat-count" id="graphEdgeCount">${edges.length}</span> edges</span>
            </div>
            <div class="graph-legend">${legendHtml}</div>
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
        // Wrap the canvas + detail panel in a flex row so the panel docks and the
        // graph reflows into the remaining width instead of floating over it.
        let stage = graphHost.querySelector('.graph-stage');
        if (!stage) {
            stage = document.createElement('div');
            stage.className = 'graph-stage';
            graphHost.insertBefore(stage, networkDiv);
            stage.appendChild(networkDiv);
        }
        stage.style.display = 'flex';
        graphHost.insertBefore(graphToolbar, stage);

        // -- Detail Panel (docked flex sibling) --
        let detailPanel = stage.querySelector('.graph-detail-panel');
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
        stage.appendChild(detailPanel);

        // Size to fit the viewport now that the toolbar/stage are in place.
        networkDiv.style.height = this.fitGraphHeight(networkDiv, nodeCount, 30) + 'px';

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

        // -- Minimap overlay (created after the network so it sits above vis's canvas) --
        let minimap = networkDiv.querySelector('.graph-minimap');
        if (minimap) minimap.remove();
        minimap = document.createElement('canvas');
        minimap.className = 'graph-minimap';
        minimap.width = 240;
        minimap.height = 160;
        networkDiv.appendChild(minimap);

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
                    padding: 40
                });
            }
        }, 200);

        // ---- Highlight + interaction helpers ----
        const ancestorsOf = (id) => {
            const chain = new Set([id]);
            let cur = id;
            while (parentMap.has(cur)) {
                cur = parentMap.get(cur);
                if (chain.has(cur)) break;        // cycle guard
                chain.add(cur);
                if (!nodeDetails.has(cur)) break; // parent not in result set
            }
            return chain;
        };

        const nodeShort = (id) => {
            const n = nodes.get(id);
            if (n && n.label) return n.label;
            return id.length > 10 ? id.substring(0, 8) + '…' : id;
        };

        const litColor = (id) => {
            const base = baseColors.get(id);
            return {
                background: base.background, border: base.border,
                highlight: { background: base.background, border: accentColor },
                hover: { background: base.background, border: accentColor }
            };
        };
        const dimColor = { background: dimNode, border: dimNode, highlight: { background: dimNode, border: dimNode }, hover: { background: dimNode, border: dimNode } };

        const restoreBase = () => {
            nodes.update([...nodeDetails.keys()].map(id => ({ id, color: litColor(id), font: { color: labelColor } })));
            edges.update(edges.getIds().map(eid => ({ id: eid, color: { color: edgeBase, opacity: 0.5, highlight: accentColor }, width: 1.5 })));
            drawMinimap();
        };

        // Light only nodes in keepSet (solid dim for the rest). When hot, also
        // brightens the edges that lie entirely within keepSet (the lineage).
        const dimExcept = (keepSet, hot) => {
            nodes.update([...nodeDetails.keys()].map(id => {
                const on = keepSet.has(id);
                return { id, color: on ? litColor(id) : dimColor, font: { color: on ? labelColor : labelDim } };
            }));
            edges.update(edges.get().map(e => {
                const on = keepSet.has(e.from) && keepSet.has(e.to);
                return {
                    id: e.id,
                    color: { color: (hot && on) ? accentColor : edgeBase, opacity: on ? (hot ? 0.95 : 0.5) : 0.1, highlight: accentColor },
                    width: (hot && on) ? 2.5 : 1.5
                };
            }));
            drawMinimap();
        };

        let selectedNodeId = null;
        let searchActive = false;
        let searchMatchSet = null;

        const reapplyResting = () => {
            if (selectedNodeId) dimExcept(ancestorsOf(selectedNodeId), true);
            else if (searchActive && searchMatchSet) dimExcept(searchMatchSet, false);
            else restoreBase();
        };

        // Keep the vis canvas sized to its (reflowing) container after the panel docks.
        const resizeGraph = () => {
            if (!this.currentChart) return;
            this.currentChart.setSize(networkDiv.clientWidth + 'px', networkDiv.clientHeight + 'px');
            this.currentChart.redraw();
        };

        // ---- Minimap ----
        const mmCtx = minimap.getContext('2d');
        const drawMinimap = () => {
            if (!this.currentChart || !minimap.isConnected) return;
            const positions = this.currentChart.getPositions();
            const ids = Object.keys(positions);
            const w = minimap.width, h = minimap.height;
            mmCtx.clearRect(0, 0, w, h);
            mmCtx.fillStyle = cv('--bg-secondary');
            mmCtx.fillRect(0, 0, w, h);
            if (ids.length === 0) return;
            let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
            ids.forEach(id => { const p = positions[id]; if (p.x < minX) minX = p.x; if (p.x > maxX) maxX = p.x; if (p.y < minY) minY = p.y; if (p.y > maxY) maxY = p.y; });
            const pad = 14;
            const spanX = Math.max(1, maxX - minX), spanY = Math.max(1, maxY - minY);
            const scale = Math.min((w - 2 * pad) / spanX, (h - 2 * pad) / spanY);
            const offX = (w - spanX * scale) / 2, offY = (h - spanY * scale) / 2;
            const toMM = (x, y) => ({ x: offX + (x - minX) * scale, y: offY + (y - minY) * scale });
            ids.forEach(id => {
                const p = positions[id];
                const m = toMM(p.x, p.y);
                const base = baseColors.get(id);
                mmCtx.fillStyle = (base && base.background) || neutralColor;
                mmCtx.beginPath();
                mmCtx.arc(m.x, m.y, rootSet.has(id) ? 2.6 : 1.7, 0, Math.PI * 2);
                mmCtx.fill();
            });
            const tl = this.currentChart.DOMtoCanvas({ x: 0, y: 0 });
            const br = this.currentChart.DOMtoCanvas({ x: networkDiv.clientWidth, y: networkDiv.clientHeight });
            const a = toMM(tl.x, tl.y), b = toMM(br.x, br.y);
            mmCtx.strokeStyle = accentColor;
            mmCtx.lineWidth = 1.5;
            mmCtx.strokeRect(a.x, a.y, b.x - a.x, b.y - a.y);
            minimap._map = { minX, minY, scale, offX, offY };
        };
        this.currentChart.on('afterDrawing', () => drawMinimap());

        // Click/drag the minimap to pan the main view.
        const mmNavigate = (ev) => {
            const map = minimap._map; if (!map) return;
            const rect = minimap.getBoundingClientRect();
            const mx = ev.clientX - rect.left, my = ev.clientY - rect.top;
            const cx = map.minX + (mx - map.offX) / map.scale;
            const cy = map.minY + (my - map.offY) / map.scale;
            this.currentChart.moveTo({ position: { x: cx, y: cy }, animation: { duration: 150 } });
        };
        let mmDragging = false;
        minimap.addEventListener('mousedown', (e) => { mmDragging = true; mmNavigate(e); e.preventDefault(); });
        if (this._mmMove) window.removeEventListener('mousemove', this._mmMove);
        if (this._mmUp) window.removeEventListener('mouseup', this._mmUp);
        this._mmMove = (e) => { if (mmDragging) mmNavigate(e); };
        this._mmUp = () => { mmDragging = false; };
        window.addEventListener('mousemove', this._mmMove);
        window.addEventListener('mouseup', this._mmUp);

        // ---- Detail panel content ----
        const buildPanel = (nodeId) => {
            const details = nodeDetails.get(nodeId) || {};
            const procKey = colorKeyOf(nodeId);
            const procColor = procKey ? Utils.tagColorFor(procKey) : neutralColor;
            const isRoot = rootSet.has(nodeId);
            const depth = details._depth !== undefined ? details._depth : null;
            const body = detailPanel.querySelector('.graph-detail-body');

            let chips = '';
            if (procKey) chips += `<span class="graph-detail-proc" style="--chip-color:${procColor}">${Utils.escapeHtml(procKey)}</span>`;
            if (isRoot) chips += `<span class="graph-detail-tag root">root</span>`;
            if (depth !== null) chips += `<span class="graph-detail-tag">depth ${Utils.escapeHtml(String(depth))}</span>`;

            let breadcrumb = '';
            if (details._path) {
                const segs = String(details._path).split(' > ');
                breadcrumb = '<div class="graph-detail-path">' + segs.map((s, i) => {
                    const id = s.trim();
                    const known = nodeDetails.has(id);
                    const lbl = known ? nodeShort(id) : (id.length > 10 ? id.substring(0, 8) + '…' : id);
                    const sep = i < segs.length - 1 ? '<span class="graph-path-sep">›</span>' : '';
                    return `<span class="graph-path-seg${known ? '' : ' unknown'}${id === nodeId ? ' current' : ''}" data-node="${Utils.escapeHtml(id)}" title="${Utils.escapeHtml(id)}">${Utils.escapeHtml(lbl)}</span>${sep}`;
                }).join('') + '</div>';
            }

            const fieldEntries = Object.entries(details).filter(([k]) => k !== '_path');
            let fieldsHtml;
            if (fieldEntries.length > 0) {
                fieldsHtml = '<div class="graph-detail-fields">' + fieldEntries.map(([k, v]) =>
                    `<div class="graph-detail-field"><div class="graph-detail-field-name">${Utils.escapeHtml(k)}</div><div class="graph-detail-field-value">${Utils.escapeHtml(String(v))}</div></div>`
                ).join('') + '</div>';
            } else {
                fieldsHtml = '<div class="graph-detail-empty">No additional fields available</div>';
            }

            const parentId = parentMap.get(nodeId);
            const canWalkUp = parentId && /\b(?:dfs|bfs)\s*\(/i.test(this.currentQuery || '');
            const actionsHtml = `
                <div class="graph-detail-actions">
                    ${canWalkUp ? '<button class="graph-detail-action" data-act="walkup" title="Re-root the traversal at this node\'s parent">Walk Up</button>' : ''}
                    <button class="graph-detail-action" data-act="subtree">Focus subtree</button>
                    <button class="graph-detail-action secondary" data-act="copy">Copy ID</button>
                </div>`;

            body.innerHTML = `
                <div class="graph-detail-id">
                    <span class="graph-detail-id-label">ID</span>
                    <span class="graph-detail-id-value">${Utils.escapeHtml(nodeId)}</span>
                </div>
                <div class="graph-detail-chips">${chips}</div>
                ${breadcrumb}
                ${actionsHtml}
                ${fieldsHtml}
            `;

            body.querySelector('[data-act="copy"]')?.addEventListener('click', () => {
                navigator.clipboard.writeText(nodeId).then(() => Toast.show('Copied', 'success'));
            });
            body.querySelector('[data-act="subtree"]')?.addEventListener('click', () => {
                const connected = this.currentChart.getConnectedNodes(nodeId);
                this.currentChart.fit({ nodes: [nodeId, ...connected], animation: { duration: 400 }, padding: 80 });
            });
            body.querySelector('[data-act="walkup"]')?.addEventListener('click', () => this.pivotTraversal(parentId));
            body.querySelectorAll('.graph-path-seg').forEach(el => {
                el.addEventListener('click', () => {
                    const id = el.getAttribute('data-node');
                    if (!nodeDetails.has(id)) return;
                    this.currentChart.selectNodes([id]);
                    selectNodeAction(id);
                });
            });
        };

        const selectNodeAction = (nodeId) => {
            selectedNodeId = nodeId;
            buildPanel(nodeId);
            detailPanel.classList.add('open');
            dimExcept(ancestorsOf(nodeId), true);
            setTimeout(() => {
                resizeGraph();
                this.currentChart.focus(nodeId, { scale: Math.max(this.currentChart.getScale(), 0.6), animation: { duration: 300, easingFunction: 'easeInOutQuad' } });
                drawMinimap();
            }, 210);
        };

        // -- Toolbar handlers --
        document.getElementById('graphFitBtn')?.addEventListener('click', () => {
            this.currentChart.fit({ animation: { duration: 400, easingFunction: 'easeInOutQuad' }, padding: 40 });
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

        // -- Node search (solid dim, never transparent) --
        const searchInput = document.getElementById('graphNodeSearch');
        if (searchInput) {
            searchInput.addEventListener('input', Utils.debounce((e) => {
                const term = e.target.value.toLowerCase().trim();
                if (!term) {
                    searchActive = false;
                    searchMatchSet = null;
                    reapplyResting();
                    return;
                }
                const match = new Set();
                nodeDetails.forEach((details, nodeId) => {
                    if (nodeId.toLowerCase().includes(term) || Object.values(details).some(v => String(v).toLowerCase().includes(term))) {
                        match.add(nodeId);
                    }
                });
                searchActive = true;
                searchMatchSet = match;
                if (!selectedNodeId) dimExcept(match, false);
            }, 200));
        }

        // -- Node click: detail panel + ancestry highlight --
        const closePanel = () => {
            detailPanel.classList.remove('open');
            selectedNodeId = null;
            this.currentChart.unselectAll();
            setTimeout(() => { resizeGraph(); reapplyResting(); }, 210);
        };
        detailPanel.querySelector('.graph-detail-close').addEventListener('click', closePanel);

        this.currentChart.on('selectNode', (params) => {
            const nodeId = params.nodes[0];
            if (!nodeId) return;
            selectNodeAction(nodeId);
        });

        this.currentChart.on('deselectNode', (params) => {
            if (params.nodes && params.nodes.length) return; // selection moved to another node
            detailPanel.classList.remove('open');
            selectedNodeId = null;
            setTimeout(() => { resizeGraph(); reapplyResting(); }, 210);
        });

        // -- Hover lights up the lineage to root --
        this.currentChart.on('hoverNode', (params) => {
            if (selectedNodeId) return;
            dimExcept(ancestorsOf(params.node), true);
        });
        this.currentChart.on('blurNode', () => {
            if (selectedNodeId) return;
            reapplyResting();
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
            const ctxParentId = parentMap.get(nodeId);
            const canWalkUp = ctxParentId && /\b(?:dfs|bfs)\s*\(/i.test(this.currentQuery || '');
            menu.innerHTML = `
                ${canWalkUp ? '<button class="graph-ctx-item" data-action="walkup">Walk Up</button>' : ''}
                <button class="graph-ctx-item" data-action="focus">Focus neighborhood</button>
                <button class="graph-ctx-item" data-action="copy">Copy node ID</button>
            `;
            document.body.appendChild(menu);

            menu.addEventListener('click', (e) => {
                const action = e.target.dataset.action;
                if (action === 'walkup') {
                    this.pivotTraversal(ctxParentId);
                } else if (action === 'focus') {
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

    // pgraph() renders pgr()'s scored provenance graph two ways behind a toolbar toggle:
    //  - Graph: a diagonal process-map (the CrowdStrike/Elastic construction). Only PROCESS
    //    nodes are placed spatially; each carries its file/net/dns activity as compact count
    //    badges and an anomaly pill. Long attack chains read as a clean diagonal spine; wide
    //    leaf-sibling fans collapse into an expandable "N processes" node so the map stays
    //    legible. Custom HTML nodes over an SVG edge layer with CSS-transform pan/zoom.
    //  - Table: an indented, collapsible outline that reads top-down at any scale. Both share
    //    one parsed model and open the source log on click.
    renderProvenanceGraph(results) {
        const networkDiv = document.getElementById('networkGraph');
        if (!networkDiv) return;
        const chartCanvas = document.getElementById('resultsChart');
        if (chartCanvas) chartCanvas.style.display = 'none';
        if (this.currentChart) { this.currentChart.destroy(); this.currentChart = null; }

        const limit = (this.chartConfig && this.chartConfig.limit) || 3000;
        // The queried start node (pgr start guid): centered on first render and ring-highlighted.
        // Must be set BEFORE the model/fanout are built -- both classify nodes relative to it
        // (home vs external tree; focus is never collapsed into an aggregate).
        this._pgFocus = (this.chartConfig && this.chartConfig.focus) || null;
        this._pgModel = this._pgBuildModel((results || []).slice(0, limit));
        this._pgComputeSevScale();      // adaptive absolute/relative anomaly shading for this graph
        this._pgExpandedAggs = new Set(); // fan-out aggregate nodes the user expanded
        this._pgComputeFanout();        // collapse large same-image sibling fans into aggregate nodes
        this._pgCollapsed = new Set();  // process guids whose spawn subtree is folded (+/-)
        this._pgSearch = '';
        this._pgMinAnomaly = 0;
        this._pgVS = { s: 1, x: 0, y: 0 };  // pan/zoom (scale, translateX, translateY)
        if (!['graph', 'table'].includes(this._pgView)) this._pgView = 'graph';

        // Stage (flex host shared with graph()/mesh()) + sibling graph & tree containers.
        const graphHost = networkDiv.closest('.chart-container') || networkDiv.parentElement;
        let stage = graphHost.querySelector('.graph-stage');
        if (!stage) { stage = document.createElement('div'); stage.className = 'graph-stage'; graphHost.insertBefore(stage, networkDiv); stage.appendChild(networkDiv); }
        stage.style.display = 'flex';
        // pgraph never uses vis; keep #networkGraph parked and render into our own elements.
        networkDiv.style.display = 'none';
        let graphDiv = stage.querySelector('.pg-graph');
        if (!graphDiv) { graphDiv = document.createElement('div'); graphDiv.className = 'pg-graph'; stage.appendChild(graphDiv); }
        let treeDiv = stage.querySelector('.pg-tree');
        if (!treeDiv) { treeDiv = document.createElement('div'); treeDiv.className = 'pg-tree'; stage.appendChild(treeDiv); }
        // Drop any leftover graph()-specific chrome docked in this host.
        const oldDetail = graphHost.querySelector('.graph-detail-panel'); if (oldDetail) oldDetail.remove();

        this._pgBuildToolbar(graphHost, stage);
        this._pgRender();
    },

    // Theme-aware colors: node type off dedicated CSS vars (fall back to the prior hex), edge
    // severity off the app's semantic vars so anomaly coloring tracks light/dark like the rest.
    _pgTypeColor(type) {
        const cv = ThemeManager.getCSSVar;
        switch (type) {
            case 'file': return cv('--pg-file') || '#8b7cc8';
            case 'net': return cv('--pg-net') || '#e0a458';
            case 'dns': return cv('--pg-dns') || '#4bb3a5';
            case 'agg': return cv('--pg-agg') || '#6e7491';
            default: return cv('--pg-process') || '#7c8699';
        }
    },
    _pgAnomalyColor(score) {
        const cv = ThemeManager.getCSSVar;
        const sev = this._pgSev(score);
        if (sev === 'high') return cv('--error') || '#e5484d';
        if (sev === 'med') return cv('--warning') || '#f5a623';
        return cv('--graph-node-neutral') || '#6b7280';
    },

    // Severity bucket (high|med|low|none) for an anomaly score, driven by an ADAPTIVE scale.
    // Absolute 0.9/0.7 thresholds are the right lens on a mature baseline (common OS activity
    // sits near 0, only the attack chain lights up). But when diffusion propagates -- or the
    // baseline is thin -- scores pile up near 1.0 and absolute thresholds paint the whole graph
    // red, killing all contrast. In that (detected) case we switch to a RELATIVE scale spread
    // across the graph's own [lo,hi] range so the hottest chain still stands out. The numeric
    // anomaly_score is never changed (agents/pills still see the real value); only the COLOR
    // adapts. See _pgComputeSevScale.
    _pgSev(a) {
        if (isNaN(a)) return 'none';
        const s = this._pgSevScale;
        if (s && s.rel) {
            const t = (a - s.lo) / (s.span || 1);
            return t >= 0.66 ? 'high' : t >= 0.33 ? 'med' : 'low';
        }
        return a >= 0.9 ? 'high' : a >= 0.7 ? 'med' : 'low';
    },
    _pgSevHot(a) { const s = this._pgSev(a); return s === 'high' || s === 'med'; },

    // Decide absolute vs relative shading from the current graph's anomaly distribution. Relative
    // kicks in only when the scores are genuinely saturated (most of the graph already clears the
    // absolute 'med' line) AND there is enough spread to relativize -- otherwise absolute stays,
    // so a normal graph is unaffected.
    _pgComputeSevScale() {
        const m = this._pgModel;
        const xs = [];
        if (m) {
            if (m.anomalyByNode) m.anomalyByNode.forEach(v => { if (!isNaN(v)) xs.push(v); });
            if (m.interactions) m.interactions.forEach(list => list.forEach(it => { if (!isNaN(it.anomaly)) xs.push(it.anomaly); }));
        }
        this._pgSevScale = { rel: false, saturated: false };
        if (xs.length < 6) return;
        xs.sort((a, b) => a - b);
        const q = (p) => xs[Math.min(xs.length - 1, Math.max(0, Math.floor(p * (xs.length - 1))))];
        const lo = xs[0], hi = xs[xs.length - 1], p15 = q(0.15);
        this._pgSevScale.saturated = p15 >= 0.7; // most of the graph already clears the absolute 'med' line
        // Engage relative shading when saturated (most of the graph clears the absolute 'med'
        // line) and there is at least a sliver of spread to relativize. The floor is deliberately
        // small: a saturated diffuse graph may only span 0.90-1.00, and that 0.02+ spread is
        // exactly the signal we want to surface. A near-uniform graph (span below the floor) stays
        // absolute -- everything really is equally anomalous, so painting it all red is honest.
        if (this._pgSevScale.saturated && hi - lo >= 0.02) this._pgSevScale = { rel: true, saturated: true, lo, span: hi - lo };
    },

    // Fan-out collapse: when a process spawns many similar children (e.g. 30x msedge.exe), those
    // siblings drown the tree. Collapse each large same-image group into ONE aggregate node
    // ("image xN"). Two things are kept individual and never folded:
    //   1. STRUCTURALLY interesting children -- a child with its own subtree, file/net/dns activity,
    //      an interaction/reconnection, or that is the focus/external/ghost.
    //   2. An anomaly OUTLIER *relative to its own sibling group* -- a member whose anomaly stands
    //      clearly above the group's typical (median) level. This is the key subtlety: promotion is
    //      GROUP-relative, not graph-relative. A fan of 30 identical msedge all at 0.97 has no
    //      outlier (they are all equally rare), so it collapses; but 19 benign children + 1 rare one
    //      still surfaces that one. (An earlier graph-relative rule kept every high-anomaly sibling,
    //      so a uniformly-anomalous fan never collapsed.)
    // CrowdStrike / Elastic Resolver pattern. Aggregates are expandable (this._pgExpandedAggs).
    _pgComputeFanout() {
        const m = this._pgModel;
        this._pgAgg = new Map();      // parentGuid -> { groups: [meta] }
        this._pgAggMeta = new Map();  // aggId -> { parent, image, members, count, anomaly, id }
        if (!m) return;
        const MIN = 5;                 // only collapse a genuinely large same-image fan (> MIN members)
        const OUTLIER = 0.15;          // an anomaly this far above the group median is a standout
        const structural = (c) => {
            if (c === this._pgFocus) return true;
            if (m.ghostProcs && m.ghostProcs.has(c)) return true;
            if (m.externalProcs && m.externalProcs.has(c)) return true;
            if ((m.spawnKids.get(c) || []).length) return true;
            const g = m.leafGroups.get(c);
            if (g && (g.file.length || g.net.length || g.dns.length)) return true;
            if ((m.interactions.get(c) || []).length) return true;
            if ((m.linkInfo && m.linkInfo.get(c) || []).length) return true;
            return false;
        };
        m.spawnKids.forEach((kids, parent) => {
            if (kids.length <= MIN) return;
            const byImage = new Map();
            kids.forEach(c => {
                if (structural(c)) return;
                const img = m.procLabel.get(c) || c;
                if (!byImage.has(img)) byImage.set(img, []);
                byImage.get(img).push(c);
            });
            const groups = [];
            let gi = 0;
            byImage.forEach((members, img) => {
                if (members.length <= MIN) return;
                // Group-relative outlier gate: fold members at/below (median + OUTLIER); a member
                // above it stays individual. A uniform group (all ~equal) folds entirely.
                const anoms = members.map(c => m.anomalyByNode.get(c)).filter(a => !isNaN(a)).sort((a, b) => a - b);
                const median = anoms.length ? anoms[Math.floor(anoms.length / 2)] : 0;
                const cutoff = median + OUTLIER;
                const fold = members.filter(c => { const a = m.anomalyByNode.get(c); return isNaN(a) || a <= cutoff; });
                if (fold.length <= MIN) return; // not enough uniform members left to bother collapsing
                const aggId = 'pgagg:' + parent + ':' + (gi++);
                let anomaly = NaN;
                fold.forEach(mm => { const a = m.anomalyByNode.get(mm); if (!isNaN(a) && (isNaN(anomaly) || a > anomaly)) anomaly = a; });
                const meta = { parent, image: img, members: fold, count: fold.length, anomaly, id: aggId };
                groups.push(meta);
                this._pgAggMeta.set(aggId, meta);
            });
            if (groups.length) this._pgAgg.set(parent, { groups });
        });
    },

    // The display children of a process: promoted individual children + one entry per collapsed
    // aggregate, in the original spawn order. Aggregates are returned as {kind:'agg'} entries
    // regardless of expansion -- an EXPANDED aggregate keeps its node and reveals its members as
    // that node's own children (kidsOf[aggId]), so both views stay consistent and re-collapsible.
    _pgDisplayChildren(guid) {
        const m = this._pgModel;
        const kids = (m && m.spawnKids.get(guid)) || [];
        const agg = this._pgAgg && this._pgAgg.get(guid);
        if (!agg) return kids.map(id => ({ kind: 'proc', id }));
        const childAgg = new Map();
        agg.groups.forEach(g => g.members.forEach(mm => childAgg.set(mm, g.id)));
        const out = [], emitted = new Set();
        kids.forEach(id => {
            const aggId = childAgg.get(id);
            if (!aggId) { out.push({ kind: 'proc', id }); return; }
            if (!emitted.has(aggId)) { emitted.add(aggId); out.push({ kind: 'agg', id: aggId }); }
        });
        return out;
    },
    // Expand/collapse a fan-out aggregate, re-rendering the active view in place.
    _pgToggleAgg(aggId, container) {
        if (!this._pgExpandedAggs) this._pgExpandedAggs = new Set();
        if (this._pgExpandedAggs.has(aggId)) this._pgExpandedAggs.delete(aggId); else this._pgExpandedAggs.add(aggId);
        if (this._pgView === 'table' && container) { this._pgRenderTree(container); return; }
        this._pgKeepView = true;
        if (this._pgGraphHost) this._pgRenderGraph(this._pgGraphHost);
    },

    _pgTypeOf(id) {
        if (!id) return 'process';
        if (id.startsWith('file:')) return 'file';
        if (id.startsWith('net:')) return 'net';
        if (id.startsWith('dns:')) return 'dns';
        return 'process';
    },
    _pgLeafNoun(type) { return type === 'file' ? 'files' : type === 'net' ? 'connections' : type === 'dns' ? 'domains' : type; },
    _pgShort(s) { s = String(s == null ? '' : s); const b = s.split(/[\\/]/).pop() || s; return b.length > 26 ? b.slice(0, 26) + '…' : b; },

    // Parse pgr rows into a model both views consume. Process->process edges (spawn, and the
    // remote_thread/process_access interactions) form the tree; file/net/dns edges are grouped
    // per (process, type) so they can be collapsed.
    _pgBuildModel(rows) {
        const procLabel = new Map();     // guid -> image label
        const spawnKids = new Map();     // parent guid -> [child guid]
        const interactions = new Map();  // src guid -> [{target,type,anomaly,label,info}]
        const leafGroups = new Map();    // parent guid -> {file:[],net:[],dns:[]} of {id,label,anomaly,info}
        const leafOwners = new Map();    // leaf id -> Set(process guid) -- reverse index for shared-leaf detection
        const leafMeta = new Map();      // leaf id -> {type,label,anomaly} for promoted shared-object nodes
        const logInfoById = new Map();   // node id -> {log_id,timestamp,fractal_id,_shard_num}
        const anomalyByNode = new Map(); // node id -> anomaly on its incoming edge
        const procMeta = new Map();      // guid -> {cmd, user} (command line + user, cmd truncated server-side)
        const procTime = new Map();      // guid -> epoch ms (process creation / first-seen time)
        const procHost = new Map();      // guid -> computer_name (for cross-host reconnection notation)
        const isChild = new Set();       // process guids seen as a spawn child
        const hasCreation = new Set();   // process guids with their OWN process_creation row (spawn child side)
        const procSet = new Set();
        const ensureProc = (g, lbl) => { procSet.add(g); if (lbl != null && lbl !== '' && !procLabel.get(g)) procLabel.set(g, lbl); else if (!procLabel.has(g)) procLabel.set(g, procLabel.get(g) || null); };
        const push = (map, k, v) => { if (!map.has(k)) map.set(k, []); map.get(k).push(v); };
        const leafIndex = new Map();      // "parent\0type\0leafId" -> entry, for O(1) leaf dedup (no per-row grp.find)
        // NaN-safe max: a later row with no score must never clobber an earlier real severity.
        const bumpAnomaly = (id, a) => { if (isNaN(a)) return; const prev = anomalyByNode.get(id); anomalyByNode.set(id, (prev == null || isNaN(prev)) ? a : Math.max(prev, a)); };

        (rows || []).forEach(r => {
            if (!r.child) return; // a spawn row for a TRUE root has an empty parent; keep it (see below)
            const et = r.event_type || '';
            const anomaly = parseFloat(r.anomaly_score);
            const info = r.log_id ? { log_id: r.log_id, timestamp: r.timestamp, fractal_id: r.fractal_id, _shard_num: r._shard_num } : null;
            const ctype = this._pgTypeOf(r.child);
            if (r.host) { // host rides each row: process rows carry the child's host, leaf rows the parent's
                if (ctype === 'process') { if (!procHost.get(r.child)) procHost.set(r.child, r.host); }
                else if (r.parent && !procHost.get(r.parent)) procHost.set(r.parent, r.host);
            }
            if (ctype === 'process') {
                if (r.parent) ensureProc(r.parent, null);
                ensureProc(r.child, r.label);
                // A spawn row IS the child's process_creation event (it comes from proc_lineage),
                // even when the parent_guid is empty. Recording this lets us tell a real root (has
                // its own creation) from a "ghost" parent that only exists as some child's
                // parent_guid (its creation event is missing / outside the time range).
                if (et === 'spawn') hasCreation.add(r.child);
                if (info) logInfoById.set(r.child, info);
                bumpAnomaly(r.child, anomaly);
                // command line + user ride on the child process row (pm-joined server-side).
                if (r.command_line || r.proc_user) {
                    const cur = procMeta.get(r.child);
                    if (!cur || (!cur.cmd && r.command_line)) procMeta.set(r.child, { cmd: r.command_line || (cur && cur.cmd) || '', user: r.proc_user || (cur && cur.user) || '' });
                }
                const t = this._pgParseTime(r.timestamp);
                if (t != null && (et === 'spawn' || !procTime.has(r.child))) procTime.set(r.child, t);
                // Empty parent => a true root: register its creation (above) but draw no spawn edge.
                if (et === 'spawn') { if (r.parent) { push(spawnKids, r.parent, r.child); isChild.add(r.child); } }
                else if (r.parent) { push(interactions, r.parent, { target: r.child, type: et, anomaly, label: r.label, info, recon: et.indexOf('reconnect') === 0 }); }
            } else {
                if (!r.parent) return; // a leaf edge (file/net/dns) must have an owning process
                ensureProc(r.parent, null);
                if (!leafGroups.has(r.parent)) leafGroups.set(r.parent, { file: [], net: [], dns: [] });
                // Dedup the same leaf under one parent: pass-2's leaf edge and a reconnect edge
                // can both arrive for it. Keep one entry, max anomaly, flagged recon if either was.
                const grp = leafGroups.get(r.parent)[ctype];
                const lk = r.parent + '\x00' + ctype + '\x00' + r.child;
                let entry = leafIndex.get(lk);
                if (!entry) { entry = { id: r.child, label: r.label, anomaly, info }; grp.push(entry); leafIndex.set(lk, entry); }
                else if (anomaly > entry.anomaly) entry.anomaly = anomaly;
                if (et.indexOf('reconnect') === 0) entry.recon = true;
                if (!leafOwners.has(r.child)) leafOwners.set(r.child, new Set());
                leafOwners.get(r.child).add(r.parent);
                if (!leafMeta.has(r.child)) leafMeta.set(r.child, { type: ctype, label: r.label, anomaly });
                else if (anomaly > leafMeta.get(r.child).anomaly) leafMeta.get(r.child).anomaly = anomaly;
                if (info) logInfoById.set(r.child, info);
                bumpAnomaly(r.child, anomaly);
            }
        });
        const roots = [];
        procSet.forEach(g => { if (!isChild.has(g)) roots.push(g); });

        // Ghost processes: referenced only as some child's parent_guid, with no process_creation
        // event of their own (missing / outside the time range). They are REAL ancestors -- the
        // lineage link exists -- so we keep them placed as the connecting parent of their children,
        // but render them distinctly (dashed, "missing creation") so a bare-GUID node is understood
        // as a data gap, not a mystery. A true root (empty parent_guid) has its own creation and is
        // NOT a ghost.
        const ghostProcs = new Set();
        procSet.forEach(g => { if (!hasCreation.has(g)) ghostProcs.add(g); });

        // Tree membership: which spawn root each process descends from. The tree containing the
        // queried start guid (_pgFocus) is "home"; everything under a different root arrived via
        // reconnection and is flagged external so the renderer can style it as a peer.
        const parentOf = new Map();
        spawnKids.forEach((kids, p) => kids.forEach(c => { if (!parentOf.has(c)) parentOf.set(c, p); }));
        const rootOf = new Map();
        procSet.forEach(g => {
            if (rootOf.has(g)) return;
            let cur = g; const chain = []; const onPath = new Set();
            while (parentOf.has(cur) && !rootOf.has(cur) && !onPath.has(cur)) { onPath.add(cur); chain.push(cur); cur = parentOf.get(cur); }
            const root = rootOf.has(cur) ? rootOf.get(cur) : cur; // reuse memoized root or the chain head
            chain.forEach(n => rootOf.set(n, root));
            if (!rootOf.has(g)) rootOf.set(g, root);
        });
        // Only classify "external" relative to a focus that is actually present; otherwise a
        // focus guid absent from the results would (wrongly) flag every node external.
        const homeRoot = (this._pgFocus && procSet.has(this._pgFocus)) ? (rootOf.get(this._pgFocus) || this._pgFocus) : null;
        const externalProcs = new Set();
        if (homeRoot) procSet.forEach(g => { if ((rootOf.get(g) || g) !== homeRoot) externalProcs.add(g); });

        // Shared object nodes: a leaf touched by processes in >= 2 DIFFERENT trees is a
        // reconnection bridge (that is the whole point -- linking trees). A leaf shared by two
        // processes within one tree is NOT a reconnection and stays a per-process chip.
        const sharedLeaves = new Set();
        leafOwners.forEach((owners, id) => {
            if (owners.size < 2) return;
            const rs = new Set();
            owners.forEach(o => rs.add(rootOf.get(o) || o));
            if (rs.size >= 2) sharedLeaves.add(id);
        });

        // Per-process reconnection links (for the table's link chip + click-to-highlight): every
        // process paired with each peer it shares a cross-tree object with, plus dropped/executed
        // file bridges. Bidirectional so either endpoint can surface its links.
        const linkInfo = new Map();  // guid -> [{type,label,peerGuid,peerHost,crossHost,info}]
        const linkSeen = new Map();  // guid -> Set(key) for O(1) dedup (not arr.some -> avoids O(k^2))
        const addLink = (g, type, label, peerGuid, info) => {
            if (!g || !peerGuid || g === peerGuid) return;
            let s = linkSeen.get(g); if (!s) { s = new Set(); linkSeen.set(g, s); }
            const key = type + '\x00' + label + '\x00' + peerGuid;
            if (s.has(key)) return;
            s.add(key);
            if (!linkInfo.has(g)) linkInfo.set(g, []);
            const ph = procHost.get(peerGuid) || '', gh = procHost.get(g) || '';
            linkInfo.get(g).push({ type, label, peerGuid, peerHost: ph, crossHost: !!(ph && gh && ph !== gh), info: info || null });
        };
        const MAX_LINK_OWNERS = 48; // a hot shared artifact touched by hundreds of procs: cap the
        // pairwise expansion (the bridge is still conveyed) so build stays bounded, not O(owners^2).
        sharedLeaves.forEach(id => {
            let owners = Array.from(leafOwners.get(id) || []);
            if (owners.length > MAX_LINK_OWNERS) owners = owners.slice(0, MAX_LINK_OWNERS);
            const meta = leafMeta.get(id) || {};
            const type = meta.type || this._pgTypeOf(id);
            owners.forEach(a => {
                // a's OWN log for this artifact, so clicking the link opens a's connection/dns/file
                // event (not the peer's process-creation log).
                const ae = leafIndex.get(a + '\x00' + type + '\x00' + id);
                const aInfo = ae ? ae.info : (logInfoById.get(id) || null);
                owners.forEach(b => { if ((rootOf.get(a) || a) !== (rootOf.get(b) || b)) addLink(a, type, meta.label || id, b, aInfo); });
            });
        });
        interactions.forEach((list, src) => list.forEach(it => {
            if (it.recon) { addLink(src, 'file', it.label, it.target, it.info); addLink(it.target, 'file', it.label, src, it.info); }
        }));

        return { procLabel, spawnKids, interactions, leafGroups, leafOwners, leafMeta, sharedLeaves, linkInfo,
            logInfoById, anomalyByNode, procMeta, procTime, procHost, procSet, roots, rootOf, homeRoot, externalProcs, ghostProcs };
    },

    // Parse pgr's "YYYY-MM-DD HH:MM:SS.mmm" (UTC, no tz) into epoch ms, or null.
    _pgParseTime(s) {
        if (!s) return null;
        const str = String(s);
        const iso = /[TZ]|[+-]\d\d:?\d\d$/.test(str) ? str : str.replace(' ', 'T') + 'Z';
        const t = Date.parse(iso);
        return isNaN(t) ? null : t;
    },
    // Absolute-aware time label: relative for recent events, absolute for old data (so a
    // year-old tree reads correctly, not "8760h ago"). Full timestamp goes in the title.
    _pgFmtTime(ms) {
        if (ms == null) return '';
        const now = Date.now(), d = new Date(ms), diff = now - ms;
        const MIN = 60000, HR = 3600000, DAY = 86400000;
        if (diff >= 0 && diff < 45000) return 'just now';
        if (diff >= 0 && diff < HR) return Math.round(diff / MIN) + 'm ago';
        if (diff >= 0 && diff < DAY) return Math.round(diff / HR) + 'h ago';
        const hm = d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', hour12: false });
        if (diff >= 0 && diff < 7 * DAY) return d.toLocaleDateString(undefined, { weekday: 'short' }) + ' ' + hm;
        const sameYear = d.getFullYear() === new Date(now).getFullYear();
        if (sameYear) return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' }) + ' ' + hm;
        return d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
    },
    // Age-independent gap between a node and its parent (how long after the parent it appeared).
    _pgFmtDelta(deltaMs) {
        if (deltaMs == null || isNaN(deltaMs)) return '';
        const s = deltaMs < 0 ? '-' : '+', a = Math.abs(deltaMs);
        if (a < 1000) return s + Math.round(a) + 'ms';
        if (a < 60000) return s + (a / 1000).toFixed(a < 10000 ? 1 : 0) + 's';
        if (a < 3600000) return s + Math.round(a / 60000) + 'm';
        if (a < 86400000) return s + (a / 3600000).toFixed(1) + 'h';
        return s + Math.round(a / 86400000) + 'd';
    },

    _pgOpenLog(info) {
        if (!info || !info.log_id || !window.LogDetail) return;
        const detailData = { log_id: info.log_id, timestamp: info.timestamp, fractal_id: info.fractal_id, _shard_num: info._shard_num };
        LogDetail.setContext([detailData], 0, false, 'search');
        LogDetail.show(detailData, false, 'search');
    },

    // Toolbar: Graph/Table segmented toggle + fit/zoom (graph-only). Mirrors .graph-toolbar.
    _pgBuildToolbar(graphHost, stage) {
        let bar = graphHost.querySelector('.graph-toolbar');
        if (bar) bar.remove();
        bar = document.createElement('div');
        bar.className = 'graph-toolbar';
        const m = this._pgModel;
        const procN = m.procSet.size, isTable = this._pgView === 'table';
        const hostN = m.procHost ? new Set(Array.from(m.procHost.values()).filter(Boolean)).size : 0;
        // Triage summary: cross-tree reconnections, rare (>=0.7) behaviors, and the peak anomaly,
        // so the reason-to-care is visible at a glance without scanning the canvas.
        const pairSet = new Set();
        if (m.linkInfo) m.linkInfo.forEach((links, g) => links.forEach(l => { const lo = g < l.peerGuid ? g : l.peerGuid, hi = g < l.peerGuid ? l.peerGuid : g; pairSet.add(lo + '\x00' + hi); }));
        const reconN = pairSet.size;
        let rareN = 0, maxAnom = 0;
        const tally = (a) => { if (!isNaN(a)) { if (a >= 0.7) rareN++; if (a > maxAnom) maxAnom = a; } };
        if (m.leafMeta) m.leafMeta.forEach(v => tally(v.anomaly));
        m.interactions.forEach(list => list.forEach(it => tally(it.anomaly)));
        const maxSev = this._pgSev(maxAnom);
        const sep = '<span class="graph-stat-separator"></span>';
        // When scores saturate near the top, shading switches to relative (spread across this
        // graph's own range) so contrast survives -- flag it so a "grey" node isn't misread as low.
        const relShade = this._pgSevScale && this._pgSevScale.rel
            ? sep + `<span class="graph-stat-item pg-stat-rel" title="Anomaly scores are saturated (diffusion or a thin baseline), so node color is shaded RELATIVE to this graph's range to keep contrast. The number on each node is still the true anomaly score.">relative shading</span>`
            : '';
        bar.innerHTML = `
            <div class="pg-view-toggle" role="tablist">
                <button class="pg-view-btn${this._pgView === 'graph' ? ' active' : ''}" data-view="graph" title="Diagonal process map">
                    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 19 19 5"/><circle cx="5" cy="19" r="2"/><circle cx="12" cy="12" r="2"/><circle cx="19" cy="5" r="2"/><path d="M12 12l4 6M12 12 8 8"/></svg>
                    <span>Graph</span>
                </button>
                <button class="pg-view-btn${this._pgView === 'table' ? ' active' : ''}" data-view="table" title="Indented process tree">
                    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="9" y1="6" x2="20" y2="6"/><line x1="9" y1="12" x2="20" y2="12"/><line x1="9" y1="18" x2="20" y2="18"/><path d="M4 5v14M4 12h3M4 6h3M4 18h3"/></svg>
                    <span>Table</span>
                </button>
            </div>
            <div class="graph-stats pg-stats-right"><span class="graph-stat-item"><span class="graph-stat-count">${procN}</span> processes</span>` +
                `${hostN ? sep + `<span class="graph-stat-item"><span class="graph-stat-count">${hostN}</span> host${hostN === 1 ? '' : 's'}</span>` : ''}` +
                `${reconN ? sep + `<span class="graph-stat-item pg-stat-recon" title="cross-tree reconnections"><span class="graph-stat-count">${reconN}</span> reconnect${reconN === 1 ? 'ion' : 'ions'}</span>` : ''}` +
                `${rareN ? sep + `<span class="graph-stat-item pg-stat-rare" title="rare/anomalous events (>= 0.70)"><span class="graph-stat-count">${rareN}</span> rare</span><span class="pg-anom pg-anom-${maxSev}" title="peak anomaly score">${maxAnom.toFixed(2)}</span>` : ''}` +
                relShade +
            `</div>
            <div class="pg-filters">
                <div class="pg-search-wrap">
                    <svg class="pg-search-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
                    <input type="text" class="pg-search" placeholder="Search nodes…" value="${(this._pgSearch || '').replace(/"/g, '&quot;')}">
                </div>
            </div>
            <div class="graph-controls pg-common-controls">
                <button class="toolbar-icon-btn" id="pgCopyIocBtn" title="Copy IOCs (IPs, domains, files) to clipboard"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg></button>
                <button class="toolbar-icon-btn" id="pgLegendBtn" title="Legend"><span class="pg-legend-q">?</span></button>
                <div class="pg-legend" hidden>
                    <div class="pg-legend-title">Anomaly</div>
                    <div class="pg-legend-row"><span class="pg-legend-swatch pg-sw-high"></span>High (rare / suspicious)</div>
                    <div class="pg-legend-row"><span class="pg-legend-swatch pg-sw-med"></span>Elevated</div>
                    <div class="pg-legend-row"><span class="pg-legend-swatch pg-sw-low"></span>Common</div>
                    <div class="pg-legend-title">Nodes</div>
                    <div class="pg-legend-row"><span class="pg-legend-swatch pg-sw-focus"></span>Start (queried)</div>
                    <div class="pg-legend-row"><span class="pg-legend-swatch pg-sw-ext"></span>Reconnected peer (other tree)</div>
                    <div class="pg-legend-row"><span class="pg-legend-swatch pg-sw-agg"></span>Collapsed similar processes (&times;N)</div>
                    <div class="pg-legend-row"><span class="pg-legend-swatch pg-sw-ghost"></span>Missing creation (parent not in time range)</div>
                    <div class="pg-legend-title">Edges</div>
                    <div class="pg-legend-row"><span class="pg-legend-line pg-ll-spawn"></span>Spawned</div>
                    <div class="pg-legend-row"><span class="pg-legend-line pg-ll-recon"></span>Reconnection (shared IP / domain / dropped file)</div>
                    <div class="pg-legend-note">Chips on a node count its files / connections / DNS / links.</div>
                </div>
            </div>
            <div class="graph-controls pg-graph-controls"${isTable ? ' style="display:none"' : ''}>
                <button class="toolbar-icon-btn" id="pgFitBtn" title="Fit to view"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"/></svg></button>
                <button class="toolbar-icon-btn" id="pgZoomInBtn" title="Zoom in"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/><path d="M11 8v6"/><path d="M8 11h6"/></svg></button>
                <button class="toolbar-icon-btn" id="pgZoomOutBtn" title="Zoom out"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/><path d="M8 11h6"/></svg></button>
            </div>`;
        graphHost.insertBefore(bar, stage);
        bar.querySelectorAll('.pg-view-btn').forEach(btn => btn.addEventListener('click', () => {
            const v = btn.dataset.view;
            if (v === this._pgView) return;
            this._pgView = v;
            bar.querySelectorAll('.pg-view-btn').forEach(b => b.classList.toggle('active', b.dataset.view === v));
            const gc = bar.querySelector('.pg-graph-controls'); if (gc) gc.style.display = v === 'table' ? 'none' : '';
            this._pgRender();
        }));
        const searchInput = bar.querySelector('.pg-search');
        if (searchInput) searchInput.addEventListener('input', Utils.debounce((e) => {
            this._pgSearch = e.target.value.trim();
            if (this._pgView === 'table') this._pgRender();
            else this._pgApplySearch();
        }, 160));
        bar.querySelector('#pgFitBtn')?.addEventListener('click', () => this._pgFit(true));
        bar.querySelector('#pgZoomInBtn')?.addEventListener('click', () => this._pgZoomBy(1.25));
        bar.querySelector('#pgZoomOutBtn')?.addEventListener('click', () => this._pgZoomBy(1 / 1.25));
        bar.querySelector('#pgCopyIocBtn')?.addEventListener('click', () => this._pgCopyIOCs());
        const legendBtn = bar.querySelector('#pgLegendBtn'), legend = bar.querySelector('.pg-legend');
        if (legendBtn && legend) {
            legendBtn.addEventListener('click', (e) => { e.stopPropagation(); legend.hidden = !legend.hidden; });
            document.addEventListener('click', (e) => { if (!legend.hidden && !legend.contains(e.target) && e.target !== legendBtn) legend.hidden = true; });
        }
    },

    // Extract the distinct indicators (external IPs, domains, written/dropped files) from the
    // current provenance subgraph and copy them, grouped, to the clipboard -- the standard SOC
    // hand-off (blocklist / threat-intel / ticket). Values are the raw artifacts, not abstracted.
    _pgCopyIOCs() {
        const m = this._pgModel; if (!m) return;
        const ips = new Set(), domains = new Set(), files = new Set();
        if (m.leafMeta) m.leafMeta.forEach(v => {
            const label = String(v.label || '').trim(); if (!label) return;
            if (v.type === 'net') ips.add(label);
            else if (v.type === 'dns') domains.add(label);
            else if (v.type === 'file') files.add(label);
        });
        const total = ips.size + domains.size + files.size;
        if (!total) { if (window.Toast) Toast.show('No IOCs in this graph', 'info'); return; }
        const sect = (title, set) => set.size ? title + ' (' + set.size + '):\n' + Array.from(set).sort().join('\n') + '\n' : '';
        const text = [sect('IPs', ips), sect('Domains', domains), sect('Files', files)].filter(Boolean).join('\n');
        const done = () => { if (window.Toast) Toast.show(`Copied ${total} IOC${total === 1 ? '' : 's'} to clipboard`, 'success'); };
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(done).catch(() => this._pgCopyFallback(text, done));
        } else this._pgCopyFallback(text, done);
    },
    _pgCopyFallback(text, done) {
        const ta = document.createElement('textarea');
        ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0';
        document.body.appendChild(ta); ta.select();
        try { document.execCommand('copy'); done(); } catch (_) { if (window.Toast) Toast.show('Copy failed', 'error'); }
        document.body.removeChild(ta);
    },

    _pgRender() {
        const networkDiv = document.getElementById('networkGraph');
        const stage = networkDiv.closest('.graph-stage');
        const graphDiv = stage && stage.querySelector('.pg-graph');
        const treeDiv = stage && stage.querySelector('.pg-tree');
        networkDiv.style.display = 'none';
        if (this._pgView === 'table') {
            if (graphDiv) graphDiv.style.display = 'none';
            if (treeDiv) { treeDiv.style.display = 'block'; this._pgRenderTree(treeDiv); }
        } else {
            if (treeDiv) treeDiv.style.display = 'none';
            if (graphDiv) { graphDiv.style.display = 'block'; this._pgRenderGraph(graphDiv); }
        }
    },

    // ---- Graph view: diagonal process-map (CrowdStrike / Elastic style) ----
    // Only PROCESS nodes are laid out; file/net/dns activity rides along as compact count
    // badges on each node. A tidy-tree + diagonal shear turns linear attack chains into a
    // clean down-right spine while branches fork off it; wide leaf-sibling fans collapse into
    // an expandable "N processes" node. Custom HTML nodes over an SVG edge layer, panned and
    // zoomed via a CSS transform on the canvas (no vis-network here).
    _pgApplyTransform(animate) {
        const c = this._pgCanvas; if (!c) return;
        c.style.transition = animate ? 'transform 260ms cubic-bezier(.4,0,.2,1)' : 'none';
        const vs = this._pgVS;
        c.style.transform = `translate(${vs.x}px, ${vs.y}px) scale(${vs.s})`;
        if (animate) setTimeout(() => { if (this._pgCanvas === c) c.style.transition = 'none'; }, 280);
    },
    _pgZoomBy(factor) {
        const host = this._pgGraphHost; if (!host) return;
        const vs = this._pgVS;
        const ns = Math.max(0.12, Math.min(3, vs.s * factor));
        const cx = host.clientWidth / 2, cy = host.clientHeight / 2;
        vs.x = cx - (cx - vs.x) * (ns / vs.s);
        vs.y = cy - (cy - vs.y) * (ns / vs.s);
        vs.s = ns;
        this._pgApplyTransform(true);
        this._pgDrawMinimap();
    },
    _pgFit(animate) {
        const host = this._pgGraphHost, b = this._pgBounds; if (!host || !b) return;
        const vw = host.clientWidth, vh = host.clientHeight;
        if (!vw || !vh || !b.w || !b.h) return;
        const pad = 46, vs = this._pgVS;
        const sByW = (vw - pad * 2) / b.w, sByH = (vh - pad * 2) / b.h;
        const full = Math.min(sByW, sByH, 1.2);
        if (full < 0.5) {
            // A tall outline: fit to width so labels stay readable, top-aligned, pan/scroll down.
            const s = Math.max(0.2, Math.min(sByW, 1.2));
            vs.s = s; vs.x = (vw - b.w * s) / 2; vs.y = pad;
        } else {
            vs.s = full; vs.x = (vw - b.w * full) / 2; vs.y = (vh - b.h * full) / 2;
        }
        this._pgApplyTransform(animate);
        this._pgDrawMinimap();
    },

    _pgRenderGraph(host) {
        const m = this._pgModel;
        const esc = Utils.escapeHtml;
        const min = this._pgMinAnomaly || 0;
        const passT = (a) => isNaN(a) || a >= min;
        const keepView = this._pgKeepView; this._pgKeepView = false;

        // Layout icons (kept inline so the renderer is self-contained and themeable).
        const ICON = {
            proc: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="3"/><rect x="9" y="9" width="6" height="6" rx="1"/><path d="M9 2v2M15 2v2M9 20v2M15 20v2M2 9h2M2 15h2M20 9h2M20 15h2"/></svg>',
            agg: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3 3 8l9 5 9-5-9-5Z"/><path d="m3 13 9 5 9-5M3 18l9 5 9-5" opacity=".55"/></svg>',
            file: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"/><path d="M14 3v5h5"/></svg>',
            net: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7h11a4 4 0 0 1 0 8H9m0 0 3-3m-3 3 3 3"/></svg>',
            dns: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c2.5 2.7 2.5 15.3 0 18M12 3c-2.5 2.7-2.5 15.3 0 18"/></svg>',
            inject: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h11m0 0-4-4m4 4-4 4"/><path d="M19 4v16"/></svg>',
            link: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 15 15 9"/><path d="M10.5 6.5 12 5a4 4 0 0 1 6 6l-1.5 1.5"/><path d="M13.5 17.5 12 19a4 4 0 0 1-6-6l1.5-1.5"/></svg>',
        };
        const sevOf = (a) => this._pgSev(a);
        const hotW = (a) => this._pgSevHot(a) ? 2.4 : 1.5;

        // 1) Layout = the spawn backbone as an indented outline: every process gets its OWN
        // row (y = preorder position) and x = depth. All leaves stay visible, labels never
        // collide, a linear chain reads as a diagonal spine, and a wide sibling group cascades
        // as an indented column. This is the CrowdStrike/Elastic process-tree construction.
        // Collapse-aware child map: a folded node reports no children, so its subtree is hidden.
        const collapsed = this._pgCollapsed;
        const expandedAggs = this._pgExpandedAggs || new Set();
        const aggMeta = this._pgAggMeta || new Map();
        const kidsOf = new Map();
        // A process reports its DISPLAY children (promoted individuals + aggregate nodes); a folded
        // node reports none. Each aggregate node in turn reports its members only when expanded, so
        // a collapsed "conhost.exe x30" is one leaf and an expanded one fans its members beneath it.
        m.spawnKids.forEach((kids, p) => kidsOf.set(p, collapsed.has(p) ? [] : this._pgDisplayChildren(p).map(c => c.id)));
        aggMeta.forEach((ag, aggId) => kidsOf.set(aggId, expandedAggs.has(aggId) ? ag.members.slice() : []));
        const rootList = (m.roots.length ? m.roots : Array.from(m.procSet)).slice();
        const pos = this._pgOutlineLayout(rootList, kidsOf);
        if (!pos.size) { host.innerHTML = '<div class="pg-empty">No processes in this graph.</div>'; return; }

        // Reconnection is drawn as ONE violet link between two linked processes (however many
        // artifacts they share), not as fan-out to shared object nodes -- that made a spiderweb.
        // The shared artifacts are revealed by clicking the link chip on the node. Unique
        // unordered pairs of cross-tree linked, both-visible processes.
        const linkPairs = new Map();
        if (m.linkInfo) m.linkInfo.forEach((links, a) => {
            if (!pos.has(a)) return;
            links.forEach(l => {
                if (!pos.has(l.peerGuid)) return;
                const lo = a < l.peerGuid ? a : l.peerGuid, hi = a < l.peerGuid ? l.peerGuid : a;
                const key = lo + '\x00' + hi;
                if (!linkPairs.has(key)) linkPairs.set(key, { a: lo, b: hi });
            });
        });

        let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
        pos.forEach(p => { if (p.x < minX) minX = p.x; if (p.x > maxX) maxX = p.x; if (p.y < minY) minY = p.y; if (p.y > maxY) maxY = p.y; });
        const PAD_L = 46, PAD_T = 40, PAD_R = 300, PAD_B = 56; // room for hex + right-hand labels
        pos.forEach(p => { p.x = p.x - minX + PAD_L; p.y = p.y - minY + PAD_T; });
        const W = (maxX - minX) + PAD_L + PAD_R, H = (maxY - minY) + PAD_T + PAD_B;
        this._pgBounds = { w: W, h: H };
        this._pgPositions = pos;

        // 2) Edges: spawn (solid backbone) + interactions (dashed cross-links), anomaly-coloured.
        // The anomaly score belongs to the EDGE (the parent->child transition), so it renders
        // as a small pill at each edge midpoint rather than on the node; low scores recede.
        const edgeSegs = []; // {x1,y1,x2,y2,a,sev,w,dash}
        m.spawnKids.forEach((kids, parent) => {
            const pp = pos.get(parent); if (!pp) return;
            this._pgDisplayChildren(parent).forEach(c => {
                const cp = pos.get(c.id); if (!cp) return;
                const a = c.kind === 'agg' ? (aggMeta.get(c.id) || {}).anomaly : m.anomalyByNode.get(c.id);
                edgeSegs.push({ x1: pp.x, y1: pp.y, x2: cp.x, y2: cp.y, a, sev: sevOf(a), w: hotW(a), dash: 0 });
            });
        });
        // Aggregate -> member edges, only while the aggregate is expanded.
        aggMeta.forEach((ag, aggId) => {
            if (!expandedAggs.has(aggId)) return;
            const ap = pos.get(aggId); if (!ap) return;
            ag.members.forEach(mm => {
                const cp = pos.get(mm); if (!cp) return;
                const a = m.anomalyByNode.get(mm);
                edgeSegs.push({ x1: ap.x, y1: ap.y, x2: cp.x, y2: cp.y, a, sev: sevOf(a), w: hotW(a), dash: 0 });
            });
        });
        m.interactions.forEach((list, src) => {
            const sp = pos.get(src); if (!sp) return;
            // reconnect_file bridges are folded into the single link edge below (via linkPairs).
            list.filter(it => passT(it.anomaly) && !it.recon).forEach(it => {
                const tp = pos.get(it.target); if (!tp) return;
                edgeSegs.push({ x1: sp.x, y1: sp.y, x2: tp.x, y2: tp.y, a: it.anomaly, sev: sevOf(it.anomaly), w: hotW(it.anomaly), dash: 1 });
            });
        });
        // One reconnection link per cross-tree pair, no arrow (it aggregates however many shared
        // artifacts/bridges the two processes have; the link chip reveals them).
        linkPairs.forEach(lp => {
            const sp = pos.get(lp.a), tp = pos.get(lp.b);
            edgeSegs.push({ x1: sp.x, y1: sp.y, x2: tp.x, y2: tp.y, a: NaN, sev: 'none', w: 1.6, dash: 1, recon: true, toObj: true });
        });
        // Straight segments: a linear chain is colinear (row == depth), so consecutive edges
        // align into one clean diagonal spine, exactly like the CrowdStrike/Elastic rail.
        // Endpoints are pulled back to the hex edge so the arrowhead sits just off the node.
        const pathFor = (e) => {
            const dx = e.x2 - e.x1, dy = e.y2 - e.y1, len = Math.hypot(dx, dy) || 1;
            const ux = dx / len, uy = dy / len;
            const x1 = e.x1 + ux * 19, y1 = e.y1 + uy * 19, x2 = e.x2 - ux * 21, y2 = e.y2 - uy * 21;
            if (e.recon) {
                // Gently bow reconnection bridges so they read as distinct links without the
                // heavy sweep that tangles a long cross-canvas curve.
                const mx = (x1 + x2) / 2, my = (y1 + y2) / 2, bow = Math.min(38, len * 0.1);
                return `M${x1.toFixed(1)},${y1.toFixed(1)} Q${(mx - uy * bow).toFixed(1)},${(my + ux * bow).toFixed(1)} ${x2.toFixed(1)},${y2.toFixed(1)}`;
            }
            return `M${x1.toFixed(1)},${y1.toFixed(1)} L${x2.toFixed(1)},${y2.toFixed(1)}`;
        };
        const marker = (sev) => `<marker id="pgar-${sev}" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" class="pg-arrow pg-e-${sev}"/></marker>`;
        let svg = `<svg class="pg-edge-layer" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}"><defs>${['high', 'med', 'low', 'none'].map(marker).join('')}</defs>`;
        edgeSegs.forEach(e => {
            const cls = `pg-e pg-e-${e.sev}${e.dash ? ' pg-e-dash' : ''}${e.recon ? ' pg-e-recon' : ''}`;
            const mk = (e.recon && e.toObj) ? '' : ` marker-end="url(#pgar-${e.sev})"`;
            svg += `<path d="${pathFor(e)}" class="${cls}" style="stroke-width:${e.w}"${mk}/>`;
        });
        svg += '</svg>';
        let elabels = '';
        edgeSegs.forEach(e => {
            if (isNaN(e.a) || e.recon) return; // recon bridges are long curves; a midpoint pill floats and clutters
            const mx = ((e.x1 + e.x2) / 2).toFixed(1), my = ((e.y1 + e.y2) / 2).toFixed(1);
            elabels += `<span class="pg-elabel pg-anom pg-anom-${e.sev}" style="left:${mx}px;top:${my}px" title="anomaly ${e.a.toFixed(2)}">${e.a.toFixed(2)}</span>`;
        });

        // 3) Nodes: hexagon chip (anomaly-coloured) + name + clickable activity chips.
        const match = new Map();
        const miniBadge = (t, n, guid) => n ? `<span class="pg-mb pg-mb-${t} pg-mb-click" data-chip="${esc(guid)}" data-ctype="${t}" title="${n} ${this._pgLeafNoun(t)} — click to inspect">${ICON[t]}${n}</span>` : '';
        let nodesHtml = '';
        pos.forEach((p, id) => {
            // Aggregate (fan-out) node: a folded group of similar siblings. Its own +/- reveals or
            // hides the members (which then lay out as its children).
            if (aggMeta.has(id)) {
                const ag = aggMeta.get(id);
                const aa = ag.anomaly;
                const exp = expandedAggs.has(id);
                match.set(id, String(ag.image || '').toLowerCase());
                const tog = `<button class="pg-toggle${exp ? '' : ' pg-toggle-plus'}" data-aggtoggle="${esc(id)}" title="${exp ? 'Collapse' : 'Expand'} ${ag.count} ${esc(this._pgShort(ag.image))} processes">${exp ? '−' : '+'}</button>`;
                nodesHtml += `<div class="pg-node pg-node-agg pg-sev-${sevOf(aa)}" data-agg="${esc(id)}" style="left:${(p.x - 18).toFixed(1)}px;top:${p.y.toFixed(1)}px" title="${ag.count} similar ${esc(String(ag.image))} processes collapsed${isNaN(aa) ? '' : ' — peak anomaly ' + aa.toFixed(2)}\nclick to ${exp ? 'collapse' : 'expand'}">` +
                    `<span class="pg-hexwrap"><span class="pg-hex">${ICON.agg}</span>${tog}</span>` +
                    `<span class="pg-node-info"><span class="pg-node-name">${esc(this._pgShort(ag.image))}</span><span class="pg-node-sub"><span class="pg-agg-count" title="${ag.count} collapsed">×${ag.count}</span></span></span></div>`;
                return;
            }
            const name = m.procLabel.get(id) || id;
            const a = m.anomalyByNode.get(id);
            const grp = m.leafGroups.get(id) || { file: [], net: [], dns: [] };
            // Shared (cross-tree) leaves are represented by the reconnection LINK, so they drop
            // out of the per-process file/net/dns chips; unshared activity still shows as chips.
            const notShared = (x) => !(m.sharedLeaves && m.sharedLeaves.has(x.id));
            const fc = grp.file.filter(x => passT(x.anomaly) && notShared(x)).length;
            const nc = grp.net.filter(x => passT(x.anomaly) && notShared(x)).length;
            const dc = grp.dns.filter(x => passT(x.anomaly) && notShared(x)).length;
            // Reconnect interactions (file bridges) are drawn as the link edge, so they don't
            // also count toward the process-interaction chip.
            const inj = (m.interactions.get(id) || []).filter(it => passT(it.anomaly) && !it.recon).length;
            // Reconnection link chip: number of shared artifacts/bridges to visible peers.
            const lc = ((m.linkInfo && m.linkInfo.get(id)) || []).filter(l => pos.has(l.peerGuid)).length;
            const info = m.logInfoById.get(id);
            const dl = info ? ` data-log='${esc(JSON.stringify(info))}'` : '';
            match.set(id, String(name).toLowerCase());
            const badges = miniBadge('file', fc, id) + miniBadge('net', nc, id) + miniBadge('dns', dc, id) +
                (inj ? `<span class="pg-mb pg-mb-inject pg-mb-click" data-chip="${esc(id)}" data-ctype="inject" title="${inj} process interaction${inj === 1 ? '' : 's'} — click to inspect">${ICON.inject}${inj}</span>` : '') +
                (lc ? `<span class="pg-mb pg-mb-link pg-mb-click" data-chip="${esc(id)}" data-ctype="link" title="${lc} reconnection${lc === 1 ? '' : 's'} — click to see what links">${ICON.link}${lc}</span>` : '');
            const sub = badges ? `<span class="pg-node-sub">${badges}</span>` : '';
            // +/- fold toggle for processes that spawned children.
            const kidN = (m.spawnKids.get(id) || []).length;
            const isCol = collapsed.has(id);
            const toggle = kidN ? `<button class="pg-toggle${isCol ? ' pg-toggle-plus' : ''}" data-toggle="${esc(id)}" title="${isCol ? 'Expand' : 'Collapse'} ${kidN} child process${kidN === 1 ? '' : 'es'}">${isCol ? '+' : '−'}</button>` : '';
            const ext = m.externalProcs && m.externalProcs.has(id);
            const ghost = m.ghostProcs && m.ghostProcs.has(id);
            const host = m.procHost && m.procHost.get(id);
            // Reconnected peers on a different host get a small muted host tag so a cross-computer
            // hop reads at a glance; same-host reconnections stay unadorned.
            const homeHost = this._pgFocus && m.procHost ? m.procHost.get(this._pgFocus) : null;
            const hostLabel = (ext && host && (!homeHost || host !== homeHost)) ? `<span class="pg-node-host" title="host">${esc(host)}</span>` : '';
            // A ghost keeps its place as the connecting parent but reads as a data gap: a hollow
            // (outlined, unfilled) hex and the raw guid (all we know). The "why" lives in the tooltip
            // and the legend, not a per-node label (which was noisy on a big tree).
            const cls = `pg-node pg-sev-${sevOf(a)}${id === this._pgFocus ? ' pg-focus' : ''}${isCol ? ' pg-collapsed' : ''}${ext ? ' pg-node-external' : ''}${ghost ? ' pg-node-ghost' : ''}`;
            nodesHtml += `<div class="${cls}"${dl} data-id="${esc(id)}" style="left:${(p.x - 18).toFixed(1)}px;top:${p.y.toFixed(1)}px" title="${esc(String(name))}${ghost ? '\nmissing process creation (not in the selected time range)' : ''}${host ? '\nhost: ' + esc(String(host)) : ''}${ext ? '\nreconnected from another tree' : ''}${info ? '\nclick to view source log' : ''}">` +
                `<span class="pg-hexwrap"><span class="pg-hex">${ICON.proc}</span>${toggle}</span>` +
                `<span class="pg-node-info"><span class="pg-node-name">${esc(this._pgShort(name))}</span>${hostLabel}${sub}</span></div>`;
        });

        // 4) Assemble. Fill the available viewport height; a transformed canvas holds edges,
        // edge labels + nodes; a minimap and a slide-out chip drawer float on top.
        const gtop = host.getBoundingClientRect().top;
        host.style.height = Math.max(460, Math.floor(window.innerHeight - gtop - 28)) + 'px';
        host.innerHTML = `<div class="pg-canvas" style="width:${W}px;height:${H}px">${svg}${elabels}${nodesHtml}</div>` +
            `<canvas class="graph-minimap pg-minimap" width="200" height="130"></canvas>` +
            `<div class="pg-drawer" hidden></div>`;
        this._pgGraphHost = host;
        this._pgCanvas = host.querySelector('.pg-canvas');
        this._pgNodeEls = host.querySelectorAll('.pg-node');
        this._pgMatch = match;
        this._pgApplyTransform(false);
        this._pgBindGraphInput(host);
        if (this._pgSearch) this._pgApplySearch();
        if (keepView) { this._pgApplyTransform(false); this._pgDrawMinimap(); }
        else requestAnimationFrame(() => {
            // First render: frame the queried start node rather than the whole graph.
            if (this._pgFocus && this._pgCenterOn(this._pgFocus, 0.95)) return;
            this._pgFit(false);
        });
    },

    // Center the viewport on a node (queried start) at a readable scale, placed in the upper
    // third so its descendants below stay in view.
    _pgCenterOn(id, scale) {
        const host = this._pgGraphHost, pos = this._pgPositions;
        if (!host || !pos || !pos.has(id)) return false;
        const p = pos.get(id), vs = this._pgVS;
        vs.s = scale || 0.95;
        vs.x = host.clientWidth / 2 - p.x * vs.s;
        vs.y = host.clientHeight * 0.38 - p.y * vs.s;
        this._pgApplyTransform(true);
        this._pgDrawMinimap();
        return true;
    },

    // Indented-outline layout (the CrowdStrike/Elastic process-tree construction): a preorder
    // DFS where every node takes its OWN row (y) and x = depth. A linear chain therefore lands
    // on a diagonal (row == depth); a wide sibling group stacks as an indented column with its
    // parent-to-child edges fanning down the left gutter, so leaf labels never collide.
    _pgOutlineLayout(roots, kidsOf) {
        const ROW_H = 52, INDENT = 112;
        let row = 0;
        const pos = new Map(), seen = new Set();
        const visit = (id, depth) => {
            if (seen.has(id)) return; seen.add(id);
            pos.set(id, { x: depth * INDENT, y: row * ROW_H }); row++;
            (kidsOf.get(id) || []).forEach(k => visit(k, depth + 1));
        };
        // roots = processes with no spawn parent, so this covers interaction-only orphans too;
        // a collapsed node's kidsOf is empty, so its descendants stay hidden (not re-rooted).
        roots.forEach(r => visit(r, 0));
        return pos;
    },

    // Pan (drag background) + zoom (wheel toward cursor) + node click, bound once per render.
    // Handlers read this._pgVS live (never a captured copy) so _pgFit replacing it can't
    // desync zoom/pan.
    _pgBindGraphInput(host) {
        host.onwheel = (e) => {
            e.preventDefault();
            const vs = this._pgVS;
            const rect = host.getBoundingClientRect();
            const mx = e.clientX - rect.left, my = e.clientY - rect.top;
            const ns = Math.max(0.12, Math.min(3, vs.s * (e.deltaY < 0 ? 1.12 : 1 / 1.12)));
            vs.x = mx - (mx - vs.x) * (ns / vs.s);
            vs.y = my - (my - vs.y) * (ns / vs.s);
            vs.s = ns;
            this._pgApplyTransform(false);
            this._pgDrawMinimap();
        };
        let dragging = false, moved = false, sx = 0, sy = 0, ox = 0, oy = 0;
        host.onpointerdown = (e) => {
            moved = false; // reset here so a click after a pan is never mistaken for a drag
            // Don't pan/capture when the press starts on a node, the minimap, or the drawer --
            // capturing the pointer would swallow their own clicks (close button, rows).
            if (e.target.closest('.pg-node') || e.target.closest('.pg-minimap') || e.target.closest('.pg-drawer')) return;
            dragging = true; sx = e.clientX; sy = e.clientY; ox = this._pgVS.x; oy = this._pgVS.y;
            host.classList.add('pg-panning');
            try { host.setPointerCapture(e.pointerId); } catch (_) { }
        };
        host.onpointermove = (e) => {
            if (!dragging) return;
            const dx = e.clientX - sx, dy = e.clientY - sy;
            if (Math.abs(dx) + Math.abs(dy) > 3) moved = true;
            this._pgVS.x = ox + dx; this._pgVS.y = oy + dy;
            this._pgApplyTransform(false);
            this._pgDrawMinimap();
        };
        const endDrag = () => { dragging = false; host.classList.remove('pg-panning'); };
        host.onpointerup = endDrag;
        host.onpointercancel = endDrag;
        host.onclick = (e) => {
            const tog = e.target.closest('.pg-toggle');
            if (tog) { // toggles are point-clicks, never drags -- always honor them
                if (tog.dataset.aggtoggle) { this._pgToggleAgg(tog.dataset.aggtoggle); return; }
                const g = tog.dataset.toggle;
                if (this._pgCollapsed.has(g)) this._pgCollapsed.delete(g); else this._pgCollapsed.add(g);
                this._pgKeepView = true;
                this._pgRenderGraph(host);
                return;
            }
            const chip = e.target.closest('.pg-mb-click');
            if (chip) { this._pgOpenDrawer(chip.dataset.chip, chip.dataset.ctype); return; }
            if (moved) return;
            const node = e.target.closest('.pg-node'); if (!node) return;
            // An aggregate node expands/collapses its members; a process node opens the in-graph
            // detail drawer (keeps the busy canvas uncluttered). Table view uses the global panel.
            if (node.dataset.agg) { this._pgToggleAgg(node.dataset.agg); return; }
            if (node.dataset.id) this._pgOpenNodeDrawer(node.dataset.id);
        };
        this._pgBindMinimap(host);
    },

    // Slide-out drawer: the file/network/dns (or interaction) values for one process, shown
    // in-graph so the artifacts stay visible without exploding them into nodes. Rows open the
    // source log. This is the interim view until NoDoze object-mediated reconnection promotes
    // these to real graph edges.
    _pgOpenDrawer(guid, type) {
        const host = this._pgGraphHost, m = this._pgModel;
        const drawer = host && host.querySelector('.pg-drawer');
        if (!drawer) return;
        const esc = Utils.escapeHtml;
        const min = this._pgMinAnomaly || 0;
        const passT = (a) => isNaN(a) || a >= min;
        const proc = m.procLabel.get(guid) || guid;
        let items, heading;
        if (type === 'link') {
            const links = (m.linkInfo && m.linkInfo.get(guid)) || [];
            const kind = (t) => t === 'file' ? 'dropped & ran' : t === 'net' ? 'shared IP' : t === 'dns' ? 'shared domain' : 'shared';
            items = links.map(l => ({ label: l.label, anomaly: NaN, info: l.info || m.logInfoById.get(l.peerGuid), sub: kind(l.type) + ' · ' + this._pgShort(m.procLabel.get(l.peerGuid) || l.peerGuid), host: l.crossHost ? l.peerHost : '', peerGuid: l.peerGuid, peerName: this._pgShort(m.procLabel.get(l.peerGuid) || l.peerGuid) }));
            heading = 'Reconnections';
        } else if (type === 'inject') {
            items = (m.interactions.get(guid) || []).filter(it => passT(it.anomaly)).map(it => ({ label: it.label || it.target, anomaly: it.anomaly, info: it.info, tag: it.type }));
            heading = 'Process interactions';
        } else {
            const grp = m.leafGroups.get(guid) || { file: [], net: [], dns: [] };
            items = (grp[type] || []).filter(x => passT(x.anomaly)).map(x => ({ label: x.label, anomaly: x.anomaly, info: x.info }));
            heading = type === 'file' ? 'File activity' : type === 'net' ? 'Network activity' : 'DNS activity';
        }
        const sevOf = (a) => this._pgSev(a);
        items.sort((x, y) => (isNaN(y.anomaly) ? 0 : y.anomaly) - (isNaN(x.anomaly) ? 0 : x.anomaly));
        const rows = items.map(it => {
            const dl = it.info ? ` data-log='${esc(JSON.stringify(it.info))}'` : '';
            const pill = isNaN(it.anomaly) ? '' : `<span class="pg-anom pg-anom-${sevOf(it.anomaly)}">${it.anomaly.toFixed(2)}</span>`;
            const hostChip = it.host ? `<span class="pg-host-chip" title="on another host">${esc(String(it.host))}</span>` : '';
            const val = esc(String(it.label || ''));
            // Two-line layout when there's a descriptor (reconnections): the actual value gets the
            // full width up top, the "shared IP / dropped & ran · peer" indicator sits muted below.
            if (it.sub) {
                // Reconnection rows get a "Dest" button that centers the graph on the peer process
                // (the row's own log opens on the row body). The shared Source button is in the head.
                const destBtn = it.peerGuid ? `<button class="pg-foci-btn" data-focus="${esc(it.peerGuid)}" title="Center the graph on ${esc(String(it.peerName || ''))}">Dest</button>` : '';
                return `<div class="pg-drawer-row pg-drawer-row2"${dl} title="${val}"><div class="pg-drawer-vline"><span class="pg-drawer-val pg-drawer-val-full">${val}</span>${pill}</div>` +
                    `<div class="pg-drawer-subline"><span class="pg-drawer-subtag">${esc(String(it.sub))}</span>${hostChip}${destBtn}</div></div>`;
            }
            const tag = it.tag ? `<span class="pg-tag">${esc(it.tag)}</span>` : '';
            return `<div class="pg-drawer-row"${dl} title="${val}"><span class="pg-drawer-val">${val}</span>${tag}${hostChip}${pill}</div>`;
        }).join('') || '<div class="pg-drawer-empty">No matching activity.</div>';
        // For reconnections, a Source button on the head centers the graph on the process these
        // links belong to (every row shares it), so it isn't repeated per row.
        const srcBtn = (type === 'link') ? `<button class="pg-foci-btn pg-foci-src" data-focus="${esc(guid)}" title="Center the graph on ${esc(String(proc))}">Source</button>` : '';
        drawer.innerHTML = `<div class="pg-drawer-head"><div class="pg-drawer-title"><span class="pg-drawer-heading">${esc(heading)}</span><span class="pg-drawer-proc" title="${esc(String(proc))}">${esc(this._pgShort(proc))}</span></div>` +
            `${srcBtn}<button class="pg-drawer-close" title="Close">&times;</button></div>` +
            `<div class="pg-drawer-count">${items.length} ${items.length === 1 ? 'entry' : 'entries'}</div>` +
            `<div class="pg-drawer-body">${rows}</div>`;
        drawer.hidden = false;
        requestAnimationFrame(() => drawer.classList.add('open'));
        const mm = host.querySelector('.pg-minimap'); if (mm) mm.style.opacity = '0';
        drawer.querySelector('.pg-drawer-close')?.addEventListener('click', () => this._pgCloseDrawer());
        drawer.querySelectorAll('[data-focus]').forEach(b => b.addEventListener('click', (e) => {
            e.stopPropagation();
            this._pgFocusNode(b.dataset.focus);
        }));
        drawer.querySelectorAll('.pg-drawer-row[data-log]').forEach(r => r.addEventListener('click', () => {
            try { this._pgOpenLog(JSON.parse(r.dataset.log)); } catch (_) { }
        }));
    },

    // Center the graph on a node and pulse it (from the reconnection drawer's Source/Dest buttons).
    // Falls back to a toast when the target isn't in the current view (e.g. a peer left collapsed).
    _pgFocusNode(id) {
        if (!id) return;
        this._pgCloseDrawer();
        if (this._pgCenterOn(id, Math.max(0.7, (this._pgVS && this._pgVS.s) || 0.9))) {
            const host = this._pgGraphHost;
            const sel = (window.CSS && CSS.escape) ? CSS.escape(id) : id;
            const el = host && host.querySelector(`.pg-node[data-id="${sel}"]`);
            if (el) { el.classList.remove('pg-pulse'); void el.offsetWidth; el.classList.add('pg-pulse'); setTimeout(() => el.classList.remove('pg-pulse'), 1300); }
        } else if (window.Toast) {
            Toast.show('That process is not in the current view (expand its branch to see it)', 'info');
        }
    },
    _pgCloseDrawer() {
        const host = this._pgGraphHost;
        const drawer = host && host.querySelector('.pg-drawer');
        if (!drawer) return;
        drawer.classList.remove('open');
        const mm = host.querySelector('.pg-minimap'); if (mm) mm.style.opacity = '';
        setTimeout(() => { if (!drawer.classList.contains('open')) drawer.hidden = true; }, 220);
    },

    // Shared-object node click: list the processes that reconnect through this artifact (the
    // rare IP/domain both trees touched), each opening its source log.
    // Node click in the graph: slide out the process's log detail in-graph (so the busy
    // canvas isn't covered by the global panel), with "Analyze from here" to re-root pgr().
    async _pgOpenNodeDrawer(guid) {
        const host = this._pgGraphHost, m = this._pgModel;
        const drawer = host && host.querySelector('.pg-drawer');
        if (!drawer) return;
        const esc = Utils.escapeHtml;
        const name = m.procLabel.get(guid) || guid;
        const a = m.anomalyByNode.get(guid);
        const meta = m.procMeta.get(guid) || {};
        const info = m.logInfoById.get(guid) || null;
        const t = m.procTime.get(guid);
        const sevOf = (x) => this._pgSev(x);
        const pill = isNaN(a) ? '' : `<span class="pg-anom pg-anom-${sevOf(a)}">${a.toFixed(2)}</span>`;
        const kv = (k, v) => `<div class="pg-drawer-kv"><span class="pg-kv-k">${esc(k)}</span><span class="pg-kv-v" title="${esc(String(v))}">${esc(String(v))}</span></div>`;
        const summary = [];
        if (meta.user) summary.push(['user', meta.user]);
        if (meta.cmd) summary.push(['command line', meta.cmd]);
        if (t != null) summary.push(['time', new Date(t).toISOString().replace('T', ' ').replace('Z', ' UTC')]);
        drawer.innerHTML =
            `<div class="pg-drawer-head"><div class="pg-drawer-title"><span class="pg-drawer-heading">Process</span><span class="pg-drawer-proc" title="${esc(String(name))}">${esc(this._pgShort(name))}</span></div>` +
            `<button class="pg-drawer-close" title="Close">&times;</button></div>` +
            `<div class="pg-drawer-actions"><button class="pg-drawer-btn pg-drawer-btn-primary" data-analyze="1" title="Re-run pgr() rooted at this process">Analyze from here</button>` +
            `${info ? '<button class="pg-drawer-btn" data-viewlog="1" title="Open the full log detail panel">Full log</button>' : ''}</div>` +
            `${pill ? `<div class="pg-drawer-count">anomaly ${pill}</div>` : ''}` +
            `<div class="pg-drawer-body">${summary.length ? `<div class="pg-kv-group">${summary.map(([k, v]) => kv(k, v)).join('')}</div>` : ''}` +
            `<div class="pg-drawer-fields">${info ? '<div class="pg-drawer-empty">Loading log details…</div>' : '<div class="pg-drawer-empty">No source log for this node.</div>'}</div></div>`;
        drawer.hidden = false;
        requestAnimationFrame(() => drawer.classList.add('open'));
        const mm = host.querySelector('.pg-minimap'); if (mm) mm.style.opacity = '0';
        drawer.querySelector('.pg-drawer-close')?.addEventListener('click', () => this._pgCloseDrawer());
        drawer.querySelector('[data-analyze]')?.addEventListener('click', () => this._pgAnalyzeFrom(guid));
        drawer.querySelector('[data-viewlog]')?.addEventListener('click', () => this._pgOpenLog(info));
        if (info && info.log_id) {
            const fieldsEl = drawer.querySelector('.pg-drawer-fields');
            try {
                const params = new URLSearchParams({ log_id: info.log_id, fractal_id: info.fractal_id || '', timestamp: info.timestamp || '', shard_num: info._shard_num || '' });
                const resp = await fetch(`/api/v1/logs/fields?${params}`);
                const data = await resp.json();
                if (fieldsEl && data && data.success && data.fields) {
                    const keys = Object.keys(data.fields).sort();
                    fieldsEl.innerHTML = keys.map(k => kv(k, data.fields[k] == null ? '' : data.fields[k])).join('') || '<div class="pg-drawer-empty">No fields.</div>';
                } else if (fieldsEl) {
                    fieldsEl.innerHTML = '<div class="pg-drawer-empty">No fields available.</div>';
                }
            } catch (e) {
                if (fieldsEl) fieldsEl.innerHTML = '<div class="pg-drawer-empty">Failed to load log details.</div>';
            }
        }
    },

    // "Analyze from here": re-root the pgr() traversal at guid (mirrors graph()'s Walk Up).
    _pgAnalyzeFrom(guid) {
        const qi = document.getElementById('queryInput');
        if (!qi) return;
        const cur = this.currentQuery || qi.value || '';
        // Escape for a BQL double-quoted string, and use a function replacement so a "$" in the
        // guid is not treated as a String.replace token (guids are braced hex, but be bulletproof).
        const q = String(guid).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        const re = /(\bpgr\s*\([^)]*?\bstart\s*=\s*)("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|[^,)\s]+)/i;
        qi.value = re.test(cur) ? cur.replace(re, (m, p1) => p1 + '"' + q + '"') : `pgr(start="${q}") | pgraph()`;
        qi.dispatchEvent(new Event('input', { bubbles: true }));
        const btn = document.getElementById('executeBtn');
        if (btn) setTimeout(() => btn.click(), 0);
        if (window.Toast) Toast.show('Analyzing from ' + this._pgShort(this._pgModel.procLabel.get(guid) || guid), 'info');
    },

    // Minimap: node dots + a viewport rectangle, in the canvas coordinate space, click/drag to pan.
    _pgDrawMinimap() {
        const host = this._pgGraphHost, b = this._pgBounds, pos = this._pgPositions;
        const mm = host && host.querySelector('.pg-minimap');
        if (!mm || !b || !pos) return;
        const cv = ThemeManager.getCSSVar;
        const ctx = mm.getContext('2d');
        const w = mm.width, h = mm.height, pad = 8;
        ctx.clearRect(0, 0, w, h);
        ctx.fillStyle = cv('--bg-secondary') || '#111';
        ctx.fillRect(0, 0, w, h);
        const scale = Math.min((w - 2 * pad) / b.w, (h - 2 * pad) / b.h);
        const offX = (w - b.w * scale) / 2, offY = (h - b.h * scale) / 2;
        // Dots carry the same anomaly shading as the canvas (adaptive absolute/relative), so the
        // minimap doubles as a heat overview -- hot clusters are findable at a glance. Cold dots
        // are drawn first and hot ones last (a touch larger) so severity reads on top.
        const m = this._pgModel;
        const neutral = cv('--graph-node-neutral') || '#6b7280';
        const sevColor = { high: cv('--error') || '#e5484d', med: cv('--warning') || '#f5a623', low: neutral, none: neutral };
        const rank = { high: 2, med: 1, low: 0, none: 0 };
        const dots = [];
        pos.forEach((p, id) => dots.push({ p, sev: this._pgSev(m && m.anomalyByNode ? m.anomalyByNode.get(id) : NaN) }));
        dots.sort((a, b) => rank[a.sev] - rank[b.sev]);
        dots.forEach(d => {
            ctx.fillStyle = sevColor[d.sev] || neutral;
            const r = d.sev === 'high' ? 2.0 : d.sev === 'med' ? 1.7 : 1.4;
            ctx.beginPath(); ctx.arc(offX + d.p.x * scale, offY + d.p.y * scale, r, 0, Math.PI * 2); ctx.fill();
        });
        // Viewport rect: which canvas region is currently visible in the host.
        const vs = this._pgVS;
        const vx = -vs.x / vs.s, vy = -vs.y / vs.s;
        const vw = host.clientWidth / vs.s, vh = host.clientHeight / vs.s;
        ctx.strokeStyle = cv('--accent-primary') || '#8b7cc8';
        ctx.lineWidth = 1.5;
        ctx.strokeRect(offX + vx * scale, offY + vy * scale, vw * scale, vh * scale);
        mm._map = { scale, offX, offY };
    },
    _pgBindMinimap(host) {
        const mm = host.querySelector('.pg-minimap'); if (!mm) return;
        const nav = (ev) => {
            const map = mm._map; if (!map) return;
            const rect = mm.getBoundingClientRect();
            const cxCanvas = (ev.clientX - rect.left - map.offX) / map.scale;
            const cyCanvas = (ev.clientY - rect.top - map.offY) / map.scale;
            const vs = this._pgVS;
            vs.x = host.clientWidth / 2 - cxCanvas * vs.s;
            vs.y = host.clientHeight / 2 - cyCanvas * vs.s;
            this._pgApplyTransform(true);
            this._pgDrawMinimap();
        };
        let down = false;
        mm.onpointerdown = (e) => { down = true; nav(e); e.stopPropagation(); try { mm.setPointerCapture(e.pointerId); } catch (_) { } };
        mm.onpointermove = (e) => { if (down) nav(e); };
        mm.onpointerup = () => { down = false; };
    },

    // Live node search: dim non-matches, accent matches. No rebuild, so pan/zoom is kept.
    _pgApplySearch() {
        if (this._pgView === 'table' || !this._pgNodeEls) return;
        const term = (this._pgSearch || '').toLowerCase();
        this._pgNodeEls.forEach(el => {
            const id = el.dataset.id || el.dataset.agg;
            const hit = !term || ((this._pgMatch.get(id) || '').includes(term));
            el.classList.toggle('pg-dim', !!term && !hit);
            el.classList.toggle('pg-match', !!term && hit);
        });
    },

    // ---- Tree view: CrowdStrike-style indented process tree ----
    // Elbow guide connectors on the left, process-name rows with per-type activity-count
    // badges (children / files / connections / domains), an anomaly severity pill, and a
    // chevron to fold subtrees. Clicking a leaf badge drills its artifacts inline.
    _pgRenderTree(container) {
        const m = this._pgModel;
        if (!this._pgTreeCollapsed) this._pgTreeCollapsed = new Set(); // guids folded
        if (!this._pgLeafOpen) this._pgLeafOpen = new Set();           // "guid:type" drilled-in
        // Show per-row host only when the result spans >1 host (a reconnection pulled in another
        // computer's tree); a single-host tree stays clean. Cross-host rows get emphasized.
        const multiHost = m.procHost ? new Set(Array.from(m.procHost.values()).filter(Boolean)).size > 1 : false;
        const focusHost = this._pgFocus && m.procHost ? m.procHost.get(this._pgFocus) : null;
        const esc = Utils.escapeHtml;
        const S = (v) => esc(this._pgShort(v));
        const ICON = {
            proc: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6 3v12M18 9a3 3 0 0 0-3 3H9m9-3a3 3 0 0 1 0 6"/><circle cx="6" cy="18" r="2.5"/><circle cx="6" cy="4" r="1.5"/><circle cx="20" cy="9" r="1.5"/></svg>',
            file: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"/><path d="M14 3v5h5"/></svg>',
            net: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7h11a4 4 0 0 1 0 8H9m0 0 3-3m-3 3 3 3"/></svg>',
            dns: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c2.5 2.7 2.5 15.3 0 18M12 3c-2.5 2.7-2.5 15.3 0 18"/></svg>',
            inject: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h11m0 0-4-4m4 4-4 4"/><path d="M19 4v16"/></svg>',
            gear: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="5" width="16" height="14" rx="2"/><path d="M8 9h5M8 13h8"/></svg>',
            link: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 15 15 9"/><path d="M10.5 6.5 12 5a4 4 0 0 1 6 6l-1.5 1.5"/><path d="M13.5 17.5 12 19a4 4 0 0 1-6-6l1.5-1.5"/></svg>',
        };
        const rows = [];
        const term = (this._pgSearch || '').toLowerCase();
        const min = this._pgMinAnomaly || 0;
        const passT = (a) => isNaN(a) || a >= min;
        const fGroups = (guid) => { const g = m.leafGroups.get(guid) || { file: [], net: [], dns: [] }; return { file: g.file.filter(x => passT(x.anomaly)), net: g.net.filter(x => passT(x.anomaly)), dns: g.dns.filter(x => passT(x.anomaly)) }; };
        const fInter = (guid) => (m.interactions.get(guid) || []).filter(it => passT(it.anomaly));
        const mLeaf = (x) => !term || String(x.label || '').toLowerCase().includes(term);
        const mInter = (it) => !term || String(it.label || it.target).toLowerCase().includes(term);
        const mNode = (guid) => !term || String(m.procLabel.get(guid) || guid).toLowerCase().includes(term);
        const subMatch = new Map();
        const computeMatch = (guid, path) => {
            if (subMatch.has(guid)) return subMatch.get(guid);
            if (path.has(guid)) return false;
            path.add(guid);
            const g = fGroups(guid);
            let hit = mNode(guid) || g.file.some(mLeaf) || g.net.some(mLeaf) || g.dns.some(mLeaf) || fInter(guid).some(mInter);
            (m.spawnKids.get(guid) || []).forEach(k => { if (computeMatch(k, path)) hit = true; });
            path.delete(guid);
            subMatch.set(guid, hit);
            return hit;
        };
        const guidesHtml = (anc, isLast) => {
            let h = '';
            for (let i = 0; i < anc.length; i++) {
                if (i === anc.length - 1) h += `<span class="pg-g pg-g-elbow${isLast ? ' pg-g-last' : ''}"></span>`;
                else h += `<span class="pg-g${anc[i] ? ' pg-g-v' : ''}"></span>`;
            }
            return h;
        };
        const anomalyPill = (a) => {
            if (isNaN(a)) return '<span class="pg-anom-spacer"></span>';
            const cls = this._pgSev(a);
            return `<span class="pg-anom pg-anom-${cls}" title="anomaly ${a.toFixed(2)}">${a.toFixed(2)}</span>`;
        };
        const hl = (text) => {
            const s = this._pgShort(text);
            if (!term) return esc(s);
            const j = s.toLowerCase().indexOf(term);
            if (j < 0) return esc(s);
            return esc(s.slice(0, j)) + '<mark class="pg-hl">' + esc(s.slice(j, j + term.length)) + '</mark>' + esc(s.slice(j + term.length));
        };
        const badge = (type, count, guid, drill) => {
            if (!count) return ''; // hide zero-count chips to cut noise
            const on = drill && this._pgLeafOpen.has(guid + ':' + type);
            const attr = (drill && count > 0) ? ` data-drill="${esc(guid)}" data-type="${type}"` : '';
            const noun = type === 'proc' ? 'child process' : type === 'file' ? 'file' : type === 'net' ? 'connection' : type === 'link' ? 'reconnection' : 'domain';
            return `<span class="pg-badge pg-badge-${type}${on ? ' pg-on' : ''}"${attr} title="${count} ${noun}${count === 1 ? '' : 's'}${drill && count > 0 ? ' — click to expand' : ''}">${ICON[type]}<b>${count}</b></span>`;
        };
        const leafChildRow = (anc, isLast, type, x) => {
            const dl = x.info ? ` data-log='${esc(JSON.stringify(x.info))}'` : '';
            rows.push(`<div class="pg-row pg-leaf"${dl}>${guidesHtml(anc, isLast)}<span class="pg-icon pg-icon-${type}">${ICON[type]}</span><span class="pg-name" title="${esc(x.label || '')}">${hl(x.label)}</span>${anomalyPill(x.anomaly)}</div>`);
        };
        // Total spawn descendants of a node (shown as +N when it's collapsed).
        const descCache = new Map();
        const descCount = (guid, path) => {
            if (descCache.has(guid)) return descCache.get(guid);
            path = path || new Set();
            if (path.has(guid)) return 0;
            path.add(guid);
            let n = 0;
            (m.spawnKids.get(guid) || []).forEach(k => { n += 1 + descCount(k, path); });
            path.delete(guid);
            descCache.set(guid, n);
            return n;
        };
        const seen = new Set();
        const walk = (guid, anc, isLast, parentGuid) => {
            const cyc = seen.has(guid);
            const groups = fGroups(guid);          // anomaly-threshold applied
            const interAll = fInter(guid);
            const kidsAll = m.spawnKids.get(guid) || [];
            const collapsed = !term && this._pgTreeCollapsed.has(guid); // search auto-expands
            const leafTotal = groups.file.length + groups.net.length + groups.dns.length;
            const childCount = kidsAll.length + interAll.length;
            const hasKids = !cyc && (childCount > 0 || leafTotal > 0);
            const info = m.logInfoById.get(guid);
            const dl = info ? ` data-log='${esc(JSON.stringify(info))}'` : '';
            const chev = hasKids ? `<button class="pg-chev${collapsed ? ' pg-collapsed' : ''}" data-guid="${esc(guid)}" title="${collapsed ? 'Expand' : 'Collapse'}"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="m9 6 6 6-6 6"/></svg></button>` : '<span class="pg-chev pg-chev-none"></span>';
            const linkN = (m.linkInfo && m.linkInfo.get(guid) || []).length;
            const badges = `<span class="pg-badges">${badge('proc', childCount, guid, false)}${badge('file', groups.file.length, guid, true)}${badge('net', groups.net.length, guid, true)}${badge('dns', groups.dns.length, guid, true)}${badge('link', linkN, guid, true)}</span>`;
            // Name + optional command-line/user subline (triage context). +N when folded.
            const dc = collapsed ? descCount(guid) : 0;
            const descHtml = dc > 0 ? ` <span class="pg-desc" title="${dc} hidden descendant process${dc === 1 ? '' : 'es'}">+${dc}</span>` : '';
            const meta = m.procMeta.get(guid) || {};
            const rowHost = m.procHost && m.procHost.get(guid);
            const showHost = multiHost && rowHost;
            let subline = '';
            if (meta.cmd || meta.user || showHost) {
                const u = meta.user ? `<span class="pg-sub-user" title="user">${esc(meta.user)}</span>` : '';
                const h = showHost ? `<span class="pg-sub-host${rowHost !== focusHost ? ' pg-sub-host-x' : ''}" title="host: ${esc(String(rowHost))}">${esc(String(rowHost))}</span>` : '';
                const c = meta.cmd ? `<span class="pg-sub-cmd" title="${esc(meta.cmd)}">${esc(meta.cmd)}</span>` : '';
                subline = `<span class="pg-subline">${u}${h}${c}</span>`;
            }
            const ghost = m.ghostProcs && m.ghostProcs.has(guid);
            // Ghost rows read as a data gap via the muted mono name + row style (see legend), not a
            // per-row text tag. The tooltip still explains on hover.
            const nameTitle = ghost ? 'missing process creation (not in the selected time range): ' + (m.procLabel.get(guid) || guid) : (m.procLabel.get(guid) || guid);
            const nameCell = `<span class="pg-name-wrap"><span class="pg-name" title="${esc(nameTitle)}">${hl(m.procLabel.get(guid) || guid)}${cyc ? ' <em class="pg-muted">(cycle)</em>' : ''}${descHtml}</span>${subline}</span>`;
            // Time (absolute-aware) + gap since parent.
            const t = m.procTime.get(guid);
            const pt = parentGuid != null ? m.procTime.get(parentGuid) : null;
            let timeCell = '';
            if (t != null) {
                const delta = pt != null ? this._pgFmtDelta(t - pt) : '';
                const full = new Date(t).toISOString().replace('T', ' ').replace('Z', ' UTC');
                timeCell = `<span class="pg-time-cell">${delta ? `<span class="pg-delta" title="time after parent">${delta}</span>` : ''}<span class="pg-time" title="${esc(full)}">${esc(this._pgFmtTime(t))}</span></span>`;
            }
            const a = m.anomalyByNode.get(guid);
            // No per-row severity accent: the anomaly pill already carries severity, and a colored
            // left border on every elevated row was visually noisy on a large tree.
            const focusCls = guid === this._pgFocus ? ' pg-focus-row' : '';
            rows.push(`<div class="pg-row pg-proc${cyc ? ' pg-cycle' : ''}${ghost ? ' pg-proc-ghost' : ''}${focusCls}" data-guid="${esc(guid)}"${dl}>${guidesHtml(anc, isLast)}${chev}<span class="pg-icon pg-icon-proc">${ICON.gear}</span>${nameCell}${badges}${timeCell}${anomalyPill(a)}</div>`);
            if (cyc || collapsed) return;
            seen.add(guid);
            // rendered children: interactions, drilled/matching artifact leaves, child processes
            const interactions = term ? interAll.filter(mInter) : interAll;
            const openTypes = term
                ? ['file', 'net', 'dns'].filter(t => groups[t].some(mLeaf))
                : ['file', 'net', 'dns'].filter(t => this._pgLeafOpen.has(guid + ':' + t) && groups[t].length);
            const leafItems = [];
            openTypes.forEach(t => (term ? groups[t].filter(mLeaf) : groups[t]).forEach(x => leafItems.push({ t, x })));
            // Fan-out: collapse large same-image sibling groups into aggregate rows. Search bypasses
            // it (every match must be reachable), so in term mode we expand to individual children.
            const childEntries = term
                ? kidsAll.filter(k => computeMatch(k, new Set())).map(id => ({ kind: 'proc', id }))
                : this._pgDisplayChildren(guid);
            // Reconnection links drill: shared cross-tree artifact -> the peer process.
            const linkList = (m.linkInfo && m.linkInfo.get(guid)) || [];
            const linkItems = (!term && this._pgLeafOpen.has(guid + ':link')) ? linkList : [];
            // Non-reconnect interactions render as their own rows; reconnect (file bridge) ones
            // are surfaced through the link drill instead, so they aren't shown twice.
            const shownInter = interactions.filter(it => !it.recon);
            const childAnc = anc.slice(); if (childAnc.length) childAnc[childAnc.length - 1] = !isLast;
            const total = shownInter.length + leafItems.length + linkItems.length + childEntries.length;
            let idx = 0;
            shownInter.forEach(it => {
                const dl2 = it.info ? ` data-log='${esc(JSON.stringify(it.info))}'` : '';
                rows.push(`<div class="pg-row pg-leaf pg-inject"${dl2}>${guidesHtml(childAnc.concat([false]), ++idx === total)}<span class="pg-icon pg-icon-inject">${ICON.inject}</span><span class="pg-name">${hl(it.label || it.target)}</span><span class="pg-tag">${esc(it.type)}</span>${anomalyPill(it.anomaly)}</div>`);
            });
            leafItems.forEach(({ t, x }) => leafChildRow(childAnc.concat([false]), ++idx === total, t, x));
            linkItems.forEach(l => {
                const peerName = m.procLabel.get(l.peerGuid) || l.peerGuid;
                const licon = ICON[l.type] || ICON.link;
                const kindTxt = l.type === 'file' ? 'dropped & ran' : l.type === 'net' ? 'shared IP' : l.type === 'dns' ? 'shared domain' : 'shared';
                const hostChip = l.crossHost && l.peerHost ? `<span class="pg-host-chip" title="on another host">${esc(String(l.peerHost))}</span>` : '';
                rows.push(`<div class="pg-row pg-leaf pg-link-item" data-peer="${esc(l.peerGuid)}" title="Reconnects to ${esc(String(peerName))}${l.crossHost ? ' on ' + esc(String(l.peerHost)) : ''} — click to jump">${guidesHtml(childAnc.concat([false]), ++idx === total)}<span class="pg-icon pg-icon-link">${licon}</span><span class="pg-name">${hl(l.label)}</span><span class="pg-link-peer">${hl(peerName)}</span>${hostChip}<span class="pg-tag pg-tag-recon">${kindTxt}</span></div>`);
            });
            childEntries.forEach(c => {
                const isLastEntry = ++idx === total;
                const cAnc = childAnc.concat([false]);
                if (c.kind === 'proc') { walk(c.id, cAnc, isLastEntry, guid); return; }
                // Aggregate row: collapsed fan of similar siblings, expandable in place.
                const ag = this._pgAggMeta.get(c.id); if (!ag) return;
                const exp = this._pgExpandedAggs && this._pgExpandedAggs.has(c.id);
                const chevA = `<button class="pg-chev${exp ? '' : ' pg-collapsed'}" data-aggtoggle="${esc(c.id)}" title="${exp ? 'Collapse' : 'Expand'} ${ag.count} similar processes"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="m9 6 6 6-6 6"/></svg></button>`;
                rows.push(`<div class="pg-row pg-agg-row" data-agg="${esc(c.id)}" title="${ag.count} similar ${esc(String(ag.image))} processes — click to ${exp ? 'collapse' : 'expand'}">${guidesHtml(cAnc, isLastEntry)}${chevA}<span class="pg-icon pg-icon-agg">${ICON.gear}</span><span class="pg-name-wrap"><span class="pg-name">${hl(ag.image)} <span class="pg-agg-count">×${ag.count}</span></span></span>${anomalyPill(ag.anomaly)}</div>`);
                if (exp) {
                    const mAnc = cAnc.slice(); mAnc[mAnc.length - 1] = !isLastEntry;
                    ag.members.forEach((mm, j) => walk(mm, mAnc.concat([false]), j === ag.members.length - 1, guid));
                }
            });
            seen.delete(guid);
        };
        const rootsAll = m.roots.length ? m.roots : Array.from(m.procSet);
        const roots = term ? rootsAll.filter(r => computeMatch(r, new Set())) : rootsAll;
        roots.forEach((r, i) => walk(r, [], i === roots.length - 1, null));

        container.innerHTML = `<div class="pg-tree-scroll" role="tree" tabindex="0">${rows.join('') || '<div class="pg-empty">No processes in this graph.</div>'}</div>`;
        const scroll = container.querySelector('.pg-tree-scroll');
        container.querySelectorAll('.pg-chev[data-guid]').forEach(btn => btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const g = btn.dataset.guid;
            if (this._pgTreeCollapsed.has(g)) this._pgTreeCollapsed.delete(g); else this._pgTreeCollapsed.add(g);
            this._pgTreeSelGuid = g;
            this._pgRenderTree(container);
        }));
        container.querySelectorAll('.pg-badge[data-drill]').forEach(b => b.addEventListener('click', (e) => {
            e.stopPropagation();
            const key = b.dataset.drill + ':' + b.dataset.type;
            if (this._pgLeafOpen.has(key)) this._pgLeafOpen.delete(key); else this._pgLeafOpen.add(key);
            this._pgRenderTree(container);
        }));
        // Selection (row highlight) doubles as the keyboard cursor; persists across re-renders
        // by guid so folding/unfolding keeps your place.
        const clearHl = () => container.querySelectorAll('.pg-row.pg-link-hl').forEach(r => r.classList.remove('pg-link-hl'));
        const rowByGuid = (g) => g ? container.querySelector(`.pg-row.pg-proc[data-guid="${(window.CSS && CSS.escape) ? CSS.escape(g) : g}"]`) : null;
        const selectRow = (row, open) => {
            container.querySelectorAll('.pg-row.pg-kbsel').forEach(r => r.classList.remove('pg-kbsel'));
            clearHl();
            if (!row) return;
            row.classList.add('pg-kbsel');
            this._pgTreeSelGuid = row.dataset.guid || null;
            // Subtly highlight the rows this process reconnects to (click-to-see-what-links).
            const g = row.dataset.guid;
            if (g && m.linkInfo && m.linkInfo.has(g)) {
                new Set(m.linkInfo.get(g).map(l => l.peerGuid)).forEach(pg => { const pr = rowByGuid(pg); if (pr) pr.classList.add('pg-link-hl'); });
            }
            row.scrollIntoView({ block: 'nearest' });
            if (open && row.dataset.log) { try { this._pgOpenLog(JSON.parse(row.dataset.log)); } catch (e) { /* ignore */ } }
        };
        container.querySelectorAll('.pg-row').forEach(row => row.addEventListener('click', () => {
            // An aggregate row expands/collapses its collapsed similar-sibling members in place.
            if (row.classList.contains('pg-agg-row')) { if (row.dataset.agg) this._pgToggleAgg(row.dataset.agg, container); return; }
            // A link drill item jumps to (and flashes) its peer process row.
            if (row.dataset.peer) {
                const pr = rowByGuid(row.dataset.peer);
                if (pr) { clearHl(); pr.classList.add('pg-link-hl'); pr.scrollIntoView({ block: 'center' }); }
                return;
            }
            selectRow(row, true);
        }));
        if (this._pgTreeSelGuid) {
            const r = container.querySelector(`.pg-row[data-guid="${(window.CSS && CSS.escape) ? CSS.escape(this._pgTreeSelGuid) : this._pgTreeSelGuid}"]`);
            if (r) r.classList.add('pg-kbsel');
        }
        if (scroll) scroll.addEventListener('keydown', (e) => {
            const list = Array.from(container.querySelectorAll('.pg-row'));
            if (!list.length) return;
            const cur = container.querySelector('.pg-row.pg-kbsel');
            const i = cur ? list.indexOf(cur) : -1;
            const g = cur && cur.dataset.guid;
            if (e.key === 'ArrowDown') { e.preventDefault(); selectRow(list[Math.min(list.length - 1, i + 1)] || list[0]); }
            else if (e.key === 'ArrowUp') { e.preventDefault(); selectRow(list[i <= 0 ? 0 : i - 1]); }
            else if (e.key === 'Enter') { e.preventDefault(); if (cur) selectRow(cur, true); }
            else if (e.key === 'ArrowRight' && g && this._pgTreeCollapsed.has(g)) { e.preventDefault(); this._pgTreeCollapsed.delete(g); this._pgTreeSelGuid = g; this._pgWantTreeFocus = true; this._pgRenderTree(container); }
            else if (e.key === 'ArrowLeft' && g) {
                e.preventDefault();
                if (!this._pgTreeCollapsed.has(g) && (m.spawnKids.get(g) || []).length) { this._pgTreeCollapsed.add(g); this._pgTreeSelGuid = g; this._pgWantTreeFocus = true; this._pgRenderTree(container); }
            }
        });
        if (this._pgWantTreeFocus && scroll) { scroll.focus(); this._pgWantTreeFocus = false; }
    },

    // mesh() renders an undirected, weighted, bidirectional network (Arkime-style
    // connections) with a force-directed layout. It shares the graph toolbar/detail
    // chrome and #networkGraph container but swaps in physics and network semantics.
    renderMesh(results) {
        const chartCanvas = document.getElementById('resultsChart');
        const networkDiv = document.getElementById('networkGraph');
        if (!networkDiv) return;

        if (chartCanvas) chartCanvas.style.display = 'none';
        networkDiv.style.display = 'block';

        if (this.currentChart) {
            this.currentChart.destroy();
            this.currentChart = null;
        }

        const cfg = this.chartConfig || {};
        const srcField = cfg.srcField;
        const dstField = cfg.dstField;
        if (!srcField || !dstField) return;

        const weightField = cfg.weightField || '_count';
        const sizeField = cfg.sizeField || '_count';
        let colorMode = cfg.color || 'auto';
        const directed = cfg.directed === true;
        const limit = cfg.limit || 100;
        const cv = ThemeManager.getCSSVar;

        const fields = this.fieldOrder || Object.keys(results[0] || {});
        const specifiedLabels = cfg.labels || [];
        const reserved = new Set([srcField, dstField, weightField, sizeField]);
        const labelFields = specifiedLabels.length > 0
            ? specifiedLabels
            : fields.filter(f => !reserved.has(f));
        const limitedResults = results.slice(0, limit);
        const truncated = results.length > limitedResults.length;

        const toNum = (v) => {
            const n = Number(v);
            return isFinite(n) ? n : 0;
        };

        // Build the node/edge model. Nodes are shared across src and dst (an IP
        // that is both a source and a destination is a single node).
        const nodeMap = new Map();   // id -> { degree, sizeSum, isSrc, isDst, details, neighbors:Map }
        const ensureNode = (id) => {
            let n = nodeMap.get(id);
            if (!n) {
                n = { degree: 0, sizeSum: 0, isSrc: false, isDst: false, details: {}, neighbors: new Map() };
                nodeMap.set(id, n);
            }
            return n;
        };
        const edgeList = []; // { from, to, weight }

        limitedResults.forEach((row) => {
            const s = row[srcField];
            const d = row[dstField];
            if (s == null || s === '' || d == null || d === '') return;
            const sId = String(s), dId = String(d);
            const w = Math.max(toNum(row[weightField]) || 1, 0.0001);
            const sizeVal = toNum(row[sizeField]) || 1;

            const sn = ensureNode(sId); sn.isSrc = true;
            const dn = ensureNode(dId); dn.isDst = true;
            sn.degree++; dn.degree++;
            sn.sizeSum += sizeVal; dn.sizeSum += sizeVal;
            sn.neighbors.set(dId, (sn.neighbors.get(dId) || 0) + w);
            dn.neighbors.set(sId, (dn.neighbors.get(sId) || 0) + w);

            labelFields.forEach(f => {
                if (row[f] != null && row[f] !== '') {
                    if (sn.details[f] === undefined) sn.details[f] = row[f];
                }
            });
            edgeList.push({ from: sId, to: dId, weight: w });
        });

        if (nodeMap.size === 0) return;

        let maxDegree = 0;
        nodeMap.forEach(n => { if (n.degree > maxDegree) maxDegree = n.degree; });

        // Coloring. 'auto' (default) colours by subnet when node IDs look like IPs,
        // else falls back to the degree ramp, so mesh() looks good for network AND
        // non-network data. 'subnet' (optional /bits, e.g. subnet/16) buckets by IP
        // CIDR block using the same tag palette + top-8 + legend + neutral overflow as
        // graph(); 'degree' is a clean grey->amber->red intensity ramp (no rainbow);
        // 'role' two-tones src vs dst; a field name buckets by that field's top-8.
        // Connection count is always encoded as node size, so color stays for meaning.
        const neutralColor = cv('--graph-node-neutral');
        const accentColor = cv('--accent-primary');
        const srcColor = '#3b82f6';
        const dstColor = '#f59e0b';
        const heatColor = MeshColor.heat;

        if (colorMode === 'auto') colorMode = MeshColor.autoMode([...nodeMap.keys()]);
        // subnet or subnet/<bits> (defaults to /24 for IPv4, /64 for IPv6).
        const subnetMatch = /^subnet(?:[/_-]?(\d{1,3}))?$/.exec(colorMode);
        const subnetBits = subnetMatch ? (subnetMatch[1] ? +subnetMatch[1] : 24) : null;
        const isSubnet = subnetMatch !== null;
        if (isSubnet) colorMode = 'subnet';

        // Categorical coloring (subnet or a field): bucket every node, keep the
        // top-8 buckets by frequency, palette them, and send the rest to neutral.
        const isCategorical = colorMode !== 'degree' && colorMode !== 'role';
        const catKeyOf = (id, n) => {
            if (isSubnet) return MeshColor.subnetKey(id, subnetBits);
            const v = n.details[colorMode];
            return (v != null && v !== '') ? String(v).toLowerCase() : null;
        };
        let catColorOf = null;
        this._meshTopKeys = [];
        this._meshHasOverflow = false;
        if (isCategorical) {
            const freq = new Map();
            nodeMap.forEach((n, id) => {
                const k = catKeyOf(id, n);
                if (k) freq.set(k, (freq.get(k) || 0) + 1);
            });
            const topKeys = [...freq.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8).map(e => e[0]);
            const topSet = new Set(topKeys);
            catColorOf = (id, n) => {
                const k = catKeyOf(id, n);
                return (k && topSet.has(k)) ? Utils.tagColorFor(k) : neutralColor;
            };
            this._meshTopKeys = topKeys;
            this._meshHasOverflow = freq.size > topKeys.length;
        }

        const nodeColor = (id, n) => {
            if (colorMode === 'role') return n.isSrc ? srcColor : dstColor;
            if (colorMode === 'degree') return heatColor(maxDegree > 1 ? (n.degree - 1) / (maxDegree - 1) : 0);
            return catColorOf(id, n);
        };

        const nodes = new vis.DataSet();
        const edges = new vis.DataSet();
        const baseColors = new Map();

        const labelFor = (id, n) => {
            if (specifiedLabels.length > 0) {
                const parts = specifiedLabels.map(f => n.details[f]).filter(v => v != null && v !== '');
                if (parts.length > 0) {
                    const joined = parts.join(' | ');
                    return joined.length > 30 ? joined.substring(0, 30) + '…' : joined;
                }
            }
            return id.length > 24 ? id.substring(0, 22) + '…' : id;
        };

        nodeMap.forEach((n, id) => {
            const fill = nodeColor(id, n);
            baseColors.set(id, fill);
            const tooltipLines = Object.entries(n.details)
                .map(([k, v]) => `<div class="graph-tooltip-row"><span class="graph-tooltip-key">${Utils.escapeHtml(k)}</span><span class="graph-tooltip-val">${Utils.escapeHtml(String(v))}</span></div>`)
                .join('');
            const titleEl = document.createElement('div');
            titleEl.innerHTML = `<div class="graph-tooltip"><div class="graph-tooltip-header">${Utils.escapeHtml(id)}</div><div class="graph-tooltip-row"><span class="graph-tooltip-key">connections</span><span class="graph-tooltip-val">${n.degree}</span></div>${tooltipLines}</div>`;
            nodes.add({
                id,
                label: labelFor(id, n),
                title: titleEl,
                value: Math.max(n.sizeSum, 1),
                color: {
                    background: fill,
                    border: fill,
                    highlight: { background: fill, border: accentColor },
                    hover: { background: fill, border: accentColor }
                }
            });
        });

        edgeList.forEach((e, i) => {
            edges.add({ id: i, from: e.from, to: e.to, value: e.weight });
        });

        // -- Toolbar (shares .graph-toolbar styling) --
        const graphHost = networkDiv.closest('.chart-container') || networkDiv.parentElement;
        let graphToolbar = graphHost.querySelector('.graph-toolbar');
        if (graphToolbar) graphToolbar.remove();

        let legendHtml = '';
        if (colorMode === 'role') {
            legendHtml = `
                <span class="graph-legend-item"><span class="graph-legend-dot" style="background:${srcColor}"></span>source</span>
                <span class="graph-legend-item"><span class="graph-legend-dot" style="background:${dstColor}"></span>destination</span>`;
        } else if (colorMode === 'degree') {
            legendHtml = `
                <span class="graph-legend-item"><span class="graph-legend-dot" style="background:${heatColor(0)}"></span>fewer</span>
                <span class="graph-legend-item"><span class="graph-legend-dot" style="background:${heatColor(0.5)}"></span></span>
                <span class="graph-legend-item"><span class="graph-legend-dot" style="background:${heatColor(1)}"></span>more connections</span>`;
        } else {
            const truncKey = (k) => k.length > 18 ? k.substring(0, 17) + '…' : k;
            legendHtml = (this._meshTopKeys || []).map(k =>
                `<span class="graph-legend-item" title="${Utils.escapeHtml(k)}"><span class="graph-legend-dot" style="background:${Utils.tagColorFor(k)}"></span>${Utils.escapeHtml(truncKey(k))}</span>`
            ).join('');
            if (this._meshHasOverflow) {
                legendHtml += `<span class="graph-legend-item"><span class="graph-legend-dot" style="background:${neutralColor}"></span>other</span>`;
            }
        }

        graphToolbar = document.createElement('div');
        graphToolbar.className = 'graph-toolbar';
        graphToolbar.innerHTML = `
            <div class="graph-stats">
                <span class="graph-stat-item"><span class="graph-stat-count" id="meshNodeCount">${nodes.length}</span> nodes</span>
                <span class="graph-stat-separator"></span>
                <span class="graph-stat-item"><span class="graph-stat-count" id="meshEdgeCount">${edges.length}</span> edges</span>
                ${truncated ? '<span class="graph-stat-separator"></span><span class="graph-stat-item" title="Increase limit= to show more">truncated to ' + limit + '</span>' : ''}
            </div>
            <div class="graph-legend">${legendHtml}</div>
            <div class="graph-search">
                <input type="number" min="0" step="1" id="meshMinWeight" class="graph-search-input" style="width:110px" placeholder="min weight">
                <input type="text" id="meshNodeSearch" class="graph-search-input" placeholder="Search nodes...">
            </div>
            <div class="graph-controls">
                <button class="toolbar-icon-btn" id="meshFitBtn" title="Fit to view">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"/></svg>
                </button>
                <button class="toolbar-icon-btn" id="meshZoomInBtn" title="Zoom in">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/><path d="M11 8v6"/><path d="M8 11h6"/></svg>
                </button>
                <button class="toolbar-icon-btn" id="meshZoomOutBtn" title="Zoom out">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/><path d="M8 11h6"/></svg>
                </button>
                <span class="graph-toolbar-sep"></span>
                <button class="toolbar-icon-btn" id="meshExportBtn" title="Export as PNG">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                </button>
            </div>
        `;
        let stage = graphHost.querySelector('.graph-stage');
        if (!stage) {
            stage = document.createElement('div');
            stage.className = 'graph-stage';
            graphHost.insertBefore(stage, networkDiv);
            stage.appendChild(networkDiv);
        }
        stage.style.display = 'flex';
        graphHost.insertBefore(graphToolbar, stage);

        // -- Detail panel (docked flex sibling) --
        let detailPanel = stage.querySelector('.graph-detail-panel');
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
        stage.appendChild(detailPanel);

        // Size to fit the viewport now that the toolbar/stage are in place.
        networkDiv.style.height = this.fitGraphHeight(networkDiv, nodeMap.size, 24) + 'px';

        // -- Create the force-directed network --
        const options = {
            layout: { hierarchical: { enabled: false } },
            physics: {
                enabled: true,
                solver: 'barnesHut',
                // High damping + soft springs + capped velocity => smooth glide,
                // not bouncy oscillation. minVelocity settles micro-jitter quickly.
                barnesHut: { gravitationalConstant: -18000, centralGravity: 0.35, springLength: 200, springConstant: 0.02, damping: 0.5, avoidOverlap: 0.2 },
                maxVelocity: 24,
                minVelocity: 0.9,
                timestep: 0.4,
                stabilization: { enabled: true, iterations: 300, updateInterval: 25, fit: true }
            },
            interaction: {
                dragNodes: true, dragView: true, zoomView: true, zoomSpeed: 1.0,
                hover: true, selectConnectedEdges: true, multiselect: false,
                keyboard: { enabled: false }, navigationButtons: false, tooltipDelay: 200,
                hideEdgesOnDrag: false, zoomExtentOnStabilize: false
            },
            nodes: {
                shape: 'dot',
                scaling: { min: 8, max: 42, label: { enabled: false } },
                borderWidth: 2,
                chosen: true,
                font: { size: 11, color: cv('--graph-label'), face: 'Inter', vadjust: -4, strokeWidth: 3, strokeColor: cv('--graph-label-stroke') }
            },
            edges: {
                color: { color: cv('--graph-edge'), opacity: 0.45, highlight: accentColor, hover: cv('--graph-edge') },
                arrows: { to: { enabled: directed, scaleFactor: 0.55, type: 'arrow' } },
                scaling: { min: 0.8, max: 9, label: { enabled: false } },
                smooth: { enabled: true, type: 'continuous', roundness: 0.4 },
                chosen: true
            },
            configure: { enabled: false }
        };

        networkDiv.style.pointerEvents = 'auto';
        networkDiv.style.touchAction = 'auto';
        this.currentChart = new vis.Network(networkDiv, { nodes, edges }, options);

        // Freeze physics once settled so the layout stays still and CPU idles.
        this.currentChart.once('stabilizationIterationsDone', () => {
            this.currentChart.setOptions({ physics: false });
        });

        // Best of both worlds: run physics only while a node is actively dragged
        // (so the neighborhood springs interactively), then let it settle briefly
        // and freeze again so the graph never churns CPU at rest.
        let meshSettleTimer = null;
        this.currentChart.on('dragStart', (params) => {
            if (!params.nodes || params.nodes.length === 0) return;
            if (meshSettleTimer) { clearTimeout(meshSettleTimer); meshSettleTimer = null; }
            // Resume live from current positions (no re-stabilization jump).
            this.currentChart.setOptions({ physics: { enabled: true, stabilization: false } });
        });
        this.currentChart.on('dragEnd', (params) => {
            if (!params.nodes || params.nodes.length === 0) return;
            if (meshSettleTimer) clearTimeout(meshSettleTimer);
            meshSettleTimer = setTimeout(() => {
                if (this.currentChart) this.currentChart.setOptions({ physics: false });
                meshSettleTimer = null;
            }, 1200);
        });
        setTimeout(() => {
            if (this.currentChart) this.currentChart.fit({ animation: { duration: 400, easingFunction: 'easeInOutQuad' }, padding: 40 });
        }, 400);

        // -- Minimap overlay (mirrors graph()): whole-graph thumbnail + viewport
        // rectangle; click/drag it to pan the main view. Dot size scales by degree. --
        let minimap = networkDiv.querySelector('.graph-minimap');
        if (minimap) minimap.remove();
        minimap = document.createElement('canvas');
        minimap.className = 'graph-minimap';
        minimap.width = 240;
        minimap.height = 160;
        networkDiv.appendChild(minimap);
        const mmCtx = minimap.getContext('2d');
        const drawMinimap = () => {
            if (!this.currentChart || !minimap.isConnected) return;
            const positions = this.currentChart.getPositions();
            const ids = Object.keys(positions);
            const w = minimap.width, h = minimap.height;
            mmCtx.clearRect(0, 0, w, h);
            mmCtx.fillStyle = cv('--bg-secondary');
            mmCtx.fillRect(0, 0, w, h);
            if (ids.length === 0) return;
            let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
            ids.forEach(id => { const p = positions[id]; if (p.x < minX) minX = p.x; if (p.x > maxX) maxX = p.x; if (p.y < minY) minY = p.y; if (p.y > maxY) maxY = p.y; });
            const pad = 14;
            const spanX = Math.max(1, maxX - minX), spanY = Math.max(1, maxY - minY);
            const scale = Math.min((w - 2 * pad) / spanX, (h - 2 * pad) / spanY);
            const offX = (w - spanX * scale) / 2, offY = (h - spanY * scale) / 2;
            const toMM = (x, y) => ({ x: offX + (x - minX) * scale, y: offY + (y - minY) * scale });
            ids.forEach(id => {
                const p = positions[id];
                const m = toMM(p.x, p.y);
                mmCtx.fillStyle = baseColors.get(id) || neutralColor;
                const deg = (nodeMap.get(id) || {}).degree || 0;
                const r = maxDegree > 1 ? 1.5 + 1.7 * ((deg - 1) / (maxDegree - 1)) : 1.8;
                mmCtx.beginPath();
                mmCtx.arc(m.x, m.y, r, 0, Math.PI * 2);
                mmCtx.fill();
            });
            const tl = this.currentChart.DOMtoCanvas({ x: 0, y: 0 });
            const br = this.currentChart.DOMtoCanvas({ x: networkDiv.clientWidth, y: networkDiv.clientHeight });
            const a = toMM(tl.x, tl.y), b = toMM(br.x, br.y);
            mmCtx.strokeStyle = accentColor;
            mmCtx.lineWidth = 1.5;
            mmCtx.strokeRect(a.x, a.y, b.x - a.x, b.y - a.y);
            minimap._map = { minX, minY, scale, offX, offY };
        };
        this.currentChart.on('afterDrawing', () => drawMinimap());

        const mmNavigate = (ev) => {
            const map = minimap._map; if (!map) return;
            const rect = minimap.getBoundingClientRect();
            const mx = ev.clientX - rect.left, my = ev.clientY - rect.top;
            const cx = map.minX + (mx - map.offX) / map.scale;
            const cy = map.minY + (my - map.offY) / map.scale;
            this.currentChart.moveTo({ position: { x: cx, y: cy }, animation: { duration: 150 } });
        };
        let mmDragging = false;
        minimap.addEventListener('mousedown', (e) => { mmDragging = true; mmNavigate(e); e.preventDefault(); });
        if (this._mmMove) window.removeEventListener('mousemove', this._mmMove);
        if (this._mmUp) window.removeEventListener('mouseup', this._mmUp);
        this._mmMove = (e) => { if (mmDragging) mmNavigate(e); };
        this._mmUp = () => { mmDragging = false; };
        window.addEventListener('mousemove', this._mmMove);
        window.addEventListener('mouseup', this._mmUp);

        // ---- Highlight helpers (neighbor-based, not ancestry) ----
        const labelColor = cv('--graph-label');
        const labelDim = cv('--graph-label-dim');
        const dimNode = cv('--graph-node-dim');
        const edgeBase = cv('--graph-edge');
        const litColor = (id) => {
            const fill = baseColors.get(id);
            return { background: fill, border: fill, highlight: { background: fill, border: accentColor }, hover: { background: fill, border: accentColor } };
        };
        const dimColor = { background: dimNode, border: dimNode, highlight: { background: dimNode, border: dimNode }, hover: { background: dimNode, border: dimNode } };

        const restoreBase = () => {
            nodes.update([...nodeMap.keys()].map(id => ({ id, color: litColor(id), font: { color: labelColor } })));
            edges.update(edges.getIds().map(eid => ({ id: eid, color: { color: edgeBase, opacity: 0.45, highlight: accentColor } })));
        };
        const highlightNeighborhood = (id, hot) => {
            const keep = new Set([id, ...(nodeMap.get(id)?.neighbors.keys() || [])]);
            nodes.update([...nodeMap.keys()].map(nid => {
                const on = keep.has(nid);
                return { id: nid, color: on ? litColor(nid) : dimColor, font: { color: on ? labelColor : labelDim } };
            }));
            edges.update(edges.get().map(e => {
                const on = (e.from === id || e.to === id);
                return { id: e.id, color: { color: (hot && on) ? accentColor : edgeBase, opacity: on ? (hot ? 0.95 : 0.45) : 0.08, highlight: accentColor } };
            }));
        };

        let selectedNodeId = null;
        const resizeGraph = () => {
            if (!this.currentChart) return;
            this.currentChart.setSize(networkDiv.clientWidth + 'px', networkDiv.clientHeight + 'px');
            this.currentChart.redraw();
        };

        // ---- Detail panel content ----
        const buildPanel = (nodeId) => {
            const n = nodeMap.get(nodeId) || { neighbors: new Map(), details: {}, degree: 0, sizeSum: 0 };
            const body = detailPanel.querySelector('.graph-detail-body');
            const fill = baseColors.get(nodeId) || neutralColor;

            let chips = `<span class="graph-detail-proc" style="--chip-color:${fill}">${n.degree} conn</span>`;
            if (n.isSrc) chips += `<span class="graph-detail-tag">src</span>`;
            if (n.isDst) chips += `<span class="graph-detail-tag">dst</span>`;

            const topNeighbors = [...n.neighbors.entries()].sort((a, b) => b[1] - a[1]).slice(0, 12);
            const neighborsHtml = topNeighbors.length > 0
                ? '<div class="graph-detail-fields">' + topNeighbors.map(([id, w]) =>
                    `<div class="graph-detail-field graph-mesh-neighbor" data-node="${Utils.escapeHtml(id)}"><div class="graph-detail-field-name">${Utils.escapeHtml(id)}</div><div class="graph-detail-field-value">${w}</div></div>`
                  ).join('') + '</div>'
                : '<div class="graph-detail-empty">No connections</div>';

            const fieldEntries = Object.entries(n.details);
            const fieldsHtml = fieldEntries.length > 0
                ? '<div class="graph-detail-fields">' + fieldEntries.map(([k, v]) =>
                    `<div class="graph-detail-field"><div class="graph-detail-field-name">${Utils.escapeHtml(k)}</div><div class="graph-detail-field-value">${Utils.escapeHtml(String(v))}</div></div>`
                  ).join('') + '</div>'
                : '';

            body.innerHTML = `
                <div class="graph-detail-id">
                    <span class="graph-detail-id-label">ID</span>
                    <span class="graph-detail-id-value">${Utils.escapeHtml(nodeId)}</span>
                </div>
                <div class="graph-detail-chips">${chips}</div>
                <div class="graph-detail-actions">
                    <button class="graph-detail-action" data-act="focus">Focus neighborhood</button>
                    <button class="graph-detail-action secondary" data-act="copy">Copy ID</button>
                </div>
                <div class="graph-detail-subhead">Connections (${n.neighbors.size})</div>
                ${neighborsHtml}
                ${fieldsHtml}
            `;
            body.querySelector('[data-act="copy"]')?.addEventListener('click', () => {
                navigator.clipboard.writeText(nodeId).then(() => Toast.show('Copied', 'success'));
            });
            body.querySelector('[data-act="focus"]')?.addEventListener('click', () => {
                const connected = this.currentChart.getConnectedNodes(nodeId);
                this.currentChart.fit({ nodes: [nodeId, ...connected], animation: { duration: 400 }, padding: 80 });
            });
            body.querySelectorAll('.graph-mesh-neighbor').forEach(el => {
                el.addEventListener('click', () => {
                    const id = el.getAttribute('data-node');
                    if (!nodeMap.has(id)) return;
                    this.currentChart.selectNodes([id]);
                    selectNodeAction(id);
                });
            });
        };

        const selectNodeAction = (nodeId) => {
            selectedNodeId = nodeId;
            buildPanel(nodeId);
            detailPanel.classList.add('open');
            highlightNeighborhood(nodeId, true);
            setTimeout(() => {
                resizeGraph();
                this.currentChart.focus(nodeId, { scale: Math.max(this.currentChart.getScale(), 0.6), animation: { duration: 300, easingFunction: 'easeInOutQuad' } });
            }, 210);
        };

        // -- Toolbar handlers --
        document.getElementById('meshFitBtn')?.addEventListener('click', () => {
            this.currentChart.fit({ animation: { duration: 400, easingFunction: 'easeInOutQuad' }, padding: 40 });
        });
        document.getElementById('meshZoomInBtn')?.addEventListener('click', () => {
            this.currentChart.moveTo({ scale: this.currentChart.getScale() * 1.3, animation: { duration: 200 } });
        });
        document.getElementById('meshZoomOutBtn')?.addEventListener('click', () => {
            this.currentChart.moveTo({ scale: this.currentChart.getScale() / 1.3, animation: { duration: 200 } });
        });
        document.getElementById('meshExportBtn')?.addEventListener('click', () => {
            const canvas = networkDiv.querySelector('canvas');
            if (!canvas) return;
            const link = document.createElement('a');
            link.download = 'bifract-mesh.png';
            link.href = canvas.toDataURL('image/png');
            link.click();
            Toast.show('Mesh exported as PNG', 'success');
        });

        // -- Min-weight filter: hide edges below threshold and orphaned nodes. --
        const minWeightInput = document.getElementById('meshMinWeight');
        if (minWeightInput) {
            minWeightInput.addEventListener('input', Utils.debounce((e) => {
                const thr = Number(e.target.value) || 0;
                const visibleNodes = new Set();
                const edgeUpdates = edges.get().map((edge, i) => {
                    const w = edgeList[edge.id]?.weight ?? 0;
                    const hidden = w < thr;
                    if (!hidden) { visibleNodes.add(edge.from); visibleNodes.add(edge.to); }
                    return { id: edge.id, hidden };
                });
                edges.update(edgeUpdates);
                nodes.update([...nodeMap.keys()].map(id => ({ id, hidden: thr > 0 && !visibleNodes.has(id) })));
                const vn = thr > 0 ? visibleNodes.size : nodeMap.size;
                const ve = edgeUpdates.filter(u => !u.hidden).length;
                const nc = document.getElementById('meshNodeCount'); if (nc) nc.textContent = vn;
                const ec = document.getElementById('meshEdgeCount'); if (ec) ec.textContent = ve;
            }, 200));
        }

        // -- Node search: dim non-matching --
        const searchInput = document.getElementById('meshNodeSearch');
        if (searchInput) {
            searchInput.addEventListener('input', Utils.debounce((e) => {
                const term = e.target.value.toLowerCase().trim();
                if (!term) { if (!selectedNodeId) restoreBase(); return; }
                const match = new Set();
                nodeMap.forEach((n, id) => {
                    if (id.toLowerCase().includes(term) || Object.values(n.details).some(v => String(v).toLowerCase().includes(term))) match.add(id);
                });
                nodes.update([...nodeMap.keys()].map(id => {
                    const on = match.has(id);
                    return { id, color: on ? litColor(id) : dimColor, font: { color: on ? labelColor : labelDim } };
                }));
            }, 200));
        }

        // -- Selection / hover --
        const closePanel = () => {
            detailPanel.classList.remove('open');
            selectedNodeId = null;
            this.currentChart.unselectAll();
            setTimeout(() => { resizeGraph(); restoreBase(); }, 210);
        };
        detailPanel.querySelector('.graph-detail-close').addEventListener('click', closePanel);

        this.currentChart.on('selectNode', (params) => {
            const nodeId = params.nodes[0];
            if (nodeId) selectNodeAction(nodeId);
        });
        this.currentChart.on('deselectNode', (params) => {
            if (params.nodes && params.nodes.length) return;
            detailPanel.classList.remove('open');
            selectedNodeId = null;
            setTimeout(() => { resizeGraph(); restoreBase(); }, 210);
        });
        this.currentChart.on('hoverNode', (params) => {
            if (selectedNodeId) return;
            highlightNeighborhood(params.node, true);
        });
        this.currentChart.on('blurNode', () => {
            if (selectedNodeId) return;
            restoreBase();
        });
        this.currentChart.on('doubleClick', (params) => {
            if (params.nodes.length > 0) {
                const nodeId = params.nodes[0];
                const connected = this.currentChart.getConnectedNodes(nodeId);
                this.currentChart.fit({ nodes: [nodeId, ...connected], animation: { duration: 400, easingFunction: 'easeInOutQuad' }, padding: 80 });
            }
        });
    },

    // Re-root the active dfs()/bfs() traversal at startId by swapping the start=
    // argument inside the existing call, preserving every other pipeline stage.
    // "Walk Up" passes the selected node's parent guid here, climbing the tree.
    pivotTraversal(startId) {
        if (!startId) return;
        const qi = document.getElementById('queryInput');
        if (!qi) return;
        const cur = this.currentQuery || qi.value || '';
        const re = /(\b(?:dfs|bfs)\s*\([^)]*?\bstart\s*=\s*)("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|[^,)\s]+)/i;
        if (!re.test(cur)) {
            Toast.show('No dfs()/bfs() in query to re-root', 'error');
            return;
        }
        qi.value = cur.replace(re, `$1"${startId}"`);
        qi.dispatchEvent(new Event('input', { bubbles: true }));
        const btn = document.getElementById('executeBtn');
        if (btn) setTimeout(() => btn.click(), 0);
        Toast.show('Walking up to parent', 'info');
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

            // Carry the @variable values so a shared link reproduces them.
            const varsArr = this.varManager ? this.varManager.serialize() : [];
            if (varsArr.length) urlParams.set('vars', this._encodeVars(varsArr));

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
                relativeUnit: urlParams.get('ru'),
                vars: urlParams.get('vars')
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

            // Restore the shared variable values, then reconcile the tray against
            // the query text so the @vars surface with their shared values.
            if (this.varManager) {
                if (shareData.vars) {
                    const seeded = this._decodeVars(shareData.vars);
                    if (seeded.size) this._pendingUrlVars = seeded;
                }
                this.syncSearchVariables();
            }

            // Set time range
            if (window.TimePicker) {
                TimePicker.applyState({
                    type: timeRangeValue,
                    customStart, customEnd,
                    relativeN, relativeUnit
                }, true);
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
