// Main application orchestrator
const App = {
    queryHistory: {
        main: { states: [''], currentIndex: 0, maxSize: 50 },
        alert: { states: [''], currentIndex: 0, maxSize: 50 }
    },
    isUndoRedoing: false,
    historyTimers: {
        main: null,
        alert: null
    },

    // resumePendingLink navigates to a deep link that was stashed before login.
    // The entry is consumed before navigating, so a destination that keeps
    // bouncing cannot loop. Returns true when a navigation was started.
    resumePendingLink() {
        let next = null;
        try {
            next = sessionStorage.getItem('bifract-pending-link');
            sessionStorage.removeItem('bifract-pending-link');
        } catch (e) {
            return false; // storage unavailable (private mode); not worth failing over
        }
        // Same-origin absolute paths only: "//host" and "/\host" point elsewhere.
        // Tab and newline are stripped first because the URL parser drops them
        // too, which would turn "/\n/host" into "//host".
        if (typeof next !== 'string') return false;
        next = next.replace(/[\t\n\r]/g, '');
        if (!next || next[0] !== '/' || next[1] === '/' || next[1] === '\\') return false;
        if (next === window.location.pathname + window.location.search) return false;
        window.location.replace(next);
        return true;
    },

    init() {
        // A deep link the user was bounced off for login lands back here once
        // authenticated. OIDC in particular returns to "/" rather than to the
        // login page, so the destination is picked up from sessionStorage.
        if (this.resumePendingLink()) return;

        if (window.Sidebar) Sidebar.init();
        if (window.GoTo) GoTo.init();

        // Initialize all modules
        if (window.TimeBar) {
            TimeBar.init();
        }

        // Changing the display zone relabels what is already on screen rather
        // than re-running the query: the data is identical, only its rendering
        // moves.
        document.addEventListener(TZ.EVENT, () => {
            if (window.QueryExecutor && QueryExecutor.currentResults && QueryExecutor.currentResults.length) {
                QueryExecutor.renderResults(QueryExecutor.currentResults);
            }
            if (window.Timeline) Timeline.redraw();
            if (window.Auth) Auth.updateTimezoneHint();
        });

        if (window.SyntaxHighlight) {
            SyntaxHighlight.init();
        }

        // Live, debounced BQL validation for the main search box. While typing we
        // only draw the subtle inline underline (+ hover tooltip); the louder
        // error banner is reserved for an explicit run. Editing also dismisses a
        // stale banner from a previous run so it never lingers over new text.
        // Set up the auto-detected @variable tray under the search editor before
        // wiring validation, so the variable set is current when validation runs.
        if (window.QueryExecutor && QueryExecutor.initVariables) {
            QueryExecutor.initVariables();
        }

        if (window.QueryValidate && window.QueryExecutor) {
            QueryValidate.attach({
                inputId: 'queryInput',
                highlightId: 'queryHighlight',
                getFractalId: () => window.FractalContext?.currentFractal?.id || undefined,
                getVariables: () => QueryExecutor.variablesPayload(),
                onEdit: () => QueryExecutor.clearQueryError(),
            });
            // Alert editor query box (static markup, shares the search-input pattern).
            QueryValidate.attach({
                inputId: 'editorQueryInput',
                highlightId: 'alertQueryHighlight',
                getFractalId: () => window.FractalContext?.currentFractal?.id || undefined,
            });
        }

        if (window.BQLLang) {
            BQLLang.load();
        }

        if (window.Autocomplete) {
            Autocomplete.init();
        }

        if (window.LogDetail) {
            LogDetail.init();
        }

        if (window.RailPanel) {
            RailPanel.init();
        }

        if (window.FieldStats) {
            FieldStats.init();
        }

        if (window.NotebookRail) {
            NotebookRail.init();
        }

        if (window.QueryPalette) {
            QueryPalette.init();
        }

        this.initToolbarMenus();

        if (window.Settings) {
            Settings.init();
        }

        // Initialize authentication
        if (window.Auth) {
            Auth.init();
        }

        // Initialize fractal management
        if (window.FractalManagement) {
            FractalManagement.init();
        }

        // Initialize the sidebar fractal/prism selector. createSelectorUI()
        // is idempotent, so this is safe to call even if a future code path
        // also initializes it (e.g. post-login).
        if (window.FractalSelector) {
            FractalSelector.init();
        }

        // Initialize toast notifications
        if (window.Toast) {
            Toast.init();
        }

        // Initialize fractal context
        if (window.FractalContext) {
            FractalContext.init();
        }

        // Initialize fractal listing
        if (window.FractalListing) {
            FractalListing.init();
        }

        // Initialize fractal manage tab
        if (window.FractalManageTab) {
            FractalManageTab.init();
        }

        // Initialize dictionaries module
        if (window.Dictionaries) {
            Dictionaries.init();
        }

        // Initialize analytics models module
        if (window.AnalyticsModels) {
            AnalyticsModels.init();
        }

        // Initialize chat module
        if (window.Chat) {
            Chat.init();
        }

        // Initialize ingest tokens module
        if (window.IngestTokens) {
            IngestTokens.init();
        }

        // Initialize instruction libraries module
        if (window.InstructionLibraries) {
            InstructionLibraries.init();
        }

        // Initialize Recall (archive search) module
        if (window.Recall) {
            Recall.init();
        }

        // Initialize performance module
        if (window.Performance) {
            Performance.init();
        }

        if (window.ContextLinks) {
            ContextLinks.init();
        }
        if (window.Normalizers) {
            Normalizers.init();
        }
        if (window.SchemaFields) {
            SchemaFields.init();
        }
        if (window.AlertFeeds) {
            AlertFeeds.init();
        }
        if (window.AttackCoverage) {
            AttackCoverage.init();
        }

        if (window.Notifications) {
            Notifications.init();
        }

        if (window.Pagination) {
            Pagination.init((pageResults) => {
                if (window.QueryExecutor) {
                    QueryExecutor.renderPage(pageResults);
                }
            });
        }

        // Restore saved time range
        if (window.QueryExecutor) {
            QueryExecutor.restoreTimeRangeFromStorage();
        }

        if (window.KebabMenu) {
            KebabMenu.init();
        }

        this.setupEventListeners();
        this._initPopState();
        this.checkStatus();

        // Check status every 30 seconds
        setInterval(() => this.checkStatus(), 30000);

    },

    // First route of the page, run by Auth once the user is known so role-gated
    // views render once instead of before and after login resolves.
    routeInitial() {
        this._navigatingFromPopState = true;
        return this.routeFromHash(null).finally(() => {
            this._navigatingFromPopState = false;
        });
    },

    // Route names the hash router accepts.
    _mainTabs: new Set(['fractalListing', 'performance', 'settings', 'normalizers', 'schema', 'api']),
    _fractalTabs: new Set(['search', 'comments', 'notebooks', 'dashboards', 'dictionaries', 'models', 'chat', 'library', 'alerts', 'ingest', 'recall', 'manage']),

    // Build the full hash string for the current navigation state.
    // Fractal-view tabs: f/{id}/{tab}/{subPath} or p/{id}/{tab}/{subPath}
    // Main-view tabs: {tab}/{subPath}
    _buildHash(tab, subPath = '') {
        let base;
        if (this.currentViewLevel === 'fractal' && window.FractalContext?.currentFractal) {
            const prefix = window.FractalContext.isPrism() ? 'p' : 'f';
            base = `${prefix}/${window.FractalContext.currentFractal.id}/${tab}`;
        } else {
            base = tab === 'fractalListing' ? '' : tab;
        }
        return subPath ? `${base}/${subPath}` : base;
    },

    // Build the history.pushState state object for the current fractal/prism context.
    // Stored alongside the hash so back-button restores can resolve the fractal name
    // without an extra API call.
    _buildFractalState() {
        const fractal = window.FractalContext?.currentFractal;
        if (!fractal) return null;
        return {
            fractalId: fractal.id,
            fractalName: fractal.name,
            fractalType: window.FractalContext.currentItemType,
        };
    },

    // Called by tab modules when navigating to a sub-view (e.g. model detail,
    // notebook editor). Suppressed during popstate/init routing so that restoring
    // state from the URL does not create duplicate history entries.
    pushSubPath(subPath) {
        if (this._navigatingFromPopState) return;
        this._pushHash(this._buildHash(this.currentView, subPath), this._buildFractalState());
    },

    // Route from the URL hash on page load and handle browser back/forward.
    // Async because cross-fractal back-navigation must await the server /select
    // call before rendering the new scope's tab.
    async routeFromHash(event) {
        const hash = window.location.hash.replace(/^#/, '');
        const segments = hash ? hash.split('/') : [];
        const prefix = segments[0] || '';

        if (!prefix || prefix === 'fractalListing') {
            // A pending share link opens its own fractal (FractalSelector lands it);
            // detouring through the listing would clear the scope it is about to use.
            if (!prefix && window.QueryExecutor?.hasUnprocessedShareLink?.()) return;
            this.showMainView('fractalListing');
            return;
        }

        // Fractal/prism view: f/{id}/{tab}/{subPath...} or p/{id}/{tab}/{subPath...}
        if (prefix === 'f' || prefix === 'p') {
            const isPrism = prefix === 'p';
            const id = segments[1];
            const tab = segments[2] || 'search';
            const subPath = segments.slice(3).join('/');

            if (!id || !window.FractalContext) {
                this.showMainView('fractalListing');
                return;
            }

            if (window.FractalContext.currentFractal?.id !== id) {
                // Resolve the fractal/prism name — needed to call setCurrentFractal/Prism.
                // Primary: state object pushed alongside the hash (always present on back nav).
                // Fallback: in-memory listing cache. Last resort: API fetch.
                let name = event?.state?.fractalName;
                if (!name) name = window.FractalListing?.getById(id, isPrism)?.name;
                if (!name) {
                    try {
                        const endpoint = isPrism ? `/api/v1/prisms/${id}` : `/api/v1/fractals/${id}`;
                        const resp = await fetch(endpoint, { credentials: 'include' });
                        if (resp.ok) {
                            const data = await resp.json();
                            // Fractal API wraps the object under "index"; prism API returns it directly
                            name = data.data?.index?.name || data.data?.name || data.name;
                        }
                    } catch (_) {}
                }
                if (!name) name = id;

                const item = { id, name };
                if (isPrism) {
                    await FractalContext.setCurrentPrism(item);
                } else {
                    await FractalContext.setCurrentFractal(item);
                }
            }

            this.showFractalView(tab, subPath);
            return;
        }

        // Main-view tab (possibly with sub-path)
        const mainTab = prefix;
        const subPath = segments.slice(1).join('/');

        // Context Links moved under Admin; keep old #context deep links working.
        if (mainTab === 'context') {
            this.showMainView('settings', subPath ? 'context/' + subPath : 'context');
            return;
        }

        if (this._mainTabs.has(mainTab)) {
            this.showMainView(mainTab, subPath);
            return;
        }

        // Legacy fractal tab format (plain tab name, no context prefix).
        // Restore context from localStorage and show the tab.
        if (this._fractalTabs.has(mainTab)) {
            if (!window.FractalContext?.currentFractal) {
                const restored = await FractalContext.restoreFromStorage();
                if (restored) {
                    this.showFractalView(mainTab, '');
                } else {
                    this.showMainView('fractalListing');
                }
                return;
            }
            this.showFractalView(mainTab, '');
            return;
        }

        this.showMainView('fractalListing');
    },

    // Push a history entry so the browser back button navigates within the app.
    // state is stored alongside the hash and recovered via event.state on popstate.
    _pushHash(hash, state = null) {
        const target = hash ? '#' + hash : '#fractalListing';
        if (window.location.hash !== target) {
            history.pushState(state, '', target);
        }
    },

    // Listen for popstate (browser back/forward) and route accordingly.
    _initPopState() {
        window.addEventListener('popstate', async (event) => {
            this._navigatingFromPopState = true;
            try {
                await this.routeFromHash(event);
                // The hash routes the view; the query string describes the search
                // within it. Each executed query is its own history entry, so Back
                // on the search tab has to re-run what that entry names.
                if (this.currentView === 'search' && window.QueryExecutor?.replayFromUrl) {
                    QueryExecutor.replayFromUrl();
                }
            } finally {
                this._navigatingFromPopState = false;
            }
        });
    },

    setupEventListeners() {
        document.querySelectorAll('[data-investigation-subtab]').forEach(btn => {
            btn.addEventListener('click', () => this.showFractalViewTab('notebooks', btn.dataset.investigationSubtab === 'evidence' ? 'evidence' : ''));
        });

        // Query input
        const queryInput = document.getElementById('queryInput');
        const executeBtn = document.getElementById('executeBtn');

        if (queryInput) {
            this.bindQueryEditorKeys(queryInput, {
                historyKey: 'main',
                onRun: () => window.QueryExecutor && QueryExecutor.execute(),
                onFormat: () => this.formatQuery(queryInput),
            });

            // Auto-resize textarea and sync highlighting
            queryInput.addEventListener('input', () => {
                // Save to history (unless we're in undo/redo operation)
                if (!this.isUndoRedoing) {
                    setTimeout(() => {
                        this.saveToHistory('main', queryInput.value);
                    }, 0);
                }

                // Let SyntaxHighlight handle both highlighting and height syncing
                if (window.SyntaxHighlight) {
                    SyntaxHighlight.updateHighlight('queryInput', 'queryHighlight');
                }
            });
        }

        if (executeBtn) {
            executeBtn.addEventListener('click', () => {
                if (window.QueryExecutor) {
                    QueryExecutor.runOrCancel();
                }
            });
        }

        // Send the current query to the active notebook.
        const sendToNotebookBtn = document.getElementById('sendToNotebookBtn');
        if (sendToNotebookBtn) {
            sendToNotebookBtn.addEventListener('click', () => {
                if (window.NotebookRail) NotebookRail.captureCurrentQuery();
            });
        }

        // Share query button
        const shareQueryBtn = document.getElementById('shareQueryBtn');
        if (shareQueryBtn) {
            shareQueryBtn.addEventListener('click', () => {
                if (window.QueryExecutor) {
                    QueryExecutor.generateAndCopyShareLink();
                }
            });
        }

        // Time picker
        if (window.TimePicker) {
            TimePicker.init();
        }

        // SQL toggle
        const toggleSqlBtn = document.getElementById('toggleSqlBtn');
        const sqlOutput = document.getElementById('sqlOutput');

        if (toggleSqlBtn && sqlOutput) {
            toggleSqlBtn.addEventListener('click', () => {
                const isHidden = sqlOutput.style.display === 'none' || !sqlOutput.style.display;
                sqlOutput.style.display = isHidden ? 'block' : 'none';
                toggleSqlBtn.textContent = isHidden ? 'Hide SQL' : 'Show SQL';
            });
        }


        // Query editor resize handles
        this.setupQueryResizeHandles();

        // Line numbers for query editors
        this.setupQueryLineNumbers();

        // Status modal
        const statusIndicator = document.getElementById('statusIndicator');
        const statusModal = document.getElementById('statusModal');
        const closeStatusBtn = document.getElementById('closeStatusBtn');
        const clearLogsBtn = document.getElementById('clearLogsBtn');

        if (statusIndicator && statusModal) {
            statusIndicator.addEventListener('click', () => {
                statusModal.style.display = 'flex';
                this.loadDetailedStatus();
            });
        }

        if (closeStatusBtn && statusModal) {
            closeStatusBtn.addEventListener('click', () => {
                statusModal.style.display = 'none';
            });
        }

        if (statusModal) {
            statusModal.addEventListener('click', (e) => {
                if (e.target === statusModal) {
                    statusModal.style.display = 'none';
                }
            });
        }

        if (clearLogsBtn) {
            clearLogsBtn.addEventListener('click', async () => {
                if (confirm('Are you sure you want to delete all logs? This cannot be undone.')) {
                    await this.clearAllLogs();
                }
            });
        }
    },

    // Wires an editor's definition rail: drag to resize, and fold away while a log
    // is open for inspection. The rail is a column of a grid, so it resizes by
    // moving the track rather than by setting a width the grid would ignore.
    // opts: { body, cssVar, storageKey, detail, foldClass, min, maxFraction }
    bindEditorRail(handle, { body, cssVar, storageKey, detail, foldClass, min = 280, maxFraction = 0.6 } = {}) {
        if (!body) return;

        if (detail && foldClass) {
            const sync = () => body.classList.toggle(foldClass, detail.classList.contains('open'));
            new MutationObserver(sync).observe(detail, { attributes: true, attributeFilter: ['class'] });
            sync();
        }

        if (!handle || !cssVar || handle.dataset.railBound) return;
        handle.dataset.railBound = '1';

        const apply = (w) => body.style.setProperty(cssVar, Math.round(w) + 'px');
        const saved = storageKey ? parseFloat(localStorage.getItem(storageKey)) : NaN;
        if (Number.isFinite(saved)) apply(saved);

        let startX = 0, startWidth = 0;
        const onMove = (e) => apply(Math.max(min, Math.min(window.innerWidth * maxFraction, startWidth + (startX - e.clientX))));
        const onUp = () => {
            handle.classList.remove('dragging');
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
            const width = parseFloat(body.style.getPropertyValue(cssVar));
            if (storageKey && Number.isFinite(width)) localStorage.setItem(storageKey, width);
        };

        handle.addEventListener('mousedown', (e) => {
            e.preventDefault();
            startX = e.clientX;
            startWidth = handle.parentElement ? handle.parentElement.offsetWidth : min;
            handle.classList.add('dragging');
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
            document.addEventListener('mousemove', onMove);
            document.addEventListener('mouseup', onUp);
        });
    },

    // The keyboard contract every BQL editor shares: Enter runs, Shift+Enter is a
    // newline, Tab indents, Ctrl+/ comments and Ctrl+Z/Y walk that editor's own
    // history. Editors differ only in what running means, so that is the option.
    // seed restarts the history at that value, for an editor that is rebuilt around
    // a different query each time it opens.
    bindQueryEditorKeys(textarea, { historyKey, onRun, onFormat, seed } = {}) {
        if (!textarea || textarea.dataset.queryKeysBound) return;
        textarea.dataset.queryKeysBound = '1';
        if (historyKey) textarea.dataset.historyKey = historyKey;
        if (historyKey && (seed !== undefined || !this.queryHistory[historyKey])) {
            this.queryHistory[historyKey] = { states: [seed || ''], currentIndex: 0, maxSize: 50 };
            clearTimeout(this.historyTimers[historyKey]);
            this.historyTimers[historyKey] = null;
        }

        textarea.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                if (onRun) onRun();
            } else if (e.key === 'Enter' && e.shiftKey) {
                // Allow new line (default behavior)
            } else if (e.key === 'Tab' && !e._autocompleteHandled) {
                e.preventDefault();
                const start = textarea.selectionStart;
                const end = textarea.selectionEnd;
                const value = textarea.value;
                textarea.value = value.substring(0, start) + '\t' + value.substring(end);
                textarea.selectionStart = textarea.selectionEnd = start + 1;
                textarea.dispatchEvent(new Event('input'));
            } else if (onFormat && e.code === 'KeyF' && e.altKey && e.shiftKey) {
                e.preventDefault();
                onFormat();
            } else if (e.key === '/' && e.ctrlKey) {
                e.preventDefault();
                this.toggleLineComment(textarea);
            } else if (historyKey && e.key === 'z' && e.ctrlKey && !e.shiftKey) {
                e.preventDefault();
                this.undo(historyKey, textarea);
            } else if (historyKey && ((e.key === 'y' && e.ctrlKey) || (e.key === 'z' && e.ctrlKey && e.shiftKey))) {
                e.preventDefault();
                this.redo(historyKey, textarea);
            }
        });
    },

    toggleLineComment(textarea) {
        const start = textarea.selectionStart;
        const end = textarea.selectionEnd;
        const value = textarea.value;

        // Find the start and end of the current line(s)
        const beforeStart = value.lastIndexOf('\n', start - 1);
        const lineStart = beforeStart === -1 ? 0 : beforeStart + 1;

        const afterEnd = value.indexOf('\n', end);
        const lineEnd = afterEnd === -1 ? value.length : afterEnd;

        // Get the selected lines
        const selectedText = value.substring(lineStart, lineEnd);
        const lines = selectedText.split('\n');

        // Check if all non-empty lines are commented
        const nonEmptyLines = lines.filter(line => line.trim() !== '');
        const allCommented = nonEmptyLines.length > 0 && nonEmptyLines.every(line => line.trim().startsWith('//'));

        // Toggle comments on all lines
        const modifiedLines = lines.map(line => {
            if (line.trim() === '') return line; // Skip empty lines

            if (allCommented) {
                // Remove comment - find first occurrence of // and remove it
                const commentIndex = line.indexOf('//');
                if (commentIndex !== -1) {
                    return line.substring(0, commentIndex) + line.substring(commentIndex + 2);
                }
                return line;
            } else {
                // Add comment at the beginning of the line (after leading whitespace)
                const match = line.match(/^(\s*)(.*)/);
                if (match) {
                    return match[1] + '//' + match[2];
                }
                return '//' + line;
            }
        });

        const newSelectedText = modifiedLines.join('\n');

        // Replace the text
        const newValue = value.substring(0, lineStart) + newSelectedText + value.substring(lineEnd);
        textarea.value = newValue;

        // Adjust selection to include the modified lines
        const lengthDiff = newSelectedText.length - selectedText.length;
        textarea.selectionStart = lineStart;
        textarea.selectionEnd = lineEnd + lengthDiff;

        // Trigger input event to update syntax highlighting
        textarea.dispatchEvent(new Event('input'));

        // Force save to history after comment toggle
        this.saveToHistoryImmediate(textarea.dataset.historyKey || 'main', textarea.value, true);
    },

    shouldSaveHistory(oldValue, newValue) {
        // Always save if it's a significant change in length (paste, delete block, etc.)
        const lengthDiff = Math.abs(newValue.length - oldValue.length);
        if (lengthDiff >= 4) return true;

        // Save at word boundaries - when we finish typing a word of 4+ characters
        const oldWords = oldValue.split(/\s+/).filter(w => w.length > 0);
        const newWords = newValue.split(/\s+/).filter(w => w.length > 0);

        // If we added a new word and it's 4+ characters, save
        if (newWords.length > oldWords.length) {
            const lastWord = newWords[newWords.length - 1];
            if (lastWord.length >= 4) return true;
        }

        // If we finished a word (added space or punctuation after 4+ chars)
        if (newValue.length > oldValue.length) {
            const lastChar = newValue[newValue.length - 1];
            if (/[\s|,;.!?(){}[\]]/.test(lastChar)) {
                // Check if the word before this separator is 4+ chars
                const beforeSeparator = newValue.substring(0, newValue.length - 1).split(/[\s|,;.!?(){}[\]]+/).pop();
                if (beforeSeparator && beforeSeparator.length >= 4) return true;
            }
        }

        return false;
    },

    saveToHistoryImmediate(type, value, force = false) {
        const history = this.queryHistory[type];
        // Don't save if the value is the same as the current state
        if (!force && history.states[history.currentIndex] === value) {
            return;
        }

        // Remove any states after current index (when we type after undoing)
        history.states = history.states.slice(0, history.currentIndex + 1);

        // Add new state
        history.states.push(value);
        history.currentIndex = history.states.length - 1;

        // Limit history size
        if (history.states.length > history.maxSize) {
            history.states.shift();
            history.currentIndex--;
        }
    },

    saveToHistoryDebounced(type, value) {
        // Clear existing timer
        if (this.historyTimers[type]) {
            clearTimeout(this.historyTimers[type]);
        }

        // Set new timer to save after 1 second of inactivity
        this.historyTimers[type] = setTimeout(() => {
            this.saveToHistoryImmediate(type, value);
        }, 1000);
    },

    saveToHistory(type, value) {
        const history = this.queryHistory[type];
        const oldValue = history.states[history.currentIndex] || '';

        // Check if we should save immediately
        if (this.shouldSaveHistory(oldValue, value)) {
            this.saveToHistoryImmediate(type, value);
        } else {
            // Otherwise, use debounced save for pauses in typing
            this.saveToHistoryDebounced(type, value);
        }
    },

    undo(type, textarea) {
        const history = this.queryHistory[type];
        if (history.currentIndex > 0) {
            history.currentIndex--;
            const newValue = history.states[history.currentIndex];
            this.isUndoRedoing = true;
            textarea.value = newValue;

            // Trigger input event to update syntax highlighting
            textarea.dispatchEvent(new Event('input'));
            this.isUndoRedoing = false;
        }
    },

    redo(type, textarea) {
        const history = this.queryHistory[type];
        if (history.currentIndex < history.states.length - 1) {
            history.currentIndex++;
            const newValue = history.states[history.currentIndex];
            this.isUndoRedoing = true;
            textarea.value = newValue;

            // Trigger input event to update syntax highlighting
            textarea.dispatchEvent(new Event('input'));
            this.isUndoRedoing = false;
        }
    },

    async checkStatus() {
        const statusDot = document.getElementById('statusDotCompact');
        const statusContainer = document.getElementById('statusIndicatorCompact');

        try {
            const response = await fetch('/api/v1/health/clickhouse');
            const data = await response.json();

            if (statusDot && statusContainer) {
                if (data.success && data.connected && data.degraded) {
                    statusDot.className = 'status-dot status-degraded';
                    const down = (data.shards_total || 0) - (data.shards_healthy || 0);
                    statusContainer.title = `ClickHouse Degraded — ${down} of ${data.shards_total} shard(s) unreachable`;
                } else if (data.success && data.connected) {
                    statusDot.className = 'status-dot status-connected';
                    statusContainer.title = 'ClickHouse Connected';
                } else {
                    statusDot.className = 'status-dot status-disconnected';
                    statusContainer.title = 'ClickHouse Disconnected';
                }
            }
        } catch (error) {
            if (statusDot && statusContainer) {
                statusDot.className = 'status-dot status-disconnected';
                statusContainer.title = 'ClickHouse Disconnected';
            }
        }
    },

    async loadDetailedStatus() {
        const detailedStatus = document.getElementById('detailedStatus');
        if (!detailedStatus) return;

        detailedStatus.innerHTML = '<div class="loading">Loading...</div>';

        try {
            const response = await fetch('/api/v1/status');
            const data = await response.json();

            const ch = data.clickhouse || {};
            const isConnected = data.success && ch.connected;

            let html = '<div class="status-grid">';
            html += `<div class="status-item"><span class="status-label">ClickHouse Status:</span><span class="status-value ${isConnected ? 'status-ok' : 'status-error'}">${isConnected ? 'Connected' : 'Disconnected'}</span></div>`;

            if (isConnected) {
                html += `<div class="status-item"><span class="status-label">Storage Used:</span><span class="status-value">${ch.table_size || this.formatBytes(ch.storage_bytes || 0)}</span></div>`;
                html += `<div class="status-item"><span class="status-label">First Ingest:</span><span class="status-value">${ch.oldest_log || 'N/A'}</span></div>`;
                html += `<div class="status-item"><span class="status-label">Last Ingest:</span><span class="status-value">${ch.newest_log || 'N/A'}</span></div>`;
            }

            html += '</div>';
            detailedStatus.innerHTML = html;
        } catch (error) {
            detailedStatus.innerHTML = '<div class="error">Failed to load status</div>';
        }
    },

    async clearAllLogs() {
        try {
            const response = await fetch('/api/v1/logs', {
                method: 'DELETE'
            });

            const data = await response.json();

            if (data.success) {
                alert('All logs have been deleted');
                // Reload status
                this.loadDetailedStatus();
                // Clear results
                const resultsTable = document.getElementById('resultsTable');
                if (resultsTable) resultsTable.innerHTML = '';
            } else {
                alert('Failed to delete logs: ' + (data.error || 'Unknown error'));
            }
        } catch (error) {
            alert('Failed to delete logs: ' + error.message);
        }
    },

    formatBytes(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    },

    currentViewLevel: 'main', // 'main' or 'fractal'
    currentView: null, // Current tab within the level

    // Show the main view (fractal listing / settings / fractal management)
    showMainView(tab = 'fractalListing', subPath = '') {
        this.currentViewLevel = 'main';
        this.currentView = tab;

        // Clear shared query state when navigating away from fractal views,
        // unless a share link is still waiting to be processed.
        if (window.QueryExecutor && !window.QueryExecutor.hasUnprocessedShareLink?.()) {
            window.QueryExecutor.clearSharedQueryState?.();
        }

        // Hide fractal view
        const fractalView = document.getElementById('fractalView');
        if (fractalView) fractalView.style.display = 'none';
        document.body.classList.remove('recall-active');

        // Show main view
        const mainView = document.getElementById('mainView');
        if (mainView) mainView.style.display = 'flex';

        // Switch to the requested tab
        this.showMainViewTab(tab, subPath);
    },

    showMainViewTab(tab, subPath = '') {
        this.currentViewLevel = 'main';
        this.currentView = tab;
        if (!this._navigatingFromPopState) {
            this._pushHash(this._buildHash(tab, subPath));
        }
        // Close alert details panel when switching to main view
        if (window.Alerts) {
            Alerts.closeAlertDetailsPanel();
            Alerts.stopPressurePolling();
        }

        // Close editor views when switching tabs
        if (window.Alerts) Alerts.closeAlertEditor();
        if (window.AnalyticsModels) AnalyticsModels.teardown();
        const actionsManageView = document.getElementById('actionsManageView');
        if (actionsManageView) {
            actionsManageView.style.display = 'none';
        }
        const normalizerEditorView = document.getElementById('normalizerEditorView');
        if (normalizerEditorView) {
            normalizerEditorView.style.display = 'none';
        }

        // Stop any running periodic updates from previous tabs
        if (window.FractalListing) FractalListing.hide();
        if (window.SettingsView) SettingsView.hide();
        if (window.Performance) Performance.hide();
        if (window.ContextLinks) ContextLinks.hide();
        if (window.Normalizers) Normalizers.hide();
        if (window.SchemaFields && SchemaFields.hide) SchemaFields.hide();

        // Hide all main view tab contents
        const fractalListingContent = document.getElementById('fractalListingTabContent');
        const mainPerformanceContent = document.getElementById('mainPerformanceTabContent');
        const mainSettingsContent = document.getElementById('mainSettingsTabContent');
        const mainNormalizersContent = document.getElementById('mainNormalizersTabContent');
        const mainSchemaContent = document.getElementById('mainSchemaTabContent');
        const mainApiContent = document.getElementById('mainApiTabContent');

        [fractalListingContent, mainPerformanceContent, mainSettingsContent, mainNormalizersContent, mainSchemaContent, mainApiContent].forEach(content => {
            if (content) content.style.display = 'none';
        });

        // Also hide inner view divs
        const settingsView = document.getElementById('settingsView');
        const performanceView = document.getElementById('performanceView');
        const normalizersView = document.getElementById('normalizersView');
        const schemaFieldsView = document.getElementById('schemaFieldsView');
        const apiExplorerView = document.getElementById('apiExplorerView');
        [settingsView, performanceView, normalizersView, schemaFieldsView, apiExplorerView].forEach(view => {
            if (view) view.style.display = 'none';
        });

        if (window.Sidebar) Sidebar.setActive('main', tab);
        document.body.classList.toggle('view-fill', this._fillViews.has(tab));

        // Show the requested tab and activate it
        switch (tab) {
            case 'fractalListing':
                if (fractalListingContent) fractalListingContent.style.display = 'flex';
                // Clear current fractal context when returning to fractal listing
                if (window.FractalContext) FractalContext.clearCurrentFractal();
                if (window.FractalListing) FractalListing.show();
                break;
            case 'performance':
                if (mainPerformanceContent) mainPerformanceContent.style.display = 'block';
                if (performanceView) performanceView.style.display = 'block';
                if (window.Performance) Performance.show(subPath);
                break;
            case 'settings':
                if (mainSettingsContent) mainSettingsContent.style.display = 'block';
                if (settingsView) settingsView.style.display = 'block';
                if (window.SettingsView) SettingsView.show(subPath);
                break;
            case 'normalizers':
                if (mainNormalizersContent) mainNormalizersContent.style.display = 'block';
                if (normalizersView) normalizersView.style.display = 'block';
                if (window.Normalizers) Normalizers.show(subPath);
                break;
            case 'schema':
                if (mainSchemaContent) mainSchemaContent.style.display = 'block';
                if (schemaFieldsView) schemaFieldsView.style.display = 'block';
                if (window.SchemaFields) SchemaFields.show();
                break;
            case 'api':
                // flex, not block: the explorer fills the viewport by flexing from
                // .container rather than subtracting a hardcoded chrome height.
                if (mainApiContent) mainApiContent.style.display = 'flex';
                if (apiExplorerView) apiExplorerView.style.display = 'flex';
                if (window.APIExplorer) APIExplorer.show();
                break;
        }
    },

    // Views sized to the viewport with internal scrolling; the rest scroll the page.
    _fillViews: new Set(['search', 'recall', 'chat', 'library', 'fractalListing', 'api']),

    // Alerts sub-tabs with their own route; the Rules list is the default.
    _alertsSubTabs: ['feeds', 'actions', 'coverage', 'policies', 'changes'],

    // Tabs that only exist for fractals, not prisms.
    _fractalOnlyTabs: new Set(['models', 'ingest', 'recall']),

    // Tabs the current scope does not have: fractal-only tabs in a prism, and
    // settings without the fractal admin role.
    _tabUnavailable(tab) {
        if (this._fractalOnlyTabs.has(tab) && window.FractalContext && FractalContext.isPrism()) return true;
        return tab === 'manage' && !(window.Auth && Auth.hasFractalRole('admin'));
    },

    // Runs on every scope change. Hiding the nav item is not enough: the tab
    // content would stay on screen showing the previous scope's data, so move the
    // user to search. Guarded on the fractal view being visible so this never
    // fires while navigating to the listing.
    updateScopedTabVisibility() {
        if (window.Sidebar) Sidebar.refresh();
        const fractalView = document.getElementById('fractalView');
        const onFractalView = fractalView && fractalView.style.display !== 'none';
        if (onFractalView && this.currentViewLevel === 'fractal' && this._tabUnavailable(this.currentView)) {
            this.showFractalViewTab('search');
        }
    },

    // Show the fractal view (search / comments / alerts / reference)
    showFractalView(tab = 'search', subPath = '') {
        this.currentViewLevel = 'fractal';
        this.currentView = tab;

        // Hide main view and stop any periodic updates
        const mainView = document.getElementById('mainView');
        if (mainView) mainView.style.display = 'none';

        // Stop fractal listing periodic updates when switching away from main view
        if (window.FractalListing) FractalListing.hide();

        // Show fractal view
        const fractalView = document.getElementById('fractalView');
        if (fractalView) fractalView.style.display = 'flex';

        if (window.Sidebar) Sidebar.refresh();

        // Switch to the requested tab
        this.showFractalViewTab(tab, subPath);
    },

    showFractalViewTab(tab, subPath = '') {
        if (this._tabUnavailable(tab)) {
            tab = 'search';
            subPath = '';
        }
        // Alerts reopens on the sub-tab last shown; resolving it here keeps the
        // pushed hash complete, so the sub-tab adds no second history entry.
        if (tab === 'alerts' && !subPath) {
            const last = document.querySelector('#alertsSubTabs .alerts-sub-tab.active')?.dataset.subtab;
            if (this._alertsSubTabs.includes(last)) subPath = last;
        }
        // Keep currentView in sync whether this was called via showFractalView()
        // or directly. pushSubPath() uses this.currentView to build hashes.
        this.currentViewLevel = 'fractal';
        this.currentView = tab;
        if (!this._navigatingFromPopState) {
            this._pushHash(this._buildHash(tab, subPath), this._buildFractalState());
        }

        // Clear shared query state when navigating away from the search tab,
        // unless a share link is still waiting to be processed. Once the link
        // has been landed on, leaving search retires its URL parameters so a
        // reload does not drag the user back to it.
        if (tab !== 'search' && window.QueryExecutor && !window.QueryExecutor.hasUnprocessedShareLink?.()) {
            window.QueryExecutor.clearSharedQueryState?.();
        }

        // Stop model backfill polling when leaving the models tab
        if (tab !== 'models' && window.AnalyticsModels && typeof AnalyticsModels.teardown === 'function') {
            AnalyticsModels.teardown();
        }

        // Stop Recall job polling when leaving the recall tab (server-side jobs
        // are unaffected; we only stop the client-side poller).
        if (tab !== 'recall' && window.Recall && typeof Recall.hide === 'function') {
            Recall.hide();
        }

        // Disconnect SSE when switching away from notebooks/dashboards
        if (tab !== 'notebooks' && window.Notebooks) {
            Notebooks.disconnectSSE();
            Notebooks.hideTOC();
        }
        if (tab !== 'dashboards' && window.Dashboards) {
            Dashboards.disconnectSSE();
            // Not folded into disconnectSSE: connectSSE calls that when moving
            // between dashboards, which would stop the ticker on a board still
            // on screen.
            Dashboards.stopUpdatedAtTicker();
        }

        // Close alert details panel when switching away from alerts tab
        if (tab !== 'alerts' && window.Alerts) {
            Alerts.closeAlertDetailsPanel();
            Alerts.stopPressurePolling();
        }

        // Navigation is the only way out of the alert editor, so tear it down on
        // every switch, including back into the alerts tab itself.
        if (window.Alerts) Alerts.closeAlertEditor();

        // Close the actions manage view when switching away from alerts tab
        if (tab !== 'alerts') {
            const actionsManageView = document.getElementById('actionsManageView');
            if (actionsManageView) {
                actionsManageView.style.display = 'none';
            }
            // Close the alert config panel and feed details panel
            if (window.Alerts) {
                Alerts.closeAlertPanel();
                Alerts.closeAlertDetailsPanel();
            }
            if (window.AlertFeeds) {
                AlertFeeds.closeDetailsPanel(true);
            }
            // The coverage drawer is position:fixed, so it must be closed rather
            // than left to disappear with its container.
            if (window.AttackCoverage) {
                AttackCoverage.hide();
            }
        }

        // Hide all fractal view tab contents
        const searchContent = document.getElementById('fractalSearchTabContent');
        const commentsContent = document.getElementById('fractalCommentsTabContent');
        const notebooksContent = document.getElementById('fractalNotebooksTabContent');
        const dashboardsContent = document.getElementById('fractalDashboardsTabContent');
        const dictionariesContent = document.getElementById('fractalDictionariesTabContent');
        const modelsContent = document.getElementById('fractalModelsTabContent');
        const chatContent = document.getElementById('fractalChatTabContent');
        const libraryContent = document.getElementById('fractalLibraryTabContent');
        const alertsContent = document.getElementById('fractalAlertsTabContent');
        const ingestContent = document.getElementById('fractalIngestTabContent');
        const recallContent = document.getElementById('fractalRecallTabContent');
        const manageContent = document.getElementById('fractalManageTabContent');

        [searchContent, commentsContent, notebooksContent, dashboardsContent, dictionariesContent, modelsContent, chatContent, libraryContent, alertsContent, ingestContent, recallContent, manageContent].forEach(content => {
            if (content) content.style.display = 'none';
        });

        document.body.classList.remove('recall-active');
        document.body.classList.toggle('view-fill', this._fillViews.has(tab));

        // Also hide the inner view divs
        const searchView = document.getElementById('searchView');
        const commentedView = document.getElementById('commentedView');
        const notebooksView = document.getElementById('notebooksView');
        const dashboardsView = document.getElementById('dashboardsView');
        const dictionariesView = document.getElementById('dictionariesView');
        const modelsView = document.getElementById('modelsView');
        const chatView = document.getElementById('chatView');
        const librariesView = document.getElementById('librariesView');
        const alertsView = document.getElementById('alertsView');
        const feedAlertsView = document.getElementById('feedAlertsView');
        const ingestView = document.getElementById('ingestView');
        const recallView = document.getElementById('recallView');
        [searchView, commentedView, notebooksView, dashboardsView, dictionariesView, modelsView, chatView, librariesView, alertsView, feedAlertsView, ingestView, recallView].forEach(view => {
            if (view) view.style.display = 'none';
        });

        const investigationSubTabs = document.getElementById('investigationSubTabs');
        if (investigationSubTabs) investigationSubTabs.style.display = 'none';
        if (window.Sidebar) Sidebar.setActive('fractal', tab);

        // Show the requested tab and activate it
        switch (tab) {
            case 'search':
                if (searchContent) searchContent.style.display = 'flex';
                if (searchView) searchView.style.display = 'flex';

                // Re-render syntax highlighting when returning to search tab
                if (window.SyntaxHighlight) {
                    SyntaxHighlight.updateHighlight('queryInput', 'queryHighlight');
                }

                // QueryExecutor.onFractalChange() will handle loading recent logs when fractal changes
                // No need to duplicate the call here as it causes race conditions
                break;
            // 'comments' is kept as an alias so links made before the merge still
            // land somewhere sensible.
            case 'comments':
                subPath = 'evidence';
                // falls through
            case 'notebooks': {
                if (investigationSubTabs) investigationSubTabs.style.display = 'flex';

                const onEvidence = subPath === 'evidence';
                document.querySelectorAll('[data-investigation-subtab]').forEach(btn => {
                    btn.classList.toggle('active', (btn.dataset.investigationSubtab === 'evidence') === onEvidence);
                });

                if (onEvidence) {
                    if (commentsContent) commentsContent.style.display = 'block';
                    if (commentedView) commentedView.style.display = 'block';
                    if (window.CommentedLogs) CommentedLogs.show();
                    if (window.RealTimeComments) RealTimeComments.markAsRead();
                } else {
                    if (notebooksContent) notebooksContent.style.display = 'block';
                    if (notebooksView) notebooksView.style.display = 'block';
                    if (window.Notebooks) {
                        Notebooks.init();
                        if (subPath) Notebooks.openNotebook(subPath);
                    }
                }
                break;
            }
            case 'dashboards':
                if (dashboardsContent) dashboardsContent.style.display = 'block';
                if (dashboardsView) dashboardsView.style.display = 'block';

                if (window.Dashboards) {
                    Dashboards.init();
                    if (subPath) Dashboards.openDashboard(subPath);
                } else {
                    console.error('[App] Dashboards module not found! Check if dashboards.js loaded properly.');
                }
                break;
            case 'dictionaries':
                if (dictionariesContent) dictionariesContent.style.display = 'block';
                if (dictionariesView) dictionariesView.style.display = 'block';

                if (window.Dictionaries) Dictionaries.show(subPath);
                break;
            case 'models':
                if (modelsContent) modelsContent.style.display = 'block';
                if (modelsView) modelsView.style.display = 'block';

                if (window.AnalyticsModels) AnalyticsModels.show(subPath);
                break;
            case 'chat':
                if (chatContent) chatContent.style.display = 'flex';
                if (chatView) chatView.style.display = 'flex';

                if (window.Chat) Chat.show(subPath);
                break;
            case 'library':
                if (libraryContent) libraryContent.style.display = 'flex';
                if (librariesView) librariesView.style.display = 'flex';

                if (window.InstructionLibraries) InstructionLibraries.show(subPath);
                break;
            case 'alerts':
                if (alertsContent) alertsContent.style.display = 'block';

                // Re-render alert query syntax highlighting when returning to alerts tab
                if (window.SyntaxHighlight) {
                    SyntaxHighlight.updateHighlight('editorQueryInput', 'alertQueryHighlight');
                }

                // A sub-tab named in the URL; any other subPath is an alert id.
                {
                    const sub = this._alertsSubTabs.includes(subPath) ? subPath : '';
                    if (sub === 'coverage') AlertFeeds.showCoverageTab();
                    else if (sub === 'actions') AlertFeeds.showActionsTab();
                    else if (sub === 'policies') AlertFeeds.showPoliciesTab();
                    else if (sub === 'changes') AlertFeeds.showChangesTab();
                    else if (sub === 'feeds') AlertFeeds.showFeedAlertsTab();
                    else {
                        AlertFeeds.activateSubTab('manual', 'alertsView');
                        Alerts.show(subPath && subPath !== 'manual' ? subPath : '');
                    }
                }
                break;
            case 'ingest':
                if (ingestContent) ingestContent.style.display = 'block';
                if (ingestView) ingestView.style.display = 'block';
                if (window.IngestTokens) IngestTokens.show();
                break;
            case 'recall':
                if (recallContent) recallContent.style.display = 'flex';
                if (recallView) recallView.style.display = 'flex';
                document.body.classList.add('recall-active');
                if (window.Recall) Recall.show(subPath);
                break;
            case 'manage':
                if (manageContent) manageContent.style.display = 'block';
                if (window.FractalManageTab) FractalManageTab.show(subPath);
                break;
        }
    },

    getCurrentView() {
        return this.currentView;
    },


    // Safe to call again after a view renders its own query box: a handle that is
    // already wired is skipped rather than given a second set of listeners.
    setupQueryResizeHandles() {
        document.querySelectorAll('.query-resize-handle').forEach(handle => {
            const targetId = handle.dataset.target;
            const textarea = document.getElementById(targetId);
            if (!textarea || handle.dataset.bound) return;
            handle.dataset.bound = '1';

            const wrapper = textarea.closest('.query-input-wrapper');
            const highlight = wrapper ? wrapper.querySelector('.query-highlight') : null;

            let startY, startHeight;

            const onMouseMove = (e) => {
                const delta = e.clientY - startY;
                const newHeight = Math.max(38, Math.min(400, startHeight + delta));
                textarea.style.height = newHeight + 'px';
                textarea.style.minHeight = newHeight + 'px';
                if (wrapper) {
                    wrapper.style.height = newHeight + 'px';
                    wrapper.style.minHeight = newHeight + 'px';
                }
                if (highlight) highlight.style.minHeight = newHeight + 'px';
            };

            const onMouseUp = () => {
                document.removeEventListener('mousemove', onMouseMove);
                document.removeEventListener('mouseup', onMouseUp);
                document.body.style.cursor = '';
                document.body.style.userSelect = '';
            };

            handle.addEventListener('mousedown', (e) => {
                e.preventDefault();
                startY = e.clientY;
                startHeight = textarea.offsetHeight;
                document.body.style.cursor = 'ns-resize';
                document.body.style.userSelect = 'none';
                document.addEventListener('mousemove', onMouseMove);
                document.addEventListener('mouseup', onMouseUp);
            });
        });
    },

    setupQueryLineNumbers() {
        document.querySelectorAll('.query-input-wrapper').forEach(wrapper => {
            const textarea = wrapper.querySelector('.search-input');
            if (!textarea || wrapper.dataset.gutterBound) return;
            wrapper.dataset.gutterBound = '1';

            const gutter = document.createElement('div');
            gutter.className = 'query-line-numbers';
            gutter.textContent = '1';
            wrapper.appendChild(gutter);

            // Hidden mirror used to measure how many visual rows each logical line
            // occupies once wrapped, so the gutter numbers stay aligned with the
            // text instead of drifting after the first wrapped line. One for the
            // page: every measurement restyles it, and a per-editor copy would be
            // left behind in the body each time a view rebuilt its query box.
            const mirror = this._lineMirror || (this._lineMirror = (() => {
                const el = document.createElement('div');
                el.setAttribute('aria-hidden', 'true');
                // pre-wrap with default (normal) overflow-wrap, matching the textarea:
                // wraps at spaces; long unbroken tokens overflow on a single row.
                el.style.cssText = 'position:absolute;visibility:hidden;left:-9999px;top:0;white-space:pre-wrap;overflow-wrap:normal;word-break:normal;';
                document.body.appendChild(el);
                return el;
            })());

            const rowsForLine = (line, cs, lineHeight) => {
                const padL = parseFloat(cs.paddingLeft) || 0;
                const padR = parseFloat(cs.paddingRight) || 0;
                const contentWidth = textarea.clientWidth - padL - padR;
                // Hidden/zero-width editor: can't measure wrapping; assume 1 row.
                if (contentWidth <= 0) return 1;
                ['fontFamily', 'fontSize', 'fontWeight', 'fontStyle', 'letterSpacing', 'lineHeight', 'tabSize']
                    .forEach(p => { mirror.style[p] = cs[p]; });
                mirror.style.width = contentWidth + 'px';
                mirror.textContent = line.length ? line : ' ';
                return Math.max(1, Math.round(mirror.offsetHeight / lineHeight));
            };

            const caretLine = () => {
                const before = textarea.value.substring(0, textarea.selectionStart);
                return (before.match(/\n/g) || []).length;
            };

            // Subtly brighten the gutter number for the caret's current line.
            const setActive = (idx) => {
                const prev = gutter.querySelector('.ql-cur');
                if (prev) prev.classList.remove('ql-cur');
                if (idx == null) return;
                const el = gutter.querySelector('span[data-ln="' + idx + '"]');
                if (el) el.classList.add('ql-cur');
            };
            const refreshActive = () => setActive(document.activeElement === textarea ? caretLine() : null);

            const update = () => {
                const cs = window.getComputedStyle(textarea);
                const lineHeight = parseFloat(cs.lineHeight) || (parseFloat(cs.fontSize) * 1.5);
                const lines = textarea.value.split('\n');
                const rows = [];
                for (let i = 0; i < lines.length; i++) {
                    rows.push('<span data-ln="' + i + '">' + (i + 1) + '</span>');
                    // Pad the continuation rows of a wrapped line with blanks.
                    const extra = rowsForLine(lines[i], cs, lineHeight) - 1;
                    for (let r = 0; r < extra; r++) rows.push('<span></span>');
                }
                gutter.innerHTML = rows.join('\n');
                refreshActive();
                gutter.scrollTop = textarea.scrollTop;
            };

            textarea.addEventListener('input', update);
            // Caret can move without changing text (arrows, click): re-mark only.
            textarea.addEventListener('keyup', refreshActive);
            textarea.addEventListener('click', refreshActive);
            textarea.addEventListener('focus', refreshActive);
            textarea.addEventListener('blur', () => setActive(null));
            textarea.addEventListener('scroll', () => {
                gutter.scrollTop = textarea.scrollTop;
            });
            // Wrapping changes with the editor's width, which a window resize is not
            // the only cause of: dragging the definition rail resizes it too. An
            // observer also stops on its own once a rebuilt editor drops this
            // textarea, where a window listener would live on holding it forever.
            new ResizeObserver(update).observe(textarea);

            update();
        });
    },

    // Reformat a BQL query so each top-level pipeline stage sits on its own line.
    // Pipes inside strings, regex literals, (), [], and {} (e.g. case branches and
    // regex alternation) are left untouched, so it never corrupts a query.
    formatBQL(q) {
        const isRegexStart = (prev) => prev === '' || "=~^$(,[{|".includes(prev);
        const segments = [];
        let buf = '';
        let i = 0;
        const n = q.length;
        let dParen = 0, dBracket = 0, dBrace = 0;
        let prevNonSpace = '';

        while (i < n) {
            const ch = q[i];

            if (ch === '"' || ch === "'") {
                const quote = ch;
                buf += ch; i++;
                while (i < n) {
                    if (q[i] === '\\' && i + 1 < n) { buf += q[i] + q[i + 1]; i += 2; continue; }
                    buf += q[i];
                    if (q[i] === quote) { i++; break; }
                    i++;
                }
                prevNonSpace = quote;
                continue;
            }
            if (ch === '/' && q[i + 1] === '/') { // line comment
                while (i < n && q[i] !== '\n') { buf += q[i]; i++; }
                continue;
            }
            if (ch === '/' && isRegexStart(prevNonSpace)) { // regex literal
                buf += ch; i++;
                while (i < n) {
                    if (q[i] === '\\' && i + 1 < n) { buf += q[i] + q[i + 1]; i += 2; continue; }
                    if (q[i] === '\n') break; // unterminated; bail
                    buf += q[i];
                    if (q[i] === '/') { i++; break; }
                    i++;
                }
                while (i < n && /[a-z]/i.test(q[i])) { buf += q[i]; i++; } // flags
                prevNonSpace = '/';
                continue;
            }

            if (ch === '(') dParen++;
            else if (ch === ')') dParen = Math.max(0, dParen - 1);
            else if (ch === '[') dBracket++;
            else if (ch === ']') dBracket = Math.max(0, dBracket - 1);
            else if (ch === '{') dBrace++;
            else if (ch === '}') dBrace = Math.max(0, dBrace - 1);

            if (ch === '|' && dParen === 0 && dBracket === 0 && dBrace === 0) {
                segments.push(buf);
                buf = '';
                i++;
                while (i < n && (q[i] === ' ' || q[i] === '\t')) i++;
                prevNonSpace = '|';
                continue;
            }

            buf += ch;
            if (ch !== ' ' && ch !== '\t' && ch !== '\n') prevNonSpace = ch;
            i++;
        }
        segments.push(buf);

        const lines = [];
        segments.map(s => s.trim()).forEach((s, k) => {
            if (k === 0) { if (s !== '') lines.push(s); }
            else lines.push('| ' + s);
        });
        return lines.join('\n');
    },

    formatQuery(textarea) {
        if (!textarea) return;
        const formatted = this.formatBQL(textarea.value);
        if (formatted === textarea.value) return;
        textarea.value = formatted;
        const end = formatted.length;
        textarea.setSelectionRange(end, end);
        // Drive highlight, growth, gutter, and history off the normal input path.
        textarea.dispatchEvent(new Event('input', { bubbles: true }));
    },

    initToolbarMenus() {
        // Queries is handled by QueryPalette (it owns its own button + popover).
        const defs = [
            { btnId: 'shareMenuBtn',   menuId: 'shareMenu',   wrapId: 'shareMenuWrap'   },
            { btnId: 'exportMenuBtn',  menuId: 'exportMenu',  wrapId: 'exportMenuWrap'  },
            { btnId: 'rowsMenuBtn',    menuId: 'rowsMenu',    wrapId: 'rowsMenuWrap'    },
            { btnId: 'alertExportMenuBtn', menuId: 'alertExportMenu', wrapId: 'alertExportMenuWrap' },
        ];

        const closeAll = () => {
            defs.forEach(({ btnId, menuId }) => {
                const m = document.getElementById(menuId);
                const b = document.getElementById(btnId);
                if (m) m.style.display = 'none';
                if (b) b.classList.remove('active');
            });
        };

        defs.forEach(({ btnId, menuId, wrapId }) => {
            const btn  = document.getElementById(btnId);
            const menu = document.getElementById(menuId);
            const wrap = document.getElementById(wrapId);
            if (!btn || !menu || !wrap) return;

            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const opening = menu.style.display === 'none';
                closeAll();
                if (opening) {
                    menu.style.display = 'block';
                    btn.classList.add('active');
                }
            });

            // Close after an item is chosen — capture phase so it fires before
            // stopPropagation in child button handlers.
            menu.addEventListener('click', () => {
                menu.style.display = 'none';
                btn.classList.remove('active');
            }, true);
        });

        document.addEventListener('click', (e) => {
            defs.forEach(({ menuId, btnId, wrapId }) => {
                const wrap = document.getElementById(wrapId);
                if (wrap && !wrap.contains(e.target)) {
                    const m = document.getElementById(menuId);
                    const b = document.getElementById(btnId);
                    if (m) m.style.display = 'none';
                    if (b) b.classList.remove('active');
                }
            });
        });

        const rowsMenu = document.getElementById('rowsMenu');
        if (rowsMenu) rowsMenu.addEventListener('click', (e) => {
            const item = e.target.closest('[data-row-mode]');
            if (item) QueryExecutor.setRowMode(item.dataset.rowMode);
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                closeAll();
                if (document.body.classList.contains('results-fullscreen')) {
                    QueryExecutor.toggleFullscreen();
                }
            }
        });
    }
};


// Make globally available
window.App = App;

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => App.init());
} else {
    App.init();
}
