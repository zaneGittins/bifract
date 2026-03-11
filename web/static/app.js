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

    init() {
        // Initialize all modules
        if (window.TimeBar) {
            TimeBar.init();
        }

        if (window.SyntaxHighlight) {
            SyntaxHighlight.init();
        }

        if (window.Autocomplete) {
            Autocomplete.init();
        }

        if (window.LogDetail) {
            LogDetail.init();
        }

        if (window.FieldStats) {
            FieldStats.init();
        }

        if (window.RecentQueries) {
            RecentQueries.init();
        }

        if (window.SavedQueries) {
            SavedQueries.init();
        }

        if (window.Settings) {
            Settings.init();
        }

        // Initialize authentication
        if (window.Auth) {
            Auth.init();
        }

        // Initialize fractal management (selector will be initialized after login)
        if (window.FractalManagement) {
            FractalManagement.init();
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

        // Initialize chat module
        if (window.Chat) {
            Chat.init();
        }

        // Initialize ingest tokens module
        if (window.IngestTokens) {
            IngestTokens.init();
        }

        // Initialize performance module
        if (window.Performance) {
            Performance.init();
        }

        if (window.SendToNotebook) {
            SendToNotebook.init();
        }

        if (window.ContextLinks) {
            ContextLinks.init();
        }
        if (window.Normalizers) {
            Normalizers.init();
        }
        if (window.AlertFeeds) {
            AlertFeeds.init();
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

        this.setupEventListeners();
        this.checkStatus();

        // Check status every 30 seconds
        setInterval(() => this.checkStatus(), 30000);

    },

    // Tab name sets for hash-based open-in-new-tab support.
    _mainTabs: new Set(['fractalListing', 'reference', 'performance', 'settings', 'context', 'normalizers']),
    _fractalTabs: new Set(['search', 'comments', 'notebooks', 'dashboards', 'dictionaries', 'chat', 'alerts', 'ingest', 'manage']),

    // Build the target URL for a given hash (used for open-in-new-tab).
    _tabUrl(hash) {
        const base = window.location.origin + window.location.pathname;
        return hash ? base + '#' + hash : base;
    },

    // Bind click + middle-click/ctrl+click on a tab button.
    // Normal click calls handler; middle/ctrl+click opens the tab URL in a new browser tab.
    _bindTab(el, handler, hash) {
        if (!el) return;
        el.addEventListener('click', (e) => {
            if (e.ctrlKey || e.metaKey) {
                e.preventDefault();
                window.open(this._tabUrl(hash), '_blank');
            } else {
                handler();
            }
        });
        el.addEventListener('auxclick', (e) => {
            if (e.button === 1) {
                e.preventDefault();
                window.open(this._tabUrl(hash), '_blank');
            }
        });
    },

    // One-time route from the URL hash on page load. No popstate/pushState.
    routeFromHash() {
        const hash = window.location.hash.replace(/^#/, '');
        if (!hash || hash === 'fractalListing') {
            this.showMainView('fractalListing');
            return;
        }
        if (this._mainTabs.has(hash)) {
            this.showMainView(hash);
            return;
        }
        if (this._fractalTabs.has(hash)) {
            if (!FractalContext.currentFractal && window.FractalContext) {
                const restored = FractalContext.restoreFromStorage();
                if (!restored) {
                    this.showMainView('fractalListing');
                    return;
                }
            }
            this.showFractalView(hash);
            return;
        }
        this.showMainView('fractalListing');
    },

    setupEventListeners() {
        // Logo click - navigate to main view (fractal listing)
        const logo = document.querySelector('.logo');
        if (logo) {
            this._bindTab(logo, () => this.showMainView('fractalListing'), '');
            logo.style.cursor = 'pointer';
        }

        // Main View Tab Buttons
        this._bindTab(document.getElementById('fractalListingTabBtn'), () => this.showMainViewTab('fractalListing'), 'fractalListing');
        this._bindTab(document.getElementById('queryReferenceTabBtn'), () => this.showMainViewTab('reference'), 'reference');
        this._bindTab(document.getElementById('mainPerformanceTabBtn'), () => this.showMainViewTab('performance'), 'performance');
        this._bindTab(document.getElementById('mainSettingsTabBtn'), () => this.showMainViewTab('settings'), 'settings');
        this._bindTab(document.getElementById('mainContextTabBtn'), () => this.showMainViewTab('context'), 'context');
        this._bindTab(document.getElementById('mainNormalizersTabBtn'), () => this.showMainViewTab('normalizers'), 'normalizers');

        // Fractal View Tab Buttons
        this._bindTab(document.getElementById('fractalSearchTabBtn'), () => this.showFractalViewTab('search'), 'search');
        this._bindTab(document.getElementById('fractalCommentsTabBtn'), () => this.showFractalViewTab('comments'), 'comments');
        this._bindTab(document.getElementById('fractalNotebooksTabBtn'), () => this.showFractalViewTab('notebooks'), 'notebooks');
        this._bindTab(document.getElementById('fractalDashboardsTabBtn'), () => this.showFractalViewTab('dashboards'), 'dashboards');
        this._bindTab(document.getElementById('fractalDictionariesTabBtn'), () => this.showFractalViewTab('dictionaries'), 'dictionaries');
        this._bindTab(document.getElementById('fractalChatTabBtn'), () => this.showFractalViewTab('chat'), 'chat');
        this._bindTab(document.getElementById('fractalAlertsTabBtn'), () => this.showFractalViewTab('alerts'), 'alerts');
        this._bindTab(document.getElementById('fractalIngestTabBtn'), () => this.showFractalViewTab('ingest'), 'ingest');
        this._bindTab(document.getElementById('fractalManageTabBtn'), () => this.showFractalViewTab('manage'), 'manage');

        // Query input
        const queryInput = document.getElementById('queryInput');
        const executeBtn = document.getElementById('executeBtn');

        if (queryInput) {
            queryInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    if (window.QueryExecutor) {
                        QueryExecutor.execute();
                    }
                } else if (e.key === 'Enter' && e.shiftKey) {
                    // Allow new line (default behavior)
                } else if (e.key === 'Tab') {
                    e.preventDefault();
                    const start = queryInput.selectionStart;
                    const end = queryInput.selectionEnd;
                    const value = queryInput.value;

                    // Insert tab character at cursor position
                    queryInput.value = value.substring(0, start) + '\t' + value.substring(end);

                    // Move cursor after the inserted tab
                    queryInput.selectionStart = queryInput.selectionEnd = start + 1;

                    // Trigger input event to update syntax highlighting
                    queryInput.dispatchEvent(new Event('input'));
                } else if (e.key === '/' && e.ctrlKey) {
                    e.preventDefault();
                    this.toggleLineComment(queryInput);
                } else if (e.key === 'z' && e.ctrlKey && !e.shiftKey) {
                    e.preventDefault();
                    this.undo('main', queryInput);
                } else if ((e.key === 'y' && e.ctrlKey) || (e.key === 'z' && e.ctrlKey && e.shiftKey)) {
                    e.preventDefault();
                    this.redo('main', queryInput);
                }
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
                    QueryExecutor.execute();
                }
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

        // Time range
        const timeRangeSelect = document.getElementById('timeRange');
        const customTimeInputs = document.getElementById('customTimeInputs');

        if (timeRangeSelect && customTimeInputs) {
            timeRangeSelect.addEventListener('change', (e) => {
                if (e.target.value === 'custom') {
                    customTimeInputs.style.display = 'flex';
                } else {
                    customTimeInputs.style.display = 'none';
                    // Save non-custom selection immediately so it persists on fractal switch
                    if (window.QueryExecutor) {
                        QueryExecutor.saveTimeRangeToStorage(e.target.value, null);
                    }
                }
            });
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
        const inputType = textarea.id === 'queryInput' ? 'main' : 'alert';
        this.saveToHistoryImmediate(inputType, textarea.value, true);
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
            const response = await fetch('/api/v1/status');
            const data = await response.json();

            if (statusDot && statusContainer) {
                if (data.success && data.clickhouse && data.clickhouse.connected) {
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
                html += `<div class="status-item"><span class="status-label">Earliest Log:</span><span class="status-value">${ch.oldest_log || 'N/A'}</span></div>`;
                html += `<div class="status-item"><span class="status-label">Latest Log:</span><span class="status-value">${ch.newest_log || 'N/A'}</span></div>`;
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
    showMainView(tab = 'fractalListing') {
        console.log('[App] Showing main view, tab:', tab);

        this.currentViewLevel = 'main';
        this.currentView = tab;

        // Clear shared query state when navigating away from fractal views
        // BUT NOT if we have share parameters that need to be processed
        if (window.QueryExecutor && typeof window.QueryExecutor.clearSharedQueryState === 'function') {
            // Check if we have share parameters before clearing
            const urlParams = new URLSearchParams(window.location.search);
            const hasShareParams = urlParams.has('q') && urlParams.has('tr') && urlParams.has('f');

            if (!hasShareParams) {
                console.log('[App] No share parameters, safe to clear shared query state');
                window.QueryExecutor.clearSharedQueryState();
            } else {
                console.log('[App] Share parameters detected, skipping clear to allow processing');
            }
        }

        // Hide fractal view
        const fractalView = document.getElementById('fractalView');
        if (fractalView) fractalView.style.display = 'none';
        document.body.classList.remove('search-active');

        // Show main view
        const mainView = document.getElementById('mainView');
        if (mainView) mainView.style.display = 'flex';

        // Toggle header navs
        const mainViewNav = document.getElementById('mainViewNav');
        const fractalViewNav = document.getElementById('fractalViewNav');
        if (mainViewNav) mainViewNav.style.display = 'flex';
        if (fractalViewNav) fractalViewNav.style.display = 'none';

        // Switch to the requested tab
        this.showMainViewTab(tab);
    },

    showMainViewTab(tab) {
        // Close alert details panel when switching to main view
        if (window.Alerts) {
            Alerts.closeAlertDetailsPanel();
            Alerts.stopPressurePolling();
        }

        // Close editor views when switching tabs
        const alertEditorView = document.getElementById('alertEditorView');
        if (alertEditorView) {
            alertEditorView.style.display = 'none';
        }
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

        // Hide all main view tab contents
        const fractalListingContent = document.getElementById('fractalListingTabContent');
        const queryReferenceContent = document.getElementById('queryReferenceTabContent');
        const mainPerformanceContent = document.getElementById('mainPerformanceTabContent');
        const mainSettingsContent = document.getElementById('mainSettingsTabContent');
        const mainContextContent = document.getElementById('mainContextTabContent');
        const mainNormalizersContent = document.getElementById('mainNormalizersTabContent');

        [fractalListingContent, queryReferenceContent, mainPerformanceContent, mainSettingsContent, mainContextContent, mainNormalizersContent].forEach(content => {
            if (content) content.style.display = 'none';
        });

        // Also hide inner view divs
        const settingsView = document.getElementById('settingsView');
        const performanceView = document.getElementById('performanceView');
        const contextLinksView = document.getElementById('contextLinksView');
        const normalizersView = document.getElementById('normalizersView');
        [settingsView, performanceView, contextLinksView, normalizersView].forEach(view => {
            if (view) view.style.display = 'none';
        });

        // Remove active class from all main view tabs
        const fractalListingTab = document.getElementById('fractalListingTabBtn');
        const queryReferenceTab = document.getElementById('queryReferenceTabBtn');
        const mainPerformanceTab = document.getElementById('mainPerformanceTabBtn');
        const mainSettingsTab = document.getElementById('mainSettingsTabBtn');
        const mainContextTab = document.getElementById('mainContextTabBtn');
        const mainNormalizersTab = document.getElementById('mainNormalizersTabBtn');

        [fractalListingTab, queryReferenceTab, mainPerformanceTab, mainSettingsTab, mainContextTab, mainNormalizersTab].forEach(tabBtn => {
            if (tabBtn) tabBtn.classList.remove('active');
        });

        // Show the requested tab and activate it
        switch (tab) {
            case 'fractalListing':
                if (fractalListingContent) fractalListingContent.style.display = 'flex';
                if (fractalListingTab) fractalListingTab.classList.add('active');
                // Clear current fractal context when returning to fractal listing
                if (window.FractalContext) FractalContext.clearCurrentFractal();
                if (window.FractalListing) FractalListing.show();
                break;
            case 'reference':
                if (queryReferenceContent) queryReferenceContent.style.display = 'block';
                if (queryReferenceTab) queryReferenceTab.classList.add('active');
                // Show reference view
                const referenceView = document.getElementById('referenceView');
                if (referenceView) referenceView.style.display = 'block';
                if (window.QueryReference) QueryReference.show();
                break;
            case 'performance':
                if (mainPerformanceContent) mainPerformanceContent.style.display = 'block';
                if (performanceView) performanceView.style.display = 'block';
                if (mainPerformanceTab) mainPerformanceTab.classList.add('active');
                if (window.Performance) Performance.show();
                break;
            case 'settings':
                if (mainSettingsContent) mainSettingsContent.style.display = 'block';
                if (settingsView) settingsView.style.display = 'block';
                if (mainSettingsTab) mainSettingsTab.classList.add('active');
                if (window.SettingsView) SettingsView.show();
                break;
            case 'context':
                if (mainContextContent) mainContextContent.style.display = 'block';
                if (contextLinksView) contextLinksView.style.display = 'block';
                if (mainContextTab) mainContextTab.classList.add('active');
                if (window.ContextLinks) ContextLinks.show();
                break;
            case 'normalizers':
                if (mainNormalizersContent) mainNormalizersContent.style.display = 'block';
                if (normalizersView) normalizersView.style.display = 'block';
                if (mainNormalizersTab) mainNormalizersTab.classList.add('active');
                if (window.Normalizers) Normalizers.show();
                break;
        }
    },

    // Show the fractal view (search / comments / alerts / reference)
    showFractalView(tab = 'search') {
        console.log('[App] Showing fractal view, tab:', tab);

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

        // Toggle header navs
        const mainViewNav = document.getElementById('mainViewNav');
        const fractalViewNav = document.getElementById('fractalViewNav');
        if (mainViewNav) mainViewNav.style.display = 'none';
        if (fractalViewNav) fractalViewNav.style.display = 'flex';

        // Hide Ingest tab for prisms (ingest tokens are per-fractal)
        const ingestTabBtn = document.getElementById('fractalIngestTabBtn');
        if (ingestTabBtn) {
            ingestTabBtn.style.display = (window.FractalContext && window.FractalContext.isPrism()) ? 'none' : '';
        }

        // Switch to the requested tab
        this.showFractalViewTab(tab);
    },

    showFractalViewTab(tab) {
        // Clear shared query state when navigating away from search tab
        // BUT NOT if we have share parameters that need to be processed
        if (tab !== 'search' && window.QueryExecutor && typeof window.QueryExecutor.clearSharedQueryState === 'function') {
            // Check if we have share parameters before clearing
            const urlParams = new URLSearchParams(window.location.search);
            const hasShareParams = urlParams.has('q') && urlParams.has('tr') && urlParams.has('f');

            if (!hasShareParams) {
                console.log('[App] No share parameters, safe to clear shared query state');
                window.QueryExecutor.clearSharedQueryState();
            } else {
                console.log('[App] Share parameters detected, skipping clear to allow processing');
            }
        }

        // Close alert details panel when switching away from alerts tab
        if (tab !== 'alerts' && window.Alerts) {
            Alerts.closeAlertDetailsPanel();
            Alerts.stopPressurePolling();
        }

        // Close alert editor and actions manage views when switching away from alerts tab
        if (tab !== 'alerts') {
            const alertEditorView = document.getElementById('alertEditorView');
            if (alertEditorView) {
                alertEditorView.style.display = 'none';
            }
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
                AlertFeeds.closeDetailsPanel();
            }
        }

        // Hide all fractal view tab contents
        const searchContent = document.getElementById('fractalSearchTabContent');
        const commentsContent = document.getElementById('fractalCommentsTabContent');
        const notebooksContent = document.getElementById('fractalNotebooksTabContent');
        const dashboardsContent = document.getElementById('fractalDashboardsTabContent');
        const dictionariesContent = document.getElementById('fractalDictionariesTabContent');
        const chatContent = document.getElementById('fractalChatTabContent');
        const alertsContent = document.getElementById('fractalAlertsTabContent');
        const ingestContent = document.getElementById('fractalIngestTabContent');
        const manageContent = document.getElementById('fractalManageTabContent');

        [searchContent, commentsContent, notebooksContent, dashboardsContent, dictionariesContent, chatContent, alertsContent, ingestContent, manageContent].forEach(content => {
            if (content) content.style.display = 'none';
        });

        // Remove search-active lock so other tabs can scroll normally
        document.body.classList.remove('search-active');

        // Also hide the inner view divs
        const searchView = document.getElementById('searchView');
        const commentedView = document.getElementById('commentedView');
        const notebooksView = document.getElementById('notebooksView');
        const dashboardsView = document.getElementById('dashboardsView');
        const dictionariesView = document.getElementById('dictionariesView');
        const chatView = document.getElementById('chatView');
        const alertsView = document.getElementById('alertsView');
        const feedAlertsView = document.getElementById('feedAlertsView');
        const ingestView = document.getElementById('ingestView');
        const referenceView = document.getElementById('referenceView');
        [searchView, commentedView, notebooksView, dashboardsView, dictionariesView, chatView, alertsView, feedAlertsView, ingestView, referenceView].forEach(view => {
            if (view) view.style.display = 'none';
        });

        // Remove active class from all fractal view tabs
        const searchTab = document.getElementById('fractalSearchTabBtn');
        const commentsTab = document.getElementById('fractalCommentsTabBtn');
        const notebooksTab = document.getElementById('fractalNotebooksTabBtn');
        const dashboardsTab = document.getElementById('fractalDashboardsTabBtn');
        const dictionariesTab = document.getElementById('fractalDictionariesTabBtn');
        const chatTab = document.getElementById('fractalChatTabBtn');
        const alertsTab = document.getElementById('fractalAlertsTabBtn');
        const ingestTab = document.getElementById('fractalIngestTabBtn');
        const manageTab = document.getElementById('fractalManageTabBtn');

        [searchTab, commentsTab, notebooksTab, dashboardsTab, dictionariesTab, chatTab, alertsTab, ingestTab, manageTab].forEach(tabBtn => {
            if (tabBtn) tabBtn.classList.remove('active');
        });

        // Show the requested tab and activate it
        switch (tab) {
            case 'search':
                if (searchContent) searchContent.style.display = 'flex';
                if (searchView) searchView.style.display = 'flex';
                if (searchTab) searchTab.classList.add('active');
                document.body.classList.add('search-active');

                // Re-render syntax highlighting when returning to search tab
                if (window.SyntaxHighlight) {
                    SyntaxHighlight.updateHighlight('queryInput', 'queryHighlight');
                }

                // QueryExecutor.onFractalChange() will handle loading recent logs when fractal changes
                // No need to duplicate the call here as it causes race conditions
                break;
            case 'comments':
                console.log('[App] Showing comments tab');
                console.log('[App] commentsContent:', commentsContent);
                console.log('[App] commentedView:', commentedView);
                if (commentsContent) {
                    commentsContent.style.display = 'block';
                    console.log('[App] Set commentsContent display to block');
                }
                if (commentedView) {
                    commentedView.style.display = 'block';
                    console.log('[App] Set commentedView display to block');
                }
                if (commentsTab) commentsTab.classList.add('active');
                console.log('[App] window.CommentedLogs exists?', !!window.CommentedLogs);
                if (window.CommentedLogs) {
                    console.log('[App] Calling CommentedLogs.show()');
                    CommentedLogs.show();
                } else {
                    console.error('[App] CommentedLogs not found! Check if commentedLogs.js loaded properly.');
                }
                break;
            case 'notebooks':
                if (notebooksContent) notebooksContent.style.display = 'block';
                if (notebooksView) notebooksView.style.display = 'block';
                if (notebooksTab) notebooksTab.classList.add('active');

                if (window.Notebooks) {
                    Notebooks.init();
                } else {
                    console.error('[App] Notebooks module not found! Check if notebooks.js loaded properly.');
                }
                break;
            case 'dashboards':
                if (dashboardsContent) dashboardsContent.style.display = 'block';
                if (dashboardsView) dashboardsView.style.display = 'block';
                if (dashboardsTab) dashboardsTab.classList.add('active');

                if (window.Dashboards) {
                    Dashboards.init();
                } else {
                    console.error('[App] Dashboards module not found! Check if dashboards.js loaded properly.');
                }
                break;
            case 'dictionaries':
                if (dictionariesContent) dictionariesContent.style.display = 'block';
                if (dictionariesView) dictionariesView.style.display = 'block';
                if (dictionariesTab) dictionariesTab.classList.add('active');

                if (window.Dictionaries) Dictionaries.show();
                break;
            case 'chat':
                if (chatContent) chatContent.style.display = 'block';
                if (chatView) chatView.style.display = 'block';
                if (chatTab) chatTab.classList.add('active');

                if (window.Chat) Chat.show();
                break;
            case 'alerts':
                if (alertsContent) alertsContent.style.display = 'block';
                if (alertsTab) alertsTab.classList.add('active');

                // Re-render alert query syntax highlighting when returning to alerts tab
                if (window.SyntaxHighlight) {
                    SyntaxHighlight.updateHighlight('editorQueryInput', 'alertQueryHighlight');
                }

                // Show the currently active sub-tab (default: manual alerts)
                {
                    const activeSubTab = document.querySelector('.alerts-sub-tab.active');
                    const subtab = activeSubTab?.dataset.subtab || 'manual';
                    if (subtab === 'feeds') {
                        if (feedAlertsView) feedAlertsView.style.display = 'block';
                        if (window.AlertFeeds) AlertFeeds.show();
                    } else {
                        if (alertsView) alertsView.style.display = 'block';
                        if (window.Alerts) Alerts.show();
                    }
                }
                break;
            case 'ingest':
                if (ingestContent) ingestContent.style.display = 'block';
                if (ingestView) ingestView.style.display = 'block';
                if (ingestTab) ingestTab.classList.add('active');
                if (window.IngestTokens) IngestTokens.show();
                break;
            case 'manage':
                if (manageContent) manageContent.style.display = 'block';
                if (manageTab) manageTab.classList.add('active');
                if (window.FractalManageTab) FractalManageTab.show();
                break;
        }
    },

    getCurrentView() {
        return this.currentView;
    },


    setupQueryResizeHandles() {
        document.querySelectorAll('.query-resize-handle').forEach(handle => {
            const targetId = handle.dataset.target;
            const textarea = document.getElementById(targetId);
            if (!textarea) return;

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
            if (!textarea) return;

            const gutter = document.createElement('div');
            gutter.className = 'query-line-numbers';
            gutter.textContent = '1';
            wrapper.appendChild(gutter);

            const update = () => {
                const lines = textarea.value.split('\n').length;
                let html = '';
                for (let i = 1; i <= lines; i++) {
                    html += i + '\n';
                }
                gutter.textContent = html.trimEnd();
                gutter.scrollTop = textarea.scrollTop;
            };

            textarea.addEventListener('input', update);
            textarea.addEventListener('scroll', () => {
                gutter.scrollTop = textarea.scrollTop;
            });

            update();
        });
    }
};

// Send to Notebook module
const SendToNotebook = {
    init() {
        const btn = document.getElementById('sendToNotebookBtn');
        if (btn) {
            btn.addEventListener('click', () => this.open());
        }
    },

    open() {
        const query = document.getElementById('queryInput')?.value?.trim();
        if (!query) {
            if (window.Toast) Toast.show('Enter a query first', 'warning');
            return;
        }

        const modalHtml = `
            <div id="sendToNotebookModal" class="modal-overlay">
                <div class="modal-content">
                    <div class="modal-header">
                        <h3>Send to Notebook</h3>
                        <button class="modal-close" id="sendToNotebookCloseBtn">&times;</button>
                    </div>
                    <div style="padding: 16px 24px 0;">
                        <input type="text" id="sendToNotebookSearch" class="dropdown-search"
                            placeholder="Search notebooks..." style="width: 100%; margin-bottom: 12px;" />
                    </div>
                    <div id="sendToNotebookList" class="stn-list" style="padding: 0 24px 24px;">
                        <div class="stn-loading">Loading notebooks...</div>
                    </div>
                </div>
            </div>
        `;

        document.body.insertAdjacentHTML('beforeend', modalHtml);

        const modal = document.getElementById('sendToNotebookModal');
        if (modal) {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) this.close();
            });
        }

        const closeBtn = document.getElementById('sendToNotebookCloseBtn');
        if (closeBtn) {
            closeBtn.addEventListener('click', () => this.close());
        }

        this._escHandler = (e) => {
            if (e.key === 'Escape') this.close();
        };
        document.addEventListener('keydown', this._escHandler);

        const searchInput = document.getElementById('sendToNotebookSearch');
        if (searchInput) {
            searchInput.focus();
            let timer;
            searchInput.addEventListener('input', (e) => {
                clearTimeout(timer);
                timer = setTimeout(() => this.loadNotebooks(e.target.value), 300);
            });
        }

        this.loadNotebooks('');
    },

    close() {
        const modal = document.getElementById('sendToNotebookModal');
        if (modal) modal.remove();
        if (this._escHandler) {
            document.removeEventListener('keydown', this._escHandler);
            this._escHandler = null;
        }
    },

    async loadNotebooks(search) {
        const list = document.getElementById('sendToNotebookList');
        if (!list) return;

        try {
            const params = new URLSearchParams({ limit: 50, offset: 0 });
            if (search) params.append('search', search);

            const response = await fetch(`/api/v1/notebooks?${params.toString()}`, {
                method: 'GET',
                credentials: 'include'
            });
            const data = await response.json();

            if (!data.success) throw new Error(data.error || 'Failed to load notebooks');

            const notebooks = data.data || [];
            if (notebooks.length === 0) {
                list.innerHTML = '<div class="stn-empty">No notebooks found</div>';
                return;
            }

            list.innerHTML = notebooks.map(nb => `
                <div class="stn-item" onclick="SendToNotebook.selectNotebook('${nb.id}', '${Utils.escapeHtml(nb.name).replace(/'/g, "\\'")}')">
                    <div class="stn-item-name">${Utils.escapeHtml(nb.name)}</div>
                    ${nb.description ? `<div class="stn-item-desc">${Utils.escapeHtml(nb.description)}</div>` : ''}
                </div>
            `).join('');
        } catch (err) {
            list.innerHTML = `<div class="stn-empty" style="color: var(--error);">Error: ${err.message}</div>`;
        }
    },

    async selectNotebook(notebookId, notebookName) {
        const query = document.getElementById('queryInput')?.value?.trim();
        if (!query) {
            this.close();
            return;
        }

        try {
            // Fetch notebook to get section count for order_index
            const nbResponse = await fetch(`/api/v1/notebooks/${notebookId}`, {
                method: 'GET',
                credentials: 'include'
            });
            const nbData = await nbResponse.json();
            if (!nbData.success) throw new Error(nbData.error || 'Failed to load notebook');

            const sections = nbData.data?.sections || [];
            const orderIndex = sections.length;

            const response = await fetch(`/api/v1/notebooks/${notebookId}/sections`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({
                    section_type: 'query',
                    title: '',
                    content: query,
                    order_index: orderIndex
                })
            });

            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to add section');

            this.close();
            if (window.Toast) Toast.success('Sent', `Query added to "${notebookName}"`);
        } catch (err) {
            if (window.Toast) Toast.error('Error', err.message);
        }
    }
};

window.SendToNotebook = SendToNotebook;

// Make globally available
window.App = App;

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => App.init());
} else {
    App.init();
}
