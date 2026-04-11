// FractalContext - Manages the current fractal/prism context for queries
const FractalContext = {
    currentFractal: null,
    currentItemType: 'fractal', // 'fractal' or 'prism'

    // Subscribers registered at init time that want to be notified on scope change.
    // See notifyFractalChange() below. Prefer subscribe() over adding to the static
    // list in notifyFractalChange() for new modules.
    _subscribers: [],

    // Monotonic counter bumped on every scope change. Modules can capture this
    // before starting an async fetch and compare on completion to discard any
    // result that belongs to a stale scope. See scopeToken() / isScopeStale().
    _scopeGeneration: 0,

    // Return a token representing the current scope. An async operation that
    // loads scope-dependent data should capture this before starting, then
    // check isScopeStale(token) before applying the result.
    scopeToken() {
        return this._scopeGeneration;
    },

    // True if the given token no longer represents the current scope, i.e.
    // the scope has changed since the token was issued and the caller should
    // discard any in-flight result.
    isScopeStale(token) {
        return token !== this._scopeGeneration;
    },

    init() {
    },

    // subscribe registers a callback that fires every time the current fractal/prism
    // scope changes. The callback should invalidate any cached data that was scoped
    // to the previous selection and re-load it if the module is currently visible.
    //
    // Modules that load scoped data MUST either:
    //   - call FractalContext.subscribe('ModuleName', () => Module.onFractalChange()) at init, or
    //   - expose a global onFractalChange method and be listed in notifyFractalChange()'s
    //     fallback module list.
    //
    // Failure to do either causes stale cross-scope data to linger in the UI after
    // the user switches fractals/prisms.
    subscribe(name, callback) {
        if (typeof callback !== 'function') return;
        // Idempotent: replace any existing subscriber with the same name. This
        // makes it safe to call subscribe() from module init functions that run
        // more than once (e.g. lazy-init on tab open).
        const existing = this._subscribers.findIndex(s => s.name === name);
        if (existing >= 0) {
            this._subscribers[existing] = { name, callback };
        } else {
            this._subscribers.push({ name, callback });
        }
    },

    isPrism() {
        return this.currentItemType === 'prism';
    },

    // Restore the current fractal/prism from localStorage (for new-tab hash routing).
    // Returns true if restored successfully, false otherwise.
    restoreFromStorage() {
        try {
            const raw = localStorage.getItem('bifract_current_context');
            if (!raw) return false;
            const saved = JSON.parse(raw);
            if (!saved || !saved.id || !saved.name) return false;
            this.currentFractal = { id: saved.id, name: saved.name };
            this.currentItemType = saved.type || 'fractal';

            if (window.FractalSelector) {
                FractalSelector.currentFractal = this.currentFractal;
                FractalSelector.updateSelectorText(saved.name);
            }
            if (window.TimeBar) {
                TimeBar.updateFractalName(saved.name);
            }

            // Sync server session
            if (this.currentItemType === 'prism') {
                this.selectPrismOnServer(saved.id);
            } else {
                this.selectFractalOnServer(saved.id);
            }
            this.notifyFractalChange();
            return true;
        } catch (e) {
            console.error('[FractalContext] Failed to restore from storage:', e);
            return false;
        }
    },

    // Persist current context to localStorage for new-tab routing
    _saveToStorage() {
        try {
            if (this.currentFractal) {
                localStorage.setItem('bifract_current_context', JSON.stringify({
                    id: this.currentFractal.id,
                    name: this.currentFractal.name,
                    type: this.currentItemType
                }));
            }
        } catch (e) {
            // localStorage may be unavailable
        }
    },

    // Set the current fractal
    setCurrentFractal(fractal) {
        this.currentFractal = fractal;
        this.currentItemType = 'fractal';
        this._saveToStorage();

        // Keep FractalSelector in sync
        if (window.FractalSelector) {
            FractalSelector.currentFractal = fractal;
            FractalSelector.updateSelectorText(fractal.name);
        }

        this.clearSearchState();

        if (window.TimeBar) {
            TimeBar.updateFractalName(fractal.name);
        }

        this.selectFractalOnServer(fractal.id);
        this.notifyFractalChange();
    },

    // Set the current prism context
    setCurrentPrism(prism) {
        this.currentFractal = prism; // stored here for compat with existing reads
        this.currentItemType = 'prism';
        this._saveToStorage();

        // Keep FractalSelector in sync
        if (window.FractalSelector) {
            FractalSelector.currentFractal = prism;
            FractalSelector.updateSelectorText(prism.name);
        }

        this.clearSearchState();

        if (window.TimeBar) {
            TimeBar.updateFractalName(prism.name);
        }

        this.selectPrismOnServer(prism.id);
        this.notifyFractalChange();
    },

    // Select fractal on server (updates session)
    async selectFractalOnServer(fractalId) {
        try {
            const response = await fetch(`/api/v1/fractals/${fractalId}/select`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include'
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.error || `HTTP ${response.status}`);
            }

        } catch (error) {
            console.error('[FractalContext] Failed to select fractal on server:', error);
            if (window.Toast) {
                Toast.error('Failed to select fractal', error.message);
            }
        }
    },

    // Select prism on server (updates session)
    async selectPrismOnServer(prismId) {
        try {
            const response = await fetch(`/api/v1/prisms/${prismId}/select`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include'
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.error || `HTTP ${response.status}`);
            }

        } catch (error) {
            console.error('[FractalContext] Failed to select prism on server:', error);
            if (window.Toast) {
                Toast.error('Failed to select prism', error.message);
            }
        }
    },

    // Get the current fractal/prism
    getCurrentFractal() {
        return this.currentFractal;
    },

    // Clear search state when switching fractals
    clearSearchState() {

        // Clear QueryExecutor state and cancel any pending requests
        if (window.QueryExecutor) {
            // Cancel any in-flight requests
            if (QueryExecutor.currentRequest) {
                QueryExecutor.currentRequest.abort();
                QueryExecutor.currentRequest = null;
            }

            QueryExecutor.currentResults = [];
            QueryExecutor.currentTimeRange = null;
            QueryExecutor.sortColumn = null;
            QueryExecutor.sortDirection = null;
            QueryExecutor.columnWidths = {};
            QueryExecutor.columnOrder = null;
            QueryExecutor.isAggregated = false;
            QueryExecutor.currentFractalId = null;
        }

        // Clear UI elements
        const elementsToReset = [
            'queryInput',
            'resultsTable',
            'error',
            'sqlOutput',
            'resultsCount',
            'executionTime',
            'pageInfo'
        ];

        // Hide export CSV button
        const exportBtn = document.getElementById('exportCsvBtn');
        if (exportBtn) {
            exportBtn.style.display = 'none';
        }

        elementsToReset.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                if (id === 'queryInput') {
                    element.value = '';
                } else if (id === 'error') {
                    element.style.display = 'none';
                } else {
                    element.innerHTML = '';
                }
            }
        });

        // Hide timeline
        if (window.Timeline) {
            Timeline.hide();
        }

        // Hide SQL preview until next query execution
        const sqlPreview = document.querySelector('.sql-preview');
        if (sqlPreview) {
            sqlPreview.style.display = 'none';
        }

        // Reset pagination if it exists
        const paginationControls = document.getElementById('paginationControls');
        if (paginationControls) {
            paginationControls.style.display = 'none';
        }

    },

    // Clear the current fractal
    clearCurrentFractal() {
        this.currentFractal = null;

        // Update time bar to show no fractal
        if (window.TimeBar) {
            TimeBar.updateFractalName(null);
        }

        // Notify modules that fractal has been cleared
        this.notifyFractalChange();
    },

    // Notify all modules that the fractal has changed.
    //
    // This is the ONLY supported way to signal a scope change to the rest of the UI.
    // Both the subscribe() registry and the static fallback list below are iterated,
    // so modules can opt in via either mechanism. Every module that loads scoped data
    // (anything per-fractal or per-prism) MUST have a handler here — otherwise the
    // UI will silently show stale cross-scope data after a switch.
    //
    // When adding a new scoped module, prefer FractalContext.subscribe() in its init.
    // The fallback list is kept for legacy modules that don't have an init phase.
    notifyFractalChange() {
        // Bump the scope generation so any in-flight async load from the old
        // scope can be discarded by modules that cooperate via scopeToken().
        this._scopeGeneration++;

        // All known scoped modules. Listing them all (even the ones that already use
        // subscribe()) is intentional: the `typeof ... === 'function'` guard makes it
        // a cheap no-op for modules that aren't loaded, and being explicit here is
        // the canonical audit point for "does this module react to scope changes?".
        const fallbackModules = [
            'Alerts',
            'AlertFeeds',
            'Chat',
            'CommentedLogs',
            'Comments',
            'Dashboards',
            'Dictionaries',
            'IngestTokens',
            'InstructionLibraries',
            'Notebooks',
            'QueryExecutor',
            'SavedQueries'
        ];

        const invoked = new Set();

        // Subscribed listeners fire first (new preferred pattern).
        for (const sub of this._subscribers) {
            try {
                sub.callback();
                invoked.add(sub.name);
            } catch (error) {
                console.error(`[FractalContext] Error notifying subscriber ${sub.name}:`, error);
            }
        }

        // Fallback: legacy modules with a global onFractalChange method. Skipped if
        // the module already received the notification via subscribe().
        for (const moduleName of fallbackModules) {
            if (invoked.has(moduleName)) continue;
            const mod = window[moduleName];
            if (mod && typeof mod.onFractalChange === 'function') {
                try {
                    mod.onFractalChange();
                } catch (error) {
                    console.error(`[FractalContext] Error notifying ${moduleName} of fractal change:`, error);
                }
            }
        }
    }
};

// Make globally available
window.FractalContext = FractalContext;
