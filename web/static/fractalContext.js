// Per-request scope propagation.
//
// The server session holds one fractal/prism per session cookie, which every
// browser tab shares. Without a per-request scope, switching context in one tab
// silently repoints every other tab: their lists, their writes, and (in prism
// context, where no fractal_id is sent) their queries all follow the session.
//
// So every same-origin /api/v1 request carries this tab's scope in X-Bifract-Scope.
// The server authorizes the header against RBAC and only falls back to the
// session scope when the header is absent (fresh load, or a recovery endpoint).
// See ScopeHeader in pkg/auth/auth.go.
(function installScopeHeader() {
    const SCOPE_HEADER = 'X-Bifract-Scope';
    const SCOPE_INVALID_HEADER = 'X-Bifract-Scope-Invalid';
    const nativeFetch = window.fetch.bind(window);
    let rejectionHandled = false;

    function currentScope() {
        const ctx = window.FractalContext;
        if (!ctx || !ctx.currentFractal || !ctx.currentFractal.id) return null;
        return (ctx.isPrism() ? 'prism:' : 'fractal:') + ctx.currentFractal.id;
    }

    function isApiCall(input) {
        try {
            const raw = (input && typeof input === 'object' && input.url) ? input.url : String(input);
            const url = new URL(raw, window.location.origin);
            return url.origin === window.location.origin && url.pathname.startsWith('/api/v1/');
        } catch (e) {
            return false;
        }
    }

    // The scope this tab holds is gone (deleted, or access revoked). Drop it and
    // return to the listing rather than letting every request fail. Debounced so
    // a burst of parallel requests triggers one recovery.
    function onScopeRejected() {
        if (rejectionHandled) return;
        rejectionHandled = true;
        setTimeout(() => { rejectionHandled = false; }, 5000);

        try {
            localStorage.removeItem('bifract_current_context');
        } catch (e) {
            // localStorage may be unavailable
        }
        if (window.FractalContext) {
            FractalContext.currentFractal = null;
            FractalContext.currentItemType = 'fractal';
        }
        if (window.FractalSelector) {
            FractalSelector.currentFractal = null;
            FractalSelector.updateSelectorText('Select fractal');
        }
        if (window.Toast) {
            Toast.error('Context unavailable', 'You no longer have access to this fractal or prism.');
        }
        if (window.App && typeof App.showMainView === 'function') {
            App.showMainView('fractalListing');
        }
    }

    function checkResponse(res) {
        if (res.status === 403 && res.headers.get(SCOPE_INVALID_HEADER)) {
            onScopeRejected();
        }
        return res;
    }

    window.fetch = function (input, init) {
        const scope = currentScope();
        if (!scope || !isApiCall(input)) return nativeFetch(input, init);

        if (typeof Request !== 'undefined' && input instanceof Request) {
            const req = new Request(input, init);
            req.headers.set(SCOPE_HEADER, scope);
            return nativeFetch(req).then(checkResponse);
        }

        const opts = Object.assign({}, init || {});
        const headers = new Headers(opts.headers || {});
        headers.set(SCOPE_HEADER, scope);
        opts.headers = headers;
        return nativeFetch(input, opts).then(checkResponse);
    };
})();

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
    // Returns true if restored successfully, false otherwise. Same contract as
    // setCurrentFractal: the server /select must succeed before any client state
    // moves, so a stale or revoked saved context leaves nothing behind.
    async restoreFromStorage() {
        try {
            const raw = localStorage.getItem('bifract_current_context');
            if (!raw) return false;
            const saved = JSON.parse(raw);
            if (!saved || !saved.id || !saved.name) return false;

            const type = saved.type || 'fractal';
            const role = type === 'prism'
                ? await this.selectPrismOnServer(saved.id)
                : await this.selectFractalOnServer(saved.id);
            if (role === null) return false;

            this._applyScope({ id: saved.id, name: saved.name }, type, role);
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

    // Move every piece of client-side scope state to `target` in one step.
    // Callers must have already committed the change on the server, so that a
    // failed /select can never leave the label, the selector, localStorage and
    // the session disagreeing about which scope the user is looking at.
    _applyScope(target, type, role) {
        this.currentFractal = target;
        this.currentItemType = type;
        this._saveToStorage();

        if (window.FractalSelector) {
            FractalSelector.currentFractal = target;
            FractalSelector.updateSelectorText(target.name);
        }
        // Role-gated UI reads the role for the CURRENT scope. Carrying the previous
        // scope's role over shows admin controls in a fractal or prism where the
        // user is only a viewer, so always take the role the server just resolved.
        if (window.Auth && Auth.currentUser) {
            if (type === 'prism') {
                Auth.currentUser.selected_prism = target.id;
                Auth.currentUser.selected_fractal = '';
                Auth.currentUser.prism_role = role || '';
                Auth.currentUser.fractal_role = '';
            } else {
                Auth.currentUser.selected_fractal = target.id;
                Auth.currentUser.selected_prism = '';
                Auth.currentUser.fractal_role = role || '';
                Auth.currentUser.prism_role = '';
            }
            if (typeof Auth.updateRBACVisibility === 'function') {
                Auth.updateRBACVisibility();
            }
        }
        this.clearSearchState();
        if (window.TimeBar) {
            TimeBar.updateFractalName(target.name);
        }
    },

    // Set the current fractal. The server /select is awaited BEFORE any client
    // state moves: on failure nothing has changed, so the UI never advertises a
    // scope the session is not actually on.
    async setCurrentFractal(fractal) {
        const role = await this.selectFractalOnServer(fractal.id);
        if (role === null) return false;
        this._applyScope(fractal, 'fractal', role);
        this.notifyFractalChange();
        return true;
    },

    // Set the current prism context. Same contract as setCurrentFractal.
    async setCurrentPrism(prism) {
        const role = await this.selectPrismOnServer(prism.id);
        if (role === null) return false;
        this._applyScope(prism, 'prism', role);
        this.notifyFractalChange();
        return true;
    },

    // Update the server session to the given fractal. Returns the role the server
    // resolved for this user on that fractal, or null on failure. Callers MUST
    // await this before moving any client state, otherwise the UI can advertise
    // a scope the session is not on.
    async selectFractalOnServer(fractalId) {
        return this._selectOnServer(`/api/v1/fractals/${fractalId}/select`, 'fractal');
    },

    async selectPrismOnServer(prismId) {
        return this._selectOnServer(`/api/v1/prisms/${prismId}/select`, 'prism');
    },

    async _selectOnServer(url, label) {
        try {
            const response = await fetch(url, {
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
            const body = await response.json().catch(() => ({}));
            // Empty string is a valid role ("no access"), so normalise to a string
            // rather than letting a missing field read as failure.
            return (body && body.data && typeof body.data.role === 'string') ? body.data.role : '';
        } catch (error) {
            console.error(`[FractalContext] Failed to select ${label} on server:`, error);
            if (window.Toast) {
                Toast.error(`Failed to select ${label}`, error.message);
            }
            return null;
        }
    },

    // Get the current fractal/prism
    getCurrentFractal() {
        return this.currentFractal;
    },

    // Clear search state when switching fractals
    clearSearchState() {
        if (window.LogDetail) LogDetail.close();

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
                    element.dispatchEvent(new Event('input'));
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

    // Leave the fractal/prism level entirely (the user navigated back to the
    // listing). Every piece of in-memory scope state has to go, including
    // currentItemType: leaving it as 'prism' makes isPrism() keep reporting true
    // with no scope selected, which mis-renders tab visibility and role checks.
    //
    // localStorage and the server session are deliberately kept. They record the
    // last scope used so a new tab or a legacy hash can resume it, and the
    // request scope header is what actually scopes traffic now.
    clearCurrentFractal() {
        this.currentFractal = null;
        this.currentItemType = 'fractal';

        if (window.FractalSelector) {
            FractalSelector.currentFractal = null;
        }
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
            'AnalyticsModels',
            'Chat',
            'CommentedLogs',
            'Comments',
            'Dashboards',
            'Dictionaries',
            'FractalManageTab',
            'IngestTokens',
            'InstructionLibraries',
            'Notebooks',
            'QueryExecutor',
            'QueryPalette',
            'RealTimeComments'
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

        // Fractal-only tabs (models, ingest, recall) have to be re-evaluated on
        // every scope change, and the user redirected off one that just became
        // invalid. Done here rather than at each call site so no switch path can
        // forget it.
        if (window.App && typeof App.updateScopedTabVisibility === 'function') {
            try {
                App.updateScopedTabVisibility();
            } catch (error) {
                console.error('[FractalContext] Error updating scoped tab visibility:', error);
            }
        }
    }
};

// Make globally available
window.FractalContext = FractalContext;
