// FractalContext - Manages the current fractal/prism context for queries
const FractalContext = {
    currentFractal: null,
    currentItemType: 'fractal', // 'fractal' or 'prism'

    init() {
        console.log('[FractalContext] Initialized');
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
            console.log(`[FractalContext] Restored ${this.currentItemType} from storage: ${saved.name} (${saved.id})`);

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
        console.log(`[FractalContext] Current fractal set to: ${fractal.name} (${fractal.id})`);

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
        console.log(`[FractalContext] Current prism set to: ${prism.name} (${prism.id})`);

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

            console.log(`[FractalContext] Server session updated to fractal: ${fractalId}`);
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

            console.log(`[FractalContext] Server session updated to prism: ${prismId}`);
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
        console.log('[FractalContext] Clearing search state for fractal switch');

        // Clear QueryExecutor state and cancel any pending requests
        if (window.QueryExecutor) {
            // Cancel any in-flight requests
            if (QueryExecutor.currentRequest) {
                console.log('[FractalContext] Cancelling pending request due to fractal switch');
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

        console.log('[FractalContext] Search state cleared');
    },

    // Clear the current fractal
    clearCurrentFractal() {
        this.currentFractal = null;
        console.log('[FractalContext] Current fractal cleared');

        // Update time bar to show no fractal
        if (window.TimeBar) {
            TimeBar.updateFractalName(null);
        }

        // Notify modules that fractal has been cleared
        this.notifyFractalChange();
    },

    // Notify all modules that the fractal has changed
    notifyFractalChange() {
        const modulesWithFractalHandlers = [
            'Alerts',
            'Chat',
            'CommentedLogs',
            'CommentGraph',
            'Comments',
            'IngestTokens',
            'QueryExecutor',
            'SavedQueries'
        ];

        modulesWithFractalHandlers.forEach(moduleName => {
            if (window[moduleName] && typeof window[moduleName].onFractalChange === 'function') {
                try {
                    console.log(`[FractalContext] Notifying ${moduleName} of fractal change`);
                    window[moduleName].onFractalChange();
                } catch (error) {
                    console.error(`[FractalContext] Error notifying ${moduleName} of fractal change:`, error);
                }
            }
        });
    }
};

// Make globally available
window.FractalContext = FractalContext;
