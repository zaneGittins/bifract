const FractalSelector = {
    currentFractal: null,
    availableFractals: [],
    availablePrisms: [],
    isLoading: false,

    init() {
        this.createSelectorUI();
        this.loadAvailableFractals();
        this.setupEventListeners();
    },

    createSelectorUI() {
        if (document.getElementById('fractalSelectorContainer')) return;
        const container = document.getElementById('sidebarScope');
        if (!container) return;

        container.innerHTML = `
            <div class="sb-scope-wrapper" id="fractalSelectorContainer">
                <button type="button" class="sb-scope-btn" id="fractalSelectorButton" aria-haspopup="true" aria-expanded="false" aria-controls="fractalSelectorMenu">
                    <svg class="sb-scope-icon" width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M8 1.5l5.5 3.25v6.5L8 14.5l-5.5-3.25v-6.5z"/></svg>
                    <span class="sb-scope-text">
                        <span class="sb-scope-kind" id="fractalSelectorKind">Fractal</span>
                        <span class="sb-scope-name" id="fractalSelectorText">Loading...</span>
                    </span>
                    <svg class="sb-scope-chevron" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 9l6 6 6-6"/></svg>
                </button>
            </div>
        `;
        // On the body, not in the sidebar: the phone drawer hides and transforms its
        // contents, which would hide the menu when it opens from a query bar chip.
        const menu = document.createElement('div');
        menu.className = 'sb-scope-menu';
        menu.id = 'fractalSelectorMenu';
        menu.innerHTML = '<div class="fractal-selector-loading">Loading fractals...</div>';
        document.body.appendChild(menu);
    },

    setupEventListeners() {
        const button = document.getElementById('fractalSelectorButton');
        if (!button) return;

        button.addEventListener('click', (e) => {
            e.stopPropagation();
            this.toggleDropdown(button);
        });
        document.querySelectorAll('[data-scope-chip]').forEach(chip => {
            chip.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggleDropdown(chip);
            });
        });
        window.addEventListener('resize', () => this.closeDropdown());

        // Delegated so scope names never reach an inline handler.
        document.getElementById('fractalSelectorMenu').addEventListener('click', (e) => {
            const item = e.target.closest('[data-scope-id]');
            if (item) {
                if (item.dataset.scopeType === 'prism') this.selectPrism(item.dataset.scopeId);
                else this.selectFractal(item.dataset.scopeId);
            } else if (e.target.closest('.fractal-selector-all')) {
                this.showAll();
            }
        });

        document.addEventListener('click', (e) => {
            if (!e.target.closest('.sb-scope-wrapper, [data-scope-chip], #fractalSelectorMenu')) this.closeDropdown();
        });

        document.addEventListener('keydown', (e) => {
            if (e.key !== 'Escape' || !this._anchor) return;
            e.preventDefault();
            const anchor = this._anchor;
            this.closeDropdown();
            anchor.focus();
        });
    },

    async loadAvailableFractals() {
        try {
            this.isLoading = true;
            if (!this.currentFractal) this.updateSelectorText('Loading...');

            const response = await fetch('/api/v1/fractals', {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include'
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to load fractals');
            }

            this.availableFractals = data.data.fractals || [];
            this.availablePrisms = data.data.prisms || [];
            this.renderFractalMenu();
            // Seeding needs the session's selected scope, which arrives with the user.
            if (window.Auth && Auth.ready) await Auth.ready;
            await this.selectCurrentFractal();
            this.updateSelectorText(this.currentFractal ? this.currentFractal.name : 'Select a fractal');

            // Process share link params now that fractals/prisms are loaded.
            await this.processShareLinkIfPresent();

        } catch (error) {
            console.error('Failed to load fractals:', error);
            this.updateSelectorText('Error');
            this.showErrorInMenu(error.message);
            this._showListingIfUnrouted();
        } finally {
            this.isLoading = false;
        }
    },

    // Check URL for share link params and auto-select the target fractal/prism.
    // Called after loadAvailableFractals so data is guaranteed ready.
    async processShareLinkIfPresent() {
        if (!window.location.search) return;
        const params = new URLSearchParams(window.location.search);
        if (!params.has('q') || !params.has('tr')) return;

        const fractalId = params.get('f');
        const prismId = params.get('p');
        if (!fractalId && !prismId) return;

        let target = null;
        let isPrism = false;

        if (prismId) {
            target = this.availablePrisms.find(p => p.id === prismId);
            isPrism = true;
        } else {
            target = this.availableFractals.find(f => f.id === fractalId);
        }

        if (!target) {
            console.warn('[FractalSelector] Share link target not found or no access');
            this._showListingIfUnrouted();
            return;
        }

        // Cancel any deferred/polling share link processing from earlier
        // attempts that ran before data was available. We handle it here.
        if (window.QueryExecutor) {
            window.QueryExecutor.hasLoadedShareLink = true;
            window.QueryExecutor.deferredShareLink = null;
            if (window.QueryExecutor.deferredPollingInterval) {
                clearInterval(window.QueryExecutor.deferredPollingInterval);
                window.QueryExecutor.deferredPollingInterval = null;
            }
        }

        // Navigate first so the search DOM is live before any query runs.
        if (window.App?.showFractalView) {
            window.App.showFractalView('search');
        }

        // Switching scope fires onFractalChange, which picks up the share params
        // itself. Awaiting matters: the scope must be committed on the server
        // before the query runs, or a prism share link (which sends no
        // fractal_id) executes against whatever scope the session was last on.
        const alreadyOnTarget = window.FractalContext?.currentFractal?.id === target.id;
        if (!alreadyOnTarget && window.FractalContext) {
            if (isPrism) {
                await window.FractalContext.setCurrentPrism(target);
            } else {
                await window.FractalContext.setCurrentFractal(target);
            }
            return;
        }

        if (window.QueryExecutor?.loadFromShareLink) {
            window.QueryExecutor.loadFromShareLink();
        }
    },

    renderFractalMenu() {
        const menu = document.getElementById('fractalSelectorMenu');
        if (!menu) return;

        const history = this._getUsageHistory();

        const items = [
            ...this.availableFractals.map(f => ({ ...f, itemType: 'fractal' })),
            ...this.availablePrisms.map(p => ({ ...p, itemType: 'prism' })),
        ];

        // Sort by last-used descending; ties broken alphabetically
        items.sort((a, b) => {
            const ta = history[a.id] || 0;
            const tb = history[b.id] || 0;
            if (tb !== ta) return tb - ta;
            return a.name.localeCompare(b.name);
        });

        const allLink = '<button type="button" class="fractal-selector-all">All fractals and prisms</button>';
        if (items.length === 0) {
            menu.innerHTML = '<div class="fractal-selector-loading">No fractals available</div>' + allLink;
            return;
        }

        menu.innerHTML = items.map(item => {
            const isCurrent = this.currentFractal && this.currentFractal.id === item.id;
            const isPrism = item.itemType === 'prism';
            const roleLabel = !isPrism && item.user_role ? item.user_role : '';
            const isDefault = !isPrism && item.is_default;
            return `
                <button type="button" class="fractal-selector-item ${isCurrent ? 'current' : ''}" data-scope-id="${Utils.escapeAttr(item.id)}" data-scope-type="${item.itemType}">
                    <span class="fractal-selector-item-name">
                        ${Utils.escapeHtml(item.name)}${isDefault ? ' (default)' : ''}
                        ${isPrism ? `<span class="prism-badge" style="font-size:9px;padding:1px 4px;margin-left:4px;">PRISM</span>` : ''}
                        ${roleLabel ? `<span style="font-size:9px;opacity:0.6;margin-left:4px;text-transform:uppercase;">${Utils.escapeHtml(roleLabel)}</span>` : ''}
                    </span>
                    ${item.description ? `<span class="fractal-selector-item-description">${Utils.escapeHtml(item.description)}</span>` : ''}
                </button>
            `;
        }).join('') + allLink;
    },

    showAll() {
        this.closeDropdown();
        if (window.App) App.showMainView('fractalListing');
    },

    // Opens the new scope from outside the fractal level; inside it, the URL moves
    // to the new scope so a reload or Back lands where the user is.
    _enterScope() {
        if (!window.App) return;
        if (App.currentViewLevel !== 'fractal') App.showFractalView('search');
        else App.pushSubPath('');
    },

    _getUsageHistory() {
        try {
            const raw = localStorage.getItem('bifract_selector_usage');
            return raw ? JSON.parse(raw) : {};
        } catch (e) {
            return {};
        }
    },

    _recordUsage(id) {
        try {
            const history = this._getUsageHistory();
            history[id] = Date.now();
            localStorage.setItem('bifract_selector_usage', JSON.stringify(history));
        } catch (e) {
            // localStorage may be unavailable
        }
    },

    async selectCurrentFractal() {
        if (this.currentFractal || this._routerOwnsScope()) return;

        // Source of truth for which scope we should render is the server
        // session, exposed via Auth.currentUser.{selected_fractal,selected_prism}.
        // Without this, the client silently falls back to "default" while the
        // server session may still be on a prism, producing a split-brain where
        // listings in the default fractal leak prism-scoped data.
        const sessionPrismID   = window.Auth?.currentUser?.selected_prism || '';
        const sessionFractalID = window.Auth?.currentUser?.selected_fractal || '';

        if (sessionPrismID) {
            const prism = this.availablePrisms.find(p => p.id === sessionPrismID);
            if (prism) {
                this._applyInitialSelection(prism, 'prism');
                return;
            }
        }

        if (sessionFractalID) {
            const fractal = this.availableFractals.find(f => f.id === sessionFractalID);
            if (fractal) {
                this._applyInitialSelection(fractal, 'fractal');
                return;
            }
        }

        // No scope in session -> pick a default and persist it so the server
        // session is no longer stale from a prior visit.
        if (this.availableFractals.length > 0) {
            const defaultFractal = this.availableFractals.find(fractal => fractal.is_default);
            const targetFractal = defaultFractal || this.availableFractals[0];
            if (targetFractal && window.FractalContext) {
                // Awaited: the hash router may be selecting a different scope
                // concurrently, and an unawaited write here can land after it and
                // leave the session on the default while the UI shows the other.
                const role = await FractalContext.selectFractalOnServer(targetFractal.id);
                if (role === null) return;
                // Re-check: the router may have won the race while we waited.
                if (this.currentFractal || this._routerOwnsScope()) return;
                this._applyInitialSelection(targetFractal, 'fractal');
            }
        }
    },

    // The initial route skips the listing while a share link is pending; when the
    // link cannot be landed the page still needs somewhere to be.
    _showListingIfUnrouted() {
        if (window.App && !App.currentViewLevel) App.showMainView('fractalListing');
    },

    // The router sets the scope for scope URLs, and the listing is the no-scope
    // level; seeding the session's scope in either case races or undoes it.
    _routerOwnsScope() {
        if (/^#[fp]\//.test(window.location.hash)) return true;
        return !!(window.App && App.currentViewLevel === 'main' && App.currentView === 'fractalListing');
    },

    // Apply an initial fractal/prism selection to every view that needs to know
    // about it: dropdown text, FractalContext, TimeBar (bottom-left label), and
    // localStorage. No server call - that's the caller's decision.
    _applyInitialSelection(target, type) {
        this.currentFractal = target;
        if (window.FractalContext) {
            FractalContext.currentFractal = target;
            FractalContext.currentItemType = type;
            FractalContext._saveToStorage();
        }
        this.updateSelectorText(target.name);
        if (window.TimeBar) {
            TimeBar.updateFractalName(target.name);
        }
        // No scope notification here, so refresh what keys off the scope directly.
        if (window.Auth) Auth.updateRBACVisibility();
        if (window.Recall) Recall.refreshAvailability();
    },

    async selectFractal(fractalId) {
        if (this.isLoading) {
            return;
        }

        try {
            this.isLoading = true;
            this.closeDropdown();

            // Find the full fractal object
            const selectedFractal = this.availableFractals.find(fractal => fractal.id === fractalId);
            if (!selectedFractal) {
                throw new Error('Selected fractal not found');
            }

            // Delegate the whole switch (server select, authoritative role,
            // client state, notifications). Keeping a second copy of this here is
            // what let the two paths drift, notably on the role refresh.
            if (!(await FractalContext.setCurrentFractal(selectedFractal))) return;

            this._recordUsage(fractalId);
            this._enterScope();

        } catch (error) {
            console.error('Failed to select fractal:', error);
            if (window.Toast) {
                Toast.show(`Failed to switch fractal: ${error.message}`, 'error');
            }
        } finally {
            this.isLoading = false;
        }
    },

    async selectPrism(prismId) {
        if (this.isLoading) return;
        try {
            this.isLoading = true;
            this.closeDropdown();

            const prism = this.availablePrisms.find(p => p.id === prismId);
            if (!prism) throw new Error('Prism not found');

            // Same delegation as selectFractal: one implementation of a scope switch.
            if (!(await FractalContext.setCurrentPrism(prism))) return;

            this._recordUsage(prismId);
            this._enterScope();

        } catch (error) {
            console.error('Failed to select prism:', error);
            if (window.Toast) Toast.show(`Failed to switch prism: ${error.message}`, 'error');
        } finally {
            this.isLoading = false;
        }
    },

    // The menu opens from the sidebar switcher or from a scope chip on a query bar.
    _anchor: null,

    toggleDropdown(anchor = document.getElementById('fractalSelectorButton')) {
        if (this._anchor === anchor) this.closeDropdown();
        else this.openDropdown(anchor);
    },

    openDropdown(anchor = document.getElementById('fractalSelectorButton')) {
        const menu = document.getElementById('fractalSelectorMenu');
        if (!menu || !anchor) return;
        if (this._anchor) this._setExpanded(this._anchor, false);
        this.renderFractalMenu();
        // Fixed positioning keeps the menu free of the sidebar's scroll clipping.
        const r = anchor.getBoundingClientRect();
        const beside = anchor.id === 'fractalSelectorButton' && document.documentElement.classList.contains('sb-collapsed');
        const top = beside ? r.top : r.bottom + 6;
        menu.style.top = `${Math.round(top)}px`;
        menu.style.left = `${Math.round(beside ? r.right + 8 : r.left)}px`;
        menu.style.minWidth = `${Math.round(Math.max(r.width, 260))}px`;
        menu.style.maxHeight = `${Math.round(Math.max(160, Math.min(420, window.innerHeight - top - 16)))}px`;
        menu.classList.add('show');
        if (window.Sidebar) Sidebar.hideTip();
        this._anchor = anchor;
        this._setExpanded(anchor, true);
    },

    closeDropdown() {
        const menu = document.getElementById('fractalSelectorMenu');
        if (menu) menu.classList.remove('show');
        if (this._anchor) this._setExpanded(this._anchor, false);
        this._anchor = null;
    },

    _setExpanded(anchor, open) {
        anchor.classList.toggle('open', open);
        anchor.setAttribute('aria-expanded', String(open));
    },

    updateSelectorText(text) {
        const textElement = document.getElementById('fractalSelectorText');
        if (textElement) textElement.textContent = text;
        const kind = document.getElementById('fractalSelectorKind');
        if (kind) kind.textContent = window.FractalContext && FractalContext.isPrism() ? 'Prism' : 'Fractal';
        const button = document.getElementById('fractalSelectorButton');
        if (button) {
            button.dataset.tip = text;
            button.classList.toggle('empty', !(window.FractalContext && FractalContext.hasScope()));
        }
        document.querySelectorAll('[data-scope-chip] .scope-chip-name').forEach(el => { el.textContent = text; });
    },

    showErrorInMenu(errorMessage) {
        const menu = document.getElementById('fractalSelectorMenu');
        if (menu) {
            menu.innerHTML = `
                <div class="fractal-selector-loading" style="color: var(--error);">
                    Error: ${Utils.escapeHtml(errorMessage)}
                    <button onclick="FractalSelector.loadAvailableFractals()"
                            style="display: block; margin-top: 8px; color: var(--accent-primary); background: none; border: none; cursor: pointer;">
                        Retry
                    </button>
                </div>
            `;
        }
    }
};

// Make globally available
window.FractalSelector = FractalSelector;