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

        const container = document.getElementById('contextPillContainer');
        if (!container) {
            console.warn('[FractalSelector] contextPillContainer not found');
            return;
        }

        container.innerHTML = `
            <span class="context-pill-sep"></span>
            <div class="context-pill-wrapper" id="fractalSelectorContainer">
                <button class="context-pill-btn" id="fractalSelectorButton">
                    <span class="context-pill-icon" id="contextTypeIcon"></span>
                    <span class="context-pill-name" id="fractalSelectorText">Loading...</span>
                    <svg class="context-pill-chevron" width="10" height="6" viewBox="0 0 10 6" fill="none">
                        <path d="M1 1L5 5L9 1" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
                    </svg>
                </button>
                <div class="context-pill-menu" id="fractalSelectorMenu">
                    <div class="fractal-selector-loading">Loading fractals...</div>
                </div>
            </div>
        `;

        this.addStyles();
    },

    addStyles() {
        if (document.getElementById('fractalSelectorStyles')) return;

        const styles = `
            <style id="fractalSelectorStyles">
            /* ---- Context pill: type icon + name inline in header ---- */
            .context-pill-sep {
                display: inline-block;
                width: 1px;
                height: 18px;
                background: var(--border-color);
                margin: 0 6px 0 2px;
                opacity: 0.5;
                flex-shrink: 0;
            }

            .context-pill-wrapper {
                position: relative;
                display: flex;
                align-items: center;
                flex-shrink: 0;
            }

            .context-pill-btn {
                background: transparent;
                border: none;
                border-radius: 5px;
                color: var(--text-primary);
                padding: 4px 6px;
                cursor: pointer;
                display: flex;
                align-items: center;
                gap: 5px;
                font-size: 13px;
                font-weight: 500;
                font-family: var(--font-main);
                transition: background 0.15s ease;
                white-space: nowrap;
                line-height: 1;
                max-width: 220px;
            }

            .context-pill-btn:hover {
                background: var(--overlay-subtle);
            }

            .context-pill-btn.open {
                background: var(--overlay-light);
            }

            .context-pill-icon {
                display: flex;
                align-items: center;
                flex-shrink: 0;
                transition: color 0.15s ease;
            }

            .context-pill-btn[data-type="fractal"] .context-pill-icon {
                color: var(--accent-secondary);
            }

            .context-pill-btn[data-type="prism"] .context-pill-icon {
                color: var(--accent-tertiary);
            }

            .context-pill-name {
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
                max-width: 180px;
            }

            .context-pill-chevron {
                flex-shrink: 0;
                opacity: 0.35;
                color: var(--text-muted);
                transition: transform 0.15s ease, opacity 0.15s ease;
            }

            .context-pill-btn:hover .context-pill-chevron,
            .context-pill-btn.open .context-pill-chevron {
                opacity: 0.65;
            }

            .context-pill-btn.open .context-pill-chevron {
                transform: rotate(180deg);
            }

            .context-pill-menu {
                position: absolute;
                top: calc(100% + 5px);
                left: 0;
                min-width: 220px;
                background: var(--bg-secondary);
                border: 1px solid var(--border-color);
                border-radius: 6px;
                box-shadow: 0 4px 16px rgba(0,0,0,0.2);
                z-index: 1000;
                display: none;
                max-height: 320px;
                overflow-y: auto;
            }

            .context-pill-menu.show {
                display: block;
            }

            /* ---- Dropdown menu items ---- */
            .fractal-selector-loading {
                padding: 12px;
                text-align: center;
                color: var(--text-secondary);
                font-size: 13px;
            }

            .fractal-selector-item {
                display: block;
                width: 100%;
                padding: 9px 12px;
                border: none;
                background: none;
                color: var(--text-primary);
                text-align: left;
                cursor: pointer;
                font-size: 13px;
                font-family: var(--font-main);
                line-height: 1.4;
                transition: background-color 0.15s ease;
                border-bottom: 1px solid var(--border-color);
            }

            .fractal-selector-item:last-child {
                border-bottom: none;
            }

            .fractal-selector-item:hover {
                background: var(--bg-hover);
            }

            .fractal-selector-item.current {
                background: var(--accent-color);
                color: #fff;
                font-weight: 500;
            }

            .fractal-selector-item.current:hover {
                background: var(--accent-hover, var(--accent-color));
                color: #fff;
            }

            .fractal-selector-item.current .fractal-selector-item-description {
                color: rgba(255,255,255,0.75);
            }

            .fractal-selector-item.current::after {
                content: '\\2713';
                float: right;
                color: #fff;
            }

            .fractal-selector-item-name {
                display: block;
                font-weight: 500;
            }

            .fractal-selector-item-description {
                display: block;
                font-size: 11px;
                color: var(--text-secondary);
                margin-top: 2px;
            }

            .fractal-selector-divider {
                height: 1px;
                background: var(--border-color);
                margin: 4px 0;
            }

            @media (max-width: 768px) {
                .context-pill-name {
                    max-width: 90px;
                }
            }
            </style>
        `;

        document.head.insertAdjacentHTML('beforeend', styles);
    },

    setupEventListeners() {
        const button = document.getElementById('fractalSelectorButton');
        const menu = document.getElementById('fractalSelectorMenu');

        if (!button || !menu) {
            console.error('Index selector elements not found');
            return;
        }

        // Toggle dropdown on button click
        button.addEventListener('click', (e) => {
            e.stopPropagation();
            this.toggleDropdown();
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.context-pill-wrapper')) {
                this.closeDropdown();
            }
        });

        // Close dropdown on escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this.closeDropdown();
            }
        });
    },

    async loadAvailableFractals() {
        try {
            this.isLoading = true;
            this.updateSelectorText('Loading...');

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
            this.selectCurrentFractal();

            // Process share link params now that fractals/prisms are loaded.
            this.processShareLinkIfPresent();

        } catch (error) {
            console.error('Failed to load fractals:', error);
            this.updateSelectorText('Error');
            this.showErrorInMenu(error.message);
        } finally {
            this.isLoading = false;
        }
    },

    // Check URL for share link params and auto-select the target fractal/prism.
    // Called after loadAvailableFractals so data is guaranteed ready.
    processShareLinkIfPresent() {
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

        // Select the context (fires onFractalChange).
        if (isPrism && window.FractalContext?.setCurrentPrism) {
            window.FractalContext.setCurrentPrism(target);
        } else if (window.FractalContext?.setCurrentFractal) {
            window.FractalContext.setCurrentFractal(target);
        }

        // Navigate to the search view, then call loadFromShareLink.
        // Data is guaranteed available since we just loaded it.
        if (window.App?.showFractalView) {
            window.App.showFractalView('search');
        }
        if (window.QueryExecutor?.loadFromShareLink) {
            window.QueryExecutor.loadFromShareLink();
        }
    },

    renderFractalMenu() {
        const menu = document.getElementById('fractalSelectorMenu');
        if (!menu) return;

        let menuHTML = '';

        this.availableFractals.forEach(fractal => {
            const isDefault = fractal.is_default;
            const isCurrent = this.currentFractal && this.currentFractal.id === fractal.id;
            const roleLabel = fractal.user_role ? fractal.user_role : '';
            menuHTML += `
                <button class="fractal-selector-item ${isCurrent ? 'current' : ''}"
                        onclick="FractalSelector.selectFractal('${Utils.escapeJs(fractal.id)}', '${Utils.escapeJs(fractal.name)}')">
                    <span class="fractal-selector-item-name">
                        ${Utils.escapeHtml(fractal.name)}${isDefault ? ' (default)' : ''}
                        ${roleLabel ? `<span style="font-size:9px;opacity:0.6;margin-left:4px;text-transform:uppercase;">${Utils.escapeHtml(roleLabel)}</span>` : ''}
                    </span>
                    ${fractal.description ? `<span class="fractal-selector-item-description">${Utils.escapeHtml(fractal.description)}</span>` : ''}
                </button>
            `;
        });

        if (this.availablePrisms.length > 0) {
            if (this.availableFractals.length > 0) {
                menuHTML += `<div class="fractal-selector-divider"></div>`;
            }
            this.availablePrisms.forEach(prism => {
                const isCurrent = this.currentFractal && this.currentFractal.id === prism.id;
                menuHTML += `
                    <button class="fractal-selector-item ${isCurrent ? 'current' : ''}"
                            onclick="FractalSelector.selectPrism('${Utils.escapeJs(prism.id)}', '${Utils.escapeJs(prism.name)}')">
                        <span class="fractal-selector-item-name">
                            ${Utils.escapeHtml(prism.name)}
                            <span class="prism-badge" style="font-size:9px;padding:1px 4px;margin-left:4px;">PRISM</span>
                        </span>
                        ${prism.description ? `<span class="fractal-selector-item-description">${Utils.escapeHtml(prism.description)}</span>` : ''}
                    </button>
                `;
            });
        }

        menu.innerHTML = menuHTML || '<div class="fractal-selector-loading">No fractals available</div>';
    },

    selectCurrentFractal() {
        if (this.currentFractal) return;

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
            if (targetFractal) {
                this._applyInitialSelection(targetFractal, 'fractal');
                if (window.FractalContext) {
                    FractalContext.selectFractalOnServer(targetFractal.id);
                }
            }
        }
    },

    // Apply an initial fractal/prism selection to every view that needs to know
    // about it: dropdown text, FractalContext, TimeBar (bottom-left label), and
    // localStorage. No server call - that's the caller's decision.
    _applyInitialSelection(target, type) {
        this.currentFractal = target;
        this.updateSelectorText(target.name);
        if (window.FractalContext) {
            FractalContext.currentFractal = target;
            FractalContext.currentItemType = type;
            FractalContext._saveToStorage();
        }
        if (window.TimeBar) {
            TimeBar.updateFractalName(target.name);
        }
    },

    async selectFractal(fractalId, fractalName) {
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

            // Update session on server
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

            this.currentFractal = selectedFractal;
            this.updateSelectorText(selectedFractal.name);

            // Update auth fractal_role from the stored user_role
            if (window.Auth && Auth.currentUser) {
                Auth.currentUser.fractal_role = selectedFractal.user_role || '';
                Auth.currentUser.selected_fractal = selectedFractal.id;
                Auth.currentUser.selected_prism = '';
                Auth.updateRBACVisibility();
            }

            // Keep FractalContext, the bottom-left time bar, and localStorage
            // in sync. This mirrors the work FractalContext.setCurrentFractal()
            // does, minus the redundant server call we already made above.
            if (window.FractalContext) {
                FractalContext.currentFractal = selectedFractal;
                FractalContext.currentItemType = 'fractal';
                FractalContext._saveToStorage();
            }
            if (window.TimeBar) {
                TimeBar.updateFractalName(selectedFractal.name);
            }

            this.refreshCurrentView();


        } catch (error) {
            console.error('Failed to select fractal:', error);
            if (window.Toast) {
                Toast.show(`Failed to switch fractal: ${error.message}`, 'error');
            }
        } finally {
            this.isLoading = false;
        }
    },

    async selectPrism(prismId, prismName) {
        if (this.isLoading) return;
        try {
            this.isLoading = true;
            this.closeDropdown();

            const prism = this.availablePrisms.find(p => p.id === prismId);
            if (!prism) throw new Error('Prism not found');

            const response = await fetch(`/api/v1/prisms/${prismId}/select`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
                credentials: 'include'
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.error || `HTTP ${response.status}`);
            }

            this.currentFractal = prism;
            this.updateSelectorText(prism.name);

            if (window.Auth && Auth.currentUser) {
                Auth.currentUser.selected_prism = prism.id;
                Auth.currentUser.selected_fractal = '';
                // Refresh role-gated UI for the new prism scope. The fractal
                // path does this too; keep them symmetrical.
                Auth.updateRBACVisibility();
            }

            if (window.FractalContext) {
                FractalContext.currentFractal = prism;
                FractalContext.currentItemType = 'prism';
                FractalContext._saveToStorage();
            }
            if (window.TimeBar) {
                TimeBar.updateFractalName(prism.name);
            }

            this.refreshCurrentView();

        } catch (error) {
            console.error('Failed to select prism:', error);
            if (window.Toast) Toast.show(`Failed to switch prism: ${error.message}`, 'error');
        } finally {
            this.isLoading = false;
        }
    },

    // Set current fractal programmatically (used by FractalListing)
    setCurrentFractal(fractalObject) {
        this.currentFractal = fractalObject;
        this.updateSelectorText(fractalObject.name);
    },

    refreshCurrentView() {
        // Notify every module that cares about scope changes. This is the single
        // source of truth for "tell the UI the scope changed" — do NOT special-case
        // individual views here, or modules added later will silently stop reloading
        // on scope switches. See FractalContext.notifyFractalChange() for the list.
        try {
            if (window.FractalContext && typeof FractalContext.notifyFractalChange === 'function') {
                FractalContext.notifyFractalChange();
            }
        } catch (error) {
            console.error('Error refreshing current view:', error);
        }
    },

    toggleDropdown() {
        const button = document.getElementById('fractalSelectorButton');
        const menu = document.getElementById('fractalSelectorMenu');

        if (!button || !menu) {
            return;
        }

        const isOpen = menu.classList.contains('show');

        if (isOpen) {
            this.closeDropdown();
        } else {
            this.openDropdown();
        }
    },

    openDropdown() {
        const button = document.getElementById('fractalSelectorButton');
        const menu = document.getElementById('fractalSelectorMenu');

        if (button && menu) {
            button.classList.add('open');
            menu.classList.add('show');
        }
    },

    closeDropdown() {
        const button = document.getElementById('fractalSelectorButton');
        const menu = document.getElementById('fractalSelectorMenu');

        if (button && menu) {
            button.classList.remove('open');
            menu.classList.remove('show');
        }
    },

    updateSelectorText(text) {
        const textElement = document.getElementById('fractalSelectorText');
        if (textElement) {
            textElement.textContent = text;
        }
        this._updateTypeIcon();
    },

    _updateTypeIcon() {
        const iconEl = document.getElementById('contextTypeIcon');
        const btn = document.getElementById('fractalSelectorButton');
        if (!iconEl) return;

        const isPrism = window.FractalContext && FractalContext.isPrism();
        if (btn) btn.dataset.type = isPrism ? 'prism' : 'fractal';

        // Same icon shape for both types; color differentiates fractal vs prism via CSS data-type
        iconEl.innerHTML = `<svg width="16" height="16" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round">
            <polygon points="1.5,1.5 1.5,12.5 12.5,7"/>
            <line x1="1.5" y1="5.5" x2="7.5" y2="5.5"/>
            <line x1="1.5" y1="8.5" x2="7.5" y2="8.5"/>
        </svg>`;
    },

    showErrorInMenu(errorMessage) {
        const menu = document.getElementById('fractalSelectorMenu');
        if (menu) {
            menu.innerHTML = `
                <div class="fractal-selector-loading" style="color: var(--error-color);">
                    Error: ${Utils.escapeHtml(errorMessage)}
                    <button onclick="FractalSelector.loadAvailableFractals()"
                            style="display: block; margin-top: 8px; color: var(--accent-color); background: none; border: none; cursor: pointer;">
                        Retry
                    </button>
                </div>
            `;
        }
    },

    showFractalManagement() {
        this.closeDropdown();

        // Navigate to fractal management view
        if (window.App && App.showView) {
            App.showView('fractalManagement');
        } else {
            // Fallback: show a simple alert for now
            alert('Index Management feature coming soon!');
        }
    },

    // Public API methods
    getCurrentFractal() {
        return this.currentFractal;
    },

    getCurrentFractalId() {
        return this.currentFractal ? this.currentFractal.id : null;
    },

    getCurrentFractalName() {
        return this.currentFractal ? this.currentFractal.name : 'Unknown';
    },

    // Method to be called when auth state changes
    onAuthChange() {
        this.currentFractal = null;
        this.availableFractals = [];
        this.availablePrisms = [];
        this.loadAvailableFractals();
    }
};

// Make globally available
window.FractalSelector = FractalSelector;