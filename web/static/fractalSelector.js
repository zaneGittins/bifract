const FractalSelector = {
    currentFractal: null,
    availableFractals: [],
    availablePrisms: [],
    isLoading: false,

    init() {
        console.log('Initializing Fractal Selector...');
        this.createSelectorUI();
        this.loadAvailableFractals();
        this.setupEventListeners();
    },

    createSelectorUI() {
        // Check if selector already exists
        if (document.getElementById('fractalSelectorContainer')) {
            console.log('[FractalSelector] Selector already exists, skipping creation');
            return;
        }

        // Find the header where we'll add the fractal selector
        const header = document.querySelector('.header');
        if (!header) {
            console.warn('Header element not found for fractal selector');
            return;
        }

        // Create fractal selector dropdown HTML
        const selectorHTML = `
            <div class="fractal-selector-container" id="fractalSelectorContainer">
                <div class="fractal-selector-dropdown">
                    <button class="fractal-selector-button" id="fractalSelectorButton">
                        <span class="fractal-selector-text" id="fractalSelectorText">Loading...</span>
                        <span class="fractal-selector-arrow">▼</span>
                    </button>
                    <div class="fractal-selector-menu" id="fractalSelectorMenu">
                        <div class="fractal-selector-loading">
                            Loading fractals...
                        </div>
                    </div>
                </div>
            </div>
        `;

        // Insert the selector into the header-right section after userInfo
        const userInfo = header.querySelector('#userInfo');
        const headerRight = header.querySelector('.header-right');
        if (userInfo) {
            userInfo.insertAdjacentHTML('afterend', selectorHTML);
        } else if (headerRight) {
            headerRight.insertAdjacentHTML('beforeend', selectorHTML);
        } else {
            header.insertAdjacentHTML('beforeend', selectorHTML);
        }

        // Add CSS styles
        this.addStyles();
    },

    addStyles() {
        if (document.getElementById('fractalSelectorStyles')) {
            return; // Styles already added
        }

        const styles = `
            <style id="fractalSelectorStyles">
            .header-right {
                position: relative;
            }

            .fractal-selector-container {
                position: absolute;
                top: 100%;
                right: 0;
                margin-top: 5px;
                z-index: 1000;
            }

            .fractal-selector-dropdown {
                position: relative;
                display: inline-block;
            }

            .fractal-selector-button {
                background: transparent;
                border: 1px solid var(--overlay-border);
                border-radius: 4px;
                color: var(--text-secondary);
                padding: 6px 10px;
                cursor: pointer;
                display: flex;
                align-items: center;
                gap: 6px;
                font-size: 12px;
                line-height: 1.3;
                min-width: 120px;
                transition: all 0.2s ease;
                opacity: 0.8;
            }

            .fractal-selector-button:hover {
                background: var(--overlay-subtle);
                border-color: var(--overlay-border-hover);
                opacity: 1;
            }

            .fractal-selector-button.open {
                background: var(--overlay-light);
                border-color: var(--accent-color);
                opacity: 1;
            }

            .fractal-selector-text {
                flex: 1;
                text-align: left;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                font-weight: 400;
                opacity: 0.9;
            }

            .fractal-selector-arrow {
                font-size: 8px;
                opacity: 0.5;
                transition: transform 0.2s ease;
            }

            .fractal-selector-button.open .fractal-selector-arrow {
                transform: rotate(180deg);
            }

            .fractal-selector-menu {
                position: absolute;
                top: 100%;
                left: 0;
                right: 0;
                background: var(--bg-secondary);
                border: 1px solid var(--border-color);
                border-radius: 6px;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
                z-index: 1000;
                display: none;
                max-height: 300px;
                overflow-y: auto;
                margin-top: 4px;
            }

            .fractal-selector-menu.show {
                display: block;
            }

            .fractal-selector-loading {
                padding: 12px;
                text-align: center;
                color: var(--text-secondary);
                font-size: 13px;
            }

            .fractal-selector-item {
                display: block;
                width: 100%;
                padding: 10px 12px;
                border: none;
                background: none;
                color: var(--text-primary);
                text-align: left;
                cursor: pointer;
                font-size: 13px;
                line-height: 1.4;
                transition: background-color 0.2s ease;
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
                color: rgba(255, 255, 255, 0.75);
            }

            .fractal-selector-item.current::after {
                content: '\2713';
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

            .fractal-selector-admin-section {
                padding: 8px 12px;
                background: var(--bg-tertiary);
                border-top: 1px solid var(--border-color);
            }

            .fractal-selector-admin-button {
                display: block;
                width: 100%;
                padding: 6px 8px;
                background: var(--accent-color);
                color: white;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 12px;
                text-align: center;
                transition: background-color 0.2s ease;
            }

            .fractal-selector-admin-button:hover {
                background: var(--accent-hover);
            }

            @media (max-width: 768px) {
                .fractal-selector-container {
                    margin-right: 8px;
                }

                .fractal-selector-button {
                    min-width: 120px;
                    padding: 6px 10px;
                    font-size: 13px;
                }

                .fractal-selector-text {
                    max-width: 80px;
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
            if (!e.target.closest('.fractal-selector-container')) {
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

        console.log('[FractalSelector] Share link detected, selecting', isPrism ? 'prism' : 'fractal', target.name);

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
        // Select the default fractal if no fractal is currently selected
        if (!this.currentFractal && this.availableFractals.length > 0) {
            const defaultFractal = this.availableFractals.find(fractal => fractal.is_default);
            const firstFractal = this.availableFractals[0];
            const targetFractal = defaultFractal || firstFractal;

            if (targetFractal) {
                this.currentFractal = targetFractal;
                this.updateSelectorText(targetFractal.name);
            }
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

            // Update current selection
            this.currentFractal = selectedFractal;
            this.updateSelectorText(selectedFractal.name);

            // Update auth fractal_role from the stored user_role
            if (window.Auth && Auth.currentUser) {
                Auth.currentUser.fractal_role = selectedFractal.user_role || '';
                Auth.updateRBACVisibility();
            }

            // Keep FractalContext in sync
            if (window.FractalContext) {
                FractalContext.currentFractal = selectedFractal;
                FractalContext.currentItemType = 'fractal';
            }

            // Refresh current view data to reflect new fractal
            this.refreshCurrentView();

            // Show success message
            if (window.Toast) {
                Toast.show(`Switched to fractal: ${selectedFractal.name}`, 'success');
            }

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

            // Keep FractalContext in sync
            if (window.FractalContext) {
                FractalContext.currentFractal = prism;
                FractalContext.currentItemType = 'prism';
            }

            this.refreshCurrentView();

            if (window.Toast) Toast.show(`Switched to prism: ${prism.name}`, 'success');
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
        console.log(`Current fractal set to: ${fractalObject.name} (${fractalObject.id})`);
    },

    refreshCurrentView() {
        // Refresh data in the current view to reflect the new fractal selection
        try {
            // Determine current view and refresh its data
            const currentView = App.getCurrentView ? App.getCurrentView() : null;

            switch (currentView) {
                case 'search':
                    // Refresh search results if there's an active query
                    if (window.QueryExecutor && QueryExecutor.onFractalChange) {
                        QueryExecutor.onFractalChange();
                    }
                    break;

                case 'alerts':
                    // Refresh alerts list
                    if (window.Alerts && Alerts.onFractalChange) {
                        Alerts.onFractalChange();
                    }
                    break;

                case 'commented':
                    // Refresh commented logs
                    if (window.CommentedLogs && CommentedLogs.onFractalChange) {
                        CommentedLogs.onFractalChange();
                    }
                    break;

                default:
                    // For other views, trigger a general refresh
                    console.log('Fractal changed, current view:', currentView);
                    break;
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