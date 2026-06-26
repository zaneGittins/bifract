/**
 * Notebooks Frontend Module
 * Handles notebook creation, editing, section management, and real-time features
 */

const Notebooks = {
    currentNotebook: null,
    activeUsers: new Set(),
    presenceInterval: null,
    eventSource: null,
    sseClientId: null,
    currentPage: 0,
    pageSize: 20,
    totalNotebooks: 0,
    searchQuery: '',
    isEditing: false,
    aiEnabled: false,
    _tocObserver: null,
    _tocVisibleSections: new Set(),
    _currentTOCActiveId: null,

    /**
     * Initialize the notebooks module
     */
    init() {

        // Ensure we're starting fresh
        this.currentNotebook = null;
        this.stopPresenceTracking();

        this.bindEvents();
        this.showNotebookListing();
        if (window.FractalContext && typeof FractalContext.subscribe === 'function') {
            FractalContext.subscribe('Notebooks', () => this.onFractalChange());
        }
    },

    onFractalChange() {
        this.currentNotebook = null;
        this.isEditing = false;
        this.stopPresenceTracking();
        this.currentPage = 0;
        this.searchQuery = '';
        // Clear rendered DOM unconditionally so stale notebooks from the
        // previous scope don't flash on tab re-entry.
        const tbody = document.getElementById('notebooksTableBody');
        if (tbody) tbody.innerHTML = '';

        const view = document.getElementById('notebooksView');
        if (view && view.offsetParent !== null) {
            this.showNotebookListing();
        }
    },

    /**
     * Bind event listeners
     */
    bindEvents() {

        // Remove any existing event listeners to prevent duplicates
        this.unbindEvents();

        // Notebook listing events
        const createBtn = document.getElementById('createNotebookBtn');
        if (createBtn) {
            createBtn.addEventListener('click', () => {
                this.showCreateNotebookModal();
            });
        } else {
            // console.warn('[Notebooks] Create notebook button not found');
        }

        const searchInput = document.getElementById('notebookSearchInput');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => this.handleSearch(e.target.value));
        }

        // Pagination events
        const prevBtn = document.getElementById('notebooksPrevBtn');
        if (prevBtn) {
            prevBtn.addEventListener('click', () => this.previousPage());
        }

        const nextBtn = document.getElementById('notebooksNextBtn');
        if (nextBtn) {
            nextBtn.addEventListener('click', () => this.nextPage());
        }

        // Notebook editor events
        const backBtn = document.getElementById('backToNotebooksBtn');
        if (backBtn) {
            backBtn.addEventListener('click', () => {
                window.App?.pushSubPath('');
                this.showNotebookListing();
            });
        }

        const saveBtn = document.getElementById('saveNotebookBtn');
        if (saveBtn) {
            saveBtn.addEventListener('click', () => this.saveCurrentNotebook());
        }

        const addSectionBtn = document.getElementById('addSectionBtn');
        if (addSectionBtn) {
            addSectionBtn.addEventListener('click', () => this.showAddSectionMenu());
        }

        const settingsBtn = document.getElementById('notebookSettingsBtn');
        if (settingsBtn) {
            settingsBtn.addEventListener('click', () => this.showNotebookSettings());
        }

        const runAllBtn = document.getElementById('runAllSectionsBtn');
        if (runAllBtn) {
            runAllBtn.addEventListener('click', () => this.runAllSections());
        }

        const sendToChatBtn = document.getElementById('sendNotebookToChatBtn');
        if (sendToChatBtn) {
            sendToChatBtn.addEventListener('click', () => this.sendNotebookToChat());
        }

        const tocBtn = document.getElementById('notebookTOCBtn');
        if (tocBtn) tocBtn.addEventListener('click', () => this.toggleTOC());

        const tocClose = document.getElementById('notebookTOCClose');
        if (tocClose) tocClose.addEventListener('click', () => this.toggleTOC());

        const tocSearch = document.getElementById('notebookTOCSearch');
        if (tocSearch) tocSearch.addEventListener('input', (e) => this.filterTOC(e.target.value));

        // Wheel over TOC → jump between sections; page scroll is unaffected
        const tocPanel = document.getElementById('notebookTOC');
        if (tocPanel) {
            tocPanel.addEventListener('wheel', (e) => {
                if (tocPanel.classList.contains('collapsed')) return;
                e.preventDefault();
                if (this._tocScrollLock) return;
                this._tocScrollLock = true;
                setTimeout(() => { this._tocScrollLock = false; }, 120);
                const dir = e.deltaY > 0 ? 1 : -1;
                const sections = [...(this.currentNotebook?.sections || [])]
                    .sort((a, b) => a.order_index - b.order_index);
                const cur = sections.findIndex(s => s.id === this._currentTOCActiveId);
                const next = sections[Math.max(0, Math.min(sections.length - 1, cur + dir))];
                if (!next) return;
                // Instant jump for wheel nav — no animation to fight
                const el = document.querySelector(`[data-section-id="${next.id}"]`);
                if (el) {
                    const targetY = window.scrollY + el.getBoundingClientRect().top - (window.innerHeight - el.offsetHeight) / 2;
                    window.scrollTo(0, Math.max(0, targetY));
                    el.style.outline = '2px solid var(--accent-primary)';
                    setTimeout(() => { el.style.outline = ''; }, 2000);
                    this._updateTOCActive(next.id);
                }
            }, { passive: false });
        }

        // Global keyboard shortcuts
        if (!this.keyboardHandler) {
            this.keyboardHandler = (e) => this.handleKeyboardShortcuts(e);
            document.addEventListener('keydown', this.keyboardHandler);
        }

        // Close kebab menus on outside click
        if (!this.kebabCloseHandler) {
            this.kebabCloseHandler = (e) => {
                if (!e.target.closest('.section-kebab-wrapper')) {
                    document.querySelectorAll('.section-kebab-menu.open').forEach(m => m.classList.remove('open'));
                }
            };
            document.addEventListener('click', this.kebabCloseHandler);
        }

    },

    /**
     * Remove event listeners
     */
    unbindEvents() {
        if (this._sectionObserver) {
            this._sectionObserver.disconnect();
            this._sectionObserver = null;
        }
        if (this._tocObserver) {
            this._tocObserver.disconnect();
            this._tocObserver = null;
        }
        this._tocVisibleSections.clear();
        this._currentTOCActiveId = null;
        if (this.keyboardHandler) {
            document.removeEventListener('keydown', this.keyboardHandler);
            this.keyboardHandler = null;
        }
        if (this.kebabCloseHandler) {
            document.removeEventListener('click', this.kebabCloseHandler);
            this.kebabCloseHandler = null;
        }
    },

    /**
     * Show notebook listing view
     */
    async showNotebookListing() {
        document.getElementById('notebookListing').style.display = 'block';
        document.getElementById('notebookEditor').style.display = 'none';
        const fab = document.getElementById('notebookTOCFab');
        if (fab) fab.style.display = 'none';

        this.currentNotebook = null;
        this.stopPresenceTracking();
        await this.loadNotebooks();
    },

    /**
     * Load notebooks from server
     */
    async loadNotebooks() {
        try {
            const tableContainer = document.querySelector('.notebooks-table-container');
            const emptyEl = document.getElementById('notebooksEmptyState');
            const paginationEl = document.querySelector('.notebooks-pagination');
            if (tableContainer) tableContainer.style.display = 'none';
            if (emptyEl) emptyEl.style.display = 'none';
            if (paginationEl) paginationEl.style.display = 'none';
            document.getElementById('notebooksTableBody').innerHTML = '<tr><td colspan="6" class="notebook-loading">Loading notebooks...</td></tr>';

            const params = new URLSearchParams({
                limit: this.pageSize,
                offset: this.currentPage * this.pageSize
            });

            if (this.searchQuery) {
                params.append('search', this.searchQuery);
            }

            const token = window.FractalContext?.scopeToken?.();
            const response = await fetch(`/api/v1/notebooks?${params.toString()}`, {
                method: 'GET',
                credentials: 'include'
            });

            const data = await response.json();
            if (window.FractalContext?.isScopeStale?.(token)) return;

            if (!data.success) {
                throw new Error(data.error || 'Failed to load notebooks');
            }

            this.renderNotebooksTable(data.data || []);
            this.updatePagination(data.total || 0, data.limit || this.pageSize, data.offset || 0);

        } catch (error) {
            console.error('[Notebooks] Error loading notebooks:', error);
            document.getElementById('notebooksTableBody').innerHTML =
                `<tr><td colspan="6" class="notebook-error">Error loading notebooks: ${error.message}</td></tr>`;
        }
    },

    /**
     * Render notebooks table
     */
    renderNotebooksTable(notebooks) {
        const tbody = document.getElementById('notebooksTableBody');
        const tableContainer = tbody.closest('.notebooks-table-container');
        const emptyEl = document.getElementById('notebooksEmptyState');

        if (notebooks.length === 0) {
            if (tableContainer) tableContainer.style.display = 'none';
            if (emptyEl) emptyEl.style.display = '';
            return;
        }

        if (tableContainer) tableContainer.style.display = '';
        if (emptyEl) emptyEl.style.display = 'none';

        tbody.innerHTML = notebooks.map(notebook => `
            <tr>
                <td>
                    <a href="javascript:void(0)" onclick="Notebooks.openNotebook('${notebook.id}')" style="color: var(--accent-primary); text-decoration: none;">
                        ${Utils.escapeHtml(notebook.name)}
                    </a>
                </td>
                <td>${Utils.escapeHtml(notebook.description || '')}</td>
                <td>
                    <span class="time-range-badge">${this.formatTimeRange(notebook)}</span>
                </td>
                <td>${this.formatRelativeTime(notebook.created_at)}</td>
                <td>${this.formatRelativeTime(notebook.updated_at)}</td>
                <td class="kebab-cell">
                    <div class="kebab-wrapper">
                        <button class="kebab-btn" onclick="KebabMenu.toggle(event,this)">⋮</button>
                        <div class="kebab-menu">
                            <button class="kebab-item" onclick="Notebooks.exportNotebook('${notebook.id}')">Export</button>
                            <button class="kebab-item" onclick="Notebooks.duplicateNotebook('${notebook.id}')">Duplicate</button>
                            <button class="kebab-item danger" onclick="Notebooks.deleteNotebook('${Utils.escapeJs(notebook.id)}', '${Utils.escapeJs(notebook.name)}')">Delete</button>
                        </div>
                    </div>
                </td>
            </tr>
        `).join('');
    },

    /**
     * Update pagination controls
     */
    updatePagination(total, limit, offset) {
        this.totalNotebooks = total;
        const totalPages = Math.ceil(total / limit);
        const currentPage = Math.floor(offset / limit) + 1;

        const paginationContainer = document.getElementById('notebooksPrevBtn')?.parentElement;
        if (paginationContainer) {
            paginationContainer.style.display = totalPages <= 1 ? 'none' : '';
        }

        document.getElementById('notebooksPaginationInfo').textContent =
            `Page ${currentPage} of ${totalPages} (${total} notebooks)`;

        document.getElementById('notebooksPrevBtn').disabled = offset === 0;
        document.getElementById('notebooksNextBtn').disabled = offset + limit >= total;
    },

    /**
     * Handle search input
     */
    handleSearch(query) {
        this.searchQuery = query.trim();
        this.currentPage = 0;
        this.debounce(() => this.loadNotebooks(), 300);
    },

    /**
     * Refresh notebook listing
     */
    refreshNotebookListing() {
        this.loadNotebooks();
    },

    /**
     * Go to previous page
     */
    previousPage() {
        if (this.currentPage > 0) {
            this.currentPage--;
            this.loadNotebooks();
        }
    },

    /**
     * Go to next page
     */
    nextPage() {
        const maxPage = Math.ceil(this.totalNotebooks / this.pageSize) - 1;
        if (this.currentPage < maxPage) {
            this.currentPage++;
            this.loadNotebooks();
        }
    },

    /**
     * Show create notebook modal
     */
    showCreateNotebookModal() {
        // Remove any existing create modal first
        const existing = document.getElementById('createNotebookModal');
        if (existing) existing.remove();

        // Create modal dynamically
        const modalHtml = `
            <div id="createNotebookModal" class="modal-overlay">
                <div class="modal-content">
                    <div class="modal-header">
                        <h3>Create New Notebook</h3>
                        <button class="modal-close" id="createNotebookCloseBtn">&times;</button>
                    </div>
                    <form id="createNotebookForm" onsubmit="Notebooks.handleCreateNotebook(event)">
                        <div class="form-group">
                            <label for="notebookName">Name *</label>
                            <input type="text" id="notebookName" name="name" required maxlength="255">
                        </div>
                        <div class="form-group">
                            <label for="notebookDescription">Description</label>
                            <textarea id="notebookDescription" name="description" rows="3" maxlength="1000"></textarea>
                        </div>
                        <div class="form-group">
                            <label for="notebookTimeRange">Time Range *</label>
                            <select id="notebookTimeRange" name="time_range_type" required>
                                <option value="1h">Last 1 hour</option>
                                <option value="24h" selected>Last 24 hours</option>
                                <option value="7d">Last 7 days</option>
                                <option value="30d">Last 30 days</option>
                                <option value="all">All Time</option>
                                <option value="custom">Custom range</option>
                            </select>
                        </div>
                        <div id="customTimeRange" class="form-group" style="display: none; margin-top: 10px; padding: 10px; border: 1px solid var(--border-color); border-radius: 6px; background: var(--bg-tertiary);">
                            <div style="margin-bottom: 10px;">
                                <label for="timeRangeStart" style="display: block; margin-bottom: 5px; font-weight: bold;">Start Time</label>
                                <input type="text" placeholder="YYYY-MM-DD HH:mm" id="timeRangeStart" name="time_range_start"
                                       style="width: 100%; padding: 8px; border: 1px solid var(--border-color); border-radius: 4px; background: var(--bg-primary); color: var(--text-primary);">
                            </div>
                            <div>
                                <label for="timeRangeEnd" style="display: block; margin-bottom: 5px; font-weight: bold;">End Time</label>
                                <input type="text" placeholder="YYYY-MM-DD HH:mm" id="timeRangeEnd" name="time_range_end"
                                       style="width: 100%; padding: 8px; border: 1px solid var(--border-color); border-radius: 4px; background: var(--bg-primary); color: var(--text-primary);">
                            </div>
                        </div>
                        <div class="form-group">
                            <label for="maxResultsPerSection">Max Results per Section</label>
                            <input type="number" id="maxResultsPerSection" name="max_results_per_section" value="1000" min="1" max="10000">
                        </div>
                        <div class="modal-buttons">
                            <button type="button" class="btn-secondary" onclick="Notebooks.closeCreateNotebookModal()">Cancel</button>
                            <button type="submit" class="btn-primary">Create Notebook</button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.insertAdjacentHTML('beforeend', modalHtml);

        const modal = document.getElementById('createNotebookModal');

        // Close on overlay click (outside modal-content)
        if (modal) {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) this.closeCreateNotebookModal();
            });
        }

        // Close on Escape key
        const escHandler = (e) => {
            if (e.key === 'Escape') {
                this.closeCreateNotebookModal();
                document.removeEventListener('keydown', escHandler);
            }
        };
        document.addEventListener('keydown', escHandler);

        // Bind close button event
        const closeBtn = document.getElementById('createNotebookCloseBtn');
        if (closeBtn) {
            closeBtn.addEventListener('click', () => this.closeCreateNotebookModal());
        }

        // Bind cancel button event
        const cancelBtn = modal?.querySelector('.btn-secondary');
        if (cancelBtn) {
            cancelBtn.addEventListener('click', () => this.closeCreateNotebookModal());
        }

        // Bind time range change event
        const timeRangeSelect = document.getElementById('notebookTimeRange');
        if (timeRangeSelect) {
            timeRangeSelect.addEventListener('change', (e) => {
                const customRange = document.getElementById('customTimeRange');
                const startInput = document.getElementById('timeRangeStart');
                const endInput = document.getElementById('timeRangeEnd');
                const isCustom = e.target.value === 'custom';

                if (customRange) {
                    customRange.style.display = isCustom ? 'block' : 'none';
                }

                // Toggle required on custom time inputs
                if (startInput) startInput.required = isCustom;
                if (endInput) endInput.required = isCustom;

                // Set default values when switching to custom
                if (isCustom && startInput && endInput) {
                    const now = new Date();
                    const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

                    // Format as local time for datetime-local input
                    const formatLocal = (d) => {
                        const pad = (n) => String(n).padStart(2, '0');
                        return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
                    };

                    startInput.value = formatLocal(twentyFourHoursAgo);
                    endInput.value = formatLocal(now);
                }
            });
        } else {
            console.error('[Notebooks] notebookTimeRange select not found');
        }

        // Focus name input
        document.getElementById('notebookName').focus();
    },

    /**
     * Close create notebook modal
     */
    closeCreateNotebookModal() {
        const modal = document.getElementById('createNotebookModal');
        if (modal) {
            modal.remove();
        }
    },

    /**
     * Handle create notebook form submission
     */
    async handleCreateNotebook(event) {
        event.preventDefault();


        // Disable the submit button to prevent double submission
        const submitBtn = event.target.querySelector('button[type="submit"]');
        const originalText = submitBtn?.textContent || 'Create Notebook';
        if (submitBtn) {
            submitBtn.disabled = true;
            submitBtn.textContent = 'Creating...';
        }

        try {
            const formData = new FormData(event.target);
            const data = {
                name: formData.get('name'),
                description: formData.get('description') || '',
                time_range_type: formData.get('time_range_type'),
                max_results_per_section: parseInt(formData.get('max_results_per_section')) || 1000
            };


            if (data.time_range_type === 'custom') {
                const start = formData.get('time_range_start');
                const end = formData.get('time_range_end');


                if (!start || !end) {
                    this.showError('Start and end times are required for custom time range');
                    return;
                }

                // Validate that start is before end
                const startDate = new Date(start);
                const endDate = new Date(end);

                if (startDate >= endDate) {
                    this.showError('Start time must be before end time');
                    return;
                }

                data.time_range_start = startDate.toISOString();
                data.time_range_end = endDate.toISOString();

            }


            const response = await fetch('/api/v1/notebooks', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                credentials: 'include',
                body: JSON.stringify(data)
            });


            if (!response.ok) {
                if (response.status === 401) {
                    throw new Error('Authentication required. Please log in.');
                }
                if (response.status === 403) {
                    throw new Error('Permission denied. Please check your access rights.');
                }
                throw new Error(`Server error: ${response.status} ${response.statusText}`);
            }

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to create notebook');
            }

            this.showSuccess('Notebook created successfully!');
            this.closeCreateNotebookModal();
            await this.loadNotebooks();

            // Optionally open the new notebook
            if (result.data && result.data.id) {
                setTimeout(() => {
                    this.openNotebook(result.data.id);
                }, 500);
            }

        } catch (error) {
            console.error('[Notebooks] Error creating notebook:', error);
            this.showError(`Error creating notebook: ${error.message}`);
        } finally {
            // Re-enable the submit button
            if (submitBtn) {
                submitBtn.disabled = false;
                submitBtn.textContent = originalText;
            }
        }
    },

    /**
     * Open notebook for editing
     */
    async openNotebook(notebookId) {
        try {

            if (!notebookId) {
                throw new Error('No notebook ID provided');
            }

            window.App?.pushSubPath(notebookId);
            this.showLoadingState('Loading notebook...');

            const response = await fetch(`/api/v1/notebooks/${notebookId}`, {
                method: 'GET',
                credentials: 'include'
            });


            if (!response.ok) {
                if (response.status === 404) {
                    throw new Error('Notebook not found');
                }
                if (response.status === 401) {
                    throw new Error('Authentication required. Please log in.');
                }
                if (response.status === 403) {
                    throw new Error('Permission denied.');
                }
                throw new Error(`Server error: ${response.status} ${response.statusText}`);
            }

            const data = await response.json();

            if (!data.success) {
                throw new Error(data.error || 'Failed to load notebook');
            }

            if (!data.data) {
                throw new Error('No notebook data received');
            }

            this.currentNotebook = data.data;

            this.showNotebookEditor();
            this.startPresenceTracking();

        } catch (error) {
            console.error('[Notebooks] Error opening notebook:', error);
            this.showError(`Error opening notebook: ${error.message}`);

            // Return to notebook listing on error
            this.showNotebookListing();
        }
    },

    /**
     * Show notebook editor
     */
    showNotebookEditor() {

        // First hide the listing and show the editor
        const listingEl = document.getElementById('notebookListing');
        const editorEl = document.getElementById('notebookEditor');

        if (!listingEl || !editorEl) {
            console.error('[Notebooks] Required elements not found:', { listingEl, editorEl });
            this.showError('Failed to show notebook editor - missing DOM elements');
            return;
        }

        listingEl.style.display = 'none';
        editorEl.style.display = 'block';
        const fab = document.getElementById('notebookTOCFab');
        if (fab) fab.style.display = 'flex';

        // Update notebook title immediately
        const titleEl = document.getElementById('notebookTitle');
        if (titleEl && this.currentNotebook) {
            titleEl.textContent = this.currentNotebook.name;
        }

        // Render variables bar
        this.renderVariablesBar();

        // Wait for DOM to be ready, then render sections
        this.waitForSectionsContainer().then(() => {
            this.renderNotebookSections();
            this.isEditing = true;
        }).catch((error) => {
            console.error('[Notebooks] Error initializing notebook editor:', error);
            this.showError('Failed to initialize notebook editor. Please try refreshing the page.');
        });
    },

    /**
     * Wait for sections container to be available in DOM
     */
    async waitForSectionsContainer() {
        const maxAttempts = 20;
        const delay = 50;

        for (let attempt = 0; attempt < maxAttempts; attempt++) {
            const sectionsContainer = document.getElementById('notebookSections');
            if (sectionsContainer) {
                return sectionsContainer;
            }

            await new Promise(resolve => setTimeout(resolve, delay));
        }

        throw new Error('Notebook sections container not found after maximum wait time');
    },

    /**
     * Render notebook sections
     */
    renderNotebookSections() {
        if (this._sectionObserver) {
            this._sectionObserver.disconnect();
            this._sectionObserver = null;
        }

        const container = document.getElementById('notebookSections');

        if (!container) {
            console.error('[Notebooks] notebookSections container not found');
            this.showError('Failed to find notebook sections container');
            return;
        }

        if (!this.currentNotebook) {
            console.error('[Notebooks] No current notebook to render');
            this.showError('No notebook loaded');
            return;
        }

        if (!this.currentNotebook.sections || this.currentNotebook.sections.length === 0) {
            container.innerHTML = `
                <div class="notebook-empty">
                    <p style="text-align: center; color: var(--text-muted); margin: 40px 0;">This notebook is empty. Add your first section to get started!</p>
                </div>
            `;
            return;
        }

        // Sort sections by order_index
        const sections = this.currentNotebook.sections.sort((a, b) => a.order_index - b.order_index);

        container.innerHTML = sections.map(section => this.renderSection(section, true)).join('');

        // Bind section events
        this.bindSectionEvents();

        // Lazy-render section content as sections scroll into view
        this._sectionObserver = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (!entry.isIntersecting) return;
                const sectionEl = entry.target;
                const contentEl = sectionEl.querySelector('.section-content');
                if (!contentEl || contentEl.dataset.rendered === 'true') return;
                const sectionId = sectionEl.dataset.sectionId;
                const section = this.currentNotebook?.sections.find(s => s.id === sectionId);
                if (!section) return;
                contentEl.innerHTML = this.renderSectionContent(section);
                contentEl.dataset.rendered = 'true';
                this._sectionObserver.unobserve(sectionEl);
            });
        }, { rootMargin: '300px' });
        container.querySelectorAll('.notebook-section').forEach(el => this._sectionObserver.observe(el));

        this.activeTagFilters = [];
        this.updateTagFilterBar();
        this.renderTOCList();
        this._setupTOCObserver();
    },

    /**
     * Render a single section
     */
    renderSection(section, lazy = false) {
        let titleHtml = '';
        let controlsHtml = '';

        if (section.section_type === 'comment_context') {
            // Parse comment data for gravatar and search icon
            let ccData = {};
            try { ccData = JSON.parse(section.content || '{}'); } catch (e) {}
            const gravatarColor = ccData.author_gravatar_color || 'var(--accent-primary)';
            const gravatarInitial = Utils.escapeHtml(ccData.author_gravatar_initial || (ccData.author || '?').charAt(0).toUpperCase());
            const displayName = Utils.escapeHtml(ccData.author_display_name || ccData.author || 'Unknown');
            const commentedAt = ccData.commented_at ? this.formatRelativeTime(ccData.commented_at) : '';
            const ccQuery = ccData.query || '';
            const ccLogId = ccData.log_id || '';

            titleHtml = `
                <span class="section-drag-handle" draggable="true" style="cursor: grab; user-select: none; padding: 4px;">⋮⋮</span>
                <span class="section-type-text" style="font-weight: 400; color: var(--text-muted); font-size: 0.8rem;">${displayName}</span>
                ${commentedAt ? `<span style="color: var(--text-muted); font-size: 0.75rem; margin-left: 4px;">${commentedAt}</span>` : ''}
                ${this.renderTagsArea(section)}
            `;

            // Search icon (magnifying glass) to open query in search view
            const searchBtn = ccQuery ? `<button class="section-control-btn comment-context-search-icon" onclick="Notebooks.openQueryInSearch(this)" data-query="${Utils.escapeHtml(ccQuery)}" title="Open query in search"><svg width="12" height="12" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="6.5" cy="6.5" r="5.5" stroke="currentColor" stroke-width="2"/><line x1="10.5" y1="10.5" x2="15" y2="15" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg></button>` : '';

            // Target icon to open log_id in search view
            const targetBtn = ccLogId ? `<button class="section-control-btn comment-context-search-icon" onclick="Notebooks.openLogIdInSearch('${Utils.escapeJs(ccLogId)}')" title="Find log in search"><svg width="12" height="12" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.5" fill="none"/><circle cx="8" cy="8" r="2.5" stroke="currentColor" stroke-width="1.5" fill="none"/><line x1="8" y1="0.5" x2="8" y2="3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><line x1="8" y1="13" x2="8" y2="15.5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><line x1="0.5" y1="8" x2="3" y2="8" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><line x1="13" y1="8" x2="15.5" y2="8" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg></button>` : '';

            // Play button to fetch log_id
            const playBtn = ccLogId ? `<button class="execute-query-btn" onclick="Notebooks.executeCommentContextSection('${section.id}', event)" style="background: var(--bg-tertiary); color: var(--text-primary); border: 1px solid var(--border-color); padding: 4px 6px; border-radius: 4px; cursor: pointer; display: flex; align-items: center; justify-content: center; font-size: 0.8rem; transition: var(--transition); margin-right: 4px;" onmouseover="this.style.background='var(--accent-primary)'; this.style.color='white'; this.style.borderColor='var(--accent-primary)'" onmouseout="this.style.background='var(--bg-tertiary)'; this.style.color='var(--text-primary)'; this.style.borderColor='var(--border-color)'" title="Fetch log">▶</button>` : '';

            controlsHtml = `
                ${searchBtn}
                ${targetBtn}
                ${playBtn}
                <button class="section-move-btn section-move-up" onclick="Notebooks.moveSectionUp('${section.id}')" style="background: var(--bg-tertiary); color: var(--text-primary); border: 1px solid var(--border-color); padding: 4px 6px; border-radius: 4px; cursor: pointer; font-size: 0.8rem; margin-right: 4px; display: inline-flex; align-items: center; justify-content: center;" title="Move Up"><svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 10 8 5 13 10"/></svg></button>
                <button class="section-move-btn section-move-down" onclick="Notebooks.moveSectionDown('${section.id}')" style="background: var(--bg-tertiary); color: var(--text-primary); border: 1px solid var(--border-color); padding: 4px 6px; border-radius: 4px; cursor: pointer; font-size: 0.8rem; margin-right: 4px; display: inline-flex; align-items: center; justify-content: center;" title="Move Down"><svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 8 11 13 6"/></svg></button>
                <div class="section-kebab-wrapper">
                    <button class="section-kebab-btn" onclick="Notebooks.toggleSectionKebab('${section.id}', event)" title="More options">⋯</button>
                    <div class="section-kebab-menu" id="kebab-menu-${section.id}">
                        <button onclick="Notebooks.moveSectionToTop('${section.id}')">Move to top</button>
                        <div class="kebab-divider"></div>
                        <button class="kebab-danger" onclick="Notebooks.deleteSection('${section.id}')">Delete</button>
                    </div>
                </div>
            `;
        } else {
            titleHtml = `
                <span class="section-drag-handle" draggable="true" style="cursor: grab; user-select: none; padding: 4px;">⋮⋮</span>
                <span class="section-type-text">${section.title ? Utils.escapeHtml(section.title) : 'Untitled Section'}</span>
                ${this.renderTagsArea(section)}
            `;

            controlsHtml = `
                ${section.section_type === 'query' ? `<button class="execute-query-btn" onclick="Notebooks.executeQuerySection('${section.id}')" style="background: var(--bg-tertiary); color: var(--text-primary); border: 1px solid var(--border-color); padding: 4px 6px; border-radius: 4px; cursor: pointer; display: flex; align-items: center; justify-content: center; font-size: 0.8rem; transition: var(--transition); margin-right: 4px;" onmouseover="this.style.background='var(--accent-primary)'; this.style.color='white'; this.style.borderColor='var(--accent-primary)'" onmouseout="this.style.background='var(--bg-tertiary)'; this.style.color='var(--text-primary)'; this.style.borderColor='var(--border-color)'" title="Execute Query">▶</button><button onclick="Notebooks.openFormatPanel('${section.id}')" style="background: var(--bg-tertiary); color: var(--text-primary); border: 1px solid var(--border-color); padding: 4px 6px; border-radius: 4px; cursor: pointer; font-size: 0.75rem; margin-right: 8px;" title="Format">&#9881;</button>` : ''}
                ${section.section_type === 'ai_summary' || section.section_type === 'ai_attack_chain' ? `<button class="execute-query-btn" onclick="Notebooks.generateAISummary('${section.id}')" id="ai-summary-btn-${section.id}" style="background: var(--bg-tertiary); color: var(--text-primary); border: 1px solid var(--border-color); padding: 4px 6px; border-radius: 4px; cursor: pointer; display: flex; align-items: center; justify-content: center; font-size: 0.8rem; transition: var(--transition); margin-right: 4px;" onmouseover="this.style.background='var(--accent-primary)'; this.style.color='white'; this.style.borderColor='var(--accent-primary)'" onmouseout="this.style.background='var(--bg-tertiary)'; this.style.color='var(--text-primary)'; this.style.borderColor='var(--border-color)'" title="${section.section_type === 'ai_attack_chain' ? 'Regenerate Attack Chain' : 'Generate AI Summary'}">▶</button>` : ''}
                <button class="section-move-btn section-move-up" onclick="Notebooks.moveSectionUp('${section.id}')" style="background: var(--bg-tertiary); color: var(--text-primary); border: 1px solid var(--border-color); padding: 4px 6px; border-radius: 4px; cursor: pointer; font-size: 0.8rem; margin-right: 4px; display: inline-flex; align-items: center; justify-content: center;" title="Move Up"><svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 10 8 5 13 10"/></svg></button>
                <button class="section-move-btn section-move-down" onclick="Notebooks.moveSectionDown('${section.id}')" style="background: var(--bg-tertiary); color: var(--text-primary); border: 1px solid var(--border-color); padding: 4px 6px; border-radius: 4px; cursor: pointer; font-size: 0.8rem; margin-right: 4px; display: inline-flex; align-items: center; justify-content: center;" title="Move Down"><svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 8 11 13 6"/></svg></button>
                ${section.section_type !== 'ai_summary' && section.section_type !== 'ai_attack_chain' ? `<button class="section-control-btn" onclick="Notebooks.toggleEditSection('${section.id}')">Edit</button>` : ''}
                <div class="section-kebab-wrapper">
                    <button class="section-kebab-btn" onclick="Notebooks.toggleSectionKebab('${section.id}', event)" title="More options">⋯</button>
                    <div class="section-kebab-menu" id="kebab-menu-${section.id}">
                        <button onclick="Notebooks.moveSectionToTop('${section.id}')">Move to top</button>
                        ${section.section_type !== 'ai_summary' && section.section_type !== 'ai_attack_chain' ? `<button onclick="Notebooks.duplicateSection('${section.id}')">Duplicate</button>` : ''}
                        <div class="kebab-divider"></div>
                        <button class="kebab-danger" onclick="Notebooks.deleteSection('${section.id}')">Delete</button>
                    </div>
                </div>
            `;
        }

        const sectionHtml = `
            <div class="notebook-section" data-section-id="${section.id}">
                <div class="section-header">
                    <div class="section-type">
                        ${titleHtml}
                    </div>
                    <div class="section-controls">
                        ${controlsHtml}
                    </div>
                </div>
                <div class="section-content" id="section-content-${section.id}" data-rendered="${lazy ? 'false' : 'true'}">
                    ${lazy ? '' : this.renderSectionContent(section)}
                </div>
            </div>
        `;

        return sectionHtml;
    },

    // ── Tag rendering ────────────────────────────────────────────────────────

    tagColorFor(tag) {
        const palette = ['#9c6ade','#6bcf7f','#5bbce4','#e07a8b','#d4a054','#ca6be0','#5bc4b5','#e07a4f','#7a9de0','#b5c44f'];
        let h = 0;
        for (let i = 0; i < tag.length; i++) h = (h * 31 + tag.charCodeAt(i)) >>> 0;
        return palette[h % palette.length];
    },

    renderTagsArea(section) {
        const tags = (section.tags || []);
        const chips = tags.map(t => {
            const color = this.tagColorFor(t);
            return `<span class="section-tag-chip" data-tag="${Utils.escapeHtml(t)}" style="--chip-color:${color}" onclick="Notebooks.onTagChipClick('${Utils.escapeJs(t)}')">${Utils.escapeHtml(t)}<button class="section-tag-remove" data-section-id="${section.id}" data-tag="${Utils.escapeHtml(t)}" onclick="event.stopPropagation();Notebooks.removeTagFromSection('${section.id}','${Utils.escapeJs(t)}')" title="Remove tag">×</button></span>`;
        }).join('');
        return `<span class="section-tags-area" id="section-tags-${section.id}">${chips}<button class="section-tag-add" data-section-id="${section.id}" onclick="Notebooks.openTagInput('${section.id}',this)" title="Add tag">+</button></span>`;
    },

    onTagChipClick(tag) {
        this.toggleTagFilter(tag);
    },

    openTagInput(sectionId, btn) {
        if (document.getElementById(`tag-input-${sectionId}`)) return;
        const input = document.createElement('input');
        input.type = 'text';
        input.id = `tag-input-${sectionId}`;
        input.className = 'section-tag-input';
        input.placeholder = 'tag name…';
        input.setAttribute('autocomplete', 'off');

        const dropdown = document.createElement('div');
        dropdown.className = 'section-tag-dropdown';
        dropdown.id = `tag-dropdown-${sectionId}`;

        const wrapper = document.createElement('span');
        wrapper.className = 'section-tag-input-wrapper';
        wrapper.appendChild(input);
        wrapper.appendChild(dropdown);

        btn.replaceWith(wrapper);
        input.focus();

        const notebookId = this.currentNotebook ? this.currentNotebook.id : null;
        if (notebookId && !this._tagCache) {
            fetch(`/api/v1/notebooks/${notebookId}/tags`, { credentials: 'include' })
                .then(r => r.json())
                .then(d => { this._tagCache = d.tags || []; this._renderTagDropdown(sectionId, input.value); })
                .catch(() => {});
        } else {
            this._renderTagDropdown(sectionId, input.value);
        }

        input.addEventListener('input', () => this._renderTagDropdown(sectionId, input.value));

        input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                const highlighted = dropdown.querySelector('.tag-option.highlighted');
                const val = highlighted ? highlighted.dataset.tag : input.value.trim().toLowerCase().replace(/[^a-z0-9_\-\.]/g, '-');
                if (val) this._commitTagInput(sectionId, val, wrapper);
            } else if (e.key === 'Escape') {
                this._cancelTagInput(sectionId, wrapper);
            } else if (e.key === 'ArrowDown') {
                this._moveDropdownHighlight(sectionId, 1);
                e.preventDefault();
            } else if (e.key === 'ArrowUp') {
                this._moveDropdownHighlight(sectionId, -1);
                e.preventDefault();
            }
        });

        input.addEventListener('blur', (e) => {
            setTimeout(() => {
                if (!document.getElementById(`tag-input-${sectionId}`)) return;
                const val = input.value.trim().toLowerCase().replace(/[^a-z0-9_\-\.]/g, '-');
                if (val) this._commitTagInput(sectionId, val, wrapper);
                else this._cancelTagInput(sectionId, wrapper);
            }, 150);
        });
    },

    _renderTagDropdown(sectionId, query) {
        const dropdown = document.getElementById(`tag-dropdown-${sectionId}`);
        if (!dropdown) return;
        const allTags = this._tagCache || [];
        const section = this.currentNotebook && this.currentNotebook.sections.find(s => s.id === sectionId);
        const existing = new Set(section ? (section.tags || []) : []);
        const q = (query || '').toLowerCase();
        const matches = allTags.filter(t => !existing.has(t) && (q === '' || t.includes(q)));
        if (matches.length === 0) { dropdown.style.display = 'none'; return; }
        dropdown.innerHTML = matches.map(t => `<div class="tag-option" data-tag="${Utils.escapeHtml(t)}" onmousedown="event.preventDefault();Notebooks._commitTagInput('${sectionId}','${Utils.escapeJs(t)}',document.getElementById('tag-input-${sectionId}').closest('.section-tag-input-wrapper'))">${Utils.escapeHtml(t)}</div>`).join('');
        dropdown.style.display = 'block';
    },

    _moveDropdownHighlight(sectionId, dir) {
        const dropdown = document.getElementById(`tag-dropdown-${sectionId}`);
        if (!dropdown) return;
        const opts = Array.from(dropdown.querySelectorAll('.tag-option'));
        if (!opts.length) return;
        const cur = opts.findIndex(o => o.classList.contains('highlighted'));
        const next = Math.max(0, Math.min(opts.length - 1, cur + dir));
        opts.forEach(o => o.classList.remove('highlighted'));
        opts[next].classList.add('highlighted');
    },

    async _commitTagInput(sectionId, tag, wrapper) {
        if (!tag) return;
        this._cancelTagInput(sectionId, wrapper);
        await this.addTagToSection(sectionId, tag);
    },

    _cancelTagInput(sectionId, wrapper) {
        const tagsArea = document.getElementById(`section-tags-${sectionId}`);
        if (tagsArea && wrapper && wrapper.parentNode === tagsArea) {
            const section = this.currentNotebook && this.currentNotebook.sections.find(s => s.id === sectionId);
            if (section) this._refreshTagsAreaDOM(sectionId, section);
        }
    },

    async addTagToSection(sectionId, tag) {
        tag = tag.trim().toLowerCase().replace(/[^a-z0-9_\-\.]/g, '-');
        if (!tag) return;
        const section = this.currentNotebook && this.currentNotebook.sections.find(s => s.id === sectionId);
        if (!section) return;
        const existing = section.tags || [];
        if (existing.includes(tag)) return;
        const newTags = [...existing, tag];
        await this._saveTagsForSection(sectionId, newTags, section);
    },

    async removeTagFromSection(sectionId, tag) {
        const section = this.currentNotebook && this.currentNotebook.sections.find(s => s.id === sectionId);
        if (!section) return;
        const newTags = (section.tags || []).filter(t => t !== tag);
        await this._saveTagsForSection(sectionId, newTags, section);
    },

    async _saveTagsForSection(sectionId, newTags, section) {
        const notebookId = this.currentNotebook.id;
        try {
            const res = await fetch(`/api/v1/notebooks/${notebookId}/sections/${sectionId}`, {
                method: 'PUT',
                headers: this.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify({ tags: newTags }),
            });
            if (!res.ok) throw new Error('Failed to save tags');
            section.tags = newTags;
            this._tagCache = null; // invalidate cache
            this._refreshTagsAreaDOM(sectionId, section);
            this.updateTagFilterBar();
            this.applyTagFilter();
        } catch (err) {
            console.error('[Notebooks] Failed to save tags:', err);
            if (window.Toast) Toast.show('Failed to save tag', 'error');
        }
    },

    _refreshTagsAreaDOM(sectionId, section) {
        const area = document.getElementById(`section-tags-${sectionId}`);
        if (!area) return;
        const tmp = document.createElement('div');
        tmp.innerHTML = this.renderTagsArea(section);
        const newArea = tmp.firstElementChild;
        area.parentNode.replaceChild(newArea, area);
    },

    // ── Tag filter bar ───────────────────────────────────────────────────────

    updateTagFilterBar() {
        if (!this.currentNotebook) return;
        const allTags = new Set();
        (this.currentNotebook.sections || []).forEach(s => (s.tags || []).forEach(t => allTags.add(t)));
        const bar = document.getElementById('notebookTagFilter');
        if (!bar) return;

        if (allTags.size === 0) {
            bar.style.display = 'none';
            this.activeTagFilters = [];
            return;
        }

        const active = this.activeTagFilters || [];
        const tagChips = Array.from(allTags).sort().map(t => {
            const color = this.tagColorFor(t);
            return `<span class="filter-chip${active.includes(t) ? ' active' : ''}" data-tag="${Utils.escapeHtml(t)}" style="--chip-color:${color}" onclick="Notebooks.toggleTagFilter('${Utils.escapeJs(t)}')">${Utils.escapeHtml(t)}</span>`;
        }).join('');

        const matchCount = this._getFilteredSectionCount();
        const total = (this.currentNotebook.sections || []).length;
        const countHtml = active.length > 0 ? `<span class="filter-count">${matchCount} of ${total} sections</span>` : '';
        const clearHtml = active.length > 0 ? `<button class="filter-clear-btn" onclick="Notebooks.clearTagFilters()">Clear</button>` : '';

        bar.innerHTML = `<span class="filter-label">Filter:</span>${tagChips}${countHtml}${clearHtml}`;
        bar.style.display = 'flex';
    },

    toggleTagFilter(tag) {
        if (!this.activeTagFilters) this.activeTagFilters = [];
        const idx = this.activeTagFilters.indexOf(tag);
        if (idx >= 0) this.activeTagFilters.splice(idx, 1);
        else this.activeTagFilters.push(tag);
        this.updateTagFilterBar();
        this.applyTagFilter();
    },

    clearTagFilters() {
        this.activeTagFilters = [];
        this.updateTagFilterBar();
        this.applyTagFilter();
    },

    _getFilteredSectionCount() {
        if (!this.currentNotebook) return 0;
        const active = this.activeTagFilters || [];
        if (active.length === 0) return (this.currentNotebook.sections || []).length;
        return (this.currentNotebook.sections || []).filter(s =>
            active.every(t => (s.tags || []).includes(t))
        ).length;
    },

    applyTagFilter() {
        const active = this.activeTagFilters || [];
        const container = document.getElementById('notebookSections');
        if (!container) return;
        container.querySelectorAll('.notebook-section').forEach(el => {
            const sectionId = el.dataset.sectionId;
            const section = this.currentNotebook && this.currentNotebook.sections.find(s => s.id === sectionId);
            if (!section) return;
            if (active.length === 0 || active.every(t => (section.tags || []).includes(t))) {
                el.style.display = '';
            } else {
                el.style.display = 'none';
            }
        });
    },

    /**
     * Render section content based on type
     */
    renderSectionContent(section) {
        if (section.section_type === 'markdown') {
            return this.renderMarkdownSection(section);
        } else if (section.section_type === 'query') {
            return this.renderQuerySection(section);
        } else if (section.section_type === 'ai_summary') {
            return this.renderAISummarySection(section);
        } else if (section.section_type === 'comment_context') {
            return this.renderCommentContextSection(section);
        } else if (section.section_type === 'ai_attack_chain') {
            return this.renderAttackChainSection(section);
        }
        return '<p class="section-error">Unknown section type</p>';
    },

    /**
     * Render markdown section
     */
    renderMarkdownSection(section) {
        // Use the same rendering logic as live preview to ensure consistency
        const renderedContent = this.renderMarkdownToHtml(section.content || '');

        return `
            <div class="markdown-section">
                <div class="markdown-preview">
                    ${renderedContent}
                </div>
            </div>
        `;
    },

    /**
     * Render AI summary section
     */
    renderAISummarySection(section) {
        const hasContent = section.content && section.content.trim() && section.content.trim() !== ' ';
        return `
            <div class="ai-summary-section">
                <span id="ai-summary-status-${section.id}" style="color: var(--text-muted); font-size: 0.85rem;"></span>
                <div id="ai-summary-content-${section.id}" style="color: var(--text-primary); line-height: 1.6; font-size: 0.95rem;">
                    ${hasContent ? Utils.escapeHtml(section.content) : '<span style="color: var(--text-muted); font-style: italic;">Press play to generate an AI summary of this notebook.</span>'}
                </div>
            </div>
        `;
    },

    /**
     * Render AI Attack Chain section with MITRE ATT&CK tactics
     */
    renderAttackChainSection(section) {
        const hasContent = section.content && section.content.trim() && section.content.trim() !== ' ';
        if (!hasContent) {
            return `
                <div class="attack-chain-section">
                    <span id="ai-summary-status-${section.id}" style="color: var(--text-muted); font-size: 0.85rem;"></span>
                    <div id="ai-summary-content-${section.id}" style="color: var(--text-muted); font-style: italic; font-size: 0.95rem;">
                        Press play to generate an AI Attack Chain summary.
                    </div>
                </div>
            `;
        }

        let data = {};
        try {
            data = JSON.parse(section.content);
        } catch {
            return `
                <div class="attack-chain-section">
                    <span id="ai-summary-status-${section.id}" style="color: var(--text-muted); font-size: 0.85rem;"></span>
                    <div id="ai-summary-content-${section.id}" style="color: var(--text-primary); line-height: 1.6; font-size: 0.95rem;">
                        ${Utils.escapeHtml(section.content)}
                    </div>
                </div>
            `;
        }

        const allTactics = [
            'Reconnaissance', 'Resource Development', 'Initial Access', 'Execution',
            'Persistence', 'Privilege Escalation', 'Defense Evasion', 'Credential Access',
            'Discovery', 'Lateral Movement', 'Collection', 'Command and Control',
            'Exfiltration', 'Impact'
        ];

        // Build lookup from tactic name to findings
        const tacticFindings = {};
        if (data.tactics && Array.isArray(data.tactics)) {
            for (const t of data.tactics) {
                if (t.tactic && t.findings) {
                    tacticFindings[t.tactic] = t.findings;
                }
            }
        }

        let tacticsHtml = '';
        for (const tactic of allTactics) {
            const findings = tacticFindings[tactic] || [];
            const count = findings.length;
            const isEmpty = count === 0;
            const badge = `<span class="attack-chain-badge${isEmpty ? ' empty' : ''}">${count}</span>`;

            let findingsHtml = '';
            if (isEmpty) {
                findingsHtml = '<p class="attack-chain-no-findings">No related findings</p>';
            } else {
                findingsHtml = '<ul class="attack-chain-findings">';
                for (const f of findings) {
                    const desc = Utils.escapeHtml(f.description || '');
                    if (f.section_id) {
                        findingsHtml += `<li class="attack-chain-finding"><a class="attack-chain-link" onclick="Notebooks.scrollToSection('${Utils.escapeJs(f.section_id)}')">${desc}</a></li>`;
                    } else {
                        findingsHtml += `<li class="attack-chain-finding">${desc}</li>`;
                    }
                }
                findingsHtml += '</ul>';
            }

            tacticsHtml += `
                <details class="attack-chain-tactic${isEmpty ? ' empty' : ''}">
                    <summary>${Utils.escapeHtml(tactic)} ${badge}</summary>
                    ${findingsHtml}
                </details>
            `;
        }

        return `
            <div class="attack-chain-section">
                <span id="ai-summary-status-${section.id}" style="color: var(--text-muted); font-size: 0.85rem;"></span>
                <div id="ai-summary-content-${section.id}">
                    <div class="attack-chain-executive">${Utils.escapeHtml(data.executive_summary || '')}</div>
                    <div class="attack-chain-tactics">${tacticsHtml}</div>
                </div>
            </div>
        `;
    },

    /**
     * Scroll to a section by its ID
     */
    scrollToSection(sectionId) {
        const el = document.querySelector(`[data-section-id="${sectionId}"]`);
        if (el) {
            this._smoothScrollToElement(el);
            el.style.outline = '2px solid var(--accent-primary)';
            setTimeout(() => { el.style.outline = ''; }, 2000);
        }
    },

    _smoothScrollToElement(el, duration = 325) {
        const rect = el.getBoundingClientRect();
        const targetY = window.scrollY + rect.top - Math.max(0, (window.innerHeight - rect.height) / 2);
        const startY = window.scrollY;
        const diff = targetY - startY;
        if (Math.abs(diff) < 1) return;
        const startTime = performance.now();
        const step = (now) => {
            const elapsed = now - startTime;
            const t = Math.min(elapsed / duration, 1);
            const ease = 1 - Math.pow(1 - t, 3);
            window.scrollTo(0, startY + diff * ease);
            if (t < 1) requestAnimationFrame(step);
        };
        requestAnimationFrame(step);
    },

    /**
     * Render comment context section (auto-generated from comments)
     */
    renderCommentContextSection(section) {
        let data = {};
        try {
            data = JSON.parse(section.content || '{}');
        } catch (e) {
            return '<div class="section-error">Invalid comment context data</div>';
        }

        const commentText = Utils.renderCommentMarkdown(data.comment_text || '');
        const query = data.query || '';
        const logId = data.log_id || '';

        let queryHtml = '';
        if (query) {
            queryHtml = `
                <div class="comment-context-query">
                    <pre class="query-display" style="background: var(--bg-tertiary); padding: 8px 12px; border-radius: 4px; font-family: var(--font-mono); font-size: 0.85rem; overflow-x: auto; line-height: 1.4; margin: 0;">${this.highlightQuerySyntax(query)}</pre>
                </div>
            `;
        }

        // Matching log results (prefetched or fetched via play button) - collapsible
        let logResultsHtml = '';
        if (logId) {
            const hasResults = section.last_results && section.last_results !== 'null' && section.last_results !== '';
            if (hasResults) {
                try {
                    let results = section.last_results;
                    if (typeof results === 'string') {
                        if (/^[A-Za-z0-9+/=]+$/.test(results)) {
                            try { results = atob(results); } catch (e) { /* use as-is */ }
                        }
                        results = JSON.parse(results);
                    }
                    if (results && results.results && results.results.length > 0) {
                        const sectionConfig = this.parseSectionChartConfig(section);
                        const tableHtml = this.renderResultsTable(results.results, results, sectionConfig);
                        logResultsHtml = `
                            <details class="comment-context-logid-details" style="margin-top: 8px;">
                                <summary style="cursor: pointer; color: var(--text-muted); font-size: 0.8rem; padding: 4px 0; user-select: none;">Log details</summary>
                                <div class="comment-context-logid" style="margin-top: 4px;"><div class="query-results-container">${tableHtml}</div></div>
                            </details>`;
                    }
                } catch (e) {
                    console.error('[Notebooks] Error parsing comment context results:', e);
                }
            }
        }

        return `
            <div class="comment-context-section">
                <div class="comment-context-body">${commentText}</div>
                ${queryHtml}
                ${logResultsHtml}
            </div>
        `;
    },

    /**
     * Execute a comment_context section's log_id query
     */
    async executeCommentContextSection(sectionId, evt) {
        const button = evt ? evt.target.closest('button') : document.querySelector(`button[onclick*="executeCommentContextSection('${sectionId}')"]`);
        try {
            if (button) {
                button.innerHTML = '<span class="spinner"></span>';
                button.disabled = true;
            }

            const section = this.currentNotebook.sections.find(s => s.id === sectionId);
            if (!section || section.section_type !== 'comment_context') {
                throw new Error('Comment context section not found');
            }

            const data = JSON.parse(section.content || '{}');
            if (!data.log_id) {
                throw new Error('No log_id in this comment');
            }

            const query = 'log_id="' + data.log_id + '"';
            // Use a wide time range to find the log regardless of when it was ingested
            const now = new Date();
            const fiveYearsAgo = new Date(now.getTime() - 5 * 365 * 24 * 60 * 60 * 1000);
            const requestBody = {
                query: query,
                query_type: 'bql',
                start: fiveYearsAgo.toISOString(),
                end: now.toISOString(),
                max_results: 1
            };

            if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
                requestBody.fractal_id = window.FractalContext.currentFractal.id;
            }

            const response = await fetch('/api/v1/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(requestBody)
            });

            const result = await response.json();
            if (!result.success) {
                throw new Error(result.error || 'Query execution failed');
            }

            section.last_executed_at = new Date().toISOString();
            const resultData = {
                results: result.results || [],
                count: (result.results || []).length,
                execution_ms: result.execution_time_ms || 0,
                chart_type: 'table',
                field_order: result.field_order || [],
                is_aggregated: false
            };
            section.last_results = JSON.stringify(resultData);

            await this.updateSectionResults(sectionId, section.last_executed_at, section.last_results);

            const contentContainer = document.getElementById(`section-content-${sectionId}`);
            if (contentContainer) {
                contentContainer.innerHTML = this.renderSectionContent(section);
                contentContainer.dataset.rendered = 'true';
            }
        } catch (error) {
            console.error('[Notebooks] Error executing comment context query:', error);
            this.showError(`Failed to fetch log: ${error.message}`);
        } finally {
            if (button) {
                button.innerHTML = '▶';
                button.disabled = false;
            }
        }
    },

    /**
     * Open a log_id query in the search view
     */
    openLogIdInSearch(logId) {
        const query = 'log_id="' + logId + '"';
        this._openInSearch(query);
    },

    /**
     * Open a query in the search view (reads from data-query attribute to avoid quoting issues)
     */
    openQueryInSearch(el) {
        const query = el.dataset.query;
        if (query) this._openInSearch(query);
    },

    _openInSearch(query) {
        const input = document.getElementById('queryInput');
        if (input) input.value = query;
        if (window.SyntaxHighlight) SyntaxHighlight.updateHighlight('queryInput', 'queryHighlight');
        if (window.App) App.showFractalViewTab('search');
        if (window.QueryExecutor) setTimeout(() => QueryExecutor.execute(), 100);
    },

    /**
     * Generate AI summary for a section
     */
    async generateAISummary(sectionId) {
        if (!this.currentNotebook) return;

        const btn = document.getElementById(`ai-summary-btn-${sectionId}`);
        const status = document.getElementById(`ai-summary-status-${sectionId}`);
        const contentDiv = document.getElementById(`ai-summary-content-${sectionId}`);

        if (btn) {
            btn.disabled = true;
            btn.style.opacity = '0.5';
        }
        if (status) status.textContent = 'Generating...';

        try {
            const response = await fetch(
                `/api/v1/notebooks/${this.currentNotebook.id}/sections/${sectionId}/summarize`,
                {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
                    credentials: 'include'
                }
            );

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to generate summary');
            }

            const summary = result.data.summary;

            const section = this.currentNotebook.sections.find(s => s.id === sectionId);
            if (section) {
                section.content = summary;
            }

            if (section && section.section_type === 'ai_attack_chain') {
                // Re-render the entire section content for attack chain
                const sectionContentEl = document.getElementById(`section-content-${sectionId}`);
                if (sectionContentEl) {
                    sectionContentEl.innerHTML = this.renderAttackChainSection(section);
                    sectionContentEl.dataset.rendered = 'true';
                }
            } else {
                if (contentDiv) {
                    contentDiv.textContent = summary;
                }
            }
            if (status) status.textContent = '';
        } catch (error) {
            console.error('[Notebooks] AI summary error:', error);
            if (status) status.textContent = error.message;
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.style.opacity = '1';
            }
        }
    },

    /**
     * Render query section
     */
    renderQuerySection(section) {

        const hasResults = section.last_results && section.last_results !== 'null' && section.last_results !== '';
        let results = null;

        if (hasResults) {
            try {
                results = section.last_results;
                if (typeof results === 'string') {
                    if (/^[A-Za-z0-9+/=]+$/.test(results)) {
                        try { results = atob(results); } catch (e) { /* use as-is */ }
                    }
                    results = JSON.parse(results);
                }
            } catch (error) {
                console.error('[Notebooks] Error parsing last_results:', error);
            }
        }

        return `
            <div class="query-section">
                <div class="query-display">
                    <pre class="query-display" style="background: var(--bg-tertiary); padding: 12px; border-radius: 6px; font-family: var(--font-mono); font-size: 0.9rem; overflow-x: auto; line-height: 1.5;">${this.highlightQuerySyntax(section.content)}</pre>
                </div>
                <div class="query-controls">
                    <div class="query-info">
                        ${section.last_executed_at ?
                            `Last executed: ${this.formatRelativeTime(section.last_executed_at)}` :
                            'Not executed yet'
                        }
                        ${section.modified_since_execution ?
                            '<span style="color: var(--warning); font-size: 0.8rem; margin-left: 8px;">• Modified</span>' :
                            ''
                        }
                    </div>
                </div>
                ${hasResults ? this.renderQueryResults(results, section) : ''}
            </div>
        `;
    },

    /**
     * Render query results
     */
    renderQueryResults(results, section) {
        if (!results) {
            return '<div class="query-error">No results available</div>';
        }

        if (!results.results) {
            return '<div class="query-error">No results available</div>';
        }

        if (results.error) {
            return `<div class="query-error">Query Error: ${Utils.escapeHtml(results.error)}</div>`;
        }

        const resultCount = results.count || results.results.length;
        const executionTime = results.execution_ms || 0;
        const chartType = results.chart_type || 'table';

        // Get section chart_config for row coloring
        const sectionConfig = this.parseSectionChartConfig(section);

        let resultsContent = '';

        // Show ONLY chart OR table, not both
        if (chartType !== 'table' && results.results.length > 0) {
            resultsContent += this.renderQueryChart(results, sectionConfig);
        } else {
            resultsContent += this.renderResultsTable(results.results, results, sectionConfig);
        }

        // For charts, use a more compact layout
        if (chartType !== 'table') {
            return `
                <div class="query-results">
                    <div class="query-meta-compact" style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0; margin-bottom: 10px; font-size: 0.85rem; color: var(--text-secondary);">
                        <span>${resultCount} rows in ${executionTime}ms • Chart: ${chartType}</span>
                    </div>
                    <div class="query-results-container" style="padding: 0;">
                        ${resultsContent}
                    </div>
                </div>
            `;
        }

        // For tables, use the full header
        return `
            <div class="query-results">
                <div class="query-results-header">
                    <h4>Query Results</h4>
                    <div class="query-meta">
                        ${resultCount} rows in ${executionTime}ms
                    </div>
                </div>
                <div class="query-results-container">
                    ${resultsContent}
                </div>
            </div>
        `;
    },

    /**
     * Render query chart
     */
    renderQueryChart(results, sectionConfig) {
        const chartType = results.chart_type || 'table';
        const chartId = `chart-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

        if (chartType === 'table' || !results.results || results.results.length === 0) {
            return '';
        }

        if (chartType === 'singleval') {
            return this.renderSingleValSection(results, sectionConfig);
        }

        if (chartType === 'graph') {
            const graphId = `graph-${chartId}`;
            const graphHtml = `
                <div class="chart-container" style="margin: 0; border: 1px solid var(--border-color); border-radius: 6px; padding: 10px; background: var(--bg-secondary);">
                    <div id="${graphId}" style="width: 100%; height: 400px;"></div>
                </div>
            `;
            setTimeout(() => {
                this.renderGraphInNotebook(graphId, results);
            }, 500);
            return graphHtml;
        }

        if (chartType === 'heatmap') {
            const heatmapId = `heatmap-${chartId}`;
            const heatmapHtml = `
                <div class="chart-container" style="margin: 0; border: 1px solid var(--border-color); border-radius: 6px; padding: 10px; background: var(--bg-secondary);">
                    <div id="${heatmapId}" style="width: 100%; overflow: auto;"></div>
                </div>
            `;
            setTimeout(() => {
                this.renderHeatmapInNotebook(heatmapId, results);
            }, 500);
            return heatmapHtml;
        }

        if (chartType === 'worldmap') {
            const mapId = `map-${chartId}`;
            const mapHtml = `
                <div class="chart-container" style="margin: 0; border: 1px solid var(--border-color); border-radius: 6px; padding: 10px; background: var(--bg-secondary);">
                    <div id="${mapId}" class="worldmap-container" style="height: 500px;"></div>
                </div>
            `;
            setTimeout(() => {
                const el = document.getElementById(mapId);
                if (el && window.BifractWorldMap) {
                    const cfg = results.chart_config || {};
                    BifractWorldMap.render(el, results.results || [], {
                        latField: cfg.latField || 'latitude',
                        lonField: cfg.lonField || 'longitude',
                        labelField: cfg.labelField || null
                    });
                }
            }, 500);
            return mapHtml;
        }

        const chartHtml = `
            <div class="chart-container" style="margin: 0; border: 1px solid var(--border-color); border-radius: 6px; padding: 10px; background: var(--bg-secondary);">
                <canvas id="${chartId}" style="background: transparent; border-radius: 4px;"></canvas>
            </div>
        `;

        setTimeout(() => {
            this.renderChartOnCanvas(chartId, results, sectionConfig);
        }, 500);

        return chartHtml;
    },

    // Merge query-time config (limit/span) with user formatting (colors/unit/etc).
    mergeChartConfig(results, sectionConfig) {
        return Object.assign({}, results.chart_config || {}, sectionConfig || {});
    },

    renderGraphInNotebook(containerId, results) {
        const container = document.getElementById(containerId);
        if (!container) return;
        BifractCharts.renderGraphSimple(container, {
            data: results.results || [],
            fields: results.field_order,
            config: results.chart_config || {}
        });
    },

    renderChartOnCanvas(chartId, results, sectionConfig) {
        const canvas = document.getElementById(chartId);
        if (!canvas) return;

        try {
            BifractCharts.renderOnCanvas(canvas, results.chart_type, {
                data: results.results,
                fields: results.field_order,
                config: this.mergeChartConfig(results, sectionConfig),
                maintainAspectRatio: true
            });
        } catch (error) {
            console.error('[Notebooks] Error rendering chart:', error);
        }
    },

    // Chart rendering delegated to BifractCharts

    renderSingleValSection(results, sectionConfig) {
        return BifractCharts.renderSingleVal(null, {
            data: results.results,
            fields: results.field_order,
            config: this.mergeChartConfig(results, sectionConfig),
            coloringRules: (sectionConfig && sectionConfig.row_coloring_rules) || [],
            returnHtml: true
        });
    },

    // renderTimeChart - removed, delegated via renderChartOnCanvas -> BifractCharts

    renderHistogramChart(canvas, data, results) {
        BifractCharts.renderHistogram(canvas, {
            data: data,
            config: results.chart_config || {},
            maintainAspectRatio: true
        });
    },

    renderHeatmapInNotebook(containerId, results) {
        const container = document.getElementById(containerId);
        if (!container) return;
        BifractCharts.renderHeatmap(container, {
            data: results.results || [],
            config: results.chart_config || {}
        });
    },

    /**
     * Render results table
     */
    renderResultsTable(results, resultMetadata, sectionConfig) {
        if (!results || results.length === 0) {
            return '<div style="padding: 20px; text-align: center; color: var(--text-muted);">No results</div>';
        }

        // Check if there are specific table columns specified (from table() function)
        let headers;

        // Try different possible field names for table columns
        const tableColumns = resultMetadata?.table_columns ||
                           resultMetadata?.columns ||
                           resultMetadata?.table_fields ||
                           resultMetadata?.selected_fields;

        if (tableColumns && tableColumns.length > 0) {
            // Use ONLY the specified columns in the specified order
            headers = tableColumns;
        } else {
            // Fall back to all columns, but filter out system fields
            const systemFields = ['_all_fields', 'raw_log', 'log_id'];
            headers = Object.keys(results[0]).filter(header => !systemFields.includes(header));
        }

        const rules = (sectionConfig && sectionConfig.row_coloring_rules) || [];

        return `
            <table class="results-table" style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr>
                        ${headers.map(header => `<th style="padding: 8px; border-bottom: 1px solid var(--border-color); text-align: left; background: var(--bg-tertiary);">${Utils.escapeHtml(header)}</th>`).join('')}
                    </tr>
                </thead>
                <tbody>
                    ${results.slice(0, 100).map(row => {
                        const rowStyle = this.getRowHighlightStyle(row, rules);
                        return `<tr style="${rowStyle}">
                            ${headers.map(header => {
                                const value = row[header];
                                const displayValue = typeof value === 'object' ? JSON.stringify(value) : String(value);
                                const cellStyle = this.getCellHighlightStyle(row, header, rules);
                                return `<td style="padding: 8px; border-bottom: 1px solid var(--border-color); font-size: 0.85rem;${cellStyle}">${Utils.escapeHtml(displayValue)}</td>`;
                            }).join('')}
                        </tr>`;
                    }).join('')}
                </tbody>
            </table>
            ${results.length > 100 ? '<div style="padding: 10px; text-align: center; color: var(--text-muted); font-size: 0.8rem;">Showing first 100 results</div>' : ''}
        `;
    },

    evaluateRule(cellVal, rule) {
        if (cellVal === undefined || cellVal === null) return false;
        const op = rule.operator || '=';
        const ruleVal = rule.value;
        if (op === 'contains') {
            return String(cellVal).toLowerCase().includes(String(ruleVal).toLowerCase());
        }
        if (op === '>' || op === '>=' || op === '<' || op === '<=') {
            const numCell = parseFloat(cellVal);
            const numRule = parseFloat(ruleVal);
            if (isNaN(numCell) || isNaN(numRule)) return false;
            if (op === '>') return numCell > numRule;
            if (op === '>=') return numCell >= numRule;
            if (op === '<') return numCell < numRule;
            return numCell <= numRule;
        }
        return String(cellVal) === String(ruleVal);
    },

    getRowHighlightStyle(row, rules) {
        if (!rules || rules.length === 0) return '';
        for (const rule of rules) {
            if (!rule.column) continue;
            if ((rule.target || 'row') !== 'row') continue;
            const cellVal = row[rule.column];
            if (this.evaluateRule(cellVal, rule)) {
                const color = rule.color || '#8b5cf6';
                return `background-color: ${color}26;`;
            }
        }
        return '';
    },

    getCellHighlightStyle(row, column, rules) {
        if (!rules || rules.length === 0) return '';
        for (const rule of rules) {
            if (!rule.column || rule.column !== column) continue;
            if ((rule.target || 'row') !== 'cell') continue;
            const cellVal = row[rule.column];
            if (this.evaluateRule(cellVal, rule)) {
                const color = rule.color || '#8b5cf6';
                return `background-color: ${color}26;`;
            }
        }
        return '';
    },

    parseSectionChartConfig(section) {
        if (!section || !section.chart_config) return {};
        const config = section.chart_config;
        if (typeof config === 'string') {
            try { return JSON.parse(config); } catch { return {}; }
        }
        return config;
    },

    /**
     * Run all executable sections in batches of 2
     */
    sendNotebookToChat() {
        if (!this.currentNotebook) return;
        if (window.Chat) {
            Chat.analyzeNotebook(this.currentNotebook);
        } else {
            this.showError('Chat is not available');
        }
    },

    async runAllSections() {
        if (!this.currentNotebook || !this.currentNotebook.sections) return;

        const executableSections = this.currentNotebook.sections.filter(s =>
            s.section_type === 'query' || s.section_type === 'comment_context'
        );

        if (executableSections.length === 0) {
            this.showError('No executable sections found');
            return;
        }

        const btn = document.getElementById('runAllSectionsBtn');
        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Running...';
        }

        const batchSize = 2;
        let completed = 0;
        let failed = 0;

        try {
            for (let i = 0; i < executableSections.length; i += batchSize) {
                const batch = executableSections.slice(i, i + batchSize);
                const promises = batch.map(section => {
                    if (section.section_type === 'query') {
                        return this._executeQuerySectionSilent(section.id).catch(() => { failed++; });
                    } else if (section.section_type === 'comment_context') {
                        return this.executeCommentContextSection(section.id, null).catch(() => { failed++; });
                    }
                });
                await Promise.all(promises);
                completed += batch.length;
                if (btn) btn.textContent = `Running (${completed}/${executableSections.length})...`;
            }

            if (failed > 0) {
                this.showError(`Completed with ${failed} error(s)`);
            } else {
                this.showSuccess(`All ${completed} sections executed`);
            }
        } catch (error) {
            console.error('[Notebooks] Error in runAllSections:', error);
            this.showError('Run all failed: ' + error.message);
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.textContent = 'Run All';
            }
        }
    },

    /**
     * Execute a query section without UI button state management (for batch runs)
     */
    async _executeQuerySectionSilent(sectionId) {
        const section = this.currentNotebook.sections.find(s => s.id === sectionId);
        if (!section || section.section_type !== 'query') {
            throw new Error('Query section not found');
        }

        const timeRange = this.getNotebookTimeRange();
        let query = section.content;
        if (this.currentNotebook.variables && this.currentNotebook.variables.length > 0) {
            for (const v of this.currentNotebook.variables) {
                if (v.name) query = query.replaceAll('@' + v.name, v.value || '*');
            }
        }
        const requestBody = {
            query: query,
            query_type: 'bql',
            start: timeRange.start,
            end: timeRange.end,
            max_results: this.currentNotebook.max_results_per_section || 1000
        };

        if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
            requestBody.fractal_id = window.FractalContext.currentFractal.id;
        }

        const response = await fetch('/api/v1/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify(requestBody)
        });

        const data = await response.json();
        if (!data.success) {
            throw new Error(data.error || 'Query execution failed');
        }

        section.last_executed_at = new Date().toISOString();
        section.modified_since_execution = false;
        const resultData = {
            results: data.results || [],
            count: (data.results || []).length,
            execution_ms: data.execution_time_ms || 0,
            sql: data.sql || '',
            chart_type: data.chart_type || 'table',
            chart_config: data.chart_config || {},
            field_order: data.field_order || [],
            is_aggregated: data.is_aggregated || false
        };
        section.last_results = JSON.stringify(resultData);

        await this.updateSectionResults(sectionId, section.last_executed_at, section.last_results);

        const contentContainer = document.getElementById(`section-content-${sectionId}`);
        if (contentContainer) {
            contentContainer.innerHTML = this.renderSectionContent(section);
            contentContainer.dataset.rendered = 'true';
        }
    },

    /**
     * Execute query section
     */
    async executeQuerySection(sectionId) {
        try {
            const button = event.target;
            const originalContent = button.innerHTML;
            button.innerHTML = '<span class="spinner"></span>';
            button.disabled = true;


            // Find the section to get the query
            const section = this.currentNotebook.sections.find(s => s.id === sectionId);
            if (!section || section.section_type !== 'query') {
                throw new Error('Query section not found');
            }

            // Prepare query execution using notebook's time range settings
            const timeRange = this.getNotebookTimeRange();
            let query = section.content;
            if (this.currentNotebook.variables && this.currentNotebook.variables.length > 0) {
                for (const v of this.currentNotebook.variables) {
                    if (v.name) query = query.replaceAll('@' + v.name, v.value || '*');
                }
            }
            const requestBody = {
                query: query,
                query_type: 'bql',
                start: timeRange.start,
                end: timeRange.end,
                max_results: this.currentNotebook.max_results_per_section || 1000
            };

            // Include fractal context (skip for prisms - server uses session)
            if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
                requestBody.fractal_id = window.FractalContext.currentFractal.id;
            }


            // Execute query using main query API
            const response = await fetch('/api/v1/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(requestBody)
            });


            const data = await response.json();

            if (!data.success) {
                throw new Error(data.error || 'Query execution failed');
            }


            // Update section with results
            section.last_executed_at = new Date().toISOString();

            // Clear the modified flag since we just executed the current content
            section.modified_since_execution = false;

            const resultData = {
                results: data.results || [],
                count: (data.results || []).length,
                execution_ms: data.execution_time_ms || 0,
                sql: data.sql || '',
                chart_type: data.chart_type || 'table',
                chart_config: data.chart_config || {},
                field_order: data.field_order || [],
                is_aggregated: data.is_aggregated || false
            };
            section.last_results = JSON.stringify(resultData);

            //     sectionId,
            //     resultCount: resultData.count,
            //     executionTime: resultData.execution_ms,
            //     chartType: resultData.chart_type,
            //     hasResults: !!resultData.results && resultData.results.length > 0
            // });

            // Update the backend with new results
            await this.updateSectionResults(sectionId, section.last_executed_at, section.last_results);

            // Re-render just this section to show results
            const sectionContainer = document.querySelector(`[data-section-id="${sectionId}"]`);
            const contentContainer = document.getElementById(`section-content-${sectionId}`);

            if (sectionContainer && contentContainer) {
                const newContent = this.renderSectionContent(section);
                contentContainer.innerHTML = newContent;
                contentContainer.dataset.rendered = 'true';
            } else {
                console.error('[Notebooks] Could not find containers to re-render section');
            }

            this.showSuccess('Query executed successfully!');

        } catch (error) {
            console.error('[Notebooks] Error executing query:', error);
            this.showError(`Query execution failed: ${error.message}`);
        } finally {
            // Re-enable buttons (both in section header and query controls)
            setTimeout(() => {
                const buttons = document.querySelectorAll(`button[onclick="Notebooks.executeQuerySection('${sectionId}')"]`);
                buttons.forEach(button => {
                    button.innerHTML = '▶';
                    button.disabled = false;
                });
            }, 100);
        }
    },

    // =====================
    // Row Coloring Panel
    // =====================

    openFormatPanel(sectionId) {
        const section = this.currentNotebook && this.currentNotebook.sections
            ? this.currentNotebook.sections.find(s => s.id === sectionId) : null;
        if (!section) return;

        let cached = {};
        if (section.last_results) {
            try {
                let raw = section.last_results;
                if (typeof raw === 'string') {
                    if (/^[A-Za-z0-9+/=]+$/.test(raw)) {
                        try { raw = atob(raw); } catch (e) { /* use as-is */ }
                    }
                    raw = JSON.parse(raw);
                }
                cached = raw || {};
            } catch (e) { cached = {}; }
        }
        const chartType = (cached && cached.chart_type) || 'table';
        const original = JSON.parse(JSON.stringify(this.parseSectionChartConfig(section) || {}));

        BifractFormat.open({
            chartType,
            config: this.parseSectionChartConfig(section),
            fields: cached.field_order || [],
            results: cached.results || [],
            onPreview: (cfg) => { section.chart_config = cfg; this.rerenderSectionContent(section); },
            onCancel: () => { section.chart_config = original; this.rerenderSectionContent(section); },
            onSave: (cfg) => this.saveSectionFormat(sectionId, cfg)
        });
    },

    rerenderSectionContent(section) {
        const contentEl = document.getElementById(`section-content-${section.id}`);
        if (contentEl) {
            contentEl.innerHTML = this.renderSectionContent(section);
            contentEl.dataset.rendered = 'true';
        }
    },

    async saveSectionFormat(sectionId, cfg) {
        const section = this.currentNotebook && this.currentNotebook.sections
            ? this.currentNotebook.sections.find(s => s.id === sectionId) : null;
        if (!section) return;
        try {
            const response = await fetch(`/api/v1/notebooks/${this.currentNotebook.id}/sections/${sectionId}`, {
                method: 'PUT',
                headers: this.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify({ chart_config: cfg })
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to save');

            section.chart_config = cfg;
            this.rerenderSectionContent(section);
            if (window.Toast) Toast.show('Formatting saved', 'success');
        } catch (err) {
            console.error('[Notebooks] Failed to save formatting:', err);
            if (window.Toast) Toast.show('Failed to save formatting', 'error');
        }
    },

    /**
     * Get time range for notebook queries based on notebook settings
     */
    getNotebookTimeRange() {
        if (!this.currentNotebook) {
            // Default to last 24 hours
            return {
                start: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
                end: new Date().toISOString()
            };
        }

        const now = new Date();
        let start, end = now;

        switch (this.currentNotebook.time_range_type) {
            case '1h':
                start = new Date(now - 60 * 60 * 1000);
                break;
            case '24h':
                start = new Date(now - 24 * 60 * 60 * 1000);
                break;
            case '7d':
                start = new Date(now - 7 * 24 * 60 * 60 * 1000);
                break;
            case '30d':
                start = new Date(now - 30 * 24 * 60 * 60 * 1000);
                break;
            case 'all':
                start = new Date('2000-01-01T00:00:00Z');
                break;
            case 'custom':
                if (this.currentNotebook.time_range_start && this.currentNotebook.time_range_end) {
                    start = new Date(this.currentNotebook.time_range_start);
                    end = new Date(this.currentNotebook.time_range_end);
                } else {
                    // Fallback to 24h if custom range is incomplete
                    start = new Date(now - 24 * 60 * 60 * 1000);
                }
                break;
            default:
                start = new Date(now - 24 * 60 * 60 * 1000);
                break;
        }

        return {
            start: start.toISOString(),
            end: end.toISOString()
        };
    },

    /**
     * Update section results in the backend
     */
    async updateSectionResults(sectionId, lastExecutedAt, lastResults) {
        try {
            const response = await fetch(`/api/v1/notebooks/${this.currentNotebook.id}/sections/${sectionId}/results`, {
                method: 'PUT',
                headers: this.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify({
                    last_executed_at: lastExecutedAt,
                    last_results: lastResults
                })
            });

            if (!response.ok) {
                // console.warn('[Notebooks] Failed to update section results in backend:', response.status);
            }
        } catch (error) {
            // console.warn('[Notebooks] Error updating section results:', error);
        }
    },

    /**
     * Connect SSE for live updates and presence
     */
    connectSSE() {
        if (!this.currentNotebook || this.eventSource) return;

        // Immediate presence update and fetch
        fetch(`/api/v1/notebooks/${this.currentNotebook.id}/presence`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({})
        }).catch(() => {});
        this.onPresenceChanged();

        this.eventSource = new EventSource(
            `/api/v1/notebooks/${this.currentNotebook.id}/events`,
            { withCredentials: true }
        );

        this.eventSource.onmessage = (e) => {
            try {
                const event = JSON.parse(e.data);
                this.handleSSEEvent(event);
            } catch (err) {
                // Ignore parse errors (e.g. keepalive comments)
            }
        };

        this.eventSource.onerror = () => {
            // EventSource auto-reconnects. No action needed.
        };

        // Lightweight DB heartbeat so presence table stays fresh (must be
        // shorter than the 30s DB expiry window)
        this.presenceInterval = setInterval(() => {
            if (this.currentNotebook) {
                fetch(`/api/v1/notebooks/${this.currentNotebook.id}/presence`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({})
                }).catch(() => {});
            }
        }, 15000);
    },

    /**
     * Disconnect SSE
     */
    disconnectSSE() {
        if (this.eventSource) {
            this.eventSource.close();
            this.eventSource = null;
            this.sseClientId = null;
        }
        if (this.presenceInterval) {
            clearInterval(this.presenceInterval);
            this.presenceInterval = null;
        }
    },

    // Keep old names as aliases for any callers
    startPresenceTracking() { this.connectSSE(); },
    stopPresenceTracking() { this.disconnectSSE(); },

    /**
     * Get SSE headers for mutation requests
     */
    sseHeaders() {
        const headers = { 'Content-Type': 'application/json' };
        if (this.sseClientId) {
            headers['X-SSE-Client-ID'] = this.sseClientId;
        }
        return headers;
    },

    /**
     * Handle incoming SSE event
     */
    handleSSEEvent(event) {
        switch (event.type) {
            case 'connected':
                this.sseClientId = event.data.client_id;
                break;
            case 'section_added':
                this.onRemoteSectionAdded(event.data);
                break;
            case 'section_removed':
                this.onRemoteSectionRemoved(event.data);
                break;
            case 'section_updated':
                this.onRemoteSectionUpdated(event.data);
                break;
            case 'section_results_updated':
                this.onRemoteSectionResultsUpdated(event.data);
                break;
            case 'sections_reordered':
                this.onRemoteSectionsReordered(event.data);
                break;
            case 'presence_joined':
            case 'presence_left':
                this.onPresenceChanged();
                break;
        }
    },

    /**
     * Handle remote section added
     */
    onRemoteSectionAdded(section) {
        if (!this.currentNotebook) return;
        if (!this.currentNotebook.sections) {
            this.currentNotebook.sections = [];
        }

        // Avoid duplicate if we already have this section
        if (this.currentNotebook.sections.find(s => s.id === section.id)) return;

        this.currentNotebook.sections.push(section);

        // Insert DOM element at correct position instead of full re-render
        const container = document.getElementById('notebookSections');
        if (!container) return;

        // Remove empty state if present
        const emptyState = container.querySelector('.notebook-empty');
        if (emptyState) emptyState.remove();

        const sectionHtml = this.renderSection(section);
        const temp = document.createElement('div');
        temp.innerHTML = sectionHtml;
        const newEl = temp.firstElementChild;

        // Find insertion point based on order_index
        const sorted = this.currentNotebook.sections.sort((a, b) => a.order_index - b.order_index);
        const idx = sorted.findIndex(s => s.id === section.id);
        const existingSections = container.querySelectorAll('.notebook-section');

        if (idx >= 0 && idx < existingSections.length) {
            container.insertBefore(newEl, existingSections[idx]);
        } else {
            container.appendChild(newEl);
        }

        // Brief highlight animation
        newEl.style.transition = 'background-color 0.5s ease';
        newEl.style.backgroundColor = 'var(--accent-primary-transparent, rgba(156, 106, 222, 0.1))';
        setTimeout(() => { newEl.style.backgroundColor = ''; }, 1500);

        this.bindSectionEvents();
        this.updateTagFilterBar();
        this.applyTagFilter();
        this.renderTOCList();
        if (this._tocObserver) this._tocObserver.observe(newEl);
    },

    /**
     * Handle remote section removed
     */
    onRemoteSectionRemoved(data) {
        if (!this.currentNotebook) return;

        const sectionId = data.id;
        const el = document.querySelector(`[data-section-id="${sectionId}"]`);

        // If user is editing this section, warn them
        if (el && el.classList.contains('editing')) {
            this.showError('This section was deleted by another user');
            this.cancelEditSection(sectionId);
        }

        this.currentNotebook.sections = this.currentNotebook.sections.filter(s => s.id !== sectionId);
        if (el) el.remove();
        this._tocVisibleSections.delete(sectionId);

        this.updateTagFilterBar();
        this.renderTOCList();

        // Show empty state if no sections left
        if (this.currentNotebook.sections.length === 0) {
            const container = document.getElementById('notebookSections');
            if (container) {
                container.innerHTML = `
                    <div class="notebook-empty">
                        <p style="text-align: center; color: var(--text-muted); margin: 40px 0;">This notebook is empty. Add your first section to get started!</p>
                    </div>
                `;
            }
        }
    },

    /**
     * Handle remote section updated
     */
    onRemoteSectionUpdated(data) {
        if (!this.currentNotebook) return;

        const section = this.currentNotebook.sections.find(s => s.id === data.id);
        if (!section) return;

        // Don't overwrite if user is currently editing this section
        const el = document.querySelector(`[data-section-id="${data.id}"]`);
        if (el && el.classList.contains('editing')) return;

        // Update local data. Use != null so an omitted/null field in a partial
        // update never clobbers existing state.
        if (data.title != null) section.title = data.title;
        if (data.content != null) section.content = data.content;
        if (data.chart_config != null) section.chart_config = data.chart_config;
        if (data.tags !== undefined) {
            section.tags = data.tags;
            this._refreshTagsAreaDOM(data.id, section);
            this.updateTagFilterBar();
            this.applyTagFilter();
        }

        // Re-render just this section's content
        const contentEl = document.getElementById(`section-content-${data.id}`);
        if (contentEl) {
            contentEl.innerHTML = this.renderSectionContent(section);
            contentEl.dataset.rendered = 'true';
        }
        // Title may have changed
        if (data.title !== undefined) this.renderTOCList();
    },

    /**
     * Handle remote section results updated (query executed)
     */
    onRemoteSectionResultsUpdated(data) {
        if (!this.currentNotebook) return;

        const section = this.currentNotebook.sections.find(s => s.id === data.id);
        if (!section) return;

        // Update local data
        if (data.last_executed_at) section.last_executed_at = data.last_executed_at;
        if (data.last_results) section.last_results = data.last_results;

        // Re-render the section content to show new results
        const contentEl = document.getElementById(`section-content-${data.id}`);
        if (contentEl) {
            contentEl.innerHTML = this.renderSectionContent(section);
            contentEl.dataset.rendered = 'true';
        }
    },

    /**
     * Handle remote sections reordered
     */
    onRemoteSectionsReordered(data) {
        if (!this.currentNotebook || !data.section_order) return;

        // Update order_index values
        const order = data.section_order;
        for (let i = 0; i < order.length; i++) {
            const section = this.currentNotebook.sections.find(s => s.id === order[i]);
            if (section) section.order_index = i;
        }

        // Reorder DOM elements
        const container = document.getElementById('notebookSections');
        if (!container) return;

        const sorted = [...this.currentNotebook.sections].sort((a, b) => a.order_index - b.order_index);
        for (const section of sorted) {
            const el = container.querySelector(`[data-section-id="${section.id}"]`);
            if (el) container.appendChild(el);
        }
        this.renderTOCList();
    },

    /**
     * Handle presence change via SSE
     */
    async onPresenceChanged() {
        if (!this.currentNotebook) return;
        try {
            const response = await fetch(`/api/v1/notebooks/${this.currentNotebook.id}/presence`, {
                method: 'GET',
                credentials: 'include'
            });
            const data = await response.json();
            if (data.success && data.data) {
                this.renderPresenceIndicators(data.data);
            }
        } catch (error) {
            // Ignore presence fetch errors
        }
    },

    /**
     * Render presence indicators
     */
    renderPresenceIndicators(presenceData) {
        const container = document.getElementById('notebookPresence');
        if (!container) return;

        // Filter out self and deduplicate by username
        const currentUsername = window.Auth && Auth.currentUser ? Auth.currentUser.username : null;
        const seen = new Set();
        const unique = presenceData.filter(user => {
            if (user.username === currentUsername) return false;
            if (seen.has(user.username)) return false;
            seen.add(user.username);
            return true;
        });

        container.innerHTML = unique.map(user => `
            <div class="presence-user" style="background-color: ${user.user_gravatar_color || '#9c6ade'}"
                 title="${Utils.escapeHtml(user.user_display_name || user.username)}">
                ${user.user_gravatar_initial || user.username.charAt(0).toUpperCase()}
            </div>
        `).join('');
    },

    // Utility functions
    formatTimeRange(notebook) {
        switch (notebook.time_range_type) {
            case '1h': return '1 hour';
            case '24h': return '24 hours';
            case '7d': return '7 days';
            case '30d': return '30 days';
            case 'custom': return 'Custom';
            default: return notebook.time_range_type;
        }
    },

    formatLocalDateTime(d) {
        const pad = (n) => String(n).padStart(2, '0');
        return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
    },

    formatRelativeTime(timestamp) {
        const date = new Date(timestamp);
        const now = new Date();
        const diffMs = now - date;
        const diffMins = Math.floor(diffMs / 60000);
        const diffHours = Math.floor(diffMs / 3600000);
        const diffDays = Math.floor(diffMs / 86400000);

        if (diffMins < 1) return 'Just now';
        if (diffMins < 60) return `${diffMins}m ago`;
        if (diffHours < 24) return `${diffHours}h ago`;
        if (diffDays < 30) return `${diffDays}d ago`;
        return date.toLocaleDateString();
    },

    showLoadingState(message = 'Loading...') {
        // Show loading in the appropriate container
        const editorContainer = document.getElementById('notebookEditor');
        const listingContainer = document.getElementById('notebookListing');

        if (editorContainer && editorContainer.style.display !== 'none') {
            editorContainer.innerHTML = `<div class="notebook-loading">${message}</div>`;
        } else if (listingContainer) {
            const tbody = document.getElementById('notebooksTableBody');
            if (tbody) {
                tbody.innerHTML = `<tr><td colspan="6" class="notebook-loading">${message}</td></tr>`;
            }
        }
    },

    showError(message) {
        // Create error toast
        const toast = document.createElement('div');
        toast.className = 'toast toast-error';
        toast.textContent = message;
        toast.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            background: var(--error);
            color: white;
            padding: 12px 16px;
            border-radius: 6px;
            box-shadow: var(--shadow-lg);
            z-index: 10000;
            animation: slideInRight 0.3s ease;
        `;

        document.body.appendChild(toast);

        // Auto remove after 5 seconds
        setTimeout(() => {
            if (toast.parentNode) {
                toast.remove();
            }
        }, 5000);

        console.error('[Notebooks]', message);
    },

    showSuccess(message) {
        // Create success toast
        const toast = document.createElement('div');
        toast.className = 'toast toast-success';
        toast.textContent = message;
        toast.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            background: var(--success);
            color: white;
            padding: 12px 16px;
            border-radius: 6px;
            box-shadow: var(--shadow-lg);
            z-index: 10000;
            animation: slideInRight 0.3s ease;
        `;

        document.body.appendChild(toast);

        // Auto remove after 3 seconds
        setTimeout(() => {
            if (toast.parentNode) {
                toast.remove();
            }
        }, 3000);

    },

    debounce(func, wait) {
        clearTimeout(this.debounceTimer);
        this.debounceTimer = setTimeout(func, wait);
    },

    handleKeyboardShortcuts(e) {
        // Ctrl/Cmd + S to save notebook
        if ((e.ctrlKey || e.metaKey) && e.key === 's' && this.isEditing) {
            e.preventDefault();
            this.saveCurrentNotebook();
        }
        // F8 — toggle section navigator
        if (e.key === 'F8' && this.isEditing) {
            e.preventDefault();
            this.toggleTOC();
        }
    },

    /**
     * Show add section menu
     */
    async showAddSectionMenu() {
        // Get the position of the Add Section button
        const addSectionBtn = document.getElementById('addSectionBtn');
        if (!addSectionBtn) {
            console.error('[Notebooks] Add section button not found');
            return;
        }

        const buttonRect = addSectionBtn.getBoundingClientRect();
        const scrollTop = window.pageYOffset || document.documentElement.scrollTop;

        // Position menu just above the button
        const menuTop = buttonRect.top + scrollTop - 10; // 10px above the button
        const menuLeft = buttonRect.left;

        // Check if AI summary option should be shown
        try {
            const resp = await fetch('/api/v1/notebooks/ai-status', { credentials: 'include' });
            const data = await resp.json();
            this.aiEnabled = data.ai_enabled || false;
        } catch { this.aiEnabled = false; }
        const hasAISummary = this.currentNotebook?.sections?.some(s => s.section_type === 'ai_summary');
        const hasAIAttackChain = this.currentNotebook?.sections?.some(s => s.section_type === 'ai_attack_chain');
        const showAISummary = this.aiEnabled && !hasAISummary;
        const showAIAttackChain = this.aiEnabled && !hasAIAttackChain;

        // Create the menu HTML
        const menuHtml = `
            <div id="addSectionMenu" class="add-section-menu show" style="position: absolute; top: ${menuTop}px; left: ${menuLeft}px; transform: translateY(-100%); z-index: 1000; background: var(--bg-secondary); border: 1px solid var(--border-color); border-radius: 8px; box-shadow: var(--shadow-lg); padding: 8px 0; min-width: 200px;">
                <button class="add-section-option" onclick="Notebooks.addSection('markdown')" style="display: block; width: 100%; padding: 12px 16px; border: none; background: none; color: var(--text-primary); font-size: 0.9rem; text-align: left; cursor: pointer; transition: var(--transition);" onmouseover="this.style.backgroundColor='var(--bg-hover)'; this.style.color='var(--accent-primary)'" onmouseout="this.style.backgroundColor='none'; this.style.color='var(--text-primary)'">
                    Add Markdown Section
                </button>
                <button class="add-section-option" onclick="Notebooks.addSection('query')" style="display: block; width: 100%; padding: 12px 16px; border: none; background: none; color: var(--text-primary); font-size: 0.9rem; text-align: left; cursor: pointer; transition: var(--transition);" onmouseover="this.style.backgroundColor='var(--bg-hover)'; this.style.color='var(--accent-primary)'" onmouseout="this.style.backgroundColor='none'; this.style.color='var(--text-primary)'">
                    Add Query Section
                </button>
                ${showAISummary ? `<button class="add-section-option" onclick="Notebooks.addSection('ai_summary')" style="display: block; width: 100%; padding: 12px 16px; border: none; background: none; color: var(--text-primary); font-size: 0.9rem; text-align: left; cursor: pointer; transition: var(--transition);" onmouseover="this.style.backgroundColor='var(--bg-hover)'; this.style.color='var(--accent-primary)'" onmouseout="this.style.backgroundColor='none'; this.style.color='var(--text-primary)'">
                    Add AI Summary
                </button>` : ''}
                ${showAIAttackChain ? `<button class="add-section-option" onclick="Notebooks.addSection('ai_attack_chain')" style="display: block; width: 100%; padding: 12px 16px; border: none; background: none; color: var(--text-primary); font-size: 0.9rem; text-align: left; cursor: pointer; transition: var(--transition);" onmouseover="this.style.backgroundColor='var(--bg-hover)'; this.style.color='var(--accent-primary)'" onmouseout="this.style.backgroundColor='none'; this.style.color='var(--text-primary)'">
                    Add AI Attack Chain Summary
                </button>` : ''}
                <button class="add-section-option" onclick="Notebooks.closeAddSectionMenu()" style="display: block; width: 100%; padding: 12px 16px; border: none; border-top: 1px solid var(--border-color); background: none; color: var(--text-muted); font-size: 0.9rem; text-align: left; cursor: pointer; transition: var(--transition); margin-top: 4px;" onmouseover="this.style.backgroundColor='var(--bg-hover)'; this.style.color='var(--text-primary)'" onmouseout="this.style.backgroundColor='none'; this.style.color='var(--text-muted)'">
                    Cancel
                </button>
            </div>
        `;

        // Remove existing menu
        this.closeAddSectionMenu();

        // Add the menu to the body
        document.body.insertAdjacentHTML('beforeend', menuHtml);

        // Close menu when clicking outside
        setTimeout(() => {
            document.addEventListener('click', this.handleAddSectionMenuClick.bind(this), { once: true });
        }, 100);
    },

    /**
     * Close add section menu
     */
    closeAddSectionMenu() {
        const menu = document.getElementById('addSectionMenu');
        if (menu) {
            menu.remove();
        }
    },

    /**
     * Handle clicks outside the add section menu
     */
    handleAddSectionMenuClick(event) {
        const menu = document.getElementById('addSectionMenu');
        if (menu && !menu.contains(event.target)) {
            this.closeAddSectionMenu();
        }
    },

    /**
     * Add a new section to the current notebook
     */
    async addSection(sectionType) {
        this.closeAddSectionMenu();

        if (!this.currentNotebook) {
            this.showError('No notebook is currently open');
            return;
        }

        try {
            // Get next order index
            const orderIndex = this.currentNotebook.sections ? this.currentNotebook.sections.length : 0;

            // Default content based on section type
            let defaultContent = '';
            let title = '';

            if (sectionType === 'markdown') {
                title = 'New Markdown Section';
                defaultContent = '# New Section\n\nAdd your markdown content here...';
            } else if (sectionType === 'query') {
                title = 'New Query Section';
                defaultContent = '// Add your BQL query here\n// Example: level=error | multi(count()) | groupby(service)';
            } else if (sectionType === 'ai_summary') {
                title = 'AI Summary';
                defaultContent = '';
            } else if (sectionType === 'ai_attack_chain') {
                title = 'AI Attack Chain Summary';
                defaultContent = '';
            }

            const sectionData = {
                section_type: sectionType,
                title: title,
                content: defaultContent,
                order_index: orderIndex
            };


            const response = await fetch(`/api/v1/notebooks/${this.currentNotebook.id}/sections`, {
                method: 'POST',
                headers: { ...this.sseHeaders(), 'Accept': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(sectionData)
            });

            if (!response.ok) {
                throw new Error(`Failed to create section: ${response.status} ${response.statusText}`);
            }

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to create section');
            }

            // Add the new section to the current notebook data and re-render
            const newSection = result.data;
            if (newSection) {
                // Add to the sections array
                if (!this.currentNotebook.sections) {
                    this.currentNotebook.sections = [];
                }
                this.currentNotebook.sections.push(newSection);

                // Re-render just the sections container instead of full refresh
                this.renderNotebookSections();
            }
            this.showSuccess(`${sectionType.charAt(0).toUpperCase() + sectionType.slice(1)} section added successfully!`);

        } catch (error) {
            console.error('[Notebooks] Error adding section:', error);
            this.showError(`Error adding section: ${error.message}`);
        }
    },

    /**
     * Toggle edit mode for a section
     */
    async toggleEditSection(sectionId) {

        if (!this.currentNotebook) {
            this.showError('No notebook is currently open');
            return;
        }

        // Find the section
        const section = this.currentNotebook.sections.find(s => s.id === sectionId);
        if (!section) {
            this.showError('Section not found');
            return;
        }

        const sectionContainer = document.querySelector(`[data-section-id="${sectionId}"]`);
        const contentContainer = document.getElementById(`section-content-${sectionId}`);

        if (!sectionContainer || !contentContainer) {
            this.showError('Section container not found');
            return;
        }

        // Check if already in edit mode
        if (sectionContainer.classList.contains('editing')) {
            this.cancelEditSection(sectionId);
            return;
        }

        // Enter edit mode
        this.enterEditMode(section, sectionContainer, contentContainer);
    },

    /**
     * Enter edit mode for a section
     */
    enterEditMode(section, sectionContainer, contentContainer) {
        sectionContainer.classList.add('editing');

        // Disable play button if this is a query section
        if (section.section_type === 'query') {
            const playButton = sectionContainer.querySelector('.execute-query-btn');
            if (playButton) {
                playButton.disabled = true;
                playButton.style.opacity = '0.5';
                playButton.style.cursor = 'not-allowed';
                playButton.title = 'Cannot execute while editing';
            }
        }

        // Hide up/down move buttons while editing
        const moveButtons = sectionContainer.querySelectorAll('.section-move-btn');
        moveButtons.forEach(btn => {
            btn.style.display = 'none';
        });

        // Create inline editor based on section type
        let editorHtml = '';

        if (section.section_type === 'markdown') {
            editorHtml = `
                <div class="inline-editor">
                    <div class="editor-fields">
                        <div class="form-group">
                            <label for="edit-title-${section.id}">Title</label>
                            <input type="text" id="edit-title-${section.id}" value="${Utils.escapeHtml(section.title || '')}" maxlength="255" style="width: 100%; padding: 8px; margin-bottom: 10px; border: 1px solid var(--border-color); border-radius: 4px; background: var(--bg-primary); color: var(--text-primary);">
                        </div>
                        <div class="form-group">
                            <label for="edit-content-${section.id}">Markdown Content</label>
                            <textarea id="edit-content-${section.id}" style="width: 100%; min-height: 200px; padding: 10px; border: 1px solid var(--border-color); border-radius: 4px; background: var(--bg-primary); color: var(--text-primary); font-family: var(--font-mono); font-size: 0.9rem; line-height: 1.5; resize: vertical; box-sizing: border-box;">${Utils.escapeHtml(section.content || '')}</textarea>
                        </div>
                        <div class="markdown-preview" style="margin-top: 10px; padding: 10px; border: 1px solid var(--border-color); border-radius: 4px; background: var(--bg-tertiary);">
                            <div class="preview-content" id="preview-content-${section.id}">
                                ${this.renderMarkdownToHtml(section.content)}
                            </div>
                        </div>
                    </div>
                    <div class="editor-controls" style="margin-top: 12px;">
                        <button class="btn-primary" onclick="Notebooks.saveEditSection('${section.id}')">Save</button>
                        <button class="btn-secondary" onclick="Notebooks.cancelEditSection('${section.id}')" style="margin-left: 8px;">Cancel</button>
                    </div>
                </div>
            `;
        } else if (section.section_type === 'query') {
            editorHtml = `
                <div class="inline-editor">
                    <div class="editor-fields">
                        <div class="form-group">
                            <label for="edit-title-${section.id}">Title</label>
                            <input type="text" id="edit-title-${section.id}" value="${Utils.escapeHtml(section.title || '')}" maxlength="255" style="width: 100%; padding: 8px; margin-bottom: 10px; border: 1px solid var(--border-color); border-radius: 4px; background: var(--bg-primary); color: var(--text-primary);">
                        </div>
                        <div class="form-group">
                            <label for="edit-content-${section.id}">BQL Query</label>
                            <div class="query-input-wrapper" style="position: relative; width: 100%;">
                                <div id="edit-highlight-${section.id}" class="query-highlight" style="position: absolute; top: 0; left: 0; width: 100%; min-height: 200px; padding: 10px; border: 1px solid transparent; border-radius: 4px; background: transparent; font-family: var(--font-mono); font-size: 0.9rem; line-height: 1.5; white-space: pre-wrap; word-wrap: break-word; overflow: hidden; pointer-events: none; z-index: 1;"></div>
                                <textarea id="edit-content-${section.id}" spellcheck="false" autocomplete="off" autocorrect="off" autocapitalize="off" style="position: relative; width: 100%; min-height: 200px; padding: 10px; border: 1px solid var(--border-color); border-radius: 4px; background: transparent; color: transparent; caret-color: var(--text-primary); font-family: var(--font-mono); font-size: 0.9rem; line-height: 1.5; resize: vertical; z-index: 2; box-sizing: border-box;">${Utils.escapeHtml(section.content || '')}</textarea>
                            </div>
                        </div>
                    </div>
                    <div class="editor-controls" style="margin-top: 12px;">
                        <button class="btn-primary" onclick="Notebooks.saveEditSection('${section.id}')">Save</button>
                        <button class="btn-secondary" onclick="Notebooks.cancelEditSection('${section.id}')" style="margin-left: 8px;">Cancel</button>
                    </div>
                </div>
            `;
        }

        // Replace content with editor
        contentContainer.innerHTML = editorHtml;

        // Update Edit button to Cancel
        const editButton = sectionContainer.querySelector('.section-control-btn');
        if (editButton && editButton.textContent.trim() === 'Edit') {
            editButton.textContent = 'Cancel';
            editButton.style.background = 'var(--error)';
            editButton.style.color = 'white';
        }

        // Setup live preview for markdown
        if (section.section_type === 'markdown') {
            const textarea = document.getElementById(`edit-content-${section.id}`);
            const preview = document.getElementById(`preview-content-${section.id}`);

            if (textarea && preview) {
                textarea.addEventListener('input', () => {
                    preview.innerHTML = this.renderMarkdownToHtml(textarea.value);
                });
            }
        }

        // Focus the content textarea and auto-expand to fit existing content
        const contentTextarea = document.getElementById(`edit-content-${section.id}`);
        if (contentTextarea) {
            // Expand to content height (minimum enforced by min-height CSS)
            contentTextarea.style.height = 'auto';
            contentTextarea.style.height = contentTextarea.scrollHeight + 'px';

            // Keep highlight overlay in sync for query sections
            if (section.section_type === 'query') {
                const highlightDiv = document.getElementById(`edit-highlight-${section.id}`);
                if (highlightDiv) highlightDiv.style.height = contentTextarea.style.height;
                contentTextarea.addEventListener('input', () => {
                    contentTextarea.style.height = 'auto';
                    contentTextarea.style.height = contentTextarea.scrollHeight + 'px';
                    if (highlightDiv) highlightDiv.style.height = contentTextarea.style.height;
                });
            }

            contentTextarea.focus();
            contentTextarea.setSelectionRange(contentTextarea.value.length, contentTextarea.value.length);

            if (section.section_type === 'query') {
                this.initializeQuerySyntaxHighlighting(section.id);
            }
        }
    },

    /**
     * Save section edit
     */
    async saveEditSection(sectionId) {

        if (!this.currentNotebook) {
            this.showError('No notebook is currently open');
            return;
        }

        const titleInput = document.getElementById(`edit-title-${sectionId}`);
        const contentTextarea = document.getElementById(`edit-content-${sectionId}`);

        if (!titleInput || !contentTextarea) {
            this.showError('Edit form elements not found');
            return;
        }

        const data = {
            title: titleInput.value.trim() || null,
            content: contentTextarea.value
        };

        // Update save button to show loading
        const saveButton = document.querySelector(`#section-${sectionId} .btn-primary`);
        const originalText = saveButton ? saveButton.textContent : 'Save';
        if (saveButton) {
            saveButton.disabled = true;
            saveButton.textContent = 'Saving...';
        }

        try {

            const response = await fetch(`/api/v1/notebooks/${this.currentNotebook.id}/sections/${sectionId}`, {
                method: 'PUT',
                headers: { ...this.sseHeaders(), 'Accept': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(data)
            });

            if (!response.ok) {
                throw new Error(`Failed to update section: ${response.status} ${response.statusText}`);
            }

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to update section');
            }

            this.showSuccess('Section updated successfully!');

            // Update the section in memory
            const section = this.currentNotebook.sections.find(s => s.id === sectionId);
            if (section) {
                section.title = data.title;
                section.content = data.content;
                section.updated_at = new Date().toISOString();

                // Mark query sections as modified since last execution
                if (section.section_type === 'query') {
                    section.modified_since_execution = true;
                }
            }

            // Exit edit mode and re-render the section
            this.exitEditMode(sectionId);

        } catch (error) {
            console.error('[Notebooks] Error updating section:', error);
            this.showError(`Error updating section: ${error.message}`);
        } finally {
            // Re-enable save button
            if (saveButton) {
                saveButton.disabled = false;
                saveButton.textContent = originalText;
            }
        }
    },

    /**
     * Cancel section edit
     */
    cancelEditSection(sectionId) {
        this.exitEditMode(sectionId);
    },

    /**
     * Exit edit mode and restore section display
     */
    exitEditMode(sectionId) {
        const sectionContainer = document.querySelector(`[data-section-id="${sectionId}"]`);
        const contentContainer = document.getElementById(`section-content-${sectionId}`);

        if (!sectionContainer || !contentContainer) {
            // console.warn('[Notebooks] Section container not found during exit edit mode');
            return;
        }

        // Remove editing class
        sectionContainer.classList.remove('editing');

        // Re-enable play button if this is a query section
        const section = this.currentNotebook.sections.find(s => s.id === sectionId);
        if (section && section.section_type === 'query') {
            const playButton = sectionContainer.querySelector('.execute-query-btn');
            if (playButton) {
                playButton.disabled = false;
                playButton.style.opacity = '';
                playButton.style.cursor = 'pointer';
                playButton.title = 'Execute Query';
            }
        }

        // Show up/down move buttons again
        const moveButtons = sectionContainer.querySelectorAll('.section-move-btn');
        moveButtons.forEach(btn => {
            btn.style.display = '';
        });

        // Section was already found above, no need to find again
        if (!section) {
            console.error('[Notebooks] Section not found in current notebook');
            return;
        }

        // Re-render section content
        contentContainer.innerHTML = this.renderSectionContent(section);

        // Update section header with new title
        const headerTitle = sectionContainer.querySelector('.section-type');
        if (headerTitle) {
            headerTitle.innerHTML = `
                <span class="section-drag-handle">⋮⋮</span>
                <span class="section-type-text">${section.title ? Utils.escapeHtml(section.title) : 'Untitled Section'}</span>
            `;
        }

        // Restore Edit button
        const editButton = sectionContainer.querySelector('.section-control-btn');
        if (editButton && (editButton.textContent.trim() === 'Cancel' || editButton.style.background)) {
            editButton.textContent = 'Edit';
            editButton.style.background = '';
            editButton.style.color = '';
        }
    },

    /**
     * Duplicate a section
     */
    async duplicateSection(sectionId) {

        if (!this.currentNotebook) {
            this.showError('No notebook is currently open');
            return;
        }

        // Find the section
        const section = this.currentNotebook.sections.find(s => s.id === sectionId);
        if (!section) {
            this.showError('Section not found');
            return;
        }

        if (section.section_type === 'ai_summary' || section.section_type === 'ai_attack_chain') {
            this.showError('AI Summary sections cannot be duplicated');
            return;
        }

        if (section.section_type === 'comment_context') {
            this.showError('Comment context sections cannot be duplicated');
            return;
        }

        try {
            const newSectionData = {
                section_type: section.section_type,
                title: (section.title || 'Untitled') + ' (Copy)',
                content: section.content || '',
                order_index: (this.currentNotebook.sections?.length || 0)
            };

            const response = await fetch(`/api/v1/notebooks/${this.currentNotebook.id}/sections`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                credentials: 'include',
                body: JSON.stringify(newSectionData)
            });

            if (!response.ok) {
                throw new Error(`Failed to duplicate section: ${response.status} ${response.statusText}`);
            }

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to duplicate section');
            }

            // Add the duplicated section to the current notebook data and re-render
            const duplicatedSection = result.data;
            if (duplicatedSection) {
                this.currentNotebook.sections.push(duplicatedSection);
                this.renderNotebookSections();
            }
            this.showSuccess('Section duplicated successfully!');

        } catch (error) {
            console.error('[Notebooks] Error duplicating section:', error);
            this.showError(`Error duplicating section: ${error.message}`);
        }
    },

    /**
     * Delete a section
     */
    async deleteSection(sectionId) {

        if (!this.currentNotebook) {
            this.showError('No notebook is currently open');
            return;
        }

        // Find the section
        const section = this.currentNotebook.sections.find(s => s.id === sectionId);
        if (!section) {
            this.showError('Section not found');
            return;
        }

        try {
            const response = await fetch(`/api/v1/notebooks/${this.currentNotebook.id}/sections/${sectionId}`, {
                method: 'DELETE',
                headers: this.sseHeaders(),
                credentials: 'include'
            });

            if (!response.ok) {
                throw new Error(`Failed to delete section: ${response.status} ${response.statusText}`);
            }

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to delete section');
            }

            // Remove the section from current notebook data and re-render
            this.currentNotebook.sections = this.currentNotebook.sections.filter(s => s.id !== sectionId);
            this.renderNotebookSections();
            this.showSuccess('Section deleted successfully!');

        } catch (error) {
            console.error('[Notebooks] Error deleting section:', error);
            this.showError(`Error deleting section: ${error.message}`);
        }
    },

    async duplicateNotebook(notebookId) {
        try {
            // Get the original notebook data
            const response = await fetch(`/api/v1/notebooks/${notebookId}`, {
                method: 'GET',
                credentials: 'include'
            });

            if (!response.ok) {
                throw new Error(`Failed to fetch notebook: ${response.status} ${response.statusText}`);
            }

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to fetch notebook');
            }

            const originalNotebook = result.data;

            // Create duplicate with modified name
            const duplicateData = {
                name: `${originalNotebook.name} (Copy)`,
                description: originalNotebook.description,
                time_range_type: originalNotebook.time_range_type,
                time_range_start: originalNotebook.time_range_start,
                time_range_end: originalNotebook.time_range_end,
                max_results_per_section: originalNotebook.max_results_per_section
            };

            // Create the duplicate notebook
            const createResponse = await fetch('/api/v1/notebooks', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(duplicateData)
            });

            if (!createResponse.ok) {
                throw new Error(`Failed to create duplicate: ${createResponse.status} ${createResponse.statusText}`);
            }

            const createResult = await createResponse.json();

            if (!createResult.success) {
                throw new Error(createResult.error || 'Failed to create duplicate');
            }

            const newNotebook = createResult.data;

            // Duplicate all sections
            if (originalNotebook.sections && originalNotebook.sections.length > 0) {
                for (const section of originalNotebook.sections) {
                    const sectionData = {
                        section_type: section.section_type,
                        title: section.title,
                        content: section.content,
                        order_index: section.order_index
                    };

                    await fetch(`/api/v1/notebooks/${newNotebook.id}/sections`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        credentials: 'include',
                        body: JSON.stringify(sectionData)
                    });
                }
            }

            this.showSuccess(`Notebook "${originalNotebook.name}" duplicated successfully!`);

            // Refresh the notebook listing
            this.loadNotebooks();

        } catch (error) {
            console.error('[Notebooks] Error duplicating notebook:', error);
            this.showError(`Error duplicating notebook: ${error.message}`);
        }
    },

    async deleteNotebook(notebookId, name) {
        if (!confirm(`Are you sure you want to delete the notebook "${name}"? This action cannot be undone.`)) {
            return;
        }

        try {

            const response = await fetch(`/api/v1/notebooks/${notebookId}`, {
                method: 'DELETE',
                credentials: 'include'
            });

            if (!response.ok) {
                if (response.status === 404) {
                    throw new Error('Notebook not found');
                }
                if (response.status === 401) {
                    throw new Error('Authentication required. Please log in.');
                }
                if (response.status === 403) {
                    throw new Error('Permission denied. You can only delete your own notebooks.');
                }
                throw new Error(`Server error: ${response.status} ${response.statusText}`);
            }

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to delete notebook');
            }

            this.showSuccess(`Notebook "${name}" deleted successfully!`);

            // If we're currently viewing the deleted notebook, return to listing
            if (this.currentNotebook && this.currentNotebook.id === notebookId) {
                window.App?.pushSubPath('');
                this.showNotebookListing();
            } else {
                // Otherwise just refresh the listing
                this.loadNotebooks();
            }

        } catch (error) {
            console.error('[Notebooks] Error deleting notebook:', error);
            this.showError(`Error deleting notebook: ${error.message}`);
        }
    },

    showNotebookSettings() {
        if (!this.currentNotebook) {
            this.showError('No notebook is currently open');
            return;
        }


        // Remove any existing settings modal first
        const existing = document.getElementById('notebookSettingsModal');
        if (existing) existing.remove();

        // Create settings modal dynamically
        const modalHtml = `
            <div id="notebookSettingsModal" class="modal-overlay">
                <div class="modal-content">
                    <div class="modal-header">
                        <h3>Notebook Settings</h3>
                        <button class="modal-close" id="notebookSettingsCloseBtn">&times;</button>
                    </div>
                    <form id="notebookSettingsForm" onsubmit="Notebooks.handleUpdateNotebook(event)">
                        <div class="form-group">
                            <label for="settingsNotebookName">Name *</label>
                            <input type="text" id="settingsNotebookName" name="name" required maxlength="255"
                                   value="${Utils.escapeHtml(this.currentNotebook.name || '')}"
                                   style="width: 100%; padding: 8px; border: 1px solid var(--border-color); border-radius: 4px; background: var(--bg-primary); color: var(--text-primary);">
                        </div>
                        <div class="form-group">
                            <label for="settingsNotebookDescription">Description</label>
                            <textarea id="settingsNotebookDescription" name="description" rows="3" maxlength="1000"
                                      style="width: 100%; padding: 8px; border: 1px solid var(--border-color); border-radius: 4px; background: var(--bg-primary); color: var(--text-primary); resize: vertical;">${Utils.escapeHtml(this.currentNotebook.description || '')}</textarea>
                        </div>
                        <div class="form-group">
                            <label for="settingsNotebookTimeRange">Time Range *</label>
                            <select id="settingsNotebookTimeRange" name="time_range_type" required
                                    style="width: 100%; padding: 8px; border: 1px solid var(--border-color); border-radius: 4px; background: var(--bg-primary); color: var(--text-primary);">
                                <option value="1h" ${this.currentNotebook.time_range_type === '1h' ? 'selected' : ''}>Last 1 hour</option>
                                <option value="24h" ${this.currentNotebook.time_range_type === '24h' ? 'selected' : ''}>Last 24 hours</option>
                                <option value="7d" ${this.currentNotebook.time_range_type === '7d' ? 'selected' : ''}>Last 7 days</option>
                                <option value="30d" ${this.currentNotebook.time_range_type === '30d' ? 'selected' : ''}>Last 30 days</option>
                                <option value="all" ${this.currentNotebook.time_range_type === 'all' ? 'selected' : ''}>All Time</option>
                                <option value="custom" ${this.currentNotebook.time_range_type === 'custom' ? 'selected' : ''}>Custom range</option>
                            </select>
                        </div>
                        <div id="settingsCustomTimeRange" class="form-group" style="display: ${this.currentNotebook.time_range_type === 'custom' ? 'block' : 'none'};">
                            <label for="settingsTimeRangeStart">Start Time</label>
                            <input type="text" placeholder="YYYY-MM-DD HH:mm" id="settingsTimeRangeStart" name="time_range_start"
                                   value="${this.currentNotebook.time_range_start ? this.formatLocalDateTime(new Date(this.currentNotebook.time_range_start)) : ''}"
                                   style="width: 100%; padding: 8px; margin-bottom: 10px; border: 1px solid var(--border-color); border-radius: 4px; background: var(--bg-primary); color: var(--text-primary);">
                            <label for="settingsTimeRangeEnd">End Time</label>
                            <input type="text" placeholder="YYYY-MM-DD HH:mm" id="settingsTimeRangeEnd" name="time_range_end"
                                   value="${this.currentNotebook.time_range_end ? this.formatLocalDateTime(new Date(this.currentNotebook.time_range_end)) : ''}"
                                   style="width: 100%; padding: 8px; border: 1px solid var(--border-color); border-radius: 4px; background: var(--bg-primary); color: var(--text-primary);">
                        </div>
                        <div class="form-group">
                            <label for="settingsMaxResultsPerSection">Max Results per Section</label>
                            <input type="number" id="settingsMaxResultsPerSection" name="max_results_per_section"
                                   value="${this.currentNotebook.max_results_per_section || 1000}" min="1" max="10000"
                                   style="width: 100%; padding: 8px; border: 1px solid var(--border-color); border-radius: 4px; background: var(--bg-primary); color: var(--text-primary);">
                        </div>
                        <div class="modal-buttons">
                            <button type="button" class="btn-secondary" onclick="Notebooks.closeNotebookSettingsModal()">Cancel</button>
                            <button type="submit" class="btn-primary">Update Settings</button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.insertAdjacentHTML('beforeend', modalHtml);

        const modal = document.getElementById('notebookSettingsModal');

        // Close on overlay click (outside modal-content)
        if (modal) {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) this.closeNotebookSettingsModal();
            });
        }

        // Close on Escape key
        const escHandler = (e) => {
            if (e.key === 'Escape') {
                this.closeNotebookSettingsModal();
                document.removeEventListener('keydown', escHandler);
            }
        };
        document.addEventListener('keydown', escHandler);

        // Bind close button event
        const closeBtn = document.getElementById('notebookSettingsCloseBtn');
        if (closeBtn) {
            closeBtn.addEventListener('click', () => this.closeNotebookSettingsModal());
        }

        // Bind cancel button event (more reliable than inline onclick)
        const cancelBtn = modal?.querySelector('.btn-secondary');
        if (cancelBtn) {
            cancelBtn.addEventListener('click', () => this.closeNotebookSettingsModal());
        }

        // Bind time range change event
        document.getElementById('settingsNotebookTimeRange').addEventListener('change', (e) => {
            const customRange = document.getElementById('settingsCustomTimeRange');
            const isCustom = e.target.value === 'custom';
            customRange.style.display = isCustom ? 'block' : 'none';

            // Set default values when switching to custom
            if (isCustom) {
                const startInput = document.getElementById('settingsTimeRangeStart');
                const endInput = document.getElementById('settingsTimeRangeEnd');

                if (startInput && endInput && (!startInput.value || !endInput.value)) {
                    const now = new Date();
                    const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
                    const pad = (n) => String(n).padStart(2, '0');
                    const fmt = (d) => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;

                    startInput.value = fmt(twentyFourHoursAgo);
                    endInput.value = fmt(now);
                }
            }
        });

        // Focus name input
        document.getElementById('settingsNotebookName').focus();
    },

    /**
     * Close notebook settings modal
     */
    closeNotebookSettingsModal() {
        const modal = document.getElementById('notebookSettingsModal');
        if (modal) {
            modal.remove();
        }
    },

    /**
     * Handle notebook settings update
     */
    async handleUpdateNotebook(event) {
        event.preventDefault();

        if (!this.currentNotebook) {
            this.showError('No notebook is currently open');
            return;
        }


        // Disable the submit button to prevent double submission
        const submitBtn = event.target.querySelector('button[type="submit"]');
        const originalText = submitBtn?.textContent || 'Update Settings';
        if (submitBtn) {
            submitBtn.disabled = true;
            submitBtn.textContent = 'Updating...';
        }

        try {
            const formData = new FormData(event.target);
            const data = {
                name: formData.get('name'),
                description: formData.get('description') || '',
                time_range_type: formData.get('time_range_type'),
                max_results_per_section: parseInt(formData.get('max_results_per_section')) || 1000
            };


            if (data.time_range_type === 'custom') {
                const start = formData.get('time_range_start');
                const end = formData.get('time_range_end');

                if (!start || !end) {
                    this.showError('Start and end times are required for custom time range');
                    return;
                }

                data.time_range_start = new Date(start).toISOString();
                data.time_range_end = new Date(end).toISOString();
            }


            const response = await fetch(`/api/v1/notebooks/${this.currentNotebook.id}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                credentials: 'include',
                body: JSON.stringify(data)
            });


            if (!response.ok) {
                if (response.status === 401) {
                    throw new Error('Authentication required. Please log in.');
                }
                if (response.status === 403) {
                    throw new Error('Permission denied. You can only modify your own notebooks.');
                }
                if (response.status === 404) {
                    throw new Error('Notebook not found.');
                }
                throw new Error(`Server error: ${response.status} ${response.statusText}`);
            }

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to update notebook');
            }

            this.showSuccess('Notebook settings updated successfully!');
            this.closeNotebookSettingsModal();

            // Update the current notebook data
            Object.assign(this.currentNotebook, data);
            this.currentNotebook.updated_at = new Date().toISOString();

            // Update the notebook title in the editor if it changed
            const titleEl = document.getElementById('notebookTitle');
            if (titleEl && data.name) {
                titleEl.textContent = data.name;
            }

        } catch (error) {
            console.error('[Notebooks] Error updating notebook:', error);
            this.showError(`Error updating notebook: ${error.message}`);
        } finally {
            // Re-enable the submit button
            if (submitBtn) {
                submitBtn.disabled = false;
                submitBtn.textContent = originalText;
            }
        }
    },

    saveCurrentNotebook() {
    },

    bindSectionEvents() {

        // Find all sections (not draggable themselves anymore)
        const sections = document.querySelectorAll('.notebook-section');
        // Find all drag handles
        const dragHandles = document.querySelectorAll('.section-drag-handle[draggable="true"]');

        // Bind drag events to handles only
        dragHandles.forEach(handle => {
            // Remove existing event listeners to prevent duplicates
            handle.removeEventListener('dragstart', this.handleDragStart);
            handle.removeEventListener('dragend', this.handleDragEnd);

            // Add drag event listeners
            handle.addEventListener('dragstart', this.handleDragStart.bind(this));
            handle.addEventListener('dragend', this.handleDragEnd.bind(this));
        });

        // Bind drop events to sections
        sections.forEach(section => {
            // Remove existing event listeners to prevent duplicates
            section.removeEventListener('dragover', this.handleDragOver);
            section.removeEventListener('drop', this.handleDrop);

            // Add drop event listeners
            section.addEventListener('dragover', this.handleDragOver.bind(this));
            section.addEventListener('drop', this.handleDrop.bind(this));
        });

        // Add double-click event listeners for section content to enter edit mode
        sections.forEach(section => {
            const sectionId = section.dataset.sectionId;
            const sectionContent = section.querySelector('.section-content');

            if (sectionContent && sectionId) {
                // Remove existing double-click listener to prevent duplicates
                sectionContent.removeEventListener('dblclick', this.handleSectionDoubleClick);

                // Add double-click listener
                sectionContent.addEventListener('dblclick', (e) => {
                    // Don't trigger edit mode if clicking on buttons, links, or interactive elements
                    if (e.target.tagName === 'BUTTON' ||
                        e.target.tagName === 'A' ||
                        e.target.closest('button') ||
                        e.target.closest('.query-controls') ||
                        e.target.closest('.section-controls')) {
                        return;
                    }

                    // Don't trigger if already in edit mode
                    if (section.classList.contains('editing')) {
                        return;
                    }

                    // Don't trigger for non-editable section types
                    const sectionData = this.currentNotebook?.sections?.find(s => s.id === sectionId);
                    if (sectionData && (sectionData.section_type === 'comment_context' || sectionData.section_type === 'ai_summary' || sectionData.section_type === 'ai_attack_chain')) {
                        return;
                    }

                    // Trigger edit mode
                    this.toggleEditSection(sectionId);
                });
            }
        });
    },

    /**
     * Handle drag start
     */
    handleDragStart(e) {
        this.draggedSection = e.target.closest('.notebook-section');
        this.draggedSectionId = this.draggedSection.dataset.sectionId;

        // Add dragging class for visual feedback
        this.draggedSection.classList.add('dragging');

        // Store the section data
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/html', this.draggedSection.outerHTML);

    },

    /**
     * Handle drag over
     */
    handleDragOver(e) {
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';

        const dropTarget = e.target.closest('.notebook-section');
        if (dropTarget && dropTarget !== this.draggedSection) {
            // Remove existing drop indicators
            document.querySelectorAll('.drop-indicator').forEach(indicator => indicator.remove());

            // Add drop indicator
            const rect = dropTarget.getBoundingClientRect();
            const midY = rect.top + rect.height / 2;
            const dropIndicator = document.createElement('div');
            dropIndicator.className = 'drop-indicator';
            dropIndicator.style.cssText = `
                position: absolute;
                left: 0;
                right: 0;
                height: 3px;
                background: var(--accent-primary);
                border-radius: 2px;
                z-index: 1000;
                box-shadow: 0 0 6px var(--accent-glow);
            `;

            if (e.clientY < midY) {
                // Insert before
                dropTarget.style.position = 'relative';
                dropIndicator.style.top = '-2px';
                dropTarget.insertBefore(dropIndicator, dropTarget.firstChild);
                this.dropPosition = 'before';
            } else {
                // Insert after
                dropTarget.style.position = 'relative';
                dropIndicator.style.bottom = '-2px';
                dropTarget.appendChild(dropIndicator);
                this.dropPosition = 'after';
            }

            this.dropTarget = dropTarget;
        }
    },

    /**
     * Handle drop
     */
    async handleDrop(e) {
        e.preventDefault();

        if (!this.draggedSection || !this.dropTarget) {
            return;
        }

        const dropTargetId = this.dropTarget.dataset.sectionId;

        if (this.draggedSectionId === dropTargetId) {
            return; // Can't drop on itself
        }


        // Calculate new order
        const draggedIndex = this.currentNotebook.sections.findIndex(s => s.id === this.draggedSectionId);
        const targetIndex = this.currentNotebook.sections.findIndex(s => s.id === dropTargetId);

        if (draggedIndex === -1 || targetIndex === -1) {
            console.error('[Notebooks] Could not find section indexes');
            return;
        }

        // Create new order array
        const sections = [...this.currentNotebook.sections];
        const [draggedSection] = sections.splice(draggedIndex, 1);

        let insertIndex = targetIndex;
        if (draggedIndex < targetIndex && this.dropPosition === 'after') {
            insertIndex = targetIndex;
        } else if (draggedIndex < targetIndex && this.dropPosition === 'before') {
            insertIndex = targetIndex - 1;
        } else if (draggedIndex > targetIndex && this.dropPosition === 'after') {
            insertIndex = targetIndex + 1;
        } else if (draggedIndex > targetIndex && this.dropPosition === 'before') {
            insertIndex = targetIndex;
        }

        sections.splice(insertIndex, 0, draggedSection);

        // Update order_index values
        sections.forEach((section, index) => {
            section.order_index = index;
        });

        try {
            // Send reorder request to server - backend expects section_order array of IDs
            const sectionOrder = sections.map(section => section.id);

            const response = await fetch(`/api/v1/notebooks/${this.currentNotebook.id}/sections/reorder`, {
                method: 'POST',
                headers: { ...this.sseHeaders(), 'Accept': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ section_order: sectionOrder })
            });

            if (!response.ok) {
                throw new Error(`Failed to reorder sections: ${response.status} ${response.statusText}`);
            }

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to reorder sections');
            }

            // Update local data and re-render
            this.currentNotebook.sections = sections;
            this.renderNotebookSections();

        } catch (error) {
            console.error('[Notebooks] Error reordering sections:', error);
            this.showError(`Error reordering sections: ${error.message}`);
        }
    },

    /**
     * Handle drag end
     */
    handleDragEnd(e) {
        // Clean up
        if (this.draggedSection) {
            this.draggedSection.classList.remove('dragging');
        }

        // Reset cursor style on drag handle
        e.target.style.cursor = 'grab';

        // Remove drop indicators
        document.querySelectorAll('.drop-indicator').forEach(indicator => indicator.remove());

        // Reset variables
        this.draggedSection = null;
        this.draggedSectionId = null;
        this.dropTarget = null;
        this.dropPosition = null;

    },

    /**
     * Move section up one position
     */
    async moveSectionUp(sectionId) {
        if (!this.currentNotebook) {
            this.showError('No notebook is currently open');
            return;
        }

        const currentIndex = this.currentNotebook.sections.findIndex(s => s.id === sectionId);
        if (currentIndex === -1) {
            this.showError('Section not found');
            return;
        }

        if (currentIndex === 0) {
            // Already at the top
            return;
        }

        // Swap with the section above
        const sections = [...this.currentNotebook.sections];
        [sections[currentIndex - 1], sections[currentIndex]] = [sections[currentIndex], sections[currentIndex - 1]];

        // Update order_index values to match new positions
        sections.forEach((section, index) => {
            section.order_index = index;
        });

        try {
            // Send reorder request to server - backend expects section_order array of IDs
            const sectionOrder = sections.map(section => section.id);

            const response = await fetch(`/api/v1/notebooks/${this.currentNotebook.id}/sections/reorder`, {
                method: 'POST',
                headers: { ...this.sseHeaders(), 'Accept': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ section_order: sectionOrder })
            });

            if (!response.ok) {
                throw new Error(`Failed to reorder sections: ${response.status} ${response.statusText}`);
            }

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to reorder sections');
            }

            // Update local data and re-render
            this.currentNotebook.sections = sections;
            this.renderNotebookSections();

        } catch (error) {
            this.showError(`Error moving section: ${error.message}`);
        }
    },

    /**
     * Move section down one position
     */
    async moveSectionDown(sectionId) {
        if (!this.currentNotebook) {
            this.showError('No notebook is currently open');
            return;
        }

        const currentIndex = this.currentNotebook.sections.findIndex(s => s.id === sectionId);
        if (currentIndex === -1) {
            this.showError('Section not found');
            return;
        }

        if (currentIndex === this.currentNotebook.sections.length - 1) {
            // Already at the bottom
            return;
        }

        // Swap with the section below
        const sections = [...this.currentNotebook.sections];
        [sections[currentIndex], sections[currentIndex + 1]] = [sections[currentIndex + 1], sections[currentIndex]];

        // Update order_index values to match new positions
        sections.forEach((section, index) => {
            section.order_index = index;
        });

        try {
            // Send reorder request to server - backend expects section_order array of IDs
            const sectionOrder = sections.map(section => section.id);

            const response = await fetch(`/api/v1/notebooks/${this.currentNotebook.id}/sections/reorder`, {
                method: 'POST',
                headers: { ...this.sseHeaders(), 'Accept': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ section_order: sectionOrder })
            });

            if (!response.ok) {
                throw new Error(`Failed to reorder sections: ${response.status} ${response.statusText}`);
            }

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to reorder sections');
            }

            // Update local data and re-render
            this.currentNotebook.sections = sections;
            this.renderNotebookSections();

        } catch (error) {
            this.showError(`Error moving section: ${error.message}`);
        }
    },

    async moveSectionToTop(sectionId) {
        if (!this.currentNotebook) return;
        const currentIndex = this.currentNotebook.sections.findIndex(s => s.id === sectionId);
        if (currentIndex <= 0) return;

        const sections = [...this.currentNotebook.sections];
        const [section] = sections.splice(currentIndex, 1);
        sections.unshift(section);
        sections.forEach((s, i) => { s.order_index = i; });

        try {
            const response = await fetch(`/api/v1/notebooks/${this.currentNotebook.id}/sections/reorder`, {
                method: 'POST',
                headers: { ...this.sseHeaders(), 'Accept': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ section_order: sections.map(s => s.id) })
            });
            if (!response.ok) throw new Error(`Failed to reorder: ${response.statusText}`);
            const result = await response.json();
            if (!result.success) throw new Error(result.error || 'Failed to reorder');
            this.currentNotebook.sections = sections;
            this.renderNotebookSections();
        } catch (error) {
            this.showError(`Error moving section: ${error.message}`);
        }
    },

    // ── Section Navigator (TOC) ──────────────────────────────────────────────

    toggleTOC() {
        const panel = document.getElementById('notebookTOC');
        if (!panel) return;
        const opening = panel.classList.contains('collapsed');
        panel.classList.toggle('collapsed', !opening);
        if (opening) {
            this.renderTOCList();
            this._setupTOCObserver();
            setTimeout(() => {
                const search = document.getElementById('notebookTOCSearch');
                if (search) search.focus();
            }, 260);
        } else {
            if (this._tocObserver) {
                this._tocObserver.disconnect();
                this._tocObserver = null;
            }
            this._tocVisibleSections.clear();
            this._currentTOCActiveId = null;
        }
    },

    renderTOCList(filterQuery) {
        const list = document.getElementById('notebookTOCList');
        if (!list) return;
        const panel = document.getElementById('notebookTOC');
        if (!panel || panel.classList.contains('collapsed')) return;

        const q = (filterQuery !== undefined ? filterQuery : (document.getElementById('notebookTOCSearch')?.value || '')).toLowerCase();
        const sections = [...(this.currentNotebook?.sections || [])].sort((a, b) => a.order_index - b.order_index);

        const typeColor = {
            markdown:       'var(--text-muted)',
            query:          'var(--accent-primary)',
            comment_context:'#d4a054',
            ai_summary:     '#9c6ade',
            ai_attack_chain:'#e07a8b',
        };
        const typeLabel = {
            markdown:       'Markdown',
            query:          'Query',
            comment_context:'Comment',
            ai_summary:     'AI Summary',
            ai_attack_chain:'Attack Chain',
        };

        if (sections.length === 0) {
            list.innerHTML = '<div class="notebook-toc-empty">No sections</div>';
            return;
        }

        let anyVisible = false;
        list.innerHTML = sections.map(section => {
            const raw = section.title || typeLabel[section.section_type] || 'Untitled';
            const title = Utils.escapeHtml(raw);
            const color = typeColor[section.section_type] || 'var(--text-muted)';
            const matches = !q || raw.toLowerCase().includes(q) || (typeLabel[section.section_type] || '').toLowerCase().includes(q);
            if (matches) anyVisible = true;
            return `<div class="notebook-toc-item${matches ? '' : ' toc-hidden'}" data-section-id="${section.id}" onclick="Notebooks.scrollToSection('${section.id}')" title="${title}">
                <span class="toc-type-dot" style="background:${color}"></span>
                <span class="toc-item-label">${title}</span>
            </div>`;
        }).join('');

        if (!anyVisible) {
            list.innerHTML += '<div class="notebook-toc-empty">No matches</div>';
        }

        // Re-apply active highlight after re-render
        if (this._currentTOCActiveId) this._updateTOCActive(this._currentTOCActiveId);
    },

    filterTOC(query) {
        this.renderTOCList(query);
    },

    _setupTOCObserver() {
        const panel = document.getElementById('notebookTOC');
        if (!panel || panel.classList.contains('collapsed')) return;

        if (this._tocObserver) {
            this._tocObserver.disconnect();
            this._tocObserver = null;
        }
        this._tocVisibleSections.clear();

        this._tocObserver = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                const id = entry.target.dataset.sectionId;
                if (entry.isIntersecting) {
                    this._tocVisibleSections.add(id);
                } else {
                    this._tocVisibleSections.delete(id);
                }
            });

            // Highlight the topmost section that has any part in the viewport
            let activeId = null;
            let minTop = Infinity;
            for (const id of this._tocVisibleSections) {
                const el = document.querySelector(`[data-section-id="${id}"]`);
                if (el) {
                    const top = el.getBoundingClientRect().top;
                    if (top < minTop) { minTop = top; activeId = id; }
                }
            }
            this._updateTOCActive(activeId);
        }, { threshold: 0 });

        document.querySelectorAll('.notebook-section').forEach(el => {
            this._tocObserver.observe(el);
        });
    },

    _updateTOCActive(sectionId) {
        this._currentTOCActiveId = sectionId;
        document.querySelectorAll('.notebook-toc-item').forEach(item => {
            const isActive = item.dataset.sectionId === sectionId;
            item.classList.toggle('active', isActive);
            if (isActive) item.scrollIntoView({ block: 'nearest' });
        });
    },

    // ────────────────────────────────────────────────────────────────────────

    toggleSectionKebab(sectionId, event) {
        event.stopPropagation();
        const menu = document.getElementById(`kebab-menu-${sectionId}`);
        const isOpen = menu.classList.contains('open');
        document.querySelectorAll('.section-kebab-menu.open').forEach(m => m.classList.remove('open'));
        if (!isOpen) menu.classList.add('open');
    },

    /**
     * Render markdown to HTML using marked.js
     */
    renderMarkdownToHtml(markdown) {
        if (!markdown) return '';

        try {
            if (typeof marked !== 'undefined' && typeof DOMPurify !== 'undefined') {
                marked.setOptions({
                    breaks: true,
                    gfm: true,
                    tables: true,
                    headerIds: false,
                    mangle: false,
                    silent: true,
                    pedantic: false,
                    smartypants: false
                });

                const html = marked.parse(markdown);
                return DOMPurify.sanitize(html, {
                    ALLOWED_TAGS: [
                        'p', 'br', 'strong', 'b', 'em', 'i', 'del', 's',
                        'code', 'pre', 'a', 'ul', 'ol', 'li', 'blockquote',
                        'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                        'table', 'thead', 'tbody', 'tr', 'th', 'td',
                        'hr', 'img', 'span', 'div', 'sup', 'sub',
                    ],
                    ALLOWED_ATTR: ['href', 'target', 'rel', 'src', 'alt', 'title', 'class'],
                });
            }
        } catch (error) {
            console.warn('[Notebooks] Error rendering markdown:', error);
        }

        // Fallback to escaped HTML
        return `<pre style="white-space: pre-wrap;">${Utils.escapeHtml(markdown)}</pre>`;
    },

    /**
     * Setup live markdown preview for editing
     */
    setupMarkdownEditor(container, initialContent = '') {
        const editorHtml = `
            <div class="markdown-section">
                <div class="section-editor">
                    <div class="markdown-editor">
                        <textarea placeholder="Enter markdown content..." style="width: 100%; height: 300px; padding: 12px; border: 1px solid var(--border-color); border-radius: 6px; background: var(--bg-primary); color: var(--text-primary); font-family: var(--font-mono); font-size: 0.9rem; line-height: 1.5; resize: vertical; outline: none;">${Utils.escapeHtml(initialContent)}</textarea>
                    </div>
                    <div class="markdown-preview">
                        <div class="preview-content" style="padding: 12px; border: 1px solid var(--border-color); border-radius: 6px; background: var(--bg-primary); min-height: 300px; overflow-y: auto;">
                            ${this.renderMarkdownToHtml(initialContent)}
                        </div>
                    </div>
                </div>
            </div>
        `;

        container.innerHTML = editorHtml;

        // Setup live preview
        const textarea = container.querySelector('textarea');
        const preview = container.querySelector('.preview-content');

        if (textarea && preview) {
            textarea.addEventListener('input', () => {
                preview.innerHTML = this.renderMarkdownToHtml(textarea.value);
            });

            textarea.addEventListener('keydown', (e) => {
                // Handle tab key for indentation
                if (e.key === 'Tab') {
                    e.preventDefault();
                    const start = textarea.selectionStart;
                    const end = textarea.selectionEnd;
                    textarea.value = textarea.value.substring(0, start) + '    ' + textarea.value.substring(end);
                    textarea.selectionStart = textarea.selectionEnd = start + 4;

                    // Update preview
                    preview.innerHTML = this.renderMarkdownToHtml(textarea.value);
                }
            });
        }

        return textarea;
    },

    /**
     * Initialize syntax highlighting for query editor
     */
    initializeQuerySyntaxHighlighting(sectionId) {
        const inputId = `edit-content-${sectionId}`;
        const highlightId = `edit-highlight-${sectionId}`;

        const queryInput = document.getElementById(inputId);
        const queryHighlight = document.getElementById(highlightId);

        if (!queryInput || !queryHighlight) {
            // console.warn('[Notebooks] Could not find elements for syntax highlighting:', {inputId, highlightId});
            return;
        }


        // Set up event listeners for real-time highlighting
        queryInput.addEventListener('input', () => this.updateQuerySyntaxHighlight(inputId, highlightId));
        queryInput.addEventListener('scroll', () => this.syncQueryScroll(inputId, highlightId));

        // Initial highlight
        this.updateQuerySyntaxHighlight(inputId, highlightId);
    },

    /**
     * Update syntax highlighting for query editor
     */
    updateQuerySyntaxHighlight(inputId, highlightId) {
        const queryInput = document.getElementById(inputId);
        const queryHighlight = document.getElementById(highlightId);

        if (!queryInput || !queryHighlight) return;

        const text = queryInput.value;

        // Use the same highlighting logic as the main search bar (BQL mode)
        let highlighted;
        if (typeof SyntaxHighlight !== 'undefined' && SyntaxHighlight.highlight) {
            highlighted = SyntaxHighlight.highlight(text);
        } else {
            // Fallback to plain text with basic styling if syntax highlighter not available
            highlighted = `<span style="color: var(--text-primary);">${Utils.escapeHtml(text)}</span>`;
        }

        queryHighlight.innerHTML = highlighted + '<br/>';
        this.syncQueryScroll(inputId, highlightId);
    },

    /**
     * Sync scroll between query input and highlight
     */
    syncQueryScroll(inputId, highlightId) {
        const queryInput = document.getElementById(inputId);
        const queryHighlight = document.getElementById(highlightId);

        if (!queryInput || !queryHighlight) return;

        queryHighlight.scrollTop = queryInput.scrollTop;
        queryHighlight.scrollLeft = queryInput.scrollLeft;
    },

    /**
     * Highlight query syntax for display (not editing)
     */
    highlightQuerySyntax(text) {
        if (!text) return '';

        // Use the same highlighting logic as the main search bar
        if (typeof SyntaxHighlight !== 'undefined' && SyntaxHighlight.highlight) {
            return SyntaxHighlight.highlight(text);
        }
        // Fallback to basic styling if syntax highlighter is not available
        return `<span style="color: var(--text-primary);">${Utils.escapeHtml(text)}</span>`;
    },

    /**
     * Get current markdown content from editor
     */
    getMarkdownEditorContent(container) {
        const textarea = container.querySelector('textarea');
        return textarea ? textarea.value : '';
    },

    async exportNotebook(notebookId) {
        try {
            const response = await fetch(`/api/v1/notebooks/${notebookId}/export`, {
                credentials: 'include'
            });
            if (!response.ok) throw new Error('Failed to export notebook');

            const blob = await response.blob();
            const disposition = response.headers.get('Content-Disposition') || '';
            const match = disposition.match(/filename="(.+?)"/);
            const filename = match ? match[1] : 'notebook.yaml';

            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);

            if (window.Toast) Toast.show('Notebook exported', 'success');
        } catch (err) {
            console.error('[Notebooks] Export failed:', err);
            if (window.Toast) Toast.show('Failed to export notebook', 'error');
        }
    },

    importNotebook() {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.yaml,.yml';
        input.onchange = async (e) => {
            const file = e.target.files[0];
            if (!file) return;

            try {
                const text = await file.text();
                const response = await fetch('/api/v1/notebooks/import', {
                    method: 'POST',
                    headers: { 'Content-Type': 'text/yaml' },
                    credentials: 'include',
                    body: text
                });

                const data = await response.json();
                if (!data.success) throw new Error(data.error || 'Import failed');

                if (window.Toast) Toast.show('Notebook imported successfully', 'success');
                this.loadNotebooks();
            } catch (err) {
                console.error('[Notebooks] Import failed:', err);
                if (window.Toast) Toast.show('Failed to import notebook: ' + err.message, 'error');
            }
        };
        input.click();
    },

    // =====================
    // Variables
    // =====================

    renderVariablesBar() {
        const container = document.getElementById('notebookVariables');
        if (!container) return;

        const vars = (this.currentNotebook && this.currentNotebook.variables) || [];

        if (vars.length === 0) {
            container.innerHTML = `<div class="variables-bar-empty">
                <button class="btn-add-variable" onclick="Notebooks.addVariable()">+ Add Variable</button>
            </div>`;
            return;
        }

        const escHtml = (s) => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
        let html = '<div class="variables-bar-items">';
        for (const v of vars) {
            const safeName = escHtml(v.name);
            const safeValue = escHtml(v.value || '*');
            html += `<div class="variable-pill">
                <span class="variable-name">@${safeName}</span>
                <input type="text" class="variable-value-input" value="${safeValue}"
                    data-var-name="${safeName}"
                    onchange="Notebooks.updateVariableValue('${safeName}', this.value)"
                    onkeydown="if(event.key==='Enter'){this.blur();}" />
                <button class="variable-remove-btn" onclick="Notebooks.removeVariable('${safeName}')" title="Remove variable">&times;</button>
            </div>`;
        }
        html += `<button class="btn-add-variable" onclick="Notebooks.addVariable()">+ Variable</button>`;
        html += '</div>';

        container.innerHTML = html;
    },

    addVariable() {
        const name = prompt('Variable name (without @):');
        if (!name || !name.trim()) return;

        const cleanName = name.trim().replace(/[^a-zA-Z0-9_]/g, '');
        if (!cleanName) {
            if (window.Toast) Toast.show('Variable name must contain only letters, numbers, or underscores', 'error');
            return;
        }

        if (!this.currentNotebook.variables) {
            this.currentNotebook.variables = [];
        }

        if (this.currentNotebook.variables.some(v => v.name === cleanName)) {
            if (window.Toast) Toast.show('Variable @' + cleanName + ' already exists', 'error');
            return;
        }

        this.currentNotebook.variables.push({ name: cleanName, value: '*' });
        this.saveVariables();
        this.renderVariablesBar();
    },

    updateVariableValue(name, value) {
        if (!this.currentNotebook || !this.currentNotebook.variables) return;

        const v = this.currentNotebook.variables.find(v => v.name === name);
        if (v) {
            v.value = value;
            this.saveVariables();
        }
    },

    removeVariable(name) {
        if (!this.currentNotebook || !this.currentNotebook.variables) return;

        this.currentNotebook.variables = this.currentNotebook.variables.filter(v => v.name !== name);
        this.saveVariables();
        this.renderVariablesBar();
    },

    async saveVariables() {
        if (!this.currentNotebook) return;
        try {
            await fetch(`/api/v1/notebooks/${this.currentNotebook.id}/variables`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ variables: this.currentNotebook.variables || [] })
            });
        } catch (err) {
            console.error('[Notebooks] Failed to save variables:', err);
        }
    }
};

// Export for module systems or global usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = Notebooks;
} else {
    window.Notebooks = Notebooks;
}