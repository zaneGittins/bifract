const FractalManagement = {
    fractals: [],
    currentPage: 1,
    pageSize: 10,
    isLoading: false,
    currentEditFractal: null,
    currentDetailFractal: null,
    createType: 'fractal',

    init() {
        console.log('Initializing Fractal Management...');
        this.setupEventListeners();
        // Don't load fractals immediately - only when view is shown
    },

    setupEventListeners() {
        // Create fractal button
        const createFractalBtn = document.getElementById('createFractalBtn');
        if (createFractalBtn) {
            createFractalBtn.addEventListener('click', () => this.showCreateFractalModal());
        }

        // Refresh fractals button
        const refreshFractalsBtn = document.getElementById('refreshFractalsBtn');
        if (refreshFractalsBtn) {
            refreshFractalsBtn.addEventListener('click', () => this.loadFractals());
        }

        // Fractal settings event listeners
        this.setupFractalSettingsEventListeners();

        // Modal close handlers
        this.setupModalHandlers();
    },

    setupModalHandlers() {
        // Create Fractal Modal
        const createModal = document.getElementById('createFractalModal');
        const createForm = document.getElementById('createFractalForm');
        const cancelCreateBtn = document.getElementById('cancelCreateBtn');

        if (createModal) {
            createModal.addEventListener('click', (e) => {
                if (e.target === createModal) {
                    this.hideCreateFractalModal();
                }
            });
        }

        const createCloseBtn = createModal?.querySelector('.close-btn');
        if (createCloseBtn) {
            createCloseBtn.addEventListener('click', () => this.hideCreateFractalModal());
        }

        if (cancelCreateBtn) {
            cancelCreateBtn.addEventListener('click', () => this.hideCreateFractalModal());
        }

        if (createForm) {
            createForm.addEventListener('submit', (e) => this.handleCreateFractal(e));
        }

        // Edit Fractal Modal
        const editModal = document.getElementById('editFractalModal');
        const editForm = document.getElementById('editFractalForm');
        const cancelEditBtn = document.getElementById('cancelEditBtn');

        if (editModal) {
            editModal.addEventListener('click', (e) => {
                if (e.target === editModal) {
                    this.hideEditFractalModal();
                }
            });
        }

        const editCloseBtn = editModal?.querySelector('.close-btn');
        if (editCloseBtn) {
            editCloseBtn.addEventListener('click', () => this.hideEditFractalModal());
        }

        if (cancelEditBtn) {
            cancelEditBtn.addEventListener('click', () => this.hideEditFractalModal());
        }

        if (editForm) {
            editForm.addEventListener('submit', (e) => this.handleEditFractal(e));
        }

        // Close modals on escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this.hideCreateFractalModal();
                this.hideEditFractalModal();
            }
        });
    },

    setupFractalSettingsEventListeners() {
        // Retention select
        const retentionSelect = document.getElementById('fractalRetentionSelect');
        if (retentionSelect) {
            retentionSelect.addEventListener('change', () => this.saveRetentionSetting());
        }
    },

    show() {
        console.log('Showing Fractal Management view...');
        // Load fractals when view is shown
        this.loadFractals();
    },

    async loadFractals() {
        try {
            this.isLoading = true;
            this.showLoadingState();

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

            this.fractals = data.data.fractals || [];
            this.renderFractals();

        } catch (error) {
            console.error('Failed to load fractals:', error);
            this.showErrorState(error.message);
            Toast.show(`Failed to load fractals: ${error.message}`, 'error');
        } finally {
            this.isLoading = false;
        }
    },

    renderFractals() {
        const container = document.getElementById('fractalsContainer');
        if (!container) {
            console.error('Fractals container not found');
            return;
        }

        if (this.fractals.length === 0) {
            container.innerHTML = this.renderEmptyState();
            return;
        }

        // Calculate pagination
        const totalPages = Math.ceil(this.fractals.length / this.pageSize);
        const start = (this.currentPage - 1) * this.pageSize;
        const end = start + this.pageSize;
        const pageFractals = this.fractals.slice(start, end);

        let html = '<div class="fractals-grid">';

        pageFractals.forEach(fractal => {
            html += this.renderFractalCard(fractal);
        });

        html += '</div>';

        // Add pagination at the bottom if needed
        if (totalPages > 1) {
            html += '<div class="fractals-pagination-container">';
            html += `<div class="fractals-pagination-info">Page ${this.currentPage} of ${totalPages}</div>`;
            html += this.renderPagination(totalPages);
            html += '</div>';
        }

        container.innerHTML = html;
        this.attachFractalCardHandlers();
    },

    renderFractalCard(fractal) {
        const isDefault = fractal.is_default;
        const isProtected = fractal.is_default || fractal.is_system;
        const logCount = fractal.log_count || 0;
        const sizeBytes = fractal.size_bytes || 0;
        const sizeFormatted = this.formatBytes(sizeBytes);
        const createdAt = new Date(fractal.created_at).toLocaleDateString();

        const dateRange = this.formatDateRange(fractal.earliest_log, fractal.latest_log);

        return `
            <div class="fractal-card ${isDefault ? 'default-fractal' : ''}" data-fractal-id="${fractal.id}">
                <div class="fractal-card-header">
                    <div class="fractal-card-title">
                        <h3>
                            ${Utils.escapeHtml(fractal.name)}
                            ${isDefault && fractal.name.toLowerCase() !== 'default' ? '<span class="default-badge">Default</span>' : ''}
                        </h3>
                        ${fractal.description ? `
                            <p class="fractal-card-description">
                                ${Utils.escapeHtml(fractal.description)}
                            </p>
                        ` : ''}
                    </div>
                    <div class="fractal-card-actions">
                        ${!isProtected ? `
                            <button class="btn-icon edit-fractal-btn"
                                    title="Edit Fractal"
                                    onclick="FractalManagement.editFractal('${fractal.id}')">
                                Edit
                            </button>
                            <button class="btn-icon delete-fractal-btn"
                                    title="Delete Fractal"
                                    onclick="FractalManagement.deleteFractal('${Utils.escapeJs(fractal.id)}', '${Utils.escapeJs(fractal.name)}')">
                                Delete
                            </button>
                        ` : ''}
                    </div>
                </div>

                <div class="fractal-card-stats">
                    <div class="stat-grid">
                        <div class="stat-item">
                            <span class="stat-label">Logs</span>
                            <span class="stat-value">${logCount.toLocaleString()}</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">Size</span>
                            <span class="stat-value">${sizeFormatted}</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">Created</span>
                            <span class="stat-value">${createdAt}</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">Created By</span>
                            <span class="stat-value">${Utils.escapeHtml(fractal.created_by)}</span>
                        </div>
                    </div>

                    ${dateRange ? `
                        <div class="date-range">
                            <span class="stat-label">Log Range:</span>
                            <span class="stat-value">${dateRange}</span>
                        </div>
                    ` : ''}
                </div>

                <div class="fractal-card-footer">
                    <button class="btn-secondary btn-sm"
                            onclick="FractalManagement.viewFractalDetails('${fractal.id}')">
                        View Details
                    </button>
                    <button class="btn-secondary btn-sm"
                            onclick="FractalManagement.refreshFractalStats('${fractal.id}')">
                        Refresh Stats
                    </button>
                </div>
            </div>
        `;
    },

    renderEmptyState() {
        return `
            <div class="empty-state">
                <div class="empty-text">No Fractals Configured</div>
                <div class="empty-actions">
                    <button onclick="FractalManagement.showCreateFractalModal()" class="btn-primary">
                        Create Your First Fractal
                    </button>
                </div>
            </div>
        `;
    },

    renderPagination(totalPages) {
        let html = '<div class="pagination">';

        // Previous button
        if (this.currentPage > 1) {
            html += `<button onclick="FractalManagement.goToPage(${this.currentPage - 1})" class="pagination-btn">‹</button>`;
        }

        // Page numbers
        for (let i = 1; i <= totalPages; i++) {
            if (i === this.currentPage) {
                html += `<button class="pagination-btn active">${i}</button>`;
            } else {
                html += `<button onclick="FractalManagement.goToPage(${i})" class="pagination-btn">${i}</button>`;
            }
        }

        // Next button
        if (this.currentPage < totalPages) {
            html += `<button onclick="FractalManagement.goToPage(${this.currentPage + 1})" class="pagination-btn">›</button>`;
        }

        html += '</div>';
        return html;
    },

    attachFractalCardHandlers() {
        // Reattach event listeners for buttons in the newly rendered cards
        // The onclick handlers in the HTML will handle most interactions
    },

    showLoadingState() {
        const container = document.getElementById('fractalsContainer');
        if (container) {
            container.innerHTML = `
                <div class="loading-state">
                    <div class="loading-spinner"></div>
                    <p>Loading fractals...</p>
                </div>
            `;
        }
    },

    showErrorState(error) {
        const container = document.getElementById('fractalsContainer');
        if (container) {
            container.innerHTML = `
                <div class="error-state">
                    <div class="error-icon">⚠️</div>
                    <h3>Failed to Load Fractals</h3>
                    <p>${Utils.escapeHtml(error)}</p>
                    <button onclick="FractalManagement.loadFractals()" class="btn-primary">
                        Retry
                    </button>
                </div>
            `;
        }
    },

    // Modal Management
    showCreateFractalModal() {
        const modal = document.getElementById('createFractalModal');
        if (modal) {
            modal.style.display = 'flex';

            const form = document.getElementById('createFractalForm');
            if (form) form.reset();

            this.setCreateType('fractal');

            const nameInput = document.getElementById('newFractalName');
            if (nameInput) setTimeout(() => nameInput.focus(), 100);
        }
    },

    setCreateType(type) {
        this.createType = type;
        const isFractal = type === 'fractal';
        const title = document.getElementById('createModalTitle');
        const submitBtn = document.getElementById('createSubmitBtn');
        const nameInput = document.getElementById('newFractalName');
        const fractalBtn = document.getElementById('createTypeFractal');
        const prismBtn = document.getElementById('createTypePrism');
        if (title) title.textContent = isFractal ? 'Create New Fractal' : 'Create New Prism';
        if (submitBtn) submitBtn.textContent = isFractal ? 'Create Fractal' : 'Create Prism';
        if (nameInput) nameInput.placeholder = isFractal ? 'Enter fractal name' : 'Enter prism name';
        if (fractalBtn) fractalBtn.classList.toggle('active', isFractal);
        if (prismBtn) prismBtn.classList.toggle('active', !isFractal);
    },

    hideCreateFractalModal() {
        const modal = document.getElementById('createFractalModal');
        if (modal) {
            modal.style.display = 'none';
        }
    },

    showEditFractalModal() {
        const modal = document.getElementById('editFractalModal');
        if (modal) {
            modal.style.display = 'flex';

            // Focus on name input
            const nameInput = document.getElementById('editFractalName');
            if (nameInput) {
                setTimeout(() => nameInput.focus(), 100);
            }
        }
    },

    hideEditFractalModal() {
        const modal = document.getElementById('editFractalModal');
        if (modal) {
            modal.style.display = 'none';
        }
        this.currentEditFractal = null;
    },

    // CRUD Operations
    async handleCreateFractal(event) {
        event.preventDefault();

        const form = event.target;
        const formData = new FormData(form);
        const name = formData.get('name').trim();
        const description = formData.get('description').trim();
        const isPrism = this.createType === 'prism';
        const label = isPrism ? 'Prism' : 'Fractal';

        if (!name) {
            Toast.show(`${label} name is required`, 'error');
            return;
        }

        if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(name)) {
            Toast.show('Name can only contain letters, numbers, hyphens, and underscores, and must start with a letter.', 'error');
            return;
        }

        try {
            const url = isPrism ? '/api/v1/prisms' : '/api/v1/fractals';
            const response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include',
                body: JSON.stringify({ name, description })
            });

            const data = await response.json();

            if (!response.ok || !data.success) {
                throw new Error(data.error || `HTTP ${response.status}`);
            }

            Toast.show(`${label} "${name}" created successfully`, 'success');
            this.hideCreateFractalModal();
            this.loadFractals();

            if (window.FractalSelector) window.FractalSelector.loadAvailableFractals();
            if (window.FractalListing) window.FractalListing.loadFractals();

        } catch (error) {
            console.error(`Failed to create ${label.toLowerCase()}:`, error);
            Toast.show(`Failed to create ${label.toLowerCase()}: ${error.message}`, 'error');
        }
    },

    editFractal(fractalId) {
        const fractal = this.fractals.find(idx => idx.id === fractalId);
        if (!fractal) {
            Toast.show('Fractal not found', 'error');
            return;
        }

        if (fractal.is_default || fractal.is_system) {
            Toast.show('System fractals cannot be renamed', 'warning');
            return;
        }

        this.currentEditFractal = fractal;

        // Populate form
        const nameInput = document.getElementById('editFractalName');
        const descInput = document.getElementById('editFractalDescription');

        if (nameInput) nameInput.value = fractal.name;
        if (descInput) descInput.value = fractal.description || '';

        this.showEditFractalModal();
    },

    async handleEditFractal(event) {
        event.preventDefault();

        if (!this.currentEditFractal) {
            Toast.show('No fractal selected for editing', 'error');
            return;
        }

        const form = event.target;
        const formData = new FormData(form);

        const fractalData = {
            name: formData.get('name').trim(),
            description: formData.get('description').trim()
        };

        if (!fractalData.name) {
            Toast.show('Fractal name is required', 'error');
            return;
        }

        if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(fractalData.name)) {
            Toast.show('Name can only contain letters, numbers, hyphens, and underscores, and must start with a letter.', 'error');
            return;
        }

        try {
            const response = await fetch(`/api/v1/fractals/${this.currentEditFractal.id}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include',
                body: JSON.stringify(fractalData)
            });

            const data = await response.json();

            if (!response.ok || !data.success) {
                throw new Error(data.error || `HTTP ${response.status}`);
            }

            Toast.show(`Fractal "${fractalData.name}" updated successfully`, 'success');
            this.hideEditFractalModal();
            this.loadFractals();

            // Update fractal selector if available
            if (window.FractalSelector) {
                window.FractalSelector.loadAvailableFractals();
            }

            // Update fractal listing if available
            if (window.FractalListing) {
                window.FractalListing.loadFractals();
            }

        } catch (error) {
            console.error('Failed to update fractal:', error);
            Toast.show(`Failed to update fractal: ${error.message}`, 'error');
        }
    },

    async deleteFractal(fractalId, fractalName) {
        const fractal = this.fractals.find(idx => idx.id === fractalId);
        if (!fractal) {
            Toast.show('Fractal not found', 'error');
            return;
        }

        if (fractal.is_default || fractal.is_system) {
            Toast.show('System fractals cannot be deleted', 'error');
            return;
        }

        const confirmed = confirm(
            `Are you sure you want to delete the fractal "${fractalName}"?\n\n` +
            `This will permanently delete:\n` +
            `• All logs in this fractal\n` +
            `• All comments in this fractal\n` +
            `• All alerts in this fractal\n\n` +
            `This action cannot be undone.`
        );

        if (!confirmed) {
            return;
        }

        try {
            const response = await fetch(`/api/v1/fractals/${fractalId}`, {
                method: 'DELETE',
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include'
            });

            const data = await response.json();

            if (!response.ok || !data.success) {
                throw new Error(data.error || `HTTP ${response.status}`);
            }

            Toast.show(`Fractal "${fractalName}" deleted successfully`, 'success');
            this.loadFractals();

            // Update fractal selector if available
            if (window.FractalSelector) {
                window.FractalSelector.loadAvailableFractals();
            }

            // Update fractal listing if available
            if (window.FractalListing) {
                window.FractalListing.loadFractals();
            }

        } catch (error) {
            console.error('Failed to delete fractal:', error);
            Toast.show(`Failed to delete fractal: ${error.message}`, 'error');
        }
    },

    // Statistics and Details
    async refreshFractalStats(fractalId) {
        const fractal = this.fractals.find(idx => idx.id === fractalId);
        if (!fractal) {
            return;
        }

        try {
            Toast.show(`Refreshing statistics for ${fractal.name}...`, 'info');

            const response = await fetch(`/api/v1/fractals/${fractalId}/stats`, {
                method: 'GET',
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include'
            });

            const data = await response.json();

            if (!response.ok || !data.success) {
                throw new Error(data.error || `HTTP ${response.status}`);
            }

            Toast.show(`Statistics refreshed for ${fractal.name}`, 'success');

            // Reload the entire fractal list to get updated stats
            this.loadFractals();

        } catch (error) {
            console.error('Failed to refresh fractal stats:', error);
            Toast.show(`Failed to refresh statistics: ${error.message}`, 'error');
        }
    },

    async refreshAllStats() {
        if (!Auth.currentUser || !Auth.currentUser.is_admin) {
            Toast.show('Only administrators can refresh all statistics', 'error');
            return;
        }

        try {
            Toast.show('Refreshing statistics for all fractals...', 'info');

            const response = await fetch('/api/v1/fractals/stats/refresh', {
                method: 'POST',
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include'
            });

            const data = await response.json();

            if (!response.ok || !data.success) {
                throw new Error(data.error || `HTTP ${response.status}`);
            }

            Toast.show('All statistics refreshed successfully', 'success');

            // Reload the fractal list to get updated stats
            this.loadFractals();

        } catch (error) {
            console.error('Failed to refresh all stats:', error);
            Toast.show(`Failed to refresh all statistics: ${error.message}`, 'error');
        }
    },

    viewFractalDetails(fractalId) {
        const fractal = this.fractals.find(idx => idx.id === fractalId);
        if (!fractal) {
            console.error('Fractal not found:', fractalId);
            return;
        }

        this.currentDetailFractal = fractal;
        this.showFractalDetailsModal(fractal);
    },

    showFractalDetailsModal(fractal) {
        // Update modal title
        const title = document.getElementById('fractalDetailsModalTitle');
        if (title) title.textContent = `Manage Fractal: ${fractal.name}`;

        // Populate basic information
        document.getElementById('detailFractalName').textContent = fractal.name;
        document.getElementById('detailFractalDescription').textContent = fractal.description || 'None';
        document.getElementById('detailFractalCreatedBy').textContent = fractal.created_by;
        document.getElementById('detailFractalCreatedAt').textContent = new Date(fractal.created_at).toLocaleString();
        document.getElementById('detailFractalUpdatedAt').textContent = new Date(fractal.updated_at).toLocaleString();

        // Populate statistics
        document.getElementById('detailFractalLogCount').textContent = (fractal.log_count || 0).toLocaleString();
        document.getElementById('detailFractalSizeBytes').textContent = this.formatBytes(fractal.size_bytes || 0);
        document.getElementById('detailFractalEarliestLog').textContent =
            fractal.earliest_log ? new Date(fractal.earliest_log).toLocaleString() : 'None';
        document.getElementById('detailFractalLatestLog').textContent =
            fractal.latest_log ? new Date(fractal.latest_log).toLocaleString() : 'None';

        // Disable delete button for default/system fractals
        const deleteBtn = document.getElementById('deleteFractalBtn');
        if (deleteBtn) {
            if (fractal.is_default || fractal.is_system) {
                deleteBtn.disabled = true;
                deleteBtn.textContent = 'Cannot Delete (System Fractal)';
                deleteBtn.style.opacity = '0.5';
            } else {
                deleteBtn.disabled = false;
                deleteBtn.textContent = 'Delete Fractal';
                deleteBtn.style.opacity = '1';
            }
        }

        // Populate retention select
        const retentionSelect = document.getElementById('fractalRetentionSelect');
        if (retentionSelect) {
            retentionSelect.value = fractal.retention_days != null ? String(fractal.retention_days) : '';
        }

        // Populate disk quota fields
        const quotaInput = document.getElementById('fractalQuotaInput');
        const quotaActionSelect = document.getElementById('fractalQuotaActionSelect');
        if (quotaInput) {
            quotaInput.value = fractal.disk_quota_bytes != null
                ? (fractal.disk_quota_bytes / (1024 ** 3)).toFixed(2)
                : '';
        }
        if (quotaActionSelect) {
            quotaActionSelect.value = fractal.disk_quota_action || 'reject';
        }

        // Show the modal
        const modal = document.getElementById('fractalDetailsModal');
        if (modal) modal.style.display = 'flex';
    },

    hideFractalDetailsModal() {
        const modal = document.getElementById('fractalDetailsModal');
        if (modal) modal.style.display = 'none';

        const errorDiv = document.getElementById('fractalDetailsError');
        if (errorDiv) errorDiv.style.display = 'none';

        this.currentDetailFractal = null;

        // Clear fractal context when closing the modal
        if (window.FractalContext) {
            FractalContext.clearCurrentFractal();
        }
    },

    async refreshSingleFractalStats() {
        if (!this.currentDetailFractal) return;

        const refreshBtn = document.getElementById('refreshFractalStatsBtn');
        const errorDiv = document.getElementById('fractalDetailsError');

        try {
            if (refreshBtn) {
                refreshBtn.disabled = true;
                refreshBtn.innerHTML = '<span class="spinner"></span> Refreshing...';
            }
            if (errorDiv) errorDiv.style.display = 'none';

            const response = await fetch(`/api/v1/fractals/${this.currentDetailFractal.id}/stats?t=${Date.now()}`, {
                credentials: 'include',
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.error || `Failed to refresh stats: ${response.status}`);
            }

            const data = await response.json();
            console.log('[FractalManagement] Refreshed stats:', data);

            // Update the current fractal data
            const updatedFractal = { ...this.currentDetailFractal, ...data.data };
            this.currentDetailFractal = updatedFractal;

            // Update the fractal in the main list
            const fractalFractal = this.fractals.findFractal(idx => idx.id === updatedFractal.id);
            if (fractalFractal !== -1) {
                this.fractals[fractalFractal] = updatedFractal;
            }

            // Update the modal display
            this.showFractalDetailsModal(updatedFractal);


        } catch (error) {
            console.error('Failed to refresh fractal stats:', error);
            if (errorDiv) {
                errorDiv.textContent = error.message;
                errorDiv.style.display = 'block';
            }
            if (window.Toast) {
                Toast.error('Refresh Failed', error.message);
            }
        } finally {
            if (refreshBtn) {
                refreshBtn.disabled = false;
                refreshBtn.innerHTML = 'Refresh Statistics';
            }
        }
    },

    confirmDeleteFractal() {
        if (!this.currentDetailFractal) return;

        const fractal = this.currentDetailFractal;
        if (fractal.is_default || fractal.is_system) {
            if (window.Toast) {
                Toast.error('Cannot Delete', 'System fractals cannot be deleted');
            }
            return;
        }

        const logCountText = fractal.log_count ? `${fractal.log_count.toLocaleString()} logs` : 'no logs';
        const sizeText = fractal.size_bytes ? this.formatBytes(fractal.size_bytes) : '0 B';

        const message = `Are you sure you want to delete the fractal "${fractal.name}"?\n\n` +
            `This fractal currently contains ${logCountText} (${sizeText}) and this action cannot be undone.\n\n` +
            `Type "${fractal.name}" below to confirm:`;

        const confirmation = prompt(message);
        if (confirmation === fractal.name) {
            this.executeDeleteFractal(fractal.id);
        } else if (confirmation !== null) {
            if (window.Toast) {
                Toast.error('Delete Cancelled', 'Fractal name did not match');
            }
        }
    },

    async executeDeleteFractal(fractalId) {
        if (!this.currentDetailFractal || this.currentDetailFractal.id !== fractalId) return;

        const errorDiv = document.getElementById('fractalDetailsError');
        const deleteBtn = document.getElementById('deleteFractalBtn');

        try {
            if (deleteBtn) {
                deleteBtn.disabled = true;
                deleteBtn.innerHTML = '<span class="spinner"></span> Deleting...';
            }
            if (errorDiv) errorDiv.style.display = 'none';

            const response = await fetch(`/api/v1/fractals/${fractalId}`, {
                method: 'DELETE',
                credentials: 'include'
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.error || `Failed to delete fractal: ${response.status}`);
            }

            console.log('[FractalManagement] Fractal deleted successfully:', fractalId);

            // Remove from local list
            this.fractals = this.fractals.filter(idx => idx.id !== fractalId);

            // Hide modal
            this.hideFractalDetailsModal();

            // Refresh the main view
            this.renderFractals();

            // Update fractal selector if available
            if (window.FractalSelector) {
                window.FractalSelector.loadAvailableFractals();
            }

            // Update fractal listing if available
            if (window.FractalListing) {
                window.FractalListing.loadFractals();
            }


        } catch (error) {
            console.error('Failed to delete fractal:', error);
            if (errorDiv) {
                errorDiv.textContent = error.message;
                errorDiv.style.display = 'block';
            }
            if (window.Toast) {
                Toast.error('Delete Failed', error.message);
            }
        } finally {
            if (deleteBtn) {
                deleteBtn.disabled = false;
                deleteBtn.innerHTML = 'Delete Fractal';
            }
        }
    },

    // Pagination
    goToPage(page) {
        const totalPages = Math.ceil(this.fractals.length / this.pageSize);
        if (page < 1 || page > totalPages) {
            return;
        }

        this.currentPage = page;
        this.renderFractals();
    },

    // Utility Methods
    formatBytes(bytes) {
        if (bytes === 0) return '0 Bytes';

        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));

        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    },

    formatDateRange(earliest, latest) {
        if (!earliest && !latest) {
            return null;
        }

        if (!earliest) {
            return `Until ${new Date(latest).toLocaleDateString()}`;
        }

        if (!latest) {
            return `From ${new Date(earliest).toLocaleDateString()}`;
        }

        const earliestDate = new Date(earliest).toLocaleDateString();
        const latestDate = new Date(latest).toLocaleDateString();

        if (earliestDate === latestDate) {
            return earliestDate;
        }

        return `${earliestDate} - ${latestDate}`;
    },

    async saveRetentionSetting() {
        if (!this.currentDetailFractal) return;

        const select = document.getElementById('fractalRetentionSelect');
        if (!select) return;

        const value = select.value;
        const body = { retention_days: value === '' ? null : parseInt(value, 10) };

        try {
            const response = await fetch(`/api/v1/fractals/${this.currentDetailFractal.id}/retention`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(body)
            });

            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to save retention');

            this.currentDetailFractal.retention_days = body.retention_days;
            Toast.show(
                body.retention_days == null
                    ? 'Retention set to unlimited'
                    : `Retention set to ${body.retention_days} days`,
                'success'
            );
        } catch (error) {
            console.error('Failed to save retention:', error);
            Toast.show(`Failed to save retention: ${error.message}`, 'error');
        }
    },

    async saveDiskQuota() {
        if (!this.currentDetailFractal) return;

        const quotaInput = document.getElementById('fractalQuotaInput');
        const quotaActionSelect = document.getElementById('fractalQuotaActionSelect');
        if (!quotaInput || !quotaActionSelect) return;

        const gbValue = quotaInput.value.trim();
        const action = quotaActionSelect.value;
        const quotaBytes = gbValue === '' ? null : Math.round(parseFloat(gbValue) * (1024 ** 3));

        if (quotaBytes !== null && (isNaN(quotaBytes) || quotaBytes < 1)) {
            Toast.show('Enter a valid quota size or leave empty for no limit', 'error');
            return;
        }

        try {
            const response = await fetch(`/api/v1/fractals/${this.currentDetailFractal.id}/disk-quota`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ quota_bytes: quotaBytes, action })
            });

            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to save quota');

            this.currentDetailFractal.disk_quota_bytes = quotaBytes;
            this.currentDetailFractal.disk_quota_action = action;
            Toast.show(
                quotaBytes == null
                    ? 'Disk quota removed'
                    : `Disk quota set to ${gbValue} GB (${action})`,
                'success'
            );
        } catch (error) {
            console.error('Failed to save disk quota:', error);
            Toast.show(`Failed to save disk quota: ${error.message}`, 'error');
        }
    },

    confirmClearFractalLogs() {
        if (!this.currentDetailFractal) return;

        const fractal = this.currentDetailFractal;
        const logCountText = fractal.log_count ? `${fractal.log_count.toLocaleString()} logs` : 'no logs';

        const confirmation = confirm(
            `Are you sure you want to clear all logs for fractal "${fractal.name}"?\n\n` +
            `This will permanently delete ${logCountText} from this fractal. The fractal structure will remain.\n\n` +
            `This action cannot be undone.`
        );

        if (confirmation) {
            this.executeClearFractalLogs(fractal.id);
        }
    },

    async executeClearFractalLogs(fractalId) {
        if (!this.currentDetailFractal || this.currentDetailFractal.id !== fractalId) return;

        const errorDiv = document.getElementById('fractalDetailsError');
        const clearBtn = document.getElementById('clearFractalLogsBtn');

        try {
            if (clearBtn) {
                clearBtn.disabled = true;
                clearBtn.innerHTML = '<span class="spinner"></span> Clearing logs...';
            }
            if (errorDiv) errorDiv.style.display = 'none';

            // Clear logs for the specific fractal using the existing logs API with fractal_id parameter
            const response = await fetch(`/api/v1/logs?fractal_id=${encodeURIComponent(this.currentDetailFractal.id)}`, {
                method: 'DELETE',
                credentials: 'include',
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });

            const data = await response.json();

            if (data.success) {

                // Refresh the fractal statistics
                await this.refreshSingleFractalStats();

            } else {
                const errorMsg = data.error || 'Unknown error';
                if (window.Toast) {
                    Toast.error('Clear Failed', errorMsg);
                } else {
                    alert('Failed to clear fractal logs: ' + errorMsg);
                }
            }

        } catch (error) {
            console.error('Failed to clear fractal logs:', error);
            if (errorDiv) {
                errorDiv.textContent = error.message;
                errorDiv.style.display = 'block';
            }
            if (window.Toast) {
                Toast.error('Clear Failed', error.message);
            }
        } finally {
            if (clearBtn) {
                clearBtn.disabled = false;
                clearBtn.innerHTML = 'Clear Fractal Logs';
            }
        }
    },

};

// Make globally available
window.FractalManagement = FractalManagement;