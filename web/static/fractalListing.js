// Fractal Listing View - Main landing page showing all fractals and prisms
const FractalListing = {
    fractals: [],
    prisms: [],
    currentPage: 1,
    pageSize: 20,
    totalPages: 1,
    isLoading: false,
    searchQuery: '',

    init() {
        this.setupEventListeners();
    },

    setupEventListeners() {
        const searchInput = document.getElementById('fractalListingSearch');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.searchQuery = e.target.value.toLowerCase();
                this.filterAndRenderFractals();
            });
        }

        const refreshBtn = document.getElementById('refreshFractalListingBtn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.loadFractals());
        }

        // Create Fractal button
        const createFractalBtn = document.getElementById('createFractalBtn');
        if (createFractalBtn) {
            createFractalBtn.addEventListener('click', () => {
                if (window.FractalManagement) {
                    FractalManagement.showCreateFractalModal();
                }
            });
        }


        // Pagination
        const prevBtn = document.getElementById('fractalListingPrevBtn');
        const nextBtn = document.getElementById('fractalListingNextBtn');

        if (prevBtn) {
            prevBtn.addEventListener('click', () => {
                if (this.currentPage > 1) {
                    this.currentPage--;
                    this.renderFractals();
                }
            });
        }

        if (nextBtn) {
            nextBtn.addEventListener('click', () => {
                if (this.currentPage < this.totalPages) {
                    this.currentPage++;
                    this.renderFractals();
                }
            });
        }
    },

    show() {
        this.loadFractals();
    },

    hide() {
        // No-op; kept for view lifecycle compatibility
    },

    async loadFractals() {
        try {
            this.isLoading = true;
            this.showLoadingState();

            const response = await fetch('/api/v1/fractals', {
                credentials: 'include'
            });

            if (!response.ok) {
                throw new Error(`Failed to load fractals: ${response.status}`);
            }

            const data = await response.json();


            // Response structure: { success: true, data: { fractals: [...], prisms: [...], total: N } }
            if (data.success && data.data) {
                this.fractals = data.data.fractals || [];
                this.prisms = data.data.prisms || [];
            } else if (data.fractals) {
                this.fractals = data.fractals || [];
                this.prisms = [];
            } else {
                this.fractals = [];
                this.prisms = [];
            }

            this.filterAndRenderFractals();
        } catch (error) {
            console.error('Failed to load fractals:', error);
            this.showError('Failed to load fractals: ' + error.message);
        } finally {
            this.isLoading = false;
        }
    },

    filterAndRenderFractals() {
        let filteredFractals = this.fractals;
        let filteredPrisms = this.prisms;

        if (this.searchQuery) {
            filteredFractals = this.fractals.filter(f =>
                f.name.toLowerCase().includes(this.searchQuery) ||
                (f.description && f.description.toLowerCase().includes(this.searchQuery))
            );
            filteredPrisms = this.prisms.filter(p =>
                p.name.toLowerCase().includes(this.searchQuery) ||
                (p.description && p.description.toLowerCase().includes(this.searchQuery))
            );
        }

        const allItems = [
            ...filteredFractals.map(f => ({ ...f, _type: 'fractal' })),
            ...filteredPrisms.map(p => ({ ...p, _type: 'prism' })),
        ];

        this.totalPages = Math.ceil(allItems.length / this.pageSize);
        if (this.currentPage > this.totalPages) {
            this.currentPage = Math.max(1, this.totalPages);
        }

        const startIdx = (this.currentPage - 1) * this.pageSize;
        const pageItems = allItems.slice(startIdx, startIdx + this.pageSize);

        this.renderFractals(pageItems, allItems.length);
    },

    renderFractals(items = [], totalCount = 0) {
        const container = document.getElementById('fractalListingContainer');
        if (!container) return;

        if (items.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <div class="empty-icon">⬢</div>
                    <p class="empty-message">${this.searchQuery ? 'No items match your search' : 'No fractals found'}</p>
                    ${!this.searchQuery ? '<p class="empty-hint">Click "Create Fractal" to get started, or check if the server is running properly.</p>' : ''}
                </div>
            `;
            this.updatePaginationUI(0, 0);
            return;
        }

        let html = `
            <div class="fractal-listing-table">
                <table>
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>Size</th>
                            <th>Retention</th>
                            <th>Archive</th>
                            <th>Latest Event</th>
                        </tr>
                    </thead>
                    <tbody>
        `;

        items.forEach(item => {
            if (item._type === 'prism') {
                const memberCount = item.member_count || 0;
                html += `
                    <tr onclick="FractalListing.openPrism('${Utils.escapeJs(item.id)}', '${Utils.escapeJs(item.name)}')">
                        <td>
                            <span class="fractal-name">${Utils.escapeHtml(item.name)}</span>
                            <span class="prism-badge">PRISM</span>
                            <span class="fractal-size" style="margin-left:6px;">${memberCount} fractal${memberCount !== 1 ? 's' : ''}</span>
                        </td>
                        <td><span class="fractal-size">--</span></td>
                        <td><span class="fractal-size">--</span></td>
                        <td><span class="fractal-size">--</span></td>
                        <td><span class="fractal-latest">--</span></td>
                    </tr>
                `;
            } else {
                const storageSize = item.size_bytes ? this.formatBytes(item.size_bytes) : '0 B';
                const latestEvent = this.formatLatestEvent(item.latest_log);
                html += `
                    <tr data-fractal-id="${item.id}" onclick="FractalListing.openFractal('${Utils.escapeJs(item.id)}', '${Utils.escapeJs(item.name)}')">
                        <td>
                            <span class="fractal-name">${Utils.escapeHtml(item.name)}</span>
                        </td>
                        <td>
                            <span class="fractal-size">${storageSize}</span>
                        </td>
                        <td>
                            <span class="fractal-size">${this.formatRetention(item.retention_days)}</span>
                        </td>
                        <td>
                            <span class="fractal-size">${this.formatArchive(item.archive_schedule)}</span>
                        </td>
                        <td>
                            <span class="fractal-latest ${this.getLatestEventClass(item.latest_log)}">${latestEvent}</span>
                        </td>
                    </tr>
                `;
            }
        });

        html += `
                    </tbody>
                </table>
            </div>
        `;

        container.innerHTML = html;
        this.updatePaginationUI(totalCount, items.length);
    },

    updatePaginationUI(totalCount, currentPageCount) {
        const pageInfo = document.getElementById('fractalListingPageInfo');
        const prevBtn = document.getElementById('fractalListingPrevBtn');
        const nextBtn = document.getElementById('fractalListingNextBtn');

        if (pageInfo) {
            const start = totalCount === 0 ? 0 : ((this.currentPage - 1) * this.pageSize) + 1;
            const end = start + currentPageCount - 1;
            pageInfo.textContent = `${start}-${end} of ${totalCount}`;
        }

        if (prevBtn) {
            prevBtn.disabled = this.currentPage <= 1;
        }

        if (nextBtn) {
            nextBtn.disabled = this.currentPage >= this.totalPages;
        }
    },

    async openFractal(fractalId, fractalName) {
        const fractal = this.fractals.find(idx => idx.id === fractalId);
        if (!fractal || !window.FractalContext) {
            console.error('[FractalListing] Fractal not found or FractalContext not available');
            return;
        }
        // Await so the server session is actually on the new fractal BEFORE
        // any tab show() handler fires a scoped list request.
        await FractalContext.setCurrentFractal(fractal);
        if (window.App) {
            App.showFractalView();
        }
    },

    async openPrism(prismId, prismName) {
        const prism = this.prisms.find(p => p.id === prismId);
        if (!prism || !window.FractalContext) {
            console.error('[FractalListing] Prism not found or FractalContext not available');
            return;
        }
        await FractalContext.setCurrentPrism(prism);
        if (window.App) {
            App.showFractalView();
        }
    },

    formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    },

    formatRetention(days) {
        if (days == null || days === 0) return 'Unlimited';
        return `${days} day${days !== 1 ? 's' : ''}`;
    },

    formatArchive(schedule) {
        if (!schedule || schedule === 'never') return 'Never';
        return schedule.charAt(0).toUpperCase() + schedule.slice(1);
    },

    formatLatestEvent(latestLog) {
        if (!latestLog) return 'No events';

        const eventDate = new Date(latestLog);
        const now = new Date();
        const diffMs = now - eventDate;
        const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
        const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
        const diffMinutes = Math.floor(diffMs / (1000 * 60));

        if (diffMinutes < 5) {
            return 'Just now';
        } else if (diffMinutes < 60) {
            return `${diffMinutes}m ago`;
        } else if (diffHours < 24) {
            return `${diffHours}h ago`;
        } else if (diffDays < 7) {
            return `${diffDays}d ago`;
        } else {
            return eventDate.toLocaleDateString();
        }
    },

    getLatestEventClass(latestLog) {
        if (!latestLog) return 'very-old';

        const eventDate = new Date(latestLog);
        const now = new Date();
        const diffMs = now - eventDate;
        const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
        const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

        if (diffHours < 1) {
            return 'recent';  // Green for very recent events
        } else if (diffDays < 1) {
            return '';        // Default color for events within 24 hours
        } else if (diffDays < 7) {
            return 'old';     // Yellow/warning for events older than 1 day
        } else {
            return 'very-old'; // Muted for very old events
        }
    },

    showLoadingState() {
        const container = document.getElementById('fractalListingContainer');
        if (container) {
            container.innerHTML = '<div class="loading">Loading fractals...</div>';
        }
    },

    showError(message) {
        const container = document.getElementById('fractalListingContainer');
        if (container) {
            container.innerHTML = `<div class="error-message">${Utils.escapeHtml(message)}</div>`;
        }
    },

};

// Make globally available
window.FractalListing = FractalListing;
