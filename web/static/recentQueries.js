// Recent queries dropdown management
const RecentQueries = {
    maxQueries: 20,
    storageKey: 'bifract_recent_queries',
    isOpen: false,

    init() {
        const btn = document.getElementById('recentQueriesBtn');
        const dropdown = document.getElementById('recentQueriesDropdown');
        const clearBtn = document.getElementById('clearRecentBtn');
        const searchInput = document.getElementById('recentQueriesSearch');

        if (btn) {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggle();
            });
        }

        if (clearBtn) {
            clearBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                if (confirm('Clear all recent queries?')) {
                    this.clear();
                }
            });
        }

        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.filterQueries(e.target.value);
            });
            searchInput.addEventListener('click', (e) => {
                e.stopPropagation();
            });
        }

        // Close on click outside
        document.addEventListener('click', (e) => {
            if (dropdown && this.isOpen && !dropdown.contains(e.target)) {
                this.close();
            }
        });

        // Close on escape
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen) {
                this.close();
            }
        });

        this.render();
    },

    toggle() {
        if (this.isOpen) {
            this.close();
        } else {
            this.open();
        }
    },

    open() {
        const dropdown = document.getElementById('recentQueriesDropdown');
        const searchInput = document.getElementById('recentQueriesSearch');

        if (dropdown) {
            dropdown.style.display = 'block';
            this.isOpen = true;
            this.render();

            // Focus search input
            if (searchInput) {
                setTimeout(() => searchInput.focus(), 100);
            }
        }
    },

    close() {
        const dropdown = document.getElementById('recentQueriesDropdown');
        const searchInput = document.getElementById('recentQueriesSearch');

        if (dropdown) {
            dropdown.style.display = 'none';
            this.isOpen = false;

            // Clear search
            if (searchInput) {
                searchInput.value = '';
            }
        }
    },

    add(query) {
        if (!query || query.trim() === '') return;

        let queries = this.getAll();

        // Remove if already exists
        queries = queries.filter(q => q !== query);

        // Add to beginning
        queries.unshift(query);

        // Keep only max
        queries = queries.slice(0, this.maxQueries);

        localStorage.setItem(this.storageKey, JSON.stringify(queries));

        if (this.isOpen) {
            this.render();
        }
    },

    getAll() {
        try {
            const stored = localStorage.getItem(this.storageKey);
            return stored ? JSON.parse(stored) : [];
        } catch (e) {
            return [];
        }
    },

    clear() {
        localStorage.removeItem(this.storageKey);
        this.render();
    },

    filterQueries(searchTerm) {
        const items = document.querySelectorAll('.recent-query-item');
        const term = searchTerm.toLowerCase();

        items.forEach(item => {
            const text = item.textContent.toLowerCase();
            if (text.includes(term)) {
                item.classList.remove('hidden');
            } else {
                item.classList.add('hidden');
            }
        });
    },

    render() {
        const list = document.getElementById('recentQueriesList');
        if (!list) return;

        const queries = this.getAll();

        if (queries.length === 0) {
            list.innerHTML = '<div class="empty-message">No recent queries</div>';
            return;
        }

        list.innerHTML = '';

        queries.forEach(query => {
            const item = document.createElement('div');
            item.className = 'recent-query-item';
            item.textContent = query;
            item.title = query;
            item.addEventListener('click', () => {
                const queryInput = document.getElementById('queryInput');
                if (queryInput) {
                    queryInput.value = query;

                    // Trigger input event to ensure all normal processing happens
                    setTimeout(() => {
                        queryInput.dispatchEvent(new Event('input', { bubbles: true }));
                        queryInput.focus();
                    }, 0);
                }
                this.close();
            });
            list.appendChild(item);
        });
    }
};

// Make globally available
window.RecentQueries = RecentQueries;
