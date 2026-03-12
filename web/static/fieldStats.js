// Field statistics sidebar for search results
const FieldStats = {
    isOpen: false,
    stats: {},
    sortMode: 'cardinality', // 'cardinality' or 'alpha'
    expandedFields: new Set(),
    filterText: '',

    init() {
        const toggleBtn = document.getElementById('fieldStatsBtn');
        if (toggleBtn) {
            toggleBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggle();
            });
        } else {
            console.error('[FieldStats] Toggle button not found');
        }
    },

    toggle() {
        this.isOpen = !this.isOpen;
        const drawer = document.getElementById('fieldStatsDrawer');
        if (!drawer) return;

        if (this.isOpen) {
            drawer.classList.add('open');
            this.refresh();
        } else {
            drawer.classList.remove('open');
        }
    },

    // Always compute stats for autocomplete, only render when sidebar is open
    refreshStats() {
        const results = QueryExecutor.currentResults;
        if (!results || results.length === 0) {
            this.stats = {};
            return;
        }
        this.compute(results);
    },

    refresh() {
        this.refreshStats();
        if (!this.isOpen) return;
        const results = QueryExecutor.currentResults;
        if (!results || results.length === 0) {
            this.renderEmpty();
            return;
        }
        // Auto-expand all fields in drawer mode
        for (const field of Object.keys(this.stats)) {
            this.expandedFields.add(field);
        }
        this.render();
    },

    compute(results) {
        this.stats = {};
        const total = results.length;

        // Flatten each row: unpack JSON object columns (like `fields`) into individual keys
        const flatRows = [];
        const skipKeys = new Set(['raw_log', '_all_fields', 'fields']);
        const allFields = new Set();

        for (const row of results) {
            const flat = {};
            for (const [key, val] of Object.entries(row)) {
                if (key === 'raw_log' || key === '_all_fields') continue;
                if (key === 'fields' && typeof val === 'object' && val !== null && !Array.isArray(val)) {
                    // Unpack the fields JSON object
                    for (const [subKey, subVal] of Object.entries(val)) {
                        flat[subKey] = subVal;
                        allFields.add(subKey);
                    }
                } else {
                    flat[key] = val;
                    allFields.add(key);
                }
            }
            flatRows.push(flat);
        }

        for (const field of allFields) {
            const valueCounts = {};
            let present = 0;
            let nullCount = 0;

            for (const row of flatRows) {
                const val = row[field];
                if (val === undefined || val === null) {
                    nullCount++;
                    continue;
                }
                present++;
                const key = typeof val === 'object' ? JSON.stringify(val) : String(val);
                valueCounts[key] = (valueCounts[key] || 0) + 1;
            }

            // Sort by count descending, take top 10
            const sorted = Object.entries(valueCounts)
                .sort((a, b) => b[1] - a[1]);
            const top = sorted.slice(0, 10);
            const uniqueCount = sorted.length;

            this.stats[field] = {
                total,
                present,
                nullCount,
                uniqueCount,
                topValues: top,
            };
        }
    },

    getFieldsSorted() {
        const fields = Object.keys(this.stats);
        if (this.sortMode === 'alpha') {
            return fields.sort((a, b) => a.localeCompare(b));
        }
        // Sort by cardinality descending
        return fields.sort((a, b) => this.stats[b].uniqueCount - this.stats[a].uniqueCount);
    },

    renderEmpty() {
        const body = document.getElementById('fieldStatsBody');
        if (!body) return;
        body.innerHTML = '<div class="fs-empty">No results to analyze</div>';
    },

    render() {
        const body = document.getElementById('fieldStatsBody');
        if (!body) return;

        const fields = this.getFieldsSorted();
        const filterLower = this.filterText.toLowerCase();

        let html = '';
        for (const field of fields) {
            if (filterLower && !field.toLowerCase().includes(filterLower)) continue;

            const stat = this.stats[field];
            const isExpanded = this.expandedFields.has(field);
            const coverage = Math.round((stat.present / stat.total) * 100);

            html += `<div class="fs-field${isExpanded ? ' expanded' : ''}" data-field="${this.escapeAttr(field)}">`;
            html += `<div class="fs-field-header" data-field="${this.escapeAttr(field)}">`;
            html += `<span class="fs-field-arrow">${isExpanded ? '\u25BC' : '\u25B6'}</span>`;
            html += `<span class="fs-field-name">${this.escapeHtml(field)}</span>`;
            html += `<span class="fs-field-badge">${stat.uniqueCount}</span>`;
            html += `</div>`;

            if (isExpanded) {
                html += `<div class="fs-field-detail">`;
                if (stat.nullCount > 0) {
                    html += `<div class="fs-coverage">Coverage: ${coverage}% (${stat.nullCount} null)</div>`;
                }
                for (const [val, count] of stat.topValues) {
                    const pct = ((count / stat.total) * 100).toFixed(1);
                    const barWidth = (count / stat.topValues[0][1]) * 100;
                    const displayVal = val.length > 40 ? val.substring(0, 40) + '...' : val;
                    html += `<div class="fs-value-row" data-field="${this.escapeAttr(field)}" data-value="${this.escapeAttr(val)}">`;
                    html += `<div class="fs-value-bar" style="width: ${barWidth}%"></div>`;
                    html += `<span class="fs-value-text" title="${this.escapeAttr(val)}">${this.escapeHtml(displayVal)}</span>`;
                    html += `<span class="fs-value-actions">`;
                    html += `<button class="fs-action-btn fs-filter-in" title="Filter in">+</button>`;
                    html += `<button class="fs-action-btn fs-filter-out" title="Filter out">&minus;</button>`;
                    html += `</span>`;
                    html += `<span class="fs-value-count">${count} (${pct}%)</span>`;
                    html += `</div>`;
                }
                if (stat.uniqueCount > 10) {
                    html += `<div class="fs-more">${stat.uniqueCount - 10} more unique values</div>`;
                }
                html += `</div>`;
            }
            html += `</div>`;
        }

        body.innerHTML = html;

        // Event delegation for field headers and value clicks
        body.onclick = (e) => {
            const header = e.target.closest('.fs-field-header');
            if (header) {
                const field = header.dataset.field;
                if (this.expandedFields.has(field)) {
                    this.expandedFields.delete(field);
                } else {
                    this.expandedFields.add(field);
                }
                this.render();
                return;
            }

            const filterIn = e.target.closest('.fs-filter-in');
            const filterOut = e.target.closest('.fs-filter-out');
            if (filterIn || filterOut) {
                const valueRow = e.target.closest('.fs-value-row');
                if (valueRow) {
                    this.addFilter(valueRow.dataset.field, valueRow.dataset.value, !!filterOut);
                }
            }
        };
    },

    addFilter(field, value, exclude = false) {
        const input = document.getElementById('queryInput');
        if (!input) return;

        const current = input.value.trim();
        let filter;
        const op = exclude ? '!=' : '=';
        if (/^\d+(\.\d+)?$/.test(value)) {
            filter = `${field}${op}${value}`;
        } else {
            filter = `${field}${op}"${value.replace(/"/g, '\\"')}"`;
        }

        if (current) {
            input.value = current + ` ${filter}`;
        } else {
            input.value = filter;
        }

        input.dispatchEvent(new Event('input', { bubbles: true }));

        if (window.Toast) {
            Toast.show(`${exclude ? 'Excluded' : 'Added'}: ${filter}`, 'info');
        }
    },

    handleFilter(value) {
        this.filterText = value;
        this.render();
    },

    toggleSort() {
        this.sortMode = this.sortMode === 'cardinality' ? 'alpha' : 'cardinality';
        const sortBtn = document.getElementById('fieldStatsSortBtn');
        if (sortBtn) {
            sortBtn.title = this.sortMode === 'cardinality' ? 'Sort alphabetically' : 'Sort by cardinality';
            sortBtn.textContent = this.sortMode === 'cardinality' ? '#' : 'Az';
        }
        this.render();
    },

    escapeHtml(str) {
        if (window.Utils) return Utils.escapeHtml(str);
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    },

    escapeAttr(str) {
        if (window.Utils) return Utils.escapeAttr(str);
        return str.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }
};

// Make globally available
window.FieldStats = FieldStats;
