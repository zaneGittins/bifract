// Query Reference View module

const QueryReference = {
    functions: [],
    operators: [],
    activeCategory: null,
    expandedCards: new Set(),

    async init() {
        const referenceTab = document.getElementById('referenceTabBtn');
        const searchTab = document.getElementById('searchTabBtn');
        const commentedTab = document.getElementById('commentedTabBtn');
        const settingsTab = document.getElementById('settingsTabBtn');

        if (referenceTab) referenceTab.addEventListener('click', () => this.show());
        if (searchTab) searchTab.addEventListener('click', () => this.hide());
        if (commentedTab) commentedTab.addEventListener('click', () => this.hide());
        if (settingsTab) settingsTab.addEventListener('click', () => this.hide());
    },

    async show() {
        const searchView = document.getElementById('searchView');
        const commentedView = document.getElementById('commentedView');
        const alertsView = document.getElementById('alertsView');
        const alertEditorView = document.getElementById('alertEditorView');
        const settingsView = document.getElementById('settingsView');
        const referenceView = document.getElementById('referenceView');
        const actionsManageView = document.getElementById('actionsManageView');

        const searchTab = document.getElementById('searchTabBtn');
        const commentedTab = document.getElementById('commentedTabBtn');
        const alertsTab = document.getElementById('alertsTabBtn');
        const settingsTab = document.getElementById('settingsTabBtn');
        const referenceTab = document.getElementById('referenceTabBtn');

        if (searchView) searchView.style.display = 'none';
        if (commentedView) commentedView.style.display = 'none';
        if (alertsView) alertsView.style.display = 'none';
        if (alertEditorView) alertEditorView.style.display = 'none';
        if (actionsManageView) actionsManageView.style.display = 'none';
        if (settingsView) settingsView.style.display = 'none';
        if (referenceView) referenceView.style.display = 'block';

        if (searchTab) searchTab.classList.remove('active');
        if (commentedTab) commentedTab.classList.remove('active');
        if (alertsTab) alertsTab.classList.remove('active');
        if (settingsTab) settingsTab.classList.remove('active');
        if (referenceTab) referenceTab.classList.add('active');

        await this.loadReference();
    },

    hide() {
        const referenceView = document.getElementById('referenceView');
        if (referenceView) referenceView.style.display = 'none';
    },

    async loadReference() {
        try {
            const response = await fetch('/api/v1/query/reference', { credentials: 'include' });
            const data = await response.json();
            this.functions = data.functions || [];
            this.operators = data.operators || [];
            this.renderReference();
        } catch (error) {
            console.error('Error loading query reference:', error);
            this.renderError('Failed to load query reference');
        }
    },

    getCategories() {
        const cats = {};
        this.functions.forEach(fn => {
            if (!cats[fn.category]) cats[fn.category] = [];
            cats[fn.category].push(fn);
        });
        return cats;
    },

    renderReference() {
        const container = document.getElementById('referenceContent');
        if (!container) return;

        const categories = this.getCategories();
        const sortedCats = Object.keys(categories).sort();

        // Category order: put most useful first
        const catOrder = ['Aggregation', 'Transformation', 'Filtering', 'Detection', 'Visualization', 'Display', 'Ordering', 'Limiting', 'Extraction', 'Enrichment', 'Traversal'];
        sortedCats.sort((a, b) => {
            const ai = catOrder.indexOf(a);
            const bi = catOrder.indexOf(b);
            if (ai === -1 && bi === -1) return a.localeCompare(b);
            if (ai === -1) return 1;
            if (bi === -1) return -1;
            return ai - bi;
        });

        let html = `
            <div class="ref-layout">
                <nav class="ref-sidebar">
                    <div class="ref-sidebar-section">
                        <div class="ref-sidebar-label">Categories</div>
                        <button class="ref-cat-btn ${!this.activeCategory ? 'active' : ''}" onclick="QueryReference.setCategory(null)">
                            All <span class="ref-cat-count">${this.functions.length}</span>
                        </button>
                        ${sortedCats.map(cat => `
                            <button class="ref-cat-btn ${this.activeCategory === cat ? 'active' : ''}" onclick="QueryReference.setCategory('${cat}')">
                                ${cat} <span class="ref-cat-count">${categories[cat].length}</span>
                            </button>
                        `).join('')}
                        <div class="ref-sidebar-divider"></div>
                        <button class="ref-cat-btn ${this.activeCategory === '_operators' ? 'active' : ''}" onclick="QueryReference.setCategory('_operators')">
                            Operators <span class="ref-cat-count">${this.operators.length}</span>
                        </button>
                    </div>
                </nav>
                <div class="ref-main">
                    <div class="ref-search-bar">
                        <input type="text" id="refSearchInput" class="ref-search-input" placeholder="Search functions, operators, descriptions..." oninput="QueryReference.filterReference(this.value)">
                        <span class="ref-result-count" id="refResultCount"></span>
                    </div>
                    <div id="refFunctionList" class="ref-function-list">
                        ${this.renderFunctionList(sortedCats, categories)}
                    </div>
                </div>
            </div>
        `;

        container.innerHTML = html;

        // Migrate search value from old input if present
        const oldInput = document.getElementById('referenceSearchInput');
        if (oldInput && oldInput.value) {
            const newInput = document.getElementById('refSearchInput');
            if (newInput) {
                newInput.value = oldInput.value;
                this.filterReference(oldInput.value);
            }
        }
    },

    renderFunctionList(sortedCats, categories) {
        const showOperators = !this.activeCategory || this.activeCategory === '_operators';
        const showFunctions = this.activeCategory !== '_operators';

        let html = '';

        if (showFunctions) {
            const catsToShow = this.activeCategory && this.activeCategory !== '_operators'
                ? [this.activeCategory]
                : sortedCats;

            catsToShow.forEach(cat => {
                if (!categories[cat]) return;
                html += `
                    <div class="ref-cat-group" data-category="${cat}">
                        <div class="ref-cat-header">${cat}<span class="ref-cat-header-count">${categories[cat].length}</span></div>
                        ${categories[cat].map(fn => this.renderCompactFunction(fn)).join('')}
                    </div>
                `;
            });
        }

        if (showOperators) {
            html += `
                <div class="ref-cat-group" data-category="_operators">
                    <div class="ref-cat-header">Operators<span class="ref-cat-header-count">${this.operators.length}</span></div>
                    ${this.operators.map(op => this.renderCompactOperator(op)).join('')}
                </div>
            `;
        }

        return html;
    },

    renderCompactFunction(fn) {
        const isExpanded = this.expandedCards.has(fn.name);
        return `
            <div class="ref-item ${isExpanded ? 'expanded' : ''}" data-name="${fn.name}" data-category="${fn.category}">
                <div class="ref-item-header" onclick="QueryReference.toggleExpand('${fn.name}')">
                    <div class="ref-item-title">
                        <code class="ref-item-name">${fn.name}</code>
                        <span class="ref-item-desc">${fn.description}</span>
                    </div>
                    <svg class="ref-chevron" width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M6 4l4 4-4 4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>
                </div>
                <div class="ref-item-detail" ${isExpanded ? '' : 'style="display:none"'}>
                    <div class="ref-detail-syntax">
                        <code>${Utils.escapeHtml(fn.syntax)}</code>
                    </div>
                    ${fn.parameters.length > 0 ? `
                        <div class="ref-detail-params">
                            ${fn.parameters.map(p => `
                                <div class="ref-param-row">
                                    <code>${p.name}</code>
                                    <span class="ref-param-type">${p.type}</span>
                                    ${p.required ? '<span class="ref-param-req">required</span>' : '<span class="ref-param-opt">optional</span>'}
                                    <span class="ref-param-desc">${p.description}</span>
                                </div>
                            `).join('')}
                        </div>
                    ` : ''}
                    <div class="ref-detail-examples">
                        ${fn.examples.map(ex => `
                            <div class="ref-example">
                                <code>${Utils.escapeHtml(ex)}</code>
                                <button class="ref-copy-btn" onclick="QueryReference.copyToQuery('${this.escapeForJs(ex)}')" title="Use in query">
                                    <svg width="13" height="13" viewBox="0 0 16 16" fill="none"><path d="M13 5.5a.5.5 0 0 1 .5.5v8a.5.5 0 0 1-.5.5H6a.5.5 0 0 1-.5-.5V6a.5.5 0 0 1 .5-.5h7zM6 4a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h7a2 2 0 0 0 2-2V6a2 2 0 0 0-2-2H6z" stroke="currentColor" fill="none"/><path d="M4 3a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1" stroke="currentColor" fill="none"/></svg>
                                </button>
                            </div>
                        `).join('')}
                    </div>
                </div>
            </div>
        `;
    },

    renderCompactOperator(op) {
        const key = '_op_' + op.operator;
        const isExpanded = this.expandedCards.has(key);
        return `
            <div class="ref-item ref-item-operator ${isExpanded ? 'expanded' : ''}" data-operator="${op.operator}">
                <div class="ref-item-header" onclick="QueryReference.toggleExpand('${this.escapeForJs(key)}')">
                    <div class="ref-item-title">
                        <code class="ref-item-name ref-op-name">${Utils.escapeHtml(op.operator)}</code>
                        <span class="ref-item-desc">${op.description}</span>
                    </div>
                    <svg class="ref-chevron" width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M6 4l4 4-4 4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>
                </div>
                <div class="ref-item-detail" ${isExpanded ? '' : 'style="display:none"'}>
                    <div class="ref-detail-examples">
                        ${op.examples.map(ex => `
                            <div class="ref-example">
                                <code>${Utils.escapeHtml(ex)}</code>
                                <button class="ref-copy-btn" onclick="QueryReference.copyToQuery('${this.escapeForJs(ex)}')" title="Use in query">
                                    <svg width="13" height="13" viewBox="0 0 16 16" fill="none"><path d="M13 5.5a.5.5 0 0 1 .5.5v8a.5.5 0 0 1-.5.5H6a.5.5 0 0 1-.5-.5V6a.5.5 0 0 1 .5-.5h7zM6 4a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h7a2 2 0 0 0 2-2V6a2 2 0 0 0-2-2H6z" stroke="currentColor" fill="none"/><path d="M4 3a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1" stroke="currentColor" fill="none"/></svg>
                                </button>
                            </div>
                        `).join('')}
                    </div>
                </div>
            </div>
        `;
    },

    toggleExpand(name) {
        if (this.expandedCards.has(name)) {
            this.expandedCards.delete(name);
        } else {
            this.expandedCards.add(name);
        }

        // Find the item and toggle detail visibility
        const items = document.querySelectorAll('.ref-item');
        items.forEach(item => {
            const itemName = item.dataset.name || ('_op_' + item.dataset.operator);
            if (itemName === name) {
                const detail = item.querySelector('.ref-item-detail');
                if (detail) {
                    if (this.expandedCards.has(name)) {
                        item.classList.add('expanded');
                        detail.style.display = '';
                    } else {
                        item.classList.remove('expanded');
                        detail.style.display = 'none';
                    }
                }
            }
        });
    },

    setCategory(cat) {
        this.activeCategory = cat;
        this.renderReference();
    },

    filterReference(searchTerm) {
        const term = searchTerm.toLowerCase().trim();
        const items = document.querySelectorAll('.ref-item');
        const groups = document.querySelectorAll('.ref-cat-group');
        let visibleCount = 0;

        items.forEach(item => {
            const name = (item.dataset.name || item.dataset.operator || '').toLowerCase();
            const text = item.textContent.toLowerCase();
            const visible = !term || name.includes(term) || text.includes(term);
            item.style.display = visible ? '' : 'none';
            if (visible) visibleCount++;
        });

        // Hide empty category groups
        groups.forEach(group => {
            const visibleItems = group.querySelectorAll('.ref-item:not([style*="display: none"])');
            group.style.display = visibleItems.length > 0 ? '' : 'none';
        });

        const countEl = document.getElementById('refResultCount');
        if (countEl) {
            countEl.textContent = term ? `${visibleCount} result${visibleCount !== 1 ? 's' : ''}` : '';
        }
    },

    copyToQuery(example) {
        const queryInput = document.getElementById('queryInput');
        if (queryInput) {
            queryInput.value = example;
            const searchTab = document.getElementById('searchTabBtn');
            if (searchTab) searchTab.click();
            queryInput.focus();
        }
    },

    renderError(message) {
        const container = document.getElementById('referenceContent');
        if (!container) return;
        container.innerHTML = `
            <div class="error-state">
                <div class="error-message">${Utils.escapeHtml(message)}</div>
            </div>
        `;
    },

    escapeForJs(text) {
        return text.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '\\"');
    }
};

document.addEventListener('DOMContentLoaded', () => {
    QueryReference.init();
});

window.QueryReference = QueryReference;
