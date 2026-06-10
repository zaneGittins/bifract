const QueryTabs = {
    tabs: [],
    activeId: null,
    nextId: 1,

    init() {
        this._createInitialTab();

        const addBtn = document.getElementById('queryTabAdd');
        if (addBtn) addBtn.addEventListener('click', () => this.newTab());

        document.addEventListener('keydown', (e) => {
            if (!e.altKey) return;
            if (e.key === 'n' || e.key === 'N') {
                e.preventDefault();
                this.newTab();
            } else if (e.key === 'w' || e.key === 'W') {
                if (this.tabs.length > 1) {
                    e.preventDefault();
                    this.closeTab(this.activeId);
                }
            } else if (e.key >= '1' && e.key <= '9') {
                const idx = parseInt(e.key) - 1;
                if (this.tabs[idx]) {
                    e.preventDefault();
                    this.switchTo(this.tabs[idx].id);
                }
            }
        });

        // Debounced label update as user types
        const queryInput = document.getElementById('queryInput');
        if (queryInput) {
            let debounce;
            queryInput.addEventListener('input', () => {
                clearTimeout(debounce);
                debounce = setTimeout(() => this._updateActiveLabel(), 400);
            });
        }
    },

    _createInitialTab() {
        const tab = { id: this.nextId++, label: 'Query 1', query: '', timeRange: '24h', customStart: '', customEnd: '' };
        this.tabs.push(tab);
        this.activeId = tab.id;
    },

    newTab() {
        this._saveCurrentState();
        const n = this.tabs.length + 1;
        const tab = {
            id: this.nextId++,
            label: `Query ${n}`,
            query: '',
            timeRange: document.getElementById('timeRangeSelect')?.value || '24h',
            customStart: '',
            customEnd: '',
        };
        this.tabs.push(tab);
        this._applyState(tab);
        this.activeId = tab.id;
        this._renderStrip();
        document.getElementById('queryInput')?.focus();
    },

    closeTab(id) {
        if (this.tabs.length <= 1) return;
        const idx = this.tabs.findIndex(t => t.id === id);
        const wasActive = id === this.activeId;
        this.tabs.splice(idx, 1);
        if (wasActive) {
            const next = this.tabs[Math.min(idx, this.tabs.length - 1)];
            this.activeId = next.id;
            this._applyState(next);
        }
        this._renderStrip();
    },

    switchTo(id) {
        if (id === this.activeId) return;
        this._saveCurrentState();
        this.activeId = id;
        this._applyState(this.tabs.find(t => t.id === id));
        this._renderStrip();
    },

    _saveCurrentState() {
        const tab = this.tabs.find(t => t.id === this.activeId);
        if (!tab) return;
        const q = document.getElementById('queryInput')?.value || '';
        tab.query = q;
        tab.timeRange = document.getElementById('timeRangeSelect')?.value || '24h';
        tab.customStart = document.getElementById('customStart')?.value || '';
        tab.customEnd = document.getElementById('customEnd')?.value || '';
        const derived = this._labelFromQuery(q);
        if (derived) tab.label = derived;
    },

    _applyState(tab) {
        if (!tab) return;

        const queryInput = document.getElementById('queryInput');
        if (queryInput) {
            queryInput.value = tab.query;
            queryInput.dispatchEvent(new Event('input'));
        }

        const tr = document.getElementById('timeRangeSelect');
        if (tr && tr.value !== tab.timeRange) {
            tr.value = tab.timeRange;
            tr.dispatchEvent(new Event('change'));
        }

        const cs = document.getElementById('customStart');
        const ce = document.getElementById('customEnd');
        if (cs) cs.value = tab.customStart;
        if (ce) ce.value = tab.customEnd;

        // Clear results area
        const resultsTable = document.getElementById('resultsTable');
        if (resultsTable) {
            resultsTable.innerHTML = '<div class="empty-state"><svg width="64" height="64" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M32 8L8 20V44L32 56L56 44V20L32 8Z" stroke="currentColor" stroke-width="2" stroke-linejoin="round" opacity="0.3"/><path d="M32 56V32M32 32L8 20M32 32L56 20" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" opacity="0.3"/></svg><p>Enter a query to search logs</p></div>';
        }
        const paginationBar = document.getElementById('paginationBar');
        if (paginationBar) paginationBar.style.display = 'none';
        const resultsCount = document.getElementById('resultsCount');
        if (resultsCount) resultsCount.textContent = '-';
        const execTime = document.getElementById('executionTime');
        if (execTime) execTime.textContent = '';
        const csvBtn = document.getElementById('exportCsvBtn');
        if (csvBtn) csvBtn.style.display = 'none';
        const outputLabel = document.getElementById('outputTypeLabel');
        if (outputLabel) outputLabel.textContent = 'Table';
    },

    _updateActiveLabel() {
        const tab = this.tabs.find(t => t.id === this.activeId);
        if (!tab) return;
        const q = document.getElementById('queryInput')?.value || '';
        const derived = this._labelFromQuery(q);
        if (!derived) return;
        tab.label = derived;
        const el = document.querySelector(`.query-tab[data-id="${tab.id}"] .query-tab-label`);
        if (el) el.textContent = derived;
    },

    _labelFromQuery(query) {
        const line = query.trim().split('\n')[0].trim();
        if (!line) return '';
        return line.length > 28 ? line.slice(0, 28) + '…' : line;
    },

    _renderStrip() {
        const strip = document.getElementById('queryTabsStrip');
        if (!strip) return;

        if (this.tabs.length <= 1) {
            strip.style.display = 'none';
            return;
        }

        strip.style.display = 'flex';
        strip.innerHTML = this.tabs.map((tab, i) => `
            <div class="query-tab${tab.id === this.activeId ? ' active' : ''}" data-id="${tab.id}">
                <span class="query-tab-index">${i + 1}</span>
                <span class="query-tab-label">${this._esc(tab.label)}</span>
                <button class="query-tab-close" data-id="${tab.id}" title="Close tab">×</button>
            </div>
        `).join('') + '<button class="query-tab-add" title="New query tab (Alt+N)">+</button>';

        strip.querySelectorAll('.query-tab').forEach(el => {
            el.addEventListener('click', (e) => {
                if (e.target.closest('.query-tab-close')) return;
                this.switchTo(parseInt(el.dataset.id));
            });
        });

        strip.querySelectorAll('.query-tab-close').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.closeTab(parseInt(btn.dataset.id));
            });
        });

        strip.querySelector('.query-tab-add')?.addEventListener('click', () => this.newTab());

        // Scroll active tab into view
        const activeEl = strip.querySelector('.query-tab.active');
        if (activeEl) activeEl.scrollIntoView({ inline: 'nearest', block: 'nearest' });
    },

    _esc(s) {
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    },
};

window.QueryTabs = QueryTabs;
