// Log detail side panel
const LogDetail = {
    init() {
        const closeBtn = document.getElementById('closePanelBtn');
        const panel = document.getElementById('logDetailPanel');
        const sendToChatBtn = document.getElementById('sendToChatBtn');

        if (closeBtn) {
            closeBtn.addEventListener('click', () => this.close());
        }

        if (sendToChatBtn) {
            sendToChatBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                if (this.currentLogData && window.Chat) {
                    this.close();
                    Chat.analyzeLog(this.currentLogData);
                }
            });
        }

        // Close on click outside
        if (panel) {
            document.addEventListener('click', (e) => {
                const commentsPanel = document.getElementById('commentsPanel');
                if (panel.classList.contains('open') &&
                    !panel.contains(e.target) &&
                    !e.target.closest('.results-table') &&
                    !(commentsPanel && commentsPanel.contains(e.target))) {
                    this.close();
                }
            });
        }

        // Close on Escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && panel && panel.classList.contains('open')) {
                this.close();
            }
        });
    },

    async show(logData, isAggregated = false) {
        const panel = document.getElementById('logDetailPanel');
        const content = document.getElementById('logDetailContent');

        if (!panel || !content) return;

        this.currentLogData = logData;
        content.innerHTML = '';

        // Build tabs
        const tabBar = document.createElement('div');
        tabBar.className = 'log-detail-tabs';

        const fieldsTab = document.createElement('button');
        fieldsTab.className = 'log-detail-tab active';
        fieldsTab.textContent = 'Fields';
        fieldsTab.dataset.tab = 'fields';

        const rawTab = document.createElement('button');
        rawTab.className = 'log-detail-tab';
        rawTab.textContent = 'Raw Log';
        rawTab.dataset.tab = 'raw';

        tabBar.appendChild(fieldsTab);
        tabBar.appendChild(rawTab);
        content.appendChild(tabBar);

        // Fields tab content
        const fieldsPane = document.createElement('div');
        fieldsPane.className = 'log-detail-tab-content active';
        fieldsPane.dataset.tab = 'fields';

        const searchContainer = document.createElement('div');
        searchContainer.className = 'log-detail-search';
        const searchInput = document.createElement('input');
        searchInput.type = 'text';
        searchInput.placeholder = 'Filter fields...';
        searchInput.className = 'log-detail-search-input';
        searchInput.addEventListener('input', (e) => {
            this.filterFields(e.target.value);
        });
        searchContainer.appendChild(searchInput);
        fieldsPane.appendChild(searchContainer);

        const fieldsContainer = document.createElement('div');
        fieldsContainer.className = 'log-fields-container';
        fieldsPane.appendChild(fieldsContainer);
        content.appendChild(fieldsPane);

        // Raw log tab content
        const rawPane = document.createElement('div');
        rawPane.className = 'log-detail-tab-content';
        rawPane.dataset.tab = 'raw';

        const rawDiv = document.createElement('div');
        rawDiv.className = 'raw-log-content';
        this.renderRawLog(logData, rawDiv);
        rawPane.appendChild(rawDiv);
        content.appendChild(rawPane);

        // Tab switching
        tabBar.addEventListener('click', (e) => {
            const tab = e.target.closest('.log-detail-tab');
            if (!tab) return;
            const target = tab.dataset.tab;
            tabBar.querySelectorAll('.log-detail-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            content.querySelectorAll('.log-detail-tab-content').forEach(p => p.classList.remove('active'));
            content.querySelector(`.log-detail-tab-content[data-tab="${target}"]`).classList.add('active');
        });

        // Render fields (excluding raw_log now)
        this.renderFields(logData, fieldsContainer);

        panel.classList.add('open');

        if (isAggregated) return;

        if (window.Comments && window.Auth && window.Auth.isAuthenticated()) {
            window.Comments.openPanel(logData, isAggregated);
        }
    },

    renderRawLog(logData, container) {
        const rawValue = logData.raw_log || '';
        if (!rawValue) {
            container.textContent = 'No raw log available';
            return;
        }

        const copyBtn = document.createElement('button');
        copyBtn.className = 'raw-log-copy-btn';
        copyBtn.title = 'Copy raw log';
        copyBtn.innerHTML = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none"><rect x="5" y="2" width="8" height="10" rx="1" stroke="currentColor" stroke-width="1.3"/><path d="M3 5v8a1 1 0 001 1h6" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/></svg>';

        let copyText = rawValue;
        try {
            const parsed = JSON.parse(rawValue);
            const jsonStr = JSON.stringify(parsed, null, 2);
            copyText = jsonStr;
            const pre = document.createElement('pre');
            if (window.QueryExecutor && window.QueryExecutor.highlightJSON) {
                pre.innerHTML = window.QueryExecutor.highlightJSON(jsonStr);
            } else {
                pre.textContent = jsonStr;
            }
            container.appendChild(pre);
        } catch (e) {
            const pre = document.createElement('pre');
            pre.textContent = rawValue;
            container.appendChild(pre);
        }

        copyBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            this.copyToClipboard(copyText, copyBtn);
        });
        container.appendChild(copyBtn);
    },

    copyToClipboard(text, btn) {
        navigator.clipboard.writeText(text).then(() => {
            btn.classList.add('copied');
            btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none"><path d="M3 8.5L6.5 12L13 4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
            setTimeout(() => {
                btn.classList.remove('copied');
                btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none"><rect x="5" y="2" width="8" height="10" rx="1" stroke="currentColor" stroke-width="1.3"/><path d="M3 5v8a1 1 0 001 1h6" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/></svg>';
            }, 1500);
        });
    },

    renderFields(logData, container, filterTerm = '') {
        container.innerHTML = '';

        let flattenedData = {};

        if (logData.timestamp) {
            flattenedData.timestamp = logData.timestamp;
        }

        if (logData.fields && typeof logData.fields === 'object') {
            Object.assign(flattenedData, logData.fields);
        }

        if (logData._all_fields && typeof logData._all_fields === 'object') {
            Object.assign(flattenedData, logData._all_fields);
        }

        for (const key of Object.keys(logData)) {
            if (key !== 'fields' && key !== '_all_fields' && key !== 'timestamp' && key !== 'raw_log') {
                flattenedData[key] = logData[key];
            }
        }

        // raw_log is now in its own tab, skip it here

        const fields = Object.keys(flattenedData);
        const sortedFields = fields.sort((a, b) => {
            if (a === 'timestamp') return -1;
            if (b === 'timestamp') return 1;
            return a.localeCompare(b);
        });

        sortedFields.forEach(key => {
            if (filterTerm && !key.toLowerCase().includes(filterTerm.toLowerCase())) {
                return;
            }

            const value = flattenedData[key];
            const fieldDiv = document.createElement('div');
            fieldDiv.className = 'log-field';
            fieldDiv.dataset.fieldName = key;

            const nameDiv = document.createElement('div');
            nameDiv.className = 'log-field-name';
            nameDiv.textContent = key;

            const valueDiv = document.createElement('div');
            valueDiv.className = 'log-field-value';

            const copyText = typeof value === 'object' && value !== null
                ? JSON.stringify(value, null, 2)
                : String(value || '');

            if (typeof value === 'object' && value !== null) {
                const jsonStr = JSON.stringify(value, null, 2);
                valueDiv.classList.add('json');
                const jsonCopyBtn = document.createElement('button');
                jsonCopyBtn.className = 'log-field-copy-btn json-copy-btn';
                jsonCopyBtn.title = 'Copy value';
                jsonCopyBtn.innerHTML = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none"><rect x="5" y="2" width="8" height="10" rx="1" stroke="currentColor" stroke-width="1.3"/><path d="M3 5v8a1 1 0 001 1h6" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/></svg>';
                jsonCopyBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this.copyToClipboard(copyText, jsonCopyBtn);
                });
                const pre = document.createElement('pre');
                pre.innerHTML = Utils.escapeHtml(jsonStr);
                valueDiv.appendChild(pre);
                valueDiv.appendChild(jsonCopyBtn);
            } else {
                const row = document.createElement('div');
                row.className = 'log-field-value-row';

                const textSpan = document.createElement('span');
                textSpan.className = 'log-field-value-text';
                textSpan.textContent = String(value || '-');

                const actions = document.createElement('span');
                actions.className = 'log-field-actions';

                const filterInBtn = document.createElement('button');
                filterInBtn.className = 'fs-action-btn fs-filter-in';
                filterInBtn.title = 'Filter in';
                filterInBtn.textContent = '+';
                filterInBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    if (window.FieldStats) {
                        FieldStats.addFilter(key, String(value), false);
                    }
                });

                const filterOutBtn = document.createElement('button');
                filterOutBtn.className = 'fs-action-btn fs-filter-out';
                filterOutBtn.title = 'Filter out';
                filterOutBtn.innerHTML = '&minus;';
                filterOutBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    if (window.FieldStats) {
                        FieldStats.addFilter(key, String(value), true);
                    }
                });

                const copyBtn = document.createElement('button');
                copyBtn.className = 'log-field-copy-btn';
                copyBtn.title = 'Copy value';
                copyBtn.innerHTML = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none"><rect x="5" y="2" width="8" height="10" rx="1" stroke="currentColor" stroke-width="1.3"/><path d="M3 5v8a1 1 0 001 1h6" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/></svg>';
                copyBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this.copyToClipboard(copyText, copyBtn);
                });

                actions.appendChild(filterInBtn);
                actions.appendChild(filterOutBtn);
                actions.appendChild(copyBtn);

                row.appendChild(textSpan);
                row.appendChild(actions);
                valueDiv.appendChild(row);
            }

            fieldDiv.appendChild(nameDiv);
            fieldDiv.appendChild(valueDiv);

            // Context link icons
            if (window.ContextLinks && typeof value !== 'object') {
                const strValue = String(value || '');
                const matchingLinks = ContextLinks.getMatchingLinks(key, strValue);
                if (matchingLinks.length > 0) {
                    const linksContainer = document.createElement('div');
                    linksContainer.className = 'context-links-icons';
                    matchingLinks.forEach(link => {
                        const btn = document.createElement('button');
                        btn.className = 'context-link-icon';
                        btn.title = link.short_name;
                        btn.innerHTML = `<svg width="10" height="10" viewBox="0 0 16 16" fill="none"><path d="M7 13L3.5 9.5M3.5 9.5L7 6M3.5 9.5H10.5C11.6 9.5 12.5 8.6 12.5 7.5V3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>${Utils.escapeHtml(link.short_name)}`;
                        btn.addEventListener('click', (e) => {
                            e.stopPropagation();
                            ContextLinks.openLink(link, strValue);
                        });
                        linksContainer.appendChild(btn);
                    });
                    fieldDiv.appendChild(linksContainer);
                }
            }

            container.appendChild(fieldDiv);
        });
    },

    filterFields(filterTerm) {
        if (!this.currentLogData) return;

        const container = document.querySelector('.log-fields-container');
        if (!container) return;

        this.renderFields(this.currentLogData, container, filterTerm);
    },

    close() {
        const panel = document.getElementById('logDetailPanel');
        if (panel) {
            panel.classList.remove('open');
        }

        if (window.Comments) {
            window.Comments.closePanel();
        }
    }
};

// Make globally available
window.LogDetail = LogDetail;
