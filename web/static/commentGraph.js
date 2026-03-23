const CommentGraph = {
    network: null,
    logFieldData: {},      // { log_id: { fields: {...} } }
    commentsByLogId: {},   // { log_id: [comment, ...] }
    availableFields: [],
    selectedFields: [],
    dataLoaded: false,
    activeTab: 'list',

    // ============================
    // Sub-tab management
    // ============================

    switchTab(tab) {
        this.activeTab = tab;
        const listView = document.getElementById('commentsListView');
        const graphView = document.getElementById('commentsGraphView');
        const tabs = document.querySelectorAll('#commentsSubTabs .comments-sub-tab');
        if (!listView || !graphView) return;

        tabs.forEach(t => t.classList.toggle('active', t.dataset.subtab === tab));

        if (tab === 'list') {
            listView.style.display = '';
            graphView.style.display = 'none';
        } else {
            listView.style.display = 'none';
            graphView.style.display = '';
            if (!this.dataLoaded) {
                this.loadGraphData();
            }
        }
    },

    showListDefault() {
        this.activeTab = 'list';
        const listView = document.getElementById('commentsListView');
        const graphView = document.getElementById('commentsGraphView');
        const tabs = document.querySelectorAll('#commentsSubTabs .comments-sub-tab');
        if (listView) listView.style.display = '';
        if (graphView) graphView.style.display = 'none';
        tabs.forEach(t => t.classList.toggle('active', t.dataset.subtab === 'list'));
    },

    // ============================
    // Data loading
    // ============================

    async loadGraphData() {
        const container = document.getElementById('commentGraphNetwork');
        if (!container) return;

        container.innerHTML = '<div class="comment-graph-empty">Loading comment data...</div>';
        this.updateStats('', '');

        try {
            // Fetch flat comments
            const resp = await fetch('/api/v1/comments/flat?limit=5000', {
                credentials: 'include'
            });
            const data = await resp.json();
            if (!data.success || !data.data) {
                container.innerHTML = '<div class="comment-graph-empty">Failed to load comments</div>';
                return;
            }

            const comments = data.data;
            if (comments.length === 0) {
                container.innerHTML = '<div class="comment-graph-empty">No comments found</div>';
                return;
            }

            // Group by log_id
            this.commentsByLogId = {};
            for (const c of comments) {
                if (!this.commentsByLogId[c.log_id]) {
                    this.commentsByLogId[c.log_id] = [];
                }
                this.commentsByLogId[c.log_id].push(c);
            }

            const logIds = Object.keys(this.commentsByLogId);
            if (logIds.length > 500) {
                container.innerHTML = '<div class="comment-graph-empty">Too many commented logs (' + logIds.length + '). Showing first 500.</div>';
                logIds.length = 500;
            }

            // Batch fetch log fields
            container.innerHTML = '<div class="comment-graph-empty">Fetching log fields for ' + logIds.length + ' logs...</div>';
            const fieldsResp = await fetch('/api/v1/comments/graph/log-fields', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ log_ids: logIds })
            });
            const fieldsData = await fieldsResp.json();

            this.logFieldData = {};
            if (fieldsData.success && fieldsData.data) {
                for (const entry of fieldsData.data) {
                    this.logFieldData[entry.log_id] = entry.fields || {};
                }
            }

            // Extract available fields
            const fieldSet = new Set();
            for (const fields of Object.values(this.logFieldData)) {
                this.collectFieldKeys(fields, '', fieldSet);
            }
            this.availableFields = [...fieldSet].sort();

            this.dataLoaded = true;
            this.populateFieldSelector();

            if (this.selectedFields.length > 0) {
                this.rebuildGraph();
            } else {
                container.innerHTML = '<div class="comment-graph-empty">Select fields above to build the relationship graph</div>';
            }
        } catch (err) {
            console.error('[CommentGraph] Error loading data:', err);
            container.innerHTML = '<div class="comment-graph-empty">Error loading data</div>';
        }
    },

    collectFieldKeys(obj, prefix, fieldSet) {
        for (const [k, v] of Object.entries(obj)) {
            const key = prefix ? prefix + '.' + k : k;
            if (v && typeof v === 'object' && !Array.isArray(v)) {
                this.collectFieldKeys(v, key, fieldSet);
            } else {
                fieldSet.add(key);
            }
        }
    },

    getNestedValue(obj, path) {
        const parts = path.split('.');
        let cur = obj;
        for (const p of parts) {
            if (cur == null || typeof cur !== 'object') return undefined;
            cur = cur[p];
        }
        return cur;
    },

    // ============================
    // Field selector
    // ============================

    populateFieldSelector() {
        const menu = document.getElementById('commentGraphFieldMenu');
        const btn = document.getElementById('commentGraphFieldBtn');
        if (!menu || !btn) return;

        menu.innerHTML = '';
        for (const field of this.availableFields) {
            const lbl = document.createElement('label');
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.value = field;
            cb.checked = this.selectedFields.includes(field);
            cb.addEventListener('change', () => this.onFieldToggle(field, cb.checked));
            lbl.appendChild(cb);
            lbl.appendChild(document.createTextNode(' ' + field));
            menu.appendChild(lbl);
        }

        this.updateFieldBtnText();

        // Toggle dropdown
        if (!btn._bound) {
            btn._bound = true;
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                menu.style.display = menu.style.display === 'none' ? '' : 'none';
            });
            document.addEventListener('click', () => {
                menu.style.display = 'none';
            });
            menu.addEventListener('click', (e) => e.stopPropagation());
        }
    },

    updateFieldBtnText() {
        const btn = document.getElementById('commentGraphFieldBtn');
        if (!btn) return;
        if (this.selectedFields.length === 0) {
            btn.textContent = 'Select fields...';
        } else if (this.selectedFields.length <= 2) {
            btn.textContent = this.selectedFields.join(', ');
        } else {
            btn.textContent = this.selectedFields.length + ' fields selected';
        }
    },

    onFieldToggle(field, checked) {
        if (checked) {
            if (!this.selectedFields.includes(field)) this.selectedFields.push(field);
        } else {
            this.selectedFields = this.selectedFields.filter(f => f !== field);
        }
        this.updateFieldBtnText();
        this.rebuildGraph();
    },

    // ============================
    // Edge computation
    // ============================

    computeEdges() {
        const minConns = parseInt(document.getElementById('commentGraphMinConns')?.value || '1', 10);
        const logIds = Object.keys(this.commentsByLogId).filter(id => this.logFieldData[id]);

        // Build inverted index: "field\x00value" -> Set<logId>
        const invertedIndex = new Map();
        for (const logId of logIds) {
            const fields = this.logFieldData[logId] || {};
            for (const fieldName of this.selectedFields) {
                const value = this.getNestedValue(fields, fieldName);
                if (value === undefined || value === null || value === '') continue;
                const strValue = String(value);
                const key = fieldName + '\x00' + strValue;
                if (!invertedIndex.has(key)) invertedIndex.set(key, new Set());
                invertedIndex.get(key).add(logId);
            }
        }

        // Build edge map: "idA|idB" -> { from, to, sharedFields }
        const edgeMap = new Map();
        for (const [key, logIdSet] of invertedIndex) {
            if (logIdSet.size < 2) continue;
            const [fieldName, fieldValue] = key.split('\x00');
            const ids = [...logIdSet].sort();
            for (let i = 0; i < ids.length; i++) {
                for (let j = i + 1; j < ids.length; j++) {
                    const edgeKey = ids[i] + '|' + ids[j];
                    if (!edgeMap.has(edgeKey)) {
                        edgeMap.set(edgeKey, { from: ids[i], to: ids[j], sharedFields: [] });
                    }
                    edgeMap.get(edgeKey).sharedFields.push({ field: fieldName, value: fieldValue });
                }
            }
        }

        const edges = [...edgeMap.values()];
        if (minConns > 1) {
            return edges.filter(e => e.sharedFields.length >= minConns);
        }
        return edges;
    },

    // ============================
    // Graph rendering
    // ============================

    rebuildGraph() {
        if (this.selectedFields.length === 0) {
            const container = document.getElementById('commentGraphNetwork');
            if (container) container.innerHTML = '<div class="comment-graph-empty">Select fields above to build the relationship graph</div>';
            this.updateStats('', '');
            return;
        }

        const edges = this.computeEdges();

        // Determine which log_ids are connected
        const connectedIds = new Set();
        for (const e of edges) {
            connectedIds.add(e.from);
            connectedIds.add(e.to);
        }

        if (connectedIds.size === 0) {
            const container = document.getElementById('commentGraphNetwork');
            if (container) container.innerHTML = '<div class="comment-graph-empty">No relationships found for selected fields</div>';
            this.updateStats(0, 0);
            return;
        }

        const cv = ThemeManager.getCSSVar;
        const edgeColors = [
            cv('--graph-edge-1'), cv('--graph-edge-2'), cv('--graph-edge-3'),
            cv('--graph-edge-4'), cv('--graph-edge-5'), cv('--graph-edge-6')
        ];

        // Assign a color index per unique field name
        const fieldColorMap = {};
        const usedFields = new Set();
        for (const e of edges) {
            for (const sf of e.sharedFields) usedFields.add(sf.field);
        }
        let ci = 0;
        for (const f of [...usedFields].sort()) {
            fieldColorMap[f] = edgeColors[ci % edgeColors.length];
            ci++;
        }

        // Build vis nodes
        const nodes = new vis.DataSet();
        for (const logId of connectedIds) {
            const comments = this.commentsByLogId[logId] || [];
            const commentCount = comments.length;
            const fields = this.logFieldData[logId] || {};
            const size = Math.min(12 + commentCount * 3, 30);

            // Pick a label from the first meaningful field value
            let label = logId.substring(0, 10);
            const firstComment = comments[0];
            if (firstComment && firstComment.text) {
                const preview = firstComment.text.substring(0, 20);
                label = preview + (firstComment.text.length > 20 ? '...' : '');
            }

            // Tooltip
            const titleEl = document.createElement('div');
            const fieldLines = Object.entries(fields).slice(0, 8)
                .map(([k, v]) => {
                    const val = typeof v === 'object' ? JSON.stringify(v) : String(v);
                    return '<div style="font-size:11px;"><b>' + Utils.escapeHtml(k) + ':</b> ' + Utils.escapeHtml(val.substring(0, 60)) + '</div>';
                }).join('');
            titleEl.innerHTML = '<div style="max-width:300px;"><div style="font-weight:600;margin-bottom:4px;">' +
                commentCount + ' comment' + (commentCount !== 1 ? 's' : '') +
                '</div>' + fieldLines + '</div>';

            nodes.add({
                id: logId,
                label: label,
                title: titleEl,
                size: size,
                mass: 1 + commentCount * 0.3,
                color: {
                    background: cv('--graph-node-child'),
                    border: cv('--graph-node-child'),
                    highlight: { background: cv('--graph-node-child-hover'), border: cv('--accent-primary') },
                    hover: { background: cv('--graph-node-child-hover'), border: cv('--accent-primary') }
                }
            });
        }

        // Build vis edges
        const visEdges = new vis.DataSet();
        let edgeId = 0;
        for (const e of edges) {
            // Combine shared fields into label
            const labels = e.sharedFields.slice(0, 3).map(sf => sf.field + ': ' + sf.value.substring(0, 20));
            const label = labels.join('\n') + (e.sharedFields.length > 3 ? '\n+' + (e.sharedFields.length - 3) + ' more' : '');

            // Use color of first field
            const color = fieldColorMap[e.sharedFields[0].field] || cv('--graph-edge');

            visEdges.add({
                id: edgeId++,
                from: e.from,
                to: e.to,
                label: label,
                width: Math.min(1 + e.sharedFields.length * 0.5, 4),
                color: { color: color, opacity: 0.6, highlight: cv('--accent-primary'), hover: color },
                font: { size: 9, color: cv('--graph-label'), strokeWidth: 2, strokeColor: cv('--graph-label-stroke'), align: 'middle' },
                smooth: { enabled: true, type: 'dynamic' }
            });
        }

        this.updateStats(nodes.length, visEdges.length);

        // Render
        const container = document.getElementById('commentGraphNetwork');
        if (!container) return;
        container.innerHTML = '';

        if (this.network) {
            this.network.destroy();
            this.network = null;
        }

        const options = {
            layout: { hierarchical: { enabled: false } },
            physics: {
                enabled: true,
                solver: 'forceAtlas2Based',
                forceAtlas2Based: {
                    gravitationalConstant: -40,
                    centralGravity: 0.005,
                    springLength: 160,
                    springConstant: 0.02,
                    damping: 0.4
                },
                stabilization: { enabled: true, iterations: 200, fit: true }
            },
            interaction: {
                dragNodes: true,
                dragView: true,
                zoomView: true,
                hover: true,
                tooltipDelay: 200,
                multiselect: false
            },
            nodes: {
                shape: 'dot',
                borderWidth: 2,
                chosen: true,
                font: {
                    size: 10,
                    color: cv('--graph-label'),
                    face: 'Inter',
                    vadjust: -4,
                    strokeWidth: 3,
                    strokeColor: cv('--graph-label-stroke')
                }
            },
            edges: {
                arrows: { to: false },
                chosen: true
            },
            configure: { enabled: false }
        };

        this.network = new vis.Network(container, { nodes, edges: visEdges }, options);

        this.network.on('click', (params) => {
            if (params.nodes.length > 0) {
                this.showNodeDetail(params.nodes[0]);
            } else {
                this.closeDetail();
            }
        });

        // Disable physics after stabilization for smooth dragging
        this.network.on('stabilizationIterationsDone', () => {
            this.network.setOptions({ physics: { enabled: false } });
        });

        this.bindToolbar();
    },

    updateStats(nodeCount, edgeCount) {
        const el = document.getElementById('commentGraphStats');
        if (!el) return;
        if (nodeCount === '' && edgeCount === '') {
            el.textContent = '';
            return;
        }
        el.textContent = nodeCount + ' nodes, ' + edgeCount + ' edges';
    },

    // ============================
    // Detail panel
    // ============================

    showNodeDetail(logId) {
        const panel = document.getElementById('commentGraphDetailPanel');
        const body = document.getElementById('commentGraphDetailBody');
        if (!panel || !body) return;

        const comments = this.commentsByLogId[logId] || [];
        const fields = this.logFieldData[logId] || {};
        const firstComment = comments[0];
        const logTimestamp = firstComment ? firstComment.log_timestamp : '';

        let html = '<div class="detail-field"><div class="detail-field-label">Log ID</div><div class="detail-field-value">' + Utils.escapeHtml(logId) + '</div></div>';

        // Key fields
        const fieldEntries = Object.entries(fields).slice(0, 10);
        if (fieldEntries.length > 0) {
            html += '<div class="detail-field"><div class="detail-field-label">Fields</div>';
            for (const [k, v] of fieldEntries) {
                const val = typeof v === 'object' ? JSON.stringify(v) : String(v);
                html += '<div class="detail-field-value"><b>' + Utils.escapeHtml(k) + ':</b> ' + Utils.escapeHtml(val.substring(0, 80)) + '</div>';
            }
            html += '</div>';
        }

        // Comments
        html += '<div class="detail-field"><div class="detail-field-label">' + comments.length + ' comment' + (comments.length !== 1 ? 's' : '') + '</div></div>';
        for (const c of comments) {
            html += '<div class="detail-comment">';
            html += '<div class="detail-comment-author">' + Utils.escapeHtml(c.author_display_name || c.author) + '</div>';
            html += '<div class="detail-comment-text comment-markdown">' + Utils.renderCommentMarkdown(c.text) + '</div>';
            if (c.tags && c.tags.length > 0) {
                html += '<div class="detail-comment-tags">';
                for (const t of c.tags) {
                    html += '<span class="detail-comment-tag">' + Utils.escapeHtml(t) + '</span>';
                }
                html += '</div>';
            }
            html += '</div>';
        }

        // View full log button
        html += '<button class="detail-view-log-btn" onclick="CommentGraph.viewFullLog(\'' +
            Utils.escapeHtml(logId) + '\', \'' + Utils.escapeHtml(logTimestamp) + '\')">View Full Log</button>';

        body.innerHTML = html;
        panel.style.display = '';
    },

    closeDetail() {
        const panel = document.getElementById('commentGraphDetailPanel');
        if (panel) panel.style.display = 'none';
    },

    async viewFullLog(logId, logTimestamp) {
        if (window.CommentedLogs && CommentedLogs.showLogDetail) {
            await CommentedLogs.showLogDetail(logId, logTimestamp);
        }
    },

    // ============================
    // Toolbar
    // ============================

    bindToolbar() {
        const fitBtn = document.getElementById('commentGraphFitBtn');
        const zoomInBtn = document.getElementById('commentGraphZoomInBtn');
        const zoomOutBtn = document.getElementById('commentGraphZoomOutBtn');
        const exportBtn = document.getElementById('commentGraphExportBtn');
        const minConns = document.getElementById('commentGraphMinConns');

        if (fitBtn && !fitBtn._bound) {
            fitBtn._bound = true;
            fitBtn.addEventListener('click', () => {
                if (this.network) this.network.fit({ animation: { duration: 300 } });
            });
        }
        if (zoomInBtn && !zoomInBtn._bound) {
            zoomInBtn._bound = true;
            zoomInBtn.addEventListener('click', () => {
                if (this.network) {
                    const scale = this.network.getScale();
                    this.network.moveTo({ scale: scale * 1.3, animation: { duration: 200 } });
                }
            });
        }
        if (zoomOutBtn && !zoomOutBtn._bound) {
            zoomOutBtn._bound = true;
            zoomOutBtn.addEventListener('click', () => {
                if (this.network) {
                    const scale = this.network.getScale();
                    this.network.moveTo({ scale: scale / 1.3, animation: { duration: 200 } });
                }
            });
        }
        if (exportBtn && !exportBtn._bound) {
            exportBtn._bound = true;
            exportBtn.addEventListener('click', () => this.exportPNG());
        }
        if (minConns && !minConns._bound) {
            minConns._bound = true;
            minConns.addEventListener('change', () => this.rebuildGraph());
        }
    },

    exportPNG() {
        if (!this.network) return;
        const canvas = document.getElementById('commentGraphNetwork')?.querySelector('canvas');
        if (!canvas) return;
        const link = document.createElement('a');
        link.download = 'comment-graph.png';
        link.href = canvas.toDataURL('image/png');
        link.click();
    },

    // ============================
    // Lifecycle
    // ============================

    onFractalChange() {
        this.dataLoaded = false;
        this.logFieldData = {};
        this.commentsByLogId = {};
        this.availableFields = [];
        this.selectedFields = [];
        if (this.network) {
            this.network.destroy();
            this.network = null;
        }
        if (this.activeTab === 'graph') {
            this.loadGraphData();
        }
    },

    destroy() {
        if (this.network) {
            this.network.destroy();
            this.network = null;
        }
    }
};

window.CommentGraph = CommentGraph;
