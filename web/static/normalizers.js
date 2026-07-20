const Normalizers = {
    normalizers: [],
    editingId: null,
    currentTransforms: [],
    currentMappings: [],
    currentSamples: [],
    _draggedTsRow: null,
    _draggedTransformIndex: null,
    _activeSampleIndex: -1,
    _dirty: false,
    _liveWired: false,
    _previewTimer: null,
    _previewAbort: null,
    _previewHits: {},
    _previewCollisions: {},

    TRANSFORMS: {
        flatten_leaf: { label: 'flatten_leaf', desc: 'Flatten nested keys to leaf name only (user.profile.name -> name)', conflicts: ['flatten_full', 'dedot'] },
        flatten_full: { label: 'flatten_full', desc: 'Flatten nested keys with full path (user.profile.name -> user_profile_name)', conflicts: ['flatten_leaf', 'dedot'] },
        snake_case:   { label: 'snake_case',   desc: 'Convert field names to snake_case',              conflicts: ['camelCase', 'PascalCase'] },
        camelCase:    { label: 'camelCase',     desc: 'Convert field names to camelCase',               conflicts: ['snake_case', 'PascalCase'] },
        PascalCase:   { label: 'PascalCase',    desc: 'Convert field names to PascalCase',              conflicts: ['snake_case', 'camelCase'] },
        dedot:        { label: 'dedot',         desc: 'Replace dots with underscores (a.b.c -> a_b_c)', conflicts: ['flatten_leaf', 'flatten_full'] },
        lowercase:  { label: 'lowercase',  desc: 'Lowercase all field names',                      conflicts: ['uppercase'] },
        uppercase:  { label: 'uppercase',  desc: 'Uppercase all field names',                      conflicts: ['lowercase'] }
    },

    TIMESTAMP_PRESETS: [
        { label: 'ISO 8601 / RFC 3339',         value: '2006-01-02T15:04:05Z07:00' },
        { label: 'ISO 8601 (nanoseconds)',       value: '2006-01-02T15:04:05.999999999Z07:00' },
        { label: 'ISO 8601 (milliseconds)',      value: '2006-01-02T15:04:05.000Z07:00' },
        { label: 'Date + Time (space)',          value: '2006-01-02 15:04:05' },
        { label: 'Date + Time (millis) / utc_time', value: '2006-01-02 15:04:05.000' },
        { label: 'Unix (seconds)',               value: 'unix' },
        { label: 'Unix (milliseconds)',          value: 'unixmilli' },
        { label: 'Unix (microseconds)',          value: 'unixmicro' },
        { label: 'Unix (nanoseconds)',           value: 'unixnano' },
        { label: 'RFC 822',                      value: '02 Jan 06 15:04 MST' },
        { label: 'RFC 850',                      value: 'Monday, 02-Jan-06 15:04:05 MST' },
        { label: 'ANSIC',                        value: 'Mon Jan _2 15:04:05 2006' },
        { label: 'Syslog (BSD)',                 value: 'Jan _2 15:04:05' },
        { label: 'Syslog (ISO)',                 value: '2006-01-02T15:04:05.000000+00:00' },
        { label: 'US Date + Time (12h)',           value: '1/2/2006 3:04:05 PM' },
        { label: 'US Date + Time (24h)',           value: '1/2/2006 15:04:05' },
        { label: 'Apache Common Log',            value: '02/Jan/2006:15:04:05 -0700' },
        { label: 'Windows FileTime (ticks)',     value: 'unixnano' },
    ],

    init() {
        const createBtn = document.getElementById('normalizerCreateBtn');
        if (createBtn) {
            createBtn.addEventListener('click', () => this.openCreateForm());
        }
    },

    show(subPath = '') {
        if (subPath === 'new') {
            this.openCreateForm();
            return;
        }
        if (subPath) {
            this.openEditForm(subPath);
            return;
        }
        const editor = document.getElementById('normalizerEditorView');
        if (editor) editor.style.display = 'none';
        this.loadNormalizers();
    },

    hide() {
        const editor = document.getElementById('normalizerEditorView');
        if (editor) editor.style.display = 'none';
    },

    async loadNormalizers() {
        try {
            const data = await HttpUtils.safeFetch('/api/v1/normalizers');
            this.normalizers = data.data.normalizers || [];
            this.renderNormalizers();
        } catch (err) {
            console.error('[Normalizers] Failed to load:', err);
            Utils.showNotification('Failed to load normalizers', 'error');
        }
    },

    renderNormalizers() {
        const container = document.getElementById('normalizersListContainer');
        if (!container) return;

        if (this.normalizers.length === 0) {
            container.innerHTML = '<p class="empty-state">No normalizers configured.</p>';
            return;
        }

        let html = `<table class="context-links-table">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Transforms</th>
                    <th>Field Mappings</th>
                    <th>Used By</th>
                    <th></th>
                </tr>
            </thead>
            <tbody>`;

        this.normalizers.forEach(n => {
            const transforms = (n.transforms || []).join(', ') || 'None';
            const mappingCount = (n.field_mappings || []).length;
            const defaultBadge = n.is_default ? ' <span class="context-link-badge">default</span>' : '';
            html += `<tr>
                <td><button class="table-name-link" onclick="Normalizers.openEditForm('${Utils.escapeJs(n.id)}')" title="Edit normalizer">${Utils.escapeHtml(n.name)}</button>${defaultBadge}</td>
                <td class="context-link-fields">${Utils.escapeHtml(transforms)}</td>
                <td>${mappingCount} mapping${mappingCount !== 1 ? 's' : ''}</td>
                <td><span class="normalizer-usage-cell" id="normalizer-usage-${n.id}">--</span></td>
                <td class="kebab-cell">
                    <div class="kebab-wrapper">
                        <button class="kebab-btn" onclick="KebabMenu.toggle(event,this)">⋮</button>
                        <div class="kebab-menu">
                            <button class="kebab-item" onclick="Normalizers.openEditForm('${n.id}')">Edit</button>
                            <button class="kebab-item" onclick="Normalizers.duplicateNormalizer('${n.id}')">Duplicate</button>
                            <button class="kebab-item" onclick="Normalizers.exportNormalizer('${n.id}', '${Utils.escapeHtml(n.name)}')">Export</button>
                            ${!n.is_default ? `<button class="kebab-item" onclick="Normalizers.setDefault('${n.id}')">Set Default</button>` : ''}
                            ${!n.is_default ? `<button class="kebab-item danger" onclick="Normalizers.deleteNormalizer('${n.id}')">Delete</button>` : ''}
                        </div>
                    </div>
                </td>
            </tr>`;
        });

        html += '</tbody></table>';
        container.innerHTML = html;

        // Load token usage for each normalizer
        this.normalizers.forEach(n => this._loadTokenUsage(n.id));
    },

    async _loadTokenUsage(normalizerId) {
        const cell = document.getElementById(`normalizer-usage-${normalizerId}`);
        if (!cell) return;
        try {
            const data = await HttpUtils.safeFetch(`/api/v1/normalizers/${normalizerId}/tokens`);
            const tokens = data.data.tokens || [];
            if (tokens.length === 0) {
                cell.textContent = 'No tokens';
                cell.className = 'normalizer-usage-cell usage-none';
                return;
            }
            // Group by fractal
            const byFractal = {};
            tokens.forEach(t => {
                if (!byFractal[t.fractal_name]) byFractal[t.fractal_name] = [];
                byFractal[t.fractal_name].push(t.token_name);
            });
            const parts = Object.entries(byFractal).map(([fractal, tnames]) => {
                const tokenList = tnames.map(n => Utils.escapeHtml(n)).join(', ');
                return `<span class="usage-fractal" title="${tokenList}">${Utils.escapeHtml(fractal)} (${tnames.length})</span>`;
            });
            cell.innerHTML = parts.join(', ');
            cell.className = 'normalizer-usage-cell';
        } catch {
            cell.textContent = '--';
        }
    },

    showEditor(title) {
        const listView = document.getElementById('normalizersView');
        const editorView = document.getElementById('normalizerEditorView');
        if (listView) listView.style.display = 'none';
        if (editorView) editorView.style.display = 'block';

        const titleEl = document.getElementById('normalizerEditorTitle');
        if (titleEl) titleEl.textContent = title;
    },

    backToList() {
        if (this._dirty && !confirm('You have unsaved changes. Leave without saving?')) return;

        clearTimeout(this._previewTimer);
        this._previewAbort?.abort();
        this._previewAbort = null;

        window.App?.pushSubPath('');
        const listView = document.getElementById('normalizersView');
        const editorView = document.getElementById('normalizerEditorView');
        if (editorView) editorView.style.display = 'none';
        if (listView) listView.style.display = 'block';
        this.editingId = null;
        this._dirty = false;
        this._syncDirtyUI();
        this.loadNormalizers();
    },

    // --- Transform list management ---

    _renderTransformsList() {
        const container = document.getElementById('normalizerTransformsList');
        if (!container) return;

        container.innerHTML = '';
        this.currentTransforms.forEach((key, i) => {
            const t = this.TRANSFORMS[key];
            if (!t) return;
            const row = document.createElement('div');
            row.className = 'transform-item';
            row.draggable = true;
            row.dataset.index = String(i);
            row.innerHTML = `
                <span class="nz-grip" title="Drag to reorder">
                    <svg width="10" height="14" viewBox="0 0 10 14" fill="currentColor" aria-hidden="true"><circle cx="2" cy="2" r="1.2"/><circle cx="8" cy="2" r="1.2"/><circle cx="2" cy="7" r="1.2"/><circle cx="8" cy="7" r="1.2"/><circle cx="2" cy="12" r="1.2"/><circle cx="8" cy="12" r="1.2"/></svg>
                </span>
                <code class="transform-item-label">${Utils.escapeHtml(t.label)}</code>
                <span class="transform-item-desc">${Utils.escapeHtml(t.desc)}</span>
                <span class="transform-item-step">step ${i + 1}</span>
                <button class="nz-x" onclick="Normalizers.removeTransform(${i})" title="Remove transform">&times;</button>
            `;
            this._wireTransformDrag(row, container, i);
            container.appendChild(row);
        });

        const count = document.getElementById('normalizerTransformCount');
        if (count) count.textContent = `${this.currentTransforms.length} active`;

        this._updateTransformSelect();
    },

    // Drag-to-reorder. Reordering changes output (flatten before snake_case is not
    // the same as after), so the preview re-runs on drop.
    _wireTransformDrag(row, container, index) {
        row.addEventListener('dragstart', (e) => {
            this._draggedTransformIndex = index;
            row.classList.add('transform-item-dragging');
            e.dataTransfer.effectAllowed = 'move';
            try { e.dataTransfer.setData('text/plain', String(index)); } catch { /* Safari */ }
        });
        row.addEventListener('dragend', () => {
            this._draggedTransformIndex = null;
            row.classList.remove('transform-item-dragging');
            container.querySelectorAll('.transform-item').forEach(r => r.classList.remove('transform-item-drop'));
        });
        row.addEventListener('dragover', (e) => {
            e.preventDefault();
            if (this._draggedTransformIndex === null || this._draggedTransformIndex === index) return;
            container.querySelectorAll('.transform-item').forEach(r => r.classList.remove('transform-item-drop'));
            row.classList.add('transform-item-drop');
        });
        row.addEventListener('dragleave', () => row.classList.remove('transform-item-drop'));
        row.addEventListener('drop', (e) => {
            e.preventDefault();
            const from = this._draggedTransformIndex;
            this._draggedTransformIndex = null;
            if (from === null || from === index) return;
            const [moved] = this.currentTransforms.splice(from, 1);
            this.currentTransforms.splice(index, 0, moved);
            this._markDirty();
            this._renderTransformsList();
            this._schedulePreview();
        });
    },

    _updateTransformSelect() {
        const select = document.getElementById('normalizerTransformSelect');
        if (!select) return;

        const available = this._getAvailableTransforms();
        select.innerHTML = '<option value="">Add a transform...</option>';
        available.forEach(key => {
            const t = this.TRANSFORMS[key];
            const opt = document.createElement('option');
            opt.value = key;
            opt.textContent = `${t.label} - ${t.desc}`;
            select.appendChild(opt);
        });
    },

    _getAvailableTransforms() {
        const blocked = new Set(this.currentTransforms);
        this.currentTransforms.forEach(key => {
            const t = this.TRANSFORMS[key];
            if (t) t.conflicts.forEach(c => blocked.add(c));
        });
        return Object.keys(this.TRANSFORMS).filter(k => !blocked.has(k));
    },

    addTransformFromSelect() {
        const select = document.getElementById('normalizerTransformSelect');
        if (!select || !select.value) return;
        this.currentTransforms.push(select.value);
        this._markDirty();
        this._renderTransformsList();
        this._schedulePreview();
    },

    removeTransform(index) {
        this.currentTransforms.splice(index, 1);
        this._markDirty();
        this._renderTransformsList();
        this._schedulePreview();
    },

    moveTransform(index, direction) {
        const newIndex = index + direction;
        if (newIndex < 0 || newIndex >= this.currentTransforms.length) return;
        const item = this.currentTransforms.splice(index, 1)[0];
        this.currentTransforms.splice(newIndex, 0, item);
        this._markDirty();
        this._renderTransformsList();
        this._schedulePreview();
    },

    // --- Form open/close ---

    openCreateForm() {
        window.App?.pushSubPath('new');
        this.editingId = null;

        document.getElementById('normalizerName').value = '';
        document.getElementById('normalizerDescription').value = '';

        this.currentTransforms = [];
        this.currentMappings = [{ sources: [], target: '' }];
        this._renderTransformsList();
        this._renderMappings();

        document.getElementById('normalizerDerivedFields').innerHTML = '';
        document.getElementById('normalizerTimestampFields').innerHTML = '';

        this._setUsageChip(null);
        this._initEditorSession('Create Normalizer');
        document.getElementById('normalizerName')?.focus();
    },

    async openEditForm(id) {
        try {
            window.App?.pushSubPath(id);
            const data = await HttpUtils.safeFetch(`/api/v1/normalizers/${id}`);
            const n = data.data;
            this.editingId = id;

            document.getElementById('normalizerName').value = n.name;
            document.getElementById('normalizerDescription').value = n.description || '';

            this.currentTransforms = (n.transforms || []).filter(t => t in this.TRANSFORMS);
            this.currentMappings = (n.field_mappings || []).map(m => ({
                sources: (m.sources || []).slice(),
                target: m.target || ''
            }));
            if (this.currentMappings.length === 0) {
                this.currentMappings.push({ sources: [], target: '' });
            }
            this._renderTransformsList();
            this._renderMappings();

            const derivedContainer = document.getElementById('normalizerDerivedFields');
            derivedContainer.innerHTML = '';
            (n.value_mappings || []).forEach(vm => {
                this.addDerivedFieldRow(vm.from_field, vm.to_field, vm.map, vm.default);
            });

            const tsContainer = document.getElementById('normalizerTimestampFields');
            tsContainer.innerHTML = '';
            (n.timestamp_fields || []).forEach(tf => {
                this.addTimestampFieldRow(tf.field, tf.format);
            });

            this._initEditorSession('Edit Normalizer');
            this._loadEditorUsage(id);
        } catch (err) {
            Utils.showNotification('Failed to load normalizer', 'error');
        }
    },

    // Shared setup for both entry points: clean dirty state, restore the rail,
    // wire live-preview inputs, and get a sample in front of the user.
    _initEditorSession(title) {
        this._dirty = false;
        this._syncDirtyUI();
        this._previewHits = {};
        this._previewCollisions = {};
        this._wireLiveInputs();
        this._restoreRailState();
        this.showEditor(title);
        this._loadCaptureFractals();
        this._schedulePreview(0);
    },

    _wireLiveInputs() {
        if (this._liveWired) return;
        this._liveWired = true;

        const dirtyOnly = ['normalizerName', 'normalizerDescription'];
        dirtyOnly.forEach(id => {
            document.getElementById(id)?.addEventListener('input', () => this._markDirty());
        });

        document.getElementById('normalizerMappingFilter')?.addEventListener('input', () => this._renderMappings());

        const input = document.getElementById('normalizerPreviewInput');
        input?.addEventListener('input', () => {
            this._activeSampleIndex = -1; // hand-edited, no longer a captured sample
            this._renderSampleBar();
            this._schedulePreview();
        });

        // Derived and timestamp rows are plain DOM; a delegated listener keeps the
        // preview live without rewriting those editors.
        document.getElementById('normalizerDerivedFields')?.addEventListener('input', () => {
            this._markDirty();
            this._schedulePreview();
        });
        document.getElementById('normalizerTimestampFields')?.addEventListener('input', () => this._markDirty());

        document.addEventListener('keydown', (e) => {
            if (!this._editorVisible()) return;
            if ((e.ctrlKey || e.metaKey) && e.key === '\\') {
                e.preventDefault();
                this.toggleRail();
            }
        });
    },

    _editorVisible() {
        const el = document.getElementById('normalizerEditorView');
        return !!el && el.style.display !== 'none';
    },

    async _loadEditorUsage(id) {
        try {
            const data = await HttpUtils.safeFetch(`/api/v1/normalizers/${id}/tokens`);
            this._setUsageChip(data.data.tokens || []);
        } catch {
            this._setUsageChip(null);
        }
    },

    // Editing a normalizer that live tokens depend on is worth knowing before you
    // hit Save, not only from the list view.
    _setUsageChip(tokens) {
        const chip = document.getElementById('normalizerUsageChip');
        const text = document.getElementById('normalizerUsageText');
        if (!chip || !text) return;
        if (!tokens || tokens.length === 0) {
            chip.style.display = 'none';
            return;
        }
        const fractals = new Set(tokens.map(t => t.fractal_name));
        text.textContent = `Live on ${tokens.length} token${tokens.length !== 1 ? 's' : ''} / ${fractals.size} fractal${fractals.size !== 1 ? 's' : ''}`;
        chip.title = tokens.map(t => `${t.token_name} (${t.fractal_name})`).join(', ');
        chip.style.display = 'inline-flex';
    },

    _markDirty() {
        if (this._dirty) return;
        this._dirty = true;
        this._syncDirtyUI();
    },

    _syncDirtyUI() {
        document.getElementById('normalizerTopbar')?.classList.toggle('nz-is-dirty', !!this._dirty);
        const save = document.getElementById('normalizerSaveBtn');
        if (save) save.disabled = !this._dirty;
    },

    discardChanges() {
        if (this._dirty && !confirm('Discard unsaved changes?')) return;
        if (this.editingId) {
            this.openEditForm(this.editingId);
        } else {
            this.openCreateForm();
        }
    },

    _editingMappingIndex: null,

    addMappingRow(sources, target) {
        const list = Array.isArray(sources)
            ? sources.slice()
            : (sources ? String(sources).split(',').map(s => s.trim()).filter(Boolean) : []);
        this.currentMappings.push({ sources: list, target: target || '' });
        this._markDirty();
        this._renderMappings();
        const scroll = document.getElementById('normalizerMappings');
        if (scroll) scroll.scrollTop = scroll.scrollHeight;
    },

    // Renders mappings from state. The preview result, when present, marks which
    // aliases actually matched the current sample and which targets collide, so
    // the row itself carries the diagnosis instead of only the output table.
    _renderMappings() {
        const container = document.getElementById('normalizerMappings');
        if (!container) return;

        const filter = (document.getElementById('normalizerMappingFilter')?.value || '').trim().toLowerCase();
        const hits = this._previewHits || {};
        const collisions = this._previewCollisions || {};

        container.innerHTML = '';
        let shown = 0;

        this.currentMappings.forEach((m, mi) => {
            const target = (m.target || '').trim();
            if (filter && !target.toLowerCase().includes(filter) &&
                !m.sources.some(s => s.toLowerCase().includes(filter))) {
                return;
            }
            shown++;

            const dupTarget = target && this.currentMappings.some((o, oi) => oi !== mi && (o.target || '').trim() === target);
            const collides = target && Object.prototype.hasOwnProperty.call(collisions, target);
            const matchedAliases = hits[mi] || [];

            const row = document.createElement('div');
            row.className = 'normalizer-mapping-row';
            if (dupTarget || collides) row.classList.add('mapping-row-collision');
            else if (matchedAliases.length) row.classList.add('mapping-row-hit');

            // Chips are addressed by index, never by value: alias names can contain
            // quotes or backslashes that would break an inline handler argument.
            const chips = m.sources.map((s, si) => {
                const matched = matchedAliases.includes(s);
                return `<span class="nz-chip${matched ? ' nz-chip-matched' : ''}" title="${matched ? 'Matched in the current sample' : 'No match in the current sample'}">${Utils.escapeHtml(s)}<button class="nz-chip-x" onclick="Normalizers.removeMappingSource(${mi}, ${si})" title="Remove alias">&times;</button></span>`;
            }).join('');

            let alert = '';
            if (collides) {
                const competing = (collisions[target] || []).map(s => Utils.escapeHtml(s)).join(', ');
                alert = `<div class="nz-row-alert">Two fields resolve to <code>${Utils.escapeHtml(target)}</code> in this sample (${competing}). Ingestion keeps only one of them, and which one is not deterministic.</div>`;
            } else if (dupTarget) {
                alert = `<div class="nz-row-alert">Another mapping already targets <code>${Utils.escapeHtml(target)}</code>.</div>`;
            }

            row.innerHTML = `
                <div class="nz-chips">${chips}<button class="nz-chip-add" onclick="Normalizers.openMappingSourcesModal(${mi})" title="Edit aliases">+ alias</button></div>
                <span class="mapping-arrow">-&gt;</span>
                <input type="text" class="mapping-target" placeholder="target_field" value="${Utils.escapeHtml(m.target || '')}" aria-label="Target field name" oninput="Normalizers.updateMappingTarget(${mi}, this.value)">
                <button class="nz-x" onclick="Normalizers.removeMapping(${mi})" title="Remove mapping">&times;</button>
                ${alert}
            `;
            container.appendChild(row);
        });

        if (this.currentMappings.length === 0) {
            container.innerHTML = '<p class="nz-empty">No field mappings.</p>';
        } else if (shown === 0) {
            container.innerHTML = '<p class="nz-empty">No mappings match the filter.</p>';
        }

        const count = document.getElementById('normalizerMappingCount');
        if (count) {
            const n = this.currentMappings.length;
            count.textContent = filter ? `${shown} of ${n} mappings` : `${n} mapping${n !== 1 ? 's' : ''}`;
        }
    },

    updateMappingTarget(index, value) {
        const m = this.currentMappings[index];
        if (!m) return;
        m.target = value;
        this._markDirty();
        this._schedulePreview();
        // Duplicate-target detection is local, so refresh rows without waiting
        // for the preview round-trip.
        clearTimeout(this._mapRerenderTimer);
        this._mapRerenderTimer = setTimeout(() => this._renderMappings(), 400);
    },

    removeMappingSource(index, sourceIndex) {
        const m = this.currentMappings[index];
        if (!m || sourceIndex < 0 || sourceIndex >= m.sources.length) return;
        m.sources.splice(sourceIndex, 1);
        this._markDirty();
        this._renderMappings();
        this._schedulePreview();
    },

    removeMapping(index) {
        this.currentMappings.splice(index, 1);
        this._markDirty();
        this._renderMappings();
        this._schedulePreview();
    },

    filterMappings() {
        this._renderMappings();
    },

    addTimestampFieldRow(field, format) {
        const container = document.getElementById('normalizerTimestampFields');
        if (!container) return;

        const row = document.createElement('div');
        row.className = 'normalizer-ts-field-row';

        // Build preset options
        let presetOptions = '<option value="">Select preset...</option>';
        this.TIMESTAMP_PRESETS.forEach(p => {
            const selected = (format && format === p.value) ? ' selected' : '';
            presetOptions += `<option value="${Utils.escapeHtml(p.value)}"${selected}>${Utils.escapeHtml(p.label)}</option>`;
        });
        // Add custom option if format doesn't match any preset
        const isCustom = format && !this.TIMESTAMP_PRESETS.some(p => p.value === format);
        if (isCustom) {
            presetOptions += `<option value="${Utils.escapeHtml(format)}" selected>Custom: ${Utils.escapeHtml(format)}</option>`;
        }

        row.innerHTML = `
            <span class="ts-drag-handle" title="Drag to reorder">⋮⋮</span>
            <input type="text" class="ts-field-name" placeholder="Field name (e.g. system_time)" value="${Utils.escapeHtml(field || '')}">
            <select class="ts-field-preset" title="Select a preset or type a custom format">
                ${presetOptions}
            </select>
            <input type="text" class="ts-field-format" placeholder="Custom Go format (e.g. 2006-01-02T15:04:05Z07:00)" value="${Utils.escapeHtml(format || '')}">
            <button class="btn-sm btn-danger mapping-remove" onclick="this.parentElement.remove()" title="Remove">&times;</button>
        `;

        // Wire up preset -> format sync
        const presetSelect = row.querySelector('.ts-field-preset');
        const formatInput = row.querySelector('.ts-field-format');
        presetSelect.addEventListener('change', () => {
            if (presetSelect.value) {
                formatInput.value = presetSelect.value;
            }
        });
        formatInput.addEventListener('input', () => {
            const match = this.TIMESTAMP_PRESETS.find(p => p.value === formatInput.value);
            presetSelect.value = match ? match.value : '';
        });

        // Drag-to-reorder: only activate drag when the handle is grabbed
        const handle = row.querySelector('.ts-drag-handle');
        handle.addEventListener('mousedown', () => { row.draggable = true; });
        handle.addEventListener('mouseup', () => { row.draggable = false; });
        handle.addEventListener('mouseleave', () => { row.draggable = false; });

        row.addEventListener('dragstart', (e) => {
            Normalizers._draggedTsRow = row;
            row.classList.add('ts-row-dragging');
            e.dataTransfer.effectAllowed = 'move';
        });
        row.addEventListener('dragend', () => {
            Normalizers._draggedTsRow = null;
            row.classList.remove('ts-row-dragging');
            row.draggable = false;
            container.querySelectorAll('.normalizer-ts-field-row').forEach(r => r.classList.remove('ts-row-drag-over'));
        });
        row.addEventListener('dragover', (e) => {
            e.preventDefault();
            if (Normalizers._draggedTsRow && Normalizers._draggedTsRow !== row) {
                container.querySelectorAll('.normalizer-ts-field-row').forEach(r => r.classList.remove('ts-row-drag-over'));
                row.classList.add('ts-row-drag-over');
            }
        });
        row.addEventListener('drop', (e) => {
            e.preventDefault();
            const dragged = Normalizers._draggedTsRow;
            if (dragged && dragged !== row) {
                const rows = [...container.querySelectorAll('.normalizer-ts-field-row')];
                const dragIdx = rows.indexOf(dragged);
                const dropIdx = rows.indexOf(row);
                container.insertBefore(dragged, dragIdx < dropIdx ? row.nextSibling : row);
                row.classList.remove('ts-row-drag-over');
            }
        });

        container.appendChild(row);
    },

    // Bulk alias editing. Chips handle one-off edits; pasting a block of 200
    // aliases is still far quicker in a textarea.
    openMappingSourcesModal(index) {
        const m = this.currentMappings[index];
        if (!m) return;
        this._editingMappingIndex = index;
        const textarea = document.getElementById('mappingSourcesTextarea');
        textarea.value = m.sources.join('\n');
        document.getElementById('mappingSourcesModal').style.display = 'flex';
        textarea.focus();
    },

    saveMappingSourcesModal() {
        const textarea = document.getElementById('mappingSourcesTextarea');
        const sources = textarea.value.split('\n').map(s => s.trim()).filter(s => s);
        const m = this.currentMappings[this._editingMappingIndex];
        if (m) {
            m.sources = sources;
            this._markDirty();
            this._renderMappings();
            this._schedulePreview();
        }
        this.closeMappingSourcesModal();
    },

    closeMappingSourcesModal() {
        document.getElementById('mappingSourcesModal').style.display = 'none';
        this._editingMappingIndex = null;
    },

    // --- Derived fields (value mappings) ---

    _editingDerivedRow: null,

    addDerivedFieldRow(fromField, toField, valueMap, def) {
        const container = document.getElementById('normalizerDerivedFields');
        if (!container) return;

        const row = document.createElement('div');
        row.className = 'normalizer-derived-row';
        row._valueMap = (valueMap && typeof valueMap === 'object') ? { ...valueMap } : {};
        const count = Object.keys(row._valueMap).length;

        row.innerHTML = `
            <input type="text" class="derived-from" placeholder="source_field (e.g. event_id)" value="${Utils.escapeHtml(fromField || '')}">
            <span class="mapping-arrow">-></span>
            <input type="text" class="derived-to" placeholder="derived_field (e.g. category)" value="${Utils.escapeHtml(toField || '')}">
            <button type="button" class="btn-sm btn-secondary derived-values-btn"><span class="derived-values-count">${count}</span> value${count !== 1 ? 's' : ''}</button>
            <input type="text" class="derived-default" placeholder="fallback (optional)" value="${Utils.escapeHtml(def || '')}">
            <button class="btn-sm btn-danger mapping-remove" onclick="this.parentElement.remove()" title="Remove">&times;</button>
        `;
        row.querySelector('.derived-values-btn').addEventListener('click', () => this.openDerivedValuesModal(row));
        row.querySelector('.mapping-remove').addEventListener('click', () => {
            this._markDirty();
            this._schedulePreview();
        });
        container.appendChild(row);
        this._markDirty();
    },

    _renderDerivedValuesCount(row) {
        const count = Object.keys(row._valueMap || {}).length;
        const btn = row.querySelector('.derived-values-btn');
        if (btn) btn.innerHTML = `<span class="derived-values-count">${count}</span> value${count !== 1 ? 's' : ''}`;
    },

    openDerivedValuesModal(row) {
        this._editingDerivedRow = row;
        const map = row._valueMap || {};
        const lines = Object.entries(map).map(([k, v]) => `${k}=${v}`).join('\n');
        const textarea = document.getElementById('derivedValuesTextarea');
        textarea.value = lines;
        document.getElementById('derivedValuesModal').style.display = 'flex';
        textarea.focus();
    },

    saveDerivedValuesModal() {
        const textarea = document.getElementById('derivedValuesTextarea');
        const map = {};
        textarea.value.split('\n').forEach(line => {
            const trimmed = line.trim();
            if (!trimmed) return;
            const idx = trimmed.indexOf('=');
            if (idx <= 0) return;
            const k = trimmed.substring(0, idx).trim();
            const v = trimmed.substring(idx + 1).trim();
            if (k) map[k] = v;
        });
        if (this._editingDerivedRow) {
            this._editingDerivedRow._valueMap = map;
            this._renderDerivedValuesCount(this._editingDerivedRow);
            this._markDirty();
            this._schedulePreview();
        }
        this.closeDerivedValuesModal();
    },

    closeDerivedValuesModal() {
        document.getElementById('derivedValuesModal').style.display = 'none';
        this._editingDerivedRow = null;
    },

    _getFormData() {
        const name = document.getElementById('normalizerName').value.trim();
        const description = document.getElementById('normalizerDescription').value.trim();

        const transforms = [...this.currentTransforms];

        // Read from state, not the DOM: the mapping list is filtered for display,
        // so hidden rows would otherwise be dropped on save.
        const fieldMappings = [];
        this.currentMappings.forEach(m => {
            const target = (m.target || '').trim();
            const sources = (m.sources || []).map(s => s.trim()).filter(Boolean);
            if (target && sources.length > 0) {
                fieldMappings.push({ sources, target });
            }
        });

        const valueMappings = [];
        const derivedRows = document.querySelectorAll('#normalizerDerivedFields .normalizer-derived-row');
        derivedRows.forEach(row => {
            const fromField = row.querySelector('.derived-from').value.trim();
            const toField = row.querySelector('.derived-to').value.trim();
            const def = row.querySelector('.derived-default').value.trim();
            const map = row._valueMap || {};
            if (fromField && toField && (Object.keys(map).length > 0 || def)) {
                valueMappings.push({ from_field: fromField, to_field: toField, map, default: def });
            }
        });

        const timestampFields = [];
        const tsRows = document.querySelectorAll('#normalizerTimestampFields .normalizer-ts-field-row');
        tsRows.forEach(row => {
            const field = row.querySelector('.ts-field-name').value.trim();
            const format = row.querySelector('.ts-field-format').value.trim();
            if (field && format) {
                timestampFields.push({ field, format });
            }
        });

        return { name, description, transforms, field_mappings: fieldMappings, value_mappings: valueMappings, timestamp_fields: timestampFields };
    },

    async saveNormalizer() {
        const body = this._getFormData();

        if (!body.name) {
            Utils.showNotification('Name is required', 'error');
            return;
        }

        try {
            if (this.editingId) {
                await HttpUtils.safeFetch(`/api/v1/normalizers/${this.editingId}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body)
                });
                Utils.showNotification('Normalizer updated', 'success');
            } else {
                await HttpUtils.safeFetch('/api/v1/normalizers', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body)
                });
                Utils.showNotification('Normalizer created', 'success');
            }
            this._dirty = false;
            this._syncDirtyUI();
            this.backToList();
        } catch (err) {
            Utils.showNotification(`Failed to save: ${err.message}`, 'error');
        }
    },

    async deleteNormalizer(id) {
        // Show token usage before confirming
        let usageMsg = '';
        try {
            const data = await HttpUtils.safeFetch(`/api/v1/normalizers/${id}/tokens`);
            const tokens = data.data.tokens || [];
            if (tokens.length > 0) {
                const names = tokens.map(t => `${t.token_name} (${t.fractal_name})`).join(', ');
                usageMsg = `\n\nCurrently used by ${tokens.length} token(s): ${names}`;
            }
        } catch { /* ignore */ }

        if (!confirm(`Delete this normalizer? Tokens using it will revert to no normalization.${usageMsg}`)) return;
        try {
            await HttpUtils.safeFetch(`/api/v1/normalizers/${id}`, { method: 'DELETE' });
            Utils.showNotification('Normalizer deleted', 'success');
            this.loadNormalizers();
        } catch (err) {
            Utils.showNotification(`Failed to delete: ${err.message}`, 'error');
        }
    },

    async setDefault(id) {
        try {
            await HttpUtils.safeFetch(`/api/v1/normalizers/${id}/set-default`, { method: 'POST' });
            Utils.showNotification('Default normalizer updated', 'success');
            this.loadNormalizers();
        } catch (err) {
            Utils.showNotification(`Failed to set default: ${err.message}`, 'error');
        }
    },

    // --- Duplicate ---

    async duplicateNormalizer(id) {
        try {
            await HttpUtils.safeFetch(`/api/v1/normalizers/${id}/duplicate`, { method: 'POST' });
            Utils.showNotification('Normalizer duplicated', 'success');
            this.loadNormalizers();
        } catch (err) {
            Utils.showNotification(`Failed to duplicate: ${err.message}`, 'error');
        }
    },

    // --- Export / Import YAML ---

    async exportNormalizer(id, name) {
        try {
            const response = await fetch(`/api/v1/normalizers/${id}/export`, { credentials: 'include' });
            if (!response.ok) throw new Error('Export failed');
            const yamlText = await response.text();
            const blob = new Blob([yamlText], { type: 'text/yaml' });
            const link = document.createElement('a');
            const safeName = name.replace(/[^a-zA-Z0-9_-]/g, '_');
            link.href = URL.createObjectURL(blob);
            link.download = `${safeName}.yaml`;
            link.style.display = 'none';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(link.href);
            Utils.showNotification('Normalizer exported', 'success');
        } catch (err) {
            Utils.showNotification(`Export failed: ${err.message}`, 'error');
        }
    },

    importNormalizer() {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.yaml,.yml';
        input.onchange = async (e) => {
            const file = e.target.files[0];
            if (!file) return;
            try {
                const text = await file.text();
                const response = await fetch('/api/v1/normalizers/import', {
                    method: 'POST',
                    headers: { 'Content-Type': 'text/yaml' },
                    credentials: 'include',
                    body: text
                });
                const data = await response.json();
                if (!data.success) throw new Error(data.error || 'Import failed');
                Utils.showNotification('Normalizer imported', 'success');
                this.loadNormalizers();
            } catch (err) {
                Utils.showNotification(`Import failed: ${err.message}`, 'error');
            }
        };
        input.click();
    },

    // --- Live preview ---

    // Coalesces bursts of edits into one request and cancels any in-flight call,
    // so a fast typist produces one preview rather than a queue of them.
    _schedulePreview(delay = 250) {
        clearTimeout(this._previewTimer);
        this._previewTimer = setTimeout(() => this.runPreview(), delay);
    },

    async runPreview() {
        const textarea = document.getElementById('normalizerPreviewInput');
        const body = document.getElementById('normalizerPreviewResults');
        if (!textarea || !body) return;

        const sampleJSON = textarea.value.trim();
        if (!sampleJSON) {
            this._renderPreviewMessage('Paste a log or capture one from a fractal to see the output.');
            return;
        }
        try {
            JSON.parse(sampleJSON);
        } catch (err) {
            this._renderPreviewMessage(`Not valid JSON: ${err.message}`, true);
            return;
        }

        // Direct fetch rather than HttpUtils.safeFetch: superseded previews are
        // aborted on every keystroke, and safeFetch logs each abort as an error.
        this._previewAbort?.abort();
        const controller = new AbortController();
        this._previewAbort = controller;

        const formData = this._getFormData();
        const started = performance.now();

        try {
            const resp = await fetch('/api/v1/normalizers/preview', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                signal: controller.signal,
                body: JSON.stringify({
                    transforms: formData.transforms,
                    field_mappings: formData.field_mappings,
                    value_mappings: formData.value_mappings,
                    sample_json: sampleJSON
                })
            });
            const data = await resp.json();
            if (controller.signal.aborted) return;
            if (!resp.ok || !data.success) {
                this._renderPreviewMessage(data.error || `Preview failed (HTTP ${resp.status})`, true);
                return;
            }
            this._renderPreview(data.data, performance.now() - started);
        } catch (err) {
            if (err.name === 'AbortError') return;
            this._renderPreviewMessage(err.message, true);
        }
    },

    _renderPreviewMessage(msg, isError) {
        const body = document.getElementById('normalizerPreviewResults');
        if (body) {
            body.innerHTML = `<tr><td colspan="3" class="nz-preview-msg${isError ? ' nz-preview-error' : ''}">${Utils.escapeHtml(msg)}</td></tr>`;
        }
        this._previewHits = {};
        this._previewCollisions = {};
        this._renderStats(null);
        document.getElementById('normalizerPreviewWarning').innerHTML = '';
        document.getElementById('normalizerRecalc').textContent = '';
    },

    _renderPreview(result, elapsedMs) {
        const fields = result.fields || [];
        const collisions = result.collisions || {};

        // Alias match info drives the chip highlighting in stage 3.
        const hits = {};
        fields.forEach(f => {
            if (f.mapping_index >= 0 && f.matched_alias) {
                (hits[f.mapping_index] = hits[f.mapping_index] || []).push(f.matched_alias);
            }
        });
        this._previewHits = hits;
        this._previewCollisions = collisions;

        const mapped = fields.filter(f => f.mapping_index >= 0).length;
        const derived = fields.filter(f => f.derived).length;
        const collisionCount = Object.keys(collisions).length;
        const passthrough = fields.length - mapped - derived;

        this._renderStats({ total: fields.length, mapped, derived, collisions: collisionCount });

        const outMeta = document.getElementById('normalizerOutputMeta');
        if (outMeta) outMeta.textContent = `${passthrough} passthrough`;

        const warn = document.getElementById('normalizerPreviewWarning');
        warn.innerHTML = collisionCount === 0 ? '' :
            `<div class="nz-warn"><strong>${collisionCount} name collision${collisionCount !== 1 ? 's' : ''}:</strong> ` +
            Object.keys(collisions).map(c => `<code>${Utils.escapeHtml(c)}</code>`).join(', ') +
            '. Ingestion keeps only one value per colliding name, and which one is not deterministic.</div>';

        const body = document.getElementById('normalizerPreviewResults');
        if (fields.length === 0) {
            body.innerHTML = '<tr><td colspan="3" class="nz-preview-msg">This sample produced no fields.</td></tr>';
        } else {
            body.innerHTML = fields.map(f => {
                let cls = 'nz-row-passthrough';
                let badge = '<span class="nz-badge nz-badge-passthrough">passthrough</span>';
                if (f.collision) {
                    cls = 'nz-row-collision';
                    badge = '<span class="nz-badge nz-badge-collision">collision</span>';
                } else if (f.derived) {
                    cls = 'nz-row-derived';
                    badge = `<span class="nz-badge nz-badge-derived">${f.override ? 'override' : 'derived'}</span>`;
                } else if (f.mapping_index >= 0) {
                    cls = 'nz-row-mapped';
                    badge = '<span class="nz-badge nz-badge-mapped">mapped</span>';
                }
                return `<tr class="${cls}">
                    <td class="nz-f-name">${Utils.escapeHtml(f.name)} ${badge}</td>
                    <td class="nz-f-val">${Utils.escapeHtml(f.value)}</td>
                    <td class="nz-f-from">${Utils.escapeHtml(f.source)}</td>
                </tr>`;
            }).join('');
        }

        const recalc = document.getElementById('normalizerRecalc');
        if (recalc) recalc.textContent = `${Math.round(elapsedMs)} ms`;

        // Collapsed rail must keep showing the collision signal.
        const stripCount = document.getElementById('normalizerStripCount');
        if (stripCount) stripCount.textContent = `${fields.length} fields`;
        const stripBadge = document.getElementById('normalizerStripBadge');
        if (stripBadge) {
            stripBadge.textContent = String(collisionCount);
            stripBadge.classList.toggle('nz-strip-badge-show', collisionCount > 0);
            stripBadge.title = `${collisionCount} name collision${collisionCount !== 1 ? 's' : ''} in the current sample`;
        }

        this._renderMappings();
    },

    _renderStats(s) {
        const el = document.getElementById('normalizerStats');
        if (!el) return;
        if (!s) {
            el.innerHTML = '';
            return;
        }
        const stat = (n, label, tone) =>
            `<div class="nz-stat ${n ? tone : 'nz-stat-zero'}"><div class="nz-stat-n">${n}</div><div class="nz-stat-l">${label}</div></div>`;
        el.innerHTML =
            stat(s.total, 'Fields out', '') +
            stat(s.mapped, 'Mapped', 'nz-stat-info') +
            stat(s.derived, 'Derived', 'nz-stat-ok') +
            stat(s.collisions, 'Collisions', 'nz-stat-warn');
    },

    // --- Sample capture ---

    async _loadCaptureFractals() {
        const select = document.getElementById('normalizerCaptureFractal');
        if (!select || select.dataset.loaded === '1') return;
        try {
            const resp = await fetch('/api/v1/fractals', { credentials: 'include' });
            const data = await resp.json();
            const fractals = (data.data?.fractals || data.fractals || []).filter(f => f && f.id);
            if (fractals.length === 0) return;

            const current = window.FractalContext?.currentFractal?.id;
            select.innerHTML = fractals
                .map(f => `<option value="${Utils.escapeHtml(f.id)}"${f.id === current ? ' selected' : ''}>${Utils.escapeHtml(f.name || f.id)}</option>`)
                .join('');
            select.dataset.loaded = '1';
        } catch {
            select.innerHTML = '<option value="">No fractals</option>';
        }
    },

    async captureSamples() {
        const btn = document.getElementById('normalizerCaptureBtn');
        const select = document.getElementById('normalizerCaptureFractal');
        const note = document.getElementById('normalizerCaptureNote');
        const fractalId = select?.value;
        if (!fractalId) {
            Utils.showNotification('Select a fractal to capture from', 'error');
            return;
        }

        const original = btn.innerHTML;
        btn.disabled = true;
        btn.textContent = 'Capturing...';
        try {
            const data = await HttpUtils.safeFetch(`/api/v1/normalizers/samples?fractal_id=${encodeURIComponent(fractalId)}&limit=5`);
            this.currentSamples = data.data.samples || [];
            if (this.currentSamples.length === 0) {
                note.textContent = 'No JSON logs found in this fractal in the last 7 days.';
                this._renderSampleBar();
                return;
            }
            const name = select.options[select.selectedIndex]?.text || 'fractal';
            note.textContent = `${this.currentSamples.length} distinct shape${this.currentSamples.length !== 1 ? 's' : ''} from ${name}`;
            this.selectSample(0);
        } catch (err) {
            note.textContent = `Capture failed: ${err.message}`;
        } finally {
            btn.disabled = false;
            btn.innerHTML = original;
        }
    },

    selectSample(index) {
        const sample = this.currentSamples[index];
        if (!sample) return;
        this._activeSampleIndex = index;
        const input = document.getElementById('normalizerPreviewInput');
        if (input) {
            try {
                input.value = JSON.stringify(JSON.parse(sample.raw_log), null, 2);
            } catch {
                input.value = sample.raw_log;
            }
        }
        this._renderSampleBar();
        this._schedulePreview(0);
    },

    _renderSampleBar() {
        const bar = document.getElementById('normalizerSampleBar');
        if (!bar) return;
        const samples = this.currentSamples || [];
        if (samples.length === 0) {
            bar.innerHTML = '';
            return;
        }
        bar.innerHTML = samples.map((s, i) => {
            const when = s.timestamp ? new Date(s.timestamp).toLocaleTimeString() : '';
            return `<button class="nz-sample-tab${i === this._activeSampleIndex ? ' nz-sample-active' : ''}" onclick="Normalizers.selectSample(${i})" title="${s.fields_num} top-level fields, captured ${Utils.escapeHtml(when)}">${s.fields_num} fields</button>`;
        }).join('');
        const meta = document.getElementById('normalizerInputMeta');
        if (meta) {
            meta.textContent = this._activeSampleIndex >= 0
                ? `captured sample ${this._activeSampleIndex + 1} of ${samples.length}`
                : 'edited';
        }
    },

    // --- Preview rail ---

    toggleRail() {
        const rail = document.getElementById('normalizerRail');
        const wb = document.getElementById('normalizerWorkbench');
        if (!rail || !wb) return;
        const collapsed = !rail.classList.contains('nz-rail-collapsed');
        rail.classList.toggle('nz-rail-collapsed', collapsed);
        wb.classList.toggle('nz-workbench-collapsed', collapsed);
        try { localStorage.setItem('bifract.normalizer.railCollapsed', collapsed ? '1' : '0'); } catch { /* private mode */ }
    },

    _restoreRailState() {
        let collapsed = false;
        try { collapsed = localStorage.getItem('bifract.normalizer.railCollapsed') === '1'; } catch { /* private mode */ }
        document.getElementById('normalizerRail')?.classList.toggle('nz-rail-collapsed', collapsed);
        document.getElementById('normalizerWorkbench')?.classList.toggle('nz-workbench-collapsed', collapsed);
    },

};

window.Normalizers = Normalizers;
