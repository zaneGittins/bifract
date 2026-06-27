// Column sizing for results tables: smart defaults, smooth pointer-driven
// resizing, double-click autofit, and per-fractal persistence.
//
// Tables using this MUST be table-layout: fixed and render a <colgroup>
// (see ColumnSizing.buildColgroup). Widths are authoritative per column, so
// columns no longer reflow as the user pages through results.
const ColumnSizing = {
    MIN: 56,
    MAX_AUTOFIT: 640,
    STORAGE_KEY: 'bifract.colLayout.v2',
    MAX_SIGS_PER_FRACTAL: 40,
    REORDER_THRESHOLD: 5,

    // Fields that should flex to fill remaining horizontal space (the log
    // message). Only one such column flexes; the first match wins.
    FLEX_FIELDS: ['raw_log', 'message', 'msg', 'body', '_raw', 'log'],

    _canvas: null,

    // --- text measurement (used by defaults + autofit) ---

    _measure(text, font) {
        if (!this._canvas) this._canvas = document.createElement('canvas');
        const ctx = this._canvas.getContext('2d');
        ctx.font = font;
        return ctx.measureText(text || '').width;
    },

    _fontOf(el, fallback) {
        if (!el) return fallback;
        const cs = getComputedStyle(el);
        if (!cs.fontSize) return fallback;
        return `${cs.fontWeight} ${cs.fontSize} ${cs.fontFamily}`;
    },

    // --- signatures + storage ---

    signature(fields) {
        // Short stable hash of the column SET (order-independent) so different
        // query shapes keep independent layouts, while reordering the same
        // columns reuses the saved widths/order rather than orphaning them.
        let h = 0;
        const s = fields.slice().sort().join('|');
        for (let i = 0; i < s.length; i++) {
            h = (h << 5) - h + s.charCodeAt(i);
            h |= 0;
        }
        return 's' + (h >>> 0).toString(36);
    },

    _readAll() {
        try {
            return JSON.parse(localStorage.getItem(this.STORAGE_KEY) || '{}') || {};
        } catch (e) {
            return {};
        }
    },

    // A stored entry is { w: {field->px}, o: [field,...] | null }.
    loadEntry(fractalId, sig) {
        const all = this._readAll();
        const byFractal = all[String(fractalId)] || {};
        const e = byFractal[sig];
        if (!e) return { w: {}, o: null };
        return { w: e.w || {}, o: e.o || null };
    },

    _writeEntry(fractalId, sig, entry) {
        try {
            const all = this._readAll();
            const key = String(fractalId);
            const byFractal = all[key] || {};
            byFractal[sig] = entry;
            // Bound growth: drop oldest signatures for this fractal.
            const sigs = Object.keys(byFractal);
            if (sigs.length > this.MAX_SIGS_PER_FRACTAL) {
                for (const old of sigs.slice(0, sigs.length - this.MAX_SIGS_PER_FRACTAL)) {
                    delete byFractal[old];
                }
            }
            all[key] = byFractal;
            localStorage.setItem(this.STORAGE_KEY, JSON.stringify(all));
        } catch (e) {
            // Storage full or unavailable: persistence is best-effort.
        }
    },

    load(fractalId, sig) {
        return this.loadEntry(fractalId, sig).w;
    },

    save(fractalId, sig, widths) {
        const e = this.loadEntry(fractalId, sig);
        e.w = widths;
        this._writeEntry(fractalId, sig, e);
    },

    loadOrder(fractalId, sig) {
        return this.loadEntry(fractalId, sig).o;
    },

    saveOrder(fractalId, sig, order) {
        const e = this.loadEntry(fractalId, sig);
        e.o = order;
        this._writeEntry(fractalId, sig, e);
    },

    clearFractal(fractalId) {
        try {
            const all = this._readAll();
            if (all[String(fractalId)]) {
                delete all[String(fractalId)];
                localStorage.setItem(this.STORAGE_KEY, JSON.stringify(all));
            }
        } catch (e) { /* noop */ }
    },

    // --- default widths (computed before the table is in the DOM) ---

    _defaultFont: '400 13px ui-sans-serif, system-ui, -apple-system, sans-serif',

    defaultWidth(field, results, isNumeric) {
        const lower = String(field).toLowerCase();
        const header = this._measure(field, '600 13px ui-sans-serif, system-ui, sans-serif');

        // Sample content width from up to 80 rows.
        let content = 0;
        const limit = Math.min(results.length, 80);
        for (let i = 0; i < limit; i++) {
            const v = results[i][field];
            if (v === undefined || v === null) continue;
            const text = typeof v === 'object' ? JSON.stringify(v) : String(v);
            const w = this._measure(text, this._defaultFont);
            if (w > content) content = w;
        }

        const raw = Math.ceil(Math.max(header, content)) + 28; // padding + sort glyph

        let min = 88, max = 360;
        if (lower === 'timestamp' || lower === 'time' || lower.endsWith('_at')) {
            min = 158; max = 210;
        } else if (lower === 'level' || lower === 'severity' || lower === 'loglevel' || lower === 'log_level' || lower === 'status') {
            min = 64; max = 120;
        } else if (lower === 'log_id' || lower === 'id' || lower === 'trace_id' || lower === 'span_id' ||
                   lower === 'host' || lower === 'hostname' || lower === 'source' || lower === 'service' || lower === 'app') {
            min = 120; max = 220;
        } else if (isNumeric) {
            min = 80; max = 150;
        }
        return Math.min(max, Math.max(min, raw));
    },

    // Returns { widths, flexField, hasFiller }.
    //   widths    : { field -> px } for every column that gets an explicit width
    //   flexField : the field rendered with no width (fills remaining space), or null
    //   hasFiller : whether a trailing auto-width filler column is appended
    resolve(fractalId, fields, results, numericFields, sig) {
        const persisted = this.load(fractalId, sig || this.signature(fields));

        let flexField = null;
        for (const f of this.FLEX_FIELDS) {
            if (fields.includes(f)) { flexField = f; break; }
        }
        // A flex column only stays auto while the user hasn't pinned it.
        const flexAuto = flexField && persisted[flexField] == null;

        const widths = {};
        fields.forEach(field => {
            if (flexAuto && field === flexField) return; // auto, no explicit width
            const isNumeric = numericFields ? numericFields.has(field) : false;
            const def = this.defaultWidth(field, results, isNumeric);
            const saved = persisted[field];
            widths[field] = (typeof saved === 'number' && saved >= this.MIN) ? saved : def;
        });

        return {
            widths,
            flexField: flexAuto ? flexField : null,
            hasFiller: !flexAuto,
        };
    },

    buildColgroup(fields, resolved) {
        let html = '<colgroup>';
        fields.forEach(field => {
            if (field === resolved.flexField) {
                html += '<col class="col-flex">';
            } else {
                html += `<col style="width:${resolved.widths[field]}px">`;
            }
        });
        if (resolved.hasFiller) html += '<col class="col-filler">';
        html += '</colgroup>';
        return html;
    },

    // --- interaction: smooth drag + double-click autofit ---

    _colFor(table, idx) {
        const cg = table.querySelector('colgroup');
        return cg ? cg.children[idx] : null;
    },

    _autofit(table, idx, th) {
        const cellFont = this._fontOf(table.querySelector('tbody td'), this._defaultFont);
        const headFont = this._fontOf(th, '600 13px ui-sans-serif, system-ui, sans-serif');
        let max = this._measure(th.textContent, headFont);
        const rows = table.querySelectorAll('tbody tr');
        for (let i = 0; i < rows.length; i++) {
            const cell = rows[i].children[idx];
            if (cell) {
                const w = this._measure(cell.textContent, cellFont);
                if (w > max) max = w;
            }
        }
        return Math.min(this.MAX_AUTOFIT, Math.max(this.MIN, Math.ceil(max) + 28));
    },

    // Persist a single column width using the layout key stamped on the table
    // (data-colsize-fractal / data-colsize-sig). Used by both the global resize
    // handler and autofit, so resize "just works" on any colgroup table emitted
    // by buildResultsTable, regardless of where in the DOM it was inserted.
    _persistWidth(table, field, width) {
        const fractalId = table.dataset.colsizeFractal || 'default';
        const sig = table.dataset.colsizeSig;
        if (!sig) return;
        const w = Object.assign({}, this.load(fractalId, sig));
        w[field] = width;
        this.save(fractalId, sig, w);
    },

    // One-time document-level delegation for resize drag + double-click autofit.
    // Avoids per-table wiring (and the matching teardown) entirely.
    initGlobalInteractions() {
        if (this._globalInit || typeof document === 'undefined') return;
        this._globalInit = true;
        document.addEventListener('pointerdown', (e) => this._onResizeStart(e), true);
        document.addEventListener('dblclick', (e) => this._onAutofit(e), true);
    },

    _onResizeStart(e) {
        if (e.button !== 0) return;
        const resizer = e.target;
        if (!resizer.classList || !resizer.classList.contains('column-resizer')) return;
        const th = resizer.parentElement;
        const table = th && th.closest('table.results-table');
        if (!table) return;
        const col = this._colFor(table, th.cellIndex);
        if (!col) return;

        e.preventDefault();
        e.stopPropagation();

        const startX = e.clientX;
        const startWidth = th.offsetWidth;
        let latest = startWidth;
        let rafId = null;

        try { resizer.setPointerCapture(e.pointerId); } catch (err) { /* noop */ }
        th.classList.add('resizing');
        document.body.classList.add('col-resizing');

        const flush = () => { rafId = null; col.style.width = latest + 'px'; };
        const onMove = (ev) => {
            latest = Math.max(this.MIN, startWidth + (ev.clientX - startX));
            if (rafId === null) rafId = requestAnimationFrame(flush);
        };
        const onUp = () => {
            if (rafId !== null) cancelAnimationFrame(rafId);
            col.style.width = latest + 'px';
            th.classList.remove('resizing');
            document.body.classList.remove('col-resizing');
            resizer.removeEventListener('pointermove', onMove);
            resizer.removeEventListener('pointerup', onUp);
            resizer.removeEventListener('pointercancel', onUp);
            try { resizer.releasePointerCapture(e.pointerId); } catch (err) { /* noop */ }
            if (Math.round(latest) !== Math.round(startWidth)) {
                this._persistWidth(table, th.dataset.field, Math.round(latest));
            }
        };

        resizer.addEventListener('pointermove', onMove);
        resizer.addEventListener('pointerup', onUp);
        resizer.addEventListener('pointercancel', onUp);
    },

    _onAutofit(e) {
        const resizer = e.target;
        if (!resizer.classList || !resizer.classList.contains('column-resizer')) return;
        const th = resizer.parentElement;
        const table = th && th.closest('table.results-table');
        if (!table) return;
        const col = this._colFor(table, th.cellIndex);
        if (!col) return;
        e.preventDefault();
        e.stopPropagation();
        const w = this._autofit(table, th.cellIndex, th);
        col.style.width = w + 'px';
        this._persistWidth(table, th.dataset.field, w);
    },

    // Wire drag-to-reorder onto every header cell. Reorder only begins once the
    // pointer crosses REORDER_THRESHOLD, so a stationary click still sorts and a
    // drag starting on the resize handle still resizes (the handle stops
    // propagation, and we ignore pointerdowns whose target is the handle).
    // onReorder(field, targetIndex) fires on drop; targetIndex is the insertion
    // slot in the current visible column order (0..N).
    attachReordering(table, onReorder) {
        if (!table) return;
        const cells = () => Array.from(table.querySelectorAll('thead th[data-field]'));

        cells().forEach(th => {
            th.addEventListener('pointerdown', (e) => {
                if (e.button !== 0) return;
                if (e.target.classList.contains('column-resizer')) return;

                const field = th.dataset.field;
                const startX = e.clientX, startY = e.clientY;
                const pointerId = e.pointerId;
                let dragging = false;
                let indicator = null;
                let targetIndex = -1;

                const place = (clientX) => {
                    const list = cells();
                    let idx = list.length;
                    for (let i = 0; i < list.length; i++) {
                        const r = list[i].getBoundingClientRect();
                        if (clientX < r.left + r.width / 2) { idx = i; break; }
                    }
                    targetIndex = idx;
                    const tableRect = table.getBoundingClientRect();
                    const x = idx >= list.length
                        ? list[list.length - 1].getBoundingClientRect().right
                        : list[idx].getBoundingClientRect().left;
                    indicator.style.left = x + 'px';
                    indicator.style.top = tableRect.top + 'px';
                    indicator.style.height = tableRect.height + 'px';
                };

                const cleanup = () => {
                    th.removeEventListener('pointermove', onMove);
                    th.removeEventListener('pointerup', onUp);
                    th.removeEventListener('pointercancel', onCancel);
                    th.classList.remove('reordering');
                    document.body.classList.remove('col-reordering');
                    if (indicator) { indicator.remove(); indicator = null; }
                    try { th.releasePointerCapture(pointerId); } catch (err) { /* noop */ }
                };

                const onMove = (ev) => {
                    if (!dragging) {
                        if (Math.abs(ev.clientX - startX) < this.REORDER_THRESHOLD &&
                            Math.abs(ev.clientY - startY) < this.REORDER_THRESHOLD) return;
                        dragging = true;
                        try { th.setPointerCapture(pointerId); } catch (err) { /* noop */ }
                        th.classList.add('reordering');
                        document.body.classList.add('col-reordering');
                        indicator = document.createElement('div');
                        indicator.className = 'col-drop-indicator';
                        document.body.appendChild(indicator);
                    }
                    place(ev.clientX);
                };
                const onUp = () => {
                    const wasDragging = dragging;
                    const ti = targetIndex;
                    cleanup();
                    if (wasDragging && ti >= 0 && onReorder) onReorder(field, ti);
                };
                const onCancel = () => cleanup();

                th.addEventListener('pointermove', onMove);
                th.addEventListener('pointerup', onUp);
                th.addEventListener('pointercancel', onCancel);
            });
        });
    },
};

window.ColumnSizing = ColumnSizing;
ColumnSizing.initGlobalInteractions();
