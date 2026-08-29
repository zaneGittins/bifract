// Active notebook, in the search page's left rail.
//
// The notebook was only reachable after an investigation: you left the search
// page, opened a notebook, and pasted. Everything worth keeping had to survive
// that round trip, so most of it did not. The rail makes the notebook something
// you add to at the moment you find something, and shows what has accumulated
// as a compact outline that can be read in notebook order or as a chronology.
//
// The active notebook is per fractal/prism. Carrying one across a scope switch
// would send every capture to a notebook the new scope cannot even read.
const NotebookRail = {
    activeId: null,
    summary: null,
    order: 'manual',        // 'manual' | 'time'
    _state: 'idle',         // 'idle' | 'loading' | 'ready' | 'error'
    _controller: null,
    _pickerOpen: false,
    _pickerTimer: null,
    _loadToken: 0,
    _pickerToken: 0,

    // log_ids already pinned into the active notebook, rebuilt on every load.
    _pinnedLogIds: new Set(),

    // Section type accents, matching the notebook page's own legend.
    _dotColors: {
        markdown: 'var(--text-muted)',
        query: 'var(--accent-primary)',
        comment_context: '#d4a054',
        ai_summary: 'var(--accent-secondary)',
        ai_attack_chain: 'var(--error)',
    },
    _typeLabels: {
        markdown: 'note',
        query: 'query',
        comment_context: 'evidence',
        ai_summary: 'summary',
        ai_attack_chain: 'attack chain',
    },

    // Counted separately from the tooltip labels: "evidence" is a mass noun and
    // does not pluralise into a count line.
    _countLabels: {
        query: ['query', 'queries'],
        comment_context: ['event', 'events'],
        markdown: ['note', 'notes'],
    },

    init() {
        if (window.RailPanel) {
            RailPanel.registerPane('notebook', {
                onShow: () => this._onShow(),
                onHide: () => this._onHide(),
            });
        }

        this._wire('nbrChooseBtn', 'click', () => this.openPicker());
        this._wire('nbrSwitchBtn', 'click', () => this.openPicker());
        this._wire('nbrPickerClose', 'click', () => this.closePicker());
        this._wire('nbrOpenBtn', 'click', () => this.openNotebook());
        this._wire('nbrAddQuery', 'click', () => this.captureCurrentQuery());
        this._wire('nbrAddNote', 'click', () => this.showNoteEditor());
        this._wire('nbrNoteCancel', 'click', () => this.hideNoteEditor());
        this._wire('nbrNoteSave', 'click', () => this.saveNote());
        this._wire('nbrOrderToggle', 'click', () => this.toggleOrder());
        this._wire('nbrNewBtn', 'click', () => this.createNotebook());

        const search = document.getElementById('nbrPickerSearch');
        if (search) {
            search.addEventListener('input', (e) => {
                clearTimeout(this._pickerTimer);
                const term = e.target.value;
                this._pickerTimer = setTimeout(() => this.loadPickerList(term), 250);
            });
        }

        const newName = document.getElementById('nbrNewName');
        if (newName) {
            newName.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') { e.preventDefault(); this.createNotebook(); }
            });
        }

        const list = document.getElementById('nbrList');
        if (list) list.addEventListener('click', (e) => this._onListClick(e));

        const pickerList = document.getElementById('nbrPickerList');
        if (pickerList) pickerList.addEventListener('click', (e) => this._onPickerClick(e));

        if (window.FractalContext) {
            FractalContext.subscribe('NotebookRail', () => this.onFractalChange());
        }

        this._restoreActive();
    },

    _wire(id, event, fn) {
        const el = document.getElementById(id);
        if (el) el.addEventListener(event, fn);
    },

    // ---- scope ------------------------------------------------------------

    _scopeId() {
        const ctx = window.FractalContext;
        if (!ctx || !ctx.currentFractal || !ctx.currentFractal.id) return null;
        return (ctx.isPrism() ? 'prism:' : 'fractal:') + ctx.currentFractal.id;
    },

    _storageKey() {
        const scope = this._scopeId();
        return scope ? `bifract_active_notebook_${scope}` : null;
    },

    _restoreActive() {
        const key = this._storageKey();
        this.activeId = null;
        this.summary = null;
        this._state = 'idle';
        if (!key) return;
        try {
            this.activeId = localStorage.getItem(key) || null;
        } catch (e) {
            this.activeId = null;
        }
    },

    _persistActive() {
        const key = this._storageKey();
        if (!key) return;
        try {
            if (this.activeId) localStorage.setItem(key, this.activeId);
            else localStorage.removeItem(key);
        } catch (e) {
            // localStorage may be unavailable; the rail still works for this session.
        }
    },

    onFractalChange() {
        this.closePicker();
        this.hideNoteEditor();
        this._abort();
        this._restoreActive();
        this.render();
        if (window.RailPanel && RailPanel.isPaneVisible('notebook')) this.load();
    },

    // ---- lifecycle --------------------------------------------------------

    _onShow() {
        this.render();
        this.load();
    },

    _onHide() {
        this._abort();
        this.closePicker();
    },

    _abort() {
        if (this._controller) { this._controller.abort(); this._controller = null; }
    },

    // Open the rail on this pane, whatever it was showing.
    reveal() {
        if (window.RailPanel) RailPanel.open('notebook');
    },

    // ---- loading ----------------------------------------------------------

    async load() {
        if (!this.activeId) { this._state = 'idle'; this.render(); return; }

        this._abort();
        const controller = new AbortController();
        this._controller = controller;
        const token = ++this._loadToken;
        const scopeToken = window.FractalContext ? FractalContext.scopeToken() : null;

        this._state = 'loading';
        this.render();

        try {
            const res = await fetch(`/api/v1/notebooks/${encodeURIComponent(this.activeId)}/summary`, {
                credentials: 'include',
                signal: controller.signal,
            });

            if (token !== this._loadToken) return;
            if (scopeToken !== null && FractalContext.isScopeStale(scopeToken)) return;

            if (res.status === 404 || res.status === 403) {
                // The notebook was deleted, or this scope cannot read it. Forget
                // it rather than failing every capture from here on.
                this.setActive(null);
                this.render();
                if (window.Toast) Toast.show('That notebook is no longer available', 'warning');
                return;
            }

            const data = await res.json();
            if (token !== this._loadToken) return;
            if (!res.ok || !data.success) throw new Error(data.error || 'Failed to load notebook');

            this.summary = data.data;
            this._indexPinnedLogIds();
            this._state = 'ready';
        } catch (err) {
            if (err.name === 'AbortError' || token !== this._loadToken) return;
            this._state = 'error';
        }
        this.render();
    },

    // The stub carries the name so the header is not blank while the summary
    // loads. It deliberately does not assert can_edit: a viewer would see
    // capture buttons that every click answers with a 403.
    setActive(notebookId, name) {
        this.activeId = notebookId || null;
        this._pinnedLogIds = new Set();
        this.summary = notebookId && name ? { id: notebookId, name, sections: [], counts: {} } : null;
        this._persistActive();
        this._state = notebookId ? 'loading' : 'idle';
    },

    _indexPinnedLogIds() {
        const ids = new Set();
        for (const sec of (this.summary?.sections || [])) {
            if (sec.section_type !== 'comment_context') continue;
            const logID = this._parseJson(sec.content).log_id;
            if (logID) ids.add(String(logID));
        }
        this._pinnedLogIds = ids;
    },

    // Whether this event is already evidence in the active notebook. Answered
    // from the loaded outline, so asking costs nothing.
    hasPinned(logID) {
        return !!logID && this._pinnedLogIds.has(String(logID));
    },

    // ---- picker -----------------------------------------------------------

    openPicker() {
        this._pickerOpen = true;
        this.render();
        this.loadPickerList('');
        const search = document.getElementById('nbrPickerSearch');
        if (search) { search.value = ''; search.focus(); }
    },

    closePicker() {
        if (!this._pickerOpen) return;
        this._pickerOpen = false;
        clearTimeout(this._pickerTimer);
        this.render();
    },

    async loadPickerList(search) {
        const list = document.getElementById('nbrPickerList');
        if (!list) return;
        list.innerHTML = '<div class="fr-empty">Loading...</div>';

        // Typed searches can resolve out of order; only the newest may render.
        const token = ++this._pickerToken;
        const scopeToken = window.FractalContext ? FractalContext.scopeToken() : null;
        try {
            const params = new URLSearchParams({ limit: '50', offset: '0' });
            if (search) params.set('search', search);
            const res = await fetch(`/api/v1/notebooks?${params.toString()}`, { credentials: 'include' });
            const data = await res.json();
            if (token !== this._pickerToken) return;
            if (scopeToken !== null && FractalContext.isScopeStale(scopeToken)) return;
            if (!res.ok || !data.success) throw new Error(data.error || 'Failed to list notebooks');

            const notebooks = data.data || [];
            if (!notebooks.length) {
                list.innerHTML = '<div class="fr-empty">No notebooks yet</div>';
                return;
            }
            list.innerHTML = notebooks.map(nb => `
                <button class="nbr-picker-item${nb.id === this.activeId ? ' active' : ''}" data-notebook-id="${Utils.escapeHtml(nb.id)}" data-notebook-name="${Utils.escapeHtml(nb.name)}">
                    <span class="nbr-picker-name">${Utils.escapeHtml(nb.name)}</span>
                    ${nb.description ? `<span class="nbr-picker-desc">${Utils.escapeHtml(nb.description)}</span>` : ''}
                </button>
            `).join('');
        } catch (err) {
            if (token !== this._pickerToken) return;
            list.innerHTML = '<div class="fr-empty">Could not load notebooks</div>';
        }
    },

    _onPickerClick(e) {
        const item = e.target.closest('[data-notebook-id]');
        if (!item) return;
        this.setActive(item.dataset.notebookId, item.dataset.notebookName);
        this._pickerOpen = false;
        this.render();
        this.load();
    },

    async createNotebook() {
        const input = document.getElementById('nbrNewName');
        const name = (input?.value || '').trim();
        if (!name) {
            if (window.Toast) Toast.show('Name the notebook first', 'warning');
            return;
        }
        try {
            const res = await fetch('/api/v1/notebooks', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ name, description: '', time_range_type: '24h', max_results_per_section: 100 }),
            });
            const data = await res.json();
            if (!res.ok || !data.success) throw new Error(data.error || 'Failed to create notebook');

            const created = data.data;
            if (input) input.value = '';
            this.setActive(created.id, created.name);
            this._pickerOpen = false;
            this.render();
            this.load();
            if (window.Toast) Toast.success('Created', `"${created.name}" is now the active notebook`);
        } catch (err) {
            if (window.Toast) Toast.error('Error', err.message);
        }
    },

    openNotebook() {
        if (!this.activeId) return;
        if (window.App) App.showFractalViewTab('notebooks');
        if (window.Notebooks && typeof Notebooks.openNotebook === 'function') {
            Notebooks.openNotebook(this.activeId);
        }
    },

    // ---- capture ----------------------------------------------------------

    // POST one section. Every capture goes through here so append placement,
    // the missing-notebook case, and the refresh are handled once.
    async _addSection(body, successMessage) {
        if (!this.activeId) {
            this.reveal();
            this.openPicker();
            if (window.Toast) Toast.show('Choose an active notebook first', 'warning');
            return false;
        }

        try {
            const res = await fetch(`/api/v1/notebooks/${encodeURIComponent(this.activeId)}/sections`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(Object.assign({ append: true }, body)),
            });
            if (res.status === 404) {
                this.setActive(null);
                this.render();
                throw new Error('That notebook no longer exists');
            }
            const data = await res.json();
            if (!res.ok || !data.success) throw new Error(data.error || 'Failed to add to notebook');

            if (window.Toast) Toast.success('Added', successMessage);
            this.load();
            return true;
        } catch (err) {
            if (window.Toast) Toast.error('Notebook', err.message);
            return false;
        }
    },

    captureCurrentQuery() {
        const query = document.getElementById('queryInput')?.value?.trim();
        if (!query) {
            if (window.Toast) Toast.show('Enter a query first', 'warning');
            return;
        }
        this.reveal();

        // A query section runs against the notebook's own range, so the search
        // window is recorded as the section's event time rather than as a second
        // competing range: it places the capture on the timeline without
        // pretending the section will re-run over that window.
        const range = window.TimePicker ? TimePicker.getTimeRange() : null;
        const eventTime = range && range.end ? range.end : new Date().toISOString();

        this._addSection({
            section_type: 'query',
            title: '',
            content: query,
            event_time: eventTime,
        }, 'Query added to the notebook');
    },

    // Pin the event a caller is looking at as evidence. Shares the section shape
    // the comment generator writes, so it renders and pivots identically.
    pinLog(logData) {
        if (!logData || !logData.log_id) {
            if (window.Toast) Toast.show('This row has no log id to pin', 'warning');
            return;
        }
        if (this.hasPinned(logData.log_id)) {
            this.reveal();
            if (window.Toast) Toast.show('Already pinned to this notebook', 'info');
            return;
        }
        this.reveal();

        const eventTime = this._isoOrNull(logData.timestamp);
        const context = {
            log_id: String(logData.log_id),
            log_timestamp: eventTime || '',
            commented_at: new Date().toISOString(),
            comment_text: '',
            query: document.getElementById('queryInput')?.value?.trim() || '',
            source: 'pin',
        };

        const author = window.Auth && Auth.currentUser;
        if (author) {
            context.author = author.username || '';
            context.author_display_name = author.display_name || author.username || '';
            context.author_gravatar_color = author.gravatar_color || '';
            context.author_gravatar_initial = author.gravatar_initial || '';
        }

        const section = {
            section_type: 'comment_context',
            title: this._pinTitle(logData),
            content: JSON.stringify(context),
        };
        if (eventTime) section.event_time = eventTime;
        this._addSection(section, 'Event pinned to the notebook');
    },

    // A pinned row needs a line that identifies it in an outline. Prefer the
    // fields an analyst scans for; fall back to the log id.
    _pinTitle(logData) {
        // Only scalar fields: a nested object stringifies to "[object Object]".
        const pick = (...keys) => {
            for (const key of keys) {
                const v = logData[key];
                if (typeof v === 'string' && v.trim()) return v.trim();
                if (typeof v === 'number' || typeof v === 'boolean') return String(v);
            }
            return '';
        };
        const text = [pick('host', 'computer', 'source'), pick('message', 'process', 'event_type', 'action')]
            .filter(Boolean).join(' - ');
        if (!text) return `Event ${String(logData.log_id).slice(0, 12)}`;
        return Array.from(text).slice(0, 80).join('');
    },

    showNoteEditor() {
        const editor = document.getElementById('nbrNoteEditor');
        if (!editor) return;
        editor.hidden = false;
        const text = document.getElementById('nbrNoteText');
        if (text) text.focus();
    },

    hideNoteEditor() {
        const editor = document.getElementById('nbrNoteEditor');
        if (editor) editor.hidden = true;
        const text = document.getElementById('nbrNoteText');
        if (text) text.value = '';
    },

    async saveNote() {
        const text = document.getElementById('nbrNoteText');
        const content = (text?.value || '').trim();
        if (!content) {
            if (window.Toast) Toast.show('Write something first', 'warning');
            return;
        }
        const ok = await this._addSection({
            section_type: 'markdown',
            title: '',
            content,
            event_time: new Date().toISOString(),
        }, 'Note added to the notebook');
        if (ok) this.hideNoteEditor();
    },

    // ---- ordering ---------------------------------------------------------

    toggleOrder() {
        this.order = this.order === 'manual' ? 'time' : 'manual';
        this.render();
    },

    // Sections with no event time sort to the end of a chronological view: they
    // have no place on a timeline, and sorting them to the epoch would bury the
    // real first event under every note in the notebook.
    _orderedSections() {
        const sections = (this.summary?.sections || []).slice();
        if (this.order !== 'time') return sections;

        const epoch = v => (v && window.TZ ? TZ.toEpoch(v) : NaN);
        return sections.sort((a, b) => {
            const at = epoch(a.event_time);
            const bt = epoch(b.event_time);
            const aMissing = !Number.isFinite(at);
            const bMissing = !Number.isFinite(bt);
            if (aMissing && bMissing) return a.order_index - b.order_index;
            if (aMissing) return 1;
            if (bMissing) return -1;
            if (at !== bt) return at - bt;
            return a.order_index - b.order_index;
        });
    },

    // ---- rendering --------------------------------------------------------

    render() {
        const empty = document.getElementById('nbrEmpty');
        const picker = document.getElementById('nbrPicker');
        const active = document.getElementById('nbrActive');
        if (!empty || !picker || !active) return;

        picker.hidden = !this._pickerOpen;
        empty.hidden = this._pickerOpen || !!this.activeId;
        active.hidden = this._pickerOpen || !this.activeId;

        // Before the early return: deactivating a notebook has to clear the
        // detail panel's pinned marks too, not just when the pane is showing.
        if (window.LogDetail && typeof LogDetail.syncPinState === 'function') LogDetail.syncPinState();

        if (active.hidden) return;

        const nameEl = document.getElementById('nbrName');
        if (nameEl) nameEl.textContent = this.summary?.name || 'Loading...';

        this._renderCounts();
        this._renderCapture();
        this._renderOrderToggle();
        this._renderList();
    },

    _renderCounts() {
        const el = document.getElementById('nbrCounts');
        if (!el) return;
        const counts = this.summary?.counts || {};
        const parts = Object.entries(this._countLabels)
            .filter(([type]) => counts[type])
            .map(([type, [one, many]]) => `${counts[type]} ${counts[type] === 1 ? one : many}`);
        el.textContent = parts.length ? parts.join(' - ') : 'Nothing captured yet';
    },

    // A viewer on the notebook's fractal can read it but not write to it. Hide
    // the capture controls rather than letting every click return a 403.
    _renderCapture() {
        const capture = document.getElementById('nbrCapture');
        const canEdit = this._state === 'ready' && this.summary?.can_edit !== false;
        if (capture) capture.hidden = !canEdit;
        if (!canEdit) this.hideNoteEditor();
    },

    _renderOrderToggle() {
        const label = document.getElementById('nbrOrderLabel');
        const btn = document.getElementById('nbrOrderToggle');
        const chronological = this.order === 'time';
        if (label) label.textContent = chronological ? 'Event order' : 'Notebook order';
        if (btn) {
            btn.classList.toggle('active', chronological);
            btn.title = chronological ? 'Sort in notebook order' : 'Sort chronologically';
        }
    },

    _renderList() {
        const list = document.getElementById('nbrList');
        if (!list) return;

        if (this._state === 'loading' && !this.summary?.sections?.length) {
            list.innerHTML = '<div class="fr-empty">Loading...</div>';
            return;
        }
        if (this._state === 'error') {
            list.innerHTML = '<div class="fr-empty">Could not load this notebook</div>';
            return;
        }

        const sections = this._orderedSections();
        if (!sections.length) {
            list.innerHTML = '<div class="fr-empty">Add the query you are running, or pin an event from its detail panel.</div>';
            return;
        }

        list.innerHTML = sections.map(sec => this._rowHtml(sec)).join('');
    },

    _rowHtml(sec) {
        const color = this._dotColors[sec.section_type] || 'var(--text-muted)';
        const title = this._rowTitle(sec);
        const when = sec.event_time ? TZ.format(sec.event_time, 'full') : '';
        const pivot = this._pivotFor(sec);

        return `
            <div class="nbr-row${pivot ? ' pivotable' : ''}" data-section-id="${Utils.escapeHtml(sec.id)}">
                <span class="nbr-dot" style="background:${color}" title="${Utils.escapeHtml(this._typeLabels[sec.section_type] || sec.section_type)}"></span>
                <span class="nbr-row-main">
                    <span class="nbr-row-title">${Utils.escapeHtml(title)}</span>
                    ${when ? `<span class="nbr-row-time" title="${Utils.escapeHtml(TZ.title(sec.event_time))}">${Utils.escapeHtml(when)}</span>` : ''}
                </span>
                ${pivot ? '<span class="nbr-row-go" title="Open in search">&rsaquo;</span>' : ''}
            </div>
        `;
    },

    _rowTitle(sec) {
        if (sec.title && sec.title.trim()) return sec.title.trim();
        if (sec.section_type === 'comment_context') {
            const data = this._parseJson(sec.content);
            const text = (data.comment_text || '').trim();
            if (text) return text;
            return data.log_id ? `Event ${String(data.log_id).slice(0, 12)}` : 'Evidence';
        }
        const firstLine = (sec.content || '').split('\n').find(l => l.trim());
        if (firstLine) return firstLine.trim();
        return this._typeLabels[sec.section_type] || 'Section';
    },

    // What clicking a row reproduces in the search view, or null when the
    // section describes no query (a note, an AI summary).
    _pivotFor(sec) {
        if (sec.section_type === 'query') {
            return { query: sec.content || '', center: sec.event_time || null };
        }
        if (sec.section_type === 'comment_context') {
            const data = this._parseJson(sec.content);
            if (!data.log_id) return null;
            return {
                query: `log_id="${data.log_id}"`,
                center: sec.event_time || data.log_timestamp || data.commented_at || null,
            };
        }
        return null;
    },

    _onListClick(e) {
        const row = e.target.closest('[data-section-id]');
        if (!row) return;
        const sec = (this.summary?.sections || []).find(s => s.id === row.dataset.sectionId);
        if (!sec) return;
        const pivot = this._pivotFor(sec);
        if (!pivot || !pivot.query) return;

        // One implementation of the pivot, shared with the notebook page: it
        // also re-centres the time range, without which an older investigation
        // silently returns nothing.
        if (window.Notebooks && typeof Notebooks._openInSearch === 'function') {
            Notebooks._openInSearch(pivot.query, pivot.center);
        }
    },

    _parseJson(raw) {
        try {
            const parsed = JSON.parse(raw || '{}');
            return (parsed && typeof parsed === 'object') ? parsed : {};
        } catch (e) {
            return {};
        }
    },

    // TZ.toEpoch, not new Date(): a result-row timestamp arrives as a bare
    // "YYYY-MM-DD HH:MM:SS" in UTC, which new Date() reads as browser-local and
    // shifts by the viewer's offset before it is stored.
    _isoOrNull(value) {
        if (!value) return null;
        const ms = window.TZ ? TZ.toEpoch(value) : Date.parse(value);
        return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
    },
};

window.NotebookRail = NotebookRail;
