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
    order: 'time',          // 'time' | 'manual'; an investigation reads back as a chronology
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

        this._wire('nbrSwitchBtn', 'click', () => this.openPicker());
        this._wire('nbrPickerClose', 'click', () => this.closePicker());
        this._wire('nbrOpenBtn', 'click', () => this.openNotebook());
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
        if (list) {
            list.addEventListener('click', (e) => this._onListClick(e));
            this._wireDragReorder(list);
        }

        const pickerList = document.getElementById('nbrPickerList');
        if (pickerList) pickerList.addEventListener('click', (e) => this._onPickerClick(e));

        if (window.FractalContext) {
            FractalContext.subscribe('NotebookRail', () => this.onFractalChange());
        }

        this._restoreActive().then(() => this.render()).then(() => this._syncCaptureState());
    },

    _wire(id, event, fn) {
        const el = document.getElementById(id);
        if (el) el.addEventListener(event, fn);
    },

    // ---- active notebook --------------------------------------------------

    // Stored per user and per scope on the server, so it
    // survives a browser change and so anything writing on the user's behalf can
    // see where captures belong. _activeReady is what every read waits on.
    _restoreActive() {
        this.activeId = null;
        this.summary = null;
        this._state = 'idle';

        this._captureEnabled = false;

        const scopeToken = window.FractalContext ? FractalContext.scopeToken() : null;
        this._activeReady = (async () => {
            if (window.FractalContext && !FractalContext.hasScope()) return;
            try {
                const res = await fetch('/api/v1/notebooks/active', { credentials: 'include' });
                if (!res.ok) return;
                const data = await res.json();
                if (scopeToken !== null && FractalContext.isScopeStale(scopeToken)) return;
                if (!data.success || !data.data) return;
                this.activeId = data.data.notebook_id || null;
                this.setCaptureEnabled(data.data.has_notebooks || data.data.has_comments);
            } catch (e) {
                // The rail still works this session without a remembered notebook.
            }
        })();
        return this._activeReady;
    },

    // Which events are already in the active notebook is what the results
    // table's stars are drawn from, so it has to be read whether or not the rail
    // is open. Gating this on the panel being visible is why a starred row came
    // back empty after a refresh.
    _syncCaptureState() {
        if (window.FractalContext && !FractalContext.hasScope()) return;
        if (!this.activeId) return;
        return this.load();
    },

    // Whether this scope has ever used notebooks or comments. Until it has, the
    // results table renders no star gutter at all, so Bifract is unchanged for
    // anyone who does not use the feature.
    captureEnabled() {
        return !!this._captureEnabled;
    },

    // Turning capture on mid-session (the first comment, the first notebook) has
    // to bring the gutter with it, which means re-rendering the table that was
    // drawn without one.
    setCaptureEnabled(enabled) {
        if (!!enabled === !!this._captureEnabled) return;
        this._captureEnabled = !!enabled;
        if (window.QueryExecutor && typeof QueryExecutor.rerenderCurrentPage === 'function') {
            QueryExecutor.rerenderCurrentPage();
        }
    },

    _persistActive() {
        if (window.FractalContext && !FractalContext.hasScope()) return;
        fetch('/api/v1/notebooks/active', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ notebook_id: this.activeId || '' }),
        }).catch(() => {});
    },

    onFractalChange() {
        this.closePicker();
        this.hideNoteEditor();
        this._abort();
        this.render();
        this._restoreActive().then(() => {
            this.render();
            this._syncCaptureState();
        });
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
        if (this._activeReady) await this._activeReady;
        if (!this.activeId) {
            this._state = 'idle';
            this.render();
            this.loadPickerList(document.getElementById('nbrPickerSearch')?.value || '');
            return;
        }

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
            if (window.StarGutter) StarGutter.syncRendered();
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
    // A locked notebook accepts no captures, and the server clears it as
    // everyone's target when it is locked. This page may still be holding the
    // id, so a refusal drops it and the next star opens a fresh notebook rather
    // than failing again.
    _dropLockedTarget(message) {
        this.activeId = null;
        this._pinnedLogIds = new Set();
        this.summary = null;
        this._state = 'idle';
        this._persistActive();
        this.render();
        if (window.StarGutter) StarGutter.syncRendered();
        if (window.Toast) Toast.show(message || 'That notebook is locked. Your next capture will start a new one.', 'warning');
    },

    // onNotebookLocked is called when the notebook was locked from this page.
    onNotebookLocked(notebookId) {
        if (!notebookId || this.activeId !== notebookId) return;
        this._dropLockedTarget('Locked. Your next capture will start a new notebook.');
    },

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
    // Resolve where a capture goes, creating a scratch notebook when the user
    // has not chosen one. Capture must not stop to ask for a name at the moment
    // someone found something.
    async _ensureActive() {
        if (this._activeReady) await this._activeReady;
        if (this.activeId) return { id: this.activeId, created: false };

        try {
            const res = await fetch('/api/v1/notebooks/active', {
                method: 'POST',
                credentials: 'include',
            });
            const data = await res.json();
            if (!res.ok || !data.success) throw new Error(data.error || 'Failed to open a notebook');

            this.activeId = data.data.notebook_id;
            this._pinnedLogIds = new Set();
            this.summary = { id: this.activeId, name: data.data.name, sections: [], counts: {} };
            this._state = 'loading';
            this.setCaptureEnabled(true);
            this.render();
            if (data.data.created) {
                this.reveal();
                if (window.Toast) Toast.show(`Capturing into "${data.data.name}" - rename it to keep it`, 'info');
            }
            return { id: this.activeId, created: !!data.data.created };
        } catch (err) {
            this.reveal();
            if (window.Toast) Toast.error('Notebook', err.message);
            return null;
        }
    },

    async _addSection(body, successMessage) {
        const target = await this._ensureActive();
        if (!target) return false;

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
            if (res.status === 409) {
                this._dropLockedTarget(data.error);
                return false;
            }
            if (!res.ok || !data.success) throw new Error(data.error || 'Failed to add to notebook');

            if (window.Toast && !target.created) Toast.success('Added', successMessage);
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
        }, 'Query step added to the notebook');
    },

    // Star the event a caller is looking at. This writes a comment with no text
    // yet, so the row is visible to the comments tab, to the row accent and to
    // comments(), and the notebook section is created alongside it server-side.
    async pinLog(logData) {
        if (!logData || !logData.log_id) {
            if (window.Toast) Toast.show('This row has no log id to pin', 'warning');
            return;
        }
        const target = await this._ensureActive();
        if (!target) return;
        if (this.hasPinned(logData.log_id)) {
            if (window.Toast) Toast.show('Already in this notebook', 'info');
            return;
        }

        const body = {
            log_id: String(logData.log_id),
            text: '',
            notebook_id: this.activeId,
            title: this._pinTitle(logData),
            query: document.getElementById('queryInput')?.value?.trim() || '',
        };
        const eventTime = this._isoOrNull(logData.timestamp);
        if (eventTime) body.log_timestamp = eventTime;

        try {
            const res = await fetch('/api/v1/comments', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(body),
            });
            if (res.status === 404) {
                this.setActive(null);
                this.render();
                throw new Error('That notebook no longer exists');
            }
            const data = await res.json();
            if (res.status === 409) {
                this._dropLockedTarget(data.error);
                return false;
            }
            if (!res.ok || !data.success) throw new Error(data.error || 'Failed to add to notebook');

            if (window.Toast && !target.created) Toast.success('Added', 'Event added to the notebook');
            this._pinnedLogIds.add(body.log_id);
            if (window.StarGutter) StarGutter.syncRendered();
            this.load();
            if (window.Comments) Comments.markCommented(body.log_id);
        } catch (err) {
            if (window.Toast) Toast.error('Notebook', err.message);
        }
    },

    // Remove an event from the active notebook. The comment goes with it only
    // when nobody wrote anything in it; an annotation someone typed survives as
    // a comment on a log the notebook no longer holds.
    async unstarLog(logID) {
        if (!logID || !this.activeId) return;

        this._pinnedLogIds.delete(String(logID));
        if (window.StarGutter) StarGutter.syncRendered();

        try {
            const res = await fetch(`/api/v1/notebooks/${encodeURIComponent(this.activeId)}/evidence/${encodeURIComponent(logID)}`, {
                method: 'DELETE',
                credentials: 'include',
            });
            const data = await res.json();
            if (res.status === 409) {
                this._dropLockedTarget(data.error);
                return;
            }
            if (!res.ok || !data.success) throw new Error(data.error || 'Failed to remove from notebook');
            this.load();
            // Removing a bare star deletes its comment, so the row's annotated
            // mark has to be re-read rather than left showing an annotation that
            // no longer exists.
            if (window.Comments) Comments.fetchCommentedLogIds();
        } catch (err) {
            this._pinnedLogIds.add(String(logID));
            if (window.StarGutter) StarGutter.syncRendered();
            if (window.Toast) Toast.error('Notebook', err.message);
        }
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

        return sections.sort((a, b) => {
            const at = this._eventMs(a);
            const bt = this._eventMs(b);
            const aMissing = !Number.isFinite(at);
            const bMissing = !Number.isFinite(bt);
            if (aMissing && bMissing) return a.order_index - b.order_index;
            if (aMissing) return 1;
            if (bMissing) return -1;
            if (at !== bt) return at - bt;
            return a.order_index - b.order_index;
        });
    },

    _eventMs(sec) {
        const v = sec && sec.event_time;
        return v && window.TZ ? TZ.toEpoch(v) : NaN;
    },

    // ---- rendering --------------------------------------------------------

    render() {
        const picker = document.getElementById('nbrPicker');
        const active = document.getElementById('nbrActive');
        if (!picker || !active) return;

        // With no notebook active the picker is the whole pane. Cancelling and
        // the orienting hint only make sense on either side of that.
        const choosing = this._pickerOpen || !this.activeId;
        picker.hidden = !choosing;
        active.hidden = choosing;

        const close = document.getElementById('nbrPickerClose');
        if (close) close.hidden = !this.activeId;
        const hint = document.getElementById('nbrPickerHint');
        if (hint) hint.hidden = !!this.activeId;

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

        list.innerHTML = this.order === 'time'
            ? this._chronologicalHtml(sections)
            : sections.map(sec => this._rowHtml(sec)).join('');
    },

    // Elapsed time between adjacent sections, which is often the finding: three
    // quiet days then a two minute burst. Only in the chronological view, since
    // notebook order is manual and its neighbours are not sequential in time.
    _chronologicalHtml(sections) {
        const parts = [];
        let prevMs = null;
        let datedAbove = false;

        for (const sec of sections) {
            const ms = this._eventMs(sec);
            const dated = Number.isFinite(ms);
            if (parts.length) {
                if (dated && prevMs !== null) {
                    parts.push(this._gapHtml(ms - prevMs));
                } else if (!dated && datedAbove) {
                    // Undated sections sort to the tail; mark the boundary once
                    // rather than implying they follow the last event.
                    parts.push('<div class="nbr-gap undated"><span class="nbr-gap-line"></span><span class="nbr-gap-label">no event time</span></div>');
                    datedAbove = false;
                }
            }
            parts.push(this._rowHtml(sec));
            if (dated) { prevMs = ms; datedAbove = true; }
        }
        return parts.join('');
    },

    _gapHtml(deltaMs) {
        const label = this._humanGap(deltaMs);
        const wide = deltaMs >= 86400000;
        return `<div class="nbr-gap${wide ? ' wide' : ''}"><span class="nbr-gap-line"></span>${label ? `<span class="nbr-gap-label">${label}</span>` : ''}</div>`;
    },

    // Coarse by design: the reader wants "a day passed", not the seconds.
    // Under a second reads as simultaneous and gets the connector with no label.
    _humanGap(ms) {
        if (!Number.isFinite(ms) || ms < 1000) return '';
        const secs = Math.floor(ms / 1000);
        if (secs < 60) return `+${secs}s`;
        const mins = Math.floor(secs / 60);
        if (mins < 60) return `+${mins}m`;
        const hours = Math.floor(mins / 60);
        if (hours < 24) {
            const rem = mins % 60;
            return rem ? `+${hours}h ${rem}m` : `+${hours}h`;
        }
        const days = Math.floor(hours / 24);
        const rem = hours % 24;
        return rem ? `+${days}d ${rem}h` : `+${days}d`;
    },

    _rowHtml(sec) {
        const color = this._dotColors[sec.section_type] || 'var(--text-muted)';
        const title = this._rowTitle(sec);
        const when = sec.event_time ? TZ.format(sec.event_time, 'full') : '';
        const pivot = this._pivotFor(sec);
        const canEdit = this.summary?.can_edit !== false;
        // Dragging only makes sense against the order it would rewrite. Event
        // order is computed from event_time, so there is nothing to drag into.
        const draggable = canEdit && this.order === 'manual';
        const tags = this._rowTags(sec);
        const open = this._editing === sec.id;

        return `
            <div class="nbr-row${pivot ? ' pivotable' : ''}${open ? ' editing' : ''}" data-section-id="${Utils.escapeHtml(sec.id)}"${draggable ? ' draggable="true"' : ''}>
                <span class="nbr-dot" style="background:${color}" title="${Utils.escapeHtml(this._typeLabels[sec.section_type] || sec.section_type)}"></span>
                <span class="nbr-row-main">
                    <span class="nbr-row-title">${Utils.escapeHtml(title)}</span>
                    ${tags.length ? `<span class="nbr-row-tags">${tags.map(t => `<span class="nbr-tag">${Utils.escapeHtml(t)}</span>`).join('')}</span>` : ''}
                    ${when ? `<span class="nbr-row-time" title="${Utils.escapeHtml(TZ.title(sec.event_time))}">${Utils.escapeHtml(when)}</span>` : ''}
                </span>
                ${canEdit ? `<button type="button" class="nbr-row-act" data-act="edit" title="Annotate">${this._pencilSvg()}</button>` : ''}
                ${canEdit ? `<button type="button" class="nbr-row-act nbr-row-danger" data-act="delete" title="Remove from the notebook">${this._trashSvg()}</button>` : ''}
                ${pivot ? '<span class="nbr-row-go" data-act="pivot" title="Open in search">&rsaquo;</span>' : ''}
            </div>
            ${open ? this._editorHtml(sec) : ''}
        `;
    },

    _pencilSvg() {
        return '<svg width="11" height="11" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M11.5 2.5l2 2L6 12l-2.5.5.5-2.5z"/></svg>';
    },

    _trashSvg() {
        return '<svg width="11" height="11" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M3 4h10M6.5 4V2.5h3V4M4.5 4l.5 9h6l.5-9"/></svg>';
    },

    // Evidence carries the comment's tags, which are what comments() and the
    // tag-to-notebook filing read. Other sections carry their own.
    _rowTags(sec) {
        if (sec.section_type === 'comment_context') {
            const data = this._parseJson(sec.content);
            return Array.isArray(data.tags) ? data.tags : [];
        }
        return Array.isArray(sec.tags) ? sec.tags : [];
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
        const editor = e.target.closest('.nbr-editor');
        if (editor) {
            this._onEditorClick(e, editor);
            return;
        }

        const row = e.target.closest('.nbr-row[data-section-id]');
        if (!row) return;
        const sec = (this.summary?.sections || []).find(s => s.id === row.dataset.sectionId);
        if (!sec) return;

        const act = e.target.closest('[data-act]');
        const which = act ? act.dataset.act : '';
        if (which === 'edit') { this.toggleEditor(sec.id); return; }
        if (which === 'delete') { this.deleteSection(sec.id); return; }

        // The row runs what it holds: a query step re-runs, an event opens in
        // search. A note has nothing to run, so it opens for editing instead of
        // swallowing the click.
        const pivot = this._pivotFor(sec);
        if (!pivot || !pivot.query) {
            if (this.summary?.can_edit !== false) this.toggleEditor(sec.id);
            return;
        }

        // One implementation of the pivot, shared with the notebook page: it
        // also re-centres the time range, without which an older investigation
        // silently returns nothing.
        if (window.Notebooks && typeof Notebooks._openInSearch === 'function') {
            Notebooks._openInSearch(pivot.query, pivot.center);
        }
    },

    // ---- reordering -------------------------------------------------------

    // Drag to reorder, and only under Notebook order: Event order is computed
    // from event_time, so a drop there would be discarded on the next render.
    _wireDragReorder(list) {
        list.addEventListener('dragstart', (e) => {
            const row = e.target.closest('.nbr-row[data-section-id]');
            if (!row || this.order !== 'manual') { e.preventDefault(); return; }
            this._dragId = row.dataset.sectionId;
            row.classList.add('dragging');
            e.dataTransfer.effectAllowed = 'move';
            // Firefox starts no drag without payload.
            e.dataTransfer.setData('text/plain', this._dragId);
        });

        list.addEventListener('dragover', (e) => {
            if (!this._dragId) return;
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
            const over = e.target.closest('.nbr-row[data-section-id]');
            list.querySelectorAll('.nbr-row.drop-before, .nbr-row.drop-after')
                .forEach(el => el.classList.remove('drop-before', 'drop-after'));
            if (!over || over.dataset.sectionId === this._dragId) return;
            const box = over.getBoundingClientRect();
            over.classList.add(e.clientY < box.top + box.height / 2 ? 'drop-before' : 'drop-after');
        });

        list.addEventListener('drop', (e) => {
            if (!this._dragId) return;
            e.preventDefault();
            const over = e.target.closest('.nbr-row[data-section-id]');
            const marker = list.querySelector('.nbr-row.drop-before, .nbr-row.drop-after');
            const before = marker ? marker.classList.contains('drop-before') : true;
            this._clearDrag(list);
            if (over) {
                this.reorderTo(this._dragId, over.dataset.sectionId, before);
            }
            this._dragId = null;
        });

        list.addEventListener('dragend', () => { this._clearDrag(list); this._dragId = null; });
    },

    _clearDrag(list) {
        list.querySelectorAll('.nbr-row.dragging, .nbr-row.drop-before, .nbr-row.drop-after')
            .forEach(el => el.classList.remove('dragging', 'drop-before', 'drop-after'));
    },

    async reorderTo(movedId, targetId, before) {
        if (!this.activeId || movedId === targetId) return;

        const order = (this.summary?.sections || [])
            .slice()
            .sort((a, b) => a.order_index - b.order_index)
            .map(s => s.id);

        const from = order.indexOf(movedId);
        if (from === -1) return;
        order.splice(from, 1);
        let to = order.indexOf(targetId);
        if (to === -1) return;
        if (!before) to += 1;
        order.splice(to, 0, movedId);

        // Repaint from the new order straight away: waiting for the round trip
        // makes the row snap back under the pointer.
        const byId = new Map((this.summary.sections || []).map(s => [s.id, s]));
        order.forEach((id, i) => { const sec = byId.get(id); if (sec) sec.order_index = i; });
        this.summary.sections = order.map(id => byId.get(id)).filter(Boolean);
        this.render();

        try {
            const res = await fetch(`/api/v1/notebooks/${encodeURIComponent(this.activeId)}/sections/reorder`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ section_order: order }),
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok || !data.success) throw new Error(data.error || 'Failed to reorder');
        } catch (err) {
            if (window.Toast) Toast.error('Notebook', err.message);
            this.load();
        }
    },

    // ---- annotating -------------------------------------------------------

    // The editor is the reason the rail exists as more than a list: a capture is
    // worth little without the sentence saying why it was captured, and a tag
    // applied days later during cleanup is a tag nobody applies.
    toggleEditor(sectionId) {
        this._editing = this._editing === sectionId ? null : sectionId;
        this.render();
        if (this._editing) {
            const field = document.querySelector('.nbr-editor .nbr-editor-text');
            if (field) { field.focus(); field.setSelectionRange(field.value.length, field.value.length); }
        }
    },

    _editorHtml(sec) {
        const evidence = sec.section_type === 'comment_context';
        const data = evidence ? this._parseJson(sec.content) : {};
        const body = evidence ? (data.comment_text || '') : (sec.content || '');
        const bodyLabel = evidence ? 'Comment' : (sec.section_type === 'markdown' ? 'Note' : 'Query');
        const editableBody = evidence || sec.section_type === 'markdown';
        const me = (window.Auth && Auth.currentUser && Auth.currentUser.username) || '';
        // A comment is one person's words. Tags are how a team organises shared
        // evidence, so those stay editable either way.
        const mine = !evidence || !data.author || data.author === me;

        return `
            <div class="nbr-editor" data-section-id="${Utils.escapeHtml(sec.id)}">
                <label class="nbr-editor-label">Title</label>
                <input type="text" class="nbr-editor-title" value="${Utils.escapeAttr(sec.title || '')}" placeholder="${Utils.escapeAttr(this._rowTitle(sec))}" maxlength="255" />

                ${editableBody ? `
                    <label class="nbr-editor-label">${bodyLabel}${mine ? '' : ` (only ${Utils.escapeHtml(data.author_display_name || data.author)} can edit this)`}</label>
                    <textarea class="nbr-editor-text" rows="3" placeholder="What did you find?"${mine ? '' : ' readonly'}>${Utils.escapeHtml(body)}</textarea>
                ` : ''}

                <label class="nbr-editor-label">Tags</label>
                <input type="text" class="nbr-editor-tags" value="${Utils.escapeAttr(this._rowTags(sec).join(', '))}" placeholder="lateral, persistence" />

                <div class="nbr-editor-actions">
                    <button type="button" class="nbr-btn" data-act="cancel">Cancel</button>
                    <button type="button" class="nbr-btn nbr-btn-primary" data-act="save">Save</button>
                </div>
            </div>
        `;
    },

    _onEditorClick(e, editor) {
        const act = e.target.closest('[data-act]');
        if (!act) return;
        if (act.dataset.act === 'cancel') { this._editing = null; this.render(); return; }
        if (act.dataset.act === 'save') this.saveEditor(editor.dataset.sectionId, editor);
    },

    _parseTagInput(value) {
        return String(value || '')
            .split(',')
            .map(t => t.trim())
            .filter(Boolean)
            .filter((t, i, all) => all.indexOf(t) === i);
    },

    async saveEditor(sectionId, editor) {
        const sec = (this.summary?.sections || []).find(s => s.id === sectionId);
        if (!sec) return;

        const evidence = sec.section_type === 'comment_context';
        const data = evidence ? this._parseJson(sec.content) : {};
        const title = editor.querySelector('.nbr-editor-title').value.trim();
        const textField = editor.querySelector('.nbr-editor-text');
        const body = textField ? textField.value : null;
        const tags = this._parseTagInput(editor.querySelector('.nbr-editor-tags').value);

        const writes = [];

        // Title lives on the section for every type: it is the outline line,
        // authored by whoever is reading, not part of what anyone wrote.
        if ((sec.title || '') !== title) {
            writes.push(this._putSection(sectionId, { title }));
        }

        if (evidence) {
            const commentID = sec.comment_id;
            if (textField && !textField.readOnly && (data.comment_text || '') !== body) {
                writes.push(this._putJson(`/api/v1/comments/${encodeURIComponent(commentID)}`, { text: body, tags }));
            } else if (!this._sameTags(this._rowTags(sec), tags)) {
                writes.push(this._putJson(`/api/v1/comments/${encodeURIComponent(commentID)}/tags`, { tags }));
            }
        } else {
            const patch = {};
            if (textField && (sec.content || '') !== body) patch.content = body;
            if (!this._sameTags(this._rowTags(sec), tags)) patch.tags = tags;
            if (Object.keys(patch).length) writes.push(this._putSection(sectionId, patch));
        }

        if (!writes.length) { this._editing = null; this.render(); return; }

        try {
            await Promise.all(writes);
            this._editing = null;
            await this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Notebook', err.message);
        }
    },

    _sameTags(a, b) {
        return a.length === b.length && a.every((t, i) => t === b[i]);
    },

    _putSection(sectionId, patch) {
        return this._putJson(`/api/v1/notebooks/${encodeURIComponent(this.activeId)}/sections/${encodeURIComponent(sectionId)}`, patch);
    },

    async _putJson(url, body) {
        const res = await fetch(url, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify(body),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data.success) throw new Error(data.error || 'Save failed');
        return data;
    },

    // ---- removing ---------------------------------------------------------

    // Evidence is removed by log rather than by section, so the star on its row
    // clears with it and an empty star comment does not survive as a stray.
    async deleteSection(sectionId) {
        const sec = (this.summary?.sections || []).find(s => s.id === sectionId);
        if (!sec || !this.activeId) return;

        if (sec.section_type === 'comment_context') {
            const logID = this._parseJson(sec.content).log_id;
            if (logID) { this.unstarLog(logID); return; }
        }

        try {
            const res = await fetch(`/api/v1/notebooks/${encodeURIComponent(this.activeId)}/sections/${encodeURIComponent(sectionId)}`, {
                method: 'DELETE',
                credentials: 'include',
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok || !data.success) throw new Error(data.error || 'Failed to remove');
            if (this._editing === sectionId) this._editing = null;
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Notebook', err.message);
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
