// Unified query command palette: merges Recent (history) and Saved queries into
// one keyboard-driven surface. Opens via the Queries button or Ctrl/Cmd-K.
//
// History is sourced from the backend (/api/v1/query-history) so it follows the
// user across devices, with a localStorage cache for instant render. Saved
// queries come from /api/v1/saved-queries and support descriptions, personal/
// shared visibility, favorites, and usage stats.

const PLAY_ICON  = '<svg width="11" height="11" viewBox="0 0 16 16" fill="none"><path d="M4 3l9 5-9 5V3z" fill="currentColor"/></svg>';
const SAVE_ICON  = '<svg width="11" height="11" viewBox="0 0 16 16" fill="none"><path d="M4 1h8a1 1 0 0 1 1 1v12l-5-3-5 3V2a1 1 0 0 1 1-1z" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/></svg>';
const EDIT_ICON  = '<svg width="11" height="11" viewBox="0 0 16 16" fill="none"><path d="M11.5 1.5l3 3L5 14H2v-3L11.5 1.5z" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"/></svg>';
const TRASH_ICON = '<svg width="11" height="11" viewBox="0 0 16 16" fill="none"><path d="M2.5 4h11M6 4V2.5h4V4M5 4l.5 9h5L11 4" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>';
const X_ICON     = '<svg width="11" height="11" viewBox="0 0 16 16" fill="none"><path d="M3 3l10 10M13 3L3 13" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>';
const STAR_OUTLINE = '<svg width="13" height="13" viewBox="0 0 16 16" fill="none"><path d="M8 1.5l1.9 4 4.4.5-3.3 3 .9 4.3L8 11.1 4.1 13.3l.9-4.3-3.3-3 4.4-.5L8 1.5z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/></svg>';
const STAR_FILLED  = '<svg width="13" height="13" viewBox="0 0 16 16" fill="currentColor"><path d="M8 1.5l1.9 4 4.4.5-3.3 3 .9 4.3L8 11.1 4.1 13.3l.9-4.3-3.3-3 4.4-.5L8 1.5z"/></svg>';

const QueryPalette = {
    isOpen: false,
    activeTab: 'history',   // 'history' | 'saved'
    searchTerm: '',
    activeIndex: 0,
    history: [],
    saved: [],
    filtered: [],
    debounceTimer: null,
    editingId: null,
    saveFormOpen: false,
    promoteText: null,      // query text being promoted from history into Saved
    _pendingHistory: null,  // run metadata captured by the executor

    init() {
        const btn = document.getElementById('queriesMenuBtn');
        if (btn) btn.addEventListener('click', (e) => { e.stopPropagation(); this.toggle(); });

        const search = document.getElementById('paletteSearch');
        if (search) {
            search.addEventListener('input', (e) => this.handleSearch(e.target.value));
            search.addEventListener('keydown', (e) => this.onSearchKeydown(e));
        }

        document.querySelectorAll('.palette-tab').forEach(tab => {
            tab.addEventListener('click', (e) => { e.stopPropagation(); this.switchTab(tab.dataset.tab); });
        });

        const saveBtn = document.getElementById('paletteSaveBtn');
        if (saveBtn) saveBtn.addEventListener('click', (e) => { e.stopPropagation(); this.toggleSaveForm(); });

        const clearBtn = document.getElementById('paletteClearBtn');
        if (clearBtn) clearBtn.addEventListener('click', (e) => { e.stopPropagation(); this.clearHistory(); });

        const palette = document.getElementById('queryPalette');
        if (palette) palette.addEventListener('click', (e) => e.stopPropagation());
        const backdrop = document.getElementById('paletteBackdrop');
        if (backdrop) backdrop.addEventListener('click', () => this.close());

        // Ctrl/Cmd-K opens from anywhere; preventDefault keeps the browser from
        // stealing it for the address bar. The Queries button is the fallback if
        // an extension grabs the shortcut.
        document.addEventListener('keydown', (e) => {
            if ((e.metaKey || e.ctrlKey) && (e.key === 'k' || e.key === 'K')) {
                e.preventDefault();
                this.toggle();
                return;
            }
            if (e.key === 'Escape' && this.isOpen) this.close();
        });

        document.addEventListener('click', (e) => {
            if (this.isOpen && palette && !palette.contains(e.target) && e.target.id !== 'queriesMenuBtn') {
                this.close();
            }
        });

        this.migrateLegacyRecent();
    },

    onFractalChange() {
        this.history = [];
        this.saved = [];
        this.editingId = null;
        this.saveFormOpen = false;
        if (this.isOpen) this.reload();
    },

    toggle() { this.isOpen ? this.close() : this.open(); },

    open() {
        const palette = document.getElementById('queryPalette');
        const backdrop = document.getElementById('paletteBackdrop');
        if (!palette) return;
        if (backdrop) backdrop.style.display = 'block';
        palette.style.display = 'flex';
        this.isOpen = true;
        this.activeIndex = 0;
        this.saveFormOpen = false;
        this.editingId = null;
        this.promoteText = null;
        // Show cached history immediately, then refresh from the server.
        if (this.activeTab === 'history') this.history = this.readCache();
        this.render();
        this.reload();
        const search = document.getElementById('paletteSearch');
        if (search) { search.value = ''; this.searchTerm = ''; setTimeout(() => search.focus(), 30); }
        const btn = document.getElementById('queriesMenuBtn');
        if (btn) btn.classList.add('active');
    },

    close() {
        const palette = document.getElementById('queryPalette');
        const backdrop = document.getElementById('paletteBackdrop');
        if (palette) palette.style.display = 'none';
        if (backdrop) backdrop.style.display = 'none';
        this.isOpen = false;
        this.saveFormOpen = false;
        this.editingId = null;
        const btn = document.getElementById('queriesMenuBtn');
        if (btn) btn.classList.remove('active');
    },

    switchTab(tab) {
        if (tab === this.activeTab) return;
        this.activeTab = tab;
        this.activeIndex = 0;
        this.saveFormOpen = false;
        this.editingId = null;
        this.promoteText = null;
        document.querySelectorAll('.palette-tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tab));
        this.render();
        this.reload();
        const search = document.getElementById('paletteSearch');
        if (search) search.focus();
    },

    reload() {
        if (this.activeTab === 'history') this.loadHistory(this.searchTerm);
        else this.loadSaved(this.searchTerm);
    },

    async loadHistory(search) {
        let url = '/api/v1/query-history';
        if (search) url += '?search=' + encodeURIComponent(search);
        try {
            const resp = await fetch(url, { credentials: 'include' });
            const data = await resp.json();
            this.history = (data.success && data.data && data.data.history) ? data.data.history : [];
            this.writeCache(this.history);
        } catch (err) {
            console.error('[QueryPalette] history load failed:', err);
        }
        if (this.activeTab === 'history') this.render();
    },

    async loadSaved(search) {
        let url = '/api/v1/saved-queries';
        if (search) url += '?search=' + encodeURIComponent(search);
        try {
            const resp = await fetch(url, { credentials: 'include' });
            const data = await resp.json();
            this.saved = (data.success && data.data && data.data.saved_queries) ? data.data.saved_queries : [];
        } catch (err) {
            console.error('[QueryPalette] saved load failed:', err);
        }
        if (this.activeTab === 'saved') this.render();
    },

    handleSearch(value) {
        this.searchTerm = value;
        this.activeIndex = 0;
        // Instant client-side fuzzy filter, plus a debounced server search that
        // widens the candidate set (server matches query text too).
        this.render();
        clearTimeout(this.debounceTimer);
        this.debounceTimer = setTimeout(() => this.reload(), 250);
    },

    onSearchKeydown(e) {
        if (e.key === 'ArrowDown') { e.preventDefault(); this.move(1); }
        else if (e.key === 'ArrowUp') { e.preventDefault(); this.move(-1); }
        else if (e.key === 'Enter') {
            e.preventDefault();
            const item = this.filtered[this.activeIndex];
            if (item) this.activate(item, !(e.metaKey || e.ctrlKey)); // plain Enter runs, Cmd/Ctrl+Enter loads
        }
        else if (e.key === 'Tab') {
            e.preventDefault();
            this.switchTab(this.activeTab === 'history' ? 'saved' : 'history');
        }
        else if ((e.metaKey || e.ctrlKey) && (e.key === 's' || e.key === 'S')) {
            e.preventDefault();
            this.toggleSaveForm();
        }
    },

    move(delta) {
        const n = this.filtered.length;
        if (!n) return;
        this.activeIndex = (this.activeIndex + delta + n) % n;
        const list = document.getElementById('paletteList');
        if (!list) return;
        list.querySelectorAll('.palette-row').forEach((el, i) => {
            el.classList.toggle('is-active', i === this.activeIndex);
            if (i === this.activeIndex) el.scrollIntoView({ block: 'nearest' });
        });
    },

    // run=true executes immediately; run=false loads into the editor for review.
    activate(item, run) {
        if (!item) return;
        if (this.activeTab === 'history') this.applyTimeRange(item);
        if (run) {
            this.runQuery(item.query_text);
            if (this.activeTab === 'saved') this.markUsed(item.id);
        } else {
            this.loadQuery(item.query_text);
        }
    },

    clickRow(i) { const it = this.filtered[i]; if (it) { this.activeIndex = i; this.activate(it, false); } },
    runIdx(i)   { const it = this.filtered[i]; if (it) { this.activeIndex = i; this.activate(it, true); } },

    runQuery(text) {
        this.loadIntoInput(text);
        this.close();
        const btn = document.getElementById('executeBtn');
        if (btn) setTimeout(() => btn.click(), 0);
    },

    loadQuery(text) { this.loadIntoInput(text); this.close(); },

    loadIntoInput(text) {
        const qi = document.getElementById('queryInput');
        if (!qi) return;
        qi.value = text;
        setTimeout(() => {
            qi.dispatchEvent(new Event('input', { bubbles: true }));
            qi.focus();
        }, 0);
    },

    applyTimeRange(it) {
        if (!it || !it.time_range) return;
        const sel = document.getElementById('timeRange');
        if (!sel || ![...sel.options].some(o => o.value === it.time_range)) return;
        sel.value = it.time_range;
        sel.dispatchEvent(new Event('change', { bubbles: true }));
        if (it.time_range === 'custom') {
            const cs = document.getElementById('customStart');
            const ce = document.getElementById('customEnd');
            if (cs && it.custom_start) cs.value = this.fmtLocal(it.custom_start);
            if (ce && it.custom_end) ce.value = this.fmtLocal(it.custom_end);
        }
    },

    // ----- Rendering -----

    currentList() {
        const items = this.activeTab === 'history' ? this.history : this.saved;
        const term = this.searchTerm.trim();
        if (!term) return items;
        const scored = [];
        items.forEach(it => {
            const hay = this.activeTab === 'saved'
                ? `${it.name} ${it.query_text} ${(it.tags || []).join(' ')} ${it.description || ''}`
                : it.query_text;
            const s = this.fuzzyScore(term, hay);
            if (s >= 0) scored.push({ it, s });
        });
        scored.sort((a, b) => b.s - a.s);
        return scored.map(x => x.it);
    },

    render() {
        const list = document.getElementById('paletteList');
        if (!list) return;

        const clearBtn = document.getElementById('paletteClearBtn');
        if (clearBtn) clearBtn.style.display = this.activeTab === 'history' ? '' : 'none';

        this.filtered = this.currentList();
        if (this.activeIndex >= this.filtered.length) this.activeIndex = Math.max(0, this.filtered.length - 1);

        const formHtml = this.saveFormOpen ? this.renderSaveForm() : '';

        if (this.filtered.length === 0 && !this.saveFormOpen) {
            list.innerHTML = this.emptyState();
            return;
        }

        const rows = this.filtered.map((it, i) =>
            this.activeTab === 'history' ? this.renderHistoryRow(it, i) : this.renderSavedRow(it, i)
        ).join('');

        list.innerHTML = formHtml + rows;
    },

    renderHistoryRow(it, i) {
        const active = i === this.activeIndex ? ' is-active' : '';
        const hl = window.SyntaxHighlight ? SyntaxHighlight.highlight(it.query_text) : this.escapeHtml(it.query_text);
        const meta = [];
        if (it.run_count > 1) meta.push(`${it.run_count}×`);
        if (it.last_run_at) meta.push(this.relTime(it.last_run_at));
        if (it.result_count != null) meta.push(`${Number(it.result_count).toLocaleString()} results`);
        const tr = it.time_range ? `<span class="palette-badge">${this.escapeHtml(this.timeLabel(it.time_range))}</span>` : '';
        const err = it.status === 'error' ? '<span class="palette-badge err">error</span>' : '';
        return `
          <div class="palette-row${active}" data-idx="${i}">
            <div class="palette-row-main" onclick="QueryPalette.clickRow(${i})">
              <div class="palette-query">${hl}</div>
              <div class="palette-meta">${tr}${err}<span>${meta.join(' · ')}</span></div>
            </div>
            <div class="palette-row-actions">
              <button class="palette-icon-btn" title="Run" onclick="event.stopPropagation();QueryPalette.runIdx(${i})">${PLAY_ICON}</button>
              <button class="palette-icon-btn" title="Save as..." onclick="event.stopPropagation();QueryPalette.promote(${i})">${SAVE_ICON}</button>
              <button class="palette-icon-btn" title="Remove" onclick="event.stopPropagation();QueryPalette.deleteHistory('${this.escapeJs(it.id)}')">${X_ICON}</button>
            </div>
          </div>`;
    },

    renderSavedRow(it, i) {
        if (this.editingId === it.id) return this.renderEditForm(it);
        const active = i === this.activeIndex ? ' is-active' : '';
        const hl = window.SyntaxHighlight ? SyntaxHighlight.highlight(it.query_text) : this.escapeHtml(it.query_text);
        const vis = it.visibility === 'personal' ? '<span class="palette-badge">personal</span>' : '';
        const tags = (it.tags || []).map(t => `<span class="palette-tag">${this.escapeHtml(t)}</span>`).join('');
        const desc = it.description ? `<div class="palette-desc">${this.escapeHtml(it.description)}</div>` : '';
        const meta = [];
        if (it.use_count) meta.push(`used ${it.use_count}×`);
        if (it.last_used_at) meta.push(this.relTime(it.last_used_at));
        return `
          <div class="palette-row${active}" data-idx="${i}">
            <button class="palette-fav${it.favorited ? ' on' : ''}" title="Favorite" onclick="event.stopPropagation();QueryPalette.toggleFavorite('${this.escapeJs(it.id)}')">${it.favorited ? STAR_FILLED : STAR_OUTLINE}</button>
            <div class="palette-row-main" onclick="QueryPalette.clickRow(${i})">
              <div class="palette-saved-head"><span class="palette-name">${this.escapeHtml(it.name)}</span>${vis}</div>
              ${desc}
              <div class="palette-query">${hl}</div>
              ${tags ? `<div class="palette-tags">${tags}</div>` : ''}
              ${meta.length ? `<div class="palette-meta"><span>${meta.join(' · ')}</span></div>` : ''}
            </div>
            <div class="palette-row-actions">
              <button class="palette-icon-btn" title="Run" onclick="event.stopPropagation();QueryPalette.runIdx(${i})">${PLAY_ICON}</button>
              <button class="palette-icon-btn" title="Edit" onclick="event.stopPropagation();QueryPalette.startEdit('${this.escapeJs(it.id)}')">${EDIT_ICON}</button>
              <button class="palette-icon-btn" title="Delete" onclick="event.stopPropagation();QueryPalette.deleteSaved('${this.escapeJs(it.id)}')">${TRASH_ICON}</button>
            </div>
          </div>`;
    },

    renderSaveForm() {
        const q = (this.promoteText || document.getElementById('queryInput')?.value || '').trim();
        const preview = q ? (window.SyntaxHighlight ? SyntaxHighlight.highlight(q) : this.escapeHtml(q)) : '<span class="palette-dim">No query in the editor</span>';
        return `
          <div class="palette-form">
            <div class="palette-form-preview">${preview}</div>
            <input id="paletteFormName" class="palette-input" placeholder="Name" maxlength="255" />
            <input id="paletteFormDesc" class="palette-input" placeholder="Description (optional)" />
            <input id="paletteFormTags" class="palette-input" placeholder="Tags (comma-separated)" />
            <label class="palette-check"><input type="checkbox" id="paletteFormPersonal" /> Personal (only visible to you)</label>
            <div class="palette-form-actions">
              <button class="btn-secondary btn-sm" onclick="QueryPalette.toggleSaveForm()">Cancel</button>
              <button class="btn-primary btn-sm" onclick="QueryPalette.submitSave()">Save query</button>
            </div>
          </div>`;
    },

    renderEditForm(it) {
        const tags = (it.tags || []).join(', ');
        const personal = it.visibility === 'personal' ? 'checked' : '';
        return `
          <div class="palette-row editing">
            <div class="palette-form">
              <input id="peName" class="palette-input" value="${this.escapeHtml(it.name)}" maxlength="255" placeholder="Name" />
              <input id="peDesc" class="palette-input" value="${this.escapeHtml(it.description || '')}" placeholder="Description (optional)" />
              <textarea id="peQuery" class="palette-input palette-textarea" placeholder="Query">${this.escapeHtml(it.query_text)}</textarea>
              <input id="peTags" class="palette-input" value="${this.escapeHtml(tags)}" placeholder="Tags (comma-separated)" />
              <label class="palette-check"><input type="checkbox" id="pePersonal" ${personal}/> Personal (only visible to you)</label>
              <div class="palette-form-actions">
                <button class="btn-secondary btn-sm" onclick="QueryPalette.cancelEdit()">Cancel</button>
                <button class="btn-primary btn-sm" onclick="QueryPalette.submitEdit('${this.escapeJs(it.id)}')">Update</button>
              </div>
            </div>
          </div>`;
    },

    emptyState() {
        if (this.activeTab === 'history') {
            return this.searchTerm
                ? '<div class="palette-empty">No matching history</div>'
                : '<div class="palette-empty">No query history yet. Run a query to see it here.</div>';
        }
        return this.searchTerm
            ? '<div class="palette-empty">No matching saved queries</div>'
            : '<div class="palette-empty">No saved queries. Run a query and press <kbd>⌘S</kbd> to save it.</div>';
    },

    // ----- Saved query actions -----

    toggleSaveForm() {
        const q = (this.promoteText || document.getElementById('queryInput')?.value || '').trim();
        if (!this.saveFormOpen && !q) {
            if (window.Toast) Toast.show('No query to save', 'warning');
            return;
        }
        // Saving always lives under the Saved tab.
        if (!this.saveFormOpen && this.activeTab !== 'saved') {
            this.switchTab('saved');
        }
        this.saveFormOpen = !this.saveFormOpen;
        if (!this.saveFormOpen) this.promoteText = null;
        this.render();
        if (this.saveFormOpen) {
            const n = document.getElementById('paletteFormName');
            if (n) setTimeout(() => n.focus(), 30);
        }
    },

    promote(i) {
        const it = this.filtered[i];
        if (!it) return;
        const text = it.query_text;
        this.switchTab('saved');
        this.promoteText = text;
        this.saveFormOpen = true;
        this.render();
        const n = document.getElementById('paletteFormName');
        if (n) setTimeout(() => n.focus(), 30);
    },

    async submitSave() {
        const name = this.val('paletteFormName').trim();
        const description = this.val('paletteFormDesc').trim();
        const tagsStr = this.val('paletteFormTags');
        const visibility = document.getElementById('paletteFormPersonal')?.checked ? 'personal' : 'shared';
        const queryText = (this.promoteText || document.getElementById('queryInput')?.value || '').trim();

        if (!name) { if (window.Toast) Toast.show('Name is required', 'warning'); return; }
        if (!queryText) { if (window.Toast) Toast.show('No query to save', 'warning'); return; }

        const tags = tagsStr.split(',').map(t => t.trim()).filter(Boolean);
        try {
            const resp = await fetch('/api/v1/saved-queries', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ name, query_text: queryText, description, tags, visibility })
            });
            const data = await resp.json();
            if (!data.success) { if (window.Toast) Toast.show(data.error || 'Failed to save', 'error'); return; }
            if (window.Toast) Toast.show('Query saved', 'success');
            this.saveFormOpen = false;
            this.promoteText = null;
            this.loadSaved(this.searchTerm);
        } catch (err) {
            console.error('[QueryPalette] save failed:', err);
            if (window.Toast) Toast.show('Failed to save query', 'error');
        }
    },

    startEdit(id) {
        this.editingId = id;
        this.render();
        const n = document.getElementById('peName');
        if (n) setTimeout(() => n.focus(), 30);
    },

    cancelEdit() { this.editingId = null; this.render(); },

    async submitEdit(id) {
        const name = this.val('peName').trim();
        const description = this.val('peDesc').trim();
        const queryText = this.val('peQuery').trim();
        const tagsStr = this.val('peTags');
        const visibility = document.getElementById('pePersonal')?.checked ? 'personal' : 'shared';

        if (!name) { if (window.Toast) Toast.show('Name is required', 'warning'); return; }
        if (!queryText) { if (window.Toast) Toast.show('Query text is required', 'warning'); return; }

        const tags = tagsStr.split(',').map(t => t.trim()).filter(Boolean);
        try {
            const resp = await fetch(`/api/v1/saved-queries/${id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ name, query_text: queryText, description, tags, visibility })
            });
            const data = await resp.json();
            if (!data.success) { if (window.Toast) Toast.show(data.error || 'Failed to update', 'error'); return; }
            if (window.Toast) Toast.show('Query updated', 'success');
            this.editingId = null;
            this.loadSaved(this.searchTerm);
        } catch (err) {
            console.error('[QueryPalette] update failed:', err);
            if (window.Toast) Toast.show('Failed to update query', 'error');
        }
    },

    async deleteSaved(id) {
        if (!confirm('Delete this saved query?')) return;
        try {
            const resp = await fetch(`/api/v1/saved-queries/${id}`, { method: 'DELETE', credentials: 'include' });
            const data = await resp.json();
            if (!data.success) { if (window.Toast) Toast.show(data.error || 'Failed to delete', 'error'); return; }
            if (window.Toast) Toast.show('Query deleted', 'success');
            this.loadSaved(this.searchTerm);
        } catch (err) {
            console.error('[QueryPalette] delete failed:', err);
            if (window.Toast) Toast.show('Failed to delete query', 'error');
        }
    },

    async toggleFavorite(id) {
        const sq = this.saved.find(s => s.id === id);
        if (!sq) return;
        const wasFav = sq.favorited;
        sq.favorited = !wasFav; // optimistic
        this.render();
        try {
            await fetch(`/api/v1/saved-queries/${id}/favorite`, {
                method: wasFav ? 'DELETE' : 'POST',
                credentials: 'include'
            });
        } catch (err) {
            sq.favorited = wasFav; // revert on failure
            this.render();
            return;
        }
        // Reload so favorites sort to the top.
        this.loadSaved(this.searchTerm);
    },

    async markUsed(id) {
        try { await fetch(`/api/v1/saved-queries/${id}/use`, { method: 'POST', credentials: 'include' }); } catch (err) { /* best-effort */ }
    },

    // ----- History actions / recording -----

    // Called by the query executor on finalize. Persists the run and updates the
    // local cache optimistically so reopening the palette is instant + fresh.
    async recordRun(meta) {
        if (!meta || !meta.query) return;
        const q = meta.query.trim();
        if (!q || q === '*') return;
        try {
            await fetch('/api/v1/query-history', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({
                    query_text: meta.query,
                    time_range: meta.timeRange || '',
                    custom_start: meta.customStart || '',
                    custom_end: meta.customEnd || '',
                    result_count: meta.resultCount != null ? meta.resultCount : null,
                    duration_ms: meta.durationMs != null ? meta.durationMs : null,
                    status: meta.status || 'ok'
                })
            });
        } catch (err) { /* best-effort; cache still updated below */ }

        let list = this.readCache().filter(h => h.query_text !== meta.query);
        list.unshift({
            id: 'local-' + new Date().getTime(),
            query_text: meta.query,
            time_range: meta.timeRange || '',
            custom_start: meta.customStart || '',
            custom_end: meta.customEnd || '',
            result_count: meta.resultCount,
            duration_ms: meta.durationMs,
            status: meta.status || 'ok',
            run_count: 1,
            last_run_at: new Date().toISOString()
        });
        list = list.slice(0, 50);
        this.writeCache(list);
        this.history = list;
        if (this.isOpen && this.activeTab === 'history') this.render();
    },

    async deleteHistory(id) {
        if (!String(id).startsWith('local-')) {
            try { await fetch('/api/v1/query-history/' + id, { method: 'DELETE', credentials: 'include' }); } catch (err) { /* ignore */ }
        }
        this.history = this.history.filter(h => h.id !== id);
        this.writeCache(this.history);
        this.render();
    },

    async clearHistory() {
        if (!confirm('Clear all query history for this fractal?')) return;
        try { await fetch('/api/v1/query-history', { method: 'DELETE', credentials: 'include' }); } catch (err) { /* ignore */ }
        this.history = [];
        this.writeCache([]);
        this.render();
    },

    // One-time import of the old localStorage recent queries into server history.
    async migrateLegacyRecent() {
        const FLAG = 'bifract_recent_queries_migrated';
        if (localStorage.getItem(FLAG)) return;
        localStorage.setItem(FLAG, '1');
        let legacy = [];
        try { legacy = JSON.parse(localStorage.getItem('bifract_recent_queries')) || []; } catch (e) { legacy = []; }
        if (!Array.isArray(legacy) || legacy.length === 0) return;
        // Record oldest first so the newest keeps the most recent last_run_at.
        for (let i = legacy.length - 1; i >= 0; i--) {
            const q = legacy[i];
            if (typeof q !== 'string' || !q.trim() || q.trim() === '*') continue;
            try {
                await fetch('/api/v1/query-history', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({ query_text: q, status: 'ok' })
                });
            } catch (e) { /* best-effort */ }
        }
        localStorage.removeItem('bifract_recent_queries');
    },

    // ----- Helpers -----

    historyCacheKey() {
        const scope = window.FractalContext?.currentFractal?.id || 'global';
        return 'bifract_query_history_' + scope;
    },
    readCache() {
        try { return JSON.parse(localStorage.getItem(this.historyCacheKey())) || []; } catch (e) { return []; }
    },
    writeCache(list) {
        try { localStorage.setItem(this.historyCacheKey(), JSON.stringify(list)); } catch (e) { /* quota */ }
    },

    fuzzyScore(needle, hay) {
        needle = needle.toLowerCase();
        hay = hay.toLowerCase();
        if (!needle) return 0;
        let n = 0, score = 0, streak = 0;
        for (let h = 0; h < hay.length && n < needle.length; h++) {
            if (hay[h] === needle[n]) { n++; streak++; score += streak; }
            else streak = 0;
        }
        return n === needle.length ? score : -1;
    },

    relTime(iso) {
        const d = new Date(iso);
        if (isNaN(d.getTime())) return '';
        const s = (new Date().getTime() - d.getTime()) / 1000;
        if (s < 60) return 'just now';
        if (s < 3600) return Math.floor(s / 60) + 'm ago';
        if (s < 86400) return Math.floor(s / 3600) + 'h ago';
        if (s < 604800) return Math.floor(s / 86400) + 'd ago';
        return d.toLocaleDateString();
    },

    fmtLocal(iso) {
        const d = new Date(iso);
        if (isNaN(d.getTime())) return '';
        const p = (n) => String(n).padStart(2, '0');
        return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
    },

    timeLabel(t) {
        const map = { '5m': '5m', '15m': '15m', '1h': '1h', '24h': '24h', '7d': '7d', '30d': '30d', 'all': 'All time', 'custom': 'Custom' };
        return map[t] || t;
    },

    val(id) { return document.getElementById(id)?.value || ''; },

    escapeHtml(str) {
        if (!str) return '';
        return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    },
    escapeJs(str) {
        if (!str) return '';
        return String(str).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    }
};

window.QueryPalette = QueryPalette;
