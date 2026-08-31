// The star gutter: one capture verb on a results row.
//
// A reserved 26px channel at the head of every row, never resizable, sortable
// or reorderable. The glyph is transparent until the row is hovered and stays
// visible once the row is in the active notebook, so a table nobody stars reads
// exactly as it did before. The same channel carries the mark that someone has
// annotated the row, rather than competing with a second border.
//
// The column is not rendered at all until the scope has a notebook or a comment
// (see NotebookRail.captureEnabled), so Bifract stays unchanged for anyone who
// never uses notebooks.
const StarGutter = {
    WIDTH: 26,

    _star(filled) {
        const d = 'M12 3.6l2.6 5.28 5.83.85-4.22 4.11.997 5.8L12 16.9l-5.21 2.74.996-5.8-4.22-4.11 5.83-.85z';
        return '<svg viewBox="0 0 24 24" width="13" height="13" aria-hidden="true">' +
            `<path class="sg-outline"${filled ? ' style="opacity:0"' : ''} d="${d}" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round"/>` +
            `<path class="sg-fill"${filled ? ' style="opacity:1"' : ''} d="${d}" fill="currentColor"/>` +
            '</svg>';
    },

    // Whether a table should carry the gutter at all.
    enabled() {
        return !!(window.NotebookRail && NotebookRail.captureEnabled());
    },

    headerHtml() {
        return '<th class="sg-gutter" scope="col"></th>';
    },

    colHtml() {
        return `<col class="sg-col" style="width:${this.WIDTH}px">`;
    },

    // logID is empty for aggregated rows, which have no event to star. The cell
    // is still emitted so every row has the same number of columns.
    cellHtml(logID) {
        if (!logID) return '<td class="sg-gutter"></td>';
        const starred = !!(window.NotebookRail && NotebookRail.hasPinned(logID));
        const label = starred ? 'Remove from the active notebook' : 'Add to the active notebook';
        return `<td class="sg-gutter"><button type="button" class="sg-star" data-log-id="${Utils.escapeAttr(logID)}"` +
            ` aria-pressed="${starred}" title="${label}" aria-label="${label}">${this._star(starred)}</button></td>`;
    },

    // Row class contributed by the gutter's own state.
    rowClass(logID) {
        return logID && window.NotebookRail && NotebookRail.hasPinned(logID) ? 'starred' : '';
    },

    // Toggle from a click or the keyboard accelerator. rowData is passed so a
    // new star can title itself from the fields an analyst scans for.
    toggle(logID, rowData) {
        if (!logID || !window.NotebookRail) return;
        if (NotebookRail.hasPinned(logID)) NotebookRail.unstarLog(logID);
        else NotebookRail.pinLog(rowData || { log_id: logID });
    },

    // Repaint the marks on rows already on screen, so starring one row does not
    // cost a re-render of the table.
    syncRendered(root) {
        const scope = root || document;
        scope.querySelectorAll('.sg-star[data-log-id]').forEach(btn => {
            const starred = !!(window.NotebookRail && NotebookRail.hasPinned(btn.dataset.logId));
            const row = btn.closest('tr');
            if (row) row.classList.toggle('starred', starred);
            btn.setAttribute('aria-pressed', String(starred));
            const label = starred ? 'Remove from the active notebook' : 'Add to the active notebook';
            btn.title = label;
            btn.setAttribute('aria-label', label);
            btn.innerHTML = this._star(starred);
        });
    },

    // `s` on the selected row. Held here rather than on the table so it keeps
    // working across re-renders and paging.
    initKeyboard() {
        if (this._keyboardInit) return;
        this._keyboardInit = true;
        document.addEventListener('keydown', (e) => {
            if (e.key !== 's' && e.key !== 'S') return;
            if (e.metaKey || e.ctrlKey || e.altKey) return;
            const t = e.target;
            if (t && (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName))) return;
            if (t && t.closest && t.closest('.cm-editor, .modal-overlay')) return;

            const btn = document.querySelector('#resultsTable .result-row.selected .sg-star');
            if (!btn) return;
            e.preventDefault();
            this.toggle(btn.dataset.logId, this._rowDataFor(btn));
        });
    },

    // The row object behind a star button, for titling a new capture.
    _rowDataFor(btn) {
        const row = btn.closest('.result-row');
        if (!row || !window.QueryExecutor) return null;
        const index = parseInt(row.dataset.index, 10);
        if (Number.isNaN(index)) return null;
        const page = window.Pagination ? Pagination.getCurrentPageResults() : QueryExecutor.currentResults;
        return (page && page[index]) || null;
    },
};

window.StarGutter = StarGutter;

document.addEventListener('DOMContentLoaded', () => StarGutter.initKeyboard());
