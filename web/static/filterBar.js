// A filter bar that shows what you chose, not what you could choose.
//
// The selects move behind one button and only reappear when you open it. Whatever is
// actually set comes back as a chip beside the search box, so a bar at rest is a search
// box and a button, and a filtered list says so in words you can clear one at a time.
const FilterBar = {
    _bars: [],

    // selects: [{ id, label }]. The select keeps its id and its options, so whatever
    // reads the value keeps working; this only changes where it lives and how it reads.
    install({ button, menu, chips, selects, onChange }) {
        const btn = document.getElementById(button);
        const panel = document.getElementById(menu);
        const chipsEl = document.getElementById(chips);
        if (!btn || !panel || !chipsEl) return null;

        const bar = { btn, panel, chipsEl, selects, onChange };
        this._bars.push(bar);

        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const opening = panel.hidden;
            this.closeAll();
            if (opening) {
                panel.hidden = false;
                btn.classList.add('active');
            }
        });

        for (const { id } of selects) {
            const select = document.getElementById(id);
            if (!select) continue;
            // The chips and the count follow the value however it was set, including
            // by code that repopulates a list of labels or feeds.
            select.addEventListener('change', () => this.render(bar));
        }

        if (!this._away) {
            this._away = (e) => {
                if (e.target.closest('.fb-pop')) return;
                this.closeAll();
            };
            document.addEventListener('click', this._away);
            document.addEventListener('keydown', (e) => { if (e.key === 'Escape') this.closeAll(); });
        }

        this.render(bar);
        return bar;
    },

    closeAll() {
        for (const bar of this._bars) {
            bar.panel.hidden = true;
            bar.btn.classList.remove('active');
        }
    },

    // Re-reads every select. Called on change and by owners that repopulate options.
    refresh() {
        this._bars.forEach(bar => this.render(bar));
    },

    render(bar) {
        const active = [];
        for (const { id, label } of bar.selects) {
            const select = document.getElementById(id);
            if (!select || !select.value || select.value === 'all') continue;
            const chosen = select.options[select.selectedIndex]?.textContent || select.value;
            active.push({ id, label, chosen });
        }

        bar.chipsEl.innerHTML = active.map(a => `
            <span class="fb-chip">
                <span class="fb-chip-label">${Utils.escapeHtml(a.label)}</span>
                <span class="fb-chip-value">${Utils.escapeHtml(a.chosen)}</span>
                <button type="button" class="fb-chip-clear" title="Clear this filter" data-select="${Utils.escapeAttr(a.id)}">&times;</button>
            </span>
        `).join('') + (active.length > 1
            ? '<button type="button" class="fb-chip-clear-all" data-clear-all="1">Clear all</button>'
            : '');

        bar.chipsEl.querySelectorAll('.fb-chip-clear').forEach(el => {
            el.addEventListener('click', () => this.clear(bar, el.dataset.select));
        });
        bar.chipsEl.querySelector('[data-clear-all]')?.addEventListener('click', () => {
            bar.selects.forEach(s => { const el = document.getElementById(s.id); if (el) el.value = 'all'; });
            this.render(bar);
            bar.onChange?.();
        });

        const count = bar.btn.querySelector('.fb-count');
        if (count) {
            count.textContent = active.length;
            count.hidden = active.length === 0;
        }
        bar.btn.classList.toggle('filtered', active.length > 0);
    },

    clear(bar, id) {
        const select = document.getElementById(id);
        if (select) select.value = 'all';
        this.render(bar);
        bar.onChange?.();
    }
};

// An empty list should say what would fill it, and why it is empty right now.
// Used wherever a tab can legitimately have nothing in it.
const EmptyState = {
    // An empty review queue is good news, so it reads like it. One of these, at random,
    // so the tab is not the same dead sentence every time.
    CLEAR_QUEUE: [
        "Queue's empty. Suspiciously quiet.",
        'Nothing to review. Someone did their homework.',
        'Empty queue. Enjoy it, it never lasts.',
        "Nothing here. Either you're fast or nobody's writing rules."
    ],

    clearQueueLine() {
        return this.CLEAR_QUEUE[Math.floor(Math.random() * this.CLEAR_QUEUE.length)];
    },

    // icon is one of the names below; action is optional { label, onclick }.
    render({ icon = 'list', title, detail = '', action = null }) {
        return `
            <div class="es">
                ${this.icon(icon)}
                <div class="es-title">${Utils.escapeHtml(title)}</div>
                ${detail ? `<div class="es-detail">${detail}</div>` : ''}
                ${action ? `<button type="button" class="btn-secondary btn-sm es-action" onclick="${action.onclick}">${Utils.escapeHtml(action.label)}</button>` : ''}
            </div>
        `;
    },

    icon(name) {
        const paths = {
            review: '<path d="M6 14l5 5 11-11" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"/><rect x="3" y="3" width="22" height="22" rx="4" stroke="currentColor" stroke-width="1.6" fill="none"/>',
            rules: '<rect x="4" y="6" width="20" height="4" rx="2" stroke="currentColor" stroke-width="1.6" fill="none"/><rect x="4" y="14" width="20" height="4" rx="2" stroke="currentColor" stroke-width="1.6" fill="none"/><rect x="4" y="22" width="12" height="4" rx="2" stroke="currentColor" stroke-width="1.6" fill="none"/>',
            grid: '<rect x="3" y="3" width="9" height="9" rx="2" stroke="currentColor" stroke-width="1.6" fill="none"/><rect x="16" y="3" width="9" height="9" rx="2" stroke="currentColor" stroke-width="1.6" fill="none"/><rect x="3" y="16" width="9" height="9" rx="2" stroke="currentColor" stroke-width="1.6" fill="none"/><rect x="16" y="16" width="9" height="9" rx="2" stroke="currentColor" stroke-width="1.6" fill="none"/>',
            list: '<path d="M5 8h18M5 14h18M5 20h12" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>'
        };
        return `<svg class="es-icon" width="28" height="28" viewBox="0 0 28 28" fill="none">${paths[name] || paths.list}</svg>`;
    }
};

window.FilterBar = FilterBar;
window.EmptyState = EmptyState;
