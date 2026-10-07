// Fact chips: the one way a score explains itself across Models and pgraph.
// Each chip states a single fact against the baseline it was measured on
// ("Seen on 1 of 30 days", "1 of 302 hosts"), so an analyst never has to
// decode a bare number. A chip with a query is a pivot into search.
const FactChips = {
    // chips: [{ label, value, tone, title, query }]
    //   label: what the fact is ("First seen"); value: the measurement ("2d ago")
    //   tone:  'alert' | 'warn' | 'muted' | '' (how unusual the fact is)
    //   title: optional longer explanation for the hover
    //   query: optional BQL; the chip becomes a button that pivots to search
    render(chips, opts = {}) {
        const esc = Utils.escapeHtml;
        const list = (chips || []).filter(c => c && (c.value !== undefined && c.value !== null && c.value !== ''));
        if (!list.length) return '';
        const items = list.map(c => {
            const tone = c.tone ? ` fact-chip-${c.tone}` : '';
            const title = c.title ? ` title="${esc(c.title)}"` : '';
            const label = c.label ? `<span class="fact-chip-label">${esc(c.label)}</span>` : '';
            const body = `${label}<b>${esc(String(c.value))}</b>`;
            return c.query
                ? `<button type="button" class="fact-chip fact-chip-pivot${tone}"${title} data-fc-query="${esc(c.query)}">${body}</button>`
                : `<span class="fact-chip${tone}"${title}>${body}</span>`;
        }).join('');
        return `<div class="fact-chips${opts.compact ? ' fact-chips-compact' : ''}">${items}</div>`;
    },

    // Delegated click handling for pivot chips under root. onPivot receives the
    // chip's query; it is bound once per root element.
    bind(root, onPivot) {
        if (!root || root._factChipsBound) return;
        root._factChipsBound = true;
        root.addEventListener('click', e => {
            const chip = e.target.closest('.fact-chip-pivot');
            if (!chip || !root.contains(chip)) return;
            e.stopPropagation();
            onPivot(chip.dataset.fcQuery);
        });
    },

    // "today", "3d ago", "5mo ago" from a date or timestamp string, or ''.
    ago(v) {
        const ms = window.TZ ? TZ.toEpoch(v) : Date.parse(v);
        if (!Number.isFinite(ms) || ms < 86400000) return '';
        const days = Math.floor((Date.now() - ms) / 86400000);
        if (days <= 0) return 'today';
        if (days < 60) return days + 'd ago';
        return Math.floor(days / 30) + 'mo ago';
    },

    // "1 of 302": a count against its population, with thousands separators.
    of(n, total) {
        const f = x => Math.round(Number(x) || 0).toLocaleString();
        return `${f(n)} of ${f(total)}`;
    },
};

window.FactChips = FactChips;
