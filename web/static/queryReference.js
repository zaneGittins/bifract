// Functions and operators in the command palette: the BQL reference, searched
// by name and description and read in the preview. Enter puts one in the query
// at the cursor; nothing else in the query changes.

const QueryReference = {
    ICON: '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><path d="M10 2.5c-1.6 0-2.2.8-2.5 2.5l-1 6c-.3 1.7-.9 2.5-2.5 2.5M5 6.5h5"/></svg>',

    _items: null,

    init() {
        const btn = document.getElementById('functionsMenuBtn');
        if (btn) btn.addEventListener('click', (e) => { e.stopPropagation(); this.open(); });
        if (window.BQLLang) BQLLang.onReady(() => { this._items = null; Palette.refresh(); });

        Palette.register({
            id: 'functions', group: 'Functions', order: 200, tab: 'functions', icon: this.ICON,
            when: ctx => ctx.hasScope,
            // Only Query writes queries, so other pages keep their own rows.
            inAll: ctx => ctx.view === 'search',
            items: () => this.items(),
            preview: it => this.preview(it),
            actions: () => [
                { label: 'Insert in query', run: it => this.insert(it) },
                { label: 'Copy example', run: it => this.copy(it) },
            ],
        });
    },

    // text pre-fills the search, as when the editor's hint asks for more.
    open(text) {
        Palette.open({ tab: 'functions', text });
    },

    items() {
        if (this._items) return this._items;
        const lang = window.BQLLang;
        if (!lang || !lang.loaded) return [];
        this._items = [
            ...lang.functions.map(fn => ({
                key: `fn:${fn.name}`,
                label: fn.name,
                mono: true,
                search: [...(fn.aliases || []), fn.category, fn.description].join(' '),
                hint: fn.category,
                fn,
            })),
            ...lang.operators.map(op => ({
                key: `op:${op.operator}`,
                label: op.operator,
                mono: true,
                search: `operator ${op.description}`,
                hint: 'Operator',
                op,
            })),
        ];
        return this._items;
    },

    preview(it) {
        const esc = Utils.escapeHtml;
        const hl = text => (window.SyntaxHighlight ? SyntaxHighlight.highlight(text) : esc(text));
        const doc = it.fn || it.op;
        const examples = (doc.examples || []).map(ex => `<pre class="pal-code pal-hl">${hl(ex)}</pre>`).join('');
        if (it.op) {
            return `<div class="pal-pv-kind">Operator</div>
                <div class="pal-pv-title mono">${esc(it.op.operator)}</div>
                <div class="pal-pv-note">${esc(it.op.description)}</div>
                ${examples ? `<div class="pal-pv-label">Examples</div>${examples}` : ''}`;
        }
        const fn = it.fn;
        const params = (fn.parameters || []).map(p => `<dt><code>${esc(p.name)}</code>${p.required ? '' : '<span class="pal-opt">optional</span>'}</dt>
            <dd>${esc(p.description || '')}${p.type ? ` <span class="pal-type">${esc(p.type)}</span>` : ''}</dd>`).join('');
        const aliases = (fn.aliases || []).length ? `<div class="pal-pv-note">Also written ${fn.aliases.map(a => `<code>${esc(a)}</code>`).join(', ')}</div>` : '';
        return `<div class="pal-pv-kind">${esc(fn.category)} function</div>
            <div class="pal-pv-title mono">${esc(fn.name)}</div>
            <div class="pal-pv-note">${esc(fn.description)}</div>
            ${aliases}
            <pre class="pal-code pal-hl">${hl(fn.syntax)}</pre>
            ${params ? `<dl class="pal-meta pal-params">${params}</dl>` : ''}
            ${examples ? `<div class="pal-pv-label">Examples</div>${examples}` : ''}`;
    },

    // The text to insert for an item, and how far back from its end the caret goes.
    snippet(it, before) {
        if (it.op) return { text: it.op.operator, back: 0 };
        let text = BQLLang.funcInsertText(it.fn);
        let back = BQLLang.funcCaretBack(it.fn);
        if (text.endsWith('(')) { text += ')'; back = 1; }
        // A pipeline command after other text starts its own stage.
        const prev = before.trimEnd();
        if (!it.fn.operand && prev && !prev.endsWith('|')) text = ' | ' + text;
        else if (prev.endsWith('|') && !before.endsWith(' ')) text = ' ' + text;
        return { text, back };
    },

    insert(it) {
        const away = !(window.App && App.currentViewLevel === 'fractal' && App.currentView === 'search');
        if (away && window.App) App.showFractalView('search');
        const qi = document.getElementById('queryInput');
        if (!qi) return;
        // Coming from another page, the old caret means nothing: add to the end.
        const start = away ? qi.value.length : qi.selectionStart;
        const end = away ? qi.value.length : qi.selectionEnd;
        const { text, back } = this.snippet(it, qi.value.slice(0, start));
        qi.setRangeText(text, start, end, 'end');
        qi.selectionStart = qi.selectionEnd = start + text.length - back;
        qi.dispatchEvent(new Event('input', { bubbles: true }));
        qi.focus();
    },

    async copy(it) {
        const doc = it.fn || it.op;
        const text = (doc.examples && doc.examples[0]) || (it.fn ? it.fn.syntax : it.op.operator);
        const ok = await Utils.copyText(text);
        if (window.Toast) Toast.show(ok ? 'Example copied' : 'Copy needs HTTPS', ok ? 'success' : 'warning');
    },
};

window.QueryReference = QueryReference;
