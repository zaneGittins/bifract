// Ask AI in the command palette: writes a BQL query from a description and
// shows it as a diff against the editor. Nothing runs until the user says so.

const QueryAssist = {
    TIME_RANGES: new Set(['5m', '15m', '1h', '6h', '12h', '24h', '7d', '30d', 'all']),

    STATUS_RETRY_MS: 30000,

    _enabled: null,     // null until /notebooks/ai-status answers
    _checking: false,
    _checkedAt: 0,
    _abort: null,
    state: null,        // { status, prompt, base, steps, result, error }

    init() {
        this.reset();
        // An answer written for one fractal must not be loaded into another.
        if (window.FractalContext) FractalContext.subscribe('QueryAssist', () => this.reset());
        Palette.register({
            id: 'ai', group: 'Ask AI', order: 1000, scope: 'ai', takeover: true, unscored: true,
            icon: Palette.ICONS.spark,
            when: ctx => ctx.hasScope && this.enabled(),
            idle: ctx => (ctx.view === 'search' ? 1 : 0),
            items: (ctx, q) => [{
                key: 'ask',
                label: q ? `Ask AI: "${q}"` : 'Describe what you want to find...',
                hintHtml: q ? '' : '<kbd>?</kbd>',
                kind: 'Ask AI',
            }],
            preview: () => `<div class="pal-pv-kind">Ask AI</div>
                <div class="pal-pv-note">Describe the logs you want in plain words. The AI reads this fractal's fields and writes a BQL query for you to review. It never runs on its own.</div>`,
            actions: () => [{ label: 'Ask AI', keepOpen: true, run: () => this.start(Palette.query()) }],
            render: (q) => this.render(q),
            enter: (q, opts) => this.enter(q, opts),
            button: name => this.button(name),
            onClose: () => this.reset(),
        });
    },

    // The answer is kept for the page's life; a failed check is retried after a
    // pause rather than on every keystroke.
    enabled() {
        if (this._enabled === null && !this._checking && Date.now() - this._checkedAt > this.STATUS_RETRY_MS) {
            this._checking = true;
            fetch('/api/v1/notebooks/ai-status', { credentials: 'include' })
                .then(r => (r.ok ? r.json() : null))
                .then(d => {
                    if (d) this._enabled = !!d.ai_enabled;
                    if (this._enabled) Palette.refresh();
                })
                .catch(() => {})
                .finally(() => {
                    this._checking = false;
                    this._checkedAt = Date.now();
                });
        }
        return !!this._enabled;
    },

    reset() {
        if (this._abort) this._abort.abort();
        this._abort = null;
        this.state = { status: 'idle', prompt: '', base: '', steps: [], result: null, last: null, error: '' };
    },

    start(text) {
        Palette.setScope('ai', text);
        if (text) this.generate(text);
    },

    // Enter on an unchanged prompt takes the result; on a new prompt it writes
    // again, refining the last result. Shift+Enter always writes again.
    enter(text, { secondary, force }) {
        if (!text) return;
        const s = this.state;
        if (s.status === 'done' && s.result && text === s.prompt && !force) this.accept(secondary);
        else if (s.status !== 'running' || text !== s.prompt) this.generate(text, force || text === s.prompt);
    },

    button(name) {
        if (name === 'load') this.accept(false);
        else if (name === 'run') this.accept(true);
        else if (name === 'retry') this.generate(this.state.prompt, true);
    },

    accept(run) {
        const r = this.state.result;
        if (!r) return;
        Palette.close(false);
        QueryPalette.loadIntoInput(r.query, null, run);
    },

    editorQuery() {
        return (document.getElementById('queryInput')?.value || '').trim();
    },

    timeRange() {
        const range = window.TimePicker ? TimePicker.serializeRange() : null;
        return range && this.TIME_RANGES.has(range.time_range) ? range.time_range : '24h';
    },

    // again repeats the last request against the same starting query; a new
    // request refines the last answer, or the editor's query if there is none.
    async generate(prompt, again) {
        if (this._abort) this._abort.abort();
        const abort = new AbortController();
        this._abort = abort;
        const prev = this.state;
        const last = prev.result || prev.last;
        const base = again && prev.status !== 'idle' ? prev.base : (last ? last.query : this.editorQuery());
        this.state = { status: 'running', prompt, base, steps: [], result: null, last, error: '' };
        Palette.refresh();

        const s = this.state;
        try {
            const resp = await fetch('/api/v1/query/assist/stream', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ prompt, current_query: base, time_range: this.timeRange() }),
                signal: abort.signal,
            });
            if (!resp.ok) {
                const body = await resp.json().catch(() => null);
                throw new Error((body && body.error) || `The server answered ${resp.status}`);
            }
            await this.readEvents(resp, ev => this.onEvent(s, ev));
            if (!s.result && !s.error) s.error = 'The AI stopped before writing a query.';
        } catch (err) {
            if (err.name === 'AbortError') return;
            s.error = err.message || 'The request failed.';
        }
        if (this.state !== s) return;
        s.status = s.result ? 'done' : 'error';
        this._abort = null;
        Palette.refresh();
    },

    async readEvents(resp, onEvent) {
        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop();
            for (const line of lines) {
                if (!line.startsWith('data: ')) continue;
                try { onEvent(JSON.parse(line.slice(6))); } catch (e) { /* partial line */ }
            }
        }
    },

    onEvent(s, ev) {
        if (this.state !== s) return;
        if (ev.type === 'tool_call') {
            const label = (typeof TOOL_LABELS !== 'undefined' && TOOL_LABELS[ev.tool_name]) || ev.tool_name.replace(/_/g, ' ');
            // Consecutive reads of the same kind read as one step.
            if (s.steps[s.steps.length - 1] !== label) s.steps.push(label);
        } else if (ev.type === 'result') {
            s.result = ev.tool_args;
        } else if (ev.type === 'error') {
            s.error = ev.content;
        }
        Palette.refresh();
    },

    // ---- Rendering ----

    render(q) {
        const s = this.state;
        const mod = Utils.isMac ? '⌘' : 'Ctrl';
        const back = '<kbd>⌫</kbd> all results';
        if (s.status === 'idle') {
            return {
                list: `<div class="pal-pv-kind">Ask AI</div>
                    <div class="pal-pv-note">Describe what you want to find, for example "failed logins by source IP" or "powershell started by office apps". If the editor has a query, you can ask to change it: "only the last hour", "add a count by host".</div>
                    <div class="pal-pv-note">The AI reads this fractal's fields and checks the query before showing it.</div>`,
                preview: '<div class="pal-pv-kind">Generated query</div><div class="pal-pv-note">The query appears here, compared with the one in your editor, for you to review before anything runs.</div>',
                foot: [q ? '<kbd>↵</kbd> write query' : 'Type a description', back, '|', 'Never runs on its own'],
            };
        }

        const steps = s.steps.map((label, i) => {
            const live = s.status === 'running' && i === s.steps.length - 1;
            return `<li class="${live ? 'live' : 'done'}">${live ? '<span class="pal-spin"></span>' : '<span class="pal-tick">&#10003;</span>'}${Utils.escapeHtml(label)}</li>`;
        }).join('');
        const working = s.status === 'running' && !s.steps.length ? '<li class="live"><span class="pal-spin"></span>Reading the request</li>' : '';
        const explanation = s.result && s.result.explanation ? `<div class="pal-pv-note">${Utils.escapeHtml(s.result.explanation)}</div>` : '';
        const list = `<div class="pal-pv-kind">How it got there</div>
            <ol class="pal-steps">${steps}${working}</ol>
            ${explanation}`;

        if (s.status === 'running') {
            return {
                list,
                preview: '<div class="pal-pv-kind">Generated query</div><div class="pal-skeleton"></div><div class="pal-skeleton short"></div>',
                foot: ['Writing a query...', back, '|', 'Never runs on its own'],
            };
        }
        if (s.status === 'error') {
            return {
                list,
                preview: `<div class="pal-pv-kind">Generated query</div>
                    <div class="pal-error" role="alert">${Utils.escapeHtml(s.error)}</div>
                    <div class="pal-pv-actions"><button type="button" class="pal-btn primary" data-ai="retry">Try again</button></div>`,
                foot: ['<kbd>↵</kbd> try again', back, '|', 'Never runs on its own'],
            };
        }

        const r = s.result;
        const changed = q && q !== s.prompt;
        const verdict = r.valid ? '<span class="pal-ok">&#10003; Valid BQL</span>' : '<span class="pal-bad">Not valid</span>';
        const why = (r.error || 'The query did not validate').trim().replace(/[^.!?]$/, '$&.');
        const invalid = r.valid ? '' : `<div class="pal-error" role="alert">${Utils.escapeHtml(why)} Load it to fix it by hand, or describe the change you want.</div>`;
        return {
            list,
            preview: `<div class="pal-pv-kind pal-pv-split">Generated query ${verdict}</div>
                <pre class="pal-code pal-hl pal-diff">${this.diffHtml(s.base, r.query)}</pre>
                ${invalid}
                ${s.base ? `<div class="pal-pv-note">Compared with the query ${s.base === this.editorQuery() ? 'in your editor' : 'from the last answer'}.</div>` : ''}
                <div class="pal-pv-actions">
                    <button type="button" class="pal-btn primary" data-ai="load">Load into editor<kbd>↵</kbd></button>
                    <button type="button" class="pal-btn" data-ai="run">Run<kbd>${mod}</kbd><kbd>↵</kbd></button>
                </div>`,
            foot: changed
                ? ['<kbd>↵</kbd> refine with this', back, '|', 'Never runs on its own']
                : ['<kbd>↵</kbd> load into editor', `<kbd>${mod}</kbd><kbd>↵</kbd> run`, '<kbd>Shift</kbd><kbd>↵</kbd> write again', '|', 'Never runs on its own'],
        };
    },

    // Pipeline stages, one per line. Only a pipe with space before and after it
    // splits, and never inside a quoted string, so an alternation in a regex or
    // a pipe in a value stays whole.
    stages(query) {
        const out = [];
        let cur = '';
        let quoted = false;
        const text = query || '';
        for (let i = 0; i < text.length; i++) {
            const c = text[i];
            if (c === '\\' && quoted) { cur += c + (text[++i] || ''); continue; }
            if (c === '"') quoted = !quoted;
            const split = !quoted && (c === '\n' || (c === '|' && /\s/.test(text[i - 1] || '') && /\s/.test(text[i + 1] || '')));
            if (split) {
                out.push(cur);
                cur = c === '|' ? '|' : '';
            } else {
                cur += c;
            }
        }
        out.push(cur);
        return out.map(l => l.trim()).filter(Boolean);
    },

    // A line diff by longest common subsequence: queries are a handful of
    // stages, so the quadratic table is tiny.
    diff(a, b) {
        const n = a.length, m = b.length;
        const t = Array.from({ length: n + 1 }, () => new Array(m + 1).fill(0));
        for (let i = n - 1; i >= 0; i--) {
            for (let j = m - 1; j >= 0; j--) {
                t[i][j] = a[i] === b[j] ? t[i + 1][j + 1] + 1 : Math.max(t[i + 1][j], t[i][j + 1]);
            }
        }
        const out = [];
        let i = 0, j = 0;
        while (i < n || j < m) {
            if (i < n && j < m && a[i] === b[j]) { out.push({ op: ' ', line: a[i] }); i++; j++; }
            else if (j < m && (i >= n || t[i][j + 1] >= t[i + 1][j])) { out.push({ op: '+', line: b[j] }); j++; }
            else { out.push({ op: '-', line: a[i] }); i++; }
        }
        return out;
    },

    diffHtml(base, next) {
        const hl = text => (window.SyntaxHighlight ? SyntaxHighlight.highlight(text) : Utils.escapeHtml(text));
        const after = this.stages(next);
        if (!base) return after.map(l => `<span class="pal-line">${hl(l)}</span>`).join('');
        const cls = { '+': 'add', '-': 'del', ' ': 'same' };
        return this.diff(this.stages(base), after)
            .map(d => `<span class="pal-line ${cls[d.op]}">${hl(d.line)}</span>`)
            .join('');
    },
};

window.QueryAssist = QueryAssist;
