// markdownEditor.js - thin factory over the vendored CodeMirror 6 (window.CM6).
//
// Provides a themed markdown editor with optional Obsidian-style "live preview"
// (markdown markers hide on inactive lines; headings size up; [[wikilinks]] render
// as clickable chips). Colors come entirely from the app's CSS variables, so the
// editor recolors automatically with the [data-theme] attribute - no theme swap.
//
// Public API:
//   const ed = MarkdownEditor.create(parentEl, {
//       value, placeholder, livePreview: true,
//       onChange(value), onSave(),                       // Cmd/Ctrl+S
//       onWikilink(name),                                // click a [[link]]
//       wikilinkTargets() -> ['Page A', 'Page B', ...],  // for [[ autocomplete + known-link styling
//   });
//   ed.getValue(); ed.setValue(str); ed.focus(); ed.destroy();
(function () {
    const WIKILINK_RE = /\[\[([^\[\]\n]+)\]\]/g;

    function ready() { return typeof window.CM6 !== 'undefined'; }

    // --- Theme: markdown token colors mapped to the app's --syntax-* / --text-* vars.
    function highlightStyle() {
        const CM6 = window.CM6;
        const { HighlightStyle, tags: t } = CM6;
        return HighlightStyle.define([
            { tag: t.heading1, class: 'cm-md-h1' },
            { tag: t.heading2, class: 'cm-md-h2' },
            { tag: t.heading3, class: 'cm-md-h3' },
            { tag: [t.heading4, t.heading5, t.heading6], class: 'cm-md-h4' },
            { tag: t.strong, fontWeight: '700' },
            { tag: t.emphasis, fontStyle: 'italic' },
            { tag: t.strikethrough, textDecoration: 'line-through' },
            { tag: t.link, color: 'var(--accent-primary)' },
            { tag: t.url, color: 'var(--accent-secondary)' },
            { tag: t.monospace, class: 'cm-md-code' },
            { tag: t.quote, color: 'var(--text-secondary)', fontStyle: 'italic' },
            { tag: t.list, color: 'var(--accent-secondary)' },
            { tag: [t.processingInstruction, t.meta], color: 'var(--text-muted)' },
        ]);
    }

    // --- Live preview decorations -------------------------------------------------
    class WikilinkWidget {
        constructor(name, known, onClick) { this.name = name; this.known = known; this.onClick = onClick; }
        eq(other) { return other.name === this.name && other.known === this.known; }
        toDOM() {
            const a = document.createElement('span');
            a.className = 'cm-wikilink' + (this.known ? '' : ' cm-wikilink-missing');
            a.textContent = this.name;
            a.setAttribute('role', 'link');
            a.title = this.known ? this.name : this.name + ' (page not found - click to create)';
            const cb = this.onClick, nm = this.name;
            a.addEventListener('mousedown', (e) => { e.preventDefault(); e.stopPropagation(); if (cb) cb(nm); });
            return a;
        }
        ignoreEvent() { return false; }
    }

    // Absolutely-positioned copy button anchored to a code block's first line.
    // A point (non-replacing) widget, so it does not affect cursor movement.
    class CopyButtonWidget {
        constructor(text) { this.text = text; }
        eq(o) { return o.text === this.text; }
        toDOM() {
            const b = document.createElement('span');
            b.className = 'cm-copy-btn';
            b.title = 'Copy code';
            b.innerHTML = '<svg width="13" height="13" viewBox="0 0 16 16" fill="none"><rect x="5" y="5" width="8" height="9" rx="1.5" stroke="currentColor" stroke-width="1.4"/><path d="M3 11V3.5A1.5 1.5 0 0 1 4.5 2H10" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/></svg>';
            const text = this.text;
            b.addEventListener('mousedown', (e) => {
                e.preventDefault(); e.stopPropagation();
                const done = () => { b.classList.add('cm-copied'); setTimeout(() => b.classList.remove('cm-copied'), 1200); };
                if (navigator.clipboard && navigator.clipboard.writeText) {
                    navigator.clipboard.writeText(text).then(done).catch(() => {});
                }
            });
            return b;
        }
        ignoreEvent() { return true; }
    }

    function buildDecorations(view, opts) {
        const CM6 = window.CM6;
        const { Decoration, syntaxTree } = CM6;
        const doc = view.state.doc;
        // IMPORTANT: this layer uses ONLY mark + line decorations. No Decoration.replace
        // and no widgets - those destabilise the cursor in this CM6 integration.
        const decos = [];

        for (const { from, to } of view.visibleRanges) {
            syntaxTree(view.state).iterate({
                from, to,
                enter: (node) => {
                    const name = node.name;
                    if (name.startsWith('ATXHeading')) {
                        const line = doc.lineAt(node.from);
                        const lvl = Math.min(6, parseInt(name.slice(10)) || 1);
                        decos.push(Decoration.line({ class: 'cm-md-line-h' + lvl }).range(line.from));
                    } else if (name === 'FencedCode' || name === 'CodeBlock') {
                        // Shade every line of the block, including the ``` fence lines.
                        const first = doc.lineAt(node.from).number;
                        const last = doc.lineAt(Math.max(node.from, node.to - 1)).number;
                        for (let ln = first; ln <= last; ln++) {
                            const line = doc.line(ln);
                            let cls = 'cm-md-codeblock';
                            if (ln === first) cls += ' cm-md-codeblock-first';
                            if (ln === last) cls += ' cm-md-codeblock-last';
                            decos.push(Decoration.line({ class: cls }).range(line.from));
                        }
                    } else if (name === 'CodeText') {
                        // Colour fenced-code content as BQL by default, reusing the query
                        // editor's tokenizer. Mark decorations only -> no cursor effect.
                        const SH = window.SyntaxHighlight;
                        if (SH && typeof SH.highlightLine === 'function') {
                            const firstLine = doc.lineAt(node.from);
                            const fenceLine = firstLine.number > 1 ? doc.line(firstLine.number - 1) : null;
                            const lang = fenceLine ? fenceLine.text.replace(/^\s*(`{3,}|~{3,})/, '').trim().toLowerCase() : '';
                            if (!lang || lang === 'bql' || lang === 'sql') {
                                const lastLine = doc.lineAt(Math.max(node.from, node.to - 1));
                                for (let ln = firstLine.number; ln <= lastLine.number; ln++) {
                                    const line = doc.line(ln);
                                    const segStart = Math.max(line.from, node.from);
                                    const segEnd = Math.min(line.to, node.to);
                                    if (segEnd <= segStart) continue;
                                    let segs = null;
                                    try { segs = SH.highlightLine(doc.sliceString(segStart, segEnd)); } catch (e) {}
                                    if (!segs) continue;
                                    let off = segStart;
                                    for (const s of segs) {
                                        const len = s.t.length;
                                        if (len > 0 && s.c && s.c !== 'hl-default') {
                                            decos.push(Decoration.mark({ class: s.c }).range(off, off + len));
                                        }
                                        off += len;
                                    }
                                }
                            }
                        }
                    }
                },
            });
            // Wikilinks: colour only (kept visible as [[text]]; Cmd/Ctrl+click follows them).
            const text = doc.sliceString(from, to);
            let m;
            WIKILINK_RE.lastIndex = 0;
            while ((m = WIKILINK_RE.exec(text)) !== null) {
                const start = from + m.index;
                decos.push(Decoration.mark({ class: 'cm-wikilink-text' }).range(start, start + m[0].length));
            }
        }
        return Decoration.set(decos, true);
    }

    function livePreviewExtension(opts) {
        const CM6 = window.CM6;
        const { ViewPlugin } = CM6;
        return ViewPlugin.fromClass(class {
            constructor(view) { this.decorations = buildDecorations(view, opts); }
            update(u) {
                if (u.docChanged || u.selectionSet || u.viewportChanged) {
                    this.decorations = buildDecorations(u.view, opts);
                }
            }
        }, { decorations: (v) => v.decorations });
    }

    // --- Wikilink autocomplete ([[ ... ) -----------------------------------------
    function wikilinkCompletion(opts) {
        return (ctx) => {
            const before = ctx.matchBefore(/\[\[[^\[\]\n]*/);
            if (!before) return null;
            const targets = (opts.wikilinkTargets && opts.wikilinkTargets()) || [];
            if (!targets.length && !ctx.explicit) return null;
            const typed = before.text.slice(2).toLowerCase();
            const options = targets
                .filter((nm) => nm.toLowerCase().includes(typed))
                .slice(0, 50)
                .map((nm) => ({ label: nm, type: 'text', apply: nm + ']]' }));
            return { from: before.from + 2, options, validFor: /[^\[\]\n]*/ };
        };
    }

    function create(parent, opts) {
        opts = opts || {};
        if (!ready()) { console.error('MarkdownEditor: CM6 bundle not loaded'); return null; }
        const CM6 = window.CM6;
        const {
            EditorState, EditorView, keymap, placeholder, history, historyKeymap,
            defaultKeymap, indentWithTab, markdown, markdownLanguage, syntaxHighlighting,
            indentOnInput, closeBrackets, closeBracketsKeymap, autocompletion,
            completionKeymap, drawSelection, highlightActiveLine,
        } = CM6;

        const exts = [
            history(),
            drawSelection(),
            indentOnInput(),
            EditorView.lineWrapping,
            markdown({ base: markdownLanguage, addKeymap: true }),
            syntaxHighlighting(highlightStyle()),
            autocompletion({ override: [wikilinkCompletion(opts)] }),
            // Typing the 3rd backtick opens a fenced code block with the cursor inside.
            EditorView.inputHandler.of((view, from, to, text) => {
                if (text !== '`' || from !== to) return false;
                const line = view.state.doc.lineAt(from);
                const lineStart = line.text.slice(0, from - line.from);
                if (/^\s*``$/.test(lineStart)) {
                    view.dispatch({
                        changes: { from, to, insert: '`\n\n```' },
                        selection: { anchor: from + 2 },
                        userEvent: 'input.type',
                    });
                    return true;
                }
                return false;
            }),
            // Cmd/Ctrl+click a [[wikilink]] to follow it (event-only; no decoration).
            EditorView.domEventHandlers({
                mousedown(e, view) {
                    if (!opts.onWikilink || !(e.metaKey || e.ctrlKey)) return false;
                    const pos = view.posAtCoords({ x: e.clientX, y: e.clientY });
                    if (pos == null) return false;
                    const line = view.state.doc.lineAt(pos);
                    const col = pos - line.from;
                    const re = /\[\[([^\[\]\n]+)\]\]/g;
                    let m;
                    while ((m = re.exec(line.text)) !== null) {
                        if (col >= m.index && col <= m.index + m[0].length) {
                            e.preventDefault();
                            opts.onWikilink(m[1].trim());
                            return true;
                        }
                    }
                    return false;
                },
            }),
            keymap.of([
                ...defaultKeymap,
                ...historyKeymap,
                ...completionKeymap,
                indentWithTab,
                {
                    key: 'Mod-s',
                    preventDefault: true,
                    run: () => { if (opts.onSave) opts.onSave(); return true; },
                },
            ]),
            EditorView.theme({}, { dark: false }), // base; colors come from style.css
            EditorView.updateListener.of((u) => {
                if (u.docChanged && opts.onChange) opts.onChange(u.state.doc.toString());
            }),
        ];
        if (opts.placeholder) exts.push(placeholder(opts.placeholder));
        // Live preview is now marks-and-lines only (BQL code highlighting, heading
        // sizing, code shading, coloured wikilinks). No replacement/widget decorations,
        // which were the cause of the cursor instability.
        if (opts.livePreview !== false) exts.push(livePreviewExtension(opts));

        const view = new EditorView({
            parent,
            state: EditorState.create({ doc: opts.value || '', extensions: exts }),
        });

        return {
            view,
            getValue: () => view.state.doc.toString(),
            setValue: (str) => view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: str || '' } }),
            focus: () => view.focus(),
            scrollTo: (pos) => {
                const p = Math.max(0, Math.min(pos, view.state.doc.length));
                view.dispatch({ selection: { anchor: p }, effects: EditorView.scrollIntoView(p, { y: 'start' }) });
                view.focus();
            },
            destroy: () => view.destroy(),
        };
    }

    window.MarkdownEditor = { create, ready };
})();
