// Query autocomplete: a caret-anchored suggestion dropdown plus the ? signature
// hint. All language data (functions, signatures, parameters) comes from the
// BQLLang registry, which loads the canonical catalog from the backend; field
// names and values come from FieldStats (the current result set). There is no
// hardcoded function list here -- adding a BQL function lights up completion and
// the ? hint automatically.
const Autocomplete = {
    // ---- ? hint state ----
    _hintVisible: false,
    _hintAnchor: null,

    // ---- on-demand menu state (the browsable list, opened with Ctrl+Space) ----
    _menuVisible: false,
    _acAnchor: null,       // textarea the menu/ghost is attached to
    _items: [],            // current candidate items
    _selected: 0,          // highlighted index
    _ctx: null,            // active completion context
    _mirror: null,         // cached mirror div for caret measurement

    // ---- inline ghost-text state (the ambient suggestion while typing) ----
    _ghost: null,          // { suffix, ctx, item, anchor } or null

    // ---- idle reveal: the menu opens itself after a brief typing pause, so the
    // feature is discoverable without a popup interrupting fast typists. ----
    _idleTimer: null,
    _idleDelay: 450,       // ms of no typing before the menu appears

    init() {
        // While typing: drive the inline ghost suggestion (or refilter an open
        // menu). No popup appears on its own -- the menu is summoned with Ctrl+Space.
        document.addEventListener('input', (e) => {
            if (e.target.tagName === 'TEXTAREA' && this._isQueryTextarea(e.target)) {
                this._checkHintTrigger(e.target);
                if (this._hintVisible) {
                    this._hideMenu();
                    this._hideGhost();
                } else {
                    this._onInput(e.target);
                }
            }
        });

        // Capture-phase keys so accept/navigate beats the editors' own Enter/Tab
        // handlers. Enter is never intercepted while only the ghost shows, so it
        // always runs the query.
        document.addEventListener('keydown', (e) => this._onKeyDownCapture(e), true);

        document.addEventListener('click', (e) => {
            if (e.target.tagName === 'TEXTAREA' && this._isQueryTextarea(e.target)) {
                this._onInput(e.target);
            }
            this._maybeCloseOnOutsideClick(e);
        });
        document.addEventListener('focusout', (e) => {
            if (e.target === this._acAnchor) {
                this._hideGhost();
                setTimeout(() => {
                    if (document.activeElement !== this._acAnchor) this._hideMenu();
                }, 150);
            }
        });
        // Reposition would drift on scroll/resize; just dismiss.
        document.addEventListener('scroll', (e) => {
            if (this._acAnchor && (e.target === this._acAnchor || (e.target.contains && e.target.contains(this._acAnchor)))) {
                this._hideGhost();
                this._hideMenu();
            }
        }, true);
        window.addEventListener('resize', () => { this._hideGhost(); this._hideMenu(); });
    },

    // Identify query editors: main search, alert editor, notebook + dashboard
    // query cells, and the analytics model builder. Markdown notebook cells share
    // the edit-content- id prefix but are NOT inside .query-input-wrapper, so the
    // wrapper check correctly excludes them.
    _isQueryTextarea(el) {
        if (el.classList.contains('search-input')) return true;
        if (el.closest('.query-input-wrapper')) return true;
        if (el.id && el.id.startsWith('wie-q-')) return true;
        if (el.id === 'modelQueryInput') return true;
        return false;
    },

    // ===================== ? signature hint =====================

    _getEnclosingFunction(text, cursorPos) {
        const before = text.substring(0, cursorPos);
        // Walk back to the nearest unmatched opener -- "(" for normal calls or "{"
        // for the brace-structured case command -- and read the name in front of it.
        let depth = 0;
        for (let i = before.length - 1; i >= 0; i--) {
            const ch = before[i];
            if (ch === ')' || ch === '}') {
                depth++;
            } else if (ch === '(' || ch === '{') {
                if (depth === 0) {
                    const match = before.substring(0, i).match(/([a-zA-Z_]\w*)\s*$/);
                    return match ? match[1] : null;
                }
                depth--;
            }
        }
        return null;
    },

    _isInsideString(text, pos) {
        let inSingle = false;
        let inDouble = false;
        for (let i = 0; i < pos; i++) {
            const ch = text[i];
            if (ch === '"' && !inSingle) inDouble = !inDouble;
            else if (ch === "'" && !inDouble) inSingle = !inSingle;
        }
        return inSingle || inDouble;
    },

    _checkHintTrigger(textarea) {
        if (!textarea) return;
        const cursorPos = textarea.selectionStart;
        const text = textarea.value;
        const charBefore = cursorPos > 0 ? text[cursorPos - 1] : '';

        if (charBefore === '?') {
            if (this._isInsideString(text, cursorPos - 1)) { this.hideHint(); return; }
            const funcName = this._getEnclosingFunction(text, cursorPos - 1);
            if (funcName && window.BQLLang) {
                const hint = BQLLang.getHint(funcName);
                if (hint && hint.signature) { this._showHint(hint, textarea); return; }
            }
        }
        if (this._hintVisible) this.hideHint();
    },

    _showHint(hint, textarea) {
        let popup = document.getElementById('functionHint');
        if (!popup) {
            popup = document.createElement('div');
            popup.id = 'functionHint';
            popup.className = 'function-hint';
            document.body.appendChild(popup);
        }

        let html = '<div class="fn-hint-header">';
        html += '<span class="fn-hint-signature">' + this._escapeHtml(hint.signature) + '</span>';
        html += '<span class="fn-hint-dismiss" title="Dismiss">&times;</span>';
        html += '</div>';

        if (hint.args.length > 0) {
            html += '<div class="fn-hint-args">';
            for (const arg of hint.args) {
                const reqClass = arg.required ? 'fn-arg-required' : 'fn-arg-optional';
                const reqLabel = arg.required ? '' : '?';
                html += '<div class="fn-hint-arg">';
                html += '<span class="fn-arg-name ' + reqClass + '">' + this._escapeHtml(arg.name) + reqLabel + '</span>';
                html += '<span class="fn-arg-desc">' + this._escapeHtml(arg.desc) + '</span>';
                html += '</div>';
            }
            html += '</div>';
        }

        html += '<div class="fn-hint-example">' + this._escapeHtml(hint.example) + '</div>';
        popup.innerHTML = html;

        const rect = textarea.getBoundingClientRect();
        popup.style.top = (rect.bottom + 4) + 'px';
        popup.style.left = rect.left + 'px';
        popup.style.maxWidth = Math.min(480, rect.width) + 'px';
        popup.style.display = 'block';
        this._hintVisible = true;
        this._hintAnchor = textarea;

        const dismissBtn = popup.querySelector('.fn-hint-dismiss');
        if (dismissBtn) {
            dismissBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.hideHint();
            });
        }
    },

    hideHint() {
        const popup = document.getElementById('functionHint');
        if (popup) popup.style.display = 'none';
        this._hintVisible = false;
    },

    _maybeCloseOnOutsideClick(e) {
        const dropdown = document.getElementById('bqlAutocomplete');
        if (this._menuVisible && dropdown && !dropdown.contains(e.target) && e.target !== this._acAnchor) {
            this._hideMenu();
        }
        const hintPopup = document.getElementById('functionHint');
        if (hintPopup && !hintPopup.contains(e.target) &&
            (!this._hintAnchor || !this._hintAnchor.contains(e.target))) {
            this.hideHint();
        }
    },

    _escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    },

    // ===================== field / value sources =====================

    _getFieldNames() {
        // Merge fields seen in the current results (FieldStats) with the fields
        // known ahead of any query (base columns + configured schema fields), so
        // completion works even before the first search. Deduplicated.
        const set = new Set();
        if (window.FieldStats && FieldStats.stats) {
            Object.keys(FieldStats.stats).forEach(f => set.add(f));
        }
        if (window.BQLLang && typeof BQLLang.knownFields === 'function') {
            BQLLang.knownFields().forEach(f => set.add(f));
        }
        return Array.from(set);
    },

    _getFieldValues(fieldName) {
        if (window.FieldStats && FieldStats.stats && FieldStats.stats[fieldName]) {
            return (FieldStats.stats[fieldName].topValues || []).map(([val, count]) => ({ value: String(val), count }));
        }
        return [];
    },

    // ===================== completion context =====================

    // Value position: cursor right after field= / field!= (optionally a partial,
    // quoted or not). Returns the field, partial text, quote state, and the start
    // index of the replaceable region.
    _getValueContext(text, cursorPos) {
        const before = text.substring(0, cursorPos);
        // Equality-style operators where the field's top values are useful:
        // = != =~ =^ =$ (the operator is captured so it never leaks into partial).
        const quoted = before.match(/([a-zA-Z_][\w.]*)(!?=[~^$]?)"([^"]*)$/);
        if (quoted) {
            return { field: quoted[1], partial: quoted[3], quoted: true, start: cursorPos - quoted[3].length };
        }
        const bare = before.match(/([a-zA-Z_][\w.]*)(!?=[~^$]?)([^"\s|()\[\]]*)$/);
        if (bare) {
            return { field: bare[1], partial: bare[3], quoted: false, start: cursorPos - bare[3].length };
        }
        return null;
    },

    // True when the cursor sits inside an unmatched "(" (function arguments).
    _insideParens(before) {
        let depth = 0;
        for (let i = before.length - 1; i >= 0; i--) {
            const ch = before[i];
            if (ch === ')') depth++;
            else if (ch === '(') { if (depth === 0) return true; depth--; }
        }
        return false;
    },

    _computeContext(textarea) {
        const value = textarea.value;
        const cursorPos = textarea.selectionStart;
        const before = value.substring(0, cursorPos);

        const vc = this._getValueContext(value, cursorPos);
        if (vc) {
            return { kind: 'value', field: vc.field, partial: vc.partial, quoted: vc.quoted, start: vc.start, end: cursorPos };
        }

        // Token being typed (identifier under the caret).
        const tokenMatch = before.match(/[a-zA-Z_][\w.]*$/);
        const partial = tokenMatch ? tokenMatch[0] : '';
        const start = cursorPos - partial.length;
        const insideParens = this._insideParens(before);
        // Empty token only opens the palette at the start of a pipeline segment.
        const afterPipe = /(^|\|)\s*$/.test(before) || (insideParens && /[(,\s]\s*$/.test(before));

        return { kind: 'token', partial, start, end: cursorPos, insideParens, afterPipe };
    },

    // ===================== candidate building =====================

    _match(label, partial) {
        if (!partial) return { ok: true, prefix: true, index: 0 };
        const l = label.toLowerCase();
        const p = partial.toLowerCase();
        const idx = l.indexOf(p);
        if (idx < 0) return { ok: false };
        return { ok: true, prefix: idx === 0, index: idx };
    },

    _buildItems(ctx) {
        if (ctx.kind === 'value') {
            const values = this._getFieldValues(ctx.field);
            const out = [];
            for (const v of values) {
                const m = this._match(v.value, ctx.partial);
                if (!m.ok) continue;
                const escaped = v.value.replace(/"/g, '\\"');
                out.push({
                    kind: 'value',
                    label: v.value,
                    insert: ctx.quoted ? escaped : '"' + escaped + '"',
                    detail: this._formatCount(v.count),
                    desc: '',
                    matchIndex: m.index,
                    matchLen: ctx.partial.length,
                    _prefix: m.prefix,
                });
                if (out.length >= 50) break;
            }
            return out;
        }

        // token context: field names + functions (+ logical keywords at top level)
        const partial = ctx.partial;
        const out = [];

        for (const f of this._getFieldNames()) {
            const m = this._match(f, partial);
            if (!m.ok) continue;
            out.push({ kind: 'field', label: f, insert: f, detail: 'field', desc: '', matchIndex: m.index, matchLen: partial.length, _prefix: m.prefix });
        }

        if (window.BQLLang) {
            for (const fn of BQLLang.functions) {
                if (!fn || !fn.name) continue;
                // Match against the name first, then any alias (for ranking), but
                // emphasize against the displayed label only.
                const nameM = this._match(fn.name, partial);
                let m = nameM;
                if (!m.ok) {
                    const alias = (fn.aliases || []).find(a => this._match(a, partial).ok);
                    if (!alias) continue;
                    m = this._match(alias, partial);
                }
                out.push({
                    kind: 'function',
                    label: fn.name,
                    insert: BQLLang.funcInsertText(fn),
                    caretBack: BQLLang.funcCaretBack(fn),
                    detail: fn.category || 'function',
                    desc: fn.description || '',
                    matchIndex: nameM.ok ? nameM.index : -1,
                    matchLen: nameM.ok ? partial.length : 0,
                    _prefix: m.prefix,
                });
            }
            if (!ctx.insideParens) {
                for (const kw of BQLLang.keywords) {
                    const m = this._match(kw.name, partial);
                    if (!m.ok) continue;
                    out.push({ kind: 'keyword', label: kw.name, insert: kw.name + ' ', detail: 'logical', desc: kw.desc, matchIndex: m.index, matchLen: partial.length, _prefix: m.prefix });
                }
            }
        }

        // Rank: prefix matches first, then field < function < keyword, then earlier
        // match position, then shorter label, then alphabetical.
        const order = { field: 0, function: 1, keyword: 2 };
        out.sort((a, b) => {
            if (a._prefix !== b._prefix) return a._prefix ? -1 : 1;
            if (order[a.kind] !== order[b.kind]) return order[a.kind] - order[b.kind];
            if (a.matchIndex !== b.matchIndex) return a.matchIndex - b.matchIndex;
            if (a.label.length !== b.label.length) return a.label.length - b.label.length;
            return a.label.localeCompare(b.label);
        });
        return out.slice(0, 50);
    },

    _formatCount(n) {
        if (n == null) return '';
        if (n >= 1000000) return (n / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
        if (n >= 1000) return (n / 1000).toFixed(1).replace(/\.0$/, '') + 'k';
        return String(n);
    },

    // ===================== input handling =====================

    _onInput(textarea) {
        // Ignore programmatic input on a field the user is not editing (e.g.
        // loading a saved query, copy-to-query, undo/redo).
        if (document.activeElement !== textarea) { this._clearIdle(); this._hideMenu(); this._hideGhost(); return; }

        // If the browsable menu is open, keep it in sync with what's typed.
        if (this._menuVisible) { this._refilterMenu(textarea); return; }

        // Otherwise drive the ambient inline ghost suggestion, and arm the idle
        // reveal so the menu surfaces if the user pauses.
        this._computeGhost(textarea);
        this._scheduleIdleReveal(textarea);
    },

    _clearIdle() {
        if (this._idleTimer) { clearTimeout(this._idleTimer); this._idleTimer = null; }
    },

    _scheduleIdleReveal(textarea) {
        this._clearIdle();
        this._idleTimer = setTimeout(() => {
            this._idleTimer = null;
            if (document.activeElement !== textarea || this._menuVisible || this._hintVisible) return;
            // Don't auto-reveal on an empty or match-all (*) query: there is nothing
            // meaningful to complete yet. Explicit Ctrl+Space still works.
            const trimmed = textarea.value.trim();
            if (trimmed === '' || trimmed === '*') return;
            const ctx = this._computeContext(textarea);
            if (this._isCompletable(ctx)) this._openMenu(textarea);
        }, this._idleDelay);
    },

    // Whether a context is worth revealing the menu for: a value slot, a token
    // being typed, or the blank start of a pipeline segment.
    _isCompletable(ctx) {
        if (ctx.kind === 'value') return true;
        if (ctx.partial && ctx.partial.length >= 1) return true;
        return !!ctx.afterPipe;
    },

    // ===================== inline ghost text =====================

    _computeGhost(textarea) {
        this._hideGhost();

        const value = textarea.value;
        const pos = textarea.selectionStart;
        // Ghost only completes at the end of the input, like a shell autosuggestion;
        // this keeps it aligned with the transparent-textarea overlay and never
        // rewrites text the user has already moved past.
        if (pos !== value.length) return;

        const ctx = this._computeContext(textarea);
        // Need something to extend: a partial token, or a committed field= value.
        if (ctx.kind !== 'value' && ctx.partial.length < 1) return;

        const items = this._buildItems(ctx);
        for (const it of items) {
            if (!it._prefix) continue; // ghost is a suffix, so prefix matches only
            const suffix = this._ghostSuffix(ctx, it);
            if (suffix) {
                this._ghost = { suffix, ctx, item: it, anchor: textarea };
                this._acAnchor = textarea;
                this._renderGhost(textarea, suffix);
                return;
            }
        }
    },

    // The text to append after the caret to complete `item`, or null when the
    // completion cannot be expressed as a clean suffix (e.g. a value needing an
    // opening quote, or a non-trivial value while unquoted).
    _ghostSuffix(ctx, item) {
        const partial = ctx.partial || '';
        if (item.kind === 'value') {
            const v = item.label;
            if (!v.toLowerCase().startsWith(partial.toLowerCase())) return null;
            const rest = v.slice(partial.length);
            if (ctx.quoted) {
                if (/["\\]/.test(v)) return null; // would need escaping; leave to menu
                return rest + '"';
            }
            if (!/^[A-Za-z0-9_.:\-]+$/.test(v)) return null; // unquoted only for simple values
            return rest;
        }
        // function / field / keyword: insert is name + a fixed tail; the label
        // starts with the partial for prefix matches, so the suffix is the tail.
        if (!item.insert.toLowerCase().startsWith(partial.toLowerCase())) return null;
        return item.insert.slice(partial.length);
    },

    _renderGhost(textarea, suffix) {
        let el = document.getElementById('bqlGhost');
        if (!el) {
            el = document.createElement('div');
            el.id = 'bqlGhost';
            el.className = 'ac-ghost';
            document.body.appendChild(el);
        }
        const computed = window.getComputedStyle(textarea);
        ['fontFamily', 'fontSize', 'fontWeight', 'fontStyle', 'letterSpacing', 'lineHeight', 'tabSize']
            .forEach(p => { el.style[p] = computed[p]; });
        el.textContent = suffix;

        const caret = this._caretCoords(textarea, textarea.value.length);
        el.style.top = caret.top + 'px';
        el.style.left = caret.left + 'px';
        el.style.display = 'block';
    },

    _hideGhost() {
        const el = document.getElementById('bqlGhost');
        if (el) el.style.display = 'none';
        this._ghost = null;
        // Any ghost-hiding path (caret move, Escape, blur, recompute) also cancels
        // a pending idle reveal; the input handler re-arms it when appropriate.
        this._clearIdle();
    },

    _acceptGhost() {
        const g = this._ghost;
        if (!g) return false;
        const ta = g.anchor;
        const value = ta.value;
        // Caret is at the end (ghost invariant); append the suffix there. A scaffold
        // (e.g. case { | }) may pull the caret back inside via the item's caretBack.
        ta.value = value + g.suffix;
        const caret = ta.value.length - ((g.item && g.item.caretBack) || 0);
        ta.setSelectionRange(caret, caret);
        ta.focus();
        this._hideGhost();
        this._rehighlight(ta);
        ta.dispatchEvent(new Event('input', { bubbles: true }));
        return true;
    },

    // ===================== on-demand menu (Ctrl+Space) =====================

    _openMenu(textarea) {
        this._hideGhost();
        if (document.activeElement !== textarea) return;
        const ctx = this._computeContext(textarea);
        const items = this._buildItems(ctx);
        if (!items.length) return;
        this._ctx = ctx;
        this._items = items;
        this._selected = 0;
        this._acAnchor = textarea;
        this._renderDropdown(textarea);
    },

    _refilterMenu(textarea) {
        const ctx = this._computeContext(textarea);
        // Once the caret reaches a spot with nothing to complete (e.g. right after
        // a closing ")" or a finished value), close the menu so Enter runs the
        // query instead of accepting a stray suggestion from the full list.
        if (!this._isCompletable(ctx)) { this._hideMenu(); return; }
        const items = this._buildItems(ctx);
        if (!items.length) { this._hideMenu(); return; }
        this._ctx = ctx;
        this._items = items;
        this._selected = 0;
        this._acAnchor = textarea;
        this._renderDropdown(textarea);
    },

    _ensureDropdown() {
        let el = document.getElementById('bqlAutocomplete');
        if (!el) {
            el = document.createElement('div');
            el.id = 'bqlAutocomplete';
            el.className = 'ac-dropdown';
            el.style.display = 'none';
            document.body.appendChild(el);
        }
        return el;
    },

    _iconFor(kind) {
        switch (kind) {
            case 'function': return { glyph: 'ƒ', cls: 'ac-icon-fn' };
            case 'field': return { glyph: '#', cls: 'ac-icon-field' };
            case 'value': return { glyph: '"', cls: 'ac-icon-value' };
            case 'keyword': return { glyph: '&&', cls: 'ac-icon-kw' };
            default: return { glyph: '*', cls: '' };
        }
    },

    _highlightMatch(label, index, len) {
        const esc = (s) => this._escapeHtml(s);
        if (len <= 0 || index < 0) return esc(label);
        return esc(label.slice(0, index)) +
            '<b>' + esc(label.slice(index, index + len)) + '</b>' +
            esc(label.slice(index + len));
    },

    _renderDropdown(textarea) {
        const el = this._ensureDropdown();
        const rows = this._items.map((it, i) => {
            const icon = this._iconFor(it.kind);
            const sel = i === this._selected ? ' selected' : '';
            const desc = it.desc ? '<span class="ac-desc">' + this._escapeHtml(it.desc) + '</span>' : '';
            const detail = it.detail ? '<span class="ac-detail">' + this._escapeHtml(it.detail) + '</span>' : '';
            return '<div class="ac-item' + sel + '" data-i="' + i + '">' +
                '<span class="ac-icon ' + icon.cls + '">' + icon.glyph + '</span>' +
                '<span class="ac-label">' + this._highlightMatch(it.label, it.matchIndex, it.matchLen) + '</span>' +
                desc + detail +
                '</div>';
        }).join('');
        el.innerHTML = rows;

        // Bind mouse interactions (mousedown keeps the textarea focused).
        el.querySelectorAll('.ac-item').forEach(row => {
            const idx = parseInt(row.dataset.i, 10);
            row.addEventListener('mousedown', (e) => {
                e.preventDefault();
                this._accept(idx);
            });
            row.addEventListener('mouseenter', () => {
                this._selected = idx;
                this._syncSelection();
            });
        });

        this._position(el, textarea);
        el.style.display = 'block';
        this._menuVisible = true;
        this._scrollSelectedIntoView();
    },

    _position(el, textarea) {
        const caret = this._caretCoords(textarea, textarea.selectionStart);
        // Measure to flip above the caret if it would overflow the viewport.
        el.style.visibility = 'hidden';
        el.style.display = 'block';
        el.style.top = '0px';
        el.style.left = '0px';
        const h = el.offsetHeight;
        const w = el.offsetWidth;

        let top = caret.top + caret.lineHeight + 2;
        if (top + h > window.innerHeight - 8) {
            const above = caret.top - h - 2;
            top = above > 8 ? above : Math.max(8, window.innerHeight - h - 8);
        }
        let left = caret.left;
        if (left + w > window.innerWidth - 8) left = Math.max(8, window.innerWidth - w - 8);

        el.style.top = top + 'px';
        el.style.left = left + 'px';
        el.style.visibility = 'visible';
    },

    // Caret pixel position within a textarea via a mirrored, identically-styled div.
    _caretCoords(textarea, pos) {
        const computed = window.getComputedStyle(textarea);
        let div = this._mirror;
        if (!div) {
            div = document.createElement('div');
            this._mirror = div;
            document.body.appendChild(div);
        }
        const s = div.style;
        s.position = 'absolute';
        s.visibility = 'hidden';
        s.whiteSpace = 'pre-wrap';
        s.wordWrap = 'break-word';
        s.overflow = 'hidden';
        s.top = '0';
        s.left = '-9999px';
        const props = ['paddingTop', 'paddingRight', 'paddingBottom', 'paddingLeft',
            'borderTopWidth', 'borderRightWidth', 'borderBottomWidth', 'borderLeftWidth',
            'fontFamily', 'fontSize', 'fontWeight', 'fontStyle', 'letterSpacing', 'lineHeight',
            'textTransform', 'wordSpacing', 'tabSize'];
        props.forEach(p => { s[p] = computed[p]; });
        // Match the textarea's outer box exactly so wrapping (and thus the caret's
        // line) lines up; border-box + offsetWidth avoids padding/border drift.
        s.boxSizing = 'border-box';
        s.width = textarea.offsetWidth + 'px';

        div.textContent = textarea.value.substring(0, pos);
        const marker = document.createElement('span');
        marker.textContent = textarea.value.substring(pos) || '.';
        div.appendChild(marker);

        const rect = textarea.getBoundingClientRect();
        const lineHeight = parseFloat(computed.lineHeight) || (parseFloat(computed.fontSize) * 1.4);
        const top = rect.top + marker.offsetTop - textarea.scrollTop;
        const left = rect.left + marker.offsetLeft - textarea.scrollLeft;
        div.removeChild(marker);
        return { top, left, lineHeight };
    },

    _move(delta) {
        if (!this._items.length) return;
        const n = this._items.length;
        this._selected = (this._selected + delta + n) % n;
        this._syncSelection();
        this._scrollSelectedIntoView();
    },

    _syncSelection() {
        const el = document.getElementById('bqlAutocomplete');
        if (!el) return;
        el.querySelectorAll('.ac-item').forEach((row, i) => {
            row.classList.toggle('selected', i === this._selected);
        });
    },

    _scrollSelectedIntoView() {
        const el = document.getElementById('bqlAutocomplete');
        if (!el) return;
        const row = el.querySelectorAll('.ac-item')[this._selected];
        if (row) row.scrollIntoView({ block: 'nearest' });
    },

    _hideMenu() {
        const el = document.getElementById('bqlAutocomplete');
        if (el) el.style.display = 'none';
        this._menuVisible = false;
        this._items = [];
        this._ctx = null;
    },

    _accept(index) {
        const item = this._items[index];
        const ctx = this._ctx;
        const ta = this._acAnchor;
        if (!item || !ctx || !ta) { this._hideMenu(); return; }

        const value = ta.value;
        const before = value.substring(0, ctx.start);
        const after = value.substring(ctx.end);
        const caret = before.length + item.insert.length - (item.caretBack || 0);

        ta.value = before + item.insert + after;
        ta.setSelectionRange(caret, caret);
        ta.focus();

        this._hideMenu();
        this._rehighlight(ta);
        // Let editors that track their own state (history, height, model query)
        // observe the change; the ghost will recompute for the next token.
        ta.dispatchEvent(new Event('input', { bubbles: true }));
    },

    // Re-render the syntax-highlight overlay for whichever editor was completed.
    _rehighlight(input) {
        if (!window.SyntaxHighlight) return;
        const id = input.id || '';
        if (id === 'queryInput') { SyntaxHighlight.updateHighlight('queryInput', 'queryHighlight'); return; }
        if (id === 'editorQueryInput') { SyntaxHighlight.updateHighlight('editorQueryInput', 'alertQueryHighlight'); return; }

        let hl = null;
        if (id === 'modelQueryInput') hl = document.getElementById('modelQueryHighlight');
        else if (id.startsWith('wie-q-')) hl = document.getElementById(id.replace('wie-q-', 'wie-h-'));
        else if (id.startsWith('edit-content-')) hl = document.getElementById(id.replace('edit-content-', 'edit-highlight-'));
        else {
            const wrapper = input.closest('.query-input-wrapper');
            if (wrapper) hl = wrapper.querySelector('.query-highlight');
        }
        if (hl) {
            hl.innerHTML = SyntaxHighlight.highlight(input.value, SyntaxHighlight.errorRanges[id]) + '<br/>';
            hl.scrollTop = input.scrollTop;
            hl.scrollLeft = input.scrollLeft;
        }
    },

    // Open the menu on Ctrl+Space. (Not Cmd+Space -- that is macOS Spotlight.)
    _isMenuTrigger(e) {
        return e.ctrlKey && !e.metaKey && !e.altKey && (e.code === 'Space' || e.key === ' ');
    },

    _onKeyDownCapture(e) {
        // ----- browsable menu is open: it owns navigation/accept -----
        if (this._menuVisible) {
            if (e.target !== this._acAnchor) return;
            switch (e.key) {
                case 'ArrowDown':
                    e.preventDefault(); e.stopImmediatePropagation(); this._move(1); break;
                case 'ArrowUp':
                    e.preventDefault(); e.stopImmediatePropagation(); this._move(-1); break;
                case 'Enter':
                case 'Tab':
                    if (this._items.length) {
                        e.preventDefault(); e.stopImmediatePropagation();
                        this._accept(this._selected);
                    }
                    break;
                case 'Escape':
                    e.preventDefault(); e.stopImmediatePropagation(); this._hideMenu(); break;
                case 'ArrowLeft':
                case 'ArrowRight':
                case 'Home':
                case 'End':
                case 'PageUp':
                case 'PageDown':
                    // Caret moves without firing input, leaving a stale range; close.
                    this._hideMenu();
                    break;
                default:
                    break;
            }
            return;
        }

        // ----- menu closed: ghost text + Ctrl+Space to summon the menu -----
        if (e.target.tagName === 'TEXTAREA' && this._isQueryTextarea(e.target) && this._isMenuTrigger(e)) {
            e.preventDefault(); e.stopImmediatePropagation();
            this._openMenu(e.target);
            return;
        }

        if (this._ghost && e.target === this._acAnchor) {
            switch (e.key) {
                case 'Tab':
                    // Accept the ghost. (Enter is deliberately left alone so it
                    // always runs the query.)
                    e.preventDefault(); e.stopImmediatePropagation(); this._acceptGhost(); return;
                case 'ArrowRight':
                    // At end-of-input only -- accept like a shell autosuggestion.
                    if (this._acAnchor.selectionStart === this._acAnchor.value.length) {
                        e.preventDefault(); e.stopImmediatePropagation(); this._acceptGhost();
                    } else {
                        this._hideGhost();
                    }
                    return;
                case 'Escape':
                    e.preventDefault(); e.stopImmediatePropagation(); this._hideGhost(); return;
                case 'ArrowLeft':
                case 'ArrowUp':
                case 'ArrowDown':
                case 'Home':
                case 'End':
                case 'PageUp':
                case 'PageDown':
                    this._hideGhost(); return; // caret leaving the end invalidates it
                default:
                    return; // printable keys fall through; input recomputes the ghost
            }
        }

        if (e.key === 'Escape' && this._hintVisible) this.hideHint();
    },
};

// Make globally available
window.Autocomplete = Autocomplete;
