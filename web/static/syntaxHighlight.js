// BQL query syntax highlighting
const SyntaxHighlight = {
    // Per-input error spans for underlining, keyed by input element id.
    // Each value is {start, end} in rune (code point) offsets, or absent.
    errorRanges: {},

    // Per-input matched-bracket pair {a, b} (two code-point offsets), keyed by
    // input id. Ephemeral: recomputed on every caret move, cleared on blur.
    matchRanges: {},

    init() {
        // Initialize main search query input
        this.initializeQueryInput('queryInput', 'queryHighlight');

        // Initialize alert editor query input
        this.initializeQueryInput('editorQueryInput', 'alertQueryHighlight');

        this._initBracketMatching();
    },

    // _initBracketMatching wires a single document-level caret listener that
    // highlights the bracket pair surrounding the caret in whichever query
    // editor is focused. selectionchange fires for the focused textarea on both
    // caret movement and typing, so one listener covers every editor (search,
    // alerts, notebooks, dashboards, models) with no per-editor wiring.
    _initBracketMatching() {
        if (this._bracketInit) return;
        this._bracketInit = true;
        document.addEventListener('selectionchange', () => {
            const el = document.activeElement;
            if (!el || el.tagName !== 'TEXTAREA' || !this._highlightElFor(el)) return;
            if (this.refreshMatch(el)) this.repaintEditor(el);
        });
        // Clear the pair highlight when an editor loses focus.
        document.addEventListener('focusout', (e) => {
            const el = e.target;
            if (!el || !el.id || this.matchRanges[el.id] === undefined) return;
            delete this.matchRanges[el.id];
            this.repaintEditor(el);
        });
    },

    initializeQueryInput(inputId, highlightId) {
        const queryInput = document.getElementById(inputId);
        const queryHighlight = document.getElementById(highlightId);

        if (!queryInput || !queryHighlight) return;

        // Sync scroll and content
        queryInput.addEventListener('input', () => this.updateHighlight(inputId, highlightId));
        queryInput.addEventListener('scroll', () => this.syncScroll(inputId, highlightId));

        // Initial highlight
        this.updateHighlight(inputId, highlightId);
    },

    // Backward compatibility - update main query input
    update() {
        this.updateHighlight('queryInput', 'queryHighlight');
    },

    updateHighlight(inputId, highlightId) {
        const queryInput = document.getElementById(inputId);
        const queryHighlight = document.getElementById(highlightId);

        if (!queryInput || !queryHighlight) return;

        const text = queryInput.value;

        const highlighted = this.highlight(text, this.errorRanges[inputId], this.matchRanges[inputId]);

        queryHighlight.innerHTML = highlighted + '<br/>';
        this.syncScroll(inputId, highlightId);
        this.syncHeight(inputId, highlightId);
    },

    // setErrorRange stores (or clears) the error span for an input WITHOUT
    // repainting. It stamps the exact query text the span was computed against;
    // highlight() only paints the underline while the text still matches, so a
    // stale span can never land on unrelated text after a programmatic .value
    // change (e.g. switching alerts or reopening an editor). Pass a falsy range
    // to clear.
    setErrorRange(inputId, range) {
        if (range && Number.isInteger(range.start) && Number.isInteger(range.end) && range.end >= range.start) {
            const input = document.getElementById(inputId);
            this.errorRanges[inputId] = { start: range.start, end: range.end, query: input ? input.value : null };
        } else {
            delete this.errorRanges[inputId];
        }
    },

    // setError stores the span and repaints. A zero-width span (start === end)
    // renders as a caret marker.
    setError(inputId, highlightId, range) {
        this.setErrorRange(inputId, range);
        this.updateHighlight(inputId, highlightId);
    },

    clearError(inputId, highlightId) {
        if (this.errorRanges[inputId] === undefined) return;
        delete this.errorRanges[inputId];
        if (highlightId) this.updateHighlight(inputId, highlightId);
    },

    // _highlightElFor returns the highlight overlay element paired with a query
    // textarea, or null if the element is not a query editor we manage. This is
    // the single mapping from textarea -> overlay used by caret-driven repaints.
    _highlightElFor(input) {
        const id = (input && input.id) || '';
        if (id === 'queryInput') return document.getElementById('queryHighlight');
        if (id === 'editorQueryInput') return document.getElementById('alertQueryHighlight');
        if (id === 'modelQueryInput') return document.getElementById('modelQueryHighlight');
        if (id.startsWith('wie-q-')) return document.getElementById(id.replace('wie-q-', 'wie-h-'));
        if (id.startsWith('edit-content-')) return document.getElementById(id.replace('edit-content-', 'edit-highlight-'));
        const w = input && input.closest ? input.closest('.query-input-wrapper') : null;
        return w ? w.querySelector('.query-highlight') : null;
    },

    // repaintEditor re-renders an editor's overlay with its current error and
    // bracket-match decorations.
    repaintEditor(input) {
        const hl = this._highlightElFor(input);
        if (!hl) return;
        const id = input.id || '';
        hl.innerHTML = this.highlight(input.value, this.errorRanges[id], this.matchRanges[id]) + '<br/>';
        hl.scrollTop = input.scrollTop;
        hl.scrollLeft = input.scrollLeft;
    },

    // refreshMatch recomputes the bracket pair around the caret for an editor.
    // Returns true when the highlighted pair changed (so the caller repaints).
    refreshMatch(input) {
        const id = input.id || '';
        if (!this._highlightElFor(input)) return false;
        let pair = null;
        // Only for a collapsed caret (not an active selection).
        if (input.selectionStart === input.selectionEnd) {
            const caret = [...input.value.slice(0, input.selectionStart)].length; // code-point offset
            pair = this._findMatchPair(input.value, caret);
        }
        const prev = this.matchRanges[id];
        const key = p => (p ? p.a + ':' + p.b : '');
        if (key(prev) === key(pair)) return false;
        if (pair) this.matchRanges[id] = pair;
        else delete this.matchRanges[id];
        return true;
    },

    // _bracketSegments returns every bracket character in the text as
    // {pos, ch} with absolute code-point offsets, reusing the tokenizer so that
    // brackets inside strings/regex (which are not hl-bracket segments) are
    // naturally excluded.
    _bracketSegments(text) {
        const lines = text.split('\n');
        let off = 0;
        const out = [];
        for (const line of lines) {
            if (line.trim() && !/^\s*\/\//.test(line)) {
                let p = 0;
                for (const seg of this.highlightLine(line)) {
                    const len = [...seg.t].length;
                    if (seg.c === 'hl-bracket' && len === 1) out.push({ pos: off + p, ch: seg.t });
                    p += len;
                }
            }
            off += [...line].length + 1; // +1 for the stripped '\n'
        }
        return out;
    },

    // _pairMap builds a bidirectional index->index map of matched brackets via a
    // typed stack. Mismatched/unbalanced brackets are simply absent from the map.
    _pairMap(brackets) {
        const opensTo = { '(': ')', '[': ']', '{': '}' };
        const closeTo = { ')': '(', ']': '[', '}': '{' };
        const stack = [];
        const map = {};
        for (let k = 0; k < brackets.length; k++) {
            const ch = brackets[k].ch;
            if (opensTo[ch]) {
                stack.push(k);
            } else if (closeTo[ch] && stack.length) {
                const top = stack[stack.length - 1];
                if (brackets[top].ch === closeTo[ch]) {
                    stack.pop();
                    map[top] = k;
                    map[k] = top;
                }
                // A closer whose top-of-stack type differs is left unmatched.
            }
        }
        return map;
    },

    // _findMatchPair returns {a, b} code-point offsets of the bracket adjacent to
    // the caret and its partner, or null. A bracket just left of the caret takes
    // precedence over one just right (matches common editor behavior).
    _findMatchPair(text, caret) {
        const brackets = this._bracketSegments(text);
        if (!brackets.length) return null;
        let idx = -1;
        for (let k = 0; k < brackets.length; k++) {
            if (brackets[k].pos === caret - 1) { idx = k; break; }
        }
        if (idx === -1) {
            for (let k = 0; k < brackets.length; k++) {
                if (brackets[k].pos === caret) { idx = k; break; }
            }
        }
        if (idx === -1) return null;
        const map = this._pairMap(brackets);
        if (!(idx in map)) return null;
        return { a: brackets[idx].pos, b: brackets[map[idx]].pos };
    },

    syncScroll(inputId, highlightId) {
        const queryInput = document.getElementById(inputId || 'queryInput');
        const queryHighlight = document.getElementById(highlightId || 'queryHighlight');

        if (!queryInput || !queryHighlight) return;

        queryHighlight.scrollTop = queryInput.scrollTop;
        queryHighlight.scrollLeft = queryInput.scrollLeft;
    },

    syncHeight(inputId, highlightId) {
        const queryInput = document.getElementById(inputId || 'queryInput');
        const queryHighlight = document.getElementById(highlightId || 'queryHighlight');

        if (!queryInput || !queryHighlight) return;

        // Measure the real rendered content height (scrollHeight) so the box grows
        // for visually wrapped lines, not just explicit newlines. Resetting height
        // first lets it both grow and shrink.
        const computedStyle = window.getComputedStyle(queryInput);
        const borderTop = parseFloat(computedStyle.borderTopWidth) || 0;
        const borderBottom = parseFloat(computedStyle.borderBottomWidth) || 0;

        const prevHeight = queryInput.style.height;
        queryInput.style.height = 'auto';
        const contentHeight = queryInput.scrollHeight + borderTop + borderBottom;
        queryInput.style.height = prevHeight;

        const height = Math.max(38, Math.min(contentHeight, 400));

        // Respect manually dragged height (stored as minHeight by resize handle)
        const manualMin = parseFloat(queryInput.style.minHeight) || 0;
        const finalHeight = Math.max(height, manualMin);

        queryInput.style.height = finalHeight + 'px';
        queryHighlight.style.height = finalHeight + 'px';

        const wrapper = queryInput.parentElement;
        if (wrapper && wrapper.classList.contains('query-input-wrapper')) {
            wrapper.style.height = finalHeight + 'px';
        }
    },

    highlight(text, errorRange, matchPair) {
        if (!text) return '';

        // Split into lines first
        const lines = text.split('\n');
        // Only apply a span that was computed against this exact text. A stamped
        // query that no longer matches means the text changed out from under the
        // span (e.g. a programmatic load), so the underline is stale -> skip it.
        const hasErr = errorRange && Number.isInteger(errorRange.start) && Number.isInteger(errorRange.end)
            && (errorRange.query == null || errorRange.query === text);
        const matchPositions = (matchPair && Number.isInteger(matchPair.a) && Number.isInteger(matchPair.b))
            ? [matchPair.a, matchPair.b] : null;

        // Track the code-point offset of each line start so a global error span
        // (rune offsets from the backend) can be mapped to line-local offsets.
        let cpOffset = 0;

        return lines.map(line => {
            const lineLen = [...line].length;
            const lineStart = cpOffset;
            const lineEnd = cpOffset + lineLen;
            cpOffset = lineEnd + 1; // +1 for the '\n' that split() removed

            const local = hasErr ? this.localErrorRange(errorRange, lineStart, lineEnd, lineLen) : null;
            let matchSet = null;
            if (matchPositions) {
                const locals = matchPositions.filter(p => p >= lineStart && p < lineEnd).map(p => p - lineStart);
                if (locals.length) matchSet = new Set(locals);
            }

            // Blank/whitespace-only lines have no glyphs to underline.
            if (!line.trim()) return this.escapeHtml(line);

            // Check for comments first
            if (/^\s*\/\//.test(line)) {
                return this.renderSegments([{ t: line, c: 'hl-comment' }], local, matchSet);
            }

            // Process the line character by character to avoid HTML corruption
            return this.renderSegments(this.highlightLine(line), local, matchSet);
        }).join('\n');
    },

    // localErrorRange maps a global rune span to line-local code-point offsets,
    // returning null when the error does not touch this line. A zero-width span
    // (caret) is expanded to a single-glyph marker so it is visible.
    localErrorRange(err, lineStart, lineEnd, lineLen) {
        if (err.end > err.start) {
            if (err.start >= lineEnd || err.end <= lineStart) return null;
            return {
                start: Math.max(0, err.start - lineStart),
                end: Math.min(lineLen, err.end - lineStart),
            };
        }
        // Zero-width caret (e.g. unexpected end of input).
        const p = err.start;
        if (p < lineStart || p > lineEnd) return null;
        let s = p - lineStart;
        let e = s + 1;
        if (s >= lineLen) { // caret past the last glyph -> mark the last glyph
            s = Math.max(0, lineLen - 1);
            e = lineLen;
        }
        if (e <= s) return null;
        return { start: s, end: e };
    },

    // renderSegments emits HTML for the tokenized segments, wrapping the portion
    // inside the error range (line-local code-point offsets) with hl-error and
    // any single-char bracket segment whose start is in matchSet with hl-match.
    // Segments that straddle the error boundary are split.
    renderSegments(segments, err, matchSet) {
        let out = '';
        let pos = 0;
        for (const seg of segments) {
            const cps = [...seg.t];
            const segStart = pos;
            const segEnd = pos + cps.length;
            pos = segEnd;

            // Gate on hl-bracket (not just single-char) so a stale match offset
            // can never decorate an unrelated token like '=', '|' or '*'.
            const isMatch = !!(matchSet && seg.c === 'hl-bracket' && cps.length === 1 && matchSet.has(segStart));

            if (!err || err.end <= segStart || err.start >= segEnd) {
                out += this.wrapSeg(seg.t, seg.c, false, isMatch);
                continue;
            }
            const a = Math.max(segStart, err.start);
            const b = Math.min(segEnd, err.end);
            if (a > segStart) out += this.wrapSeg(cps.slice(0, a - segStart).join(''), seg.c, false, false);
            out += this.wrapSeg(cps.slice(a - segStart, b - segStart).join(''), seg.c, true, isMatch);
            if (b < segEnd) out += this.wrapSeg(cps.slice(b - segStart).join(''), seg.c, false, false);
        }
        return out;
    },

    wrapSeg(text, cls, isErr, isMatch) {
        if (text === '') return '';
        let c = cls || '';
        if (isErr) c = (c ? c + ' ' : '') + 'hl-error';
        if (isMatch) c = (c ? c + ' ' : '') + 'hl-match';
        return `<span class="${c}">${this.escapeHtml(text)}</span>`;
    },

    escapeHtml(text) {
        return text.replace(/&/g, '&amp;')
                   .replace(/</g, '&lt;')
                   .replace(/>/g, '&gt;')
                   .replace(/"/g, '&quot;')
                   .replace(/'/g, '&#039;');
    },

    // highlightLine tokenizes a single line into an array of {t, c} segments,
    // where t is the raw (unescaped) source text and c is the highlight class.
    // The concatenation of all segment texts equals the input line, so a caller
    // can map source offsets onto segments (see renderSegments).
    highlightLine(line) {
        const result = [];
        let i = 0;

        while (i < line.length) {
            let matched = false;

            // Check for strings first (highest priority)
            if (line[i] === '"') {
                const stringMatch = line.substring(i).match(/^"(?:[^"\\]|\\.)*"/);
                if (stringMatch) {
                    result.push({ t: stringMatch[0], c: 'hl-string' });
                    i += stringMatch[0].length;
                    matched = true;
                }
            } else if (line[i] === "'") {
                const stringMatch = line.substring(i).match(/^'(?:[^'\\]|\\.)*'/);
                if (stringMatch) {
                    result.push({ t: stringMatch[0], c: 'hl-string' });
                    i += stringMatch[0].length;
                    matched = true;
                }
            }
            // Check for line comments (//)
            else if (line[i] === '/' && line[i + 1] === '/') {
                result.push({ t: line.substring(i), c: 'hl-comment' });
                i = line.length;
                matched = true;
            }
            // Check for regex patterns /pattern/flags
            else if (line[i] === '/') {
                const regexMatch = line.substring(i).match(/^\/(?:[^\/\n\\]|\\.)+\/[gimsu]*/);
                if (regexMatch) {
                    result.push({ t: regexMatch[0], c: 'hl-regex' });
                    i += regexMatch[0].length;
                    matched = true;
                }
            }
            // Query variables: @name. Only when '@' is not preceded by a word char
            // (so user@host is left alone), mirroring the backend substitution
            // boundary rule. Inside strings the '@' is already consumed by the
            // string token above, so quoted text is never matched.
            else if (line[i] === '@' && !(i > 0 && /[a-zA-Z0-9_]/.test(line[i - 1]))) {
                const varMatch = line.substring(i).match(/^@[a-zA-Z_][a-zA-Z0-9_]*/);
                if (varMatch) {
                    result.push({ t: varMatch[0], c: 'hl-variable' });
                    i += varMatch[0].length;
                    matched = true;
                }
            }
            // Identifiers: functions, field names, booleans, keywords, or bare values
            else if (/[a-zA-Z_]/.test(line[i])) {
                const ident = line.substring(i).match(/^[a-zA-Z_][a-zA-Z0-9_.]*/)[0];
                const rest = line.substring(i + ident.length);

                // Reserved keywords first: AND/OR/NOT are never functions, so they
                // must win over the "identifier(" rule (e.g. NOT (a=1) is a keyword
                // before a group, not a function call).
                if (/^(?:AND|OR|NOT)$/i.test(ident)) {
                    result.push({ t: ident, c: 'hl-keyword' });
                } else if (/^\s*\(/.test(rest) || ident.toLowerCase() === 'case') {
                    // Function call: an identifier immediately followed by "(" (mirrors
                    // the backend lexer), plus the brace-style `case` command.
                    // Intentionally not tied to a hardcoded list so new/renamed
                    // functions highlight automatically.
                    result.push({ t: ident, c: 'hl-function' });
                } else if (/^\s*(?:!=|>=|<=|=~|=\^|=\$|=|>|<)/.test(rest)) {
                    // Field name (word before a comparison operator)
                    result.push({ t: ident, c: 'hl-field' });
                } else if (/^(?:true|false)$/i.test(ident)) {
                    result.push({ t: ident, c: 'hl-boolean' });
                } else {
                    // Bare identifier (values in =~/=^/=$ lists, or plain values after =)
                    result.push({ t: ident, c: 'hl-string' });
                }
                i += ident.length;
                matched = true;
            }
            // Check for numbers
            else if (/\d/.test(line[i])) {
                const numMatch = line.substring(i).match(/^\d+\.?\d*/);
                if (numMatch) {
                    result.push({ t: numMatch[0], c: 'hl-number' });
                    i += numMatch[0].length;
                    matched = true;
                }
            }
            // Check for multi-character operators first
            else if (line.substring(i, i + 2) === ':=') {
                result.push({ t: ':=', c: 'hl-assignment' });
                i += 2;
                matched = true;
            } else if (line.substring(i, i + 2) === '!=' ||
                       line.substring(i, i + 2) === '<=' ||
                       line.substring(i, i + 2) === '>=' ||
                       line.substring(i, i + 2) === '=~' ||
                       line.substring(i, i + 2) === '=^' ||
                       line.substring(i, i + 2) === '=$') {
                result.push({ t: line.substring(i, i + 2), c: 'hl-operator' });
                i += 2;
                matched = true;
            }
            // Single character tokens
            else if (line[i] === '|') {
                result.push({ t: '|', c: 'hl-pipe' });
                i++;
                matched = true;
            } else if (line[i] === '=' || line[i] === '<' || line[i] === '>') {
                result.push({ t: line[i], c: 'hl-operator' });
                i++;
                matched = true;
            } else if (line[i] === '*') {
                result.push({ t: '*', c: 'hl-wildcard' });
                i++;
                matched = true;
            } else if (/[\[\](){}]/.test(line[i])) {
                result.push({ t: line[i], c: 'hl-bracket' });
                i++;
                matched = true;
            }

            if (!matched) {
                // Wrap unhighlighted text in default segments so it stays visible
                result.push({ t: line[i], c: 'hl-default' });
                i++;
            }
        }

        return result;
    },

    highlightSQL(text) {
        if (!text) return '';

        const lines = text.split('\n');
        return lines.map(line => {
            if (!line.trim()) return this.escapeHtml(line);

            // Check for SQL comments
            if (/^\s*(--|\/\*|\*|\/\/)/.test(line)) {
                return `<span class="hl-comment">${this.escapeHtml(line)}</span>`;
            }

            return this.highlightSQLLine(line);
        }).join('\n');
    },

    highlightSQLLine(line) {
        const result = [];
        let i = 0;

        // SQL Keywords
        const sqlKeywords = [
            'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'ORDER BY', 'GROUP BY',
            'HAVING', 'LIMIT', 'OFFSET', 'AS', 'DISTINCT', 'COUNT', 'SUM', 'AVG',
            'MAX', 'MIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN',
            'ON', 'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL', 'ASC', 'DESC', 'CASE',
            'WHEN', 'THEN', 'ELSE', 'END', 'WITH', 'UNION', 'INTERSECT', 'EXCEPT'
        ];

        // SQL Functions (commonly used in ClickHouse)
        const sqlFunctions = [
            'formatDateTime', 'toString', 'toDate', 'position', 'match',
            'positionCaseInsensitive', 'length', 'substring', 'lower', 'upper',
            'trim', 'ltrim', 'rtrim', 'concat', 'replace', 'split', 'extract',
            'now', 'today', 'yesterday', 'toStartOfDay', 'toStartOfHour'
        ];

        while (i < line.length) {
            let matched = false;

            // Check for strings first (highest priority)
            if (line[i] === '"') {
                const stringMatch = line.substring(i).match(/^"(?:[^"\\]|\\.)*"/);
                if (stringMatch) {
                    result.push(`<span class="hl-string">${this.escapeHtml(stringMatch[0])}</span>`);
                    i += stringMatch[0].length;
                    matched = true;
                }
            } else if (line[i] === "'") {
                const stringMatch = line.substring(i).match(/^'(?:[^'\\]|\\.)*'/);
                if (stringMatch) {
                    result.push(`<span class="hl-string">${this.escapeHtml(stringMatch[0])}</span>`);
                    i += stringMatch[0].length;
                    matched = true;
                }
            }
            // Check for SQL keywords and functions
            else if (/[a-zA-Z_]/.test(line[i])) {
                // Try to match multi-word keywords first
                let foundKeyword = false;
                const remainingText = line.substring(i);

                for (const keyword of sqlKeywords.sort((a, b) => b.length - a.length)) {
                    if (remainingText.toUpperCase().startsWith(keyword) &&
                        (i + keyword.length >= line.length || !/[a-zA-Z0-9_]/.test(line[i + keyword.length]))) {
                        result.push(`<span class="hl-keyword">${this.escapeHtml(line.substring(i, i + keyword.length))}</span>`);
                        i += keyword.length;
                        matched = true;
                        foundKeyword = true;
                        break;
                    }
                }

                if (!foundKeyword) {
                    // Check for functions
                    const funcMatch = line.substring(i).match(/^([a-zA-Z_][a-zA-Z0-9_]*)(?=\s*\()/);
                    if (funcMatch) {
                        const funcName = funcMatch[1];
                        if (sqlFunctions.some(f => f.toLowerCase() === funcName.toLowerCase())) {
                            result.push(`<span class="hl-function">${this.escapeHtml(funcMatch[0])}</span>`);
                            i += funcMatch[0].length;
                            matched = true;
                        }
                    }

                    if (!matched) {
                        // Check for identifiers (table names, column names, etc.)
                        const identMatch = line.substring(i).match(/^[a-zA-Z_][a-zA-Z0-9_.]*/);
                        if (identMatch) {
                            result.push(`<span class="hl-field">${this.escapeHtml(identMatch[0])}</span>`);
                            i += identMatch[0].length;
                            matched = true;
                        }
                    }
                }
            }
            // Check for numbers
            else if (/\d/.test(line[i])) {
                const numMatch = line.substring(i).match(/^\d+\.?\d*/);
                if (numMatch) {
                    result.push(`<span class="hl-number">${this.escapeHtml(numMatch[0])}</span>`);
                    i += numMatch[0].length;
                    matched = true;
                }
            }
            // Check for operators
            else if (line.substring(i, i + 2) === ':=' ||
                     line.substring(i, i + 2) === '!=' ||
                     line.substring(i, i + 2) === '<=' ||
                     line.substring(i, i + 2) === '>=') {
                result.push(`<span class="hl-operator">${this.escapeHtml(line.substring(i, i + 2))}</span>`);
                i += 2;
                matched = true;
            }
            else if (/[=<>+\-*\/]/.test(line[i])) {
                result.push(`<span class="hl-operator">${this.escapeHtml(line[i])}</span>`);
                i++;
                matched = true;
            }
            // Check for brackets and parentheses
            else if (/[\[\](){}]/.test(line[i])) {
                result.push(`<span class="hl-bracket">${this.escapeHtml(line[i])}</span>`);
                i++;
                matched = true;
            }
            // Check for commas and semicolons
            else if (/[,;]/.test(line[i])) {
                result.push(`<span class="hl-operator">${this.escapeHtml(line[i])}</span>`);
                i++;
                matched = true;
            }

            if (!matched) {
                // Wrap unhighlighted text in default spans
                result.push(`<span class="hl-default">${this.escapeHtml(line[i])}</span>`);
                i++;
            }
        }

        return result.join('');
    }
};

// Make globally available
window.SyntaxHighlight = SyntaxHighlight;
