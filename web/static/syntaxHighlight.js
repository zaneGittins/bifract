// BQL query syntax highlighting
const SyntaxHighlight = {
    // Per-input error spans for underlining, keyed by input element id.
    // Each value is {start, end} in rune (code point) offsets, or absent.
    errorRanges: {},

    init() {
        // Initialize main search query input
        this.initializeQueryInput('queryInput', 'queryHighlight');

        // Initialize alert editor query input
        this.initializeQueryInput('editorQueryInput', 'alertQueryHighlight');
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

        const highlighted = this.highlight(text, this.errorRanges[inputId]);

        queryHighlight.innerHTML = highlighted + '<br/>';
        this.syncScroll(inputId, highlightId);
        this.syncHeight(inputId, highlightId);
    },

    // setError underlines the rune span [start, end) in the given input and
    // re-renders. A zero-width span (start === end) renders as a caret marker.
    // Pass a falsy range to clear.
    setError(inputId, highlightId, range) {
        if (range && Number.isInteger(range.start) && Number.isInteger(range.end) && range.end >= range.start) {
            this.errorRanges[inputId] = { start: range.start, end: range.end };
        } else {
            delete this.errorRanges[inputId];
        }
        this.updateHighlight(inputId, highlightId);
    },

    clearError(inputId, highlightId) {
        if (this.errorRanges[inputId] === undefined) return;
        delete this.errorRanges[inputId];
        if (highlightId) this.updateHighlight(inputId, highlightId);
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

    highlight(text, errorRange) {
        if (!text) return '';

        // Split into lines first
        const lines = text.split('\n');
        const hasErr = errorRange && Number.isInteger(errorRange.start) && Number.isInteger(errorRange.end);

        // Track the code-point offset of each line start so a global error span
        // (rune offsets from the backend) can be mapped to line-local offsets.
        let cpOffset = 0;

        return lines.map(line => {
            const lineLen = [...line].length;
            const lineStart = cpOffset;
            const lineEnd = cpOffset + lineLen;
            cpOffset = lineEnd + 1; // +1 for the '\n' that split() removed

            const local = hasErr ? this.localErrorRange(errorRange, lineStart, lineEnd, lineLen) : null;

            // Blank/whitespace-only lines have no glyphs to underline.
            if (!line.trim()) return this.escapeHtml(line);

            // Check for comments first
            if (/^\s*\/\//.test(line)) {
                return this.renderSegments([{ t: line, c: 'hl-comment' }], local);
            }

            // Process the line character by character to avoid HTML corruption
            return this.renderSegments(this.highlightLine(line), local);
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
    // that falls inside the error range (line-local code-point offsets) with the
    // hl-error class. Segments that straddle the error boundary are split.
    renderSegments(segments, err) {
        let out = '';
        let pos = 0;
        for (const seg of segments) {
            const cps = [...seg.t];
            const segStart = pos;
            const segEnd = pos + cps.length;
            pos = segEnd;

            if (!err || err.end <= segStart || err.start >= segEnd) {
                out += this.wrapSeg(seg.t, seg.c, false);
                continue;
            }
            const a = Math.max(segStart, err.start);
            const b = Math.min(segEnd, err.end);
            if (a > segStart) out += this.wrapSeg(cps.slice(0, a - segStart).join(''), seg.c, false);
            out += this.wrapSeg(cps.slice(a - segStart, b - segStart).join(''), seg.c, true);
            if (b < segEnd) out += this.wrapSeg(cps.slice(b - segStart).join(''), seg.c, false);
        }
        return out;
    },

    wrapSeg(text, cls, isErr) {
        if (text === '') return '';
        const c = isErr ? (cls ? cls + ' hl-error' : 'hl-error') : cls;
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
