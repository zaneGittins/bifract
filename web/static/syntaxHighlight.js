// BQL query syntax highlighting
const SyntaxHighlight = {
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

        const highlighted = this.highlight(text);

        queryHighlight.innerHTML = highlighted + '<br/>';
        this.syncScroll(inputId, highlightId);
        this.syncHeight(inputId, highlightId);
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

    highlight(text) {
        if (!text) return '';

        // Split into lines first
        const lines = text.split('\n');

        return lines.map(line => {
            if (!line.trim()) return this.escapeHtml(line);

            // Check for comments first
            if (/^\s*\/\//.test(line)) {
                return `<span class="hl-comment">${this.escapeHtml(line)}</span>`;
            }

            // Process the line character by character to avoid HTML corruption
            return this.highlightLine(line);
        }).join('\n');
    },

    escapeHtml(text) {
        return text.replace(/&/g, '&amp;')
                   .replace(/</g, '&lt;')
                   .replace(/>/g, '&gt;')
                   .replace(/"/g, '&quot;')
                   .replace(/'/g, '&#039;');
    },

    highlightLine(line) {
        const result = [];
        let i = 0;

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
            // Check for line comments (//)
            else if (line[i] === '/' && line[i + 1] === '/') {
                result.push(`<span class="hl-comment">${this.escapeHtml(line.substring(i))}</span>`);
                i = line.length;
                matched = true;
            }
            // Check for regex patterns /pattern/flags
            else if (line[i] === '/') {
                const regexMatch = line.substring(i).match(/^\/(?:[^\/\n\\]|\\.)+\/[gimsu]*/);
                if (regexMatch) {
                    result.push(`<span class="hl-regex">${this.escapeHtml(regexMatch[0])}</span>`);
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
                    result.push(`<span class="hl-keyword">${this.escapeHtml(ident)}</span>`);
                } else if (/^\s*\(/.test(rest) || ident.toLowerCase() === 'case') {
                    // Function call: an identifier immediately followed by "(" (mirrors
                    // the backend lexer), plus the brace-style `case` command.
                    // Intentionally not tied to a hardcoded list so new/renamed
                    // functions highlight automatically.
                    result.push(`<span class="hl-function">${this.escapeHtml(ident)}</span>`);
                } else if (/^\s*(?:!=|>=|<=|=~|=\^|=\$|=|>|<)/.test(rest)) {
                    // Field name (word before a comparison operator)
                    result.push(`<span class="hl-field">${this.escapeHtml(ident)}</span>`);
                } else if (/^(?:true|false)$/i.test(ident)) {
                    result.push(`<span class="hl-boolean">${this.escapeHtml(ident)}</span>`);
                } else {
                    // Bare identifier (values in =~/=^/=$ lists, or plain values after =)
                    result.push(`<span class="hl-string">${this.escapeHtml(ident)}</span>`);
                }
                i += ident.length;
                matched = true;
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
            // Check for multi-character operators first
            else if (line.substring(i, i + 2) === ':=') {
                result.push(`<span class="hl-assignment">:=</span>`);
                i += 2;
                matched = true;
            } else if (line.substring(i, i + 2) === '!=' ||
                       line.substring(i, i + 2) === '<=' ||
                       line.substring(i, i + 2) === '>=' ||
                       line.substring(i, i + 2) === '=~' ||
                       line.substring(i, i + 2) === '=^' ||
                       line.substring(i, i + 2) === '=$') {
                result.push(`<span class="hl-operator">${this.escapeHtml(line.substring(i, i + 2))}</span>`);
                i += 2;
                matched = true;
            }
            // Single character tokens
            else if (line[i] === '|') {
                result.push(`<span class="hl-pipe">|</span>`);
                i++;
                matched = true;
            } else if (line[i] === '=' || line[i] === '<' || line[i] === '>') {
                result.push(`<span class="hl-operator">${this.escapeHtml(line[i])}</span>`);
                i++;
                matched = true;
            } else if (line[i] === '*') {
                result.push(`<span class="hl-wildcard">*</span>`);
                i++;
                matched = true;
            } else if (/[\[\](){}]/.test(line[i])) {
                result.push(`<span class="hl-bracket">${this.escapeHtml(line[i])}</span>`);
                i++;
                matched = true;
            }

            if (!matched) {
                // Wrap unhighlighted text in default spans so it's visible
                result.push(`<span class="hl-default">${this.escapeHtml(line[i])}</span>`);
                i++;
            }
        }

        return result.join('');
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
