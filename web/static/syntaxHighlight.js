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

        const lineCount = (queryInput.value.match(/\n/g) || []).length + 1;
        const computedStyle = window.getComputedStyle(queryInput);
        const lineHeight = parseFloat(computedStyle.lineHeight);
        const paddingTop = parseFloat(computedStyle.paddingTop);
        const paddingBottom = parseFloat(computedStyle.paddingBottom);
        const borderTop = parseFloat(computedStyle.borderTopWidth);
        const borderBottom = parseFloat(computedStyle.borderBottomWidth);

        const contentHeight = lineCount * lineHeight + paddingTop + paddingBottom + borderTop + borderBottom;
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
            // Check for regex patterns /pattern/flags
            else if (line[i] === '/') {
                const regexMatch = line.substring(i).match(/^\/(?:[^\/\n\\]|\\.)+\/[gimsu]*/);
                if (regexMatch) {
                    result.push(`<span class="hl-regex">${this.escapeHtml(regexMatch[0])}</span>`);
                    i += regexMatch[0].length;
                    matched = true;
                }
            }
            // Check for functions
            else if (/[a-zA-Z_]/.test(line[i])) {
                const funcMatch = line.substring(i).match(/^(groupBy|count|sum|avg|max|min|percentile|stdDev|median|mad|multi|sort|limit|head|tail|table|regex|replace|concat|lowercase|lower|now|timeChart|timechart|singleval|case|eval|in|piechart|barchart|graph|bucket|match|selectfirst|selectlast|uppercase|chain|bfs|dfs|len|levenshtein|base64Decode|dedup|cidr|split|substr|urldecode|coalesce|hash|comment|collect|top|sprintf|strftime|skewness|kurtosis|frequency|modifiedZScore|madOutlier|iqr|headTail|analyzeFields|histogram|heatmap|lookupIP|lookupip|geoip|graphWorld|graphworld|worldmap|join)(?=\s*\()/i);
                if (funcMatch) {
                    result.push(`<span class="hl-function">${this.escapeHtml(funcMatch[0])}</span>`);
                    i += funcMatch[0].length;
                    matched = true;
                } else {
                    // Check for field names (word before =)
                    const fieldMatch = line.substring(i).match(/^[a-zA-Z_][a-zA-Z0-9_.]*(?=\s*=)/);
                    if (fieldMatch) {
                        result.push(`<span class="hl-field">${this.escapeHtml(fieldMatch[0])}</span>`);
                        i += fieldMatch[0].length;
                        matched = true;
                    } else {
                        // Check for boolean values
                        const boolMatch = line.substring(i).match(/^(true|false)\b/i);
                        if (boolMatch) {
                            result.push(`<span class="hl-boolean">${this.escapeHtml(boolMatch[0])}</span>`);
                            i += boolMatch[0].length;
                            matched = true;
                        } else {
                            // Check for keywords
                            const keywordMatch = line.substring(i).match(/^(AND|OR|NOT)\b/i);
                            if (keywordMatch) {
                                result.push(`<span class="hl-keyword">${this.escapeHtml(keywordMatch[0])}</span>`);
                                i += keywordMatch[0].length;
                                matched = true;
                            }
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
            // Check for multi-character operators first
            else if (line.substring(i, i + 2) === ':=') {
                result.push(`<span class="hl-assignment">:=</span>`);
                i += 2;
                matched = true;
            } else if (line.substring(i, i + 2) === '!=' ||
                       line.substring(i, i + 2) === '<=' ||
                       line.substring(i, i + 2) === '>=') {
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
