// Utility functions
const Utils = {
    // Keyboard shortcuts use Cmd on Apple platforms and Ctrl elsewhere.
    isMac: /Mac|iPhone|iPad/.test(navigator.platform),

    // Columns the server attaches to a result row for the detail panel's benefit
    // rather than for display: they are excluded from field_order, so they must
    // also be excluded wherever a column list is derived from row keys instead.
    HIDDEN_ROW_FIELDS: ['_ingest_timestamp'],

    // Filter a derived column list down to what a user should see or pick from.
    visibleFields(names) {
        return (names || []).filter(n => !Utils.HIDDEN_ROW_FIELDS.includes(n));
    },

    formatDateTimeLocal(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        return `${year}-${month}-${day} ${hours}:${minutes}`;
    },

    // Absolute timestamp in the user's display zone. A bare ClickHouse string
    // carries no zone and is UTC; TZ.toEpoch knows that, the native Date parser
    // does not.
    formatTimestamp(ts, style) {
        if (!ts) return '-';
        return TZ.format(ts, style || 'datetime') || '-';
    },

    // Hover text for a rendered timestamp: the zone it is shown in, plus the
    // UTC value when they differ.
    timestampTitle(ts) {
        return ts ? TZ.title(ts) : '';
    },

    // Compact relative age: "just now", "45s ago", "3m ago", "2d ago".
    // Accepts an ISO string, a Date, or epoch milliseconds. Returns '' for a
    // missing or unparseable value so callers choose their own fallback.
    timeAgo(when) {
        if (when === null || when === undefined || when === '') return '';
        const t = TZ.toEpoch(when);
        if (!Number.isFinite(t)) return '';

        const s = Math.max(0, Math.floor((Date.now() - t) / 1000));
        if (s < 10) return 'just now';
        if (s < 60) return `${s}s ago`;
        const m = Math.floor(s / 60);
        if (m < 60) return `${m}m ago`;
        const h = Math.floor(m / 60);
        if (h < 24) return `${h}h ago`;
        return `${Math.floor(h / 24)}d ago`;
    },

    getCurrentUTC() {
        return TZ.format(Date.now(), 'time');
    },

    respondJSON(w, status, data) {
        // Helper for potential future use
        return JSON.stringify(data);
    },

    // Debounce function to limit the rate of function calls
    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    },

    // Escape HTML to prevent XSS
    escapeHtml(text) {
        if (typeof text !== 'string') return text;
        const map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return text.replace(/[&<>"']/g, function(m) { return map[m]; });
    },

    // Escape for use in HTML attributes (data-*, title, etc.)
    escapeAttr(text) {
        if (typeof text !== 'string') return '';
        return text.replace(/&/g, '&amp;')
                   .replace(/"/g, '&quot;')
                   .replace(/'/g, '&#039;')
                   .replace(/</g, '&lt;')
                   .replace(/>/g, '&gt;');
    },

    // Deterministic per-tag color from a fixed palette. The same tag string
    // always maps to the same color, used for notebook section tags and alert
    // labels so chips are visually distinguishable and consistent app-wide.
    tagColorFor(tag) {
        // Shares the chart series palette so a label and a series of the same
        // name read as one colour language. Resolved lazily: pages that load
        // utils.js without charts.js (login) never call this.
        const palette = (window.BifractCharts && window.BifractCharts.SERIES_COLORS) || ['#9465d0'];
        let h = 0;
        const s = String(tag || '');
        for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
        return palette[h % palette.length];
    },

    // A list's search and filters have nothing to act on while it is empty. Hide
    // them, but not the toolbar, which also carries the page's actions.
    hideToolbarFilters(toolbarId, hidden) {
        document.getElementById(toolbarId)
            ?.querySelectorAll('.admin-search, .inline-perm-toggle')
            .forEach(el => { el.hidden = hidden; });
    },

    // Escape for embedding in JS string literals inside HTML onclick attributes.
    // HTML-decodes attribute values before JS execution, so we must escape at
    // the JS level. Hex-escape < and > to prevent HTML parser interference.
    escapeJs(text) {
        if (typeof text !== 'string') return '';
        return text.replace(/\\/g, '\\\\')
                   .replace(/'/g, "\\'")
                   .replace(/"/g, '\\"')
                   .replace(/\n/g, '\\n')
                   .replace(/\r/g, '\\r')
                   .replace(/</g, '\\x3c')
                   .replace(/>/g, '\\x3e');
    },

    // Render a subset of markdown suitable for comments.
    // Supports: bold, italic, strikethrough, inline code, code blocks,
    // bullet/numbered lists, and links. Headings and images are excluded.
    renderCommentMarkdown(text) {
        if (typeof text !== 'string' || !text) return '';
        if (window.marked && window.DOMPurify) {
            try {
                const renderer = new marked.Renderer();
                // Strip headings - render as plain bold text instead
                renderer.heading = function({ text }) {
                    return `<p><strong>${text}</strong></p>`;
                };
                // Strip images
                renderer.image = function() {
                    return '';
                };
                // Strip horizontal rules
                renderer.hr = function() {
                    return '';
                };
                // Strip tables
                renderer.table = function() {
                    return '';
                };
                // Open links in new tab
                renderer.link = function({ href, text }) {
                    const escaped = Utils.escapeAttr(href || '');
                    return `<a href="${escaped}" target="_blank" rel="noopener">${text}</a>`;
                };
                const html = marked.parse(text, {
                    renderer,
                    breaks: true,
                    gfm: true,
                    headerIds: false,
                    mangle: false,
                });
                return DOMPurify.sanitize(html, {
                    ALLOWED_TAGS: [
                        'p', 'br', 'strong', 'b', 'em', 'i', 'del', 's',
                        'code', 'pre', 'a', 'ul', 'ol', 'li', 'blockquote',
                    ],
                    ALLOWED_ATTR: ['href', 'target', 'rel'],
                });
            } catch (e) {
                // Fall through to escaped plaintext
            }
        }
        return Utils.escapeHtml(text).replace(/\n/g, '<br>');
    },

    // Show notification toast
    showNotification(message, type = 'info') {
        // Remove existing notifications
        const existing = document.querySelectorAll('.notification-toast');
        existing.forEach(el => el.remove());

        // Create notification element
        const notification = document.createElement('div');
        notification.className = `notification-toast ${type}`;
        notification.textContent = message;

        // Add to page
        document.body.appendChild(notification);

        // Auto-remove after 4 seconds
        setTimeout(() => {
            if (notification.parentNode) {
                notification.remove();
            }
        }, 4000);
    },

    // Highlights YAML line by line. Text inside a block scalar (| or >) stays text, and
    // a # starts a comment only outside quotes and after whitespace, as in YAML itself.
    highlightYAML(yamlText) {
        if (typeof yamlText !== 'string') return yamlText;
        const span = (cls, text) => text ? `<span class="yaml-${cls}">${this.escapeHtml(text)}</span>` : '';
        const value = (text) => {
            const t = text.trim();
            if (!t) return this.escapeHtml(text);
            const lead = text.slice(0, text.indexOf(t));
            const trail = text.slice(lead.length + t.length);
            let cls = 'string';
            if (/^[|>][-+]?\d*$/.test(t) || /^(true|false|null|~)$/i.test(t)) cls = 'keyword';
            else if (/^-?\d+(\.\d+)?$/.test(t)) cls = 'number';
            return lead + span(cls, t) + trail;
        };
        // Splits a value from a trailing comment.
        const splitComment = (text) => {
            let i = text.length - text.trimStart().length;
            const quote = text[i] === '"' || text[i] === "'" ? text[i] : null;
            if (quote) {
                for (i++; i < text.length; i++) {
                    if (quote === '"' && text[i] === '\\') { i++; continue; }
                    if (text[i] === quote) {
                        if (quote === "'" && text[i + 1] === "'") { i++; continue; }
                        break;
                    }
                }
            }
            for (; i < text.length; i++) {
                if (text[i] === '#' && (i === 0 || /\s/.test(text[i - 1]))) return [text.slice(0, i), text.slice(i)];
            }
            return [text, ''];
        };

        // Finds a mapping key: quoted, or plain up to the first colon followed by
        // whitespace, without a comment before it. Returns [whole, key, gap].
        const keyOf = (text) => {
            let end;
            if (text[0] === '"' || text[0] === "'") {
                const q = text.match(/^("(?:[^"\\]|\\.)*"|'(?:[^']|'')*')/);
                if (!q) return null;
                end = q[1].length;
                while (text[end] === ' ') end++;
                if (text[end] !== ':') return null;
            } else {
                if (!text || /^[\s#]/.test(text)) return null;
                end = text.search(/:(?=\s|$)/);
                if (end <= 0 || /\s#/.test(text.slice(0, end))) return null;
            }
            if (!/^(\s|$)/.test(text.slice(end + 1))) return null;
            const key = text.slice(0, end).trimEnd();
            return [text.slice(0, end + 1), key, text.slice(key.length, end)];
        };

        let block = null; // column of the key that opened a block scalar
        return yamlText.split('\n').map(line => {
            const indent = line.length - line.trimStart().length;
            if (block !== null) {
                if (line.trim() === '' || indent > block) return span('string', line);
                block = null;
            }
            let out = this.escapeHtml(line.slice(0, indent));
            let rest = line.slice(indent);
            let m;
            while ((m = rest.match(/^-( +|$)/))) {
                out += span('list', '-') + m[1];
                rest = rest.slice(m[0].length);
            }
            const keyColumn = line.length - rest.length;
            m = keyOf(rest);
            if (m) {
                out += span('key', m[1]) + m[2] + ':';
                rest = rest.slice(m[0].length);
            }
            const [val, comment] = splitComment(rest);
            if (/^\s*[|>][-+]?\d*\s*$/.test(val)) block = m ? keyColumn : indent;
            return out + value(val) + span('comment', comment);
        }).join('\n');
    },

    // Setup YAML syntax highlighting on an element
    setupYAMLHighlighting(textareaElement) {
        if (!textareaElement) return;

        // Create a preview div for syntax highlighting
        const preview = document.createElement('div');
        preview.className = 'yaml-preview';
        preview.style.cssText = `
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            padding: ${textareaElement.style.padding || '0.625rem 0.875rem'};
            margin: 0;
            border: none;
            font-family: ${textareaElement.style.fontFamily || 'Monaco, Menlo, Ubuntu Mono, monospace'};
            font-size: ${textareaElement.style.fontSize || '0.8125rem'};
            line-height: ${textareaElement.style.lineHeight || '1.5'};
            white-space: pre-wrap;
            word-wrap: break-word;
            color: transparent;
            background: transparent;
            pointer-events: none;
            z-index: 1;
            overflow: hidden;
        `;

        // Create wrapper
        const wrapper = document.createElement('div');
        wrapper.className = 'yaml-editor-wrapper';
        wrapper.style.cssText = 'position: relative; display: inline-block; width: 100%;';

        // Style the textarea
        textareaElement.style.cssText += `
            position: relative;
            z-index: 2;
            background: transparent;
            color: var(--text-primary);
        `;

        // Insert wrapper
        textareaElement.parentNode.insertBefore(wrapper, textareaElement);
        wrapper.appendChild(preview);
        wrapper.appendChild(textareaElement);

        // Update highlighting
        const updateHighlighting = () => {
            const highlighted = this.highlightYAML(textareaElement.value);
            preview.innerHTML = highlighted;
        };

        // Sync scrolling
        const syncScroll = () => {
            preview.scrollTop = textareaElement.scrollTop;
            preview.scrollLeft = textareaElement.scrollLeft;
        };

        // Event listeners
        textareaElement.addEventListener('input', updateHighlighting);
        textareaElement.addEventListener('scroll', syncScroll);

        // Initial highlighting
        updateHighlighting();

        return wrapper;
    },

    // Message from a failed API response. The API answers errors as
    // {success:false, error, code}; a few endpoints outside that contract
    // (ingest, OIDC) still answer plain text, so fall back to the body.
    async errorMessage(response, fallback) {
        const body = await response.text();
        try {
            const parsed = JSON.parse(body);
            if (parsed && typeof parsed.error === 'string' && parsed.error) return parsed.error;
        } catch (_) { /* not JSON: use the raw body below */ }
        return body.trim() || fallback || `Request failed (${response.status})`;
    },

    // Machine-readable classification of a failed response, or '' when the
    // Result rows as plain objects, columns on screen first and everything else after.
    // A projected row carries the rest of its event in _all_fields; the projection wins,
    // since that is what the query asked for.
    //
    // expandNormLog unpacks the normalized event that an unprojected row carries as a
    // JSON string, so the output is the event's own fields rather than one escaped blob.
    resultsToObjects(results, fieldOrder, { expandNormLog = false } = {}) {
        const order = fieldOrder || [];
        return (results || []).map(row => {
            let flat = row;
            if (row._all_fields && typeof row._all_fields === 'object') {
                const { _all_fields, ...projected } = row;
                flat = { ..._all_fields, ...projected };
            }
            if (expandNormLog && typeof flat.norm_log === 'string' && flat.norm_log.trim()) {
                try {
                    const parsed = JSON.parse(flat.norm_log);
                    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                        const { norm_log, ...rest } = flat;
                        flat = { ...parsed, ...rest };
                    }
                } catch (_) {
                    // Not JSON after all; the column stays as it is.
                }
            }
            const out = {};
            for (const f of order) if (f in flat) out[f] = flat[f];
            for (const k of Object.keys(flat)) if (!(k in out)) out[k] = flat[k];
            return out;
        });
    },

    // Copies text, reporting whether it worked. The clipboard needs a secure context,
    // so an install served over plain HTTP falls back to whatever the caller offers.
    async copyText(text) {
        try {
            if (!navigator.clipboard) return false;
            await navigator.clipboard.writeText(text);
            return true;
        } catch (_) {
            return false;
        }
    },

    // endpoint predates the error contract.
    async errorCode(response) {
        try {
            const parsed = JSON.parse(await response.text());
            return (parsed && parsed.code) || '';
        } catch (_) {
            return '';
        }
    },

    // Measure the space actually left below a scroll box and publish it as
    // --fit-height, which its stylesheet reads: `max-height: var(--fit-height, <fallback>)`.
    //
    // A fixed budget (calc(100vh - 300px)) has to guess how much chrome sits above
    // the box, and wherever the guess runs high the box stops short of the bottom
    // bar. The fixed time bar and the page's own bottom padding come off the total:
    // sizing past them puts the box underneath them and gives the page a scrollbar
    // it does not need. Publishing a variable rather than an inline max-height
    // leaves the cascade intact, so a rule that opts out (a mobile breakpoint, an
    // embedded panel) still wins.
    fitBelow(el, minHeight = 240) {
        if (!el) return 0;
        const bar = document.querySelector('.time-bar');
        const container = el.closest('.container');
        const pad = container ? parseFloat(getComputedStyle(container).paddingBottom) || 0 : 0;
        const barHeight = bar && getComputedStyle(bar).position === 'fixed' ? bar.offsetHeight : 0;
        const available = window.innerHeight - el.getBoundingClientRect().top - barHeight - pad;
        const height = Math.max(minHeight, Math.min(Math.round(available), window.innerHeight));

        // Only write on a real change: these run from a ResizeObserver watching the
        // same element, and rewriting the value it just read restarts the cycle.
        const current = parseFloat(el.style.getPropertyValue('--fit-height'));
        if (!Number.isFinite(current) || Math.abs(current - height) > 1) {
            el.style.setProperty('--fit-height', height + 'px');
        }
        return height;
    }
};

// Make globally available
window.Utils = Utils;

const KebabMenu = {
    init() {
        document.addEventListener('click', e => {
            if (e.target.matches('.kebab-item') || !e.target.closest('.kebab-wrapper')) {
                document.querySelectorAll('.kebab-wrapper.open').forEach(w => w.classList.remove('open'));
            }
        });
        document.addEventListener('scroll', () => {
            document.querySelectorAll('.kebab-wrapper.open').forEach(w => w.classList.remove('open'));
        }, true);
    },
    _position(wrapper, btn) {
        const menu = wrapper.querySelector('.kebab-menu');
        const rect = btn.getBoundingClientRect();
        menu.style.top = (rect.bottom + 4) + 'px';
        menu.style.left = (rect.right - menu.offsetWidth) + 'px';
    },
    toggle(event, btn) {
        event.stopPropagation();
        const wrapper = btn.closest('.kebab-wrapper');
        const isOpen = wrapper.classList.contains('open');
        document.querySelectorAll('.kebab-wrapper.open').forEach(w => w.classList.remove('open'));
        if (!isOpen) {
            wrapper.classList.add('open');
            requestAnimationFrame(() => KebabMenu._position(wrapper, btn));
        }
    }
};
window.KebabMenu = KebabMenu;
