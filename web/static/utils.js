// Utility functions
const Utils = {
    formatDateTimeLocal(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        return `${year}-${month}-${day} ${hours}:${minutes}`;
    },

    formatTimestamp(ts) {
        if (!ts) return '-';
        try {
            const date = new Date(ts);
            return date.toISOString().replace('T', ' ').substring(0, 19);
        } catch (e) {
            return String(ts);
        }
    },

    getRelativeTime(timestamp) {
        if (!timestamp) return '';

        const now = new Date();
        const past = new Date(timestamp);
        let remaining = Math.floor((now - past) / 1000);
        if (remaining < 0) remaining = 0;

        const days = Math.floor(remaining / 86400);
        remaining %= 86400;
        const hours = Math.floor(remaining / 3600);
        remaining %= 3600;
        const minutes = Math.floor(remaining / 60);

        const parts = [];
        if (days > 0) parts.push(`${days} day${days > 1 ? 's' : ''}`);
        if (hours > 0) parts.push(`${hours} hour${hours > 1 ? 's' : ''}`);
        if (minutes > 0) parts.push(`${minutes} minute${minutes > 1 ? 's' : ''}`);

        if (parts.length === 0) return 'just now';
        return parts.join(', ') + ' ago';
    },

    getCurrentUTC() {
        const now = new Date();
        return now.toISOString().substring(11, 19) + ' UTC';
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

    // Simple YAML syntax highlighter
    highlightYAML(yamlText) {
        if (typeof yamlText !== 'string') return yamlText;

        // Escape HTML first to prevent XSS, then apply highlighting
        const escaped = this.escapeHtml(yamlText);

        return escaped
            // Keys (word followed by colon)
            .replace(/^(\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:/gm, '$1<span class="yaml-key">$2</span>:')
            // String values (quoted strings - use HTML-escaped quotes)
            .replace(/:\s*&quot;([^&]*)&quot;/g, ': <span class="yaml-string">&quot;$1&quot;</span>')
            .replace(/:\s*&#039;([^&]*)&#039;/g, ': <span class="yaml-string">&#039;$1&#039;</span>')
            // Boolean values
            .replace(/:\s*(true|false)\b/g, ': <span class="yaml-boolean">$1</span>')
            // Numbers
            .replace(/:\s*(\d+(?:\.\d+)?)\b/g, ': <span class="yaml-number">$1</span>')
            // Comments
            .replace(/(#.*$)/gm, '<span class="yaml-comment">$1</span>')
            // List items
            .replace(/^(\s*)-\s*/gm, '$1<span class="yaml-list">-</span> ')
            // Multi-line strings (|- or |)
            .replace(/:\s*(\|[-]?)\s*$/gm, ': <span class="yaml-multiline">$1</span>');
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
    }
};

// Make globally available
window.Utils = Utils;
