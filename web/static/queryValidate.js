// ErrorPopover: a single shared, lightweight popover that shows a query editor's
// current error message on hover. Replaces the native title tooltip (no ~1s
// delay, theme-styled). One delegated mouseover/mouseout pair on document
// covers every editor; the textarea is the top pointer layer, so hovering it
// anywhere reveals the message. Keyed by input id.
const ErrorPopover = {
    messages: {},
    _el: null,
    _forId: null,

    set(inputId, message) {
        this._ensure();
        if (message) {
            this.messages[inputId] = message;
            if (this._forId === inputId) this._render(inputId); // refresh if currently shown
        } else {
            delete this.messages[inputId];
            if (this._forId === inputId) this.hide();
        }
    },

    _ensure() {
        if (this._el) return;
        const el = document.createElement('div');
        el.className = 'query-error-popover';
        el.style.display = 'none';
        document.body.appendChild(el);
        this._el = el;
        document.addEventListener('mouseover', (e) => {
            const t = e.target;
            if (t && t.tagName === 'TEXTAREA' && t.id && this.messages[t.id]) this.show(t.id);
        });
        document.addEventListener('mouseout', (e) => {
            const t = e.target;
            if (t && t.tagName === 'TEXTAREA' && t.id && this._forId === t.id) this.hide();
        });
        // Hide if the anchored editor loses focus (covers it being removed/replaced
        // under a stationary cursor, e.g. deleting a notebook cell), since no
        // mouseout fires in that case.
        document.addEventListener('focusout', (e) => {
            if (e.target && e.target.id && this._forId === e.target.id) this.hide();
        });
        // A scrolled or resized anchor would leave the popover stranded; just hide.
        window.addEventListener('scroll', () => this.hide(), true);
        window.addEventListener('resize', () => this.hide());
    },

    show(inputId) {
        if (!this.messages[inputId]) return;
        this._forId = inputId;
        this._render(inputId);
    },

    _render(inputId) {
        const input = document.getElementById(inputId);
        const msg = this.messages[inputId];
        if (!input || !input.isConnected || !msg || !this._el) { this.hide(); return; }
        this._el.textContent = msg;
        this._el.style.display = 'block';
        const r = input.getBoundingClientRect();
        this._el.style.maxWidth = Math.max(220, Math.min(r.width, 480)) + 'px';
        // Clamp horizontally so it never runs off the right edge.
        const elW = this._el.offsetWidth;
        const elH = this._el.offsetHeight;
        let left = window.scrollX + r.left;
        const maxLeft = window.scrollX + document.documentElement.clientWidth - elW - 8;
        if (left > maxLeft) left = Math.max(window.scrollX + 8, maxLeft);
        // Below the input by default; flip above when it would fall below the fold.
        let top = window.scrollY + r.bottom + 6;
        if (r.bottom + 6 + elH > document.documentElement.clientHeight && r.top - 6 - elH > 0) {
            top = window.scrollY + r.top - 6 - elH;
        }
        this._el.style.top = top + 'px';
        this._el.style.left = left + 'px';
    },

    hide() {
        this._forId = null;
        if (this._el) this._el.style.display = 'none';
    },
};
window.ErrorPopover = ErrorPopover;

// Live BQL validation: parses/translates a query on the backend WITHOUT
// executing it, so editors can underline errors and show a message as the user
// types. Shared by every query editor (search, alerts, notebooks, dashboards,
// models). Underlining is delegated to SyntaxHighlight.setError/clearError; the
// message banner is left to each caller via the onError/onClear callbacks.
const QueryValidate = {
    _timers: {},
    DEBOUNCE_MS: 350,

    // run validates a query and resolves to {valid, error, error_type, error_pos}
    // or null on a network/abort failure (caller should treat null as "unknown",
    // not "invalid", to avoid flagging good queries during transient failures).
    async run(query, fractalId) {
        try {
            const body = { query };
            if (fractalId) body.fractal_id = fractalId;
            const res = await fetch('/api/v1/query/validate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(body),
            });
            if (!res.ok) return null;
            return await res.json();
        } catch (e) {
            return null;
        }
    },

    // applyResult underlines (or clears) the error span and sets a hover tooltip
    // (the textarea is the top pointer layer, so a native title works on every
    // surface). Safe to call with execution results or {} to clear.
    //
    // Surfaces that drive their overlay with their own highlight() call (model,
    // notebook, dashboard editors) pass a `rerender` callback; the error span is
    // stashed in SyntaxHighlight.errorRanges (single source of truth) and their
    // callback repaints. Surfaces backed by SyntaxHighlight.updateHighlight
    // (search, alerts) omit rerender and pass highlightId.
    applyResult(inputId, highlightId, res, rerender) {
        if (window.SyntaxHighlight) {
            const range = (res && res.error_pos) ? res.error_pos : null;
            const valid = range && Number.isInteger(range.start) && Number.isInteger(range.end) && range.end >= range.start;
            if (rerender) {
                // These editors already repaint via their own input listener, so
                // only repaint here when the error span actually changes.
                const had = SyntaxHighlight.errorRanges[inputId] !== undefined;
                SyntaxHighlight.setErrorRange(inputId, valid ? range : null);
                if (valid || had) rerender();
            } else if (valid) {
                SyntaxHighlight.setError(inputId, highlightId, range);
            } else {
                SyntaxHighlight.clearError(inputId, highlightId);
            }
        }
        if (window.ErrorPopover) {
            ErrorPopover.set(inputId, (res && res.error) ? res.error : null);
        }
    },

    // attach wires debounced live validation to a textarea/highlight pair.
    //   getFractalId() optional, returns the fractal to validate against.
    //   onEdit()  fires immediately on every keystroke (e.g. dismiss a stale
    //             run-error banner so it never lingers over edited text).
    //   onError(message, res) / onClear() render/hide a caller message banner.
    //             Most surfaces leave these unset: live editing should show only
    //             the subtle inline underline, not a loud banner.
    // Returns a detach function.
    attach({ inputId, highlightId, getFractalId, onError, onClear, onEdit, rerender }) {
        const input = document.getElementById(inputId);
        if (!input) return () => {};

        const handler = () => {
            // Drop the stale underline immediately: its offsets no longer line up
            // with the edited text. Validation re-adds it if still invalid.
            this.applyResult(inputId, highlightId, {}, rerender);
            if (onEdit) onEdit();
            clearTimeout(this._timers[inputId]);
            const query = input.value;
            this._timers[inputId] = setTimeout(async () => {
                const res = await this.run(query, getFractalId ? getFractalId() : undefined);
                // Discard stale responses: the field changed while we awaited.
                if (input.value !== query) return;
                if (!res || res.valid) {
                    this.applyResult(inputId, highlightId, {}, rerender);
                    if (onClear) onClear();
                    return;
                }
                this.applyResult(inputId, highlightId, res, rerender);
                if (onError) onError(res.error, res);
            }, this.DEBOUNCE_MS);
        };

        input.addEventListener('input', handler);
        return () => {
            input.removeEventListener('input', handler);
            clearTimeout(this._timers[inputId]);
        };
    },
};

window.QueryValidate = QueryValidate;
