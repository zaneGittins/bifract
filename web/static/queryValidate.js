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
                if (valid) {
                    SyntaxHighlight.errorRanges[inputId] = { start: range.start, end: range.end };
                    rerender();
                } else if (had) {
                    delete SyntaxHighlight.errorRanges[inputId];
                    rerender();
                }
            } else if (valid) {
                SyntaxHighlight.setError(inputId, highlightId, range);
            } else {
                SyntaxHighlight.clearError(inputId, highlightId);
            }
        }
        const input = document.getElementById(inputId);
        if (input) {
            if (res && res.error) input.title = res.error;
            else input.removeAttribute('title');
        }
    },

    // attach wires debounced live validation to a textarea/highlight pair.
    //   getFractalId() optional, returns the fractal to validate against.
    //   onError(message, res) renders the caller's message banner.
    //   onClear() hides it.
    // Returns a detach function.
    attach({ inputId, highlightId, getFractalId, onError, onClear, rerender }) {
        const input = document.getElementById(inputId);
        if (!input) return () => {};

        const handler = () => {
            // Drop the stale underline immediately: its offsets no longer line up
            // with the edited text. Validation re-adds it if still invalid.
            this.applyResult(inputId, highlightId, {}, rerender);
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
