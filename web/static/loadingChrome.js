// When to show loading chrome, not what it looks like.
//
// The search page and the alert editor consume the same query stream, and they had
// drifted apart on this: search deferred its indicator so a fast query never flashed,
// while the editor replaced the results with a spinner the instant a request started,
// wiping the previous results on every keystroke-triggered run.
//
// The policy lives here once so the two cannot drift again. Appearance stays with the
// caller, which is the part that legitimately differs.
const LoadingChrome = {
    // How long a query may run before it is worth telling the user it is running. Below
    // this, showing anything is noise the user never asked to see. Raise toward 1000 to
    // keep sub-second queries fully silent. This is the only such knob: the search page
    // and the alert editor both read it.
    DELAY_MS: 500,

    // handlers:
    //   show(mode, ctx)  paint the indicator, 'spinner' or 'bar'
    //   hide()           remove it without implying completion
    //   finish()         let a shown indicator settle (a bar runs to full)
    create(handlers) {
        return {
            _timer: null,
            _handlers: handlers || {},
            // shown is false until the delay elapses, so a caller can tell "finished
            // before we ever said anything" from "finished after".
            shown: false,
            // mode starts as spinner and becomes 'bar' when the meta frame says the
            // query streams. It can change while the timer is still pending.
            mode: 'spinner',
            // gotRows lets a streaming caller keep prior results on screen rather than
            // replacing them with a placeholder it is about to overwrite.
            gotRows: false,

            begin(ctx) {
                this.clear();
                if (this._handlers.hide) this._handlers.hide();
                this.shown = false;
                this.mode = 'spinner';
                this.gotRows = false;

                this._timer = setTimeout(() => {
                    this._timer = null;
                    this.shown = true;
                    if (this._handlers.show) this._handlers.show(this.mode, ctx);
                }, LoadingChrome.DELAY_MS);
            },

            setMode(mode) {
                this.mode = mode;
            },

            markRows() {
                this.gotRows = true;
            },

            // The query has produced its answer. The stream may stay open afterwards to
            // drain histogram chunks, so end() can be far away: without this a pending
            // indicator fires *after* the results are on screen and paints over them.
            settle() {
                this.clear();
            },

            end() {
                this.clear();
                if (this.shown) {
                    if (this._handlers.finish) this._handlers.finish(this.mode);
                } else if (this._handlers.hide) {
                    this._handlers.hide();
                }
                const wasShown = this.shown;
                this.shown = false;
                return wasShown;
            },

            clear() {
                if (this._timer) {
                    clearTimeout(this._timer);
                    this._timer = null;
                }
            }
        };
    }
};

window.LoadingChrome = LoadingChrome;
