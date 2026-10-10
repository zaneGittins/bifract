// One confirmation dialog for every destructive action, so how hard an action is to
// trigger follows its consequence rather than whoever wrote the button. Anything that
// loses data permanently passes `phrase`, which must be typed exactly.
const DangerConfirm = {
    _els: null,
    _opts: null,
    _busy: false,
    _returnFocus: null,

    // open({ title, body, phrase, confirmLabel, busyLabel, extra, isReady, onConfirm })
    //   body: array of paragraphs, each a string or { text, muted }; always rendered as text.
    //   extra: optional element shown above the phrase (e.g. a picker); isReady() gates it.
    //   onConfirm: async; the dialog stays open and busy until it settles, closes on
    //   success, and shows a thrown error inline so the user can retry or cancel.
    open(opts) {
        // A second destructive action must not replace one that is still running.
        if (this._busy) return;
        const els = this._build();
        this._opts = opts;
        this._busy = false;
        this._returnFocus = document.activeElement;

        els.title.textContent = opts.title;
        els.body.replaceChildren(...(opts.body || []).map(p => {
            const el = document.createElement('p');
            el.textContent = typeof p === 'string' ? p : p.text;
            if (p.muted) el.className = 'danger-confirm-muted';
            return el;
        }));
        els.extra.replaceChildren(...(opts.extra ? [opts.extra] : []));

        els.phraseGroup.hidden = !opts.phrase;
        els.phraseValue.textContent = opts.phrase || '';
        els.input.value = '';
        els.input.placeholder = opts.phrase || '';
        els.error.hidden = true;
        els.cancel.disabled = false;
        els.confirm.textContent = opts.confirmLabel || 'Confirm';
        this.revalidate();

        els.modal.style.display = 'flex';
        // Without a phrase, default focus stays off the destructive button.
        setTimeout(() => (opts.phrase ? els.input : els.cancel).focus(), 50);
    },

    // Re-checks whether the confirm button may be pressed; callers whose `extra`
    // content changes (e.g. options loading in) call this.
    revalidate() {
        const { input, confirm } = this._els;
        const o = this._opts || {};
        const phraseOK = !o.phrase || input.value.trim() === o.phrase;
        const ready = typeof o.isReady === 'function' ? o.isReady() : true;
        confirm.disabled = this._busy || !phraseOK || !ready;
    },

    close() {
        if (this._busy || !this._els) return;
        this._els.modal.style.display = 'none';
        this._opts = null;
        this._returnFocus?.focus?.();
    },

    async _submit() {
        const els = this._els;
        if (els.confirm.disabled || !this._opts) return;
        this._busy = true;
        els.error.hidden = true;
        els.cancel.disabled = true;
        els.confirm.disabled = true;
        els.confirm.innerHTML = '<span class="spinner"></span> ';
        els.confirm.append(this._opts.busyLabel || 'Working...');
        try {
            await this._opts.onConfirm();
            this._busy = false;
            this.close();
        } catch (err) {
            this._busy = false;
            els.error.textContent = (err && err.message ? err.message : String(err)).trim();
            els.error.hidden = false;
            els.cancel.disabled = false;
            els.confirm.textContent = this._opts.confirmLabel || 'Confirm';
            this.revalidate();
        }
    },

    // Keeps Tab inside the dialog, so focus cannot reach a trigger behind the overlay.
    _trapFocus(e) {
        const focusable = [...this._els.modal.querySelectorAll('input, button, select, textarea, [tabindex]:not([tabindex="-1"])')]
            .filter(el => !el.disabled && el.offsetParent !== null);
        if (!focusable.length) return;
        const first = focusable[0], last = focusable[focusable.length - 1];
        if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
        else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    },

    _build() {
        if (this._els) return this._els;
        const modal = document.createElement('div');
        modal.className = 'modal danger-confirm';
        modal.style.display = 'none';
        modal.innerHTML = `
            <div class="modal-content" role="dialog" aria-modal="true" aria-labelledby="dangerConfirmTitle">
                <div class="modal-header"><h3 id="dangerConfirmTitle" class="danger-confirm-title"></h3></div>
                <div class="modal-body">
                    <div class="danger-confirm-body"></div>
                    <div class="danger-confirm-extra"></div>
                    <div class="form-group danger-confirm-phrase">
                        <label for="dangerConfirmInput">Type <strong class="danger-confirm-phrase-value"></strong> to confirm</label>
                        <input type="text" id="dangerConfirmInput" autocomplete="off" spellcheck="false">
                    </div>
                    <p class="danger-confirm-error" role="alert" hidden></p>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn-secondary danger-confirm-cancel">Cancel</button>
                    <button type="button" class="btn-danger danger-confirm-ok" disabled></button>
                </div>
            </div>`;
        document.body.appendChild(modal);

        const q = sel => modal.querySelector(sel);
        const els = this._els = {
            modal,
            title: q('.danger-confirm-title'),
            body: q('.danger-confirm-body'),
            extra: q('.danger-confirm-extra'),
            phraseGroup: q('.danger-confirm-phrase'),
            phraseValue: q('.danger-confirm-phrase-value'),
            input: q('#dangerConfirmInput'),
            error: q('.danger-confirm-error'),
            cancel: q('.danger-confirm-cancel'),
            confirm: q('.danger-confirm-ok'),
        };

        els.input.addEventListener('input', () => this.revalidate());
        els.cancel.addEventListener('click', () => this.close());
        els.confirm.addEventListener('click', () => this._submit());
        modal.addEventListener('click', e => { if (e.target === modal) this.close(); });
        modal.addEventListener('keydown', e => {
            if (e.key === 'Escape') { e.stopPropagation(); this.close(); }
            if (e.key === 'Enter' && e.target === els.input) { e.preventDefault(); this._submit(); }
            if (e.key === 'Tab') this._trapFocus(e);
        });
        return els;
    },
};

window.DangerConfirm = DangerConfirm;
