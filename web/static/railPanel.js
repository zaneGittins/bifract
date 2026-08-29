// Left rail shell: one push-layout panel, two panes.
//
// Fields and Notebook share the rail rather than owning one each. Two 300px
// rails would take a third of a laptop's width, and the two are used at
// different moments: Fields to explore a result set, Notebook to capture from
// it. Sharing the shell also keeps the mutual exclusion with the detail panel
// on narrow viewports to a single rule.
//
// Panes register themselves with onShow/onHide callbacks so neither pane's
// module has to know the other exists.
const RailPanel = {
    isOpen: false,
    tab: 'fields',
    _panes: {},

    // Below this width the rail and the detail panel cannot both be readable.
    narrowViewport: 1200,

    init() {
        const rail = document.getElementById('fieldsRail');
        if (!rail) return;

        rail.querySelectorAll('[data-rail-tab]').forEach(btn => {
            btn.addEventListener('click', () => this.select(btn.dataset.railTab));
        });

        const closeBtn = document.getElementById('fieldsRailClose');
        if (closeBtn) closeBtn.addEventListener('click', () => this.close());

        this._wireToggle('fieldsRailToggle', 'fields');
        this._wireToggle('notebookRailToggle', 'notebook');

        this._sync();
    },

    _wireToggle(id, tab) {
        const btn = document.getElementById(id);
        if (btn) btn.addEventListener('click', () => this.toggle(tab));
    },

    // registerPane wires a pane's lifecycle. onShow fires when the pane becomes
    // the visible one, onHide when it stops being visible for any reason
    // (tab switch or the rail closing), so a pane can drop in-flight work.
    registerPane(name, handlers) {
        this._panes[name] = handlers || {};
    },

    // toggle: open on `tab`, switch to it, or close if it is already showing.
    toggle(tab) {
        if (this.isOpen && this.tab === tab) {
            this.close();
            return;
        }
        this.open(tab);
    },

    open(tab) {
        const wasVisible = this.isOpen ? this.tab : null;
        this.isOpen = true;
        if (tab) this.tab = tab;

        if (window.LogDetail && window.innerWidth < this.narrowViewport) {
            const panel = document.getElementById('logDetailPanel');
            if (panel && panel.classList.contains('open')) LogDetail.close();
        }

        this._sync(wasVisible);
    },

    select(tab) {
        if (!tab) return;
        if (this.isOpen && this.tab === tab) return;
        this.open(tab);
    },

    close() {
        if (!this.isOpen) return;
        const wasVisible = this.tab;
        this.isOpen = false;
        this._sync(wasVisible);
    },

    // _sync drives the DOM from state and fires the pane callbacks. `wasVisible`
    // is the pane that was showing before this change, so a pane is only told to
    // hide when it actually stops being visible.
    _sync(wasVisible) {
        const rail = document.getElementById('fieldsRail');
        if (!rail) return;

        rail.classList.toggle('open', this.isOpen);
        rail.querySelectorAll('[data-rail-tab]').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.railTab === this.tab);
        });
        rail.querySelectorAll('[data-rail-pane]').forEach(pane => {
            pane.hidden = pane.dataset.railPane !== this.tab;
        });

        const fieldsToggle = document.getElementById('fieldsRailToggle');
        if (fieldsToggle) fieldsToggle.classList.toggle('active', this.isOpen && this.tab === 'fields');
        const notebookToggle = document.getElementById('notebookRailToggle');
        if (notebookToggle) notebookToggle.classList.toggle('active', this.isOpen && this.tab === 'notebook');

        const nowVisible = this.isOpen ? this.tab : null;
        if (wasVisible && wasVisible !== nowVisible) this._fire(wasVisible, 'onHide');
        if (nowVisible && nowVisible !== wasVisible) this._fire(nowVisible, 'onShow');
    },

    _fire(pane, hook) {
        const handlers = this._panes[pane];
        const fn = handlers && handlers[hook];
        if (typeof fn === 'function') {
            try {
                fn();
            } catch (e) {
                console.error(`[RailPanel] ${pane}.${hook} failed:`, e);
            }
        }
    },

    isPaneVisible(tab) {
        return this.isOpen && this.tab === tab;
    },
};

window.RailPanel = RailPanel;
