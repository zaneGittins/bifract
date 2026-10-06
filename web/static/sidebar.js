// Left sidebar: primary navigation for the selected fractal/prism and the instance.

const SB_ICON_ATTRS = 'width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"';

const SB_ICONS = {
    search: '<circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/>',
    archive: '<rect x="3" y="4" width="18" height="5" rx="1"/><path d="M5 9v10a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V9M10 13h4"/>',
    notebook: '<path d="M5 4a2 2 0 0 1 2-2h12v18H7a2 2 0 0 0-2 2z"/><path d="M5 22V4M9 7h6"/>',
    dashboard: '<rect x="3" y="3" width="7" height="9" rx="1"/><rect x="14" y="3" width="7" height="5" rx="1"/><rect x="14" y="12" width="7" height="9" rx="1"/><rect x="3" y="16" width="7" height="5" rx="1"/>',
    shield: '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><path d="M12 8v4M12 16h.01"/>',
    chat: '<path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>',
    library: '<path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20V3H6.5A2.5 2.5 0 0 0 4 5.5z"/><path d="M4 19.5A2.5 2.5 0 0 0 6.5 22H20v-5"/>',
    table: '<rect x="3" y="4" width="18" height="16" rx="1"/><path d="M3 10h18M9 10v10"/>',
    chart: '<path d="M3 3v18h18"/><path d="M7 15l4-4 3 3 5-6"/>',
    ingest: '<path d="M12 3v12M7 10l5 5 5-5"/><path d="M4 17v3h16v-3"/>',
    sliders: '<path d="M4 6h9M17 6h3M4 12h3M11 12h9M4 18h11M19 18h1"/><circle cx="15" cy="6" r="2"/><circle cx="9" cy="12" r="2"/><circle cx="17" cy="18" r="2"/>',
    transform: '<path d="M4 7h13l-3-3M20 17H7l3 3"/>',
    columns: '<path d="M4 4h16v16H4zM9.5 4v16M14.5 4v16"/>',
    code: '<path d="M8 8l-4 4 4 4M16 8l4 4-4 4"/>',
    pulse: '<path d="M3 12h4l3-8 4 16 3-8h4"/>',
    gear: '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.6 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.6a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>',
    collapse: '<path d="M11 17l-5-5 5-5M18 17l-5-5 5-5"/>',
    expand: '<path d="M13 17l5-5-5-5M6 17l5-5-5-5"/>',
    chevron: '<path d="M6 9l6 6 6-6"/>',
    layers: '<path d="M12 2l9 5-9 5-9-5z"/><path d="M3 12l9 5 9-5M3 17l9 5 9-5"/>',
};

const Sidebar = {
    // Fractal-level pages grouped by job. key is the App tab id.
    GROUPS: [
        { id: 'investigate', label: 'Investigate', items: [
            { key: 'search', label: 'Query', icon: 'search' },
            { key: 'recall', label: 'Recall', icon: 'archive', gate: () => !!(window.Recall && Recall.available) },
            { key: 'notebooks', label: 'Notebooks', icon: 'notebook' },
            { key: 'dashboards', label: 'Dashboards', icon: 'dashboard' },
        ] },
        { id: 'detect', label: 'Detect', items: [
            { key: 'alerts', label: 'Alerts', icon: 'shield' },
        ] },
        { id: 'ai', label: 'AI', items: [
            { key: 'chat', label: 'Chat', icon: 'chat' },
            { key: 'library', label: 'Instructions', icon: 'library' },
        ] },
        { id: 'data', label: 'Data', items: [
            { key: 'dictionaries', label: 'Lookups', icon: 'table' },
            { key: 'models', label: 'Analytics', icon: 'chart' },
            { key: 'ingest', label: 'Ingest', icon: 'ingest' },
        ] },
    ],

    // Instance-level pages, tenant admins only.
    ADMIN: [
        { key: 'normalizers', label: 'Normalizers', icon: 'transform' },
        { key: 'schema', label: 'Schema', icon: 'columns' },
        { key: 'api', label: 'API', icon: 'code' },
        { key: 'performance', label: 'System', icon: 'pulse' },
        { key: 'settings', label: 'Settings', icon: 'gear' },
    ],

    // Tabs highlighted through another item.
    ALIASES: { comments: 'notebooks' },

    // Pages that need the width. Keep in sync with the pre-paint script in index.html.
    WORKSPACE_TABS: new Set(['search', 'recall']),

    PREFS_KEY: 'bifract-sidebar',

    _rendered: false,
    _initialized: false,
    _level: null,
    _tab: null,
    _tip: null,

    icon(name) {
        return `<svg ${SB_ICON_ATTRS}>${SB_ICONS[name] || ''}</svg>`;
    },

    _itemHtml(item, level) {
        return `<a class="sb-item" href="#" data-nav="${item.key}" data-level="${level}" data-tip="${Utils.escapeAttr(item.label)}">
            ${this.icon(item.icon)}<span class="sb-label">${Utils.escapeHtml(item.label)}</span></a>`;
    },

    _render() {
        const nav = document.getElementById('sidebarNav');
        if (!nav) return false;
        const groups = this.GROUPS.map(g => `
            <div class="sb-group" data-group="${g.id}">
                <div class="sb-group-label">${g.label}</div>
                ${g.items.map(i => this._itemHtml(i, 'fractal')).join('')}
            </div>`).join('');
        nav.innerHTML = `
            <div class="sb-group" data-group="home">
                ${this._itemHtml({ key: 'fractalListing', label: 'Fractals', icon: 'layers' }, 'main')}
            </div>
            ${groups}
            <div class="sb-bottom">
                <div class="sb-group" data-group="scope-settings">
                    <a class="sb-item" href="#" data-nav="manage" data-level="fractal" data-tip="Fractal settings">
                        ${this.icon('sliders')}<span class="sb-label" id="sidebarManageLabel">Fractal settings</span></a>
                </div>
                <div class="sb-group sb-admin" data-group="admin" hidden>
                    <button type="button" class="sb-group-label sb-group-toggle" id="sidebarAdminToggle" aria-expanded="true" aria-controls="sidebarAdminItems">
                        <span>Admin</span>${this.icon('chevron')}
                    </button>
                    <div id="sidebarAdminItems" class="sb-group-items">
                        ${this.ADMIN.map(i => this._itemHtml(i, 'main')).join('')}
                    </div>
                </div>
            </div>`;
        this._rendered = true;
        return true;
    },

    init() {
        if (this._initialized || (!this._rendered && !this._render())) return;
        this._initialized = true;
        const aside = document.getElementById('sidebar');
        aside.addEventListener('click', (e) => this._onClick(e));
        document.getElementById('sidebarAdminToggle').addEventListener('click', () => this._toggleAdmin());
        document.getElementById('sidebarToggle').addEventListener('click', () => this.toggleCollapsed());
        aside.addEventListener('mouseover', (e) => this._showTip(e.target));
        aside.addEventListener('focusin', (e) => this._showTip(e.target));
        aside.addEventListener('mouseout', (e) => { if (!aside.contains(e.relatedTarget)) this.hideTip(); });
        aside.addEventListener('focusout', () => this.hideTip());
        aside.addEventListener('click', () => this.hideTip());
        // Canvas-backed views size from window resize events.
        aside.addEventListener('transitionend', (e) => {
            if (e.target !== aside || e.propertyName !== 'width') return;
            document.documentElement.classList.remove('sb-animate');
            window.dispatchEvent(new Event('resize'));
        });
        this._applyAdminOpen(this._prefs().adminOpen !== false);
        this._renderToggle(document.documentElement.classList.contains('sb-collapsed'));
        this.refresh();
    },

    // Plain left clicks route in-app; modified and middle clicks fall through to the
    // browser, which opens the item's href in a new tab.
    _onClick(e) {
        const link = e.target.closest('a[data-nav], a[data-home]');
        if (!link || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
        e.preventDefault();
        this.hideTip();
        if (!window.App) return;
        if (link.hasAttribute('data-home')) {
            App.showMainView('fractalListing');
        } else if (link.dataset.level === 'main') {
            App.showMainView(link.dataset.nav);
        } else {
            App.showFractalView(link.dataset.nav);
        }
    },

    // Re-evaluate visibility, labels and links from auth, scope and feature state.
    refresh() {
        if (!this._rendered && !this._render()) return;
        const ctx = window.FractalContext;
        const scope = ctx && ctx.hasScope() ? ctx.currentFractal : null;
        const isPrism = !!(scope && ctx.isPrism());
        const user = window.Auth ? Auth.currentUser : null;
        const prefix = isPrism ? 'p' : 'f';

        const aside = document.getElementById('sidebar');
        aside.querySelectorAll('a[data-level="fractal"]').forEach(a => {
            const item = this._find(a.dataset.nav);
            let visible = !!scope;
            if (visible && isPrism && window.App && App._fractalOnlyTabs.has(a.dataset.nav)) visible = false;
            if (visible && item && item.gate && !item.gate()) visible = false;
            if (visible && a.dataset.nav === 'manage') visible = !!(window.Auth && Auth.hasFractalRole('admin'));
            a.hidden = !visible;
            a.href = scope ? `#${prefix}/${scope.id}/${a.dataset.nav}` : '#';
        });
        aside.querySelectorAll('a[data-level="main"]').forEach(a => {
            a.href = `#${a.dataset.nav}`;
        });
        aside.querySelectorAll('.sb-group').forEach(g => {
            if (g.dataset.group === 'admin' || g.dataset.group === 'home') return;
            g.hidden = !g.querySelector('a.sb-item:not([hidden])');
        });
        aside.querySelector('[data-group="admin"]').hidden = !(user && user.is_admin);

        const role = document.querySelector('#userClickable .user-role');
        if (role && window.Auth) role.textContent = Auth.roleText();

        const manageLabel = isPrism ? 'Prism settings' : 'Fractal settings';
        document.getElementById('sidebarManageLabel').textContent = manageLabel;
        aside.querySelector('a[data-nav="manage"]').dataset.tip = manageLabel;
    },

    _find(key) {
        for (const g of this.GROUPS) {
            const hit = g.items.find(i => i.key === key);
            if (hit) return hit;
        }
        return null;
    },

    // Called by App on every view switch.
    setActive(level, tab) {
        if (!this._rendered) return;
        this._level = level;
        this._tab = tab;
        const key = this.ALIASES[tab] || tab;
        document.querySelectorAll('#sidebar a[data-nav]').forEach(a => {
            const on = a.dataset.level === level && a.dataset.nav === key;
            a.classList.toggle('active', on);
            if (on) a.setAttribute('aria-current', 'page');
            else a.removeAttribute('aria-current');
        });
        if (level === 'main' && this.ADMIN.some(i => i.key === key)) this._applyAdminOpen(true);
        this._applyCollapsed();
    },

    _pageClass() {
        return this._level === 'fractal' && this.WORKSPACE_TABS.has(this._tab) ? 'workspace' : 'page';
    },

    _prefs() {
        try {
            const p = JSON.parse(localStorage.getItem(this.PREFS_KEY));
            return p && typeof p === 'object' ? p : {};
        } catch (e) {
            return {};
        }
    },

    _savePrefs(patch) {
        try {
            localStorage.setItem(this.PREFS_KEY, JSON.stringify({ ...this._prefs(), ...patch }));
        } catch (e) {
            // localStorage may be unavailable
        }
    },

    isCollapsed() {
        const cls = this._pageClass();
        const saved = this._prefs()[cls];
        return typeof saved === 'boolean' ? saved : cls === 'workspace';
    },

    toggleCollapsed() {
        this._savePrefs({ [this._pageClass()]: !this.isCollapsed() });
        this._applyCollapsed(true);
    },

    // Only a manual toggle animates; page switches snap so content never reflows
    // through intermediate widths.
    _applyCollapsed(animate = false) {
        const collapsed = this.isCollapsed();
        this._renderToggle(collapsed);
        const root = document.documentElement;
        if (root.classList.contains('sb-collapsed') === collapsed) return;
        const motion = animate && !window.matchMedia('(prefers-reduced-motion: reduce)').matches;
        root.classList.toggle('sb-animate', motion);
        root.classList.toggle('sb-collapsed', collapsed);
        this.hideTip();
        // Canvas-backed views size from window resize events.
        if (!motion) requestAnimationFrame(() => window.dispatchEvent(new Event('resize')));
    },

    _renderToggle(collapsed) {
        const btn = document.getElementById('sidebarToggle');
        const label = collapsed ? 'Expand sidebar' : 'Collapse sidebar';
        btn.setAttribute('aria-label', label);
        btn.setAttribute('aria-expanded', String(!collapsed));
        btn.dataset.tip = label;
        btn.innerHTML = `${this.icon(collapsed ? 'expand' : 'collapse')}<span class="sb-label">Collapse</span>`;
    },

    _toggleAdmin() {
        const open = document.getElementById('sidebarAdminToggle').getAttribute('aria-expanded') !== 'true';
        this._applyAdminOpen(open);
        this._savePrefs({ adminOpen: open });
    },

    _applyAdminOpen(open) {
        document.getElementById('sidebarAdminToggle').setAttribute('aria-expanded', String(open));
        document.getElementById('sidebarAdminItems').classList.toggle('closed', !open);
    },

    // Labels are hidden when collapsed, so a tooltip carries them.
    _showTip(target) {
        const el = target && target.closest ? target.closest('[data-tip]') : null;
        if (!el || el.getAttribute('aria-expanded') === 'true' ||
            !document.documentElement.classList.contains('sb-collapsed')) {
            this.hideTip();
            return;
        }
        if (!this._tip) {
            this._tip = document.createElement('div');
            this._tip.className = 'sb-tooltip';
            this._tip.setAttribute('role', 'tooltip');
            document.body.appendChild(this._tip);
        }
        const r = el.getBoundingClientRect();
        this._tip.textContent = el.dataset.tip;
        this._tip.style.top = `${Math.round(r.top + r.height / 2)}px`;
        this._tip.style.left = `${Math.round(r.right + 8)}px`;
        this._tip.classList.add('show');
    },

    hideTip() {
        if (this._tip) this._tip.classList.remove('show');
    },
};

window.Sidebar = Sidebar;
