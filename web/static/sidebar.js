// Left sidebar: primary navigation for the selected fractal/prism and the instance.

// Drawn on a 16px grid with 1px strokes; straight edges sit on half-pixel
// coordinates so they land on whole device pixels at 1x and 2x.
const SB_ICON_ATTRS = 'width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"';

const SB_ICONS = {
    search: '<circle cx="7" cy="7" r="4.5"/><path d="M10.5 10.5l4 4"/>',
    archive: '<rect x="1.5" y="2.5" width="13" height="3" rx="0.5"/><path d="M2.5 5.5v8h11v-8M6.5 8.5h3"/>',
    notebook: '<rect x="3.5" y="1.5" width="10" height="13" rx="1"/><path d="M6.5 1.5v13"/>',
    dashboard: '<rect x="1.5" y="1.5" width="5" height="7" rx="1"/><rect x="9.5" y="1.5" width="5" height="4" rx="1"/><rect x="9.5" y="8.5" width="5" height="6" rx="1"/><rect x="1.5" y="11.5" width="5" height="3" rx="1"/>',
    shield: '<path d="M8 14.5s5.5-2.5 5.5-7V3.5L8 1.5 2.5 3.5v4c0 4.5 5.5 7 5.5 7z"/>',
    chat: '<path d="M2.5 2.5h11a1 1 0 0 1 1 1v7a1 1 0 0 1-1 1H7.5l-3 2.5v-2.5h-2a1 1 0 0 1-1-1v-7a1 1 0 0 1 1-1z"/>',
    library: '<path d="M3.5 1.5h6l3 3v10h-9z"/><path d="M9.5 1.5v3h3"/>',
    table: '<rect x="1.5" y="2.5" width="13" height="11" rx="1"/><path d="M1.5 6.5h13M5.5 6.5v7"/>',
    chart: '<path d="M1.5 1.5v13h13"/><path d="M4.5 10.5l3-3 2 2 4-4.5"/>',
    ingest: '<path d="M7.5 1.5v8M4.5 6.5l3 3 3-3M2.5 11.5v3h10v-3"/>',
    sliders: '<path d="M7.5 4.5h7M1.5 11.5h7"/><circle cx="4.5" cy="4.5" r="2"/><circle cx="11.5" cy="11.5" r="2"/>',
    transform: '<path d="M2.5 4.5h10M10.5 2.5l2 2-2 2M13.5 11.5h-10M5.5 9.5l-2 2 2 2"/>',
    columns: '<rect x="1.5" y="1.5" width="13" height="13" rx="1"/><path d="M6.5 1.5v13M9.5 1.5v13"/>',
    code: '<path d="M5.5 4.5l-3.5 3.5 3.5 3.5M10.5 4.5l3.5 3.5-3.5 3.5"/>',
    pulse: '<path d="M1.5 8.5h3l2-5 3 9 2-4h3"/>',
    gear: '<circle cx="8" cy="8" r="2"/><path d="M14.34 6.58v2.84l-1.22-.53-.87 2.1 1.24.49-2.01 2.01-.49-1.24-2.1.87.53 1.22H6.58l.53-1.22-2.1-.87-.49 1.24-2.01-2.01 1.24-.49-.87-2.1-1.22.53V6.58l1.22.53.87-2.1-1.24-.49 2.01-2.01.49 1.24 2.1-.87-.53-1.22h2.84l-.53 1.22 2.1.87.49-1.24 2.01 2.01-1.24.49.87 2.1z"/>',
    key: '<circle cx="5" cy="11" r="3.5"/><path d="M7.5 8.5l6-6M11.5 4.5l2 2M9.5 6.5l1.5 1.5"/>',
    layers: '<path d="M8 1.5l6.5 3.5L8 8.5 1.5 5z"/><path d="M1.5 8l6.5 3.5L14.5 8M1.5 11l6.5 3.5 6.5-3.5"/>',
    collapse: '<path d="M7.5 4.5L4 8l3.5 3.5M12.5 4.5L9 8l3.5 3.5"/>',
    expand: '<path d="M8.5 4.5L12 8l-3.5 3.5M3.5 4.5L7 8l-3.5 3.5"/>',
    chevron: '<path d="M4 6l4 4 4-4"/>',
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

    // Keep in sync with sidebar.css and the pre-paint script in index.html.
    NARROW: window.matchMedia('(max-width: 900px)'),
    MOBILE: window.matchMedia('(max-width: 640px)'),

    _rendered: false,
    _initialized: false,
    _level: null,
    _tab: null,
    _tip: null,
    _narrowExpanded: false, // session-only expand while the window is narrow

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
                    <button type="button" class="sb-group-label sb-group-toggle" id="sidebarAdminToggle" data-tip="Admin" aria-expanded="false" aria-controls="sidebarAdminItems">
                        <span class="sb-admin-icon">${this.icon('key')}</span><span class="sb-label">Admin</span>${this.icon('chevron')}
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
        this._applyAdminOpen(this._prefs().adminOpen === true);
        this._renderToggle(document.documentElement.classList.contains('sb-collapsed'));

        document.getElementById('sidebarOpen').addEventListener('click', () => this.openDrawer());
        document.getElementById('sidebarBackdrop').addEventListener('click', () => this.closeDrawer());
        // On window so popups inside the drawer handle Escape first and claim it.
        window.addEventListener('keydown', (e) => {
            if (e.key !== 'Escape' || e.defaultPrevented ||
                !document.documentElement.classList.contains('sb-drawer-open')) return;
            this.closeDrawer();
        });
        const onBreakpoint = () => {
            this._narrowExpanded = false;
            if (!this.MOBILE.matches) this.closeDrawer();
            this._applyCollapsed();
        };
        this.NARROW.addEventListener('change', onBreakpoint);
        this.MOBILE.addEventListener('change', onBreakpoint);
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
        // Query and Recall keep Ctrl/Cmd+K for their own palettes.
        document.getElementById('sidebarGoTo').classList.toggle('no-kbd', this._pageClass() === 'workspace');
        const active = document.querySelector(`#sidebar a[data-nav][aria-current="page"]`);
        document.getElementById('sidebarMobileTitle').textContent = active ? active.dataset.tip : '';
        this.closeDrawer();
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

    // Phones get a full drawer; narrow windows a rail unless expanded for the
    // session; otherwise the saved choice for the page class applies.
    isCollapsed() {
        if (this.MOBILE.matches) return false;
        if (this.NARROW.matches) return !this._narrowExpanded;
        const cls = this._pageClass();
        const saved = this._prefs()[cls];
        return typeof saved === 'boolean' ? saved : cls === 'workspace';
    },

    toggleCollapsed() {
        if (this.NARROW.matches) this._narrowExpanded = !this._narrowExpanded;
        else this._savePrefs({ [this._pageClass()]: !this.isCollapsed() });
        this._applyCollapsed(true);
    },

    openDrawer() {
        document.documentElement.classList.add('sb-drawer-open');
        document.getElementById('sidebarOpen').setAttribute('aria-expanded', 'true');
        const first = document.querySelector('#sidebar a[aria-current="page"]') || document.querySelector('#sidebar a, #sidebar button');
        if (first) first.focus();
    },

    closeDrawer() {
        if (!document.documentElement.classList.contains('sb-drawer-open')) return;
        document.documentElement.classList.remove('sb-drawer-open');
        const opener = document.getElementById('sidebarOpen');
        opener.setAttribute('aria-expanded', 'false');
        // Focus must not stay inside the drawer once it is hidden.
        if (document.getElementById('sidebar').contains(document.activeElement)) opener.focus();
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
