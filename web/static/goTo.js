// Go to: the palette's pages, sub-pages, fractals, prisms and objects in the
// current scope.

const GoTo = {
    // Sub-tabs reachable as "Page > Sub-tab", read from their own markup so labels
    // and role gating stay in one place.
    SUBTABS: [
        { parent: 'alerts', level: 'fractal', selector: '#alertsSubTabs .alerts-sub-tab', route: b => b.dataset.subtab },
        { parent: 'notebooks', level: 'fractal', selector: '[data-investigation-subtab="evidence"]', route: () => 'evidence' },
        { parent: 'manage', level: 'fractal', selector: '#manageSubTabs .alerts-sub-tab', prism: false, route: b => b.dataset.subtab },
        { parent: 'manage', level: 'fractal', selector: '#prismManageSubTabs .alerts-sub-tab', prism: true, route: b => b.dataset.subtab },
        { parent: 'performance', level: 'main', selector: '#perfSubTabs .alerts-sub-tab', route: b => b.dataset.subtab },
        { parent: 'settings', level: 'main', selector: '#settingsSubTabs .alerts-sub-tab', route: b => b.dataset.subtab },
    ],

    // Objects in the current scope. Searched sources are queried per keystroke so
    // large scopes stay complete; listed ones are small and filtered locally.
    OBJECTS: [
        { group: 'Dashboards', kind: 'Dashboard', tab: 'dashboards', url: '/api/v1/dashboards?limit=8', search: true },
        { group: 'Notebooks', kind: 'Notebook', tab: 'notebooks', url: '/api/v1/notebooks?limit=8', search: true },
        { group: 'Alert rules', kind: 'Alert rule', tab: 'alerts', url: '/api/v1/alerts?limit=8', search: true },
        { group: 'Lookups', kind: 'Lookup', tab: 'dictionaries', url: '/api/v1/dictionaries' },
        { group: 'Analytics models', kind: 'Analytics model', tab: 'models', url: '/api/v1/models', fractalOnly: true },
    ],

    open(r) {
        if (!window.App) return;
        if (r.scope) {
            if (r.prism) FractalSelector.selectPrism(r.scope);
            else FractalSelector.selectFractal(r.scope);
        } else if (r.level === 'main') {
            App.showMainView(r.tab, r.sub);
        } else {
            App.showFractalView(r.tab, r.sub);
        }
    },

    pages() {
        const out = [];
        document.querySelectorAll('#sidebar a[data-nav]').forEach(a => {
            if (a.hidden || a.closest('[hidden]')) return;
            out.push({ label: a.dataset.tip, kind: 'Page', level: a.dataset.level, tab: a.dataset.nav, sub: '' });
        });
        const parents = new Map(out.map(p => [`${p.level}:${p.tab}`, p.label]));
        const isPrism = !!(window.FractalContext && FractalContext.isPrism());
        for (const s of this.SUBTABS) {
            const parent = parents.get(`${s.level}:${s.parent}`);
            if (!parent || (s.prism !== undefined && s.prism !== isPrism)) continue;
            document.querySelectorAll(s.selector).forEach(b => {
                if (b.classList.contains('rbac-hidden') || b.hidden) return;
                // Own text only: some sub-tabs carry a count badge.
                const text = [...b.childNodes].filter(n => n.nodeType === Node.TEXT_NODE).map(n => n.textContent).join('').trim();
                out.push({ label: `${parent} > ${text}`, kind: 'Page', level: s.level, tab: s.parent, sub: s.route(b), isSub: true });
            });
        }
        return out.map(p => ({ ...p, key: `${p.level}:${p.tab}:${p.sub}` }));
    },

    scopes() {
        if (!window.FractalSelector) return [];
        const current = window.FractalContext && FractalContext.hasScope() ? FractalContext.currentFractal.id : null;
        return [
            ...FractalSelector.availableFractals.map(f => ({ label: f.name, hint: 'Fractal', kind: 'Fractal', scope: f.id, prism: false })),
            ...FractalSelector.availablePrisms.map(p => ({ label: p.name, hint: 'Prism', kind: 'Prism', scope: p.id, prism: true })),
        ].filter(s => s.scope !== current).map(s => ({ ...s, key: s.scope }));
    },

    async fetchObjects(source, q) {
        const url = q ? `${source.url}&search=${encodeURIComponent(q)}` : source.url;
        const res = await fetch(url, { credentials: 'include' });
        const body = res.ok ? await res.json() : null;
        const rows = body && body.success && Array.isArray(body.data) ? body.data : [];
        return rows.filter(o => o && o.id && o.name)
            .map(o => ({ label: o.name, kind: source.kind, key: String(o.id), level: 'fractal', tab: source.tab, sub: String(o.id) }));
    },

    register() {
        const openAction = (label) => [{ label, run: r => this.open(r) }];
        Palette.register({
            id: 'pages', group: 'Pages', order: 300, scope: 'goto', icon: Palette.ICONS.page,
            // Every top-level page when idle, except on Query and Recall where the
            // page's own rows lead.
            idle: ctx => (ctx.workspace ? 0 : Infinity),
            items: (ctx, q) => this.pages().filter(p => q || !p.isSub),
            actions: () => openAction('Open'),
        });
        Palette.register({
            id: 'fractals', group: 'Fractals', order: 310, scope: 'goto', icon: Palette.ICONS.scope,
            idle: ctx => (ctx.workspace ? 0 : Palette.PER_GROUP),
            items: () => this.scopes(),
            actions: () => openAction('Switch'),
        });
        this.OBJECTS.forEach((source, i) => {
            Palette.register({
                id: `obj:${source.tab}`, group: source.group, order: 320 + i, scope: 'goto', icon: Palette.ICONS.object,
                search: !!source.search,
                when: ctx => ctx.hasScope && !(source.fractalOnly && ctx.isPrism),
                // Objects only appear once there is something to search for.
                idle: () => 0,
                fetch: q => this.fetchObjects(source, q),
                actions: () => openAction('Open'),
            });
        });
    },
};

window.GoTo = GoTo;
