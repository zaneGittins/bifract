// Time bar and the display-timezone switcher.
//
// The live clock doubles as the zone indicator. In an ops tool the risk is not
// that the setting is hard to find, it is that a timestamp gets misread during
// an incident, so the active zone stays on screen at all times and the control
// that changes it is the same element.
const TimeBar = {
    intervalId: null,
    _pickerOpen: false,
    _activeIndex: -1,

    init() {
        this.updateClock();
        this.intervalId = setInterval(() => this.updateClock(), 1000);
        this.initPicker();
        document.addEventListener(TZ.EVENT, () => {
            this.updateClock();
            if (this._pickerOpen) this.renderZones();
        });
    },

    updateClock() {
        const zoneEl = document.getElementById('tzClockZone');
        const timeEl = document.getElementById('tzClockTime');
        if (!zoneEl || !timeEl) return;
        const now = Date.now();
        zoneEl.textContent = TZ.abbrev(now);
        timeEl.textContent = TZ.format(now, 'time');
        const btn = document.getElementById('tzClockBtn');
        if (btn) btn.title = `${TZ.zone()} (${TZ.offsetLabel(now)})\nClick to change the display timezone`;
    },

    initPicker() {
        const btn = document.getElementById('tzClockBtn');
        const search = document.getElementById('tzSearch');
        const list = document.getElementById('tzList');
        if (!btn || !search || !list) return;

        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            this._pickerOpen ? this.closePicker() : this.openPicker();
        });

        search.addEventListener('input', () => {
            this._activeIndex = -1;
            this.renderZones(search.value);
        });

        search.addEventListener('keydown', (e) => {
            const rows = Array.from(list.querySelectorAll('.tz-row'));
            if (e.key === 'Escape') { this.closePicker(); return; }
            if (e.key === 'Enter') {
                e.preventDefault();
                const row = rows[Math.max(0, this._activeIndex)];
                if (row) this.selectZone(row.dataset.zone);
                return;
            }
            if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;
            e.preventDefault();
            if (!rows.length) return;
            const delta = e.key === 'ArrowDown' ? 1 : -1;
            this._activeIndex = (Math.max(0, this._activeIndex) + delta + rows.length) % rows.length;
            rows.forEach((r, i) => r.classList.toggle('active', i === this._activeIndex));
            rows[this._activeIndex].scrollIntoView({ block: 'nearest' });
        });

        list.addEventListener('click', (e) => {
            const row = e.target.closest('.tz-row');
            if (row) this.selectZone(row.dataset.zone);
        });

        document.addEventListener('click', (e) => {
            if (!this._pickerOpen) return;
            const wrap = e.target.closest('.tz-picker-wrap');
            if (!wrap) this.closePicker();
        });
    },

    openPicker() {
        const panel = document.getElementById('tzPicker');
        const search = document.getElementById('tzSearch');
        const btn = document.getElementById('tzClockBtn');
        if (!panel) return;
        this._pickerOpen = true;
        this._activeIndex = -1;
        panel.style.display = 'block';
        if (btn) { btn.classList.add('open'); btn.setAttribute('aria-expanded', 'true'); }
        if (search) { search.value = ''; search.focus(); }
        this.renderZones();
    },

    closePicker() {
        const panel = document.getElementById('tzPicker');
        const btn = document.getElementById('tzClockBtn');
        if (!panel) return;
        this._pickerOpen = false;
        panel.style.display = 'none';
        if (btn) { btn.classList.remove('open'); btn.setAttribute('aria-expanded', 'false'); }
    },

    async selectZone(zone) {
        if (!zone) return;
        this.closePicker();
        await TZ.setZone(zone);
    },

    // Zones the user is most likely to want come first and stay unfiltered-first:
    // UTC (what the logs are in), their own, and whatever they last switched to.
    renderZones(filter) {
        const list = document.getElementById('tzList');
        const offsetEl = document.getElementById('tzPickerOffset');
        if (!list) return;
        if (offsetEl) offsetEl.textContent = TZ.offsetLabel();

        const q = (filter || '').trim().toLowerCase();
        const match = z => !q || z.toLowerCase().replace(/_/g, ' ').includes(q.replace(/_/g, ' '));

        const browser = TZ.browserZone();
        const pinned = ['UTC'];
        if (browser && browser !== 'UTC') pinned.push(browser);
        for (const z of TZ.recent()) if (!pinned.includes(z)) pinned.push(z);

        const seen = new Set(pinned);
        const rest = TZ.zoneList().filter(z => !seen.has(z) && match(z));
        const quick = pinned.filter(match);

        let html = '';
        if (quick.length) {
            html += '<div class="tz-group-label">Quick</div>';
            html += quick.map(z => this._row(z, z === browser && z !== 'UTC' ? 'Browser' : '')).join('');
        }
        if (rest.length) {
            html += '<div class="tz-group-label">All zones</div>';
            html += rest.map(z => this._row(z, '')).join('');
        }
        if (!quick.length && !rest.length) {
            html = '<div class="tz-empty">No matching zone</div>';
        }
        list.innerHTML = html;

        const active = list.querySelector(`.tz-row[data-zone="${CSS.escape(TZ.zone())}"]`);
        if (active && !q) active.scrollIntoView({ block: 'nearest' });
    },

    // Each row previews the current time in that zone, which is what makes the
    // list scannable. Formatters are cached per zone inside TZ, so the cost is
    // paid once per session rather than per open.
    _row(zone, badge) {
        const saved = TZ.zone();
        let time = '', offset = '';
        try {
            TZ._zone = zone;
            time = TZ.format(Date.now(), 'time');
            offset = TZ.offsetLabel();
        } catch (e) {
            /* a zone the engine knows by name but cannot format */
        } finally {
            TZ._zone = saved;
        }

        const selected = zone === saved ? ' selected' : '';
        const badgeHtml = badge ? `<span class="tz-row-badge">${Utils.escapeHtml(badge)}</span>` : '';
        return `<button type="button" class="tz-row${selected}" data-zone="${Utils.escapeAttr(zone)}">
            <span class="tz-row-name">${Utils.escapeHtml(zone.replace(/_/g, ' '))}${badgeHtml}</span>
            <span class="tz-row-meta"><span class="tz-row-time">${time}</span><span class="tz-row-offset">${offset}</span></span>
        </button>`;
    },

    updateFractalName(fractalName) {
        const element = document.getElementById('currentFractalName');
        if (element) {
            element.textContent = fractalName || 'No fractal selected';
        }
    },

    destroy() {
        if (this.intervalId) {
            clearInterval(this.intervalId);
        }
    }
};

// Make globally available
window.TimeBar = TimeBar;
