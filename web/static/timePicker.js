const TimePicker = {
    state: {
        type: '24h',
        relativeN: 4,
        relativeUnit: 'hours',
        customStart: null,
        customEnd: null,
    },

    _presetMs: {
        '5m':  5 * 60 * 1000,
        '15m': 15 * 60 * 1000,
        '30m': 30 * 60 * 1000,
        '1h':  60 * 60 * 1000,
        '2h':  2 * 60 * 60 * 1000,
        '4h':  4 * 60 * 60 * 1000,
        '6h':  6 * 60 * 60 * 1000,
        '12h': 12 * 60 * 60 * 1000,
        '24h': 24 * 60 * 60 * 1000,
        '7d':  7 * 24 * 60 * 60 * 1000,
        '14d': 14 * 24 * 60 * 60 * 1000,
        '30d': 30 * 24 * 60 * 60 * 1000,
    },

    _unitMs: {
        minutes: 60 * 1000,
        hours:   60 * 60 * 1000,
        days:    24 * 60 * 60 * 1000,
        weeks:   7 * 24 * 60 * 60 * 1000,
    },

    _unitBySuffix: { m: 'minutes', h: 'hours', d: 'days', w: 'weeks' },
    _unitShort: { minutes: 'm', hours: 'h', days: 'd', weeks: 'w' },

    CALENDAR_LABELS: {
        today: 'Today', yesterday: 'Yesterday',
        this_week: 'This week', last_week: 'Last week',
        this_month: 'This month', last_month: 'Last month',
    },

    // Where "all" starts, and the floor for any offset.
    ALL_START_MS: Date.UTC(2000, 0, 1),

    isPreset(type) { return Object.hasOwn(this._presetMs, type); },
    isCalendar(type) { return Object.hasOwn(this.CALENDAR_LABELS, type); },
    isValidType(type) {
        return this.isPreset(type) || this.isCalendar(type) || ['relative', 'custom', 'all', 'live'].includes(type);
    },

    // Calendar ranges in the display zone, weeks starting Monday. Those named
    // "this" or "today" run to now; the others close at a midnight.
    calendarRange(type, nowMs = Date.now()) {
        const p = TZ.parts(nowMs);
        if (!p || !this.isCalendar(type)) return null;
        const midnight = (y, m, d) => {
            const dt = new Date(Date.UTC(y, m - 1, d));
            const pad = n => String(n).padStart(2, '0');
            return TZ.parseWallClock(`${dt.getUTCFullYear()}-${pad(dt.getUTCMonth() + 1)}-${pad(dt.getUTCDate())} 00:00`);
        };
        const dow = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].indexOf(p.weekday);
        const { year: y, month: m, day: d } = p;
        const [start, end] = {
            today:      [midnight(y, m, d), nowMs],
            yesterday:  [midnight(y, m, d - 1), midnight(y, m, d)],
            this_week:  [midnight(y, m, d - dow), nowMs],
            last_week:  [midnight(y, m, d - dow - 7), midnight(y, m, d - dow)],
            this_month: [midnight(y, m, 1), nowMs],
            last_month: [midnight(y, m - 1, 1), midnight(y, m, 1)],
        }[type];
        return { start: new Date(start).toISOString(), end: new Date(end).toISOString() };
    },

    // False for ranges that close in the past, where a "last N" widening would miss them.
    endsNow(type = this.state.type) {
        return !['custom', 'yesterday', 'last_week', 'last_month'].includes(type);
    },

    // One Start/End bound: "now", an offset back from now ("7d", "90m"), or a
    // wall clock in the display zone ("2026-10-20 08:00", "2026-10-20").
    _parseBound(str, nowMs) {
        const s = String(str || '').trim();
        if (/^now$/i.test(s)) return { ms: nowMs, now: true };
        const rel = /^-?(\d+)\s*([mhdw])$/i.exec(s);
        if (rel) {
            const n = parseInt(rel[1], 10);
            const unit = this._unitBySuffix[rel[2].toLowerCase()];
            const ms = nowMs - n * this._unitMs[unit];
            return n > 0 && ms >= this.ALL_START_MS ? { ms, n, unit } : null;
        }
        const ms = TZ.parseWallClock(/^\d{4}-\d{2}-\d{2}$/.test(s) ? `${s} 00:00` : s);
        return Number.isFinite(ms) ? { ms } : null;
    },

    // Picker state for the Start/End inputs, or null when they do not form a
    // range. Unedited inputs keep the current range, and an offset ending now
    // stays relative (or its preset) so it rolls forward on re-run.
    parseInputs(startStr, endStr, nowMs = Date.now()) {
        const shown = this.boundsText(this.state, nowMs);
        if (String(startStr || '').trim() === shown.start && (String(endStr || '').trim() || 'now') === shown.end) {
            return { type: this.state.type };
        }
        const start = this._parseBound(startStr, nowMs);
        const end = this._parseBound(endStr || 'now', nowMs);
        if (!start || !end || start.ms >= end.ms) return null;
        if (end.now && start.n) {
            const span = start.n * this._unitMs[start.unit];
            const preset = Object.keys(this._presetMs).find(k => this._presetMs[k] === span);
            return preset ? { type: preset } : { type: 'relative', relativeN: start.n, relativeUnit: start.unit };
        }
        return { type: 'custom', customStart: new Date(start.ms).toISOString(), customEnd: new Date(end.ms).toISOString() };
    },

    // What the Start/End inputs show for a state, in the syntax they accept.
    boundsText(state, nowMs = Date.now()) {
        const { type } = state;
        if (this.isPreset(type)) return { start: type, end: 'now' };
        if (type === 'relative') {
            return { start: `${state.relativeN}${this._unitShort[state.relativeUnit] || 'h'}`, end: 'now' };
        }
        const range = type === 'custom'
            ? { start: state.customStart, end: state.customEnd }
            : this.calendarRange(type, nowMs);
        if (range && range.start && range.end) {
            const secs = [range.start, range.end].some(v => TZ.parts(v)?.second);
            const fmt = v => TZ.formatInput(v, secs);
            return { start: fmt(range.start), end: this.endsNow(type) ? 'now' : fmt(range.end) };
        }
        return { start: '', end: 'now' };
    },

    getTimeRange() {
        const { type, relativeN, relativeUnit, customStart, customEnd } = this.state;
        const now = new Date();

        if (type === 'custom') {
            if (customStart && customEnd) return { start: customStart, end: customEnd };
            return { start: new Date(now - 86400000).toISOString(), end: now.toISOString() };
        }

        if (type === 'relative') {
            const ms = relativeN * (this._unitMs[relativeUnit] || this._unitMs.hours);
            const start = Math.max(now - ms, this.ALL_START_MS);
            return { start: new Date(Number.isFinite(start) ? start : now - 86400000).toISOString(), end: now.toISOString() };
        }

        if (this.isCalendar(type)) return this.calendarRange(type, now.getTime());

        if (type === 'all') {
            return { start: new Date(this.ALL_START_MS).toISOString(), end: now.toISOString() };
        }

        // Live reads arrivals itself; anything asking for a plain range gets its window.
        if (type === 'live') {
            return { start: new Date(now - 15 * 60 * 1000).toISOString(), end: now.toISOString() };
        }

        const ms = this.isPreset(type) ? this._presetMs[type] : this._presetMs['24h'];
        return { start: new Date(now - ms).toISOString(), end: now.toISOString() };
    },

    getLabel() {
        const { type, relativeN, relativeUnit, customStart, customEnd } = this.state;
        if (type === 'all') return 'All Time';
        if (type === 'live') return 'Live';
        if (this.isPreset(type)) return `Last ${type}`;
        if (this.isCalendar(type)) return this.CALENDAR_LABELS[type];
        if (type === 'relative') return `Last ${relativeN}${this._unitShort[relativeUnit] || relativeUnit[0]}`;
        if (type === 'custom') {
            if (customStart && customEnd) {
                const fmt = d => {
                    const p = TZ.parts(d);
                    return p ? `${TZ.MONTHS[p.month - 1]} ${p.day}` : '';
                };
                return `${fmt(customStart)} – ${fmt(customEnd)}`;
            }
            return 'Custom';
        }
        return 'Last 24h';
    },

    setState(newState, silent) {
        // Range types arrive from URLs and storage; an unknown one changes nothing.
        if ('type' in newState && !this.isValidType(newState.type)) return;
        Object.assign(this.state, newState);
        // Leaving the Live range ends Live, whether or not a search runs next.
        if (this.state.type !== 'live' && window.LiveSearch && LiveSearch.active) LiveSearch.stop();
        this._updateLabel();
        this._updateActivePreset();
        this._syncInputs();
        if (!silent) this.saveToStorage();
    },

    // applyState restores a full time-range selection (from a saved query,
    // history entry, or share link) into the picker.
    // `state` uses camelCase: { type, relativeN, relativeUnit, customStart, customEnd }.
    applyState(state, silent) {
        if (!state || !state.type) return;
        const newState = { type: state.type };
        if (state.type === 'custom' && state.customStart && state.customEnd) {
            newState.customStart = state.customStart;
            newState.customEnd = state.customEnd;
        } else if (state.type === 'relative' && state.relativeN) {
            newState.relativeN = parseInt(state.relativeN, 10);
            newState.relativeUnit = state.relativeUnit || 'hours';
        }
        this.setState(newState, silent);
    },

    // serializeRange snapshots the current selection into the snake_case shape
    // persisted with saved queries and query history. Only the fields relevant
    // to the active type are populated.
    serializeRange() {
        const { type, relativeN, relativeUnit, customStart, customEnd } = this.state;
        const out = { time_range: type };
        if (type === 'custom') {
            out.custom_start = customStart || '';
            out.custom_end = customEnd || '';
        } else if (type === 'relative') {
            out.relative_n = relativeN;
            out.relative_unit = relativeUnit;
        }
        return out;
    },

    _storageKey(suffix) {
        const fractalId = window.FractalContext?.currentFractal?.id
            || window.FractalSelector?.getCurrentFractalId?.()
            || 'default';
        return `bifract_${suffix}_${fractalId}`;
    },

    saveToStorage() {
        try {
            const { type, relativeN, relativeUnit, customStart, customEnd } = this.state;
            localStorage.setItem(this._storageKey('time_range_type'), type);
            if (type === 'custom') {
                localStorage.setItem(this._storageKey('custom_time_range'), JSON.stringify({ start: customStart, end: customEnd }));
            } else if (type === 'relative') {
                localStorage.setItem(this._storageKey('relative_time_range'), JSON.stringify({ n: relativeN, unit: relativeUnit }));
            }
        } catch (e) {}
    },

    restoreFromStorage() {
        try {
            const type = localStorage.getItem(this._storageKey('time_range_type')) || '24h';
            const newState = { type };

            if (type === 'custom') {
                const saved = localStorage.getItem(this._storageKey('custom_time_range'));
                if (saved) {
                    const range = JSON.parse(saved);
                    newState.customStart = range.start;
                    newState.customEnd = range.end;
                }
            } else if (type === 'relative') {
                const saved = localStorage.getItem(this._storageKey('relative_time_range'));
                if (saved) {
                    const { n, unit } = JSON.parse(saved);
                    newState.relativeN = n;
                    newState.relativeUnit = unit;
                }
            }

            this.setState(newState, true);
        } catch (e) {}
    },

    // The Start/End inputs always show the applied range. Wall clocks are in
    // the display zone, matching the results table.
    _syncInputs() {
        const text = this.boundsText(this.state);
        for (const [id, value] of [['tpStart', text.start], ['tpEnd', text.end]]) {
            const el = document.getElementById(id);
            if (!el) continue;
            el.value = value;
            this._markInvalid(el, false);
        }
    },

    _markInvalid(el, invalid) {
        el.classList.toggle('invalid', invalid);
        el.setAttribute('aria-invalid', String(invalid));
    },

    _updateLabel() {
        const el = document.getElementById('timePickerLabel');
        if (el) el.textContent = this.getLabel();
        const btn = document.getElementById('timePickerBtn');
        if (btn) btn.classList.toggle('is-live', this.state.type === 'live');
    },

    _updateActivePreset() {
        document.querySelectorAll('#timePickerPanel .tp-preset').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.value === this.state.type);
        });
    },

    open() {
        // Start/End dates are read in the display zone, so the panel says which.
        const zoneTag = document.getElementById('tpZone');
        if (zoneTag) zoneTag.textContent = TZ.abbrev();
        this._syncInputs();
        const panel = document.getElementById('timePickerPanel');
        const backdrop = document.getElementById('timePickerBackdrop');
        const btn = document.getElementById('timePickerBtn');
        if (panel) panel.style.display = 'block';
        if (backdrop) backdrop.style.display = 'block';
        if (btn) btn.classList.add('active');
    },

    close() {
        const panel = document.getElementById('timePickerPanel');
        const backdrop = document.getElementById('timePickerBackdrop');
        const btn = document.getElementById('timePickerBtn');
        if (panel) panel.style.display = 'none';
        if (backdrop) backdrop.style.display = 'none';
        if (btn) btn.classList.remove('active');
    },

    toggle() {
        const panel = document.getElementById('timePickerPanel');
        if (!panel) return;
        if (panel.style.display === 'none' || !panel.style.display) {
            this.open();
        } else {
            this.close();
        }
    },

    _applyAndExecute(newState) {
        this.setState(newState);
        this.close();
        if (window.QueryExecutor) QueryExecutor.execute();
    },

    init() {
        const btn = document.getElementById('timePickerBtn');
        const backdrop = document.getElementById('timePickerBackdrop');

        if (btn) {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggle();
            });
        }

        if (backdrop) {
            backdrop.addEventListener('click', () => this.close());
        }

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') this.close();
        });

        document.querySelectorAll('#timePickerPanel .tp-preset').forEach(presetBtn => {
            presetBtn.addEventListener('click', () => {
                this._applyAndExecute({ type: presetBtn.dataset.value });
            });
        });

        const startEl = document.getElementById('tpStart');
        const endEl = document.getElementById('tpEnd');
        const applyRange = () => {
            const next = this.parseInputs(startEl?.value, endEl?.value);
            if (next) {
                this._applyAndExecute(next);
                return;
            }
            // Flag whichever bound does not parse, or both when they are inverted.
            const bad = [startEl, endEl].filter((el, i) => !this._parseBound(el?.value || (i ? 'now' : ''), Date.now()));
            (bad.length ? bad : [startEl, endEl]).forEach(el => el && this._markInvalid(el, true));
        };
        document.getElementById('tpRangeApply')?.addEventListener('click', applyRange);
        [startEl, endEl].forEach(el => {
            el?.addEventListener('keydown', e => { if (e.key === 'Enter') applyRange(); });
            el?.addEventListener('input', () => this._markInvalid(el, false));
        });

        this._updateLabel();
        this._updateActivePreset();
    },
};

window.TimePicker = TimePicker;
