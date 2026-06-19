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
        '30d': 30 * 24 * 60 * 60 * 1000,
    },

    _unitMs: {
        minutes: 60 * 1000,
        hours:   60 * 60 * 1000,
        days:    24 * 60 * 60 * 1000,
        weeks:   7 * 24 * 60 * 60 * 1000,
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
            return { start: new Date(now - ms).toISOString(), end: now.toISOString() };
        }

        if (type === 'all') {
            return { start: new Date('2000-01-01T00:00:00Z').toISOString(), end: now.toISOString(), selective: true };
        }

        const ms = this._presetMs[type] || this._presetMs['24h'];
        return { start: new Date(now - ms).toISOString(), end: now.toISOString() };
    },

    getLabel() {
        const { type, relativeN, relativeUnit, customStart, customEnd } = this.state;
        const unitShort = { minutes: 'm', hours: 'h', days: 'd', weeks: 'w' };
        const presetLabels = {
            '5m': 'Last 5m', '15m': 'Last 15m', '30m': 'Last 30m',
            '1h': 'Last 1h', '2h': 'Last 2h', '4h': 'Last 4h',
            '6h': 'Last 6h', '12h': 'Last 12h', '24h': 'Last 24h',
            '7d': 'Last 7d', '30d': 'Last 30d',
        };
        if (type === 'all') return 'All Time';
        if (type === 'relative') return `Last ${relativeN}${unitShort[relativeUnit] || relativeUnit[0]}`;
        if (type === 'custom') {
            if (customStart && customEnd) {
                const fmt = d => new Date(d).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
                return `${fmt(customStart)} – ${fmt(customEnd)}`;
            }
            return 'Custom';
        }
        return presetLabels[type] || 'Last 24h';
    },

    setState(newState, silent) {
        Object.assign(this.state, newState);
        this._updateLabel();
        this._updateActivePreset();
        if (!silent) this.saveToStorage();
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
                    const absStart = document.getElementById('tpAbsStart');
                    const absEnd = document.getElementById('tpAbsEnd');
                    if (absStart) absStart.value = this._toDatetimeLocal(range.start);
                    if (absEnd) absEnd.value = this._toDatetimeLocal(range.end);
                }
            } else if (type === 'relative') {
                const saved = localStorage.getItem(this._storageKey('relative_time_range'));
                if (saved) {
                    const { n, unit } = JSON.parse(saved);
                    newState.relativeN = n;
                    newState.relativeUnit = unit;
                    const nEl = document.getElementById('tpRelativeN');
                    const unitEl = document.getElementById('tpRelativeUnit');
                    if (nEl) nEl.value = n;
                    if (unitEl) unitEl.value = unit;
                }
            }

            this.setState(newState, true);
        } catch (e) {}
    },

    _toDatetimeLocal(iso) {
        const d = new Date(iso);
        const pad = n => String(n).padStart(2, '0');
        return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
    },

    _parseAbsInput(val) {
        if (!val) return null;
        const d = new Date(val.trim().replace(' ', 'T'));
        return isNaN(d) ? null : d.toISOString();
    },

    _updateLabel() {
        const el = document.getElementById('timePickerLabel');
        if (el) el.textContent = this.getLabel();
    },

    _updateActivePreset() {
        document.querySelectorAll('.tp-preset').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.value === this.state.type);
        });
    },

    open() {
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
        const relativeApply = document.getElementById('tpRelativeApply');
        const absApply = document.getElementById('tpAbsApply');
        const relativeNEl = document.getElementById('tpRelativeN');
        const relativeUnitEl = document.getElementById('tpRelativeUnit');

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

        document.querySelectorAll('.tp-preset').forEach(presetBtn => {
            presetBtn.addEventListener('click', () => {
                this._applyAndExecute({ type: presetBtn.dataset.value });
            });
        });

        if (relativeApply) {
            relativeApply.addEventListener('click', () => {
                const n = parseInt(relativeNEl?.value || '4', 10);
                const unit = relativeUnitEl?.value || 'hours';
                if (n > 0) this._applyAndExecute({ type: 'relative', relativeN: n, relativeUnit: unit });
            });
        }

        if (relativeNEl) {
            relativeNEl.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') relativeApply?.click();
            });
        }

        if (absApply) {
            const applyAbs = () => {
                const startEl = document.getElementById('tpAbsStart');
                const endEl = document.getElementById('tpAbsEnd');
                const start = this._parseAbsInput(startEl?.value);
                const end = this._parseAbsInput(endEl?.value);
                if (start && end) {
                    this._applyAndExecute({ type: 'custom', customStart: start, customEnd: end });
                }
            };
            absApply.addEventListener('click', applyAbs);
            document.getElementById('tpAbsStart')?.addEventListener('keydown', e => { if (e.key === 'Enter') applyAbs(); });
            document.getElementById('tpAbsEnd')?.addEventListener('keydown', e => { if (e.key === 'Enter') applyAbs(); });
        }

        this._updateLabel();
        this._updateActivePreset();
    },
};

window.TimePicker = TimePicker;
