// Display timezone. Everything Bifract stores is UTC; this module is the single
// place that decides how a UTC instant is rendered, and how a wall-clock string
// the user types is interpreted. The zone lives on the user's account so it
// follows them across devices, with a localStorage mirror so the first paint
// after a reload does not flash UTC before /auth/user answers.
const TZ = {
    STORAGE_KEY: 'bifract-timezone',
    RECENT_KEY: 'bifract-timezone-recent',
    EVENT: 'bifract:timezone',

    _zone: 'UTC',
    _fmtCache: new Map(),

    init() {
        try {
            const saved = localStorage.getItem(this.STORAGE_KEY);
            if (saved && this._valid(saved)) this._zone = saved;
        } catch (e) { /* private mode */ }
    },

    zone() { return this._zone; },
    isUTC() { return this._zone === 'UTC'; },

    // Adopt the server's stored zone without writing it back. Returns true when
    // this changed the active zone, so the caller can re-render.
    adopt(zone) {
        if (!zone || !this._valid(zone) || zone === this._zone) return false;
        this._zone = zone;
        this._fmtCache.clear();
        try { localStorage.setItem(this.STORAGE_KEY, zone); } catch (e) { /* private mode */ }
        this._announce();
        return true;
    },

    // Change the zone and persist it to the account. Applies locally first so
    // the UI never waits on the round trip; a failed save surfaces as a toast
    // and leaves the local zone in place until the next page load reconciles.
    async setZone(zone) {
        if (!this._valid(zone)) return false;
        const changed = zone !== this._zone;
        this._zone = zone;
        this._fmtCache.clear();
        try {
            localStorage.setItem(this.STORAGE_KEY, zone);
            this._pushRecent(zone);
        } catch (e) { /* private mode */ }
        if (changed) this._announce();

        try {
            const res = await fetch('/api/v1/auth/preferences', {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ display_timezone: zone })
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok || !data.success) throw new Error(data.error || 'save failed');
        } catch (e) {
            if (window.Toast) Toast.error('Timezone applied for this session but could not be saved to your account.');
        }
        return true;
    },

    _announce() {
        document.dispatchEvent(new CustomEvent(this.EVENT, { detail: { zone: this._zone } }));
    },

    _valid(zone) {
        if (!zone || typeof zone !== 'string' || zone.length > 64) return false;
        try {
            new Intl.DateTimeFormat('en-US', { timeZone: zone });
            return true;
        } catch (e) {
            return false;
        }
    },

    // -- Parsing -----------------------------------------------------------

    // Milliseconds since epoch, or NaN. A bare "YYYY-MM-DD HH:MM:SS" carries no
    // zone and is always UTC here: that is what ClickHouse returns for a
    // timestamp column, and it is what the native Date parser gets wrong (it
    // reads such a string as browser-local).
    toEpoch(value) {
        if (value === null || value === undefined || value === '') return NaN;
        if (value instanceof Date) return value.getTime();
        if (typeof value === 'number') return value;
        const s = String(value).trim();
        if (/^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?$/.test(s)) {
            const iso = s.replace(' ', 'T');
            return Date.parse(iso.length === 10 ? iso + 'T00:00:00Z' : iso + 'Z');
        }
        return Date.parse(s);
    },

    // Interpret "YYYY-MM-DD HH:MM[:SS]" as a wall clock in the display zone.
    // Solves for the instant whose rendering in that zone matches the input,
    // which is the only way to do this correctly across a DST change: the
    // offset depends on the answer. Two corrections converge everywhere; a
    // time inside a spring-forward gap resolves to the instant after it.
    parseWallClock(str) {
        if (!str) return NaN;
        const m = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(String(str).trim());
        if (!m) return NaN;
        const naive = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +(m[6] || 0));
        if (this.isUTC()) return naive;
        let guess = naive;
        for (let i = 0; i < 3; i++) {
            const next = naive - this._offsetMs(guess);
            if (next === guess) break;
            guess = next;
        }
        return guess;
    },

    // Zone offset from UTC at a given instant, in milliseconds.
    _offsetMs(epochMs) {
        const p = this.parts(epochMs);
        if (!p) return 0;
        const asIfUTC = Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second);
        return asIfUTC - Math.floor(epochMs / 1000) * 1000;
    },

    // -- Formatting --------------------------------------------------------

    _formatter(key, options) {
        const cacheKey = this._zone + '|' + key;
        let f = this._fmtCache.get(cacheKey);
        if (!f) {
            f = new Intl.DateTimeFormat('en-US', Object.assign({ timeZone: this._zone }, options));
            this._fmtCache.set(cacheKey, f);
        }
        return f;
    },

    // Calendar parts of an instant as seen in the display zone.
    parts(value) {
        const ms = this.toEpoch(value);
        if (!Number.isFinite(ms)) return null;
        const f = this._formatter('parts', {
            year: 'numeric', month: '2-digit', day: '2-digit',
            hour: '2-digit', minute: '2-digit', second: '2-digit',
            weekday: 'short', hourCycle: 'h23'
        });
        const out = { ms };
        for (const p of f.formatToParts(ms)) {
            if (p.type === 'weekday') out.weekday = p.value;
            else if (p.type !== 'literal') out[p.type] = parseInt(p.value, 10);
        }
        out.month = out.month || 1;
        return out;
    },

    // Short zone label for the current zone at a given instant ("UTC", "MDT").
    abbrev(value) {
        if (this.isUTC()) return 'UTC';
        const ms = value === undefined ? Date.now() : this.toEpoch(value);
        if (!Number.isFinite(ms)) return this._zone;
        const f = this._formatter('abbrev', { timeZoneName: 'short' });
        const part = f.formatToParts(ms).find(p => p.type === 'timeZoneName');
        return part ? part.value : this._zone;
    },

    // Unambiguous offset label ("UTC+00:00", "UTC-06:00").
    offsetLabel(value) {
        const ms = value === undefined ? Date.now() : this.toEpoch(value);
        if (!Number.isFinite(ms)) return '';
        const mins = Math.round(this._offsetMs(ms) / 60000);
        const sign = mins < 0 ? '-' : '+';
        const a = Math.abs(mins);
        return `UTC${sign}${String(Math.floor(a / 60)).padStart(2, '0')}:${String(a % 60).padStart(2, '0')}`;
    },

    // Render an instant. Styles:
    //   datetime  2026-08-25 14:23:11   (default, matches the results table)
    //   minute    2026-08-25 14:23
    //   date      2026-08-25
    //   time      14:23:11
    //   full      2026-08-25 14:23:11 MDT
    //   friendly  Aug 25, 2026 14:23
    format(value, style) {
        style = style || 'datetime';
        // A ClickHouse timestamp displayed in UTC needs no conversion at all,
        // which keeps the default install off the Intl path entirely.
        if (this.isUTC() && typeof value === 'string') {
            const m = /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})(:\d{2})?/.exec(value.trim());
            if (m) {
                const sec = m[3] || ':00';
                if (style === 'datetime') return `${m[1]} ${m[2]}${sec}`;
                if (style === 'minute') return `${m[1]} ${m[2]}`;
                if (style === 'date') return m[1];
                if (style === 'time') return `${m[2]}${sec}`;
                if (style === 'full') return `${m[1]} ${m[2]}${sec} UTC`;
            }
        }
        const p = this.parts(value);
        if (!p) return value === null || value === undefined || value === '' ? '' : String(value);
        const p2 = n => String(n).padStart(2, '0');
        const date = `${p.year}-${p2(p.month)}-${p2(p.day)}`;
        const hm = `${p2(p.hour)}:${p2(p.minute)}`;
        const hms = `${hm}:${p2(p.second)}`;
        switch (style) {
            case 'minute': return `${date} ${hm}`;
            case 'date': return date;
            case 'time': return hms;
            case 'full': return `${date} ${hms} ${this.abbrev(p.ms)}`;
            case 'friendly': return `${this.MONTHS[p.month - 1]} ${p.day}, ${p.year} ${hm}`;
            default: return `${date} ${hms}`;
        }
    },

    MONTHS: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],

    // Hover text for a rendered timestamp. Always states the zone, and states
    // the UTC value alongside it when they differ, so a displayed time can be
    // reconciled against a raw log or another tool without a second lookup.
    title(value) {
        const ms = this.toEpoch(value);
        if (!Number.isFinite(ms)) return '';
        const shown = `${this.format(ms, 'full')} (${this.offsetLabel(ms)})`;
        if (this.isUTC()) return shown;
        const saved = this._zone;
        this._zone = 'UTC';
        const utc = this.format(ms, 'datetime');
        this._zone = saved;
        return `${shown}\n${utc} UTC`;
    },

    // "YYYY-MM-DD HH:MM" in the display zone, for the absolute-range inputs.
    formatInput(value, withSeconds) {
        return this.format(value, withSeconds ? 'datetime' : 'minute');
    },

    // -- Zone catalogue ----------------------------------------------------

    browserZone() {
        try { return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'; } catch (e) { return 'UTC'; }
    },

    zoneList() {
        if (this._zones) return this._zones;
        let list = [];
        try {
            if (typeof Intl.supportedValuesOf === 'function') list = Intl.supportedValuesOf('timeZone');
        } catch (e) { /* older engine */ }
        if (!list.length) list = this.FALLBACK_ZONES.slice();
        if (!list.includes('UTC')) list.unshift('UTC');
        this._zones = list;
        return list;
    },

    // Only reached on an engine without Intl.supportedValuesOf. Any IANA name
    // still works if set from another device; this is just what the picker offers.
    FALLBACK_ZONES: [
        'UTC', 'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles',
        'America/Sao_Paulo', 'Europe/London', 'Europe/Paris', 'Europe/Berlin', 'Europe/Moscow',
        'Asia/Dubai', 'Asia/Kolkata', 'Asia/Shanghai', 'Asia/Tokyo', 'Asia/Singapore',
        'Australia/Sydney', 'Pacific/Auckland'
    ],

    recent() {
        try { return JSON.parse(localStorage.getItem(this.RECENT_KEY)) || []; } catch (e) { return []; }
    },

    _pushRecent(zone) {
        const list = this.recent().filter(z => z !== zone);
        list.unshift(zone);
        localStorage.setItem(this.RECENT_KEY, JSON.stringify(list.slice(0, 5)));
    }
};

TZ.init();
window.TZ = TZ;
