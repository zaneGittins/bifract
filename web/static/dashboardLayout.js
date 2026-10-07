// Grid layout engine shared by the dashboard editor and the shared wallboard.
// Widgets sit on a CSS grid of COLS columns and ROW px tracks with GAP gutters;
// positions are integers in those units ({id, x, y, w, h}).
const DashboardLayout = {
    COLS: 24,
    ROW: 18,
    GAP: 8,
    MIN_W: 3,
    MIN_H: 4,

    get PITCH() { return this.ROW + this.GAP; },

    fromWidget(w) {
        return { id: w.id, x: w.pos_x, y: w.pos_y, w: w.width, h: w.height };
    },

    fromWidgets(widgets) {
        return (widgets || []).map(w => this.fromWidget(w));
    },

    // Grid placement goes through custom properties so the narrow-screen media
    // query can restack widgets without fighting inline styles.
    place(el, it) {
        el.style.setProperty('--gx', it.x + 1);
        el.style.setProperty('--gy', it.y + 1);
        el.style.setProperty('--gw', it.w);
        el.style.setProperty('--gh', it.h);
    },

    // Reading order, used for keyboard focus and the stacked mobile layout.
    applyOrder(items, byId) {
        this.sorted(items).forEach((it, i) => {
            const el = byId(it.id);
            if (el) el.style.setProperty('--order', i);
        });
    },

    bottom(items) {
        return items.reduce((m, i) => Math.max(m, i.y + i.h), 0);
    },

    sorted(items) {
        return [...items].sort((a, b) => a.y - b.y || a.x - b.x);
    },

    collides(a, b) {
        return a.id !== b.id &&
            a.x < b.x + b.w && a.x + a.w > b.x &&
            a.y < b.y + b.h && a.y + a.h > b.y;
    },

    clamp(it) {
        it.w = Math.min(Math.max(it.w, 1), this.COLS);
        it.h = Math.max(it.h, 1);
        it.x = Math.min(Math.max(it.x, 0), this.COLS - it.w);
        it.y = Math.max(it.y, 0);
        return it;
    },

    // Float every widget up into free space, top-left first.
    compact(items) {
        const placed = [];
        for (const it of this.sorted(items)) {
            while (it.y > 0 && !placed.some(p => this.collides({ ...it, y: it.y - 1 }, p))) it.y--;
            let hit;
            while ((hit = placed.find(p => this.collides(it, p)))) it.y = hit.y + hit.h;
            placed.push(it);
        }
        return items;
    },

    // Move a widget out of the way of `item`: above it when there is room
    // (so dragging down past a neighbour swaps them), else below it.
    _displace(items, item, other, allowUp) {
        if (allowUp) {
            const up = { ...other, y: item.y - other.h };
            if (up.y >= 0 && !items.some(o => o.id !== other.id && this.collides(up, o))) {
                other.y = up.y;
                return;
            }
        }
        other.y = item.y + item.h;
        this._pushAway(items, other, false);
    },

    _pushAway(items, item, allowUp) {
        for (const o of this.sorted(items)) {
            if (this.collides(item, o)) this._displace(items, item, o, allowUp);
        }
    },

    // Return a new compacted layout with widget `id` moved/resized to `rect`.
    apply(items, id, rect) {
        const out = items.map(i => ({ ...i }));
        const it = out.find(i => i.id === id);
        if (!it) return out;
        Object.assign(it, rect);
        this.clamp(it);
        this._pushAway(out, it, true);
        return this.compact(out);
    },

    // First free slot of w x h, scanning rows top-down, so new widgets fill gaps.
    findSlot(items, w, h) {
        const bottom = this.bottom(items);
        for (let y = 0; y <= bottom; y++) {
            for (let x = 0; x + w <= this.COLS; x++) {
                const probe = { id: '\0', x, y, w, h };
                if (!items.some(o => this.collides(probe, o))) return { x, y };
            }
        }
        return { x: 0, y: bottom };
    },

    // Widgets whose placement differs between two layouts.
    diff(before, after) {
        const prev = new Map(before.map(i => [i.id, i]));
        return after.filter(i => {
            const p = prev.get(i.id);
            return !p || p.x !== i.x || p.y !== i.y || p.w !== i.w || p.h !== i.h;
        });
    },

    // Pixel geometry of a grid element.
    metrics(grid) {
        const width = grid.clientWidth;
        const colPitch = (width + this.GAP) / this.COLS;
        return {
            width,
            colPitch,
            rowPitch: this.PITCH,
            stacked: getComputedStyle(grid).gridTemplateColumns.split(' ').length < this.COLS,
        };
    },

    rectPx(m, it) {
        return {
            left: it.x * m.colPitch,
            top: it.y * m.rowPitch,
            width: it.w * m.colPitch - this.GAP,
            height: it.h * m.rowPitch - this.GAP,
        };
    },
};

window.DashboardLayout = DashboardLayout;
