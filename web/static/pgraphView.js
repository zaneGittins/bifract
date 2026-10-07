// Pure helpers behind the pgraph view (queryExecutor.js, _pg*): which processes the
// anomalous-path view keeps, the fact chips that explain a pgr score, Table time labels,
// and search pivots. No DOM access, so it is unit-tested in Node (test/e2e/pgraph-unit.spec.js).
const PgView = {
    MONTHS: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    DAY: 86400000,

    // ---- Anomalous path ----

    // Processes that own or receive an edge at or above the anomaly line, plus extra anchors
    // (seed, novel roots, revealed nodes). m is the _pgBuildModel shape.
    pathAnchors(m, isHot, extra) {
        const out = new Set();
        const add = (g) => { if (g != null && m.procSet.has(g)) out.add(g); };
        m.anomalyByNode.forEach((a, id) => { if (isHot(a)) add(id); });
        m.leafGroups.forEach((grp, g) => {
            if (['file', 'net', 'dns'].some(t => (grp[t] || []).some(x => isHot(x.anomaly)))) add(g);
        });
        m.interactions.forEach((list, src) => list.forEach(it => {
            if (isHot(it.anomaly)) { add(src); add(it.target); }
        }));
        (extra || []).forEach(add);
        return out;
    },

    // Anchors plus every spawn ancestor: the path from a root down to each anchor.
    pathKeep(anchors, parentOf) {
        const keep = new Set();
        anchors.forEach(g => {
            let c = g;
            while (c != null && !keep.has(c)) { keep.add(c); c = parentOf.get(c); }
        });
        return keep;
    },

    // Splits display children ({kind:'proc'|'agg', id, members}) into shown entries and the
    // process count folded into one "+N hidden" marker. An aggregate stays while any member does.
    splitChildren(entries, shown, size) {
        const vis = [];
        let hidden = 0, branches = 0;
        entries.forEach(e => {
            const keep = e.kind === 'agg' ? (e.members || []).some(shown) : shown(e.id);
            if (keep) { vis.push(e); return; }
            branches++;
            hidden += e.kind === 'agg' ? (e.members || []).reduce((s, mm) => s + size(mm), 0) : size(e.id);
        });
        return { vis, hidden, branches };
    },

    // ---- Reconnected peers ----

    // Groups reconnected peer trees under the bridge that links them toward the home tree.
    // bridges: [{ key, type, label, anomaly, owners: [guid] }]; rootOf(guid) is the tree root;
    // peers: the non-home tree roots; score(root) is a peer tree's peak anomaly. A breadth-first
    // walk from home over trees and bridges (strongest bridge first) gives each peer the strongest
    // bridge reaching it from a tree nearer home, so a chain of peers still reads outward from the
    // investigation. Groups come strongest first, peers by score; unreachable peers go to '#other'.
    peerGroups(bridges, rootOf, home, peers, score) {
        const strength = (b) => (Number.isFinite(b.anomaly) ? b.anomaly : 0);
        const order = bridges
            .map(b => Object.assign({}, b, { trees: Array.from(new Set(b.owners.map(rootOf))) }))
            .filter(b => b.trees.length >= 2)
            .sort((x, y) => strength(y) - strength(x) || x.trees.length - y.trees.length || (x.key < y.key ? -1 : x.key > y.key ? 1 : 0));
        const peerSet = new Set(peers);
        const reached = new Set([home]);
        const assign = new Map(); // peer root -> bridge
        let frontier = new Set([home]);
        while (frontier.size) {
            const next = new Set();
            order.forEach(b => {
                if (!b.trees.some(t => frontier.has(t))) return;
                b.trees.forEach(t => {
                    if (reached.has(t) || !peerSet.has(t)) return;
                    reached.add(t); assign.set(t, b); next.add(t);
                });
            });
            frontier = next;
        }
        const groups = new Map();
        const groupOf = (b) => {
            if (!groups.has(b.key)) groups.set(b.key, { key: b.key, type: b.type, label: b.label, anomaly: b.anomaly, rank: b.rank, owners: b.owners || [], from: [], peers: [] });
            return groups.get(b.key);
        };
        peers.forEach(r => {
            const b = assign.get(r);
            if (!b) { groupOf({ key: '#other', type: 'other', label: 'Other linked trees', anomaly: NaN, rank: Infinity }).peers.push({ root: r, via: r, score: score(r) }); return; }
            const via = b.owners.find(o => rootOf(o) === r);
            groupOf(Object.assign(b, { rank: order.indexOf(b) })).peers.push({ root: r, via: via != null ? via : r, score: score(r) });
        });
        const out = Array.from(groups.values());
        // The near side of each bridge: its owners outside this group's own peer trees.
        out.forEach(g => {
            const mine = new Set(g.peers.map(p => p.root));
            g.from = g.owners.filter(o => !mine.has(rootOf(o)));
        });
        out.forEach(g => g.peers.sort((x, y) => (Number.isFinite(y.score) ? y.score : -1) - (Number.isFinite(x.score) ? x.score : -1)));
        return out.sort((x, y) => (x.key === '#other') - (y.key === '#other') || x.rank - y.rank);
    },

    // "WKSTN-00235" from "WKSTN-00235.corp.example.com"; IPs stay whole.
    shortHost(h) {
        const s = String(h || '');
        return /^\d+\.\d+\.\d+\.\d+$/.test(s) ? s : s.split('.')[0];
    },

    // ---- Time ----

    // Table time label: wall clock to the second, with the date only when it differs from the
    // previous row's. parts is TZ.parts (display-zone calendar parts).
    timeLabel(ms, prevMs, parts, nowYear) {
        const p = ms == null ? null : parts(ms);
        if (!p) return null;
        const pad = n => String(n).padStart(2, '0');
        const time = `${pad(p.hour)}:${pad(p.minute)}:${pad(p.second)}`;
        const q = prevMs == null ? null : parts(prevMs);
        const sameDay = !!q && q.year === p.year && q.month === p.month && q.day === p.day;
        const date = sameDay ? '' : `${this.MONTHS[p.month - 1]} ${p.day}${p.year !== nowYear ? ', ' + p.year : ''}`;
        return { date, time };
    },

    // Signed gap between a node and its parent ("+6.9s", "-10d").
    fmtDelta(deltaMs) {
        if (deltaMs == null || isNaN(deltaMs)) return '';
        const s = deltaMs < 0 ? '-' : '+', a = Math.abs(deltaMs);
        if (a < 1000) return s + Math.round(a) + 'ms';
        if (a < 60000) return s + (a / 1000).toFixed(a < 10000 ? 1 : 0) + 's';
        if (a < 3600000) return s + Math.round(a / 60000) + 'm';
        if (a < 86400000) return s + (a / 3600000).toFixed(1) + 'h';
        return s + Math.round(a / 86400000) + 'd';
    },

    dayMs(day) { const t = Date.parse(String(day || '') + 'T00:00:00Z'); return isNaN(t) ? NaN : t; },

    // "today", "3d ago", "5mo ago".
    ago(ms, now) {
        if (!Number.isFinite(ms)) return '';
        const days = Math.floor((now - ms) / this.DAY);
        if (days <= 0) return 'today';
        if (days < 60) return days + 'd ago';
        return Math.floor(days / 30) + 'mo ago';
    },

    // Whether a relationship's first-seen day is worth a chip: 'new' when first observed inside
    // the investigation window, 'recent' when in the newest fifth of the observed baseline,
    // otherwise '' (an established relationship; repeating its age on every row is noise).
    firstSeenKind(fsMs, { windowStartMs, baselineStartMs, now }) {
        if (!Number.isFinite(fsMs)) return '';
        if (windowStartMs != null && fsMs >= windowStartMs) return 'new';
        if (Number.isFinite(baselineStartMs)) {
            const span = now - baselineStartMs;
            if (span > 0 && fsMs > baselineStartMs && now - fsMs <= span * 0.2) return 'recent';
        }
        return '';
    },

    // ---- Score explanation ----

    // Fact chips for one pgr score explanation (_pgWhyOf). ctx: { now, windowStartMs,
    // targetQuery } where targetQuery (BQL) turns "Target on" into a search pivot.
    whyChips(w, ctx = {}) {
        if (!w) return [];
        const now = ctx.now || Date.now();
        const n = v => Math.round(v).toLocaleString('en-US');
        const chips = [];
        if (w.firstSeen) {
            const t = this.dayMs(w.firstSeen);
            const isNew = ctx.windowStartMs != null && t >= ctx.windowStartMs;
            chips.push({
                label: 'First seen', value: this.ago(t, now) || w.firstSeen, tone: isNew ? 'alert' : 'muted',
                title: `Relationship first observed ${w.firstSeen}${isNew ? ', inside the window under investigation' : ''}`,
            });
        }
        if (w.totalHosts > 0) {
            const capped = w.tgtHosts >= 256;
            const rare = !capped && w.tgtHosts <= Math.max(1, w.totalHosts * 0.01);
            const uncommon = !capped && w.tgtHosts <= w.totalHosts * 0.05;
            chips.push({
                label: 'Target on', value: `${capped ? '256+' : n(w.tgtHosts)} of ${n(w.totalHosts)} hosts`,
                tone: rare ? 'alert' : uncommon ? 'warn' : '',
                title: `The target was seen on ${capped ? 'at least 256' : n(w.tgtHosts)} of ${n(w.totalHosts)} hosts in the baseline` +
                    (ctx.targetQuery ? '. Click to search for it' : ''),
                query: ctx.targetQuery || '',
            });
        }
        if (w.basis === 'transition' && w.src > 0) {
            const share = w.edge / w.src;
            chips.push({
                label: 'Other host-days', value: `${n(w.edge)} of ${n(w.src)}`,
                tone: w.edge === 0 ? 'alert' : share < 0.05 ? 'warn' : '',
                title: `Of the ${n(w.src)} host-days on which this source did the same kind of thing elsewhere, ${n(w.edge)} reached this target`,
            });
        } else if (w.basis === 'new_source') {
            chips.push({
                label: 'Target host-days', value: n(w.tgt), tone: w.tgt === 0 ? 'alert' : '',
                title: 'The source never ran on another host; host-days on which anything else touched this target',
            });
        }
        if (w.basis !== 'no_source') {
            chips.push({ label: 'Own', value: w.own.toFixed(2), title: 'Score of this edge on its own' });
            const inherited = w.final - w.own;
            if (inherited >= 0.005) {
                chips.push({
                    label: 'Inherited', value: '+' + inherited.toFixed(2), tone: 'warn',
                    title: 'Lifted by an anomalous ancestor (diffusion). Shown score = own + inherited',
                });
            }
        }
        return chips;
    },

    // ---- Search pivots ----

    bqlQuote(s) { return '"' + String(s).replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '"'; },

    // The fractal-wide search for a file/net/dns target, or '' for other kinds.
    targetQuery(type, label) {
        const f = { net: 'dst_ip', dns: 'query', file: 'target_file' }[type];
        return f && label ? `${f}=${this.bqlQuote(label)}` : '';
    },

    // Which hosts ran this image.
    imageQuery(image) {
        return image ? `bifract_category="process_creation" image=${this.bqlQuote(image)} | groupby(computer_name) | sort(_count, order=desc)` : '';
    },

    // A hash field value may be a bare digest or Sysmon's "SHA256=..,MD5=.." list; search the
    // strongest digest by substring in the latter case.
    hashQuery(field, value) {
        const v = String(value || '').trim();
        if (!field || !v) return '';
        if (!v.includes('=')) return `${field}=${this.bqlQuote(v)}`;
        const pick = /SHA256=([0-9a-f]+)/i.exec(v) || /SHA1=([0-9a-f]+)/i.exec(v) || /MD5=([0-9a-f]+)/i.exec(v) || /=([0-9a-f]{16,})/i.exec(v);
        return pick ? `${field}=~${pick[1]}` : '';
    },

    // The public /go/search deep link (pkg/deeplink). scope: { kind: 'fractal'|'prism', name }.
    searchUrl(origin, q, scope, from, to) {
        const p = new URLSearchParams();
        p.set('q', q);
        if (scope && scope.name) p.set(scope.kind === 'prism' ? 'prism' : 'fractal', scope.name);
        if (from) p.set('from', from);
        if (to) p.set('to', to);
        return `${origin}/go/search?${p.toString()}`;
    },
};

window.PgView = PgView;
