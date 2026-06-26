// BQL language registry: the single source of truth for query completion and
// the ? signature hint. Loads the canonical function/operator catalog from the
// backend (/api/v1/query/reference) so completion, hints, and the Reference tab
// never drift from what the engine actually supports.
//
// Syntax highlighting deliberately does NOT consult this module: it treats any
// identifier immediately followed by "(" as a function, mirroring the backend
// lexer, so it stays correct even before this loads and for unknown functions.
const BQLLang = {
    loaded: false,
    functions: [],
    operators: [],
    // Reserved logical keywords. These are the only reserved words in BQL
    // (the backend lexer reserves nothing else), so hardcoding the trio here is
    // not the drift-prone duplication we are eliminating.
    keywords: [
        { name: 'AND', desc: 'Logical AND' },
        { name: 'OR', desc: 'Logical OR' },
        { name: 'NOT', desc: 'Logical NOT' },
    ],

    // Base columns of the log table (always queryable, independent of the JSON
    // fields). Surfaced in completion alongside the configured schema fields.
    baseColumns: ['timestamp', 'raw_log', 'ingest_timestamp', 'fractal_id', 'log_id'],
    // Configured schema fields (project defaults + admin-defined custom), loaded
    // from the backend so completion knows useful field names before any query runs.
    schemaFields: [],

    _byName: {},
    _readyCbs: [],

    async load() {
        try {
            const res = await fetch('/api/v1/query/reference', { credentials: 'include' });
            const data = await res.json();
            this.functions = Array.isArray(data.functions) ? data.functions : [];
            this.operators = Array.isArray(data.operators) ? data.operators : [];
        } catch (e) {
            console.error('BQLLang: failed to load query reference', e);
            this.functions = [];
            this.operators = [];
        }
        try {
            const res = await fetch('/api/v1/query/fields', { credentials: 'include' });
            const data = await res.json();
            const fields = data && data.data && Array.isArray(data.data.fields) ? data.data.fields : [];
            this.schemaFields = fields;
        } catch (e) {
            this.schemaFields = [];
        }
        this._index();
        this.loaded = true;
        const cbs = this._readyCbs;
        this._readyCbs = [];
        cbs.forEach(cb => { try { cb(); } catch (e) { /* ignore */ } });
    },

    // Field names known ahead of any query: base columns + configured schema
    // fields, deduplicated, base columns first.
    knownFields() {
        const seen = new Set();
        const out = [];
        for (const f of this.baseColumns) { if (!seen.has(f)) { seen.add(f); out.push(f); } }
        for (const f of this.schemaFields) { if (f && !seen.has(f)) { seen.add(f); out.push(f); } }
        return out;
    },

    onReady(cb) {
        if (this.loaded) cb();
        else this._readyCbs.push(cb);
    },

    _index() {
        this._byName = {};
        for (const fn of this.functions) {
            if (!fn || !fn.name) continue;
            this._byName[fn.name.toLowerCase()] = fn;
            (fn.aliases || []).forEach(a => { this._byName[String(a).toLowerCase()] = fn; });
        }
    },

    getFunction(name) {
        if (!name) return null;
        return this._byName[String(name).toLowerCase()] || null;
    },

    // Clean signature for display: strip the leading "| " pipe marker from syntax.
    _signature(fn) {
        return String(fn.syntax || fn.name || '').replace(/^\s*\|\s*/, '').trim();
    },

    // Hint payload for the ? popup, derived from the canonical doc.
    getHint(name) {
        const fn = this.getFunction(name);
        if (!fn) return null;
        return {
            name: fn.name,
            signature: this._signature(fn),
            args: (fn.parameters || []).map(p => ({
                name: p.name,
                desc: p.description || '',
                required: !!p.required,
            })),
            example: (fn.examples && fn.examples[0]) || this._signature(fn),
        };
    },

    // Insert text for a completed function. Zero-arg functions get closed parens;
    // the brace-style `case` command gets a trailing space; everything else opens
    // a paren and leaves the caret inside.
    funcInsertText(fn) {
        const name = fn.name;
        // case is brace-structured, not paren-structured: scaffold the block and
        // (via funcCaretBack) drop the caret inside it.
        if (name.toLowerCase() === 'case') return name + ' {  }';
        const noParams = !fn.parameters || fn.parameters.length === 0;
        return noParams ? name + '()' : name + '(';
    },

    // How many characters from the end of the inserted text the caret should land,
    // so brace/paren scaffolds leave the cursor in the right spot. 0 = at the end.
    funcCaretBack(fn) {
        return fn.name.toLowerCase() === 'case' ? 2 : 0; // inside `case { | }`
    },
};

window.BQLLang = BQLLang;
