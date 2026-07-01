// Unified query-variable system shared by search, dashboards and notebooks.
//
// A variable is written as @name in a query (name matches [A-Za-z_][A-Za-z0-9_]*).
// Variables are NOT added by hand: a VariableManager scans the query text (or, for
// dashboards/notebooks, every widget/section query), auto-adds any @name it finds
// with a default value of "*", and drops any variable no longer referenced. Values
// the user sets are remembered with the saved artifact.
//
// Substitution is authoritative on the backend (pkg/bqlvars); the scan/substitute
// helpers here mirror those exact rules (quote-aware, boundary-aware, prefix-safe)
// so the tray reflects precisely what the server will expand.

class VariableManager {
    // opts:
    //   container : element or element id that hosts the tray
    //   onChange(name, value) : a value input changed (caller persists / re-runs)
    //   onVarsChanged(names)  : the variable SET changed (added/removed)
    //   autoRun               : reserved hint for callers (unused here)
    constructor(opts = {}) {
        this.container = typeof opts.container === 'string'
            ? document.getElementById(opts.container)
            : opts.container || null;
        this.onChange = opts.onChange || null;
        this.onVarsChanged = opts.onVarsChanged || null;
        this.onOverlayClear = opts.onOverlayClear || null;
        this.autoRun = !!opts.autoRun;
        this.values = new Map(); // name -> value (insertion order = display order)
        // Transient display-only overlay (e.g. a pivot drilldown). When set, the
        // affected pills SHOW the overlay value and lock, but this.values (the
        // persisted set) is untouched, so a drilldown never rewrites the defaults.
        this.displayOverlay = null; // Map name -> value, or null
    }

    // Show a transient overlay of values (drilldown). Does not mutate this.values.
    setDisplayOverlay(map) {
        this.displayOverlay = (map && map.size) ? map : null;
        this.render();
    }

    clearDisplayOverlay() {
        if (!this.displayOverlay) return;
        this.displayOverlay = null;
        this.render();
    }

    // ---- static scanning (mirrors pkg/bqlvars) ----

    static _isNameStart(ch) { return ch === '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'); }
    static _isNamePart(ch) { return VariableManager._isNameStart(ch) || (ch >= '0' && ch <= '9'); }

    // _scan walks text and invokes cb(name, start, end) for every @name that is
    // outside any quoted string and not preceded by a word char. Quoted regions
    // (single or double, backslash-escaped) are opaque, so text like
    // "user@example.com" is never treated as a variable.
    static _scan(text, cb) {
        if (!text || text.indexOf('@') === -1) return;
        let quote = '';
        for (let i = 0; i < text.length;) {
            const c = text[i];
            if (quote) {
                if (c === '\\') { i += 2; continue; }
                if (c === quote) quote = '';
                i++;
                continue;
            }
            if (c === '"' || c === "'") { quote = c; i++; continue; }
            if (c === '@' && !(i > 0 && VariableManager._isNamePart(text[i - 1]))) {
                const j = i + 1;
                if (j < text.length && VariableManager._isNameStart(text[j])) {
                    let k = j + 1;
                    while (k < text.length && VariableManager._isNamePart(text[k])) k++;
                    cb(text.substring(j, k), i, k);
                    i = k;
                    continue;
                }
            }
            i++;
        }
    }

    // detectNames returns the distinct @names referenced in text (a string or an
    // array of strings), in first-seen order.
    static detectNames(text) {
        const texts = Array.isArray(text) ? text : [text];
        const seen = new Set();
        const names = [];
        for (const t of texts) {
            VariableManager._scan(t || '', (name) => {
                if (!seen.has(name)) { seen.add(name); names.push(name); }
            });
        }
        return names;
    }

    // substitute expands @name tokens in query using values (a Map, plain object,
    // or [{name,value}] array). Empty value -> "*"; unknown names left intact.
    // Client-side preview only; execution substitutes server-side.
    static substitute(query, values) {
        const map = VariableManager._asMap(values);
        if (!map.size || !query || query.indexOf('@') === -1) return query;
        let out = '';
        let last = 0;
        VariableManager._scan(query, (name, start, end) => {
            if (!map.has(name)) return;
            let val = map.get(name);
            if (val === '' || val == null) val = '*';
            out += query.substring(last, start) + val;
            last = end;
        });
        return out + query.substring(last);
    }

    static _asMap(values) {
        if (values instanceof Map) return values;
        const m = new Map();
        if (Array.isArray(values)) {
            for (const v of values) { if (v && v.name) m.set(v.name, v.value); }
        } else if (values && typeof values === 'object') {
            for (const k of Object.keys(values)) m.set(k, values[k]);
        }
        return m;
    }

    // ---- instance state ----

    isEmpty() { return this.values.size === 0; }

    // serialize returns the [{name,value}] shape persisted in the variables column.
    serialize() {
        const out = [];
        for (const [name, value] of this.values) out.push({ name, value: value == null ? '' : value });
        return out;
    }

    // load replaces the current set from a persisted [{name,value}] array.
    load(arr) {
        this.values = new Map();
        if (Array.isArray(arr)) {
            for (const v of arr) {
                if (v && typeof v.name === 'string' && v.name) {
                    this.values.set(v.name, v.value == null ? '*' : String(v.value));
                }
            }
        }
        this.render();
    }

    setValue(name, value) {
        if (this.values.has(name)) this.values.set(name, value);
    }

    getValue(name) { return this.values.get(name); }

    // syncFromText reconciles the variable set against the current query text
    // (string or array of strings): adds new @names (default "*"), preserves
    // existing values, drops names no longer referenced anywhere. Order follows
    // first appearance. Returns true if the set of names changed.
    syncFromText(text) {
        const names = VariableManager.detectNames(text);
        const nameSet = new Set(names);
        const prevKeys = Array.from(this.values.keys());
        let changed = false;

        for (const existing of prevKeys) {
            if (!nameSet.has(existing)) { this.values.delete(existing); changed = true; }
        }
        const next = new Map();
        for (const name of names) {
            if (this.values.has(name)) {
                next.set(name, this.values.get(name));
            } else {
                next.set(name, '*');
                changed = true;
            }
        }
        this.values = next;

        if (changed) {
            this.render();
            if (this.onVarsChanged) this.onVarsChanged(names);
        } else {
            // Membership and values are unchanged, but the user may have reordered
            // the @vars in the query; repaint so the tray order tracks the query
            // (display only -- no persist, since order is not semantic).
            const orderChanged = prevKeys.length === names.length &&
                prevKeys.some((k, idx) => k !== names[idx]);
            if (orderChanged) this.render();
        }
        return changed;
    }

    // ---- rendering ----

    render() {
        if (!this.container) return;
        if (this.values.size === 0) {
            this.container.innerHTML = '';
            this.container.classList.remove('has-vars');
            return;
        }
        this.container.classList.add('has-vars');

        const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
            .replace(/>/g, '&gt;').replace(/"/g, '&quot;');

        // Preserve an in-progress edit: if one of our value inputs is focused
        // (e.g. a remote SSE update reconciles the set mid-type), remember it so
        // we can restore focus + caret + uncommitted text after the rebuild.
        const active = document.activeElement;
        let restore = null;
        if (active && active.classList && active.classList.contains('variable-value-input')
            && this.container.contains(active)) {
            restore = { name: active.getAttribute('data-var-name'), value: active.value,
                        start: active.selectionStart, end: active.selectionEnd };
        }

        const ov = this.displayOverlay;
        let html = '<div class="variables-bar-items">';
        for (const [name, value] of this.values) {
            const safeName = esc(name);
            const overridden = ov && ov.has(name);
            const dispVal = overridden ? ov.get(name) : value;
            // In a drilldown, overridden pills show the drilldown value with a
            // distinct style and a clear (x) that exits the drilldown; they are
            // read-only so an accidental edit can't persist the transient value.
            const pillClass = overridden ? 'variable-pill drilldown-override' : 'variable-pill';
            const ro = overridden ? ' readonly' : '';
            const clearBtn = overridden
                ? `<button type="button" class="variable-drilldown-clear" title="Exit drilldown" aria-label="Exit drilldown for ${safeName}">&#x2715;</button>`
                : '';
            html += `<div class="${pillClass}">
                <span class="variable-name">@${safeName}</span>
                <input type="text" class="variable-value-input" value="${esc(dispVal)}"
                    data-var-name="${safeName}" spellcheck="false"${ro}
                    aria-label="Value for variable ${safeName}" />
                ${clearBtn}
            </div>`;
        }
        html += '</div>';
        this.container.innerHTML = html;

        if (restore) {
            const again = this.container.querySelector(`.variable-value-input[data-var-name="${CSS.escape(restore.name)}"]`);
            if (again) {
                again.value = restore.value;
                again.focus();
                try { again.setSelectionRange(restore.start, restore.end); } catch (e) { /* ignore */ }
            }
        }

        // Wire inputs programmatically so multiple managers can coexist without
        // colliding on a single global handler.
        this.container.querySelectorAll('.variable-value-input').forEach((input) => {
            if (input.readOnly) return; // locked overlay pill: no edit/persist
            const name = input.getAttribute('data-var-name');
            input.addEventListener('change', () => {
                this.values.set(name, input.value);
                if (this.onChange) this.onChange(name, input.value);
            });
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') { e.preventDefault(); input.blur(); }
            });
        });

        this.container.querySelectorAll('.variable-drilldown-clear').forEach((btn) => {
            btn.addEventListener('click', () => { if (this.onOverlayClear) this.onOverlayClear(); });
        });
    }
}

window.VariableManager = VariableManager;
