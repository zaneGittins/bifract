// BifractFormat - Shared, chart-type-aware formatting panel.
// Used by dashboards and notebooks. The caller provides an adapter (ctx) that
// supplies the current chart type / config / cached results and handles live
// preview, cancel (restore) and save (persist). The panel never re-queries:
// formatting is presentation-only and applies against cached results.
//
// ctx = {
//   chartType: string,               // 'table' | 'singleval' | 'piechart' | 'barchart' | 'timechart' | ...
//   config:    object,               // current chart_config (parsed)
//   fields:    string[],             // field_order (optional, for series detection)
//   results:   object[],             // cached result rows (optional, for series detection)
//   onPreview(cfg): void,            // re-render from cache with cfg, no persist
//   onCancel(): void,                // restore original config + re-render
//   onSave(cfg): void|Promise        // persist cfg + re-render
// }
window.BifractFormat = {

    SWATCHES: ['#ef4444', '#f97316', '#eab308', '#22c55e', '#14b8a6', '#3b82f6', '#8b5cf6', '#ec4899'],
    PALETTE_NAMES: { default: 'Default', colorblind: 'Colorblind-safe', warm: 'Warm', cool: 'Cool', mono: 'Monochrome' },
    UNIT_OPTIONS: [
        ['number', 'Number (1.2K)'],
        ['bytes', 'Bytes (KiB/MiB)'],
        ['bytes_si', 'Bytes (KB/MB)'],
        ['duration_ms', 'Duration (from ms)'],
        ['duration_s', 'Duration (from s)'],
        ['percent', 'Percent'],
        ['none', 'Raw']
    ],

    _ctx: null,
    _cfg: null,
    _seriesLabels: [],

    open(ctx) {
        this._removePanel(true);
        this._ctx = ctx;
        this._cfg = JSON.parse(JSON.stringify(ctx.config || {}));

        const overlay = document.createElement('div');
        overlay.className = 'row-coloring-overlay';
        overlay.id = 'formatOverlay';
        overlay.addEventListener('click', () => this.cancel());
        document.body.appendChild(overlay);

        const panel = document.createElement('div');
        panel.className = 'row-coloring-panel format-panel';
        panel.id = 'formatPanel';
        panel.innerHTML = `
            <div class="panel-header">
                <h3>Format</h3>
                <button class="widget-btn" onclick="BifractFormat.cancel()" style="background:none;border:none;color:var(--text-primary);cursor:pointer;font-size:1.1rem;">&#x2715;</button>
            </div>
            <div class="panel-body" id="formatBody">${this._renderBody()}</div>
            <div class="panel-footer">
                <button class="btn-secondary" onclick="BifractFormat.cancel()">Cancel</button>
                <button class="btn-primary" onclick="BifractFormat.save()">Save</button>
            </div>
        `;
        document.body.appendChild(panel);

        requestAnimationFrame(() => {
            overlay.classList.add('open');
            panel.classList.add('open');
        });
    },

    _renderBody() {
        const t = this._ctx.chartType;
        if (t === 'table') return this._sectionRules();
        if (t === 'singleval') return this._sectionStat() + this._sectionUnit();
        if (t === 'piechart') return this._sectionColors('pie') + this._sectionLegend() + this._sectionUnit();
        if (t === 'barchart') return this._sectionColors('series') + this._sectionUnit() + this._sectionLegend();
        if (t === 'timechart') return this._sectionColors('series') + this._sectionLegend() + this._sectionUnit();
        return `<div class="fmt-empty">No formatting options for this visualization yet.</div>`;
    },

    _rerenderBody() {
        const body = document.getElementById('formatBody');
        if (body) body.innerHTML = this._renderBody();
    },

    _section(title, inner) {
        return `<div class="format-section"><div class="format-section-title">${title}</div>${inner}</div>`;
    },

    // ---- Colors ----

    _sectionColors(kind) {
        const esc = Utils.escapeHtml;
        const cfg = this._cfg;
        const fallback = kind === 'pie' ? BifractCharts.PIE_COLORS : BifractCharts.SERIES_COLORS;
        const palette = (cfg.colors && cfg.colors.palette) || 'default';
        const palArr = (palette !== 'default' && BifractCharts.PALETTES[palette]) ? BifractCharts.PALETTES[palette] : fallback;
        const overrides = (cfg.colors && cfg.colors.overrides) || {};
        const labels = this._seriesLabels = this._detectLabels();

        const paletteSelect = `
            <label class="fmt-label">Palette</label>
            <select class="fmt-select" onchange="BifractFormat.setPalette(this.value)">
                ${Object.keys(this.PALETTE_NAMES).map(p => `<option value="${p}" ${palette === p ? 'selected' : ''}>${this.PALETTE_NAMES[p]}</option>`).join('')}
            </select>`;

        let list;
        if (labels.length) {
            list = `<div class="fmt-series-list">` + labels.map((label, i) => {
                const eff = overrides[label] || palArr[i % palArr.length];
                return `<div class="fmt-series-row">
                    <span class="fmt-series-name" title="${esc(label)}">${esc(label)}</span>
                    <div class="rule-color-swatches">
                        ${this.SWATCHES.map(c => `<button type="button" class="rule-swatch${overrides[label] === c ? ' active' : ''}" style="background:${c};" onclick="BifractFormat._swatchClick(this,'series:${i}','${c}')"></button>`).join('')}
                        <input type="color" class="rule-color-custom" value="${eff}" oninput="BifractFormat._colorInput(this,'series:${i}')">
                        ${overrides[label] ? `<button type="button" class="fmt-reset" onclick="BifractFormat.resetOverride(${i})" title="Reset to palette">&#x21bb;</button>` : ''}
                    </div>
                </div>`;
            }).join('') + `</div>`;
        } else {
            list = `<div class="fmt-hint">Run the query to customize individual ${kind === 'pie' ? 'slice' : 'series'} colors.</div>`;
        }

        return this._section('Colors', paletteSelect + list);
    },

    setPalette(value) {
        this._cfg.colors = this._cfg.colors || {};
        this._cfg.colors.palette = value;
        this._rerenderBody();
        this._preview();
    },

    resetOverride(i) {
        const label = this._seriesLabels[i];
        if (this._cfg.colors && this._cfg.colors.overrides) delete this._cfg.colors.overrides[label];
        this._rerenderBody();
        this._preview();
    },

    // ---- Legend ----

    _sectionLegend() {
        const cfg = this._cfg;
        const show = !(cfg.legend && cfg.legend.show === false);
        return this._section('Legend',
            `<label class="fmt-check"><input type="checkbox" ${show ? 'checked' : ''} onchange="BifractFormat.setPath('legend.show', this.checked)"> Show legend</label>`);
    },

    // ---- Unit / value format ----

    _sectionUnit() {
        const cfg = this._cfg;
        const unit = cfg.unit || {};
        const type = unit.type || 'number';
        const dec = unit.decimals != null ? unit.decimals : '';
        return this._section('Value format', `
            <label class="fmt-label">Unit</label>
            <select class="fmt-select" onchange="BifractFormat.setPath('unit.type', this.value)">
                ${this.UNIT_OPTIONS.map(([v, l]) => `<option value="${v}" ${type === v ? 'selected' : ''}>${l}</option>`).join('')}
            </select>
            <label class="fmt-label">Decimals</label>
            <input type="number" min="0" max="6" class="fmt-input" placeholder="auto" value="${dec}" oninput="BifractFormat.setUnitDecimals(this.value)">
        `);
    },

    setUnitDecimals(v) {
        this._cfg.unit = this._cfg.unit || {};
        this._cfg.unit.decimals = (v === '' || v == null) ? null : v;
        this._preview();
    },

    // ---- Stat thresholds ----

    _sectionStat() {
        const esc = Utils.escapeHtml;
        const stat = this._cfg.stat || {};
        const mode = stat.colorMode || 'value';
        const thr = stat.thresholds || [];
        const ops = ['>=', '>', '<', '<=', '='];

        const rows = thr.map((t, i) => `
            <div class="row-coloring-rule">
                <div class="rule-row-top">
                    <select class="fmt-select-sm" onchange="BifractFormat.updThr(${i},'op',this.value)">
                        ${ops.map(o => `<option ${ (t.op || '>=') === o ? 'selected' : ''}>${o}</option>`).join('')}
                    </select>
                    <input type="number" class="fmt-input-sm" placeholder="value" value="${esc(t.value != null ? String(t.value) : '')}" oninput="BifractFormat.updThr(${i},'value',this.value)">
                    <button class="remove-rule-btn" onclick="BifractFormat.delThr(${i})">&#x2715;</button>
                </div>
                <div class="rule-color-swatches">
                    ${this.SWATCHES.map(c => `<button type="button" class="rule-swatch${t.color === c ? ' active' : ''}" style="background:${c};" onclick="BifractFormat._swatchClick(this,'thr:${i}','${c}')"></button>`).join('')}
                    <input type="color" class="rule-color-custom" value="${t.color || '#8b5cf6'}" oninput="BifractFormat._colorInput(this,'thr:${i}')">
                </div>
            </div>
        `).join('');

        return this._section('Thresholds', `
            <div class="fmt-hint">Color the value when it crosses a threshold. First match wins.</div>
            <div class="rule-target-toggle fmt-mode">
                <button type="button" class="rule-target-btn ${mode === 'value' ? 'active' : ''}" onclick="BifractFormat.setColorMode('value',this)">Value</button>
                <button type="button" class="rule-target-btn ${mode === 'background' ? 'active' : ''}" onclick="BifractFormat.setColorMode('background',this)">Background</button>
            </div>
            <div class="fmt-thr-list">${rows}</div>
            <button class="btn-sm btn-secondary" onclick="BifractFormat.addThr()" style="margin-top:8px;width:100%;">+ Add threshold</button>
        `);
    },

    setColorMode(mode, btn) {
        btn.parentElement.querySelectorAll('.rule-target-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this._cfg.stat = this._cfg.stat || {};
        this._cfg.stat.colorMode = mode;
        this._preview();
    },

    addThr() {
        this._cfg.stat = this._cfg.stat || {};
        this._cfg.stat.thresholds = this._cfg.stat.thresholds || [];
        this._cfg.stat.thresholds.push({ op: '>=', value: '', color: '#ef4444' });
        this._rerenderBody();
    },

    updThr(i, key, val) {
        if (!this._cfg.stat || !this._cfg.stat.thresholds) return;
        this._cfg.stat.thresholds[i][key] = val;
        this._preview();
    },

    delThr(i) {
        this._cfg.stat.thresholds.splice(i, 1);
        this._rerenderBody();
        this._preview();
    },

    // ---- Table conditional formatting ----

    _sectionRules() {
        const rules = this._cfg.row_coloring_rules || [];
        const body = rules.length
            ? `<div class="fmt-rule-list">${rules.map((r, i) => this._ruleRow(r, i)).join('')}</div>`
            : `<div class="fmt-rule-list"><div class="row-coloring-empty">No rules configured</div></div>`;
        return this._section('Conditional formatting', `
            <div class="fmt-hint">Highlight cells or rows where a column matches a condition.</div>
            ${body}
            <button class="btn-sm btn-secondary" onclick="BifractFormat.addRule()" style="margin-top:8px;width:100%;">+ Add rule</button>
        `);
    },

    _ruleRow(r, i) {
        const esc = Utils.escapeHtml;
        const op = r.operator || '=';
        const target = r.target || 'row';
        const opLabel = { '=': '=', 'contains': 'contains', '>': '&gt;', '>=': '&gt;=', '<': '&lt;', '<=': '&lt;=' };
        return `<div class="row-coloring-rule">
            <div class="rule-row-top">
                <input type="text" class="rule-column" placeholder="Column" value="${esc(r.column || '')}" oninput="BifractFormat.updRule(${i},'column',this.value)">
                <select class="rule-operator" onchange="BifractFormat.updRule(${i},'operator',this.value)">
                    ${['=', 'contains', '>', '>=', '<', '<='].map(o => `<option value="${o}" ${op === o ? 'selected' : ''}>${opLabel[o]}</option>`).join('')}
                </select>
                <input type="text" class="rule-value" placeholder="Value" value="${esc(r.value || '')}" oninput="BifractFormat.updRule(${i},'value',this.value)">
                <button class="remove-rule-btn" onclick="BifractFormat.delRule(${i})">&#x2715;</button>
            </div>
            <div class="rule-row-bottom">
                <div class="rule-target-toggle">
                    <button type="button" class="rule-target-btn ${target === 'cell' ? 'active' : ''}" onclick="BifractFormat.setRuleTarget(${i},'cell',this)">Cell</button>
                    <button type="button" class="rule-target-btn ${target === 'row' ? 'active' : ''}" onclick="BifractFormat.setRuleTarget(${i},'row',this)">Row</button>
                </div>
                <div class="rule-color-swatches">
                    ${this.SWATCHES.map(c => `<button type="button" class="rule-swatch${r.color === c ? ' active' : ''}" style="background:${c};" onclick="BifractFormat._swatchClick(this,'rule:${i}','${c}')"></button>`).join('')}
                    <input type="color" class="rule-color-custom" value="${r.color || '#8b5cf6'}" oninput="BifractFormat._colorInput(this,'rule:${i}')">
                </div>
            </div>
        </div>`;
    },

    addRule() {
        this._cfg.row_coloring_rules = this._cfg.row_coloring_rules || [];
        this._cfg.row_coloring_rules.push({ column: '', operator: '=', value: '', color: '#8b5cf6', target: 'row' });
        this._rerenderBody();
    },

    updRule(i, key, val) {
        if (!this._cfg.row_coloring_rules) return;
        this._cfg.row_coloring_rules[i][key] = val;
        this._preview();
    },

    delRule(i) {
        this._cfg.row_coloring_rules.splice(i, 1);
        this._rerenderBody();
        this._preview();
    },

    setRuleTarget(i, t, btn) {
        btn.parentElement.querySelectorAll('.rule-target-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this._cfg.row_coloring_rules[i].target = t;
        this._preview();
    },

    // ---- Color application (shared by swatches + custom picker) ----

    _swatchClick(btn, target, color) {
        btn.parentElement.querySelectorAll('.rule-swatch').forEach(s => s.classList.remove('active'));
        btn.classList.add('active');
        const custom = btn.parentElement.querySelector('.rule-color-custom');
        if (custom) custom.value = color;
        this._applyColor(target, color);
        this._preview();
    },

    _colorInput(input, target) {
        input.parentElement.querySelectorAll('.rule-swatch').forEach(s => s.classList.remove('active'));
        this._applyColor(target, input.value);
        this._preview();
    },

    _applyColor(target, color) {
        const sep = target.indexOf(':');
        const kind = target.substring(0, sep);
        const key = target.substring(sep + 1);
        if (kind === 'rule') {
            this._cfg.row_coloring_rules[+key].color = color;
        } else if (kind === 'thr') {
            this._cfg.stat.thresholds[+key].color = color;
        } else if (kind === 'series') {
            const label = this._seriesLabels[+key];
            this._cfg.colors = this._cfg.colors || {};
            this._cfg.colors.overrides = this._cfg.colors.overrides || {};
            this._cfg.colors.overrides[label] = color;
        }
    },

    // ---- Generic nested setter (mutate + preview, no re-render) ----

    setPath(path, value) {
        const parts = path.split('.');
        let o = this._cfg;
        for (let i = 0; i < parts.length - 1; i++) {
            if (o[parts[i]] == null || typeof o[parts[i]] !== 'object') o[parts[i]] = {};
            o = o[parts[i]];
        }
        o[parts[parts.length - 1]] = value;
        this._preview();
    },

    // ---- Series label detection (for color overrides) ----

    _detectLabels() {
        const ct = this._ctx.chartType;
        const data = this._ctx.results || [];
        if (!data.length) return [];
        const fields = (this._ctx.fields && this._ctx.fields.length) ? this._ctx.fields : Object.keys(data[0] || {});

        if (ct === 'piechart' || ct === 'barchart') {
            const lf = fields[0];
            const limit = (this._ctx.config && this._ctx.config.limit) || 10;
            const seen = [];
            for (const r of data) {
                const v = String(r[lf] == null ? 'Unknown' : r[lf]);
                if (seen.indexOf(v) < 0) seen.push(v);
                if (seen.length >= limit) break;
            }
            return seen;
        }

        if (ct === 'timechart') {
            const timeField = fields.find(f => f === 'time_bucket') || fields[0];
            const valLike = f => f === '_count' || f === 'count' || /^(sum|avg|min|max|bucket|stddev)_/.test(f);
            const groupFields = fields.filter(f => f !== timeField && !valLike(f));
            if (groupFields.length) {
                const gf = groupFields[0];
                const seen = [];
                for (const r of data) {
                    const v = String(r[gf] == null ? 'Unknown' : r[gf]);
                    if (seen.indexOf(v) < 0) seen.push(v);
                }
                return seen;
            }
            const vf = fields.find(valLike) || fields[1];
            return [String(vf || 'value').replace(/_/g, ' ')];
        }

        return [];
    },

    // ---- Lifecycle ----

    _preview() {
        if (this._ctx && this._ctx.onPreview) this._ctx.onPreview(this._cfg);
    },

    save() {
        const cb = this._ctx && this._ctx.onSave;
        const cfg = this._cfg;
        this._removePanel();
        if (cb) cb(cfg);
    },

    cancel() {
        const cb = this._ctx && this._ctx.onCancel;
        this._removePanel();
        if (cb) cb();
    },

    _removePanel(immediate) {
        const p = document.getElementById('formatPanel');
        const o = document.getElementById('formatOverlay');
        if (immediate) {
            if (p) p.remove();
            if (o) o.remove();
            return;
        }
        if (p) { p.classList.remove('open'); setTimeout(() => p.remove(), 300); }
        if (o) { o.classList.remove('open'); setTimeout(() => o.remove(), 300); }
    }
};
