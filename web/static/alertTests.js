// Alert tests: sample events an alert should or should not match, edited in the
// editor's Tests tab and evaluated whenever the editor runs its query.
//
// Events are held client-side while editing and persist with the alert on save, so a
// brand new alert can be tested before it exists.
const AlertTests = {
    EXPECT_MATCH: 'match',
    EXPECT_NO_MATCH: 'no_match',

    MAX_TESTS: 25,
    MAX_EVENTS_PER_ALERT: 50,

    _tests: [],
    _outcomes: null,
    _running: false,
    _composing: false,
    // Only true once the saved corpus is actually in hand. Until then the editor must
    // not send a corpus at all: sending [] would delete every saved test.
    _loaded: false,
    _selected: new Set(),
    _index: null,
    // Full events resolved for a projected row, keyed by log_id. Keeps the gutter's
    // synchronous state lookup in step with what capture actually stored.
    _fullEvents: new Map(),
    _composerError: '',
    _sessionId: null,
    _alertId: null,
    _lastError: '',

    // ---- Lifecycle ----

    // Called when the editor opens. alertId is null for a new alert.
    async load(alertId) {
        this._alertId = alertId || null;
        this._loaded = !alertId; // a new alert has nothing to lose
        this._tests = [];
        this._outcomes = null;
        this._lastError = '';
        this._selected = new Set();
        this._index = null;
        this._fullEvents = new Map();
        this._sessionId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;

        if (alertId) {
            try {
                const res = await fetch(`/api/v1/alerts/${alertId}/tests`, { credentials: 'include' });
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                const payload = await res.json();
                this._tests = (payload.data || []).map(t => ({
                    name: t.name,
                    expectation: t.expectation,
                    events: t.events || []
                }));
                this._loaded = true;
            } catch (e) {
                // Left unloaded on purpose: the save path omits tests entirely rather
                // than replacing a corpus it never saw.
                this._lastError = e.message;
            }
        }

        this.invalidateIndex();
        this.render();
        this.updateChip();
        // tests.match_count and tests.no_match_count read this corpus.
        window.AlertPolicy?.schedule();
    },

    // Called when the editor closes, so the server can drop the loaded events early.
    release() {
        const id = this._sessionId;
        this._tests = [];
        this._outcomes = null;
        this._selected = new Set();
        this._index = null;
        this._fullEvents = new Map();
        this._sessionId = null;
        this._alertId = null;
        this.updateChip();

        if (id) {
            fetch(`/api/v1/alerts/tests/session/${encodeURIComponent(id)}`, {
                method: 'DELETE',
                credentials: 'include'
            }).catch(() => {});
        }
    },

    // What the alert save payload should carry. Undefined means "unchanged", which is
    // what the server reads as "leave the stored corpus alone".
    payload() {
        if (!this._loaded) return undefined;
        return this._tests.map((t, i) => ({
            name: t.name,
            expectation: t.expectation,
            events: t.events,
            position: i
        }));
    },

    count() {
        return this._tests.length;
    },

    // The last run, for a policy check that reads whether the tests pass.
    lastRun() {
        return this._outcomes;
    },

    eventCount() {
        return this._tests.reduce((n, t) => n + t.events.length, 0);
    },

    // ---- Running ----

    // Runs alongside the editor's own query. The corpus is loaded server-side once per
    // session, so an iteration on the query costs one query per test and no reload.
    async run(queryString) {
        if (!this._sessionId || this._tests.length === 0) {
            this._outcomes = null;
            this.updateChip();
            this.render();
            return;
        }
        if (!queryString || !queryString.trim()) return;

        this._running = true;
        this._lastError = '';
        this.updateChip();
        this.render();

        try {
            const res = await fetch('/api/v1/alerts/tests/run', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    session_id: this._sessionId,
                    query_string: queryString,
                    tests: this.payload()
                })
            });

            const payload = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(payload.error || `HTTP ${res.status}`);
            this._outcomes = payload.data || null;
        } catch (e) {
            this._outcomes = null;
            // Tests run on the same debounce as the query, so a half-typed query would
            // otherwise paint a parse error here on every keystroke. The query field
            // already reports its own syntax; this tab stays quiet until it parses.
            this._lastError = /invalid query syntax/i.test(e.message) ? '' : e.message;
        } finally {
            this._running = false;
            this.updateChip();
            this.render();
            this.repaintGutter();
            window.AlertPolicy?.schedule();
        }
    },

    // ---- Capture ----

    // The results gutter: one marking verb on a row, in the same reserved channel the
    // star gutter uses in search. Clicking cycles unmarked -> should match -> should not
    // match -> unmarked, and once a run has happened the mark carries its own pass or
    // fail state, so the outcome is visible against the event that produced it.
    gutter: {
        BUTTON_SELECTOR: '.tg-mark',

        // The channel only appears in the alert editor, and only for row-shaped results:
        // an aggregated row is not an event and has nothing to mark.
        enabled() {
            return true;
        },

        headerHtml() {
            return '<th class="tg-gutter" scope="col"></th>';
        },

        colHtml() {
            return '<col class="tg-col" style="width:26px">';
        },

        cellHtml(logID, row) {
            const state = AlertTests.stateForRow(row);
            if (!state) return '<td class="tg-gutter"></td>';

            return `<td class="tg-gutter"><button type="button" class="tg-mark tg-${state.kind}"` +
                ` title="${Utils.escapeAttr(state.title)}" aria-label="${Utils.escapeAttr(state.title)}">` +
                `${AlertTests.markGlyph(state.kind)}</button></td>`;
        },

        rowClass(logID, row) {
            const state = AlertTests.stateForRow(row);
            return state && state.kind !== 'none' ? 'tg-marked' : '';
        },

        onClick(button, row) {
            AlertTests.cycleRow(row);
        }
    },

    markGlyph(kind) {
        if (kind === 'match') {
            return '<svg viewBox="0 0 24 24" width="13" height="13" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12.5l5 5L20 6.5"/></svg>';
        }
        if (kind === 'no_match') {
            return '<svg viewBox="0 0 24 24" width="13" height="13" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><line x1="5" y1="12" x2="19" y2="12"/></svg>';
        }
        return '<svg viewBox="0 0 24 24" width="13" height="13" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="7.5"/></svg>';
    },

    // How a result row currently reads in the gutter.
    //
    // A row maps to a test by the content of the event it would capture rather than by
    // log_id, so the mapping survives a reload with nothing extra persisted, and a
    // pasted event that happens to be identical reads as the same test.
    stateForRow(row) {
        const event = (row?.log_id && this._fullEvents.get(row.log_id)) || this.toNormalizedEvent(row);
        if (!event) return null;

        const entry = this.eventIndex().get(this.eventKey(event));
        if (!entry) {
            return { kind: 'none', title: 'Mark as an event this alert should match' };
        }

        const outcome = this.outcomeFor(entry.name);
        const expects = entry.expectation === this.EXPECT_MATCH ? 'should match' : 'should not match';
        let title = `In test "${entry.name}" (${expects})`;
        if (outcome) title += outcome.passed ? ', passing' : `, failing: ${outcome.reason || ''}`;
        title += entry.expectation === this.EXPECT_MATCH
            ? '. Click to expect no match.'
            : '. Click to remove from tests.';

        return { kind: entry.expectation, title, outcome };
    },

    // Stable key for an event, independent of key order.
    eventKey(event) {
        return JSON.stringify(Object.keys(event).sort().map(k => [k, event[k]]));
    },

    // Rebuilt whenever the corpus changes, so a table render is a map lookup per row
    // rather than a scan of every test.
    eventIndex() {
        if (this._index) return this._index;

        this._index = new Map();
        for (const test of this._tests) {
            for (const event of test.events) {
                this._index.set(this.eventKey(event), { name: test.name, expectation: test.expectation });
            }
        }
        return this._index;
    },

    invalidateIndex() {
        this._index = null;
    },

    // Cycles a row through the three states. A row marked from here becomes a
    // single-event test; several rows are combined into one scenario from the Tests
    // tab, which is where the whole corpus is visible.
    async cycleRow(row) {
        const event = await this.resolveEvent(row);
        if (!event) {
            Toast.error('Could not capture this event', 'This row carries no fields to test against.');
            return;
        }

        const key = this.eventKey(event);
        const found = this.findEvent(key);

        if (!found) {
            this.addTest(this.EXPECT_MATCH, [event], { silent: true });
        } else if (this._tests[found.testIndex].expectation === this.EXPECT_MATCH) {
            this._tests[found.testIndex].expectation = this.EXPECT_NO_MATCH;
            this.afterCorpusChange();
        } else {
            this.removeEvent(found.testIndex, found.eventIndex);
            return;
        }

        this.repaintGutter();
    },

    // A projected row holds only the columns the query asked for, so the field a rule
    // filters on may not be present at all: marking one and testing against it would
    // assert about a fragment. The row still identifies its log, so the whole event is
    // fetched before it becomes a test.
    async resolveEvent(row) {
        const cached = row?.log_id && this._fullEvents.get(row.log_id);
        if (cached) return cached;

        let event;
        if (this.isProjected(row)) {
            event = await this.fetchFullEvent(row);
            if (!event) {
                Toast.error('Could not capture this event',
                    'This row is a projection and the full event could not be loaded.');
                return null;
            }
        } else {
            event = this.toNormalizedEvent(row);
        }

        if (row?.log_id && event) this._fullEvents.set(row.log_id, event);
        return event;
    },

    // A row is a projection when it carries neither the normalized blob nor a loaded
    // field set: table() and groupby() return their named columns and nothing else.
    isProjected(row) {
        if (!row) return false;
        const hasFields = row.fields && typeof row.fields === 'object' && !Array.isArray(row.fields);
        return !row.norm_log && !hasFields;
    },

    // The same lookup the log detail panel uses to fill its Fields tab.
    async fetchFullEvent(row) {
        if (!row.log_id || !row.fractal_id || !row.timestamp) return null;

        const params = new URLSearchParams({
            log_id: row.log_id,
            fractal_id: row.fractal_id,
            timestamp: row.timestamp
        });
        if (row._shard_num !== undefined && row._shard_num !== null) {
            params.set('shard_num', row._shard_num);
        }

        try {
            const res = await fetch(`/api/v1/logs/fields?${params}`, { credentials: 'include' });
            if (!res.ok) return null;
            const data = await res.json();
            if (!data.success || !data.fields) return null;
            return this.stripBookkeeping(data.fields, row.timestamp);
        } catch (e) {
            return null;
        }
    },

    findEvent(key) {
        for (let t = 0; t < this._tests.length; t++) {
            for (let e = 0; e < this._tests[t].events.length; e++) {
                if (this.eventKey(this._tests[t].events[e]) === key) return { testIndex: t, eventIndex: e };
            }
        }
        return null;
    },

    // Repaints the marks on rows already on screen, so marking one row does not cost a
    // re-render of the results table.
    repaintGutter() {
        document.querySelectorAll('#alertResultsPane .tg-mark').forEach(btn => {
            const rowEl = btn.closest('.result-row');
            const index = rowEl ? parseInt(rowEl.dataset.index, 10) : -1;
            const row = window.Alerts?.getCurrentAlertPageResults?.()[index];
            const state = row ? this.stateForRow(row) : null;
            if (!state) return;

            btn.className = `tg-mark tg-${state.kind}`;
            btn.title = state.title;
            btn.setAttribute('aria-label', state.title);
            btn.innerHTML = this.markGlyph(state.kind);
            if (rowEl) rowEl.classList.toggle('tg-marked', state.kind !== 'none');
        });
    },

    // Extracts the normalized field set from a search result row.
    //
    // A result row is storage bookkeeping plus `norm_log`, the canonical normalized JSON
    // that BQL actually runs against; the detail panel additionally lazy-loads the same
    // data into `fields`. Either is the event. The bookkeeping columns are not: log_id
    // and fractal_id are assigned at insert, and carrying them would tie a test event to
    // the real row it was copied from.
    toNormalizedEvent(log) {
        if (!log || typeof log !== 'object') return null;

        let fields = null;
        if (log.fields && typeof log.fields === 'object' && !Array.isArray(log.fields)) {
            fields = log.fields;
        } else if (typeof log.norm_log === 'string' && log.norm_log.trim()) {
            try {
                const parsed = JSON.parse(log.norm_log);
                if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) fields = parsed;
            } catch (e) {
                fields = null;
            }
        }

        // A projected query (table(), groupby()) returns plain columns and no norm_log,
        // so fall back to the row itself.
        if (!fields) fields = log;

        return this.stripBookkeeping(fields, log.timestamp);
    },

    // Nested values are passed through untouched: the server flattens them on the way
    // in, exactly as it does for a real ingested event. Stringifying them here would
    // produce one opaque field instead of the ones a rule matches on.
    stripBookkeeping(fields, timestamp) {
        const drop = new Set(['log_id', 'fractal_id', 'ingest_timestamp', 'norm_log', 'raw_log', 'normalizer', '_shard_num']);
        const out = {};
        for (const [k, v] of Object.entries(fields)) {
            if (drop.has(k) || v === null || v === undefined || v === '') continue;
            out[k] = v;
        }

        // Keep the event's own time: it never enters the evaluation window, but rules
        // that order events by it (chain) read it.
        if (timestamp && !out.timestamp) out.timestamp = timestamp;

        return Object.keys(out).length > 0 ? out : null;
    },

    // afterCorpusChange is the single place the corpus edit is published: the row index,
    // the results gutter, the chip, the pane and the run all follow from it.
    afterCorpusChange({ rerun = true } = {}) {
        this.invalidateIndex();
        this._outcomes = null;
        this.render();
        this.updateChip();
        this.repaintGutter();
        window.AlertPolicy?.schedule();
        if (rerun) this.rerun();
    },

    addTest(expectation, events, opts = {}) {
        if (this._tests.length >= this.MAX_TESTS) {
            Toast.error('Too many tests', `An alert keeps at most ${this.MAX_TESTS} tests.`);
            return;
        }
        if (this.eventCount() + events.length > this.MAX_EVENTS_PER_ALERT) {
            Toast.error('Too many events', `An alert keeps at most ${this.MAX_EVENTS_PER_ALERT} events across its tests.`);
            return;
        }

        const base = this.suggestName(events);
        let name = base;
        for (let n = 2; this._tests.some(t => t.name === name); n++) name = `${base} ${n}`;

        this._loaded = true;
        this._tests.push({ name, expectation, events });
        if (!opts.silent) this.openTab();
        this.afterCorpusChange();
    },

    // Names come from the event, never from the expectation: the gutter flips a test
    // from "should match" to "should not match" in place, and a name that stated the
    // expectation would then be a lie. A name drawn from the event stays true and is
    // what an author actually scans for.
    IDENTITY_FIELDS: [
        'process_name', 'image', 'Image', 'original_file_name', 'target_image',
        'user', 'User', 'computer_name', 'host', 'src_ip', 'url', 'query',
        'event_id', 'bifract_category'
    ],

    suggestName(events) {
        const event = events[0] || {};
        for (const field of this.IDENTITY_FIELDS) {
            const value = event[field];
            if (typeof value !== 'string' || !value.trim()) continue;
            return this.shortValue(value);
        }

        for (const [k, v] of Object.entries(event)) {
            if (k === 'timestamp' || typeof v !== 'string' || !v.trim()) continue;
            return this.shortValue(v);
        }
        return 'Untitled test';
    },

    // Paths are the common case and their tail is the identifying part.
    shortValue(value) {
        const tail = value.split(/[\\/]/).pop() || value;
        const text = tail.trim() || value.trim();
        return text.length > 44 ? text.slice(0, 43) + '\u2026' : text;
    },

    appendEvent(index, event) {
        const test = this._tests[index];
        if (!test) return;
        if (this.eventCount() + 1 > this.MAX_EVENTS_PER_ALERT) {
            Toast.error('Too many events', `An alert keeps at most ${this.MAX_EVENTS_PER_ALERT} events across its tests.`);
            return;
        }

        test.events.push(event);
        this.afterCorpusChange();
    },

    removeTest(index) {
        this._tests.splice(index, 1);
        this.afterCorpusChange();
    },

    removeEvent(testIndex, eventIndex) {
        const test = this._tests[testIndex];
        if (!test) return;
        if (test.events.length === 1) {
            this.removeTest(testIndex);
            return;
        }
        test.events.splice(eventIndex, 1);
        this.afterCorpusChange();
    },

    setExpectation(index, expectation) {
        if (!this._tests[index]) return;
        this._tests[index].expectation = expectation;
        this.afterCorpusChange();
    },

    rename(index, name) {
        if (!this._tests[index]) return;
        const previous = this._tests[index].name;
        this._tests[index].name = name.trim() || previous;
        if (this._selected.delete(previous)) this._selected.add(this._tests[index].name);
        this.invalidateIndex();
        this.render();
    },

    // Combining is how a scenario gets built: a compound or scheduled rule only fires
    // when it sees several events at once, and the gutter deliberately makes one test
    // per row. Selection lives here rather than on the results table because judging
    // what belongs in one scenario means seeing the whole corpus.
    // Selection is held by test name rather than by index: removing or combining
    // tests splices the array, and an index captured before that points at the wrong
    // test afterwards, or at nothing.
    toggleSelection(index) {
        const name = this._tests[index]?.name;
        if (!name) return;
        if (this._selected.has(name)) this._selected.delete(name);
        else this._selected.add(name);
        this.render();
    },

    combineSelected() {
        const picked = this._tests
            .map((t, i) => (this._selected.has(t.name) ? i : -1))
            .filter(i => i >= 0);
        if (picked.length < 2) return;

        const expectations = new Set(picked.map(i => this._tests[i].expectation));
        if (expectations.size > 1) {
            Toast.error('Cannot combine', 'Select tests that share the same expectation.');
            return;
        }

        const target = this._tests[picked[0]];
        for (const i of picked.slice(1)) target.events.push(...this._tests[i].events);
        for (const i of picked.slice(1).reverse()) this._tests.splice(i, 1);

        this._selected.clear();
        this.afterCorpusChange();
    },

    // Pasting is for events that do not exist in storage yet: a new log source, a
    // vendor sample, a detection written ahead of its telemetry. The composer is inline
    // rather than a dialog so the JSON stays visible next to its error.
    openComposer() {
        this._composing = true;
        this._composerError = '';
        this.openTab();
        this.render();
        document.getElementById('atComposerInput')?.focus();
    },

    closeComposer() {
        this._composing = false;
        this._composerError = '';
        this.render();
    },

    submitComposer() {
        const input = document.getElementById('atComposerInput');
        const expectation = document.getElementById('atComposerExpect')?.value || this.EXPECT_MATCH;
        const raw = (input?.value || '').trim();

        if (!raw) {
            this._composerError = 'Paste one event object, or an array of them.';
            this.render();
            return;
        }

        let parsed;
        try {
            parsed = JSON.parse(raw);
        } catch (e) {
            this._composerError = e.message;
            this.render();
            return;
        }

        const objects = (Array.isArray(parsed) ? parsed : [parsed])
            .filter(e => e && typeof e === 'object' && !Array.isArray(e));
        const events = objects.map(e => this.stripBookkeeping(e, e.timestamp)).filter(Boolean);

        if (events.length === 0) {
            this._composerError = 'Expected an object with fields, or an array of them.';
            this.render();
            return;
        }

        this._composing = false;
        this._composerError = '';
        this.addTest(expectation, events);
    },

    rerun() {
        const query = document.getElementById('editorQueryInput')?.value || '';
        if (query.trim()) this.run(query);
    },

    // ---- Rendering ----

    openTab() {
        document.querySelector('#alertResultTabs .ert-tab[data-pane="tests"]')?.click();
    },

    updateChip() {
        const chip = document.getElementById('alertTestsChip');
        if (!chip) return;

        if (this._tests.length === 0) {
            chip.hidden = true;
            return;
        }

        chip.hidden = false;
        chip.className = 'ert-chip';
        if (this._running) {
            chip.textContent = '...';
            return;
        }
        if (!this._outcomes) {
            chip.textContent = String(this._tests.length);
            return;
        }

        const { passed, failed } = this._outcomes;
        chip.textContent = `${passed}/${passed + failed}`;
        chip.classList.add(failed > 0 ? 'fail' : 'pass');
    },

    outcomeFor(name) {
        return this._outcomes?.outcomes?.find(o => o.name === name) || null;
    },

    render() {
        const pane = document.getElementById('alertTestsPane');
        if (!pane) return;

        pane.innerHTML = `
            <div class="at-head">
                <div class="at-summary">${this.renderSummary()}</div>
                <div class="at-actions">
                    ${this._selected.size >= 2
                        ? `<button type="button" class="btn-secondary btn-sm" onclick="AlertTests.combineSelected()">Combine ${this._selected.size} into one scenario</button>`
                        : ''}
                    <button type="button" class="btn-secondary btn-sm" onclick="AlertTests.openComposer()">Paste events</button>
                </div>
            </div>
            ${this._composing ? this.renderComposer() : ''}
            ${this._tests.length === 0 ? this.renderEmpty() : `
                <div class="at-groups">
                    ${this.renderGroup('Should match', this.EXPECT_MATCH)}
                    ${this.renderGroup('Should not match', this.EXPECT_NO_MATCH)}
                </div>
            `}
        `;
    },

    renderComposer() {
        return `
            <div class="at-composer">
                <div class="at-composer-head">
                    <span>Paste an event, or an array of them</span>
                    <select id="atComposerExpect" class="at-test-expect">
                        <option value="match">Should match</option>
                        <option value="no_match">Should not match</option>
                    </select>
                </div>
                <textarea id="atComposerInput" class="at-composer-input" spellcheck="false"
                          placeholder='{"process_name": "cmd.exe", "user": "svc-backup"}'></textarea>
                ${this._composerError ? `<div class="at-composer-error">${Utils.escapeHtml(this._composerError)}</div>` : ''}
                <div class="at-composer-actions">
                    <span class="at-composer-hint">Normalized fields, not raw vendor JSON.</span>
                    <button type="button" class="btn-secondary btn-sm" onclick="AlertTests.closeComposer()">Cancel</button>
                    <button type="button" class="btn-primary btn-sm" onclick="AlertTests.submitComposer()">Add test</button>
                </div>
            </div>
        `;
    },

    renderEmpty() {
        // Shown only for a compound rule, whose aggregated results give the gutter
        // nothing to mark.
        const compound = document.getElementById('alertTypeSelect')?.value === 'compound';
        return `
            <div class="at-empty">
                <p>No tests.</p>
                <p class="at-empty-hint">Mark a result row from its gutter, or paste an event.</p>
                ${compound ? '<p class="at-empty-hint">Aggregates have nothing to mark. Mark on the filter, then combine.</p>' : ''}
            </div>
        `;
    },

    renderSummary() {
        if (this._lastError) {
            return `<span class="at-status at-status-error">${Utils.escapeHtml(this._lastError)}</span>`;
        }
        if (this._tests.length === 0) return '';
        if (this._running) return '<span class="at-status">Running...</span>';
        if (!this._outcomes) return '<span class="at-status">Not run yet.</span>';

        const { passed, failed } = this._outcomes;
        if (failed === 0) {
            return `<span class="at-status at-status-pass">All ${passed} test${passed === 1 ? '' : 's'} pass</span>`;
        }
        return `<span class="at-status at-status-fail">${failed} of ${passed + failed} failing</span>`;
    },

    renderGroup(title, expectation) {
        const entries = this._tests
            .map((test, index) => ({ test, index }))
            .filter(e => e.test.expectation === expectation);

        if (entries.length === 0) return '';

        return `
            <div class="at-group">
                <div class="at-group-head">${title}<span class="at-group-count">${entries.length}</span></div>
                ${entries.map(e => this.renderTest(e.test, e.index)).join('')}
            </div>
        `;
    },

    renderTest(test, index) {
        const outcome = this.outcomeFor(test.name);
        let state = 'pending';
        if (outcome) state = outcome.passed ? 'pass' : 'fail';

        return `
            <div class="at-test at-test-${state}">
                <div class="at-test-head">
                    <input type="checkbox" class="at-test-pick" title="Select to combine with another test"
                           ${this._selected.has(test.name) ? 'checked' : ''}
                           onchange="AlertTests.toggleSelection(${index})" />
                    <span class="at-test-state" title="${state}"></span>
                    <input type="text" class="at-test-name" value="${Utils.escapeHtml(test.name)}"
                           onchange="AlertTests.rename(${index}, this.value)" />
                    <span class="at-test-count">${test.events.length} event${test.events.length === 1 ? '' : 's'}</span>
                    <select class="at-test-expect" onchange="AlertTests.setExpectation(${index}, this.value)">
                        <option value="match"${test.expectation === 'match' ? ' selected' : ''}>Should match</option>
                        <option value="no_match"${test.expectation === 'no_match' ? ' selected' : ''}>Should not match</option>
                    </select>
                    <button type="button" class="at-test-remove" title="Remove test" onclick="AlertTests.removeTest(${index})">&times;</button>
                </div>
                ${outcome && !outcome.passed && outcome.reason
                    ? `<div class="at-test-reason">${Utils.escapeHtml(outcome.reason)}</div>` : ''}
                <div class="at-events">
                    ${test.events.map((event, i) => this.renderEvent(event, index, i)).join('')}
                </div>
            </div>
        `;
    },

    renderEvent(event, testIndex, eventIndex) {
        const summary = Object.entries(event)
            .slice(0, 6)
            .map(([k, v]) => `${k}=${v}`)
            .join('  ');

        return `
            <div class="at-event">
                <code class="at-event-summary" title="${Utils.escapeHtml(JSON.stringify(event, null, 2))}">${Utils.escapeHtml(summary)}</code>
                <button type="button" class="at-event-remove" title="Remove event"
                        onclick="AlertTests.removeEvent(${testIndex}, ${eventIndex})">&times;</button>
            </div>
        `;
    }
};

window.AlertTests = AlertTests;
