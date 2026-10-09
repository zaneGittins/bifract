// Settings View module

const SettingsView = {
    isActive: false,
    mtlsEnabled: false,

    async init() {
        // Set up user management handlers
        const addUserBtn = document.getElementById('addUserBtnSettings');
        const createUserBtn = document.getElementById('createUserBtnSettings');
        const cancelBtn = document.getElementById('cancelAddUserBtnSettings');
        const clearLogsBtn = document.getElementById('clearLogsBtnSettings');

        if (addUserBtn) {
            addUserBtn.addEventListener('click', () => this.showAddUserForm());
        }

        const usersSearch = document.getElementById('usersSearch');
        if (usersSearch) {
            usersSearch.addEventListener('input', (e) => {
                this._userFilter = e.target.value;
                this._paintUsers();
            });
        }

        if (createUserBtn) {
            createUserBtn.addEventListener('click', () => this.createUser());
        }

        if (cancelBtn) {
            cancelBtn.addEventListener('click', () => this.hideAddUserForm());
        }

        if (clearLogsBtn) {
            clearLogsBtn.addEventListener('click', () => this.clearLogs());
        }

        // Set up system limits dropdowns
        ['alertTimeoutSettings', 'queryTimeoutSettings', 'queryCPUPercentSettings', 'queryMemoryPercentSettings', 'alertEvalIntervalSettings',
         'recallTimeoutSettings', 'recallMaxBytesSettings', 'recallConcurrencySettings',
         'recallCPUPercentSettings', 'recallMemoryPercentSettings', 'schemaSweepIntervalSettings',
         'pgrSensitivitySettings', 'alertRevisionRetentionSettings'].forEach(id => {
            const select = document.getElementById(id);
            if (select) select.addEventListener('change', () => this.saveSettings(select));
        });
        const archiveToggle = document.getElementById('archiveEnabledToggle');
        if (archiveToggle) {
            archiveToggle.addEventListener('change', () => this.saveArchiveEnabled());
        }
        const clearCatalogBtn = document.getElementById('archiveClearCatalogBtn');
        if (clearCatalogBtn) {
            clearCatalogBtn.addEventListener('click', () => this.confirmClearCatalog());
        }
        const clearSpoolBtn = document.getElementById('archiveClearSpoolBtn');
        if (clearSpoolBtn) {
            clearSpoolBtn.addEventListener('click', () => this.confirmClearSpool());
        }
        const endpointAnalysisToggle = document.getElementById('endpointAnalysisToggle');
        if (endpointAnalysisToggle) {
            endpointAnalysisToggle.addEventListener('change', () => this.saveEndpointAnalysis());
        }
        const requireMFAToggle = document.getElementById('requireMFAToggle');
        if (requireMFAToggle) {
            requireMFAToggle.addEventListener('change', () => this.saveRequireMFA());
        }
        const sharedLinksToggle = document.getElementById('sharedLinksEnabledToggle');
        if (sharedLinksToggle) {
            sharedLinksToggle.addEventListener('change', () => this.saveSharedLinksEnabled());
        }

        const distQueueResetBtn = document.getElementById('distQueueResetBtn');
        if (distQueueResetBtn) {
            distQueueResetBtn.addEventListener('click', () => this.confirmDistQueueReset());
        }
        document.getElementById('endpointBaselinesClearBtn')
            ?.addEventListener('click', () => this.confirmClearEndpointBaselines());

        this.loadDistQueueShards();
        this.initSectionRail();
    },

    // Shows the row whenever the deployment is cluster mode (the button opens
    // the shard picker; per-shard stats are fetched fresh when the modal
    // opens, not held live in the row itself). Single-node deployments get
    // null back and the row stays hidden.
    async loadDistQueueShards() {
        const item = document.getElementById('distQueueResetItem');
        if (!item) return;
        try {
            const res = await fetch('/api/v1/system/distribution-queue/shards', { credentials: 'include' });
            if (!res.ok) { item.style.display = 'none'; return; }
            const shards = await res.json();
            item.style.display = (shards && shards.length) ? '' : 'none';
        } catch (err) {
            console.error('[Settings] distribution queue shard load error:', err);
        }
    },

    // The shard picker is filled after the dialog opens, from fresh per-shard stats,
    // so it never shows numbers from whenever the Settings page first loaded.
    confirmDistQueueReset() {
        const select = document.createElement('select');
        select.className = 'setting-select';
        select.style.width = '100%';
        select.setAttribute('aria-label', 'Shard');
        select.innerHTML = '<option value="">Loading shards...</option>';
        select.disabled = true;
        select.addEventListener('change', () => DangerConfirm.revalidate());

        DangerConfirm.open({
            title: 'Reset Distribution Queue',
            body: [
                'This drops and recreates logs_distributed on the selected shard, discarding its queued log batches.',
                { text: "Other shards and this shard's own stored logs are unaffected.", muted: true },
            ],
            extra: select,
            isReady: () => !select.disabled && !!select.value,
            phrase: 'RESET QUEUE',
            confirmLabel: 'Reset Queue',
            busyLabel: 'Resetting...',
            onConfirm: async () => {
                const shardNum = Number(select.value);
                await HttpUtils.safeFetch('/api/v1/system/distribution-queue/reset', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ shard_num: shardNum }),
                });
                if (window.Toast) Toast.success('Distribution Queue Reset', `Shard ${shardNum}'s queue was cleared.`);
            },
        });

        HttpUtils.safeFetch('/api/v1/system/distribution-queue/shards').then(shards => {
            select.replaceChildren(...(shards || []).map(s => {
                const parts = s.unreachable ? ['unreachable'] : [`${s.data_files} file(s) queued`];
                if (!s.unreachable && s.broken_data_files) parts.push(`${s.broken_data_files} broken`);
                if (!s.unreachable && s.error_count) parts.push(`${s.error_count} error(s)`);
                return new Option(`Shard ${s.shard_num}: ${parts.join(', ')}`, String(s.shard_num));
            }));
            if (!select.options.length) select.replaceChildren(new Option('No shards found', ''));
        }).catch(() => {
            select.replaceChildren(new Option('Failed to load shards', ''));
        }).finally(() => {
            select.disabled = false;
            DangerConfirm.revalidate();
        });
    },

    // Section rail for the Settings sub-tab: click to scroll, and highlight the
    // section currently in view. Observed sections are static, so a single
    // observer set up once is enough.
    initSectionRail() {
        const rail = document.getElementById('settingsSectionRail');
        if (!rail) return;

        const items = Array.from(rail.querySelectorAll('.sp-rail-item'));
        const sections = items
            .map(item => document.getElementById(item.dataset.section))
            .filter(Boolean);

        const activate = (id) => {
            items.forEach(item => item.classList.toggle('active', item.dataset.section === id));
        };

        items.forEach(item => {
            item.addEventListener('click', () => {
                const target = document.getElementById(item.dataset.section);
                if (!target) return;
                target.scrollIntoView({ behavior: 'smooth', block: 'start' });
                activate(item.dataset.section);
            });
        });

        if (!sections.length || !window.IntersectionObserver) return;

        // Track visible sections and always highlight the topmost one, so the
        // rail stays stable when several sections share the viewport.
        const visible = new Set();
        const observer = new IntersectionObserver(entries => {
            entries.forEach(entry => {
                if (entry.isIntersecting) visible.add(entry.target.id);
                else visible.delete(entry.target.id);
            });
            const topmost = sections.find(s => visible.has(s.id));
            if (topmost) activate(topmost.id);
        }, { rootMargin: '-8% 0px -70% 0px', threshold: 0 });

        sections.forEach(s => observer.observe(s));
    },

    // Clearing the catalog is rejected server-side while archiving is on. Reflect
    // that in the UI so the action is visibly unavailable rather than failing
    // after the user has already confirmed it.
    syncClearCatalogGuard(archiveEnabled) {
        // Both clear-catalog and clear-spool require archiving disabled (the server
        // enforces it too); reflect that on both controls.
        [['archiveClearCatalogBtn', 'archiveClearCatalogBlocked'],
        ['archiveClearSpoolBtn', 'archiveClearSpoolBlocked']].forEach(([btnId, blockedId]) => {
            const btn = document.getElementById(btnId);
            const blocked = document.getElementById(blockedId);
            if (btn) {
                btn.disabled = !!archiveEnabled;
                btn.title = archiveEnabled ? 'Disable the Iceberg archive first' : '';
            }
            if (blocked) blocked.style.display = archiveEnabled ? '' : 'none';
        });
    },

    // The spool is a transient buffer, so no phrase is required, but it still discards
    // un-archived data, so it is never a single click.
    confirmClearSpool() {
        DangerConfirm.open({
            title: 'Clear Archive Spool',
            body: [
                'This discards un-archived buffered logs on every ingest pod. Each pod applies it within about 10 seconds.',
                { text: 'Not needed when migrating backends: there the buffered tail correctly bridges the gap into the new archive.', muted: true },
            ],
            confirmLabel: 'Clear Spool',
            busyLabel: 'Requesting...',
            onConfirm: async () => {
                const d = await HttpUtils.safeFetch('/api/v1/system/archive/spool/clear', { method: 'POST' });
                if (window.Toast) {
                    Toast.success('Spool Clear Requested', (d && d.message) || 'Each ingest pod will clear its spool shortly.');
                }
            },
        });
    },

    // Loads the Iceberg archive enable state. The toggle is disabled (with a
    // hint) until the archiver machinery is provisioned.
    async loadArchiveToggle() {
        const toggle = document.getElementById('archiveEnabledToggle');
        const hint = document.getElementById('archiveToggleHint');
        if (!toggle) return;
        try {
            const res = await fetch('/api/v1/system/archive', { credentials: 'include' });
            if (!res.ok) return;
            const d = await res.json();
            toggle.checked = !!d.enabled;
            toggle.disabled = !d.provisioned;
            this.syncClearCatalogGuard(d.enabled);
            if (hint) {
                // read_blocked means archiving works but ClickHouse cannot read
                // the archive back, so restore and recall are off. Worth saying:
                // the toggle looks entirely healthy otherwise.
                let msg = '';
                if (!d.provisioned) {
                    msg = 'Not provisioned. Run bifract --upgrade to add the archiver.';
                } else if (d.read_blocked) {
                    msg = `Restore and recall unavailable: ${d.read_blocked}`;
                }
                hint.textContent = msg;
                hint.style.display = msg ? '' : 'none';
            }
        } catch (err) {
            console.error('[Settings] archive status load error:', err);
        }
    },

    async saveArchiveEnabled() {
        const toggle = document.getElementById('archiveEnabledToggle');
        if (!toggle) return;
        const enabled = toggle.checked;
        this.syncClearCatalogGuard(enabled);
        try {
            const res = await fetch('/api/v1/system/archive/enabled', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ enabled })
            });
            if (!res.ok) {
                const msg = await Utils.errorMessage(res);
                throw new Error(msg || 'Failed to update archive setting');
            }
            if (window.Toast) {
                Toast.success('Archive ' + (enabled ? 'Enabled' : 'Disabled'),
                    enabled ? 'Logs are now being copied to the Iceberg archive.' : 'Archiving paused. Existing archived data is retained.');
            }
        } catch (err) {
            toggle.checked = !enabled; // revert on failure
            this.syncClearCatalogGuard(!enabled);
            if (window.Toast) Toast.error('Archive Update Failed', err.message);
        }
    },

    // The server rejects this while archiving is on; the row guard mirrors that.
    confirmClearCatalog() {
        DangerConfirm.open({
            title: 'Clear Iceberg Catalog',
            body: [
                'This drops every archived table from the Iceberg catalog and resets the archive footprint to zero.',
                { text: 'Data files in object storage are not deleted. Empty the bucket or container manually to reclaim space and avoid stale files shadowing new tables. Log data in ClickHouse is not affected.', muted: true },
            ],
            phrase: 'CLEAR CATALOG',
            confirmLabel: 'Clear Catalog',
            busyLabel: 'Clearing...',
            onConfirm: async () => {
                await HttpUtils.safeFetch('/api/v1/system/archive/clear', { method: 'POST' });
                if (window.Toast) Toast.success('Catalog Cleared', 'The archive was reset to zero. Re-enable archiving to start fresh.');
                this.loadArchiveToggle();
            },
        });
    },

    confirmClearEndpointBaselines() {
        DangerConfirm.open({
            title: 'Clear Endpoint Analytics Baselines',
            body: [
                'This empties the process lineage, frequency and edge baselines on every node. They rebuild from newly ingested logs while Collect baselines is on.',
                { text: 'Until they refill, nearly every process edge is first-seen, so pgr() flags most activity as high severity. Log data is not affected.', muted: true },
            ],
            phrase: 'CLEAR BASELINES',
            confirmLabel: 'Clear Baselines',
            busyLabel: 'Clearing...',
            onConfirm: async () => {
                await HttpUtils.safeFetch('/api/v1/system/endpoint-analysis/clear', { method: 'POST' });
                if (window.Toast) Toast.success('Baselines Cleared', 'They rebuild from newly ingested logs.');
            },
        });
    },

    async loadEndpointAnalysisToggle() {
        const toggle = document.getElementById('endpointAnalysisToggle');
        if (!toggle) return;
        try {
            const res = await fetch('/api/v1/system/endpoint-analysis', { credentials: 'include' });
            if (!res.ok) return;
            const d = await res.json();
            toggle.checked = !!d.enabled;
        } catch (err) {
            console.error('[Settings] endpoint-analysis load error:', err);
        }
    },

    async saveEndpointAnalysis() {
        const toggle = document.getElementById('endpointAnalysisToggle');
        const hint = document.getElementById('endpointAnalysisHint');
        if (!toggle) return;
        const enabled = toggle.checked;
        toggle.disabled = true;
        if (hint) hint.textContent = enabled ? 'Enabling…' : 'Disabling…';
        try {
            const res = await fetch('/api/v1/system/endpoint-analysis', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ enabled })
            });
            if (!res.ok) {
                const msg = await Utils.errorMessage(res);
                throw new Error(msg || 'Failed to update setting');
            }
            if (window.Toast) {
                Toast.success('Endpoint Analytics ' + (enabled ? 'Enabled' : 'Disabled'),
                    enabled ? 'Baselines are now building from new logs.' : 'Baseline collection paused. Existing baseline data is retained.');
            }
        } catch (err) {
            toggle.checked = !enabled; // revert on failure
            if (window.Toast) Toast.error('Update Failed', err.message);
        } finally {
            toggle.disabled = false;
            if (hint) hint.textContent = '';
        }
    },

    async loadSharedLinksToggle() {
        const toggle = document.getElementById('sharedLinksEnabledToggle');
        if (!toggle) return;
        try {
            const res = await fetch('/api/v1/system/shared-links', { credentials: 'include' });
            if (!res.ok) return;
            const d = await res.json();
            toggle.checked = !!d.enabled;
        } catch (err) {
            console.error('[Settings] shared-links load error:', err);
        }
    },

    // Turning the requirement on locks out everyone who has not enrolled until
    // they do, so the count of affected people is shown before it takes effect.
    async saveRequireMFA() {
        const toggle = document.getElementById('requireMFAToggle');
        if (!toggle) return;

        if (toggle.checked) {
            const unenrolled = (this._users || []).filter(u =>
                !u.totp_enrolled && u.auth_provider !== 'oidc' && u.enabled !== false && !u.invite_pending);
            const summary = unenrolled.length === 1
                ? '1 user will be asked to set up an authenticator at their next sign in.'
                : `${unenrolled.length} users will be asked to set up an authenticator at their next sign in.`;
            if (!confirm(`Require two-factor authentication?\n\n${summary}\nThey cannot use Bifract until they enroll.`)) {
                toggle.checked = false;
                return;
            }
        }

        await this.saveSettings(toggle);
        await this.loadSettings();
    },

    // Without a pepper there is no key to encrypt enrollment secrets with, so
    // the server refuses the setting. Say so up front instead of on save.
    async loadMFAAvailability() {
        const toggle = document.getElementById('requireMFAToggle');
        const blocked = document.getElementById('requireMFABlocked');
        if (!toggle || !blocked) return;
        try {
            const response = await fetch('/api/v1/auth/mfa/status', { credentials: 'include' });
            const data = await response.json();
            const available = data.success && data.data && data.data.available;
            toggle.disabled = !available;
            blocked.style.display = available ? 'none' : '';
            if (!available) {
                blocked.textContent = 'Unavailable: this deployment has no BIFRACT_PASSWORD_PEPPER set, so enrollment secrets could not be encrypted at rest.';
            }
        } catch (error) {
            console.error('[Settings] MFA availability error:', error);
        }
    },

    async saveSharedLinksEnabled() {
        const toggle = document.getElementById('sharedLinksEnabledToggle');
        const hint = document.getElementById('sharedLinksHint');
        if (!toggle) return;
        const enabled = toggle.checked;
        toggle.disabled = true;
        if (hint) hint.textContent = enabled ? 'Enabling…' : 'Disabling…';
        try {
            const res = await fetch('/api/v1/system/shared-links', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ enabled })
            });
            if (!res.ok) {
                const msg = await Utils.errorMessage(res);
                throw new Error(msg || 'Failed to update setting');
            }
            if (window.Toast) {
                Toast.success('Dashboard Shared Links ' + (enabled ? 'Enabled' : 'Disabled'),
                    enabled ? 'Analysts can now create public read-only dashboard links.' : 'Existing links now return not-found. They remain revocable.');
            }
        } catch (err) {
            toggle.checked = !enabled; // revert on failure
            if (window.Toast) Toast.error('Update Failed', err.message);
        } finally {
            toggle.disabled = false;
            if (hint) hint.textContent = '';
        }
    },

    switchSubTab(tabName, skipPush = false) {
        // The Limits tab was renamed to Settings; keep old deep links working.
        if (tabName === 'limits') tabName = 'settings';
        if (!skipPush) window.App?.pushSubPath(tabName);
        const tabBar = document.getElementById('settingsSubTabs');
        if (tabBar) {
            tabBar.querySelectorAll('.alerts-sub-tab').forEach(btn => btn.classList.remove('active'));
            const activeBtn = tabBar.querySelector(`.alerts-sub-tab[data-subtab="${tabName}"]`);
            if (activeBtn) activeBtn.classList.add('active');
        }
        document.querySelectorAll('.settings-sub-panel').forEach(panel => panel.style.display = 'none');
        const panel = document.getElementById('settingsSubTab' + tabName.charAt(0).toUpperCase() + tabName.slice(1));
        if (panel) panel.style.display = '';
        // Entering the Groups tab always lands on the list; detail is restored separately.
        if (tabName === 'groups' && window.GroupsView) GroupsView.closeDetail();
        if (tabName === 'context' && window.ContextLinks) ContextLinks.show();
        if (tabName === 'apikeys' && window.APIKeysAdmin) APIKeysAdmin.show();
    },

    async show(subPath = '') {
        this.isActive = true;

        // subPath is "<subTab>" or "<subTab>/<groupId>" (groups detail deep-link).
        const slash = subPath.indexOf('/');
        const subTab = slash === -1 ? subPath : subPath.slice(0, slash);
        const detailId = slash === -1 ? '' : subPath.slice(slash + 1);

        // Reveal the requested panel before loading, so a deep link shows its own
        // sub-tab immediately instead of the default one until every fetch lands.
        if (subTab) this.switchSubTab(subTab, true);

        // These loads are independent, so awaiting them in sequence would make the
        // page cost the sum of every round trip. Each has its own error handling,
        // so none of them can reject and abort the batch.
        const groupsLoad = window.GroupsView ? GroupsView.loadGroups() : null;
        await Promise.all([
            this.loadSettings(),
            this.loadPgrCalibration(),
            this.loadArchiveToggle(),
            this.loadEndpointAnalysisToggle(),
            this.loadSharedLinksToggle(),
            this.loadMTLSStatus(),
            this.loadMFAAvailability(),
            this.loadUsers(),
        ]);

        if (subTab === 'context' && detailId && window.ContextLinks) {
            ContextLinks.show(detailId);
        }
        if (subTab === 'groups' && detailId && window.GroupsView) {
            if (groupsLoad) await groupsLoad;
            GroupsView.openDetail(detailId, true);
        }
    },

    hide() {
        const settingsView = document.getElementById('settingsView');
        if (settingsView) {
            settingsView.style.display = 'none';
        }

        this.isActive = false;
    },

    // Controls whose stored value only takes effect if ClickHouse permits it.
    // Without this a share the server refused still renders as the configured
    // percentage, so the page reports a limit that is not in force.
    CAPABILITY_GATED_CONTROLS: {
        queryCPUPercentSettings: 'workload_scheduling',
        recallCPUPercentSettings: 'workload_scheduling',
        queryMemoryPercentSettings: 'server_memory_budget',
        recallMemoryPercentSettings: 'server_memory_budget',
    },

    // Disables a control and says why, reusing the sp-row-blocked treatment the
    // archive and danger-zone rows already use. The hint is created on demand so
    // each new gated control needs no extra markup.
    applyCapabilityGates(capabilities) {
        const caps = capabilities || {};
        for (const [controlId, capKey] of Object.entries(this.CAPABILITY_GATED_CONTROLS)) {
            const control = document.getElementById(controlId);
            if (!control) continue;
            const cap = caps[capKey] || {};
            // Unknown means not yet exercised, not broken; only an explicit
            // refusal disables the control.
            const blocked = cap.state === 'unavailable';

            control.disabled = blocked;
            const info = control.closest('.sp-row')?.querySelector('.sp-row-info');
            if (!info) continue;

            let hint = info.querySelector('.sp-row-blocked');
            if (!blocked) {
                if (hint) hint.style.display = 'none';
                continue;
            }
            if (!hint) {
                hint = document.createElement('p');
                hint.className = 'sp-row-blocked';
                info.appendChild(hint);
            }
            hint.textContent = cap.reason
                ? `Not in force: ${cap.reason}`
                : 'Not in force: this ClickHouse server does not support it.';
            hint.style.display = '';
        }
    },

    async loadSettings() {
        try {
            const response = await fetch('/api/v1/settings', { credentials: 'include' });
            const data = await response.json();

            if (data.success) {
                this.applyCapabilityGates(data.capabilities);
                // Load system limits
                const alertTimeoutSelect = document.getElementById('alertTimeoutSettings');
                if (alertTimeoutSelect) {
                    alertTimeoutSelect.value = String(data.settings.alert_timeout_seconds || 5);
                }
                const queryTimeoutSelect = document.getElementById('queryTimeoutSettings');
                if (queryTimeoutSelect) {
                    queryTimeoutSelect.value = String(data.settings.query_timeout_seconds ?? 60);
                }
                const queryCPUPercentSelect = document.getElementById('queryCPUPercentSettings');
                if (queryCPUPercentSelect) {
                    queryCPUPercentSelect.value = String(data.settings.query_cpu_percent ?? 50);
                }
                const queryMemoryPercentSelect = document.getElementById('queryMemoryPercentSettings');
                if (queryMemoryPercentSelect) {
                    queryMemoryPercentSelect.value = String(data.settings.query_memory_percent ?? 50);
                }
                const alertEvalIntervalSelect = document.getElementById('alertEvalIntervalSettings');
                if (alertEvalIntervalSelect) {
                    alertEvalIntervalSelect.value = String(data.settings.alert_eval_interval_seconds || 60);
                }
                const alertRevisionRetentionSelect = document.getElementById('alertRevisionRetentionSettings');
                if (alertRevisionRetentionSelect) {
                    alertRevisionRetentionSelect.value = String(data.settings.alert_revision_retention || 10);
                }
                const recallTimeoutSelect = document.getElementById('recallTimeoutSettings');
                if (recallTimeoutSelect) {
                    recallTimeoutSelect.value = String(data.settings.recall_timeout_seconds || 900);
                }
                const recallMaxBytesSelect = document.getElementById('recallMaxBytesSettings');
                if (recallMaxBytesSelect) {
                    recallMaxBytesSelect.value = String(data.settings.recall_max_bytes_read ?? 0);
                }
                const recallConcurrencySelect = document.getElementById('recallConcurrencySettings');
                if (recallConcurrencySelect) {
                    recallConcurrencySelect.value = String(data.settings.recall_concurrency || 5);
                }
                const pgrSensSelect = document.getElementById('pgrSensitivitySettings');
                if (pgrSensSelect) {
                    pgrSensSelect.value = String(data.settings.pgr_sensitivity_percent || 2);
                }
                const schemaSweepSelect = document.getElementById('schemaSweepIntervalSettings');
                if (schemaSweepSelect) {
                    schemaSweepSelect.value = String(data.settings.schema_sweep_interval_minutes || 15);
                }
                const recallCPUSelect = document.getElementById('recallCPUPercentSettings');
                if (recallCPUSelect) {
                    recallCPUSelect.value = String(data.settings.recall_cpu_percent ?? 25);
                }
                const recallMemorySelect = document.getElementById('recallMemoryPercentSettings');
                if (recallMemorySelect) {
                    recallMemorySelect.value = String(data.settings.recall_memory_percent ?? 25);
                }
                const requireMFAToggle = document.getElementById('requireMFAToggle');
                if (requireMFAToggle) {
                    requireMFAToggle.checked = data.settings.require_mfa === true;
                }
            }
        } catch (error) {
            console.error('Failed to load settings:', error);
        }
    },

    // Brief "Saved" confirmation next to the control that changed. Autosaving
    // selects otherwise give no signal that the change took effect.
    flashSaved(el) {
        if (!el || !el.parentElement) return;
        const control = el.parentElement;
        control.querySelector('.sp-saved')?.remove();
        const pill = document.createElement('span');
        pill.className = 'sp-saved';
        pill.textContent = 'Saved';
        control.insertBefore(pill, el);
        setTimeout(() => pill.classList.add('fade'), 1200);
        setTimeout(() => pill.remove(), 1600);
    },

    async saveSettings(triggerEl) {
        try {
            const response = await fetch('/api/v1/settings', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({
                    alert_timeout_seconds: parseInt(document.getElementById('alertTimeoutSettings')?.value || '5', 10),
                    query_timeout_seconds: parseInt(document.getElementById('queryTimeoutSettings')?.value || '60', 10),
                    query_cpu_percent: parseInt(document.getElementById('queryCPUPercentSettings')?.value || '50', 10),
                    query_memory_percent: parseInt(document.getElementById('queryMemoryPercentSettings')?.value || '50', 10),
                    alert_eval_interval_seconds: parseInt(document.getElementById('alertEvalIntervalSettings')?.value || '60', 10),
                    alert_revision_retention: parseInt(document.getElementById('alertRevisionRetentionSettings')?.value || '10', 10),
                    recall_timeout_seconds: parseInt(document.getElementById('recallTimeoutSettings')?.value || '900', 10),
                    recall_max_bytes_read: parseInt(document.getElementById('recallMaxBytesSettings')?.value || '0', 10),
                    recall_concurrency: parseInt(document.getElementById('recallConcurrencySettings')?.value || '5', 10),
                    recall_cpu_percent: parseInt(document.getElementById('recallCPUPercentSettings')?.value || '25', 10),
                    recall_memory_percent: parseInt(document.getElementById('recallMemoryPercentSettings')?.value || '25', 10),
                    schema_sweep_interval_minutes: parseInt(document.getElementById('schemaSweepIntervalSettings')?.value || '15', 10),
                    pgr_sensitivity_percent: parseFloat(document.getElementById('pgrSensitivitySettings')?.value || '2'),
                    require_mfa: document.getElementById('requireMFAToggle')?.checked === true
                })
            });

            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to save settings');
            this.flashSaved(triggerEl);
            this.loadPgrCalibration();
        } catch (error) {
            console.error('Failed to save settings:', error);
            if (window.Toast) Toast.error('Save Failed', error.message);
            // Re-read the server state so the control never shows an unsaved value.
            this.loadSettings();
        }
    },

    async loadMTLSStatus() {
        try {
            const response = await fetch('/api/v1/users/mtls-status', { credentials: 'include' });
            const data = await response.json();
            if (data.success && data.data) {
                this.mtlsEnabled = data.data.mtls_enabled === true;
            }
        } catch {
            this.mtlsEnabled = false;
        }
    },

    async downloadClientCert(username) {
        const password = prompt('Enter a password to protect the .p12 certificate:');
        if (!password) return;

        try {
            const response = await fetch(`/api/v1/users/${encodeURIComponent(username)}/client-cert`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ password })
            });

            if (!response.ok) {
                const data = await response.json();
                if (window.Toast) Toast.error('Error', data.error || 'Failed to generate certificate');
                return;
            }

            const blob = await response.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${username}.p12`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        } catch (error) {
            console.error('Error downloading client cert:', error);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    async loadUsers() {
        try {
            const response = await fetch('/api/v1/users?limit=500', { credentials: 'include' });
            const data = await response.json();

            if (data.success) {
                this.renderUsers(data.data || []);
            } else {
                // API call succeeded but returned error
                this.renderUsers([]);
            }
        } catch (error) {
            console.error('Failed to load users:', error);
            // Network error or other failure - still show empty state
            this.renderUsers([]);
        }
    },

    renderUsers(users) {
        // Keep the full set so the Edit modal can resolve display name/role by
        // username instead of threading them (unescaped) through inline onclick handlers.
        this._users = users;
        this._paintUsers();
    },

    _paintUsers() {
        const container = document.getElementById('usersListSettings');
        if (!container) return;

        const all = this._users || [];
        if (all.length === 0) {
            container.innerHTML = '<div class="admin-empty">Only the default admin user exists</div>';
            return;
        }

        const q = (this._userFilter || '').trim().toLowerCase();
        const users = q
            ? all.filter(u => `${u.display_name || ''} ${u.username || ''}`.toLowerCase().includes(q))
            : all;

        if (users.length === 0) {
            container.innerHTML = '<div class="admin-empty">No users match this search</div>';
            return;
        }

        let html = '<table class="users-table"><thead><tr>';
        html += '<th>User</th><th>Role</th><th>Status</th><th class="kebab-th"></th>';
        html += '</tr></thead><tbody>';

        const currentUser = Auth.getCurrentUser();

        users.forEach(user => {
            const isSelf = currentUser && currentUser.username === user.username;
            const lastLogin = user.last_login ? TZ.format(user.last_login, 'friendly') : 'Never';
            const isAdmin = currentUser && currentUser.is_admin;

            const u = Utils.escapeJs(user.username);

            // Primary action: clicking the user's identity opens the Edit modal
            // (mirrors notebooks/dashboards opening on name-click). Admins only.
            const infoClick = isAdmin ? ` user-info-clickable" onclick="SettingsView.openEditUserModal('${u}')" title="Edit user"` : '"';
            const selfTag = isSelf ? ' <span class="user-self-tag">You</span>' : '';

            html += `<tr>`;
            html += `<td>
                <div class="user-cell">
                    <div class="gravatar" style="background-color: ${user.gravatar_color}">
                        ${user.gravatar_initial}
                    </div>
                    <div class="user-info${infoClick}>
                        <div class="user-name">${Utils.escapeHtml(user.display_name)}${selfTag}</div>
                        <div class="user-username">@${Utils.escapeHtml(user.username)}</div>
                    </div>
                </div>
            </td>`;
            const mfaBadge = user.totp_enrolled
                ? `<span class="user-mfa-badge" title="Two-factor authentication enabled"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg></span>`
                : '';
            html += `<td><span class="role-badge role-${user.role}">${user.role === 'admin' ? 'Tenant Admin' : 'User'}</span>${mfaBadge}</td>`;

            const isDisabled = user.enabled === false;

            if (isDisabled) {
                html += `<td><span class="role-badge role-disabled">Disabled</span></td>`;
            } else if (user.invite_pending) {
                html += `<td><span class="role-badge role-pending">Invite pending</span></td>`;
            } else {
                html += `<td class="text-muted">${lastLogin}</td>`;
            }

            // Secondary / destructive actions live in a hover-revealed kebab overflow menu.
            const items = [];
            if (this.mtlsEnabled && isAdmin && !user.invite_pending) {
                items.push(`<button class="kebab-item" onclick="SettingsView.downloadClientCert('${u}')">Download mTLS Cert</button>`);
            }
            if (user.invite_pending && isAdmin) {
                items.push(`<button class="kebab-item" onclick="SettingsView.resetInvite('${u}')">Resend Invite</button>`);
            } else if (!isSelf && isAdmin) {
                items.push(`<button class="kebab-item" onclick="SettingsView.resetPassword('${u}')">Reset Password</button>`);
            }
            if (!isSelf && isAdmin && !user.invite_pending) {
                items.push(isDisabled
                    ? `<button class="kebab-item" onclick="SettingsView.setUserEnabled('${u}', true)">Enable</button>`
                    : `<button class="kebab-item" onclick="SettingsView.setUserEnabled('${u}', false)">Disable</button>`);
            }
            if (isAdmin && user.totp_enrolled) {
                items.push(`<button class="kebab-item" onclick="SettingsView.resetMFA('${u}')">Reset Two-Factor</button>`);
            }
            if (!isSelf && isAdmin) {
                items.push(`<button class="kebab-item danger" onclick="SettingsView.deleteUser('${u}')">Delete</button>`);
            }

            html += `<td class="kebab-cell">`;
            if (items.length) {
                html += `<div class="kebab-wrapper"><button class="kebab-btn" onclick="KebabMenu.toggle(event,this)" title="More actions">&#8942;</button><div class="kebab-menu">${items.join('')}</div></div>`;
            }
            html += `</td></tr>`;
        });

        html += '</tbody></table>';
        container.innerHTML = html;
    },

    openEditUserModal(username) {
        const user = (this._users || []).find(x => x.username === username);
        if (!user) return;
        const modal = document.getElementById('editUserModal');
        if (!modal) return;
        this._editUserUsername = username;
        document.getElementById('editUserUsername').value = '@' + username;
        document.getElementById('editUserDisplayName').value = user.display_name || '';
        const roleSelect = document.getElementById('editUserRole');
        roleSelect.value = user.role;
        document.getElementById('editUserError').textContent = '';

        // Prevent changing your own role (an admin could otherwise lock themselves out).
        const currentUser = Auth.getCurrentUser();
        const isSelf = currentUser && currentUser.username === username;
        roleSelect.disabled = isSelf;
        document.getElementById('editUserRoleHint').style.display = isSelf ? 'none' : 'block';
        document.getElementById('editUserRoleSelfNote').style.display = isSelf ? 'block' : 'none';
        modal.style.display = 'flex';
        setTimeout(() => document.getElementById('editUserDisplayName')?.focus(), 100);
    },

    hideEditUserModal() {
        const modal = document.getElementById('editUserModal');
        if (modal) modal.style.display = 'none';
        this._editUserUsername = null;
    },

    async saveUserEdit() {
        const username = this._editUserUsername;
        if (!username) return;
        const displayName = document.getElementById('editUserDisplayName')?.value.trim();
        const role = document.getElementById('editUserRole')?.value;
        const errorDiv = document.getElementById('editUserError');
        errorDiv.textContent = '';

        if (!displayName) {
            errorDiv.textContent = 'Display name cannot be empty';
            return;
        }

        try {
            const response = await fetch(`/api/v1/users/${encodeURIComponent(username)}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ display_name: displayName, role })
            });

            const data = await response.json();
            if (data.success) {
                this.hideEditUserModal();
                await this.loadUsers();
            } else {
                errorDiv.textContent = data.error || 'Failed to update user';
            }
        } catch (error) {
            console.error('Error updating user:', error);
            errorDiv.textContent = 'Network error. Please try again.';
        }
    },

    // The way back in for someone who has lost both their device and their
    // recovery codes. They enroll again at their next sign in.
    async resetMFA(username) {
        if (!confirm(`Reset two-factor authentication for @${username}?\n\nThey will be signed out and asked to set up an authenticator again at their next sign in.`)) {
            return;
        }

        try {
            const response = await fetch('/api/v1/auth/admin-reset-mfa', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ username })
            });

            const data = await response.json();
            if (data.success) {
                if (window.Toast) Toast.success('Two-Factor Reset', data.message);
                await this.loadUsers();
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to reset two-factor authentication');
            }
        } catch (error) {
            console.error('Error resetting MFA:', error);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    async setUserEnabled(username, enabled) {
        const action = enabled ? 'enable' : 'disable';
        if (!enabled && !confirm(`Disable @${username}? They will be signed out and unable to log in until re-enabled.`)) {
            return;
        }

        try {
            const response = await fetch(`/api/v1/users/${encodeURIComponent(username)}/enabled`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ enabled })
            });

            const data = await response.json();
            if (data.success) {
                if (window.Toast) Toast.success('Success', data.message || `User ${action}d`);
                await this.loadUsers();
            } else {
                if (window.Toast) Toast.error('Error', data.error || `Failed to ${action} user`);
            }
        } catch (error) {
            console.error(`Error trying to ${action} user:`, error);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    showAddUserForm() {
        const modal = document.getElementById('createUserModal');
        if (modal) modal.style.display = 'flex';
        // Reset to the form view in case a previous invite result is still shown.
        document.getElementById('addUserFormSection').style.display = 'block';
        const inviteSection = document.getElementById('addUserInviteSection');
        inviteSection.style.display = 'none';
        inviteSection.innerHTML = '';
        setTimeout(() => document.getElementById('newUsernameSettings')?.focus(), 100);
    },

    hideAddUserForm() {
        const modal = document.getElementById('createUserModal');
        if (modal) modal.style.display = 'none';
        document.getElementById('newUsernameSettings').value = '';
        document.getElementById('newDisplayNameSettings').value = '';
        document.getElementById('newUserRoleSettings').value = 'user';
        document.getElementById('addUserErrorSettings').textContent = '';
        document.getElementById('addUserFormSection').style.display = 'block';
        const inviteSection = document.getElementById('addUserInviteSection');
        inviteSection.style.display = 'none';
        inviteSection.innerHTML = '';
    },

    async createUser() {
        const username = document.getElementById('newUsernameSettings').value.trim();
        const displayName = document.getElementById('newDisplayNameSettings').value.trim();
        const role = document.getElementById('newUserRoleSettings').value;
        const errorDiv = document.getElementById('addUserErrorSettings');

        errorDiv.textContent = '';

        if (!username) {
            errorDiv.textContent = 'Username is required';
            return;
        }

        try {
            const response = await fetch('/api/v1/auth/register', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({
                    username,
                    display_name: displayName || username,
                    role
                })
            });

            const data = await response.json();

            if (data.success) {
                await this.loadUsers();
                this.showInviteInModal(data.data.invite_url, username);
            } else {
                errorDiv.textContent = data.error || 'Failed to create user';
            }
        } catch (error) {
            console.error('Error creating user:', error);
            errorDiv.textContent = 'Network error. Please try again.';
        }
    },

    showInviteInModal(path, username) {
        const url = window.location.origin + path;
        document.getElementById('addUserFormSection').style.display = 'none';
        const section = document.getElementById('addUserInviteSection');
        section.style.display = 'block';
        section.innerHTML = `
            <div class="invite-link-content">
                <div class="invite-link-header">Invite link for <strong>${Utils.escapeHtml(username)}</strong></div>
                <div class="invite-link-note">Share this link with the user. It expires in 7 days.</div>
                <div class="invite-link-row">
                    <input type="text" class="invite-link-input" value="${Utils.escapeHtml(url)}" readonly id="inviteLinkInputModal">
                    <button class="btn-primary btn-sm" onclick="SettingsView.copyInviteLink('inviteLinkInputModal')">Copy</button>
                </div>
            </div>
            <div class="form-actions">
                <button class="btn-secondary" onclick="SettingsView.hideAddUserForm()">Done</button>
            </div>
        `;
    },

    showInviteLink(path, username) {
        const url = window.location.origin + path;
        let container = document.getElementById('inviteLinkBanner');
        if (!container) {
            container = document.createElement('div');
            container.id = 'inviteLinkBanner';
            container.className = 'invite-link-banner';
            const settingsCard = document.getElementById('usersListSettings').closest('.settings-card');
            settingsCard.insertBefore(container, settingsCard.firstChild);
        }
        container.style.display = 'block';
        container.innerHTML = `
            <div class="invite-link-content">
                <div class="invite-link-header">Invite link for <strong>${Utils.escapeHtml(username)}</strong></div>
                <div class="invite-link-note">Share this link with the user. It expires in 7 days.</div>
                <div class="invite-link-row">
                    <input type="text" class="invite-link-input" value="${Utils.escapeHtml(url)}" readonly id="inviteLinkInput">
                    <button class="btn-primary btn-sm" onclick="SettingsView.copyInviteLink()">Copy</button>
                </div>
            </div>
            <button class="invite-link-close" onclick="SettingsView.hideInviteLink()">&times;</button>
        `;
    },

    hideInviteLink() {
        const banner = document.getElementById('inviteLinkBanner');
        if (banner) banner.style.display = 'none';
    },

    copyInviteLink(inputId = 'inviteLinkInput') {
        const input = document.getElementById(inputId);
        if (input) {
            navigator.clipboard.writeText(input.value);
        }
    },

    async resetInvite(username) {
        try {
            const response = await fetch('/api/v1/auth/invite/reset', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ username })
            });

            const data = await response.json();
            if (data.success) {
                this.showInviteLink(data.data.invite_url, username);
            } else {
                if (window.Toast) {
                    Toast.error('Error', data.error || 'Failed to regenerate invite');
                }
            }
        } catch (error) {
            console.error('Error resetting invite:', error);
        }
    },

    async resetPassword(username) {
        if (!confirm(`Reset password for '${username}'? They will need to use an invite link to set a new password.`)) {
            return;
        }

        try {
            const response = await fetch('/api/v1/auth/admin-reset-password', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ username })
            });

            const data = await response.json();

            if (data.success) {
                await this.loadUsers();
                this.showInviteLink(data.data.invite_url, username);
                if (window.Toast) {
                    Toast.success('Password Reset', `Password reset for '${username}'. Share the invite link.`);
                }
            } else {
                if (window.Toast) {
                    Toast.error('Error', data.error || 'Failed to reset password');
                } else {
                    alert('Failed to reset password: ' + (data.error || 'Unknown error'));
                }
            }
        } catch (error) {
            console.error('Error resetting password:', error);
            if (window.Toast) {
                Toast.error('Network Error', 'Please try again.');
            }
        }
    },

    async deleteUser(username) {
        if (!confirm(`Are you sure you want to delete user '${username}'?`)) {
            return;
        }

        try {
            const response = await fetch(`/api/v1/users?username=${encodeURIComponent(username)}`, {
                method: 'DELETE',
                credentials: 'include'
            });

            const data = await response.json();

            if (data.success) {
                await this.loadUsers();
                alert(`User '${username}' deleted successfully.`);
            } else {
                alert('Failed to delete user: ' + (data.error || 'Unknown error'));
            }
        } catch (error) {
            console.error('Error deleting user:', error);
            alert('Network error. Please try again.');
        }
    },

    clearLogs() {
        DangerConfirm.open({
            title: 'Clear All Logs',
            body: [
                'This permanently deletes every log in every fractal, along with their comments and everything derived from them, including endpoint analytics baselines.',
                { text: 'Fractals, alerts, dashboards and settings remain.', muted: true },
            ],
            phrase: 'DELETE ALL LOGS',
            confirmLabel: 'Delete All Logs',
            busyLabel: 'Deleting...',
            onConfirm: async () => {
                const data = await HttpUtils.safeFetch('/api/v1/logs', { method: 'DELETE' });
                if (data && data.success === false) throw new Error(data.error || 'Failed to clear logs');
                if (window.Toast) Toast.success('Logs Cleared', 'All logs and their comments were deleted.');
            },
        });
    },

};

// Make globally available
window.SettingsView = SettingsView;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    SettingsView.init();
});

// Process-graph calibration readout: what the derived cutoffs are, how much they currently flag,
// and whether the node-stability terms have enough baseline to be active. Without this the
// sensitivity knob would be a dial with no gauge.
SettingsView.loadPgrCalibration = async function () {
    const el = document.getElementById('pgrCalibrationReadout');
    if (!el) return;
    try {
        const r = await fetch('/api/v1/system/pgr-calibration', { credentials: 'same-origin' });
        if (!r.ok) { el.textContent = 'Unavailable.'; return; }
        const j = await r.json();
        const parts = [];
        if (j.cutoffs && j.cutoffs.calibrated) {
            parts.push(`Calibrated from ${Number(j.cutoffs.samples).toLocaleString()} scored edges: high at ${j.cutoffs.high.toFixed(2)}, medium at ${j.cutoffs.med.toFixed(2)}.`);
            if (j.observed) parts.push(`Currently ${j.observed.high_percent.toFixed(1)}% render high, ${j.observed.med_percent.toFixed(1)}% medium or above.`);
        } else {
            const n = (j.cutoffs && j.cutoffs.samples) || 0;
            parts.push(`Not yet calibrated (${Number(n).toLocaleString()} scored edges so far); using default cutoffs until enough process graphs have been run.`);
        }
        const bl = j.baselines || [];
        if (bl.length) {
            const active = bl.filter(b => b.stability_active).length;
            parts.push(`Baseline maturity: ${active} of ${bl.length} fractal(s) have the ${j.min_stability_days}+ days of history that node-stability scoring needs; the rest score on transition frequency alone.`);
        }
        el.textContent = parts.join(' ');
    } catch (e) {
        el.textContent = 'Unavailable.';
    }
};
