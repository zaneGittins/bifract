// Settings View module

const SettingsView = {
    isActive: false,
    mtlsEnabled: false,

    async init() {
        // Set up tab navigation
        const settingsTab = document.getElementById('settingsTabBtn');
        const searchTab = document.getElementById('searchTabBtn');
        const commentedTab = document.getElementById('commentedTabBtn');

        if (settingsTab) {
            settingsTab.addEventListener('click', () => this.show());
        }

        if (searchTab) {
            searchTab.addEventListener('click', () => this.hide());
        }

        if (commentedTab) {
            commentedTab.addEventListener('click', () => this.hide());
        }

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
        ['alertTimeoutSettings', 'queryTimeoutSettings', 'alertEvalIntervalSettings'].forEach(id => {
            const select = document.getElementById(id);
            if (select) select.addEventListener('change', () => this.saveSettings(select));
        });
        const archiveToggle = document.getElementById('archiveEnabledToggle');
        if (archiveToggle) {
            archiveToggle.addEventListener('change', () => this.saveArchiveEnabled());
        }
        const clearCatalogBtn = document.getElementById('archiveClearCatalogBtn');
        if (clearCatalogBtn) {
            clearCatalogBtn.addEventListener('click', () => this.openClearCatalogModal());
        }
        const clearSpoolBtn = document.getElementById('archiveClearSpoolBtn');
        if (clearSpoolBtn) {
            clearSpoolBtn.addEventListener('click', () => this.clearSpool());
        }
        const endpointAnalysisToggle = document.getElementById('endpointAnalysisToggle');
        if (endpointAnalysisToggle) {
            endpointAnalysisToggle.addEventListener('change', () => this.saveEndpointAnalysis());
        }
        const sharedLinksToggle = document.getElementById('sharedLinksEnabledToggle');
        if (sharedLinksToggle) {
            sharedLinksToggle.addEventListener('change', () => this.saveSharedLinksEnabled());
        }

        this.loadDistQueueShards();
        this.initSectionRail();
    },

    // Loads per-shard distribution queue stats and renders one row + Reset
    // button per shard that actually has something queued. Single-node
    // deployments and healthy clusters get an empty (hidden) list back.
    async loadDistQueueShards() {
        const item = document.getElementById('distQueueResetItem');
        const container = document.getElementById('distQueueShardList');
        if (!item || !container) return;
        try {
            const res = await fetch('/api/v1/system/distribution-queue/shards', { credentials: 'include' });
            if (!res.ok) { item.style.display = 'none'; return; }
            const shards = await res.json();
            const affected = (shards || []).filter(s => s.unreachable || s.data_files > 0 || s.broken_data_files > 0 || s.error_count > 0);
            item.style.display = affected.length ? '' : 'none';
            container.innerHTML = '';
            affected.forEach(s => {
                const row = document.createElement('div');
                row.className = 'dist-queue-shard-row';
                const label = document.createElement('span');
                label.className = 'dist-queue-shard-label';
                if (s.unreachable) {
                    label.textContent = `Shard ${s.shard_num} (${s.host}): unreachable, cannot report or reset right now`;
                    row.appendChild(label);
                    container.appendChild(row);
                    return; // no Reset button: the same connection failure would just repeat
                }
                const parts = [`${s.data_files} file(s) queued`];
                if (s.broken_data_files) parts.push(`${s.broken_data_files} broken`);
                if (s.error_count) parts.push(`${s.error_count} error(s)`);
                label.textContent = `Shard ${s.shard_num} (${s.host}): ${parts.join(', ')}`;
                const btn = document.createElement('button');
                btn.className = 'danger-btn';
                btn.textContent = 'Reset Queue';
                btn.dataset.shard = String(s.shard_num);
                btn.addEventListener('click', () => this.resetDistQueueShard(btn));
                row.appendChild(label);
                row.appendChild(btn);
                container.appendChild(row);
            });
        } catch (err) {
            console.error('[Settings] distribution queue shard load error:', err);
        }
    },

    // Two-step arm-then-confirm, matching clearSpool(): this discards a
    // shard's queued-but-unsendable batches, which is real but bounded data
    // loss (the same category as clearSpool), not the whole-catalog severity
    // that warrants a typed-phrase modal.
    async resetDistQueueShard(btn) {
        const shardNum = btn.dataset.shard;
        if (btn.dataset.armed !== '1') {
            btn.dataset.armed = '1';
            btn.dataset.label = btn.textContent;
            btn.textContent = 'Click again to confirm';
            clearTimeout(btn._armTimer);
            btn._armTimer = setTimeout(() => {
                btn.dataset.armed = '0';
                btn.textContent = btn.dataset.label || 'Reset Queue';
            }, 5000);
            return;
        }
        btn.dataset.armed = '0';
        clearTimeout(btn._armTimer);
        const original = btn.dataset.label || 'Reset Queue';
        try {
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner"></span> Resetting...';
            const res = await fetch('/api/v1/system/distribution-queue/reset', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ shard_num: Number(shardNum) })
            });
            if (!res.ok) {
                const msg = await res.text();
                throw new Error(msg || 'Failed to reset distribution queue');
            }
            if (window.Toast) Toast.success('Distribution Queue Reset', `Shard ${shardNum}'s queue was cleared.`);
            this.loadDistQueueShards();
        } catch (err) {
            if (window.Toast) Toast.error('Reset Failed', (err.message || '').trim());
            btn.innerHTML = original;
            btn.disabled = false;
        }
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

    // Requests a spool clear across all ingest pods. Two-step arm (click, then
    // confirm) rather than a typed modal: the spool is a transient buffer, so this
    // is less catastrophic than clearing the catalog, but it still discards
    // un-archived data, so it is not a single silent click.
    async clearSpool() {
        const btn = document.getElementById('archiveClearSpoolBtn');
        if (!btn || btn.disabled) return;
        if (!this._spoolArmed) {
            this._spoolArmed = true;
            btn.dataset.label = btn.textContent;
            btn.textContent = 'Click again to confirm';
            clearTimeout(this._spoolArmTimer);
            this._spoolArmTimer = setTimeout(() => {
                this._spoolArmed = false;
                btn.textContent = btn.dataset.label || 'Clear Spool';
            }, 5000);
            return;
        }
        this._spoolArmed = false;
        clearTimeout(this._spoolArmTimer);
        const original = btn.dataset.label || 'Clear Spool';
        try {
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner"></span> Requesting...';
            const res = await fetch('/api/v1/system/archive/spool/clear', {
                method: 'POST',
                credentials: 'include'
            });
            if (!res.ok) {
                const msg = await res.text();
                throw new Error(msg || 'Failed to clear spool');
            }
            const d = await res.json().catch(() => ({}));
            if (window.Toast) {
                Toast.success('Spool Clear Requested', d.message || 'Each ingest pod will clear its spool shortly.');
            }
        } catch (err) {
            if (window.Toast) Toast.error('Clear Spool Failed', (err.message || '').trim());
        } finally {
            btn.innerHTML = original;
            this.syncClearCatalogGuard(document.getElementById('archiveEnabledToggle')?.checked);
        }
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
                hint.textContent = d.provisioned ? '' : 'Not provisioned. Run bifract --upgrade to add the archiver.';
                hint.style.display = d.provisioned ? 'none' : '';
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
                const msg = await res.text();
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

    // Opens the typed-confirmation dialog. The destructive call itself lives in
    // clearCatalog(), which the dialog invokes once the phrase matches.
    openClearCatalogModal() {
        const modal = document.getElementById('archiveClearCatalogModal');
        const input = document.getElementById('archiveClearCatalogConfirmInput');
        const confirmBtn = document.getElementById('archiveClearCatalogConfirmBtn');
        if (!modal || !input || !confirmBtn) return;

        input.value = '';
        confirmBtn.disabled = true;
        modal.style.display = 'flex';
        setTimeout(() => input.focus(), 100);

        const phrase = 'CLEAR CATALOG';
        const validate = () => {
            confirmBtn.disabled = input.value.trim().toUpperCase() !== phrase;
        };
        // Replace prior handlers so reopening the dialog never stacks listeners.
        input.oninput = validate;
        input.onkeydown = (e) => {
            if (e.key === 'Enter' && !confirmBtn.disabled) this.clearCatalog();
            if (e.key === 'Escape') this.closeClearCatalogModal();
        };
        confirmBtn.onclick = () => this.clearCatalog();
        document.getElementById('archiveClearCatalogCancelBtn').onclick = () => this.closeClearCatalogModal();
    },

    closeClearCatalogModal() {
        const modal = document.getElementById('archiveClearCatalogModal');
        if (modal) modal.style.display = 'none';
    },

    // Clears the Iceberg catalog (all archived tables + namespace) via the
    // admin-only endpoint, resetting the archive footprint to zero. The server
    // also rejects this while archiving is enabled; the UI guard mirrors that.
    async clearCatalog() {
        this.closeClearCatalogModal();
        const btn = document.getElementById('archiveClearCatalogBtn');
        const original = btn ? btn.innerHTML : '';
        try {
            if (btn) {
                btn.disabled = true;
                btn.innerHTML = '<span class="spinner"></span> Clearing...';
            }
            const res = await fetch('/api/v1/system/archive/clear', {
                method: 'POST',
                credentials: 'include'
            });
            if (!res.ok) {
                const msg = await res.text();
                throw new Error(msg || 'Failed to clear catalog');
            }
            if (window.Toast) {
                Toast.success('Catalog Cleared', 'The archive was reset to zero. Re-enable archiving to start fresh.');
            }
            // Refresh the toggle/status view (footprint is shown under System -> Archive).
            this.loadArchiveToggle();
        } catch (err) {
            if (window.Toast) Toast.error('Clear Catalog Failed', err.message.trim());
        } finally {
            if (btn) {
                btn.innerHTML = original;
                // Restore the guard rather than blindly re-enabling: the archive
                // may have been switched back on while the request was in flight.
                this.syncClearCatalogGuard(document.getElementById('archiveEnabledToggle')?.checked);
            }
        }
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
                const msg = await res.text();
                throw new Error(msg || 'Failed to update setting');
            }
            if (window.Toast) {
                Toast.success('Endpoint Behavioral Analytics ' + (enabled ? 'Enabled' : 'Disabled'),
                    enabled ? 'Process baselines are now building from new logs.' : 'Per-insert analysis paused. Existing baseline data is retained.');
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
                const msg = await res.text();
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
    },

    async show(subPath = '') {
        // Hide other views
        const searchView = document.getElementById('searchView');
        const commentedView = document.getElementById('commentedView');
        const alertsView = document.getElementById('alertsView');
        const alertEditorView = document.getElementById('alertEditorView');
        const settingsView = document.getElementById('settingsView');
        const referenceView = document.getElementById('referenceView');
        const searchTab = document.getElementById('searchTabBtn');
        const commentedTab = document.getElementById('commentedTabBtn');
        const alertsTab = document.getElementById('alertsTabBtn');
        const settingsTab = document.getElementById('settingsTabBtn');
        const referenceTab = document.getElementById('referenceTabBtn');

        if (searchView) searchView.style.display = 'none';
        if (commentedView) commentedView.style.display = 'none';
        if (alertsView) alertsView.style.display = 'none';
        if (alertEditorView) alertEditorView.style.display = 'none';
        const actionsManageView = document.getElementById('actionsManageView');
        if (actionsManageView) actionsManageView.style.display = 'none';
        if (referenceView) referenceView.style.display = 'none';
        if (settingsView) settingsView.style.display = 'block';

        if (searchTab) searchTab.classList.remove('active');
        if (commentedTab) commentedTab.classList.remove('active');
        if (alertsTab) alertsTab.classList.remove('active');
        if (referenceTab) referenceTab.classList.remove('active');
        if (settingsTab) settingsTab.classList.add('active');

        this.isActive = true;

        // Load data
        await this.loadSettings();
        await this.loadArchiveToggle();
        await this.loadEndpointAnalysisToggle();
        await this.loadSharedLinksToggle();
        await this.loadMTLSStatus();
        await this.loadUsers();

        // Load groups if available
        const groupsLoad = window.GroupsView ? GroupsView.loadGroups() : null;

        // subPath is "<subTab>" or "<subTab>/<groupId>" (groups detail deep-link).
        if (subPath) {
            const slash = subPath.indexOf('/');
            const subTab = slash === -1 ? subPath : subPath.slice(0, slash);
            const detailId = slash === -1 ? '' : subPath.slice(slash + 1);
            this.switchSubTab(subTab, true);
            if (subTab === 'context' && detailId && window.ContextLinks) {
                ContextLinks.show(detailId);
            }
            if (subTab === 'groups' && detailId && window.GroupsView) {
                if (groupsLoad) await groupsLoad;
                GroupsView.openDetail(detailId, true);
            }
        }
    },

    hide() {
        const settingsView = document.getElementById('settingsView');
        if (settingsView) {
            settingsView.style.display = 'none';
        }

        this.isActive = false;
    },

    async loadSettings() {
        try {
            const response = await fetch('/api/v1/settings', { credentials: 'include' });
            const data = await response.json();

            if (data.success) {
                // Load system limits
                const alertTimeoutSelect = document.getElementById('alertTimeoutSettings');
                if (alertTimeoutSelect) {
                    alertTimeoutSelect.value = String(data.settings.alert_timeout_seconds || 5);
                }
                const queryTimeoutSelect = document.getElementById('queryTimeoutSettings');
                if (queryTimeoutSelect) {
                    queryTimeoutSelect.value = String(data.settings.query_timeout_seconds ?? 60);
                }
                const alertEvalIntervalSelect = document.getElementById('alertEvalIntervalSettings');
                if (alertEvalIntervalSelect) {
                    alertEvalIntervalSelect.value = String(data.settings.alert_eval_interval_seconds || 60);
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
                    alert_eval_interval_seconds: parseInt(document.getElementById('alertEvalIntervalSettings')?.value || '60', 10)
                })
            });

            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to save settings');
            this.flashSaved(triggerEl);
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
            const response = await fetch('/api/v1/users', { credentials: 'include' });
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
            const lastLogin = user.last_login ? new Date(user.last_login).toLocaleString() : 'Never';
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
            html += `<td><span class="role-badge role-${user.role}">${user.role === 'admin' ? 'Tenant Admin' : 'User'}</span></td>`;

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

    async clearLogs() {
        if (!confirm('Are you sure you want to delete ALL logs and comments? This cannot be undone!')) {
            return;
        }

        if (!confirm('This will PERMANENTLY delete all logs and their associated comments. Are you absolutely sure?')) {
            return;
        }

        try {
            const response = await fetch('/api/v1/logs', {
                method: 'DELETE',
                credentials: 'include'
            });

            const data = await response.json();

            if (data.success) {
                // logs cleared; nothing further to refresh here
            } else {
                const errorMsg = data.error || 'Unknown error';
                if (window.Toast) {
                    Toast.error('Cleanup Failed', errorMsg);
                } else {
                    alert('Failed to clear logs: ' + errorMsg);
                }
            }
        } catch (error) {
            console.error('Error clearing logs:', error);
            if (window.Toast) {
                Toast.error('Network Error', 'Please try again.');
            } else {
                alert('Network error. Please try again.');
            }
        }
    },

};

// Make globally available
window.SettingsView = SettingsView;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    SettingsView.init();
});
