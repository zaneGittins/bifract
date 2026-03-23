// Fractal Manage Tab - Handles per-fractal management within the fractal view
const FractalManageTab = {
    currentFractal: null,
    currentPrismData: null,

    init() {
        console.log('[FractalManageTab] Initialized');
        this.setupEventListeners();
    },

    setupEventListeners() {
        // Retention select
        const retentionSelect = document.getElementById('manageFractalRetentionSelect');
        if (retentionSelect) {
            retentionSelect.addEventListener('change', () => {
                this.saveRetentionSetting();
                this.updateLifecycleSummary();
            });
        }

        // Archive schedule selects
        const archiveScheduleSelect = document.getElementById('manageFractalArchiveSchedule');
        if (archiveScheduleSelect) {
            archiveScheduleSelect.addEventListener('change', () => {
                this.saveArchiveSchedule();
                this.updateLifecycleSummary();
            });
        }
        const maxArchivesSelect = document.getElementById('manageFractalMaxArchives');
        if (maxArchivesSelect) {
            maxArchivesSelect.addEventListener('change', () => {
                this.saveArchiveSchedule();
                this.updateLifecycleSummary();
            });
        }

        // Quota action select - show/hide rollover warning
        const quotaActionSelect = document.getElementById('manageFractalQuotaAction');
        if (quotaActionSelect) {
            quotaActionSelect.addEventListener('change', () => this.updateRolloverWarning());
        }

        // Action buttons
        const editFractalBtn = document.getElementById('manageEditFractalBtn');
        if (editFractalBtn) {
            editFractalBtn.addEventListener('click', () => this.editFractal());
        }

        const deleteFractalBtn = document.getElementById('manageDeleteFractalBtn');
        if (deleteFractalBtn) {
            deleteFractalBtn.addEventListener('click', () => this.confirmDeleteFractal());
        }

        const clearLogsBtn = document.getElementById('manageClearFractalLogsBtn');
        if (clearLogsBtn) {
            clearLogsBtn.addEventListener('click', () => this.confirmClearFractalLogs());
        }

        const manageAPIKeysBtn = document.getElementById('manageAPIKeysBtn');
        if (manageAPIKeysBtn) {
            manageAPIKeysBtn.addEventListener('click', () => this.showAPIKeysModal());
        }
    },

    show() {
        console.log('[FractalManageTab] Showing manage tab');

        if (window.FractalContext && window.FractalContext.currentFractal) {
            this.currentFractal = window.FractalContext.currentFractal;

            const prismSection = document.getElementById('prismManageSection');
            const fractalSection = document.getElementById('fractalOnlyManageSection');

            if (window.FractalContext.isPrism()) {
                if (prismSection) prismSection.style.display = '';
                if (fractalSection) fractalSection.style.display = 'none';
                this.loadPrismDetails();
            } else {
                if (prismSection) prismSection.style.display = 'none';
                if (fractalSection) fractalSection.style.display = '';
                this.loadFractalDetails();
            }
        } else {
            console.error('[FractalManageTab] No current fractal context available');
            this.showError('No fractal selected');
        }
    },

    async loadPrismDetails() {
        if (!this.currentFractal) return;

        const prism = this.currentFractal;

        // Reset to overview subtab
        this.switchPrismSubTab('overview');

        const title = document.getElementById('managePrismTitle');
        if (title) title.textContent = `Manage Prism: ${prism.name}`;

        const nameEl = document.getElementById('managePrismName');
        if (nameEl) nameEl.textContent = prism.name;
        const descEl = document.getElementById('managePrismDescription');
        if (descEl) descEl.textContent = prism.description || 'None';
        const createdByEl = document.getElementById('managePrismCreatedBy');
        if (createdByEl) createdByEl.textContent = prism.created_by || '';
        const createdAtEl = document.getElementById('managePrismCreatedAt');
        if (createdAtEl) createdAtEl.textContent = prism.created_at ? new Date(prism.created_at).toLocaleString() : '';
        const updatedAtEl = document.getElementById('managePrismUpdatedAt');
        if (updatedAtEl) updatedAtEl.textContent = prism.updated_at ? new Date(prism.updated_at).toLocaleString() : '';

        // Load full prism details (including members) from API
        try {
            const resp = await fetch(`/api/v1/prisms/${prism.id}`, { credentials: 'include' });
            if (resp.ok) {
                const data = await resp.json();
                if (data.success && data.data) {
                    this.currentPrismData = data.data;
                    const members = data.data.members || [];
                    this.renderPrismMembers(members);
                    const countEl = document.getElementById('managePrismMemberCount');
                    if (countEl) countEl.textContent = members.length;
                }
            }
        } catch (err) {
            console.error('[FractalManageTab] Failed to load prism details:', err);
        }

        this.loadAvailableFractalsForPrism();
        this.currentPrismId = prism.id;
        if (window.GroupsView) GroupsView.loadPrismPermissions(prism.id);
    },

    async loadAvailableFractalsForPrism() {
        const select = document.getElementById('prismAddMemberSelect');
        if (!select) return;

        try {
            const resp = await fetch('/api/v1/fractals', { credentials: 'include' });
            if (!resp.ok) return;
            const data = await resp.json();
            const fractals = (data.success && data.data && data.data.fractals) ? data.data.fractals : [];

            const currentMembers = (this.currentPrismData && this.currentPrismData.members)
                ? this.currentPrismData.members.map(m => m.fractal_id)
                : [];

            select.innerHTML = '<option value="">Select a fractal to add...</option>';
            fractals.forEach(f => {
                if (!currentMembers.includes(f.id)) {
                    const opt = document.createElement('option');
                    opt.value = f.id;
                    opt.textContent = f.name;
                    select.appendChild(opt);
                }
            });
        } catch (err) {
            console.error('[FractalManageTab] Failed to load fractals for prism:', err);
        }
    },

    renderPrismMembers(members) {
        const container = document.getElementById('prismMembersList');
        if (!container) return;

        if (!members || members.length === 0) {
            container.innerHTML = '<div style="color:var(--text-muted);font-size:0.875rem;padding:0.5rem 0;">No member fractals. Add fractals below.</div>';
            return;
        }

        let html = '<table class="prism-members-table"><thead><tr><th>Fractal</th><th></th></tr></thead><tbody>';
        members.forEach(m => {
            const name = m.fractal_name || m.fractal_id;
            html += `<tr>
                <td>${Utils.escapeHtml(name)}</td>
                <td class="prism-members-actions"><button class="btn-icon btn-delete" onclick="FractalManageTab.removePrismMember('${m.fractal_id}')" title="Remove">&#x2715;</button></td>
            </tr>`;
        });
        html += '</tbody></table>';
        container.innerHTML = html;
    },

    async addPrismMember() {
        if (!this.currentFractal) return;
        const select = document.getElementById('prismAddMemberSelect');
        if (!select || !select.value) return;

        const fractalId = select.value;
        try {
            const resp = await fetch(`/api/v1/prisms/${this.currentFractal.id}/members`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ fractal_id: fractalId })
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error || 'Failed to add member');

            if (window.Toast) Toast.success('Member Added', 'Fractal added to prism');
            await this.loadPrismDetails();
        } catch (err) {
            console.error('[FractalManageTab] addPrismMember error:', err);
            if (window.Toast) Toast.error('Add Failed', err.message);
            this.showPrismError(err.message);
        }
    },

    async removePrismMember(fractalId) {
        if (!this.currentFractal) return;
        try {
            const resp = await fetch(`/api/v1/prisms/${this.currentFractal.id}/members/${fractalId}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error || 'Failed to remove member');

            if (window.Toast) Toast.success('Member Removed', 'Fractal removed from prism');
            await this.loadPrismDetails();
        } catch (err) {
            console.error('[FractalManageTab] removePrismMember error:', err);
            if (window.Toast) Toast.error('Remove Failed', err.message);
            this.showPrismError(err.message);
        }
    },

    async editPrism() {
        if (!this.currentFractal) return;

        const newName = prompt('Prism name:', this.currentFractal.name);
        if (newName === null) return;
        const newDesc = prompt('Description:', this.currentFractal.description || '');
        if (newDesc === null) return;

        try {
            const resp = await fetch(`/api/v1/prisms/${this.currentFractal.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ name: newName.trim(), description: newDesc.trim() })
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error || 'Failed to update prism');

            this.currentFractal.name = newName.trim();
            this.currentFractal.description = newDesc.trim();
            if (window.FractalContext) FractalContext.currentFractal = this.currentFractal;

            if (window.Toast) Toast.success('Prism Updated', 'Prism details saved');
            this.loadPrismDetails();
        } catch (err) {
            console.error('[FractalManageTab] editPrism error:', err);
            if (window.Toast) Toast.error('Update Failed', err.message);
            this.showPrismError(err.message);
        }
    },

    async deletePrism() {
        if (!this.currentFractal) return;

        const confirmation = prompt(`Type "${this.currentFractal.name}" to confirm deletion of this prism:`);
        if (confirmation !== this.currentFractal.name) {
            if (confirmation !== null && window.Toast) Toast.error('Delete Cancelled', 'Name did not match');
            return;
        }

        try {
            const resp = await fetch(`/api/v1/prisms/${this.currentFractal.id}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error || 'Failed to delete prism');

            if (window.Toast) Toast.success('Prism Deleted', `Prism "${this.currentFractal.name}" deleted`);
            if (window.App) App.showMainView('fractalListing');
        } catch (err) {
            console.error('[FractalManageTab] deletePrism error:', err);
            if (window.Toast) Toast.error('Delete Failed', err.message);
            this.showPrismError(err.message);
        }
    },

    showPrismError(message) {
        const errorDiv = document.getElementById('managePrismError');
        if (errorDiv) {
            errorDiv.textContent = message;
            errorDiv.style.display = 'block';
        }
    },

    loadFractalDetails() {
        if (!this.currentFractal) return;

        const fractal = this.currentFractal;

        // Reset to overview subtab
        this.switchSubTab('overview');

        // Update title
        const title = document.getElementById('manageFractalTitle');
        if (title) title.textContent = `Manage Fractal: ${fractal.name}`;

        // Populate basic information
        document.getElementById('manageFractalName').textContent = fractal.name;
        document.getElementById('manageFractalDescription').textContent = fractal.description || 'None';
        document.getElementById('manageFractalCreatedBy').textContent = fractal.created_by;
        document.getElementById('manageFractalCreatedAt').textContent = new Date(fractal.created_at).toLocaleString();
        document.getElementById('manageFractalUpdatedAt').textContent = new Date(fractal.updated_at).toLocaleString();

        // Populate statistics
        document.getElementById('manageFractalLogCount').textContent = (fractal.log_count || 0).toLocaleString();
        document.getElementById('manageFractalSizeBytes').textContent = this.formatBytes(fractal.size_bytes || 0);
        document.getElementById('manageFractalEarliestLog').textContent =
            fractal.earliest_log ? new Date(fractal.earliest_log).toLocaleString() : 'None';
        document.getElementById('manageFractalLatestLog').textContent =
            fractal.latest_log ? new Date(fractal.latest_log).toLocaleString() : 'None';

        // Hide delete action for default/system fractals
        const isProtected = fractal.is_default || fractal.is_system;
        const deleteDangerItem = document.getElementById('deleteFractalDangerItem');
        if (deleteDangerItem) {
            deleteDangerItem.style.display = isProtected ? 'none' : '';
        }

        // Hide edit button for default/system fractals
        const editBtnInline = document.getElementById('manageEditFractalBtnInline');
        if (editBtnInline) {
            editBtnInline.style.display = isProtected ? 'none' : '';
        }

        // Populate retention select
        const retentionSelect = document.getElementById('manageFractalRetentionSelect');
        if (retentionSelect) {
            retentionSelect.value = fractal.retention_days != null ? String(fractal.retention_days) : '';
        }

        // Populate archive schedule selects
        const archiveScheduleSelect = document.getElementById('manageFractalArchiveSchedule');
        if (archiveScheduleSelect) {
            archiveScheduleSelect.value = fractal.archive_schedule || 'never';
        }
        const maxArchivesSelect = document.getElementById('manageFractalMaxArchives');
        if (maxArchivesSelect) {
            maxArchivesSelect.value = fractal.max_archives != null ? String(fractal.max_archives) : '';
        }

        // Populate disk quota fields
        const quotaInput = document.getElementById('manageFractalQuotaInput');
        if (quotaInput) {
            quotaInput.value = fractal.disk_quota_bytes != null
                ? (fractal.disk_quota_bytes / (1024 ** 3)).toFixed(0)
                : '';
        }
        const quotaActionSelect = document.getElementById('manageFractalQuotaAction');
        if (quotaActionSelect) {
            quotaActionSelect.value = fractal.disk_quota_action || 'reject';
        }
        this.updateRolloverWarning();
        this.updateLifecycleSummary();

        // Load permissions
        if (window.GroupsView) {
            GroupsView.loadPermissions(fractal.id);
        }

        // Load archives
        if (window.Archives) {
            Archives.loadArchives(fractal.id);
        }
    },

    confirmDeleteFractal() {
        if (!this.currentFractal) return;

        const fractal = this.currentFractal;
        if (fractal.is_default || fractal.is_system) {
            if (window.Toast) {
                Toast.error('Cannot Delete', 'System fractals cannot be deleted');
            }
            return;
        }

        const logCountText = fractal.log_count ? `${fractal.log_count.toLocaleString()} logs` : 'no logs';
        const sizeText = fractal.size_bytes ? this.formatBytes(fractal.size_bytes) : '0 B';

        const message = `Are you sure you want to delete the fractal "${fractal.name}"?\n\n` +
            `This fractal currently contains ${logCountText} (${sizeText}) and this action cannot be undone.\n\n` +
            `Type "${fractal.name}" below to confirm:`;

        const confirmation = prompt(message);
        if (confirmation === fractal.name) {
            this.executeDeleteFractal();
        } else if (confirmation !== null) {
            if (window.Toast) {
                Toast.error('Delete Cancelled', 'Fractal name did not match');
            }
        }
    },

    async executeDeleteFractal() {
        if (!this.currentFractal) return;

        const deleteBtn = document.getElementById('manageDeleteFractalBtn');

        try {
            if (deleteBtn) {
                deleteBtn.disabled = true;
                deleteBtn.innerHTML = '<span class="spinner"></span> Deleting...';
            }
            this.hideError();

            const response = await fetch(`/api/v1/fractals/${this.currentFractal.id}`, {
                method: 'DELETE',
                credentials: 'include'
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.error || `Failed to delete fractal: ${response.status}`);
            }

            console.log('[FractalManageTab] Fractal deleted successfully:', this.currentFractal.id);

            if (window.Toast) {
                Toast.success('Fractal Deleted', `Fractal "${this.currentFractal.name}" has been deleted`);
            }

            // Navigate back to main fractal listing
            if (window.App) {
                App.showMainView('fractalListing');
            }

        } catch (error) {
            console.error('Failed to delete fractal:', error);
            this.showError(error.message);
            if (window.Toast) {
                Toast.error('Delete Failed', error.message);
            }
        } finally {
            if (deleteBtn) {
                deleteBtn.disabled = false;
                deleteBtn.innerHTML = 'Delete';
            }
        }
    },

    async saveRetentionSetting() {
        if (!this.currentFractal) return;

        const select = document.getElementById('manageFractalRetentionSelect');
        if (!select) return;

        const value = select.value;
        const body = { retention_days: value === '' ? null : parseInt(value, 10) };

        try {
            const response = await fetch(`/api/v1/fractals/${this.currentFractal.id}/retention`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(body)
            });

            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to save retention');

            this.currentFractal.retention_days = body.retention_days;
            if (window.FractalContext) FractalContext.currentFractal = this.currentFractal;

            if (window.Toast) {
                Toast.success(
                    'Retention Updated',
                    body.retention_days == null
                        ? 'Retention set to unlimited'
                        : `Retention set to ${body.retention_days} days`
                );
            }
        } catch (error) {
            console.error('Failed to save retention:', error);
            if (window.Toast) {
                Toast.error('Retention Save Failed', error.message);
            }
        }
    },

    async saveArchiveSchedule() {
        if (!this.currentFractal) return;

        const scheduleSelect = document.getElementById('manageFractalArchiveSchedule');
        const maxSelect = document.getElementById('manageFractalMaxArchives');
        if (!scheduleSelect) return;

        const body = {
            archive_schedule: scheduleSelect.value,
            max_archives: maxSelect && maxSelect.value !== '' ? parseInt(maxSelect.value, 10) : null
        };

        try {
            const response = await fetch(`/api/v1/fractals/${this.currentFractal.id}/archive-schedule`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(body)
            });

            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to save archive schedule');

            this.currentFractal.archive_schedule = body.archive_schedule;
            this.currentFractal.max_archives = body.max_archives;
            if (window.FractalContext) FractalContext.currentFractal = this.currentFractal;

            if (window.Toast) {
                Toast.success(
                    'Archive Schedule Updated',
                    body.archive_schedule === 'never'
                        ? 'Automatic archiving disabled'
                        : `Archives will be created ${body.archive_schedule}`
                );
            }
        } catch (error) {
            console.error('Failed to save archive schedule:', error);
            if (window.Toast) {
                Toast.error('Archive Schedule Save Failed', error.message);
            }
        }
    },

    confirmClearFractalLogs() {
        if (!this.currentFractal) return;

        const fractal = this.currentFractal;
        const logCountText = fractal.log_count ? `${fractal.log_count.toLocaleString()} logs` : 'no logs';

        const confirmation = confirm(
            `Are you sure you want to clear all logs for fractal "${fractal.name}"?\n\n` +
            `This will permanently delete ${logCountText} from this fractal. The fractal structure will remain.\n\n` +
            `This action cannot be undone.`
        );

        if (confirmation) {
            this.executeClearFractalLogs();
        }
    },

    async executeClearFractalLogs() {
        if (!this.currentFractal) return;

        const clearBtn = document.getElementById('manageClearFractalLogsBtn');

        try {
            if (clearBtn) {
                clearBtn.disabled = true;
                clearBtn.innerHTML = '<span class="spinner"></span> Clearing logs...';
            }
            this.hideError();

            // Clear logs for the specific fractal using the existing logs API with fractal_id parameter
            const response = await fetch(`/api/v1/logs?fractal_id=${encodeURIComponent(this.currentFractal.id)}`, {
                method: 'DELETE',
                credentials: 'include',
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });

            const data = await response.json();

            if (data.success) {
                if (window.Toast) {
                    Toast.success('Success', data.message || `All logs for fractal "${this.currentFractal.name}" have been cleared`);
                }

            } else {
                const errorMsg = data.error || 'Unknown error';
                if (window.Toast) {
                    Toast.error('Clear Failed', errorMsg);
                } else {
                    alert('Failed to clear fractal logs: ' + errorMsg);
                }
                this.showError(errorMsg);
            }

        } catch (error) {
            console.error('Failed to clear fractal logs:', error);
            this.showError(error.message);
            if (window.Toast) {
                Toast.error('Clear Failed', error.message);
            }
        } finally {
            if (clearBtn) {
                clearBtn.disabled = false;
                clearBtn.innerHTML = 'Clear Fractal Logs';
            }
        }
    },

    showError(message) {
        const errorDiv = document.getElementById('manageFractalError');
        if (errorDiv) {
            errorDiv.textContent = message;
            errorDiv.style.display = 'block';
        }
    },

    hideError() {
        const errorDiv = document.getElementById('manageFractalError');
        if (errorDiv) {
            errorDiv.style.display = 'none';
        }
    },

    formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    },

    editFractal() {
        if (!this.currentFractal) return;

        if (this.currentFractal.is_default || this.currentFractal.is_system) {
            if (window.Toast) {
                Toast.error('Cannot Edit', 'System fractals cannot be renamed');
            }
            return;
        }

        // Use FractalManagement's edit modal
        if (window.FractalManagement) {
            FractalManagement.currentEditFractal = this.currentFractal;

            // Populate form
            const nameInput = document.getElementById('editFractalName');
            const descInput = document.getElementById('editFractalDescription');

            if (nameInput) nameInput.value = this.currentFractal.name;
            if (descInput) descInput.value = this.currentFractal.description || '';

            FractalManagement.showEditFractalModal();
        } else {
            console.error('[FractalManageTab] FractalManagement not available for edit modal');
        }
    },

    updateLifecycleSummary() {
        const el = document.getElementById('lifecycleSummary');
        if (!el) return;

        const retentionSelect = document.getElementById('manageFractalRetentionSelect');
        const archiveSelect = document.getElementById('manageFractalArchiveSchedule');

        const retentionDays = retentionSelect ? retentionSelect.value : '';
        const schedule = archiveSelect ? archiveSelect.value : 'never';

        const parts = [];

        if (retentionDays === '') {
            parts.push('Logs are kept indefinitely.');
        } else {
            parts.push(`Logs older than ${retentionDays} days are deleted hourly.`);
        }

        if (schedule !== 'never') {
            parts.push(`Encrypted backups are created ${schedule}.`);
            if (retentionDays !== '') {
                parts.push('A 1-day buffer ensures backups complete before deletion.');
            }
        } else if (retentionDays !== '') {
            parts.push('No backups are configured, so deleted logs are gone permanently.');
        }

        el.textContent = parts.join(' ');
    },

    updateRolloverWarning() {
        const actionSelect = document.getElementById('manageFractalQuotaAction');
        const warning = document.getElementById('manageQuotaRolloverWarning');
        if (actionSelect && warning) {
            warning.style.display = actionSelect.value === 'rollover' ? '' : 'none';
        }
    },

    async saveDiskQuota() {
        if (!this.currentFractal) return;

        const quotaInput = document.getElementById('manageFractalQuotaInput');
        const quotaActionSelect = document.getElementById('manageFractalQuotaAction');
        if (!quotaInput || !quotaActionSelect) return;

        const gbValue = quotaInput.value.trim();
        const action = quotaActionSelect.value;
        const quotaBytes = gbValue === '' ? null : Math.round(parseFloat(gbValue) * (1024 ** 3));

        if (quotaBytes !== null && (isNaN(quotaBytes) || quotaBytes < 1)) {
            if (window.Toast) Toast.error('Invalid Quota', 'Enter a valid size or leave empty for no limit');
            return;
        }

        try {
            const response = await fetch(`/api/v1/fractals/${this.currentFractal.id}/disk-quota`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ quota_bytes: quotaBytes, action })
            });

            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to save quota');

            this.currentFractal.disk_quota_bytes = quotaBytes;
            this.currentFractal.disk_quota_action = action;
            if (window.FractalContext) FractalContext.currentFractal = this.currentFractal;

            if (window.Toast) {
                Toast.success(
                    'Quota Updated',
                    quotaBytes == null
                        ? 'Disk quota removed'
                        : `Disk quota set to ${gbValue} GB (${action})`
                );
            }
        } catch (error) {
            console.error('Failed to save disk quota:', error);
            if (window.Toast) Toast.error('Quota Save Failed', error.message);
        }
    },

    switchSubTab(tabName) {
        // Update tab buttons (scoped to the manage subtabs container)
        const tabBar = document.getElementById('manageSubTabs');
        if (tabBar) {
            tabBar.querySelectorAll('.alerts-sub-tab').forEach(btn => btn.classList.remove('active'));
            const activeBtn = tabBar.querySelector(`.alerts-sub-tab[data-subtab="${tabName}"]`);
            if (activeBtn) activeBtn.classList.add('active');
        }

        // Show/hide panels
        document.querySelectorAll('.manage-sub-panel').forEach(panel => panel.style.display = 'none');
        const panel = document.getElementById('manageSubTab' + tabName.charAt(0).toUpperCase() + tabName.slice(1));
        if (panel) panel.style.display = '';

        // Reload archives when switching to lifecycle tab
        if (tabName === 'lifecycle' && window.Archives && this.currentFractal) {
            Archives.loadArchives(this.currentFractal.id);
        }
    },

    switchPrismSubTab(tabName) {
        // Update tab buttons (scoped to the prism subtabs container)
        const tabBar = document.getElementById('prismManageSubTabs');
        if (tabBar) {
            tabBar.querySelectorAll('.alerts-sub-tab').forEach(btn => btn.classList.remove('active'));
            const activeBtn = tabBar.querySelector(`.alerts-sub-tab[data-subtab="${tabName}"]`);
            if (activeBtn) activeBtn.classList.add('active');
        }

        // Show/hide prism panels
        document.querySelectorAll('.prism-sub-panel').forEach(panel => panel.style.display = 'none');
        const panel = document.getElementById('prismSubTab' + tabName.charAt(0).toUpperCase() + tabName.slice(1));
        if (panel) panel.style.display = '';
    },

    showAPIKeysModal() {
        if (!this.currentFractal) {
            if (window.Toast) {
                Toast.error('Error', 'No fractal selected');
            }
            return;
        }

        // Check if APIKeys component is available
        if (window.APIKeys) {
            window.APIKeys.showAPIKeysModal();
        } else {
            if (window.Toast) {
                Toast.error('Error', 'API Keys component not loaded');
            }
            console.error('APIKeys component not available');
        }
    },

    showPrismAPIKeys() {
        if (!this.currentFractal) {
            if (window.Toast) {
                Toast.error('Error', 'No prism selected');
            }
            return;
        }

        if (window.APIKeys) {
            window.APIKeys.showPrismAPIKeysModal(this.currentFractal);
        } else {
            if (window.Toast) {
                Toast.error('Error', 'API Keys component not loaded');
            }
            console.error('APIKeys component not available');
        }
    },

};

// Make globally available
window.FractalManageTab = FractalManageTab;