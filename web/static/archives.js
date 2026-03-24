const Archives = {
    currentFractalId: null,
    pollInterval: null,
    pendingRestoreArchiveId: null,
    pendingRestoreGroupId: null,
    pendingRestoreIsResume: false,
    expandedGroups: new Set(),
    currentPage: 0,
    pageSize: 20,
    allItems: [],

    init() {
        // Button uses onclick in HTML, no addEventListener needed
    },

    async loadArchives(fractalId) {
        this.currentFractalId = fractalId;
        this.stopPolling();

        const container = document.getElementById('archivesList');
        if (!container) return;

        try {
            const response = await fetch(`/api/v1/fractals/${fractalId}/archives`, {
                credentials: 'include'
            });
            const data = await response.json();

            if (!data.success) {
                container.innerHTML = `<p class="text-muted">Unable to load archives.</p>`;
                return;
            }

            this.allItems = data.data.items || [];
            this.renderItems(container);

            const hasActive = this.allItems.some(item => {
                if (item.type === 'group') {
                    return item.group.status === 'in_progress' || item.group.status === 'restoring';
                }
                return item.archive && (item.archive.status === 'in_progress' || item.archive.status === 'restoring');
            });
            if (hasActive) {
                this.startPolling();
            }
        } catch (err) {
            container.innerHTML = `<p class="text-muted">Unable to load archives.</p>`;
        }
    },

    renderItems(container) {
        if (!this.allItems || this.allItems.length === 0) {
            container.innerHTML = `<p class="text-muted" style="margin-top: 0.75rem;">No archives yet.</p>`;
            return;
        }

        const totalPages = Math.ceil(this.allItems.length / this.pageSize);
        const start = this.currentPage * this.pageSize;
        const pageItems = this.allItems.slice(start, start + this.pageSize);

        let rows = '';
        for (const item of pageItems) {
            if (item.type === 'group') {
                rows += this.renderGroup(item.group);
            } else if (item.archive) {
                rows += this.renderStandaloneArchive(item.archive);
            }
        }

        let html = `
            <table class="archives-table">
                <thead>
                    <tr>
                        <th style="width:2rem;"></th>
                        <th>Status</th>
                        <th>Type</th>
                        <th>Period</th>
                        <th>Logs</th>
                        <th>Size</th>
                        <th>Created</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>${rows}</tbody>
            </table>`;

        if (totalPages > 1) {
            html += this.renderPagination(totalPages);
        }

        container.innerHTML = html;
    },

    renderGroup(group) {
        const isExpanded = this.expandedGroups.has(group.id) ||
            group.status === 'in_progress' || group.status === 'restoring';
        const chevron = isExpanded ? '&#9660;' : '&#9654;';
        const statusBadge = this.renderGroupStatus(group);
        const splitLabel = this.splitLabel(group.split_granularity);
        const archiveCount = group.archive_count || 0;
        const completedCount = group.completed_count || 0;
        const totalLogs = group.total_log_count || 0;
        const totalSize = group.total_size_bytes || 0;
        const date = new Date(group.created_at).toLocaleDateString('en-US', {
            year: 'numeric', month: 'short', day: 'numeric',
            hour: '2-digit', minute: '2-digit'
        });
        const typeBadge = group.archive_type === 'scheduled'
            ? '<span class="archive-badge archive-badge-info">Scheduled</span>'
            : '<span class="archive-badge">Manual</span>';

        let info = splitLabel;
        if (archiveCount > 0) {
            if (group.status === 'in_progress') {
                info += ` - ${completedCount} of ${archiveCount}`;
            } else {
                info += ` - ${archiveCount} archive${archiveCount !== 1 ? 's' : ''}`;
            }
        }

        const actions = this.renderGroupActions(group);

        let html = `<tr class="archive-group-header" onclick="Archives.toggleGroup('${group.id}')">
            <td class="archive-group-toggle">${chevron}</td>
            <td>${statusBadge}</td>
            <td>${typeBadge}</td>
            <td>${Utils.escapeHtml(info)}</td>
            <td>${totalLogs > 0 ? totalLogs.toLocaleString() : '-'}</td>
            <td>${totalSize > 0 ? this.formatBytes(totalSize) : '-'}</td>
            <td>${date}</td>
            <td onclick="event.stopPropagation()">${actions}</td>
        </tr>`;

        if (isExpanded && group.archives && group.archives.length > 0) {
            for (const archive of group.archives) {
                html += this.renderGroupChild(archive);
            }
        }

        return html;
    },

    renderGroupChild(archive) {
        const statusBadge = this.renderStatus(archive);
        const size = this.formatBytes(archive.size_bytes);
        const label = archive.period_label || Utils.escapeHtml(archive.filename);
        const actions = this.renderActions(archive);

        return `<tr class="archive-group-child">
            <td></td>
            <td>${statusBadge}</td>
            <td></td>
            <td class="archive-period-label">${Utils.escapeHtml(label)}</td>
            <td>${archive.log_count > 0 ? archive.log_count.toLocaleString() : '-'}</td>
            <td>${archive.size_bytes > 0 ? size : '-'}</td>
            <td></td>
            <td>${actions}</td>
        </tr>`;
    },

    renderStandaloneArchive(a) {
        const statusBadge = this.renderStatus(a);
        const size = this.formatBytes(a.size_bytes);
        const date = new Date(a.created_at).toLocaleDateString('en-US', {
            year: 'numeric', month: 'short', day: 'numeric',
            hour: '2-digit', minute: '2-digit'
        });
        const timeRange = this.formatTimeRange(a);
        const actions = this.renderActions(a);
        const typeBadge = a.archive_type === 'scheduled'
            ? '<span class="archive-badge archive-badge-info">Scheduled</span>'
            : '<span class="archive-badge">Manual</span>';

        return `<tr>
            <td></td>
            <td>${statusBadge}</td>
            <td>${typeBadge}</td>
            <td title="${Utils.escapeHtml(timeRange)}">${Utils.escapeHtml(a.filename)}</td>
            <td>${a.log_count.toLocaleString()}</td>
            <td>${size}</td>
            <td>${date}</td>
            <td>${actions}</td>
        </tr>`;
    },

    renderGroupStatus(group) {
        const { status, error_message: errorMessage, completed_count, archive_count } = group;
        switch (status) {
            case 'completed':
                return `<span class="archive-badge archive-badge-success">&#x2713; Completed</span>`;
            case 'in_progress': {
                const progress = archive_count > 0 ? ` ${completed_count}/${archive_count}` : '';
                return `<span class="archive-badge archive-badge-active"><span class="spinner-sm"></span> Archiving${progress}</span>`;
            }
            case 'restoring':
                return `<span class="archive-badge archive-badge-active"><span class="spinner-sm"></span> Restoring</span>`;
            case 'partial':
                return `<span class="archive-badge archive-badge-warning" title="${Utils.escapeHtml(errorMessage || '')}">Partial</span>`;
            case 'failed':
                return `<span class="archive-badge archive-badge-error" title="${Utils.escapeHtml(errorMessage || '')}">Failed</span>`;
            default:
                return `<span class="archive-badge">${Utils.escapeHtml(status)}</span>`;
        }
    },

    renderStatus(archive) {
        const { status, error_message: errorMessage, log_count: logCount, checksum,
                restore_lines_sent: restoreLinesSent, restore_error: restoreError } = archive;
        switch (status) {
            case 'completed': {
                const verified = checksum ? ' title="SHA-256 verified"' : ' title="No checksum (created before integrity verification was added)"';
                const icon = checksum ? '&#x2713; ' : '';
                let badge = `<span class="archive-badge archive-badge-success"${verified}>${icon}Completed</span>`;
                if (restoreError) {
                    const resumeInfo = restoreLinesSent > 0
                        ? ` (${this.formatCount(restoreLinesSent)} / ${this.formatCount(logCount)} logs restored)`
                        : '';
                    badge += `<br><span class="archive-badge archive-badge-error" title="${Utils.escapeHtml(restoreError)}" style="margin-top:0.25rem">Restore failed${resumeInfo}</span>`;
                }
                return badge;
            }
            case 'in_progress': {
                const progress = logCount > 0 ? ` ${this.formatCount(logCount)} logs` : '';
                return `<span class="archive-badge archive-badge-active"><span class="spinner-sm"></span> Archiving${progress}</span>`;
            }
            case 'restoring': {
                let progress = '';
                if (restoreLinesSent > 0 && logCount > 0) {
                    const pct = Math.min(99, Math.round((restoreLinesSent / logCount) * 100));
                    progress = ` ${pct}% (${this.formatCount(restoreLinesSent)} / ${this.formatCount(logCount)})`;
                } else if (restoreLinesSent > 0) {
                    progress = ` ${this.formatCount(restoreLinesSent)} logs`;
                }
                return `<span class="archive-badge archive-badge-active"><span class="spinner-sm"></span> Restoring${progress}</span>`;
            }
            case 'failed':
                return `<span class="archive-badge archive-badge-error" title="${Utils.escapeHtml(errorMessage || '')}">Failed</span>`;
            default:
                return `<span class="archive-badge">${Utils.escapeHtml(status)}</span>`;
        }
    },

    renderGroupActions(group) {
        if (group.status === 'in_progress' || group.status === 'restoring') {
            return `<button class="btn-danger-sm btn-xs" onclick="Archives.confirmCancelGroup('${group.id}')">Cancel</button>`;
        }

        let actions = '';
        if (group.status === 'completed' || group.status === 'partial') {
            actions += `<button class="btn-secondary btn-xs" onclick="Archives.confirmRestoreGroup('${group.id}')">Restore All</button> `;
        }
        actions += `<button class="btn-danger-sm btn-xs" onclick="Archives.confirmDeleteGroup('${group.id}')">Delete</button>`;
        return actions;
    },

    renderActions(archive) {
        if (archive.status === 'in_progress' || archive.status === 'restoring') {
            return `<button class="btn-danger-sm btn-xs" onclick="Archives.confirmCancel('${archive.id}')">Cancel</button>`;
        }

        let actions = '';
        if (archive.status === 'completed') {
            const hasPartialRestore = archive.restore_lines_sent > 0 && !!archive.restore_error;
            const label = hasPartialRestore ? 'Resume' : 'Restore';
            actions += `<button class="btn-secondary btn-xs" onclick="Archives.confirmRestore('${archive.id}', ${hasPartialRestore})">${label}</button> `;
        }
        actions += `<button class="btn-danger-sm btn-xs" onclick="Archives.confirmDelete('${archive.id}')">Delete</button>`;
        return actions;
    },

    renderPagination(totalPages) {
        const prevDisabled = this.currentPage === 0 ? ' disabled' : '';
        const nextDisabled = this.currentPage >= totalPages - 1 ? ' disabled' : '';
        return `<div class="archive-pagination">
            <button class="btn-secondary btn-xs"${prevDisabled} onclick="Archives.prevPage()">Prev</button>
            <span class="archive-page-info">${this.currentPage + 1} / ${totalPages}</span>
            <button class="btn-secondary btn-xs"${nextDisabled} onclick="Archives.nextPage()">Next</button>
        </div>`;
    },

    prevPage() {
        if (this.currentPage > 0) {
            this.currentPage--;
            const container = document.getElementById('archivesList');
            if (container) this.renderItems(container);
        }
    },

    nextPage() {
        const totalPages = Math.ceil(this.allItems.length / this.pageSize);
        if (this.currentPage < totalPages - 1) {
            this.currentPage++;
            const container = document.getElementById('archivesList');
            if (container) this.renderItems(container);
        }
    },

    toggleGroup(groupId) {
        if (this.expandedGroups.has(groupId)) {
            this.expandedGroups.delete(groupId);
        } else {
            this.expandedGroups.add(groupId);
        }
        const container = document.getElementById('archivesList');
        if (container) this.renderItems(container);
    },

    splitLabel(granularity) {
        switch (granularity) {
            case 'hour': return 'Hourly Split';
            case 'day': return 'Daily Split';
            case 'week': return 'Weekly Split';
            case 'none': return 'Single Archive';
            default: return granularity || 'Archive';
        }
    },

    formatTimeRange(archive) {
        if (!archive.time_range_start || !archive.time_range_end) return 'N/A';
        const start = new Date(archive.time_range_start).toLocaleString();
        const end = new Date(archive.time_range_end).toLocaleString();
        return `${start} to ${end}`;
    },

    formatCount(n) {
        if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
        if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
        if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
        return n.toString();
    },

    formatBytes(bytes) {
        if (!bytes || bytes === 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
    },

    async createArchive() {
        if (!this.currentFractalId && window.FractalContext && FractalContext.currentFractal) {
            this.currentFractalId = FractalContext.currentFractal.id;
        }
        if (!this.currentFractalId) {
            Toast.error('Archive Failed', 'No fractal selected');
            return;
        }

        const splitSelect = document.getElementById('archiveSplitGranularity');
        const split = splitSelect ? splitSelect.value : 'none';

        const btn = document.getElementById('createArchiveBtn');
        try {
            if (btn) {
                btn.disabled = true;
                btn.innerHTML = '<span class="spinner-sm"></span> Starting...';
            }

            const response = await fetch(`/api/v1/fractals/${this.currentFractalId}/archives`, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ split })
            });
            const data = await response.json();

            if (!data.success) {
                Toast.error('Archive Failed', data.error || 'Failed to create archive');
                return;
            }

            Toast.success('Archive Started', 'Archive creation is in progress.');
            this.loadArchives(this.currentFractalId);
        } catch (err) {
            Toast.error('Archive Failed', err.message);
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.textContent = 'Create Archive';
            }
        }
    },

    // Single archive restore
    async confirmRestore(archiveId, isResume) {
        if (!this.currentFractalId && window.FractalContext && FractalContext.currentFractal) {
            this.currentFractalId = FractalContext.currentFractal.id;
        }
        this.pendingRestoreArchiveId = archiveId;
        this.pendingRestoreGroupId = null;
        this.pendingRestoreIsResume = !!isResume;

        const dialog = document.getElementById('archiveRestoreDialog');
        const info = document.getElementById('archiveRestoreInfo');
        if (!dialog) return;

        if (isResume) {
            info.textContent = 'A previous restore was interrupted. Provide the same ingest API key to resume from where it left off.';
        } else {
            info.textContent = 'Provide an ingest API key for the target fractal. The token determines which fractal logs are restored into.';
        }
        dialog.style.display = '';
    },

    // Group restore
    async confirmRestoreGroup(groupId) {
        if (!this.currentFractalId && window.FractalContext && FractalContext.currentFractal) {
            this.currentFractalId = FractalContext.currentFractal.id;
        }
        this.pendingRestoreArchiveId = null;
        this.pendingRestoreGroupId = groupId;
        this.pendingRestoreIsResume = false;

        const dialog = document.getElementById('archiveRestoreDialog');
        const info = document.getElementById('archiveRestoreInfo');
        if (!dialog) return;

        info.textContent = 'Provide an ingest API key for the target fractal. All archives in this group will be restored sequentially.';
        dialog.style.display = '';
    },

    cancelRestore() {
        this.pendingRestoreArchiveId = null;
        this.pendingRestoreGroupId = null;
        this.pendingRestoreIsResume = false;
        const dialog = document.getElementById('archiveRestoreDialog');
        if (dialog) dialog.style.display = 'none';
        const tokenInput = document.getElementById('archiveRestoreToken');
        if (tokenInput) tokenInput.value = '';
    },

    async executeRestore() {
        const isGroup = !!this.pendingRestoreGroupId;
        const id = isGroup ? this.pendingRestoreGroupId : this.pendingRestoreArchiveId;
        if (!id || !this.currentFractalId) return;

        const confirmBtn = document.getElementById('archiveRestoreConfirmBtn');
        const tokenInput = document.getElementById('archiveRestoreToken');

        const ingestToken = tokenInput ? tokenInput.value.trim() : '';
        if (!ingestToken) {
            Toast.error('Missing Token', 'An ingest API key is required to restore an archive.');
            return;
        }

        try {
            if (confirmBtn) {
                confirmBtn.disabled = true;
                confirmBtn.innerHTML = '<span class="spinner-sm"></span> Restoring...';
            }

            const url = isGroup
                ? `/api/v1/fractals/${this.currentFractalId}/archive-groups/${id}/restore`
                : `/api/v1/fractals/${this.currentFractalId}/archives/${id}/restore`;

            const response = await fetch(url, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    ingest_token: ingestToken,
                    clear_existing: !this.pendingRestoreIsResume
                })
            });
            const data = await response.json();

            if (!data.success) {
                Toast.error('Restore Failed', data.error || 'Failed to start restore');
                return;
            }

            Toast.success('Restore Started', 'Archive restore is in progress.');
            this.cancelRestore();
            this.loadArchives(this.currentFractalId);
        } catch (err) {
            Toast.error('Restore Failed', err.message);
        } finally {
            if (confirmBtn) {
                confirmBtn.disabled = false;
                confirmBtn.textContent = 'Restore';
            }
        }
    },

    async confirmCancel(archiveId) {
        if (!confirm('Cancel this operation?')) return;

        try {
            const response = await fetch(
                `/api/v1/fractals/${this.currentFractalId}/archives/${archiveId}/cancel`,
                { method: 'POST', credentials: 'include' }
            );
            const data = await response.json();

            if (!data.success) {
                Toast.error('Cancel Failed', data.error || 'Failed to cancel operation');
                return;
            }

            Toast.success('Operation Cancelled', 'The operation has been stopped.');
            this.loadArchives(this.currentFractalId);
        } catch (err) {
            Toast.error('Cancel Failed', err.message);
        }
    },

    async confirmCancelGroup(groupId) {
        if (!confirm('Cancel this group operation?')) return;

        try {
            const response = await fetch(
                `/api/v1/fractals/${this.currentFractalId}/archive-groups/${groupId}/cancel`,
                { method: 'POST', credentials: 'include' }
            );
            const data = await response.json();

            if (!data.success) {
                Toast.error('Cancel Failed', data.error || 'Failed to cancel operation');
                return;
            }

            Toast.success('Operation Cancelled', 'The group operation has been stopped.');
            this.loadArchives(this.currentFractalId);
        } catch (err) {
            Toast.error('Cancel Failed', err.message);
        }
    },

    confirmDelete(archiveId) {
        if (!confirm('Delete this archive? This cannot be undone.')) return;
        this.deleteArchive(archiveId);
    },

    async deleteArchive(archiveId) {
        try {
            const response = await fetch(
                `/api/v1/fractals/${this.currentFractalId}/archives/${archiveId}`,
                { method: 'DELETE', credentials: 'include' }
            );
            const data = await response.json();

            if (!data.success) {
                Toast.error('Delete Failed', data.error || 'Failed to delete archive');
                return;
            }

            Toast.success('Archive Deleted', 'Archive has been removed.');
            this.loadArchives(this.currentFractalId);
        } catch (err) {
            Toast.error('Delete Failed', err.message);
        }
    },

    confirmDeleteGroup(groupId) {
        if (!confirm('Delete this archive group and all its archives? This cannot be undone.')) return;
        this.deleteGroup(groupId);
    },

    async deleteGroup(groupId) {
        try {
            const response = await fetch(
                `/api/v1/fractals/${this.currentFractalId}/archive-groups/${groupId}`,
                { method: 'DELETE', credentials: 'include' }
            );
            const data = await response.json();

            if (!data.success) {
                Toast.error('Delete Failed', data.error || 'Failed to delete archive group');
                return;
            }

            Toast.success('Group Deleted', 'Archive group and all archives have been removed.');
            this.expandedGroups.delete(groupId);
            this.loadArchives(this.currentFractalId);
        } catch (err) {
            Toast.error('Delete Failed', err.message);
        }
    },

    startPolling() {
        this.stopPolling();
        this.pollInterval = setInterval(() => {
            if (this.currentFractalId) {
                this.loadArchives(this.currentFractalId);
            }
        }, 3000);
    },

    stopPolling() {
        if (this.pollInterval) {
            clearInterval(this.pollInterval);
            this.pollInterval = null;
        }
    }
};

window.Archives = Archives;
document.addEventListener('DOMContentLoaded', () => Archives.init());
