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

    init() {},

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
                container.innerHTML = '<p class="text-muted">Unable to load archives.</p>';
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
            if (hasActive) this.startPolling();
        } catch {
            container.innerHTML = '<p class="text-muted">Unable to load archives.</p>';
        }
    },

    renderItems(container) {
        if (!this.allItems || this.allItems.length === 0) {
            container.innerHTML = '<p class="text-muted" style="margin-top: 0.75rem;">No archives yet.</p>';
            return;
        }

        const totalPages = Math.ceil(this.allItems.length / this.pageSize);
        const start = this.currentPage * this.pageSize;
        const pageItems = this.allItems.slice(start, start + this.pageSize);

        let cards = '';
        for (const item of pageItems) {
            if (item.type === 'group') {
                cards += this.renderGroup(item.group);
            } else if (item.archive) {
                cards += this.renderStandaloneArchive(item.archive);
            }
        }

        let html = `<div class="archive-list">${cards}</div>`;
        if (totalPages > 1) {
            html += this.renderPagination(totalPages);
        }
        container.innerHTML = html;
    },

    // ── Group rendering ──────────────────────────────────────────────

    renderGroup(group) {
        const isExpanded = this.expandedGroups.has(group.id) ||
            group.status === 'in_progress' || group.status === 'restoring';
        const archiveCount = group.archive_count || 0;
        const totalLogs = group.total_log_count || 0;
        const totalSize = group.total_size_bytes || 0;
        const date = this.formatDate(group.created_at);
        const statusBadge = this.renderGroupStatus(group);
        const actions = this.renderGroupActions(group);

        // Build a concise summary line.
        const countLabel = archiveCount === 1 ? '1 archive' : `${archiveCount} archives`;
        const splitWord = this.splitWord(group.split_granularity);
        let summary = splitWord ? `${countLabel}, ${splitWord}` : countLabel;
        if (group.status === 'in_progress') {
            const done = group.completed_count || 0;
            summary = `${done} / ${archiveCount} archives`;
        }

        const chevronClass = isExpanded ? 'archive-chevron expanded' : 'archive-chevron';

        let html = `<div class="archive-card${isExpanded ? ' expanded' : ''}">
            <div class="archive-card-header" onclick="Archives.toggleGroup('${group.id}')">
                <div class="archive-card-left">
                    <span class="${chevronClass}">
                        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"/></svg>
                    </span>
                    ${statusBadge}
                    <span class="archive-card-summary">${Utils.escapeHtml(summary)}</span>
                </div>
                <div class="archive-card-right">
                    <span class="archive-card-meta">${totalLogs > 0 ? this.formatCount(totalLogs) + ' logs' : ''}</span>
                    <span class="archive-card-meta">${totalSize > 0 ? this.formatBytes(totalSize) : ''}</span>
                    <span class="archive-card-meta">${date}</span>
                    <span class="archive-card-actions" onclick="event.stopPropagation()">${actions}</span>
                </div>
            </div>`;

        if (isExpanded && group.archives && group.archives.length > 0) {
            html += '<div class="archive-children">';
            for (const archive of group.archives) {
                html += this.renderGroupChild(archive);
            }
            html += '</div>';
        }

        html += '</div>';
        return html;
    },

    renderGroupChild(archive) {
        const statusBadge = this.renderStatus(archive);
        const label = archive.period_label || archive.filename;
        const actions = this.renderActions(archive);

        return `<div class="archive-child-row">
            <div class="archive-card-left">
                ${statusBadge}
                <span class="archive-child-label">${Utils.escapeHtml(label)}</span>
            </div>
            <div class="archive-card-right">
                <span class="archive-card-meta">${archive.log_count > 0 ? this.formatCount(archive.log_count) + ' logs' : '-'}</span>
                <span class="archive-card-meta">${archive.size_bytes > 0 ? this.formatBytes(archive.size_bytes) : '-'}</span>
                <span class="archive-card-meta"></span>
                <span class="archive-card-actions">${actions}</span>
            </div>
        </div>`;
    },

    // ── Standalone archive rendering ─────────────────────────────────

    renderStandaloneArchive(a) {
        const statusBadge = this.renderStatus(a);
        const date = this.formatDate(a.created_at);
        const actions = this.renderActions(a);

        return `<div class="archive-card">
            <div class="archive-card-header">
                <div class="archive-card-left">
                    <span class="archive-chevron-spacer"></span>
                    ${statusBadge}
                    <span class="archive-card-summary">${Utils.escapeHtml(a.filename)}</span>
                </div>
                <div class="archive-card-right">
                    <span class="archive-card-meta">${a.log_count > 0 ? this.formatCount(a.log_count) + ' logs' : '-'}</span>
                    <span class="archive-card-meta">${a.size_bytes > 0 ? this.formatBytes(a.size_bytes) : '-'}</span>
                    <span class="archive-card-meta">${date}</span>
                    <span class="archive-card-actions">${actions}</span>
                </div>
            </div>
        </div>`;
    },

    // ── Status badges ────────────────────────────────────────────────

    renderGroupStatus(group) {
        const { status, error_message: err, completed_count, archive_count } = group;
        switch (status) {
            case 'completed':
                return '<span class="archive-badge archive-badge-success">Completed</span>';
            case 'in_progress':
                return `<span class="archive-badge archive-badge-active"><span class="spinner-sm"></span> Archiving</span>`;
            case 'restoring':
                return `<span class="archive-badge archive-badge-active"><span class="spinner-sm"></span> Restoring</span>`;
            case 'partial':
                return `<span class="archive-badge archive-badge-warning" title="${Utils.escapeHtml(err || '')}">Partial</span>`;
            case 'failed':
                return `<span class="archive-badge archive-badge-error" title="${Utils.escapeHtml(err || '')}">Failed</span>`;
            default:
                return `<span class="archive-badge">${Utils.escapeHtml(status)}</span>`;
        }
    },

    renderStatus(archive) {
        const { status, error_message: err, log_count: logCount, checksum,
                restore_lines_sent: sent, restore_error: restoreErr } = archive;
        switch (status) {
            case 'completed': {
                let badge = `<span class="archive-badge archive-badge-success"${checksum ? ' title="SHA-256 verified"' : ''}>Completed</span>`;
                if (restoreErr) {
                    const info = sent > 0 ? ` ${this.formatCount(sent)}/${this.formatCount(logCount)}` : '';
                    badge += ` <span class="archive-badge archive-badge-error" title="${Utils.escapeHtml(restoreErr)}">Restore failed${info}</span>`;
                }
                return badge;
            }
            case 'in_progress': {
                const info = logCount > 0 ? ` ${this.formatCount(logCount)}` : '';
                return `<span class="archive-badge archive-badge-active"><span class="spinner-sm"></span> Archiving${info}</span>`;
            }
            case 'restoring': {
                let info = '';
                if (sent > 0 && logCount > 0) {
                    const pct = Math.min(99, Math.round((sent / logCount) * 100));
                    info = ` ${pct}%`;
                }
                return `<span class="archive-badge archive-badge-active"><span class="spinner-sm"></span> Restoring${info}</span>`;
            }
            case 'failed':
                return `<span class="archive-badge archive-badge-error" title="${Utils.escapeHtml(err || '')}">Failed</span>`;
            default:
                return `<span class="archive-badge">${Utils.escapeHtml(status)}</span>`;
        }
    },

    // ── Action buttons ───────────────────────────────────────────────

    renderGroupActions(group) {
        if (group.status === 'in_progress' || group.status === 'restoring') {
            return this.actionBtn('Cancel', 'danger', `Archives.confirmCancelGroup('${group.id}')`);
        }
        let html = '';
        if (group.status === 'completed' || group.status === 'partial') {
            html += this.actionBtn('Restore', 'secondary', `Archives.confirmRestoreGroup('${group.id}')`) + ' ';
        }
        html += this.actionBtn('Delete', 'danger', `Archives.confirmDeleteGroup('${group.id}')`);
        return html;
    },

    renderActions(archive) {
        if (archive.status === 'in_progress' || archive.status === 'restoring') {
            return this.actionBtn('Cancel', 'danger', `Archives.confirmCancel('${archive.id}')`);
        }
        let html = '';
        if (archive.status === 'completed') {
            const partial = archive.restore_lines_sent > 0 && !!archive.restore_error;
            const label = partial ? 'Resume' : 'Restore';
            html += this.actionBtn(label, 'secondary', `Archives.confirmRestore('${archive.id}', ${partial})`) + ' ';
        }
        html += this.actionBtn('Delete', 'danger', `Archives.confirmDelete('${archive.id}')`);
        return html;
    },

    actionBtn(label, style, onclick) {
        const cls = style === 'danger' ? 'archive-action-btn archive-action-danger' : 'archive-action-btn';
        return `<button class="${cls}" onclick="${onclick}">${label}</button>`;
    },

    // ── Pagination ───────────────────────────────────────────────────

    renderPagination(totalPages) {
        const prevDis = this.currentPage === 0 ? ' disabled' : '';
        const nextDis = this.currentPage >= totalPages - 1 ? ' disabled' : '';
        return `<div class="archive-pagination">
            <button class="archive-action-btn"${prevDis} onclick="Archives.prevPage()">Prev</button>
            <span class="archive-page-info">${this.currentPage + 1} / ${totalPages}</span>
            <button class="archive-action-btn"${nextDis} onclick="Archives.nextPage()">Next</button>
        </div>`;
    },

    prevPage() {
        if (this.currentPage > 0) {
            this.currentPage--;
            const c = document.getElementById('archivesList');
            if (c) this.renderItems(c);
        }
    },

    nextPage() {
        const total = Math.ceil(this.allItems.length / this.pageSize);
        if (this.currentPage < total - 1) {
            this.currentPage++;
            const c = document.getElementById('archivesList');
            if (c) this.renderItems(c);
        }
    },

    toggleGroup(groupId) {
        if (this.expandedGroups.has(groupId)) {
            this.expandedGroups.delete(groupId);
        } else {
            this.expandedGroups.add(groupId);
        }
        const c = document.getElementById('archivesList');
        if (c) this.renderItems(c);
    },

    // ── Formatting helpers ───────────────────────────────────────────

    splitWord(g) {
        switch (g) {
            case 'hour': return 'hourly';
            case 'day': return 'daily';
            case 'week': return 'weekly';
            default: return '';
        }
    },

    formatDate(ts) {
        return new Date(ts).toLocaleDateString('en-US', {
            month: 'short', day: 'numeric', year: 'numeric'
        });
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

    // ── Create archive ───────────────────────────────────────────────

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

    // ── Restore flow ─────────────────────────────────────────────────

    async populateRestoreDropdown() {
        const select = document.getElementById('archiveRestoreTarget');
        if (!select) return;

        select.innerHTML = '<option value="">Loading...</option>';

        try {
            const response = await fetch('/api/v1/fractals', { credentials: 'include' });
            const data = await response.json();
            if (!data.success || !data.data || !data.data.fractals) {
                select.innerHTML = '<option value="">Failed to load fractals</option>';
                return;
            }

            select.innerHTML = '';
            for (const f of data.data.fractals) {
                const opt = document.createElement('option');
                opt.value = f.id;
                opt.textContent = f.name;
                if (f.id === this.currentFractalId) opt.selected = true;
                select.appendChild(opt);
            }
        } catch {
            select.innerHTML = '<option value="">Failed to load fractals</option>';
        }
    },

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

        await this.populateRestoreDropdown();
        info.textContent = isResume
            ? 'A previous restore was interrupted. It will resume from where it left off.'
            : 'Select which fractal to restore logs into.';
        dialog.style.display = '';
    },

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

        await this.populateRestoreDropdown();
        info.textContent = 'All archives in this group will be restored sequentially.';
        dialog.style.display = '';
    },

    cancelRestore() {
        this.pendingRestoreArchiveId = null;
        this.pendingRestoreGroupId = null;
        this.pendingRestoreIsResume = false;
        const dialog = document.getElementById('archiveRestoreDialog');
        if (dialog) dialog.style.display = 'none';
    },

    async executeRestore() {
        const isGroup = !!this.pendingRestoreGroupId;
        const id = isGroup ? this.pendingRestoreGroupId : this.pendingRestoreArchiveId;
        if (!id || !this.currentFractalId) return;

        const confirmBtn = document.getElementById('archiveRestoreConfirmBtn');
        const targetSelect = document.getElementById('archiveRestoreTarget');
        const targetFractalId = targetSelect ? targetSelect.value : '';

        if (!targetFractalId) {
            Toast.error('No Target', 'Please select a target fractal.');
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
                    target_fractal_id: targetFractalId,
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

    // ── Cancel / Delete ──────────────────────────────────────────────

    async confirmCancel(archiveId) {
        if (!confirm('Cancel this operation?')) return;
        try {
            const response = await fetch(
                `/api/v1/fractals/${this.currentFractalId}/archives/${archiveId}/cancel`,
                { method: 'POST', credentials: 'include' }
            );
            const data = await response.json();
            if (!data.success) { Toast.error('Cancel Failed', data.error); return; }
            this.loadArchives(this.currentFractalId);
        } catch (err) { Toast.error('Cancel Failed', err.message); }
    },

    async confirmCancelGroup(groupId) {
        if (!confirm('Cancel this group operation?')) return;
        try {
            const response = await fetch(
                `/api/v1/fractals/${this.currentFractalId}/archive-groups/${groupId}/cancel`,
                { method: 'POST', credentials: 'include' }
            );
            const data = await response.json();
            if (!data.success) { Toast.error('Cancel Failed', data.error); return; }
            this.loadArchives(this.currentFractalId);
        } catch (err) { Toast.error('Cancel Failed', err.message); }
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
            if (!data.success) { Toast.error('Delete Failed', data.error); return; }
            this.loadArchives(this.currentFractalId);
        } catch (err) { Toast.error('Delete Failed', err.message); }
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
            if (!data.success) { Toast.error('Delete Failed', data.error); return; }
            this.expandedGroups.delete(groupId);
            this.loadArchives(this.currentFractalId);
        } catch (err) { Toast.error('Delete Failed', err.message); }
    },

    // ── Polling ──────────────────────────────────────────────────────

    startPolling() {
        this.stopPolling();
        this.pollInterval = setInterval(() => {
            if (this.currentFractalId) this.loadArchives(this.currentFractalId);
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
