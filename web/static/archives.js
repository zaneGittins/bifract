const Archives = {
    currentFractalId: null,
    pollInterval: null,
    pendingRestoreArchiveId: null,

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

            this.renderArchivesList(data.data.archives, container);

            const hasActive = data.data.archives.some(a =>
                a.status === 'in_progress' || a.status === 'restoring'
            );
            if (hasActive) {
                this.startPolling();
            }
        } catch (err) {
            container.innerHTML = `<p class="text-muted">Unable to load archives.</p>`;
        }
    },

    renderArchivesList(archives, container) {
        if (!archives || archives.length === 0) {
            container.innerHTML = `<p class="text-muted" style="margin-top: 0.75rem;">No archives yet.</p>`;
            return;
        }

        const rows = archives.map(a => {
            const statusBadge = this.renderStatus(a.status, a.error_message, a.log_count);
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
                <td>${statusBadge}</td>
                <td>${typeBadge}</td>
                <td>${Utils.escapeHtml(a.filename)}</td>
                <td>${a.log_count.toLocaleString()}</td>
                <td>${size}</td>
                <td title="${Utils.escapeHtml(timeRange)}">${date}</td>
                <td>${actions}</td>
            </tr>`;
        }).join('');

        container.innerHTML = `
            <table class="archives-table">
                <thead>
                    <tr>
                        <th>Status</th>
                        <th>Type</th>
                        <th>Filename</th>
                        <th>Logs</th>
                        <th>Size</th>
                        <th>Created</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>${rows}</tbody>
            </table>`;
    },

    renderStatus(status, errorMessage, logCount) {
        switch (status) {
            case 'completed':
                return `<span class="archive-badge archive-badge-success">Completed</span>`;
            case 'in_progress': {
                const progress = logCount > 0 ? ` ${this.formatCount(logCount)} logs` : '';
                return `<span class="archive-badge archive-badge-active"><span class="spinner-sm"></span> Archiving${progress}</span>`;
            }
            case 'restoring':
                return `<span class="archive-badge archive-badge-active"><span class="spinner-sm"></span> Restoring</span>`;
            case 'failed':
                return `<span class="archive-badge archive-badge-error" title="${Utils.escapeHtml(errorMessage || '')}">Failed</span>`;
            default:
                return `<span class="archive-badge">${Utils.escapeHtml(status)}</span>`;
        }
    },

    renderActions(archive) {
        if (archive.status === 'in_progress' || archive.status === 'restoring') {
            return `<button class="btn-danger-sm btn-xs" onclick="Archives.confirmCancel('${archive.id}')">Cancel</button>`;
        }

        let actions = '';
        if (archive.status === 'completed') {
            actions += `<button class="btn-secondary btn-xs" onclick="Archives.confirmRestore('${archive.id}')">Restore</button> `;
        }
        actions += `<button class="btn-danger-sm btn-xs" onclick="Archives.confirmDelete('${archive.id}')">Delete</button>`;
        return actions;
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
        if (bytes === 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
    },

    async createArchive() {
        // Fallback: get fractal ID from context if loadArchives wasn't called
        if (!this.currentFractalId && window.FractalContext && FractalContext.currentFractal) {
            this.currentFractalId = FractalContext.currentFractal.id;
        }
        if (!this.currentFractalId) {
            Toast.error('Archive Failed', 'No fractal selected');
            return;
        }

        const btn = document.getElementById('createArchiveBtn');
        try {
            if (btn) {
                btn.disabled = true;
                btn.innerHTML = '<span class="spinner-sm"></span> Starting...';
            }

            const response = await fetch(`/api/v1/fractals/${this.currentFractalId}/archives`, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' }
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

    async confirmRestore(archiveId) {
        if (!this.currentFractalId && window.FractalContext && FractalContext.currentFractal) {
            this.currentFractalId = FractalContext.currentFractal.id;
        }
        this.pendingRestoreArchiveId = archiveId;

        const dialog = document.getElementById('archiveRestoreDialog');
        const info = document.getElementById('archiveRestoreInfo');
        const clearCheckbox = document.getElementById('archiveRestoreClear');

        if (!dialog) return;

        info.textContent = 'Provide an ingest API key for the target fractal. The token determines which fractal logs are restored into.';
        clearCheckbox.checked = true;
        dialog.style.display = '';
    },

    cancelRestore() {
        this.pendingRestoreArchiveId = null;
        const dialog = document.getElementById('archiveRestoreDialog');
        if (dialog) dialog.style.display = 'none';
        const tokenInput = document.getElementById('archiveRestoreToken');
        if (tokenInput) tokenInput.value = '';
    },

    async executeRestore() {
        if (!this.pendingRestoreArchiveId || !this.currentFractalId) return;

        const clearCheckbox = document.getElementById('archiveRestoreClear');
        const confirmBtn = document.getElementById('archiveRestoreConfirmBtn');
        const tokenInput = document.getElementById('archiveRestoreToken');

        const clearExisting = clearCheckbox ? clearCheckbox.checked : true;
        const ingestToken = tokenInput ? tokenInput.value.trim() : '';

        if (!ingestToken) {
            Toast.error('Missing Token', 'An ingest API key is required to restore an archive.');
            return;
        }

        if (clearExisting) {
            if (!confirm('This will clear existing logs in the target fractal and replace them with the archived data. Continue?')) return;
        }

        try {
            if (confirmBtn) {
                confirmBtn.disabled = true;
                confirmBtn.innerHTML = '<span class="spinner-sm"></span> Restoring...';
            }

            const response = await fetch(
                `/api/v1/fractals/${this.currentFractalId}/archives/${this.pendingRestoreArchiveId}/restore`,
                {
                    method: 'POST',
                    credentials: 'include',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ clear_existing: clearExisting, ingest_token: ingestToken })
                }
            );
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

    confirmCancel(archiveId) {
        if (!confirm('Cancel this operation? The incomplete archive will be removed.')) return;
        this.deleteArchive(archiveId);
    },

    confirmDelete(archiveId) {
        if (!confirm('Delete this archive? This cannot be undone.')) return;
        this.deleteArchive(archiveId);
    },

    async deleteArchive(archiveId) {
        try {
            const response = await fetch(
                `/api/v1/fractals/${this.currentFractalId}/archives/${archiveId}`,
                {
                    method: 'DELETE',
                    credentials: 'include'
                }
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
