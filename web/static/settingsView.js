// Settings View module

const SettingsView = {
    statusUpdateInterval: null,
    lastLogCount: 0,
    isActive: false,

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
        const alertTimeoutSelect = document.getElementById('alertTimeoutSettings');
        if (alertTimeoutSelect) {
            alertTimeoutSelect.addEventListener('change', () => this.saveSettings());
        }
        const queryTimeoutSelect = document.getElementById('queryTimeoutSettings');
        if (queryTimeoutSelect) {
            queryTimeoutSelect.addEventListener('change', () => this.saveSettings());
        }
    },

    switchSubTab(tabName) {
        const tabBar = document.getElementById('settingsSubTabs');
        if (tabBar) {
            tabBar.querySelectorAll('.alerts-sub-tab').forEach(btn => btn.classList.remove('active'));
            const activeBtn = tabBar.querySelector(`.alerts-sub-tab[data-subtab="${tabName}"]`);
            if (activeBtn) activeBtn.classList.add('active');
        }
        document.querySelectorAll('.settings-sub-panel').forEach(panel => panel.style.display = 'none');
        const panel = document.getElementById('settingsSubTab' + tabName.charAt(0).toUpperCase() + tabName.slice(1));
        if (panel) panel.style.display = '';
    },

    async show() {
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
        await this.loadStatus();
        await this.loadUsers();

        // Load groups if available
        if (window.GroupsView) {
            GroupsView.loadGroups();
        }

        // Start real-time updates
        this.startRealTimeUpdates();
    },

    hide() {
        const settingsView = document.getElementById('settingsView');
        if (settingsView) {
            settingsView.style.display = 'none';
        }

        this.isActive = false;
        this.stopRealTimeUpdates();
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
            }
        } catch (error) {
            console.error('Failed to load settings:', error);
        }
    },

    async saveSettings() {
        try {
            const response = await fetch('/api/v1/settings', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({
                    alert_timeout_seconds: parseInt(document.getElementById('alertTimeoutSettings')?.value || '5', 10),
                    query_timeout_seconds: parseInt(document.getElementById('queryTimeoutSettings')?.value || '60', 10)
                })
            });

            const data = await response.json();
            if (!data.success) {
                alert('Failed to save settings');
            }
        } catch (error) {
            console.error('Failed to save settings:', error);
            alert('Failed to save settings');
        }
    },

    async loadStatus(showUpdateIndicator = false) {
        const container = document.getElementById('settingsStatus');
        if (!container) return;

        try {
            const response = await fetch('/api/v1/status', { credentials: 'include' });
            const data = await response.json();

            if (data.success) {
                const ch = data.clickhouse || {};
                const sys = data.system || {};
                const currentLogCount = ch.total_logs || 0;

                // Check if log count changed (only after initial load)
                const isInitialLoad = this.lastLogCount === 0;
                const logCountChanged = !isInitialLoad && this.lastLogCount !== currentLogCount;
                const countIncrease = logCountChanged && currentLogCount > this.lastLogCount;

                // Update internal counter
                this.lastLogCount = currentLogCount;

                // Disk pressure indicator
                const diskPct = ch.disk_used_pct || 0;
                let diskClass = 'status-ok';
                if (diskPct >= 90) diskClass = 'status-error';
                else if (diskPct >= 80) diskClass = 'status-warn';

                const timeRange = ch.oldest_log && ch.newest_log
                    ? this.formatTimestamp(ch.oldest_log) + ' to ' + this.formatTimestamp(ch.newest_log)
                    : 'No logs';

                container.innerHTML = `
                    <div class="status-rows">
                        <div class="detail-row">
                            <span class="detail-label">ClickHouse:</span>
                            <span class="detail-value ${ch.connected ? 'status-ok' : 'status-error'}">${ch.connected ? 'Connected' : 'Disconnected'}</span>
                        </div>
                        <div class="detail-row">
                            <span class="detail-label">Uptime:</span>
                            <span class="detail-value">${ch.uptime || '-'}</span>
                        </div>
                        <div class="detail-row ${logCountChanged ? 'status-updated' : ''}">
                            <span class="detail-label">Total Logs:</span>
                            <span class="detail-value log-count-value">${currentLogCount.toLocaleString()}${countIncrease ? ' <span class="count-increase">↑</span>' : ''}</span>
                        </div>
                        <div class="detail-row">
                            <span class="detail-label">Log Storage:</span>
                            <span class="detail-value">${ch.table_size || '0 B'}</span>
                        </div>
                        <div class="detail-row">
                            <span class="detail-label">Disk Usage:</span>
                            <span class="detail-value ${diskClass}">${diskPct}%${ch.disk_free ? ' (' + ch.disk_free + ' free)' : ''}</span>
                        </div>
                        <div class="detail-row">
                            <span class="detail-label">Log Time Range:</span>
                            <span class="detail-value">${timeRange}</span>
                        </div>
                        <div class="detail-row">
                            <span class="detail-label">Fractals:</span>
                            <span class="detail-value">${sys.fractal_count || 0}</span>
                        </div>
                        <div class="detail-row">
                            <span class="detail-label">Users:</span>
                            <span class="detail-value">${sys.user_count || 0}</span>
                        </div>
                    </div>
                `;

                // Add visual feedback for log count changes
                if (logCountChanged) {
                    const logCountElement = container.querySelector('.status-item.status-updated');
                    if (logCountElement) {
                        logCountElement.classList.add('highlight-update');
                        setTimeout(() => {
                            logCountElement.classList.remove('highlight-update', 'status-updated');
                        }, 2000);
                    }
                }
            }
        } catch (error) {
            console.error('Failed to load status:', error);
        }
    },

    formatTimestamp(ts) {
        if (!ts) return '-';
        try {
            const d = new Date(ts);
            if (isNaN(d.getTime())) return ts;
            return d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
        } catch {
            return ts;
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
        const container = document.getElementById('usersListSettings');
        if (!container) return;

        if (users.length === 0) {
            container.innerHTML = '<div class="no-data">Only the default admin user exists</div>';
            return;
        }

        let html = '<table class="users-table"><thead><tr>';
        html += '<th>User</th><th>Role</th><th>Status</th><th>Actions</th>';
        html += '</tr></thead><tbody>';

        const currentUser = Auth.getCurrentUser();

        users.forEach(user => {
            const isSelf = currentUser && currentUser.username === user.username;
            const lastLogin = user.last_login ? new Date(user.last_login).toLocaleString() : 'Never';
            const isAdmin = currentUser && currentUser.is_admin;

            html += `<tr>`;
            html += `<td>
                <div class="user-cell">
                    <div class="gravatar" style="background-color: ${user.gravatar_color}">
                        ${user.gravatar_initial}
                    </div>
                    <div class="user-info">
                        <div class="user-name">${Utils.escapeHtml(user.display_name)}</div>
                        <div class="user-username">@${Utils.escapeHtml(user.username)}</div>
                    </div>
                </div>
            </td>`;
            html += `<td><span class="role-badge role-${user.role}">${user.role === 'admin' ? 'Tenant Admin' : 'User'}</span></td>`;

            if (user.invite_pending) {
                html += `<td><span class="role-badge role-pending">Invite pending</span></td>`;
            } else {
                html += `<td class="text-muted">${lastLogin}</td>`;
            }

            html += `<td class="user-actions-cell">`;
            if (isAdmin) {
                html += `<button class="btn-edit-user" onclick="SettingsView.editUserInline(this.closest('tr'), '${Utils.escapeJs(user.username)}', '${Utils.escapeJs(user.display_name)}', '${Utils.escapeJs(user.role)}')">Edit</button>`;
            }
            if (user.invite_pending) {
                html += `<button class="btn-invite-reset" onclick="SettingsView.resetInvite('${Utils.escapeJs(user.username)}')" title="Regenerate invite link">Resend Invite</button>`;
            } else if (!isSelf && isAdmin) {
                html += `<button class="btn-secondary btn-sm" onclick="SettingsView.resetPassword('${Utils.escapeJs(user.username)}')">Reset Password</button>`;
            }
            if (!isSelf) {
                html += `<button class="btn-delete-user" onclick="SettingsView.deleteUser('${Utils.escapeJs(user.username)}')">Delete</button>`;
            } else {
                html += '<span class="text-muted">You</span>';
            }
            html += `</td></tr>`;
        });

        html += '</tbody></table>';
        container.innerHTML = html;
    },

    editUserInline(row, username, displayName, role) {
        // Replace the row content with editable fields
        const cells = row.querySelectorAll('td');
        if (!cells || cells.length < 4) return;

        // Replace display name with input
        const nameDiv = cells[0].querySelector('.user-name');
        if (nameDiv) {
            const input = document.createElement('input');
            input.type = 'text';
            input.value = displayName;
            input.className = 'edit-user-input';
            input.id = 'editDisplayName';
            nameDiv.replaceWith(input);
            input.focus();
            input.select();
        }

        // Replace role badge with select
        cells[1].innerHTML = `<select id="editUserRole" class="edit-user-select">
            <option value="user" ${role === 'user' ? 'selected' : ''}>User</option>
            <option value="admin" ${role === 'admin' ? 'selected' : ''}>Tenant Admin</option>
        </select>`;

        // Replace actions with save/cancel
        cells[3].innerHTML = `
            <button class="btn-primary btn-sm" onclick="event.stopPropagation(); SettingsView.saveUserEdit('${Utils.escapeJs(username)}')">Save</button>
            <button class="btn-secondary btn-sm" onclick="event.stopPropagation(); SettingsView.loadUsers()">Cancel</button>
        `;

    },

    async saveUserEdit(username) {
        const displayName = document.getElementById('editDisplayName')?.value.trim();
        const role = document.getElementById('editUserRole')?.value;

        if (!displayName) {
            if (window.Toast) Toast.error('Error', 'Display name cannot be empty');
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
                if (window.Toast) Toast.success('Updated', `User '${username}' updated`);
                await this.loadUsers();
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to update user');
            }
        } catch (error) {
            console.error('Error updating user:', error);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    showAddUserForm() {
        document.getElementById('addUserFormSettings').style.display = 'block';
        document.getElementById('usersListSettings').style.opacity = '0.5';
        document.getElementById('newUsernameSettings').focus();
    },

    hideAddUserForm() {
        document.getElementById('addUserFormSettings').style.display = 'none';
        document.getElementById('usersListSettings').style.opacity = '1';
        document.getElementById('newUsernameSettings').value = '';
        document.getElementById('newDisplayNameSettings').value = '';
        document.getElementById('newUserRoleSettings').value = 'user';
        document.getElementById('addUserErrorSettings').textContent = '';
        this.hideInviteLink();
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
                this.hideAddUserForm();
                await this.loadUsers();
                this.showInviteLink(data.data.invite_url, username);
            } else {
                errorDiv.textContent = data.error || 'Failed to create user';
            }
        } catch (error) {
            console.error('Error creating user:', error);
            errorDiv.textContent = 'Network error. Please try again.';
        }
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

    copyInviteLink() {
        const input = document.getElementById('inviteLinkInput');
        if (input) {
            navigator.clipboard.writeText(input.value).then(() => {
                if (window.Toast) {
                    Toast.success('Copied', 'Invite link copied to clipboard');
                }
            });
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
                if (window.Toast) {
                    Toast.success('Cleanup Complete', 'All logs and comments have been deleted');
                } else {
                    alert('All logs and comments have been deleted.');
                }
                await this.loadStatus();
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

    startRealTimeUpdates() {
        // Clear any existing interval
        this.stopRealTimeUpdates();

        // Update every 3 seconds
        this.statusUpdateInterval = setInterval(async () => {
            if (this.isActive) {
                await this.loadStatus(true); // Show update indicator
            }
        }, 3000);
    },

    stopRealTimeUpdates() {
        if (this.statusUpdateInterval) {
            clearInterval(this.statusUpdateInterval);
            this.statusUpdateInterval = null;
        }
    },

};

// Make globally available
window.SettingsView = SettingsView;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    SettingsView.init();
});

// Cleanup on page unload
window.addEventListener('beforeunload', () => {
    SettingsView.stopRealTimeUpdates();
});
