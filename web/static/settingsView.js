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
        window.App?.pushSubPath(tabName);
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
        await this.loadMTLSStatus();
        await this.loadUsers();

        // Load groups if available
        if (window.GroupsView) {
            GroupsView.loadGroups();
        }

        if (subPath) this.switchSubTab(subPath);
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
        const container = document.getElementById('usersListSettings');
        if (!container) return;

        // Keep the rendered set so the Edit modal can resolve display name/role by
        // username instead of threading them (unescaped) through inline onclick handlers.
        this._users = users;

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

            const isDisabled = user.enabled === false;

            if (isDisabled) {
                html += `<td><span class="role-badge role-disabled">Disabled</span></td>`;
            } else if (user.invite_pending) {
                html += `<td><span class="role-badge role-pending">Invite pending</span></td>`;
            } else {
                html += `<td class="text-muted">${lastLogin}</td>`;
            }

            const u = Utils.escapeJs(user.username);
            html += `<td class="user-actions-cell">`;

            // Primary inline action: Edit (opens a modal; identity is never edited in place).
            if (isAdmin) {
                html += `<button class="btn-edit-user" onclick="SettingsView.openEditUserModal('${u}')">Edit</button>`;
            }

            // Secondary / destructive actions collapse into a kebab overflow menu.
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

            if (items.length) {
                html += `<div class="kebab-wrapper"><button class="kebab-btn" onclick="KebabMenu.toggle(event,this)" title="More actions">&#8942;</button><div class="kebab-menu">${items.join('')}</div></div>`;
            }
            if (isSelf) {
                html += '<span class="text-muted">You</span>';
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
