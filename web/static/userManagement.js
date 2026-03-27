// User Management module for Bifract

const UserManagement = {
    users: [],

    async init() {
        // Event listeners
        const addUserBtn = document.getElementById('addUserBtn');
        const createUserBtn = document.getElementById('createUserBtn');
        const cancelAddUserBtn = document.getElementById('cancelAddUserBtn');

        if (addUserBtn) {
            addUserBtn.addEventListener('click', () => this.showAddUserForm());
        }

        if (createUserBtn) {
            createUserBtn.addEventListener('click', () => this.createUser());
        }

        if (cancelAddUserBtn) {
            cancelAddUserBtn.addEventListener('click', () => this.hideAddUserForm());
        }

        // Load users if admin
        if (Auth.getCurrentUser() && Auth.getCurrentUser().is_admin) {
            await this.loadUsers();
        }
    },

    async loadUsers() {
        try {
            const response = await fetch('/api/v1/users', {
                credentials: 'include'
            });

            const data = await response.json();

            if (data.success) {
                this.users = data.data || [];
                this.renderUsers();
            } else {
                console.error('Failed to load users:', data.error);
                // Still render empty state
                this.users = [];
                this.renderUsers();
            }
        } catch (error) {
            console.error('Error loading users:', error);
            // Still render empty state
            this.users = [];
            this.renderUsers();
        }
    },

    renderUsers() {
        const container = document.getElementById('usersList');
        if (!container) return;

        if (this.users.length === 0) {
            container.innerHTML = '<div class="no-data">Only the default admin user exists</div>';
            return;
        }

        let html = '<table class="users-table"><thead><tr>';
        html += '<th>User</th>';
        html += '<th>Role</th>';
        html += '<th>Last Login</th>';
        html += '<th>Actions</th>';
        html += '</tr></thead><tbody>';

        const currentUser = Auth.getCurrentUser();

        this.users.forEach(user => {
            const isSelf = currentUser && currentUser.username === user.username;
            const lastLogin = user.last_login
                ? new Date(user.last_login).toLocaleString()
                : 'Never';

            html += '<tr>';
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
            html += `<td class="text-muted">${lastLogin}</td>`;
            html += `<td>`;

            if (!isSelf) {
                html += `<button class="btn-delete-user" onclick="UserManagement.deleteUser('${Utils.escapeJs(user.username)}')">Delete</button>`;
            } else {
                html += '<span class="text-muted">Current user</span>';
            }

            html += `</td>`;
            html += '</tr>';
        });

        html += '</tbody></table>';
        container.innerHTML = html;
    },

    showAddUserForm() {
        document.getElementById('addUserForm').style.display = 'block';
        document.getElementById('usersList').style.opacity = '0.5';
        document.getElementById('newUsername').focus();
    },

    hideAddUserForm() {
        document.getElementById('addUserForm').style.display = 'none';
        document.getElementById('usersList').style.opacity = '1';
        document.getElementById('newUsername').value = '';
        document.getElementById('newPassword').value = '';
        document.getElementById('newDisplayName').value = '';
        document.getElementById('newUserRole').value = 'user';
        document.getElementById('addUserError').textContent = '';
    },

    async createUser() {
        const username = document.getElementById('newUsername').value.trim();
        const password = document.getElementById('newPassword').value;
        const displayName = document.getElementById('newDisplayName').value.trim();
        const role = document.getElementById('newUserRole').value;
        const errorDiv = document.getElementById('addUserError');

        errorDiv.textContent = '';

        // Validate
        if (!username || !password) {
            errorDiv.textContent = 'Username and password are required';
            return;
        }

        if (password.length < 4) {
            errorDiv.textContent = 'Password must be at least 4 characters';
            return;
        }

        try {
            const response = await fetch('/api/v1/auth/register', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({
                    username,
                    password,
                    display_name: displayName || username,
                    role
                })
            });

            const data = await response.json();

            if (data.success) {
                this.hideAddUserForm();
                await this.loadUsers();
                alert(`User '${username}' created successfully!`);
            } else {
                errorDiv.textContent = data.error || 'Failed to create user';
            }
        } catch (error) {
            console.error('Error creating user:', error);
            errorDiv.textContent = 'Network error. Please try again.';
        }
    },

    async deleteUser(username) {
        if (!confirm(`Are you sure you want to delete user '${username}'? This action cannot be undone.`)) {
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

};

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    UserManagement.init();
});
