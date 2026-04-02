// Theme manager - runs immediately to prevent flash
const ThemeManager = {
    STORAGE_KEY: 'bifract-theme',

    init() {
        const saved = localStorage.getItem(this.STORAGE_KEY);
        if (saved === 'light') {
            document.documentElement.setAttribute('data-theme', 'light');
        }
    },

    isDark() {
        return document.documentElement.getAttribute('data-theme') !== 'light';
    },

    toggle() {
        const isNowDark = this.isDark();
        const newTheme = isNowDark ? 'light' : 'dark';

        // Smooth transition
        document.body.classList.add('theme-transitioning');
        if (newTheme === 'light') {
            document.documentElement.setAttribute('data-theme', 'light');
        } else {
            document.documentElement.removeAttribute('data-theme');
        }
        localStorage.setItem(this.STORAGE_KEY, newTheme);

        // Update toggle icon
        this.updateIcon();

        // Remove transition class after animation completes
        setTimeout(() => document.body.classList.remove('theme-transitioning'), 350);
    },

    updateIcon() {
        const icon = document.getElementById('themeToggleIcon');
        if (!icon) return;
        if (this.isDark()) {
            // Moon icon - click to switch to light
            icon.innerHTML = '<path d="M21 12.79A9 9 0 1 1 11.21 3a7 7 0 0 0 9.79 9.79z" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>';
        } else {
            // Sun icon - click to switch to dark
            icon.innerHTML = '<circle cx="12" cy="12" r="5" fill="none" stroke="currentColor" stroke-width="2"/><line x1="12" y1="1" x2="12" y2="3" stroke="currentColor" stroke-width="2" stroke-linecap="round"/><line x1="12" y1="21" x2="12" y2="23" stroke="currentColor" stroke-width="2" stroke-linecap="round"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64" stroke="currentColor" stroke-width="2" stroke-linecap="round"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78" stroke="currentColor" stroke-width="2" stroke-linecap="round"/><line x1="1" y1="12" x2="3" y2="12" stroke="currentColor" stroke-width="2" stroke-linecap="round"/><line x1="21" y1="12" x2="23" y2="12" stroke="currentColor" stroke-width="2" stroke-linecap="round"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36" stroke="currentColor" stroke-width="2" stroke-linecap="round"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>';
        }
    },

    // Read a CSS variable value (for JS chart/canvas use)
    getCSSVar(name) {
        return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    }
};

// Apply theme immediately (before DOM content loaded) to prevent flash
ThemeManager.init();
window.ThemeManager = ThemeManager;

// User preferences (localStorage-backed)
const UserPrefs = {
    SHOW_SQL_KEY: 'bifract-show-sql',

    showSQL() {
        return localStorage.getItem(this.SHOW_SQL_KEY) === 'true';
    },

    toggleShowSQL() {
        const newVal = !this.showSQL();
        localStorage.setItem(this.SHOW_SQL_KEY, String(newVal));
        document.querySelectorAll('.sql-preview').forEach(el => {
            el.style.display = newVal ? '' : 'none';
        });
        return newVal;
    },

    apply() {
        if (!this.showSQL()) {
            document.querySelectorAll('.sql-preview').forEach(el => {
                el.style.display = 'none';
            });
        }
    }
};
window.UserPrefs = UserPrefs;

// Global 401 interceptor: redirect to login when session is invalid
(function() {
    const originalFetch = window.fetch;
    window.fetch = function(input, init) {
        return originalFetch.apply(this, arguments).then(function(response) {
            if (response.status === 401 && window.location.pathname !== '/login.html') {
                const url = typeof input === 'string' ? input : (input && input.url) || '';
                // Don't redirect on auth endpoints (they handle 401 themselves)
                if (!url.includes('/api/v1/auth/')) {
                    window.location.href = '/login.html';
                }
            }
            return response;
        });
    };
})();

// Authentication module for Bifract
const Auth = {
    currentUser: null,
    loginModal: null,

    async init() {
        console.log('[Auth] Initializing auth module...');
        await this.checkCurrentUser();
        // Close dropdown on outside click
        document.addEventListener('click', (e) => {
            const menu = document.getElementById('userMenuDropdown');
            const trigger = document.getElementById('userClickable');
            if (menu && trigger && !trigger.contains(e.target) && !menu.contains(e.target)) {
                menu.classList.remove('open');
            }
        });
        console.log('[Auth] Auth module initialization complete');
    },

    async logout() {
        try {
            const response = await fetch('/api/v1/auth/logout', {
                method: 'POST',
                credentials: 'include'
            });

            const data = await response.json();
            if (data.success) {
                this.currentUser = null;
                window.location.href = '/login.html';
            }
        } catch (error) {
            console.error('Logout error:', error);
        }
    },

    async checkCurrentUser() {
        try {
            const response = await fetch('/api/v1/auth/user', {
                credentials: 'include'
            });

            if (response.ok) {
                const data = await response.json();
                if (data.success) {
                    if (data.user && data.user.force_password_change) {
                        window.location.href = '/login.html';
                        return;
                    }
                    this.currentUser = data.user;
                    this.showLoggedInUI();
                    return;
                }
            }
        } catch (error) {
            // User not logged in
        }

        this.showLoggedOutUI();
    },

    showLoggedInUI() {
        const userInfo = document.getElementById('userInfo');
        if (userInfo && this.currentUser) {
            let roleText = 'No Access';
            if (this.currentUser.is_admin) {
                roleText = 'Tenant Admin';
            } else {
                const fr = this.currentUser.fractal_role || '';
                if (fr === 'admin') roleText = 'Fractal Admin';
                else if (fr === 'analyst') roleText = 'Analyst';
                else if (fr === 'viewer') roleText = 'Viewer';
            }
            const themeLabel = ThemeManager.isDark() ? 'Light Mode' : 'Dark Mode';
            const sqlLabel = UserPrefs.showSQL() ? 'Hide Generated SQL' : 'Show Generated SQL';
            userInfo.innerHTML = `
                <div class="user-display" id="userDisplayContainer">
                    <div class="user-clickable" id="userClickable" onclick="Auth.toggleMenu()">
                        <div class="gravatar" style="background-color: ${this.currentUser.gravatar_color}">
                            ${this.currentUser.gravatar_initial}
                        </div>
                        <div class="user-info-text">
                            <span class="username">${this.currentUser.display_name}</span>
                            <span class="user-role">${roleText}</span>
                        </div>
                    </div>
                    <div class="user-menu-dropdown" id="userMenuDropdown">
                        <button class="user-menu-item" onclick="ThemeManager.toggle(); Auth.updateThemeLabel();">
                            <svg id="themeToggleIcon" viewBox="0 0 24 24" width="16" height="16"></svg>
                            <span id="themeToggleLabel">${themeLabel}</span>
                        </button>
                        <button class="user-menu-item" onclick="UserPrefs.toggleShowSQL(); Auth.updateSQLLabel();">
                            <svg id="sqlToggleIcon" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>
                            <span id="sqlToggleLabel">${sqlLabel}</span>
                        </button>
                        <button class="user-menu-item" onclick="Auth.showChangePassword()">
                            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
                            <span>Change Password</span>
                        </button>
                        <div class="user-menu-divider"></div>
                        <button class="user-menu-item logout-item" onclick="Auth.logout()">
                            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
                            <span>Logout</span>
                        </button>
                    </div>
                </div>
            `;
            ThemeManager.updateIcon();
        }

        if (window.App) {
            App.routeFromHash();
        }

        if (this.currentUser && this.currentUser.is_admin) {
            console.log('[Auth] User is admin, showing admin-only elements');
            document.querySelectorAll('.admin-only').forEach(el => {
                console.log('[Auth] Found admin-only element:', el.id, el.tagName);
                if (el.id === 'settingsView') {
                    console.log('[Auth] Skipping settingsView');
                    return;
                }
                el.classList.remove('admin-only');
                el.removeAttribute('style');
                console.log('[Auth] Showed element:', el.id);
            });

            if (window.UserManagement) {
                window.UserManagement.loadUsers();
            }
        }

        // Update fractal-role-based visibility
        this.updateRBACVisibility();

        const loginButton = document.getElementById('loginButton');
        if (loginButton) {
            loginButton.style.display = 'none';
        }
    },

    showLoggedOutUI() {
        if (window.location.pathname !== '/login.html') {
            window.location.href = '/login.html';
        }
    },

    isAuthenticated() {
        return this.currentUser !== null;
    },

    getCurrentUser() {
        return this.currentUser;
    },

    // RBAC helpers
    getFractalRole() {
        return this.currentUser ? (this.currentUser.fractal_role || '') : '';
    },

    hasFractalRole(minRole) {
        if (!this.currentUser) return false;
        if (this.currentUser.is_admin) return true;
        const weights = { viewer: 1, analyst: 2, admin: 3 };
        return (weights[this.getFractalRole()] || 0) >= (weights[minRole] || 0);
    },

    updateRBACVisibility() {
        // Toggle rbac-hidden class based on fractal role.
        // Using a class instead of inline display avoids clobbering
        // tab-switching logic that also controls display.
        const isAdmin = this.hasFractalRole('admin');
        document.querySelectorAll('.fractal-admin-only').forEach(el => {
            el.classList.toggle('rbac-hidden', !isAdmin);
        });
        const isAnalyst = this.hasFractalRole('analyst');
        document.querySelectorAll('.fractal-analyst-only').forEach(el => {
            el.classList.toggle('rbac-hidden', !isAnalyst);
        });
    },

    toggleMenu() {
        const menu = document.getElementById('userMenuDropdown');
        if (menu) {
            menu.classList.toggle('open');
        }
    },

    updateThemeLabel() {
        const label = document.getElementById('themeToggleLabel');
        if (label) {
            label.textContent = ThemeManager.isDark() ? 'Light Mode' : 'Dark Mode';
        }
    },

    updateSQLLabel() {
        const label = document.getElementById('sqlToggleLabel');
        if (label) {
            label.textContent = UserPrefs.showSQL() ? 'Hide Generated SQL' : 'Show Generated SQL';
        }
    },

    showError(errorDiv, message) {
        if (errorDiv) {
            errorDiv.textContent = message;
            errorDiv.style.display = 'block';
        }
        console.error('[Auth] Error:', message);
    },

    showChangePassword() {
        // Close the user menu
        const menu = document.getElementById('userMenuDropdown');
        if (menu) menu.classList.remove('open');

        // Remove existing modal if present
        let modal = document.getElementById('changePasswordModal');
        if (modal) modal.remove();

        modal = document.createElement('div');
        modal.id = 'changePasswordModal';
        modal.className = 'modal';
        modal.style.display = 'flex';
        modal.innerHTML = `
            <div class="modal-content modal-sm">
                <div class="modal-header">
                    <h3>Change Password</h3>
                    <button class="close-panel-btn" onclick="Auth.closeChangePassword()">&times;</button>
                </div>
                <div class="modal-body">
                    <div class="form-group">
                        <label for="cpCurrentPassword">Current Password</label>
                        <input type="password" id="cpCurrentPassword" placeholder="Enter current password">
                    </div>
                    <div class="form-group">
                        <label for="cpNewPassword">New Password</label>
                        <input type="password" id="cpNewPassword" placeholder="Enter new password">
                        <p class="help-text">Minimum 12 characters.</p>
                    </div>
                    <div class="form-group">
                        <label for="cpConfirmPassword">Confirm New Password</label>
                        <input type="password" id="cpConfirmPassword" placeholder="Confirm new password">
                    </div>
                    <div id="cpError" class="error-message" style="display: none;"></div>
                    <div class="form-actions">
                        <button id="cpSubmitBtn" class="btn-primary" onclick="Auth.submitChangePassword()">Change Password</button>
                        <button class="btn-secondary" onclick="Auth.closeChangePassword()">Cancel</button>
                    </div>
                </div>
            </div>
        `;
        document.body.appendChild(modal);

        // Close on backdrop click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) Auth.closeChangePassword();
        });

        document.getElementById('cpCurrentPassword').focus();
    },

    closeChangePassword() {
        const modal = document.getElementById('changePasswordModal');
        if (modal) modal.remove();
    },

    async submitChangePassword() {
        const currentPassword = document.getElementById('cpCurrentPassword').value;
        const newPassword = document.getElementById('cpNewPassword').value;
        const confirmPassword = document.getElementById('cpConfirmPassword').value;
        const errorDiv = document.getElementById('cpError');
        const submitBtn = document.getElementById('cpSubmitBtn');

        errorDiv.style.display = 'none';
        errorDiv.textContent = '';

        if (!currentPassword || !newPassword || !confirmPassword) {
            errorDiv.textContent = 'All fields are required';
            errorDiv.style.display = 'block';
            return;
        }

        if (newPassword !== confirmPassword) {
            errorDiv.textContent = 'New passwords do not match';
            errorDiv.style.display = 'block';
            return;
        }

        if (newPassword.length < 12) {
            errorDiv.textContent = 'New password must be at least 12 characters';
            errorDiv.style.display = 'block';
            return;
        }

        submitBtn.disabled = true;
        submitBtn.textContent = 'Changing...';

        try {
            const response = await fetch('/api/v1/auth/change-password', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({
                    current_password: currentPassword,
                    new_password: newPassword
                })
            });

            const data = await response.json();

            if (data.success) {
                this.closeChangePassword();
                if (window.Toast) {
                    Toast.success('Password Changed', 'Your password has been updated successfully');
                } else {
                    alert('Password changed successfully.');
                }
            } else {
                errorDiv.textContent = data.error || 'Failed to change password';
                errorDiv.style.display = 'block';
            }
        } catch (error) {
            console.error('[Auth] Change password error:', error);
            errorDiv.textContent = 'Network error. Please try again.';
            errorDiv.style.display = 'block';
        } finally {
            submitBtn.disabled = false;
            submitBtn.textContent = 'Change Password';
        }
    },

    testLogin() {
        console.log('[Auth] Testing login modal...');
        console.log('[Auth] Modal exists:', !!this.loginModal);
        console.log('[Auth] Modal visible:', this.loginModal && this.loginModal.classList.contains('show'));

        const usernameField = document.getElementById('loginUsername');
        const passwordField = document.getElementById('loginPassword');

        console.log('[Auth] Form elements:', {
            username: !!usernameField,
            password: !!passwordField,
            usernameValue: usernameField ? `"${usernameField.value}"` : 'N/A',
            passwordValue: passwordField ? `"${passwordField.value}"` : 'N/A'
        });

        if (usernameField) {
            usernameField.value = 'admin';
            passwordField.value = 'bifract';
            console.log('[Auth] Set test values - try submitting now');
        }
    }
};

window.Auth = Auth;

document.addEventListener('DOMContentLoaded', () => {
    Auth.init();
});
