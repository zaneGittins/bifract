// Self-service two-factor management. The forced enrollment flow lives on the
// login page; this is the voluntary path from the user menu.

const MFA = {
    status: null,
    enrollment: null,
    recoveryCodes: [],

    async open() {
        this.close();

        const modal = document.createElement('div');
        modal.id = 'mfaModal';
        modal.className = 'modal';
        modal.style.display = 'flex';
        modal.innerHTML = `
            <div class="modal-content modal-sm">
                <div class="modal-header">
                    <h3>Two-Factor Authentication</h3>
                    <button class="close-panel-btn" onclick="MFA.close()">&times;</button>
                </div>
                <div class="modal-body" id="mfaModalBody">
                    <div class="mfa-loading">Loading...</div>
                </div>
            </div>
        `;
        document.body.appendChild(modal);
        modal.addEventListener('click', (e) => {
            if (e.target === modal) MFA.close();
        });

        await this.loadStatus();
    },

    close() {
        document.getElementById('mfaModal')?.remove();
        this.enrollment = null;
        this.recoveryCodes = [];
    },

    setBody(html) {
        const el = document.getElementById('mfaModalBody');
        if (el) el.innerHTML = html;
    },

    async loadStatus() {
        try {
            const response = await fetch('/api/v1/auth/mfa/status', { credentials: 'include' });
            const data = await response.json();
            this.status = (data.success && data.data) || null;
        } catch (error) {
            console.error('[MFA] status error:', error);
            this.status = null;
        }
        this.render();
    },

    render() {
        if (!this.status) {
            this.setBody('<div class="mfa-note">Could not load your two-factor status.</div>');
            return;
        }

        if (this.recoveryCodes.length) {
            this.renderRecoveryCodes();
            return;
        }
        if (this.enrollment) {
            this.renderEnrollment();
            return;
        }
        if (this.status.enrolled) {
            this.renderEnrolled();
            return;
        }
        this.renderNotEnrolled();
    },

    renderNotEnrolled() {
        const unavailable = !this.status.available;
        this.setBody(`
            <p class="mfa-note">
                An authenticator app generates a short code that changes every 30 seconds.
                Bifract asks for it after your password, so a stolen password is not enough on its own.
            </p>
            ${unavailable ? `
                <div class="mfa-blocked">
                    Two-factor authentication is unavailable: this deployment has no
                    <code>BIFRACT_PASSWORD_PEPPER</code> set, so enrollment secrets could not be encrypted at rest.
                </div>
            ` : ''}
            <div id="mfaError" class="error-message" style="display: none;"></div>
            <div class="form-actions">
                <button class="btn-primary" id="mfaStartBtn" onclick="MFA.startEnrollment()" ${unavailable ? 'disabled' : ''}>Set Up Authenticator</button>
                <button class="btn-secondary" onclick="MFA.close()">Close</button>
            </div>
        `);
    },

    renderEnrolled() {
        const remaining = this.status.recovery_codes_remaining;
        const low = remaining <= 2;
        this.setBody(`
            <div class="mfa-state-on">
                <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg>
                <span>Your account is protected by an authenticator app.</span>
            </div>
            <p class="mfa-note ${low ? 'mfa-warn' : ''}">
                ${remaining} recovery ${remaining === 1 ? 'code' : 'codes'} remaining.
                ${low ? 'Generate a new set so you are not locked out.' : ''}
            </p>
            ${this.status.required ? `
                <p class="mfa-note">Your administrator requires two-factor authentication, so it cannot be removed.</p>
            ` : ''}
            <div id="mfaError" class="error-message" style="display: none;"></div>
            <div class="form-group">
                <label for="mfaPassword">Confirm your password to make changes</label>
                <input type="password" id="mfaPassword" placeholder="Current password" autocomplete="current-password">
            </div>
            ${this.status.required ? '' : `
                <div class="form-group">
                    <label for="mfaDisableCode">Verification code</label>
                    <input type="text" id="mfaDisableCode" placeholder="000000" inputmode="numeric" autocomplete="one-time-code" maxlength="6">
                </div>
            `}
            <div class="form-actions">
                <button class="btn-primary" id="mfaRegenBtn" onclick="MFA.regenerateCodes()">New Recovery Codes</button>
                ${this.status.required ? '' : `<button class="btn-danger" id="mfaDisableBtn" onclick="MFA.disable()">Remove</button>`}
                <button class="btn-secondary" onclick="MFA.close()">Close</button>
            </div>
        `);
    },

    renderEnrollment() {
        this.setBody(`
            <p class="mfa-note">Scan this with Google Authenticator, 1Password, Authy, or any TOTP app.</p>
            <div class="mfa-qr" id="mfaQRBox"></div>
            <div class="mfa-secret" id="mfaSecretBox" title="Click to copy">${Utils.escapeHtml(this.enrollment.secret)}</div>
            <p class="mfa-hint">Can't scan? Click the key to copy it and enter it by hand.</p>
            <div id="mfaError" class="error-message" style="display: none;"></div>
            <div class="form-group">
                <label for="mfaConfirmCode">Verification code</label>
                <input type="text" id="mfaConfirmCode" placeholder="000000" inputmode="numeric" autocomplete="one-time-code" maxlength="6">
            </div>
            <div class="form-actions">
                <button class="btn-primary" id="mfaConfirmBtn" onclick="MFA.confirm()">Confirm</button>
                <button class="btn-secondary" onclick="MFA.cancelEnrollment()">Cancel</button>
            </div>
        `);

        const qr = document.getElementById('mfaQRBox');
        if (this.enrollment.qr_svg) {
            // Generated by us; the payload is encoded as modules, not as text.
            qr.innerHTML = this.enrollment.qr_svg;
        } else {
            qr.style.display = 'none';
        }
        document.getElementById('mfaSecretBox').addEventListener('click', (e) => {
            navigator.clipboard?.writeText(e.target.textContent.trim());
            if (window.Toast) Toast.success('Copied', 'Setup key copied to the clipboard');
        });
        document.getElementById('mfaConfirmCode').focus();
    },

    renderRecoveryCodes() {
        this.setBody(`
            <p class="mfa-note">
                Save these somewhere safe. They are the only way back in if you lose your device,
                each one works once, and they are not shown again.
            </p>
            <div class="mfa-codes">${this.recoveryCodes.map(c => `<span>${Utils.escapeHtml(c)}</span>`).join('')}</div>
            <div class="form-actions">
                <button class="btn-primary" onclick="MFA.finishRecoveryCodes()">I have saved these codes</button>
                <button class="btn-secondary" onclick="MFA.copyRecoveryCodes()">Copy All</button>
            </div>
        `);
    },

    copyRecoveryCodes() {
        navigator.clipboard?.writeText(this.recoveryCodes.join('\n'));
        if (window.Toast) Toast.success('Copied', 'Recovery codes copied to the clipboard');
    },

    async finishRecoveryCodes() {
        this.recoveryCodes = [];
        this.enrollment = null;
        await this.loadStatus();
    },

    cancelEnrollment() {
        this.enrollment = null;
        this.render();
    },

    error(message) {
        const el = document.getElementById('mfaError');
        if (!el) return;
        el.textContent = message;
        el.style.display = 'block';
    },

    async startEnrollment() {
        const btn = document.getElementById('mfaStartBtn');
        if (btn) btn.disabled = true;
        try {
            const response = await fetch('/api/v1/auth/mfa/enroll', {
                method: 'POST',
                credentials: 'include'
            });
            const data = await response.json();
            if (!data.success) {
                this.error(data.error || 'Could not start enrollment');
                return;
            }
            this.enrollment = data.data;
            this.render();
        } catch (error) {
            console.error('[MFA] enroll error:', error);
            this.error('Network error. Please try again.');
        } finally {
            if (btn) btn.disabled = false;
        }
    },

    async confirm() {
        const code = document.getElementById('mfaConfirmCode')?.value.trim();
        if (!code) {
            this.error('Enter the code from your authenticator');
            return;
        }
        const btn = document.getElementById('mfaConfirmBtn');
        btn.disabled = true;
        try {
            const response = await fetch('/api/v1/auth/mfa/confirm', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ code })
            });
            const data = await response.json();
            if (!data.success) {
                this.error(data.error || 'Could not confirm the code');
                return;
            }
            this.recoveryCodes = (data.data && data.data.recovery_codes) || [];
            this.enrollment = null;
            if (window.Toast) Toast.success('Two-Factor Enabled', 'Your account now asks for a code at sign in');
            this.render();
        } catch (error) {
            console.error('[MFA] confirm error:', error);
            this.error('Network error. Please try again.');
        } finally {
            btn.disabled = false;
        }
    },

    async regenerateCodes() {
        const password = document.getElementById('mfaPassword')?.value;
        if (!password) {
            this.error('Confirm your password first');
            return;
        }
        const btn = document.getElementById('mfaRegenBtn');
        btn.disabled = true;
        try {
            const response = await fetch('/api/v1/auth/mfa/recovery-codes', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ password })
            });
            const data = await response.json();
            if (!data.success) {
                this.error(data.error || 'Could not generate recovery codes');
                return;
            }
            this.recoveryCodes = (data.data && data.data.recovery_codes) || [];
            this.render();
        } catch (error) {
            console.error('[MFA] recovery codes error:', error);
            this.error('Network error. Please try again.');
        } finally {
            btn.disabled = false;
        }
    },

    async disable() {
        const password = document.getElementById('mfaPassword')?.value;
        const code = document.getElementById('mfaDisableCode')?.value.trim();
        if (!password || !code) {
            this.error('Enter your password and a current code');
            return;
        }
        if (!confirm('Remove two-factor authentication from your account?')) return;

        const btn = document.getElementById('mfaDisableBtn');
        btn.disabled = true;
        try {
            const response = await fetch('/api/v1/auth/mfa/disable', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ password, code })
            });
            const data = await response.json();
            if (!data.success) {
                this.error(data.error || 'Could not remove the authenticator');
                return;
            }
            if (window.Toast) Toast.success('Two-Factor Removed', 'Your account no longer asks for a code');
            await this.loadStatus();
        } catch (error) {
            console.error('[MFA] disable error:', error);
            this.error('Network error. Please try again.');
        } finally {
            btn.disabled = false;
        }
    }
};

window.MFA = MFA;
