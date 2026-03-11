const Settings = {
    currentSettings: {},

    init() {
        this.setupEventListeners();
        this.loadSettings();
    },

    setupEventListeners() {
        const settingsBtn = document.getElementById('settingsBtn');
        const settingsModal = document.getElementById('settingsModal');
        const closeSettingsBtn = document.getElementById('closeSettingsBtn');
        const saveSettingsBtn = document.getElementById('saveSettingsBtn');

        if (settingsBtn) {
            settingsBtn.addEventListener('click', () => {
                this.showModal();
            });
        }

        if (closeSettingsBtn) {
            closeSettingsBtn.addEventListener('click', () => {
                this.hideModal();
            });
        }

        if (saveSettingsBtn) {
            saveSettingsBtn.addEventListener('click', () => {
                this.saveSettings();
            });
        }

        // Close modal when clicking outside
        if (settingsModal) {
            settingsModal.addEventListener('click', (e) => {
                if (e.target === settingsModal) {
                    this.hideModal();
                }
            });
        }
    },

    async loadSettings() {
        try {
            const response = await fetch('/api/v1/settings');
            const data = await response.json();

            if (data.success && data.settings) {
                this.currentSettings = data.settings;
                this.updateUI();
            }
        } catch (error) {
            console.error('Failed to load settings:', error);
        }
    },

    async saveSettings() {
        try {
            const response = await fetch('/api/v1/settings', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(this.currentSettings)
            });

            const data = await response.json();

            if (data.success) {
                Utils.showNotification('Settings saved successfully', 'success');
                this.hideModal();
            } else {
                Utils.showNotification('Failed to save settings: ' + (data.error || 'Unknown error'), 'error');
            }
        } catch (error) {
            console.error('Failed to save settings:', error);
            Utils.showNotification('Failed to save settings', 'error');
        }
    },

    updateUI() {
    },

    showModal() {
        const modal = document.getElementById('settingsModal');
        if (modal) {
            modal.style.display = 'flex';
            this.updateUI();
        }
    },

    hideModal() {
        const modal = document.getElementById('settingsModal');
        if (modal) {
            modal.style.display = 'none';
        }
    }
};

// Export to window
window.Settings = Settings;
