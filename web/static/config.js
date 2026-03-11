// Bifract Configuration
const BifractConfig = {
    version: "v0.0.1",

    // Other configuration options can be added here
    app: {
        name: "Bifract",
        pollInterval: 30000, // 30 seconds for real-time notifications
        defaultPageSize: 50
    },

    timeouts: {
        queryTimeout: 30000, // 30 seconds
        toastDuration: 4000  // 4 seconds
    }
};

// Make globally available
window.BifractConfig = BifractConfig;

// Initialize version display when DOM is loaded
document.addEventListener('DOMContentLoaded', async () => {
    const versionElement = document.getElementById('bifractVersion');
    if (!versionElement) return;

    try {
        const resp = await fetch('/api/v1/version');
        if (resp.ok) {
            const data = await resp.json();
            if (data.version) {
                BifractConfig.version = data.version;
            }
        }
    } catch (e) {
        // Fall back to hardcoded version
    }
    versionElement.textContent = `Bifract version: ${BifractConfig.version}`;
});