// Time bar and relative timestamps
const TimeBar = {
    intervalId: null,

    init() {
        this.updateClock();
        this.intervalId = setInterval(() => this.updateClock(), 1000);
    },

    updateClock() {
        const element = document.getElementById('currentTimeUTC');
        if (element) {
            element.textContent = 'UTC: ' + Utils.getCurrentUTC();
        }
    },

    updateFractalName(fractalName) {
        const element = document.getElementById('currentFractalName');
        if (element) {
            element.textContent = fractalName || 'No fractal selected';
        }
    },

    destroy() {
        if (this.intervalId) {
            clearInterval(this.intervalId);
        }
    }
};

// Make globally available
window.TimeBar = TimeBar;
