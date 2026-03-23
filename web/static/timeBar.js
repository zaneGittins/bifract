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

    addRelativeTimeTooltips() {
        const timestampCells = document.querySelectorAll('.timestamp-cell');
        timestampCells.forEach(cell => {
            const timestamp = cell.textContent.trim();
            if (timestamp && timestamp !== '-') {
                const relativeTime = Utils.getRelativeTime(timestamp);
                cell.title = relativeTime;

                // Add hover tooltip
                cell.addEventListener('mouseenter', function(e) {
                    const tooltip = document.createElement('div');
                    tooltip.className = 'timestamp-tooltip';
                    tooltip.textContent = relativeTime;
                    this.appendChild(tooltip);
                });

                cell.addEventListener('mouseleave', function() {
                    const tooltip = this.querySelector('.timestamp-tooltip');
                    if (tooltip) {
                        tooltip.remove();
                    }
                });
            }
        });
    },

    destroy() {
        if (this.intervalId) {
            clearInterval(this.intervalId);
        }
    }
};

// Make globally available
window.TimeBar = TimeBar;
