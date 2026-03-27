// Professional Toast Notification System
const Toast = {
    container: null,
    toastId: 0,

    init() {
        // Create toast container if it doesn't exist
        if (!this.container) {
            this.container = document.createElement('div');
            this.container.className = 'toast-container';
            document.body.appendChild(this.container);
        }
    },

    show(type, title, message, options = {}) {
        this.init();

        // Support legacy (message, type) calling convention
        const validTypes = ['success', 'error', 'warning', 'info'];
        if (!validTypes.includes(type) && validTypes.includes(title)) {
            const actualType = title;
            const actualTitle = type;
            type = actualType;
            title = actualTitle;
            message = undefined;
        }

        const defaultOptions = {
            duration: type === 'error' ? 8000 : 6000,
            closable: true,
            showProgress: true
        };

        const config = { ...defaultOptions, ...options };
        const toastId = ++this.toastId;

        // Create toast element
        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.dataset.toastId = toastId;

        // Get icon based on type
        const icon = this.getIcon(type);

        // Build toast HTML
        toast.innerHTML = `
            <div class="toast-icon">${icon}</div>
            <div class="toast-content">
                <div class="toast-title">${Utils.escapeHtml(title)}</div>
                ${message ? `<div class="toast-message">${Utils.escapeHtml(message)}</div>` : ''}
            </div>
            ${config.closable ? '<button class="toast-close" title="Close">&times;</button>' : ''}
            ${config.showProgress ? '<div class="toast-progress"></div>' : ''}
        `;

        // Add to container
        this.container.appendChild(toast);

        // Click handler for toast content (if callback provided)
        if (config.onClick && typeof config.onClick === 'function') {
            toast.style.cursor = 'pointer';
            toast.addEventListener('click', (e) => {
                // Don't trigger on close button click
                if (e.target.classList.contains('toast-close')) return;
                config.onClick();
                this.hide(toastId);
            });
        }

        // Close button handler
        if (config.closable) {
            const closeBtn = toast.querySelector('.toast-close');
            closeBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.hide(toastId);
            });
        }

        // Show with animation
        requestAnimationFrame(() => {
            toast.classList.add('show');
        });

        // Auto hide with progress bar
        if (config.duration > 0) {
            let timeRemaining = config.duration;
            const progressBar = toast.querySelector('.toast-progress');

            if (progressBar && config.showProgress) {
                progressBar.style.width = '100%';
                progressBar.style.transitionDuration = `${config.duration}ms`;
                requestAnimationFrame(() => {
                    progressBar.style.width = '0%';
                });
            }

            const hideTimer = setTimeout(() => {
                this.hide(toastId);
            }, config.duration);

            // Pause on hover
            toast.addEventListener('mouseenter', () => {
                clearTimeout(hideTimer);
                if (progressBar) {
                    const currentWidth = progressBar.offsetWidth;
                    const totalWidth = progressBar.parentElement.offsetWidth;
                    progressBar.style.width = `${(currentWidth / totalWidth) * 100}%`;
                    progressBar.style.transitionDuration = '0ms';
                }
            });

            toast.addEventListener('mouseleave', () => {
                if (progressBar) {
                    const currentWidth = progressBar.offsetWidth;
                    const totalWidth = progressBar.parentElement.offsetWidth;
                    const remainingPercent = (currentWidth / totalWidth) * 100;
                    const remainingTime = (remainingPercent / 100) * config.duration;

                    if (remainingTime > 0) {
                        progressBar.style.transitionDuration = `${remainingTime}ms`;
                        progressBar.style.width = '0%';

                        setTimeout(() => {
                            this.hide(toastId);
                        }, remainingTime);
                    } else {
                        this.hide(toastId);
                    }
                }
            });
        }

        return toastId;
    },

    hide(toastId) {
        const toast = this.container?.querySelector(`[data-toast-id="${toastId}"]`);
        if (!toast) return;

        toast.classList.add('hide');
        toast.classList.remove('show');

        setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, 300);
    },

    hideAll() {
        if (!this.container) return;

        const toasts = this.container.querySelectorAll('.toast');
        toasts.forEach(toast => {
            const toastId = parseInt(toast.dataset.toastId);
            this.hide(toastId);
        });
    },

    // Convenience methods
    success(title, message, options) {
        return this.show('success', title, message, options);
    },

    error(title, message, options) {
        return this.show('error', title, message, options);
    },

    warning(title, message, options) {
        return this.show('warning', title, message, options);
    },

    info(title, message, durationOrOptions, onClick) {
        // Support both old and new API
        let options = {};
        if (typeof durationOrOptions === 'number') {
            // Legacy API: info(title, message, duration, onClick)
            options.duration = durationOrOptions;
            if (onClick) options.onClick = onClick;
        } else if (typeof durationOrOptions === 'function') {
            // New API: info(title, message, onClick)
            options.onClick = durationOrOptions;
        } else if (typeof durationOrOptions === 'object') {
            // New API: info(title, message, options)
            options = durationOrOptions || {};
        }
        return this.show('info', title, message, options);
    },

    // Get appropriate icon for toast type
    getIcon(type) {
        const icons = {
            success: '✓',
            error: '✕',
            warning: '⚠',
            info: 'ℹ'
        };
        return icons[type] || 'ℹ';
    },

};

// Make globally available
window.Toast = Toast;

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => {
    Toast.init();
});