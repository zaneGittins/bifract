// Real-time Comment Notifications module

const RealTimeComments = {
    knownCommentIds: new Set(),
    pollInterval: null,
    pollIntervalMs: 5000, // 5 seconds
    isEnabled: false,
    initialLoadCompleted: false,

    init() {
        // Use a delayed check for authentication since Auth module might not be fully loaded yet
        const checkAuthAndStart = async () => {
            if (window.Auth && window.Auth.isAuthenticated()) {
                // Load initial comments FIRST to avoid showing old comments as "new"
                await this.loadInitialComments();
                // Wait a bit after initial load before enabling notifications
                setTimeout(() => {
                    this.initialLoadCompleted = true;
                    this.start();
                }, 2000); // 2 second delay to ensure we don't show toasts for existing comments
                return true;
            }
            return false;
        };

        // Try immediately
        checkAuthAndStart().then(success => {
            if (!success) {
                // Try again after a short delay (Auth module might still be loading)
                setTimeout(async () => {
                    const secondAttempt = await checkAuthAndStart();
                }, 1000);
            }
        });

        // Listen for authentication changes
        document.addEventListener('auth:login', async () => {
            // Reset flags on login
            this.initialLoadCompleted = false;
            this.knownCommentIds.clear();

            // Load initial comments FIRST to avoid showing old comments as "new" on login
            await this.loadInitialComments();
            // Wait before enabling notifications
            setTimeout(() => {
                this.initialLoadCompleted = true;
                this.start();
            }, 2000);
        });
        document.addEventListener('auth:logout', () => {
            this.stop();
            this.initialLoadCompleted = false;
            this.knownCommentIds.clear();
        });
    },

    async loadInitialComments() {
        if (!window.Auth || !window.Auth.isAuthenticated()) {
            return;
        }

        try {
            // Load initial comments to populate known set (without notifications)
            const response = await fetch('/api/v1/logs/commented?limit=1000', {
                credentials: 'include'
            });

            const data = await response.json();
            if (data.success && data.data) {
                // Extract all comment IDs from the initial load
                data.data.forEach(log => {
                    if (log.comments_json) {
                        try {
                            const comments = JSON.parse(log.comments_json);
                            comments.forEach(comment => {
                                if (comment.id) {
                                    this.knownCommentIds.add(comment.id);
                                }
                            });
                        } catch (e) {
                            // Ignore parse errors for initial load
                        }
                    }
                });
            }
        } catch (error) {
            console.error('[RealTimeComments] Failed to load initial comments:', error);
        }
    },

    start() {
        if (this.isEnabled) return;

        this.isEnabled = true;

        // Poll immediately, then at intervals
        this.checkForNewComments();
        this.pollInterval = setInterval(() => this.checkForNewComments(), this.pollIntervalMs);
    },

    stop() {
        if (!this.isEnabled) return;

        this.isEnabled = false;

        if (this.pollInterval) {
            clearInterval(this.pollInterval);
            this.pollInterval = null;
        }
    },

    async checkForNewComments() {
        if (!window.Auth || !window.Auth.isAuthenticated()) {
            this.stop();
            return;
        }

        try {
            // Get current fractal context if available
            let url = '/api/v1/logs/commented?limit=100'; // Only check recent comments
            if (window.FractalContext && window.FractalContext.currentFractal) {
                url += `&fractal_id=${window.FractalContext.currentFractal.id}`;
            }

            const response = await fetch(url, {
                credentials: 'include'
            });

            const data = await response.json();

            if (data.success && data.data) {
                this.processComments(data.data);
            }
        } catch (error) {
            // Continue polling even if there's an error
        }
    },

    processComments(logs) {
        // Don't show notifications if initial load hasn't completed
        if (!this.initialLoadCompleted) {
            // Still process comments to add them to known set, but don't notify
            logs.forEach(log => {
                if (log.comments_json) {
                    try {
                        const comments = JSON.parse(log.comments_json);
                        comments.forEach(comment => {
                            if (comment.id && !this.knownCommentIds.has(comment.id)) {
                                this.knownCommentIds.add(comment.id);
                            }
                        });
                    } catch (e) {
                        // Ignore parse errors
                    }
                }
            });
            return;
        }

        const newComments = [];
        const currentUser = window.Auth?.getCurrentUser?.();
        const currentUsername = currentUser?.username || null;

        logs.forEach(log => {
            if (log.comments_json) {
                try {
                    const comments = JSON.parse(log.comments_json);
                    comments.forEach(comment => {
                        if (comment.id && !this.knownCommentIds.has(comment.id)) {
                            // This is a new comment!
                            this.knownCommentIds.add(comment.id);

                            // Only notify if the comment is NOT from the current user
                            if (!currentUsername || comment.author !== currentUsername) {
                                newComments.push({
                                    comment: comment,
                                    log: log
                                });
                            }
                        }
                    });
                } catch (e) {
                    // Ignore parse errors
                }
            }
        });

        // Show notifications for new comments (excluding own comments)
        if (newComments.length > 0) {
            this.showNotifications(newComments);
        }
    },

    showNotifications(newComments) {
        if (!window.Toast) {
            return;
        }

        // Group notifications to avoid spam
        if (newComments.length === 1) {
            const { comment, log } = newComments[0];
            const truncatedText = comment.text.length > 50
                ? comment.text.substring(0, 50) + '...'
                : comment.text;

            Toast.info(
                'New Comment',
                `${comment.author}: ${truncatedText}`,
                {
                    duration: 8000, // Show for 8 seconds
                    onClick: () => this.navigateToComment(log, comment)
                }
            );
        } else {
            // Multiple new comments
            const fractalName = window.FractalContext?.currentFractal?.name || 'current fractal';

            Toast.info(
                'New Comments',
                `${newComments.length} new comments in ${fractalName}`,
                {
                    duration: 8000,
                    onClick: () => this.navigateToComments()
                }
            );
        }
    },

    navigateToComment(log, comment) {
        // If we're already in fractal view, switch to comments tab
        if (window.App && window.App.currentViewLevel === 'fractal') {
            window.App.showFractalViewTab('comments');
            // Try to highlight the specific log if possible
            if (window.CommentedLogs) {
                window.CommentedLogs.show();
            }
            return;
        }

        // If we have fractal context, navigate to that fractal's comments
        if (window.FractalContext && window.App) {
            // Show the fractal view with comments tab
            window.App.showFractalView('comments');
        }
    },

    navigateToComments() {
        // If we're already in fractal view, switch to comments tab
        if (window.App && window.App.currentViewLevel === 'fractal') {
            window.App.showFractalViewTab('comments');
        } else if (window.App) {
            // Navigate to fractal view with comments
            window.App.showFractalView('comments');
        }
    },

    // Manual test method for debugging
    testNotification() {
        if (window.Toast) {
            Toast.info(
                'Test Notification',
                'This is a test of the real-time comment system',
                {
                    duration: 5000,
                    onClick: () => {}
                }
            );
        }
    },

    // Debug method to check polling status
    getStatus() {
        return {
            isEnabled: this.isEnabled,
            pollInterval: this.pollInterval !== null,
            knownCommentIds: this.knownCommentIds.size,
            authenticated: window.Auth?.isAuthenticated() || false
        };
    },

    // Force start for debugging
    forceStart() {
        this.start();
    },

    // Complete reset and restart
    reset() {
        this.stop();
        this.knownCommentIds.clear();
        this.initialLoadCompleted = false;
        this.loadInitialComments().then(() => {
            setTimeout(() => {
                this.initialLoadCompleted = true;
                this.start();
            }, 2000);
        });
    },

    // Debug method to check all dependencies
    checkDependencies() {
        const currentUser = window.Auth?.getCurrentUser?.();
        const currentUsername = currentUser?.username || null;
        const status = {
            Auth: !!window.Auth,
            isAuthenticated: window.Auth?.isAuthenticated() || false,
            currentUser: currentUser,
            currentUsername: currentUsername,
            Toast: !!window.Toast,
            FractalContext: !!window.FractalContext,
            currentFractal: window.FractalContext?.currentFractal?.id || 'none',
            isEnabled: this.isEnabled,
            knownComments: this.knownCommentIds.size,
            initialLoadCompleted: this.initialLoadCompleted
        };
        return status;
    },

    // Force a test with fake data to verify the notification pipeline
    simulateNewComment() {
        const fakeComment = {
            comment: {
                id: 'test-' + Date.now(),
                text: 'This is a test comment to verify notifications are working',
                author: 'Test User',
                timestamp: new Date().toISOString()
            },
            log: {
                id: 'test-log-id',
                message: 'Test log message'
            }
        };
        this.showNotifications([fakeComment]);
    }
};

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    RealTimeComments.init();
});

// More aggressive initialization attempts to ensure it starts
const attemptStart = (attempt = 1) => {
    if (RealTimeComments.isEnabled) {
        return;
    }

    if (!window.Auth) {
        if (attempt < 10) {
            setTimeout(() => attemptStart(attempt + 1), 1000);
        }
        return;
    }

    if (!window.Auth.isAuthenticated()) {
        if (attempt < 10) {
            setTimeout(() => attemptStart(attempt + 1), 1000);
        }
        return;
    }

    if (!window.Toast) {
        if (attempt < 10) {
            setTimeout(() => attemptStart(attempt + 1), 1000);
        }
        return;
    }

    RealTimeComments.forceStart();
};

// Start attempting after page loads
setTimeout(() => attemptStart(1), 1000);

// Also try every 5 seconds for the first 30 seconds if not started
const retryInterval = setInterval(() => {
    if (!RealTimeComments.isEnabled && window.Auth?.isAuthenticated() && window.Toast) {
        RealTimeComments.forceStart();
    } else if (RealTimeComments.isEnabled) {
        clearInterval(retryInterval);
    }
}, 5000);

// Stop retry attempts after 30 seconds
setTimeout(() => {
    clearInterval(retryInterval);
}, 30000);

// Export for potential use by other modules
window.RealTimeComments = RealTimeComments;