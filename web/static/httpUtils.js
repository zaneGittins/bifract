// HTTP Utilities for safe response handling

const HttpUtils = {
    // Safe JSON parsing with proper error handling
    async safeJsonResponse(response) {
        try {
            return await response.json();
        } catch (jsonError) {
            // If the request was aborted, re-throw so callers can detect AbortError
            if (jsonError.name === 'AbortError') {
                throw jsonError;
            }
            console.error('JSON parsing error:', jsonError, 'Response status:', response.status);

            // Provide better error messages based on status
            if (response.status === 0) {
                throw new Error('Network error - please check your connection');
            } else if (response.status === 408 || response.status === 504) {
                throw new Error('Request timeout - server took too long to respond');
            } else if (response.status >= 500) {
                throw new Error('Server error - the request may have timed out or failed');
            } else if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            // For successful responses that failed to parse as JSON
            throw new Error('Invalid response format - server returned malformed data');
        }
    },

    // Safe fetch with automatic JSON parsing and error handling
    async safeFetch(url, options = {}) {
        try {
            const response = await fetch(url, {
                credentials: 'include', // Include cookies by default
                ...options
            });

            // For non-ok responses, try to get error details
            if (!response.ok) {
                let errorMessage;
                try {
                    const errorData = await response.json();
                    errorMessage = errorData.error || errorData.message || `HTTP ${response.status}`;
                } catch {
                    errorMessage = `HTTP ${response.status}: ${response.statusText}`;
                }
                throw new Error(errorMessage);
            }

            return await this.safeJsonResponse(response);
        } catch (error) {
            // Re-throw with context about the request
            const method = options.method || 'GET';
            console.error(`[HttpUtils] ${method} ${url} failed:`, error.message);
            throw error;
        }
    }
};

// Make globally available
window.HttpUtils = HttpUtils;