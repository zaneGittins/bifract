// Comments module for Bifract log detail panel

const Comments = {
    currentLogID: null,
    currentLogData: null,
    editingCommentId: null,
    commentedLogIds: new Set(), // Cache of log IDs that have comments
    pendingTags: [], // Tags being composed for the current comment
    knownTags: [], // Cached existing tags for autocomplete
    selectedSuggestionIndex: -1, // Currently highlighted suggestion

    async init() {
        // Set up close button for comments panel
        const closeBtn = document.getElementById('closeCommentsPanelBtn');
        if (closeBtn) {
            closeBtn.addEventListener('click', () => this.closePanel());
        }

        // Set up search button for comments panel
        const searchBtn = document.getElementById('searchLogBtn');
        if (searchBtn) {
            searchBtn.addEventListener('click', () => this.searchForCurrentLog());
        }

        // Tag input: Enter or comma adds a tag, with autocomplete
        const tagField = document.getElementById('commentTagField');
        if (tagField) {
            tagField.addEventListener('keydown', (e) => {
                const dropdown = document.getElementById('tagSuggestionsDropdown');
                const items = dropdown ? dropdown.querySelectorAll('.tag-suggestion-item') : [];

                if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    if (items.length > 0) {
                        this.selectedSuggestionIndex = Math.min(this.selectedSuggestionIndex + 1, items.length - 1);
                        this.highlightSuggestion(items);
                    }
                    return;
                }
                if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    if (items.length > 0) {
                        this.selectedSuggestionIndex = Math.max(this.selectedSuggestionIndex - 1, 0);
                        this.highlightSuggestion(items);
                    }
                    return;
                }
                if (e.key === 'Escape') {
                    this.hideSuggestions();
                    return;
                }
                if (e.key === 'Enter' || e.key === ',') {
                    e.preventDefault();
                    // Use selected suggestion if one is highlighted
                    if (this.selectedSuggestionIndex >= 0 && items.length > 0) {
                        const val = items[this.selectedSuggestionIndex].dataset.tag;
                        this.addTag(val);
                    } else {
                        const val = tagField.value.replace(/,/g, '').trim();
                        if (val) this.addTag(val);
                    }
                    tagField.value = '';
                    this.hideSuggestions();
                } else if (e.key === 'Backspace' && tagField.value === '' && this.pendingTags.length > 0) {
                    this.pendingTags.pop();
                    this.renderPendingTags();
                }
            });

            tagField.addEventListener('input', () => {
                this.showSuggestions(tagField.value);
            });

            tagField.addEventListener('focus', () => {
                this.showSuggestions(tagField.value);
            });

            // Close suggestions when clicking outside
            document.addEventListener('click', (e) => {
                if (!e.target.closest('.comment-tags-input')) {
                    this.hideSuggestions();
                }
            });
        }

        // Event delegation for dynamically created elements
        document.addEventListener('click', (e) => {
            if (e.target.matches('.btn-edit-comment')) {
                this.startEditComment(e.target.dataset.id);
            } else if (e.target.matches('.btn-delete-comment')) {
                this.deleteComment(e.target.dataset.id);
            } else if (e.target.matches('.btn-cancel-edit')) {
                this.cancelEditComment();
            } else if (e.target.matches('.btn-save-edit')) {
                this.saveEditComment();
            } else if (e.target.matches('.comment-tag-remove')) {
                const tag = e.target.dataset.tag;
                this.pendingTags = this.pendingTags.filter(t => t !== tag);
                this.renderPendingTags();
            } else if (e.target.matches('.comment-query-link')) {
                this.loadQueryFromComment(e.target.dataset.query);
            } else if (e.target.closest('.tag-suggestion-item')) {
                const item = e.target.closest('.tag-suggestion-item');
                this.addTag(item.dataset.tag);
                const tagField = document.getElementById('commentTagField');
                if (tagField) tagField.value = '';
                this.hideSuggestions();
            }
        });
    },

    addTag(val) {
        if (val && !this.pendingTags.includes(val)) {
            this.pendingTags.push(val);
            this.renderPendingTags();
        }
    },

    async fetchKnownTags() {
        try {
            const response = await fetch('/api/v1/comments/tags', { credentials: 'include' });
            const data = await response.json();
            if (data.success) {
                this.knownTags = data.data || [];
            }
        } catch (err) {
            console.error('[Comments] Failed to fetch known tags:', err);
        }
    },

    showSuggestions(query) {
        const container = document.getElementById('commentTagsInput');
        if (!container) return;

        let dropdown = document.getElementById('tagSuggestionsDropdown');

        const q = (query || '').replace(/,/g, '').trim().toLowerCase();
        // Filter: exclude already-pending tags, match prefix
        const matches = this.knownTags.filter(t =>
            !this.pendingTags.includes(t) && (q === '' || t.toLowerCase().includes(q))
        );

        if (matches.length === 0) {
            this.hideSuggestions();
            return;
        }

        if (!dropdown) {
            dropdown = document.createElement('div');
            dropdown.id = 'tagSuggestionsDropdown';
            dropdown.className = 'tag-suggestions-dropdown';
            container.appendChild(dropdown);
        }

        this.selectedSuggestionIndex = -1;
        dropdown.innerHTML = matches.map(tag =>
            `<div class="tag-suggestion-item" data-tag="${Utils.escapeHtml(tag)}">${Utils.escapeHtml(tag)}</div>`
        ).join('');
        dropdown.style.display = 'block';
    },

    hideSuggestions() {
        const dropdown = document.getElementById('tagSuggestionsDropdown');
        if (dropdown) {
            dropdown.style.display = 'none';
        }
        this.selectedSuggestionIndex = -1;
    },

    highlightSuggestion(items) {
        items.forEach((item, i) => {
            item.classList.toggle('active', i === this.selectedSuggestionIndex);
        });
        // Scroll into view if needed
        if (items[this.selectedSuggestionIndex]) {
            items[this.selectedSuggestionIndex].scrollIntoView({ block: 'nearest' });
        }
    },

    renderPendingTags() {
        const container = document.getElementById('commentTagsChips');
        if (!container) return;
        container.innerHTML = this.pendingTags.map(tag =>
            `<span class="comment-tag-chip">${Utils.escapeHtml(tag)}<span class="comment-tag-remove" data-tag="${Utils.escapeHtml(tag)}">&times;</span></span>`
        ).join('');
    },

    loadQueryFromComment(query) {
        if (!query) return;
        this.closePanel();
        if (window.LogDetail) window.LogDetail.close();
        if (window.App && window.App.showFractalViewTab) {
            window.App.showFractalViewTab('search');
        }
        const queryInput = document.getElementById('queryInput');
        if (queryInput) {
            queryInput.value = query;
            if (window.SyntaxHighlight) {
                SyntaxHighlight.updateHighlight('queryInput', 'queryHighlight');
            }
            if (window.QueryExecutor && window.QueryExecutor.execute) {
                setTimeout(() => window.QueryExecutor.execute(), 100);
            }
        }
    },

    async openPanel(logData, isAggregated = false) {
        const panel = document.getElementById('commentsPanel');

        if (!panel) {
            console.error('[Comments] Panel element not found!');
            return;
        }

        this.currentLogData = logData;

        // For aggregated queries, silently disable comments (no error message)
        if (isAggregated) {
            console.log('[Comments] Silently skipping comments for aggregated query');
            return;
        }

        // ALWAYS use log_id from ClickHouse - no fallback, no generation
        if (!logData.log_id) {
            console.error('[Comments] No log_id in logData! Cannot add comments.');
            console.error('[Comments] Available keys in logData:', Object.keys(logData));
            if (window.Toast) {
                Toast.warning('Comments Unavailable', 'This log does not have an ID. Comments are not available.');
            } else {
                alert('This log does not have an ID. Cannot add comments.');
            }
            return;
        }

        this.currentLogID = String(logData.log_id);
        console.log('[Comments] Using log_id from ClickHouse:', this.currentLogID);

        // Clear the input and tags
        const input = document.getElementById('commentInput');
        if (input) input.value = '';
        this.pendingTags = [];
        this.renderPendingTags();
        const tagField = document.getElementById('commentTagField');
        if (tagField) tagField.value = '';

        // Load comments and known tags in parallel
        this.fetchKnownTags();
        await this.loadComments(logData);

        // Open the panel
        console.log('[Comments] Adding "open" class to panel');
        panel.classList.add('open');
        console.log('[Comments] Panel classes:', panel.className);
    },

    closePanel() {
        const panel = document.getElementById('commentsPanel');
        if (panel) {
            panel.classList.remove('open');
        }
        this.currentLogID = null;
        this.currentLogData = null;

        // Also close the log detail panel
        const detailPanel = document.getElementById('logDetailPanel');
        if (detailPanel) {
            detailPanel.classList.remove('open');
        }
    },

    searchForCurrentLog() {
        if (!this.currentLogID) {
            if (window.Toast) {
                Toast.warning('No Log Selected', 'No log ID available for search');
            }
            return;
        }

        // Close the comment panels
        this.closePanel();
        if (window.LogDetail) {
            window.LogDetail.close();
        }

        // Switch to search view
        if (window.App && window.App.showFractalViewTab) {
            window.App.showFractalViewTab('search');
        }

        // Fill in the search query
        const queryInput = document.getElementById('queryInput');
        if (queryInput) {
            queryInput.value = `@log_id="${this.currentLogID}"`;

            // Execute the search
            if (window.QueryExecutor && window.QueryExecutor.execute) {
                setTimeout(() => {
                    window.QueryExecutor.execute();
                }, 100);
            }
        }

        if (window.Toast) {
            Toast.info('Searching', `Looking for log ID: ${this.currentLogID.substring(0, 8)}...`);
        }
    },

    async loadComments(logData) {
        this.currentLogData = logData;

        // ALWAYS use log_id from ClickHouse - no fallback
        if (!logData.log_id) {
            console.error('[Comments] No log_id in logData during loadComments');
            return;
        }

        this.currentLogID = String(logData.log_id);

        try {
            const response = await fetch(`/api/v1/logs/${this.currentLogID}/comments`, {
                credentials: 'include'
            });

            const data = await response.json();

            if (data.success) {
                this.renderComments(data.data || []);
            } else {
                console.error('Failed to load comments:', data.error);
            }
        } catch (error) {
            console.error('Error loading comments:', error);
        }
    },

    async saveComment() {
        if (!Auth.isAuthenticated()) {
            if (window.Toast) {
                Toast.warning('Login Required', 'Please login to add comments');
            } else {
                alert('Please login to add comments');
            }
            return;
        }

        const textArea = document.getElementById('commentInput');
        const text = textArea.value.trim();

        if (!text) {
            if (window.Toast) {
                Toast.warning('Comment Required', 'Please enter a comment');
            } else {
                alert('Please enter a comment');
            }
            return;
        }

        // Debug: Check what's in currentLogData
        console.log('[Comments] DEBUG currentLogData keys:', Object.keys(this.currentLogData));
        console.log('[Comments] DEBUG currentLogData.timestamp:', this.currentLogData.timestamp);

        // Ensure timestamp is in RFC3339 format
        let timestamp = this.currentLogData.timestamp;

        if (timestamp) {
            try {
                // ClickHouse returns timestamps without timezone (e.g. "2026-03-22 18:37:11.329").
                // These are UTC but new Date() would interpret them as local time.
                // Append 'Z' to ensure correct UTC parsing before converting to ISO string.
                let toParse = timestamp;
                if (!toParse.endsWith('Z') && !toParse.includes('+') && !toParse.includes('T')) {
                    toParse = toParse.replace(' ', 'T') + 'Z';
                }
                const date = new Date(toParse);
                if (isNaN(date.getTime())) {
                    throw new Error('Invalid date');
                }
                timestamp = date.toISOString();
            } catch (error) {
                console.error('[Comments] Failed to convert timestamp:', error);
                if (window.Toast) {
                    Toast.error('Invalid Timestamp', 'Unable to process log timestamp');
                } else {
                    alert('Invalid timestamp format');
                }
                return;
            }
        } else {
            console.error('[Comments] No timestamp in currentLogData');
            if (window.Toast) {
                Toast.error('Missing Timestamp', 'Log has no timestamp');
            } else {
                alert('Log has no timestamp');
            }
            return;
        }

        // Collect tags from input
        const tagField = document.getElementById('commentTagField');
        if (tagField && tagField.value.trim()) {
            const val = tagField.value.trim();
            if (!this.pendingTags.includes(val)) {
                this.pendingTags.push(val);
            }
            tagField.value = '';
        }

        // Get current query from search bar
        const queryInput = document.getElementById('queryInput');
        const currentQuery = queryInput ? queryInput.value.trim() : '';

        const requestBody = {
            log_id: String(this.currentLogID),  // Ensure it's a string
            log_timestamp: String(timestamp),    // Ensure it's a string
            text: String(text),                  // Ensure it's a string
            tags: this.pendingTags.slice(),
            query: currentQuery,
            fractal_id: this.currentLogData.fractal_id || ''
        };

        console.log('[Comments] Saving comment with body:', requestBody);
        console.log('[Comments] Types:', {
            log_id: typeof requestBody.log_id,
            log_timestamp: typeof requestBody.log_timestamp,
            text: typeof requestBody.text
        });

        try {
            const response = await fetch('/api/v1/comments', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(requestBody)
            });

            const data = await response.json();

            if (data.success) {
                textArea.value = '';
                this.pendingTags = [];
                this.renderPendingTags();
                this.fetchKnownTags(); // Refresh tags cache with any new tags
                await this.loadComments(this.currentLogData);

                // Add this log to the commented logs cache so highlighting appears immediately
                if (this.currentLogID) {
                    this.commentedLogIds.add(this.currentLogID);
                    console.log('[Comments] Added log to commented cache:', this.currentLogID);

                    // Update the specific row's highlighting without re-rendering everything
                    this.updateRowHighlighting(this.currentLogID, true);
                }
            } else {
                const errorMsg = data.error || 'Unknown error';
                if (window.Toast) {
                    Toast.error('Failed to Save Comment', errorMsg);
                } else {
                    alert('Failed to save comment: ' + errorMsg);
                }
            }
        } catch (error) {
            console.error('Error saving comment:', error);
            if (window.Toast) {
                Toast.error('Network Error', 'Please try again.');
            } else {
                alert('Network error. Please try again.');
            }
        }
    },

    async startEditComment(id) {
        this.editingCommentId = id;
        const commentItem = document.querySelector(`.comment-item[data-id="${id}"]`);
        if (!commentItem) return;

        const commentText = commentItem.querySelector('.comment-text').textContent;

        // Replace comment display with edit form
        const commentBody = commentItem.querySelector('.comment-body');
        commentBody.innerHTML = `
            <textarea class="comment-edit-input" id="editCommentInput">${Utils.escapeHtml(commentText)}</textarea>
            <div class="comment-edit-actions">
                <button class="btn-save-edit btn-primary">Save</button>
                <button class="btn-cancel-edit">Cancel</button>
            </div>
        `;

        document.getElementById('editCommentInput').focus();
    },

    async saveEditComment() {
        const textArea = document.getElementById('editCommentInput');
        const text = textArea.value.trim();

        if (!text) {
            if (window.Toast) {
                Toast.warning('Comment Required', 'Comment cannot be empty');
            } else {
                alert('Comment cannot be empty');
            }
            return;
        }

        try {
            const response = await fetch(`/api/v1/comments/${this.editingCommentId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ text, tags: [] })
            });

            const data = await response.json();

            if (data.success) {
                this.editingCommentId = null;
                await this.loadComments(this.currentLogData);
            } else {
                const errorMsg = data.error || 'Unknown error';
                if (window.Toast) {
                    Toast.error('Failed to Update Comment', errorMsg);
                } else {
                    alert('Failed to update comment: ' + errorMsg);
                }
            }
        } catch (error) {
            console.error('Error updating comment:', error);
            if (window.Toast) {
                Toast.error('Network Error', 'Please try again.');
            } else {
                alert('Network error. Please try again.');
            }
        }
    },

    cancelEditComment() {
        this.editingCommentId = null;
        this.loadComments(this.currentLogData);
    },

    async deleteComment(id) {
        try {
            const response = await fetch(`/api/v1/comments/${id}`, {
                method: 'DELETE',
                credentials: 'include'
            });

            const data = await response.json();

            if (data.success) {
                await this.loadComments(this.currentLogData);

                // Check if the log still has comments after deletion
                const commentsContainer = document.getElementById('commentsList');
                const hasRemainingComments = commentsContainer && !commentsContainer.querySelector('.empty-comments');

                if (this.currentLogID) {
                    if (hasRemainingComments) {
                        // Still has comments, ensure it's in the cache
                        this.commentedLogIds.add(this.currentLogID);
                        console.log('[Comments] Log still has comments, keeping in cache:', this.currentLogID);
                        this.updateRowHighlighting(this.currentLogID, true);
                    } else {
                        // No more comments, remove from cache
                        this.commentedLogIds.delete(this.currentLogID);
                        console.log('[Comments] Log has no more comments, removed from cache:', this.currentLogID);
                        this.updateRowHighlighting(this.currentLogID, false);
                    }
                }

                // Refresh the commented logs table if it's currently visible
                if (window.CommentedLogs) {
                    console.log('[Comments] Refreshing CommentedLogs after comment deletion');
                    window.CommentedLogs.fetchLogs(window.CommentedLogs.currentPage);
                }
            } else {
                const errorMsg = data.error || 'Unknown error';
                if (window.Toast) {
                    Toast.error('Failed to Delete Comment', errorMsg);
                } else {
                    alert('Failed to delete comment: ' + errorMsg);
                }
            }
        } catch (error) {
            console.error('Error deleting comment:', error);
            if (window.Toast) {
                Toast.error('Network Error', 'Please try again.');
            } else {
                alert('Network error. Please try again.');
            }
        }
    },

    renderComments(comments) {
        const container = document.getElementById('commentsList');

        if (!container) return;

        if (comments.length === 0) {
            const emptyMessages = [
                'Be the first to drop some knowledge!',
                'No comments yet. Break the ice!',
                'Drop your thoughts here. We\'re all ears!',
                'First comment? You\'ll go down in history!',
                'Silence is golden, but comments are better!'
            ];
            const randomMessage = emptyMessages[Math.floor(Math.random() * emptyMessages.length)];

            container.innerHTML = `
                <div class="empty-comments">
                    <div class="empty-icon">
                        <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" style="opacity: 0.4;">
                            <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
                        </svg>
                    </div>
                    <div class="empty-text">${randomMessage}</div>
                </div>
            `;
            return;
        }

        let html = '';
        const currentUser = Auth.getCurrentUser();

        comments.forEach(comment => {
            const isAuthor = currentUser && currentUser.username === comment.author;
            const tags = comment.tags || [];
            const query = comment.query || '';

            const tagsHtml = tags.length > 0
                ? `<div class="comment-tags">${tags.map(t => `<span class="comment-tag-badge">${Utils.escapeHtml(t)}</span>`).join('')}</div>`
                : '';

            const queryHtml = query
                ? `<div class="comment-query"><span class="comment-query-label">Query:</span> <span class="comment-query-link" data-query="${Utils.escapeHtml(query)}">${Utils.escapeHtml(query.length > 60 ? query.substring(0, 57) + '...' : query)}</span></div>`
                : '';

            html += `
                <div class="comment-item" data-id="${comment.id}">
                    <div class="comment-header">
                        <div class="gravatar" style="background-color: ${comment.author_gravatar_color}">
                            ${comment.author_gravatar_initial}
                        </div>
                        <div class="comment-meta">
                            <span class="comment-author">${Utils.escapeHtml(comment.author_display_name)}</span>
                            <span class="comment-date">${this.formatDate(comment.created_at)}</span>
                            ${comment.updated_at !== comment.created_at ? '<span class="comment-edited">(edited)</span>' : ''}
                        </div>
                    </div>
                    ${tagsHtml}
                    <div class="comment-body">
                        <div class="comment-text comment-markdown">${Utils.renderCommentMarkdown(comment.text)}</div>
                    </div>
                    ${queryHtml}
                    ${isAuthor ? `
                        <div class="comment-actions">
                            <button class="btn-edit-comment" data-id="${comment.id}">Edit</button>
                            <button class="btn-delete-comment" data-id="${comment.id}">Delete</button>
                        </div>
                    ` : ''}
                </div>
            `;
        });

        container.innerHTML = html;

        // Update comment count badge if it exists
        const countBadge = document.querySelector('.comment-count-badge');
        if (countBadge) countBadge.textContent = `(${comments.length})`;
    },


    formatDate(dateStr) {
        const date = new Date(dateStr);
        const now = new Date();
        const diffMs = now - date;
        const diffMins = Math.floor(diffMs / 60000);
        const diffHours = Math.floor(diffMs / 3600000);
        const diffDays = Math.floor(diffMs / 86400000);

        if (diffMins < 1) return 'just now';
        if (diffMins < 60) return `${diffMins}m ago`;
        if (diffHours < 24) return `${diffHours}h ago`;
        if (diffDays < 7) return `${diffDays}d ago`;

        return date.toLocaleDateString('en-US', {
            month: 'short',
            day: 'numeric',
            year: date.getFullYear() !== now.getFullYear() ? 'numeric' : undefined
        });
    },

    // Fetch and cache all log IDs that have comments
    async fetchCommentedLogIds() {
        try {
            // Fetch all commented logs (up to 1000 for caching)
            const response = await fetch(
                `/api/v1/logs/commented?limit=1000&offset=0`,
                { credentials: 'include' }
            );

            const data = await response.json();

            if (data.success && data.data) {
                // Clear and rebuild the cache
                this.commentedLogIds.clear();
                data.data.forEach(log => {
                    if (log.log_id) {
                        this.commentedLogIds.add(log.log_id);
                    }
                });
                console.log('[Comments] Cached', this.commentedLogIds.size, 'commented log IDs');
            }
        } catch (error) {
            console.error('[Comments] Error fetching commented log IDs:', error);
        }
    },

    // Check if a log has comments
    hasComments(logData) {
        // Use log_id directly from data if available
        const logId = logData.log_id;
        if (logId) {
            return this.commentedLogIds.has(logId);
        }
        // Can't check without ID
        return false;
    },

    async generateLogID(logData) {
        // Client-side implementation matching backend SHA256 hash
        // Uses timestamp_nanos + raw_log for hash
        const timestamp = new Date(logData.timestamp).getTime() * 1000000; // Convert to nanoseconds
        const rawLog = logData.raw_log || JSON.stringify(logData);
        const data = `${timestamp}:${rawLog}`;

        // Use Web Crypto API for SHA-256 (matching backend)
        try {
            const encoder = new TextEncoder();
            const dataBytes = encoder.encode(data);
            const hashBuffer = await crypto.subtle.digest('SHA-256', dataBytes);
            const hashArray = Array.from(new Uint8Array(hashBuffer));
            const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');

            // Return first 32 hex characters (16 bytes) to match backend
            return hashHex.substring(0, 32);
        } catch (error) {
            console.error('[Comments] Failed to generate SHA-256 hash:', error);
            // Fallback to simple hash
            return this.simpleHash(data);
        }
    },

    simpleHash(str) {
        // Fallback hash for older browsers
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            const char = str.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash;
        }
        return Math.abs(hash).toString(16).padStart(32, '0');
    },

    // Update row highlighting in search results without full re-render
    updateRowHighlighting(logId, hasComments) {
        console.log('[Comments] Updating row highlighting for log:', logId, 'hasComments:', hasComments);

        // Find all rows in the search results table
        const resultRows = document.querySelectorAll('.results-table tbody tr, .results-table .result-row');

        resultRows.forEach(row => {
            // Look for the log ID in data attributes first (most reliable)
            const rowLogId = row.getAttribute('data-log-id') || row.dataset?.logId;

            if (rowLogId === logId) {
                if (hasComments) {
                    row.classList.add('has-comments');
                    console.log('[Comments] Added has-comments class to row via data attribute');
                } else {
                    row.classList.remove('has-comments');
                    console.log('[Comments] Removed has-comments class from row via data attribute');
                }
                return;
            }

            // Fallback: Look for log ID in row content (for cases where data attributes aren't set)
            const cells = row.querySelectorAll('td');
            let foundMatch = false;

            cells.forEach(cell => {
                const cellText = cell.textContent || '';
                const cellHtml = cell.innerHTML || '';

                // Look for exact log ID match or partial match at the beginning
                if (cellText === logId || cellText.startsWith(logId.substring(0, 12)) ||
                    cellHtml.includes(logId) || cellHtml.includes(`"${logId}"`)) {
                    foundMatch = true;
                }
            });

            if (foundMatch) {
                if (hasComments) {
                    row.classList.add('has-comments');
                    console.log('[Comments] Added has-comments class to row via content search');
                } else {
                    row.classList.remove('has-comments');
                    console.log('[Comments] Removed has-comments class from row via content search');
                }
            }
        });

        // Also trigger a manual check by looking at QueryExecutor's current results
        if (window.QueryExecutor && window.QueryExecutor.lastResults) {
            console.log('[Comments] Refreshing comment highlighting on current results');
            // Re-apply highlighting to all current results
            this.fetchCommentedLogIds();
        }
    },

    // ============================
    // Fractal Context Management
    // ============================

    // Refresh commented log IDs cache when index context changes
    onFractalChange() {
        console.log('[Comments] Fractal changed, refreshing commented log IDs cache');
        this.fetchCommentedLogIds();
    }
};

// Make globally available
window.Comments = Comments;

// Initialize comments module
document.addEventListener('DOMContentLoaded', () => {
    Comments.init();
});
