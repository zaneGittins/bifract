// Commented Logs view module - flat comment list with filtering, sorting, selection, and bulk operations

const CommentedLogs = {
    allComments: [],
    filteredComments: [],
    selectedIds: new Set(),
    currentPage: 1,
    pageSize: 25,
    sortColumn: null,
    sortDirection: null,

    async init() {
        const commentedTab = document.getElementById('commentedTabBtn');
        if (commentedTab) {
            commentedTab.addEventListener('click', () => this.show());
        }

        const searchTab = document.getElementById('searchTabBtn');
        if (searchTab) {
            searchTab.addEventListener('click', () => this.hide());
        }

        const refreshBtn = document.getElementById('commentedRefreshBtn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.fetchComments());
        }

        const generateBtn = document.getElementById('generateNotebookBtn');
        if (generateBtn) {
            generateBtn.addEventListener('click', () => this.showGenerateNotebookModal());
        }
    },

    async show() {
        await this.fetchComments();
    },

    hide() {},

    // ============================
    // Data Fetching
    // ============================

    async fetchComments() {
        if (!Auth.isAuthenticated()) {
            this.renderEmpty('Please login to view comments');
            return;
        }

        try {
            let url = '/api/v1/comments/flat?limit=5000';

            const response = await fetch(url, { credentials: 'include' });
            const data = await response.json();

            if (data.success) {
                this.allComments = data.data || [];
                this.populateTagFilter();
                this.populateAuthorFilter();
                this.filterComments();
            } else {
                this.renderEmpty('Failed to load comments: ' + (data.error || 'Unknown error'));
            }
        } catch (error) {
            console.error('[CommentedLogs] Error fetching comments:', error);
            this.renderEmpty('Network error. Please try again.');
        }
    },

    // ============================
    // Filtering (client-side)
    // ============================

    filterComments() {
        const search = (document.getElementById('commentedSearchInput')?.value || '').toLowerCase();
        const tagFilter = document.getElementById('commentedTagFilter')?.value || 'all';
        const authorFilter = document.getElementById('commentedAuthorFilter')?.value || 'all';

        this.filteredComments = this.allComments.filter(c => {
            if (search) {
                const textMatch = c.text && c.text.toLowerCase().includes(search);
                const logIdMatch = c.log_id && c.log_id.toLowerCase().includes(search);
                const authorMatch = (c.author_display_name || c.author || '').toLowerCase().includes(search);
                const queryMatch = (c.query || '').toLowerCase().includes(search);
                const tagMatch = c.tags && c.tags.some(t => t.toLowerCase().includes(search));
                if (!textMatch && !logIdMatch && !authorMatch && !queryMatch && !tagMatch) return false;
            }

            if (tagFilter !== 'all') {
                if (!c.tags || !c.tags.includes(tagFilter)) return false;
            }

            if (authorFilter !== 'all' && c.author !== authorFilter) return false;

            return true;
        });

        this.currentPage = 1;
        this.selectedIds.clear();
        this.applySorting();
        this.renderTable();
    },

    populateTagFilter() {
        const select = document.getElementById('commentedTagFilter');
        if (!select) return;
        const currentVal = select.value;
        const tags = new Set();
        this.allComments.forEach(c => (c.tags || []).forEach(t => tags.add(t)));
        let html = '<option value="all">All Tags</option>';
        [...tags].sort().forEach(t => {
            html += `<option value="${Utils.escapeHtml(t)}">${Utils.escapeHtml(t)}</option>`;
        });
        select.innerHTML = html;
        if ([...tags].includes(currentVal)) select.value = currentVal;
    },

    populateAuthorFilter() {
        const select = document.getElementById('commentedAuthorFilter');
        if (!select) return;
        const currentVal = select.value;
        const authors = new Map();
        this.allComments.forEach(c => {
            if (!authors.has(c.author)) {
                authors.set(c.author, c.author_display_name || c.author);
            }
        });
        let html = '<option value="all">All Authors</option>';
        [...authors.entries()].sort((a, b) => a[1].localeCompare(b[1])).forEach(([username, display]) => {
            html += `<option value="${Utils.escapeHtml(username)}">${Utils.escapeHtml(display)}</option>`;
        });
        select.innerHTML = html;
        if (authors.has(currentVal)) select.value = currentVal;
    },

    setTagFilter(tag) {
        const select = document.getElementById('commentedTagFilter');
        if (select) select.value = tag;
        this.filterComments();
    },

    // ============================
    // Sorting
    // ============================

    toggleSort(column) {
        if (this.sortColumn === column) {
            if (this.sortDirection === 'asc') {
                this.sortDirection = 'desc';
            } else {
                this.sortColumn = null;
                this.sortDirection = null;
            }
        } else {
            this.sortColumn = column;
            this.sortDirection = 'asc';
        }
        this.applySorting();
        this.renderTable();
    },

    applySorting() {
        if (!this.sortColumn) return;
        const col = this.sortColumn;
        const dir = this.sortDirection === 'asc' ? 1 : -1;

        this.filteredComments.sort((a, b) => {
            let va, vb;
            switch (col) {
                case 'author':
                    va = (a.author_display_name || a.author).toLowerCase();
                    vb = (b.author_display_name || b.author).toLowerCase();
                    return va < vb ? -dir : va > vb ? dir : 0;
                case 'created_at':
                    va = new Date(a.created_at).getTime();
                    vb = new Date(b.created_at).getTime();
                    return (va - vb) * dir;
                case 'tag_count':
                    va = (a.tags || []).length;
                    vb = (b.tags || []).length;
                    return (va - vb) * dir;
                default:
                    return 0;
            }
        });
    },

    sortIndicator(column) {
        if (this.sortColumn !== column) return '';
        return this.sortDirection === 'asc' ? ' &#9650;' : ' &#9660;';
    },

    // ============================
    // Selection
    // ============================

    getPageComments() {
        const start = (this.currentPage - 1) * this.pageSize;
        return this.filteredComments.slice(start, start + this.pageSize);
    },

    toggleSelectAll() {
        const pageComments = this.getPageComments();
        const allSelected = pageComments.length > 0 && pageComments.every(c => this.selectedIds.has(c.id));
        if (allSelected) {
            pageComments.forEach(c => this.selectedIds.delete(c.id));
        } else {
            pageComments.forEach(c => this.selectedIds.add(c.id));
        }
        this.renderTable();
    },

    toggleSelect(commentId) {
        if (this.selectedIds.has(commentId)) {
            this.selectedIds.delete(commentId);
        } else {
            this.selectedIds.add(commentId);
        }
        this.updateCheckboxStates();
        this.updateBulkActions();
    },

    updateCheckboxStates() {
        const pageComments = this.getPageComments();
        const allSelected = pageComments.length > 0 && pageComments.every(c => this.selectedIds.has(c.id));
        const selectAllCb = document.getElementById('commentedSelectAll');
        if (selectAllCb) selectAllCb.checked = allSelected;
    },

    updateBulkActions() {
        const addBtn = document.getElementById('commentedBulkAddTagBtn');
        const removeBtn = document.getElementById('commentedBulkRemoveTagBtn');
        const deleteBtn = document.getElementById('commentedBulkDeleteBtn');
        const countSpan = document.getElementById('commentedSelectedCount');
        const hasSelection = this.selectedIds.size > 0;

        if (addBtn) addBtn.style.display = hasSelection ? '' : 'none';
        if (removeBtn) removeBtn.style.display = hasSelection ? '' : 'none';
        if (deleteBtn) deleteBtn.style.display = hasSelection ? '' : 'none';
        if (countSpan) countSpan.textContent = hasSelection ? `${this.selectedIds.size} selected` : '';
    },

    // ============================
    // Bulk Tag Operations
    // ============================

    async bulkAddTag() {
        if (this.selectedIds.size === 0) return;
        const tag = prompt('Enter tag to add:');
        if (!tag || !tag.trim()) return;

        try {
            const data = await HttpUtils.safeFetch('/api/v1/comments/bulk-add-tag', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    comment_ids: [...this.selectedIds],
                    tag: tag.trim()
                })
            });
            this.selectedIds.clear();
            await this.fetchComments();
        } catch (err) {
            console.error('[CommentedLogs] Bulk add tag failed:', err);
            if (window.Toast) Toast.error('Failed', err.message);
        }
    },

    async bulkRemoveTag() {
        if (this.selectedIds.size === 0) return;

        const selectedComments = this.allComments.filter(c => this.selectedIds.has(c.id));
        const tagSet = new Set();
        selectedComments.forEach(c => (c.tags || []).forEach(t => tagSet.add(t)));
        if (tagSet.size === 0) {
            if (window.Toast) Toast.error('No Tags', 'Selected comments have no tags to remove');
            return;
        }

        const tagList = [...tagSet].sort().join(', ');
        const tag = prompt(`Enter tag to remove (${tagList}):`);
        if (!tag || !tag.trim()) return;

        try {
            const data = await HttpUtils.safeFetch('/api/v1/comments/bulk-remove-tag', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    comment_ids: [...this.selectedIds],
                    tag: tag.trim()
                })
            });
            this.selectedIds.clear();
            await this.fetchComments();
        } catch (err) {
            console.error('[CommentedLogs] Bulk remove tag failed:', err);
            if (window.Toast) Toast.error('Failed', err.message);
        }
    },

    // ============================
    // Bulk Delete
    // ============================

    async bulkDelete() {
        if (this.selectedIds.size === 0) return;

        const count = this.selectedIds.size;
        const confirmed = confirm(`Delete ${count} selected comment${count !== 1 ? 's' : ''}? This cannot be undone.`);
        if (!confirmed) return;

        try {
            const data = await HttpUtils.safeFetch('/api/v1/comments/bulk-delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ comment_ids: [...this.selectedIds] })
            });
            this.selectedIds.clear();
            await this.fetchComments();
            // Refresh the commented log IDs cache so row highlighting updates
            if (window.Comments) Comments.fetchCommentedLogIds();
        } catch (err) {
            console.error('[CommentedLogs] Bulk delete failed:', err);
            if (window.Toast) Toast.error('Failed', err.message);
        }
    },

    // ============================
    // Rendering
    // ============================

    renderTable() {
        const container = document.getElementById('commentedResults');
        if (!container) return;

        if (this.filteredComments.length === 0) {
            this.renderEmpty(this.allComments.length === 0
                ? 'No comments found'
                : 'No comments match the current filters');
            this.updateBulkActions();
            return;
        }

        const totalPages = Math.max(1, Math.ceil(this.filteredComments.length / this.pageSize));
        if (this.currentPage > totalPages) this.currentPage = totalPages;
        const pageComments = this.getPageComments();
        const allPageSelected = pageComments.length > 0 && pageComments.every(c => this.selectedIds.has(c.id));

        let html = `
            <div class="alerts-table-container">
                <div class="alerts-table-header">
                    <div class="alerts-count">
                        Showing ${pageComments.length} of ${this.filteredComments.length} comments
                        ${this.filteredComments.length !== this.allComments.length
                            ? ` (filtered from ${this.allComments.length} total)` : ''}
                    </div>
                    <div class="alerts-page-size">
                        <label>Show:</label>
                        <select onchange="CommentedLogs.changePageSize(this.value)">
                            <option value="10" ${this.pageSize===10?'selected':''}>10</option>
                            <option value="25" ${this.pageSize===25?'selected':''}>25</option>
                            <option value="50" ${this.pageSize===50?'selected':''}>50</option>
                            <option value="100" ${this.pageSize===100?'selected':''}>100</option>
                        </select>
                    </div>
                </div>
                <table class="alerts-table">
                    <thead>
                        <tr>
                            <th style="width:32px"><input type="checkbox" id="commentedSelectAll"
                                ${allPageSelected ? 'checked' : ''}
                                onchange="CommentedLogs.toggleSelectAll()" title="Select all on page" /></th>
                            <th class="sortable-th" onclick="CommentedLogs.toggleSort('created_at')">
                                Date${this.sortIndicator('created_at')}</th>
                            <th class="sortable-th" onclick="CommentedLogs.toggleSort('author')">
                                Author${this.sortIndicator('author')}</th>
                            <th>Comment</th>
                            <th class="sortable-th" onclick="CommentedLogs.toggleSort('tag_count')">
                                Tags${this.sortIndicator('tag_count')}</th>
                            <th>Log</th>
                        </tr>
                    </thead>
                    <tbody>`;

        for (const comment of pageComments) {
            const isSelected = this.selectedIds.has(comment.id);
            const created = new Date(comment.created_at).toLocaleString();
            const textPreview = comment.text.length > 100
                ? comment.text.substring(0, 100) + '...' : comment.text;
            const tags = comment.tags || [];
            const logIdShort = comment.log_id.length > 12
                ? comment.log_id.substring(0, 12) + '..' : comment.log_id;

            const maxTags = 3;
            let tagsHtml = '';
            if (tags.length > 0) {
                const shown = tags.slice(0, maxTags);
                tagsHtml = shown.map(t =>
                    `<span class="label-pill" onclick="event.stopPropagation(); CommentedLogs.setTagFilter('${Utils.escapeJs(t)}')" title="${Utils.escapeHtml(t)}">${Utils.escapeHtml(t.length > 20 ? t.substring(0, 18) + '..' : t)}</span>`
                ).join(' ');
                if (tags.length > maxTags) {
                    tagsHtml += ` <span class="label-pill label-pill-more" title="${Utils.escapeHtml(tags.slice(maxTags).join(', '))}">+${tags.length - maxTags}</span>`;
                }
            } else {
                tagsHtml = '<span class="text-muted">-</span>';
            }

            html += `
                <tr class="alert-row" data-comment-id="${comment.id}"
                    data-log-id="${Utils.escapeHtml(comment.log_id)}"
                    data-log-ts="${Utils.escapeHtml(comment.log_timestamp)}"
                    data-fractal-id="${Utils.escapeHtml(comment.fractal_id || '')}">
                    <td onclick="event.stopPropagation()">
                        <input type="checkbox" ${isSelected ? 'checked' : ''}
                            onchange="CommentedLogs.toggleSelect('${comment.id}')" />
                    </td>
                    <td class="comment-date-cell">${Utils.escapeHtml(created)}</td>
                    <td class="comment-author-cell">
                        <div class="comment-author-row">
                            <span class="gravatar-sm" style="background-color: ${comment.author_gravatar_color || 'var(--accent-primary)'}">
                                ${Utils.escapeHtml(comment.author_gravatar_initial || comment.author.charAt(0).toUpperCase())}
                            </span>
                            <span>${Utils.escapeHtml(comment.author_display_name || comment.author)}</span>
                        </div>
                    </td>
                    <td class="comment-text-cell comment-markdown" title="${Utils.escapeHtml(comment.text)}">${Utils.renderCommentMarkdown(textPreview)}</td>
                    <td class="comment-tags-cell">${tagsHtml}</td>
                    <td class="comment-log-cell" title="${Utils.escapeHtml(comment.log_id)}">
                        <code>${Utils.escapeHtml(logIdShort)}</code>
                    </td>
                </tr>`;
        }

        html += `
                    </tbody>
                </table>
                ${this.renderPaginationHtml(totalPages)}
            </div>`;

        container.innerHTML = html;
        this.addRowClickHandlers();
        this.updateBulkActions();
    },

    addRowClickHandlers() {
        document.querySelectorAll('#commentedResults .alert-row').forEach(row => {
            row.addEventListener('click', () => {
                const logId = row.dataset.logId;
                const logTs = row.dataset.logTs;
                const fractalId = row.dataset.fractalId;
                if (logId) this.showLogDetail(logId, logTs, fractalId);
            });
        });
    },

    renderEmpty(message) {
        const container = document.getElementById('commentedResults');
        if (!container) return;

        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">
                    <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" style="opacity: 0.4;">
                        <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
                    </svg>
                </div>
                <div class="empty-message">${Utils.escapeHtml(message)}</div>
            </div>
        `;
    },

    // ============================
    // Pagination
    // ============================

    renderPaginationHtml(totalPages) {
        if (totalPages <= 1) return '';

        let html = '<div class="pagination-controls">';
        html += `<span class="pagination-info">Page ${this.currentPage} of ${totalPages} (${this.filteredComments.length} total)</span>`;
        html += '<div class="pagination-buttons">';

        if (this.currentPage > 1) {
            html += `<button onclick="CommentedLogs.goToPage(${this.currentPage - 1})" class="pagination-btn">Previous</button>`;
        }
        if (this.currentPage < totalPages) {
            html += `<button onclick="CommentedLogs.goToPage(${this.currentPage + 1})" class="pagination-btn">Next</button>`;
        }

        html += '</div></div>';
        return html;
    },

    goToPage(page) {
        this.currentPage = page;
        this.renderTable();
    },

    changePageSize(size) {
        this.pageSize = parseInt(size, 10) || 25;
        this.currentPage = 1;
        this.renderTable();
    },

    // ============================
    // Log Detail
    // ============================

    async showLogDetail(logId, logTimestamp, fractalId) {
        try {
            let requestBody = {
                timestamp: logTimestamp,
                log_id: logId
            };

            // Use the comment's own fractal_id if available.
            // For prism-scoped comments, fractal_id is empty - omit it
            // so the backend searches across all fractals by log_id.
            if (fractalId) {
                requestBody.fractal_id = fractalId;
            }

            const response = await fetch('/api/v1/logs/by-timestamp', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(requestBody)
            });

            const data = await response.json();

            if (data.success && data.log) {
                if (window.LogDetail) {
                    LogDetail.show(data.log);
                }
            } else {
                if (window.Toast) {
                    Toast.error('Not Found', data.error || 'Log not found in ClickHouse');
                }
            }
        } catch (error) {
            console.error('[CommentedLogs] Error fetching log details:', error);
            if (window.Toast) {
                Toast.error('Network Error', 'Failed to fetch log details');
            }
        }
    },

    // ============================
    // Fractal Context
    // ============================

    onFractalChange() {
        const commentedView = document.getElementById('commentedView');
        if (commentedView && commentedView.offsetParent !== null) {
            this.fetchComments();
        }
    },

    // ============================
    // Generate Notebook
    // ============================

    async showGenerateNotebookModal() {
        const existing = document.getElementById('generateNotebookModal');
        if (existing) existing.remove();

        // Fetch tags and AI status in parallel
        let tags = [];
        let aiEnabled = false;
        try {
            const [tagsResp, aiResp] = await Promise.all([
                fetch('/api/v1/comments/tags', { credentials: 'include' }),
                fetch('/api/v1/notebooks/ai-status', { credentials: 'include' })
            ]);
            const tagsData = await tagsResp.json();
            if (tagsData.success && Array.isArray(tagsData.data)) tags = tagsData.data;
            const aiData = await aiResp.json();
            aiEnabled = aiData.ai_enabled || false;
        } catch { /* ignore */ }

        if (tags.length === 0) {
            if (window.Toast) Toast.info('No Tags', 'No comment tags found. Tag some comments first.');
            return;
        }

        const attackChainOption = aiEnabled ? `
            <div class="gnb-attack-chain">
                <label class="gnb-checkbox-label">
                    <input type="checkbox" id="generateNotebookAttackChain" class="gnb-checkbox">
                    <span class="gnb-checkbox-box">
                        <svg viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M2.5 6l2.5 2.5 4.5-4.5"/>
                        </svg>
                    </span>
                    AI Attack Chain Summary
                </label>
                <small class="gnb-attack-chain-desc">
                    Maps findings to MITRE ATT&CK tactics with collapsible sections.
                </small>
            </div>
        ` : '';

        const tagItems = tags.map(t => {
            const escaped = Utils.escapeHtml(t);
            return `<button type="button" class="gnb-tag-item" data-tag="${escaped}">
                <svg class="gnb-tag-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5">
                    <path d="M2.5 3.5h4.586a1 1 0 0 1 .707.293l5.414 5.414a1 1 0 0 1 0 1.414l-3.586 3.586a1 1 0 0 1-1.414 0L2.793 8.793A1 1 0 0 1 2.5 8.086V3.5z"/>
                    <circle cx="5.5" cy="6.5" r="1" fill="currentColor" stroke="none"/>
                </svg>
                <span>${escaped}</span>
            </button>`;
        }).join('');

        const modalHtml = `
            <div id="generateNotebookModal" class="modal-overlay">
                <div class="modal-content" style="max-width: 420px;">
                    <div class="modal-header">
                        <h3>Generate Notebook from Comments</h3>
                        <button class="modal-close" id="generateNotebookCloseBtn">&times;</button>
                    </div>
                    <form id="generateNotebookForm">
                        <input type="hidden" id="generateNotebookTag" value="">
                        <div class="form-group" style="padding: 1rem 1.25rem;">
                            <label>Tag</label>
                            <div class="gnb-tag-search-wrap">
                                <svg class="gnb-search-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5">
                                    <circle cx="7" cy="7" r="4.5"/>
                                    <path d="M10.5 10.5L14 14"/>
                                </svg>
                                <input type="text" id="generateNotebookTagSearch" class="gnb-tag-search"
                                       placeholder="Search tags..." autocomplete="off">
                            </div>
                            <div id="generateNotebookTagList" class="gnb-tag-list">
                                ${tagItems}
                            </div>
                            <small style="color: var(--text-muted); display: block; margin-top: 0.5rem;">
                                Select a tag to generate a notebook from its comments.
                            </small>
                        </div>
                        ${attackChainOption}
                        <div class="modal-buttons" style="padding: 0.75rem 1.25rem 1.25rem;">
                            <button type="button" class="btn-ghost" onclick="CommentedLogs.closeGenerateNotebookModal()">Cancel</button>
                            <button type="submit" class="btn-ghost gnb-generate-btn" id="generateNotebookSubmitBtn" disabled>Generate</button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.insertAdjacentHTML('beforeend', modalHtml);

        // Tag search filtering
        document.getElementById('generateNotebookTagSearch').addEventListener('input', (e) => {
            const q = e.target.value.toLowerCase();
            document.querySelectorAll('.gnb-tag-item').forEach(item => {
                const match = item.dataset.tag.toLowerCase().includes(q);
                item.style.display = match ? '' : 'none';
            });
        });

        // Tag item selection
        document.getElementById('generateNotebookTagList').addEventListener('click', (e) => {
            const item = e.target.closest('.gnb-tag-item');
            if (!item) return;
            document.querySelectorAll('.gnb-tag-item').forEach(p => p.classList.remove('selected'));
            item.classList.add('selected');
            document.getElementById('generateNotebookTag').value = item.dataset.tag;
            document.getElementById('generateNotebookSubmitBtn').disabled = false;
        });

        const modal = document.getElementById('generateNotebookModal');
        modal.addEventListener('click', (e) => {
            if (e.target === modal) this.closeGenerateNotebookModal();
        });
        document.getElementById('generateNotebookCloseBtn').addEventListener('click', () => this.closeGenerateNotebookModal());
        document.getElementById('generateNotebookForm').addEventListener('submit', (e) => this.handleGenerateNotebook(e));
    },

    closeGenerateNotebookModal() {
        const modal = document.getElementById('generateNotebookModal');
        if (modal) modal.remove();
    },

    async handleGenerateNotebook(event) {
        event.preventDefault();

        const tag = document.getElementById('generateNotebookTag').value.trim();
        if (!tag) return;

        const attackChainEl = document.getElementById('generateNotebookAttackChain');
        const attackChain = attackChainEl ? attackChainEl.checked : false;

        const submitBtn = document.getElementById('generateNotebookSubmitBtn');
        if (submitBtn) {
            submitBtn.disabled = true;
            submitBtn.textContent = 'Generating...';
        }

        try {
            const response = await fetch('/api/v1/notebooks/generate-from-comments', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ tag: tag, attack_chain: attackChain })
            });

            const data = await response.json();

            if (data.success) {
                this.closeGenerateNotebookModal();
                if (window.Toast) {
                    Toast.success('Notebook Generated', `Created "Notebook: ${tag}" with ${data.data.sections} sections`);
                }
            } else {
                if (window.Toast) {
                    Toast.error('Generation Failed', data.error || 'Unknown error');
                } else {
                    alert(data.error || 'Failed to generate notebook');
                }
                if (submitBtn) {
                    submitBtn.disabled = false;
                    submitBtn.textContent = 'Generate';
                }
            }
        } catch (error) {
            console.error('[CommentedLogs] Generate notebook error:', error);
            if (window.Toast) {
                Toast.error('Network Error', 'Failed to generate notebook');
            } else {
                alert('Network error. Please try again.');
            }
            if (submitBtn) {
                submitBtn.disabled = false;
                submitBtn.textContent = 'Generate';
            }
        }
    }
};

window.CommentedLogs = CommentedLogs;

document.addEventListener('DOMContentLoaded', () => {
    CommentedLogs.init();
});
