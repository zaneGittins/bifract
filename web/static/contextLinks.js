// Context Links management and log detail integration
const ContextLinks = {
    links: [],
    enabledLinks: null,
    editingId: null,

    init() {
        const createBtn = document.getElementById('contextLinkCreateBtn');
        if (createBtn) {
            createBtn.addEventListener('click', () => this.openCreateForm());
        }
        // Pre-load enabled links for log detail panel
        this.loadEnabledLinks();
    },

    show() {
        this.loadContextLinks();
    },

    hide() {},

    async loadContextLinks() {
        try {
            const data = await HttpUtils.safeFetch('/api/v1/context-links');
            this.links = data.data.context_links || [];
            this.renderContextLinks();
        } catch (err) {
            console.error('[ContextLinks] Failed to load:', err);
            Utils.showNotification('Failed to load context links', 'error');
        }
    },

    async loadEnabledLinks() {
        try {
            const data = await HttpUtils.safeFetch('/api/v1/context-links/enabled');
            this.enabledLinks = data.data.context_links || [];
        } catch (err) {
            console.error('[ContextLinks] Failed to load enabled links:', err);
            this.enabledLinks = [];
        }
    },

    renderContextLinks() {
        const container = document.getElementById('contextLinksListContainer');
        if (!container) return;

        if (this.links.length === 0) {
            container.innerHTML = '<p class="empty-state">No context links configured.</p>';
            return;
        }

        let html = `<table class="context-links-table">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Match Fields</th>
                    <th>Regex</th>
                    <th>Enabled</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>`;

        this.links.forEach(link => {
            const fields = (link.match_fields || []).map(f => Utils.escapeHtml(f)).join(', ');
            const regex = link.validation_regex ? Utils.escapeHtml(link.validation_regex) : '-';
            const defaultBadge = link.is_default ? ' <span class="context-link-badge">default</span>' : '';
            html += `<tr>
                <td>${Utils.escapeHtml(link.short_name)}${defaultBadge}</td>
                <td class="context-link-fields">${fields}</td>
                <td class="context-link-regex"><code>${regex}</code></td>
                <td>
                    <label class="toggle-switch">
                        <input type="checkbox" ${link.enabled ? 'checked' : ''} onchange="ContextLinks.toggleEnabled('${link.id}', this.checked)">
                        <span class="toggle-slider"></span>
                    </label>
                </td>
                <td class="context-link-actions">
                    <button class="btn-sm btn-secondary" onclick="ContextLinks.openEditForm('${link.id}')" title="Edit">Edit</button>
                    <button class="btn-sm btn-danger" onclick="ContextLinks.deleteLink('${link.id}')" title="Delete">Delete</button>
                </td>
            </tr>`;
        });

        html += '</tbody></table>';
        container.innerHTML = html;
    },

    openCreateForm() {
        this.editingId = null;
        const modal = document.getElementById('contextLinkModal');
        if (!modal) return;

        document.getElementById('contextLinkModalTitle').textContent = 'Create Context Link';
        document.getElementById('contextLinkName').value = '';
        document.getElementById('contextLinkFields').value = '';
        document.getElementById('contextLinkRegex').value = '';
        document.getElementById('contextLinkURL').value = '';
        document.getElementById('contextLinkRedirectWarning').checked = true;
        document.getElementById('contextLinkEnabled').checked = true;

        modal.style.display = 'flex';
    },

    async openEditForm(id) {
        try {
            const data = await HttpUtils.safeFetch(`/api/v1/context-links/${id}`);
            const link = data.data;
            this.editingId = id;

            const modal = document.getElementById('contextLinkModal');
            if (!modal) return;

            document.getElementById('contextLinkModalTitle').textContent = 'Edit Context Link';
            document.getElementById('contextLinkName').value = link.short_name;
            document.getElementById('contextLinkFields').value = (link.match_fields || []).join(', ');
            document.getElementById('contextLinkRegex').value = link.validation_regex || '';
            document.getElementById('contextLinkURL').value = link.context_link;
            document.getElementById('contextLinkRedirectWarning').checked = link.redirect_warning;
            document.getElementById('contextLinkEnabled').checked = link.enabled;

            modal.style.display = 'flex';
        } catch (err) {
            Utils.showNotification('Failed to load context link', 'error');
        }
    },

    closeModal() {
        const modal = document.getElementById('contextLinkModal');
        if (modal) modal.style.display = 'none';
        this.editingId = null;
    },

    async saveLink() {
        const name = document.getElementById('contextLinkName').value.trim();
        const fieldsRaw = document.getElementById('contextLinkFields').value.trim();
        const regex = document.getElementById('contextLinkRegex').value.trim();
        const url = document.getElementById('contextLinkURL').value.trim();
        const redirectWarning = document.getElementById('contextLinkRedirectWarning').checked;
        const enabled = document.getElementById('contextLinkEnabled').checked;

        if (!name || !fieldsRaw || !url) {
            Utils.showNotification('Name, match fields, and URL are required', 'error');
            return;
        }

        const matchFields = fieldsRaw.split(',').map(f => f.trim()).filter(f => f);

        const body = {
            short_name: name,
            match_fields: matchFields,
            validation_regex: regex,
            context_link: url,
            redirect_warning: redirectWarning,
            enabled: enabled
        };

        try {
            if (this.editingId) {
                await HttpUtils.safeFetch(`/api/v1/context-links/${this.editingId}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body)
                });
                Utils.showNotification('Context link updated', 'success');
            } else {
                await HttpUtils.safeFetch('/api/v1/context-links', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body)
                });
                Utils.showNotification('Context link created', 'success');
            }
            this.closeModal();
            this.loadContextLinks();
            this.loadEnabledLinks();
        } catch (err) {
            Utils.showNotification(`Failed to save: ${err.message}`, 'error');
        }
    },

    async toggleEnabled(id, enabled) {
        const link = this.links.find(l => l.id === id);
        if (!link) return;

        const body = {
            short_name: link.short_name,
            match_fields: link.match_fields,
            validation_regex: link.validation_regex,
            context_link: link.context_link,
            redirect_warning: link.redirect_warning,
            enabled: enabled
        };

        try {
            await HttpUtils.safeFetch(`/api/v1/context-links/${id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });
            link.enabled = enabled;
            this.loadEnabledLinks();
        } catch (err) {
            Utils.showNotification('Failed to update', 'error');
            this.loadContextLinks();
        }
    },

    async deleteLink(id) {
        if (!confirm('Delete this context link?')) return;
        try {
            await HttpUtils.safeFetch(`/api/v1/context-links/${id}`, { method: 'DELETE' });
            Utils.showNotification('Context link deleted', 'success');
            this.loadContextLinks();
            this.loadEnabledLinks();
        } catch (err) {
            Utils.showNotification('Failed to delete', 'error');
        }
    },

    // Returns matching context links for a given field name and value
    getMatchingLinks(fieldName, fieldValue) {
        if (!this.enabledLinks || !fieldName || !fieldValue) return [];
        const lowerField = fieldName.toLowerCase();
        const strValue = String(fieldValue);

        return this.enabledLinks.filter(link => {
            const fieldsMatch = (link.match_fields || []).some(f => f.toLowerCase() === lowerField);
            if (!fieldsMatch) return false;
            if (link.validation_regex) {
                try {
                    return new RegExp(link.validation_regex).test(strValue);
                } catch {
                    return false;
                }
            }
            return true;
        });
    },

    // Build the URL for a context link with the value substituted
    buildURL(link, value) {
        return link.context_link.replace('<ATTR_VALUE>', encodeURIComponent(value));
    },

    // Open a context link, optionally showing a redirect warning
    openLink(link, value) {
        const url = this.buildURL(link, value);
        if (link.redirect_warning) {
            try {
                const domain = new URL(url).hostname;
                if (!confirm(`You are about to leave Bifract and visit ${domain}. Continue?`)) {
                    return;
                }
            } catch {
                if (!confirm('You are about to open an external link. Continue?')) {
                    return;
                }
            }
        }
        window.open(url, '_blank', 'noopener,noreferrer');
    }
};

window.ContextLinks = ContextLinks;
