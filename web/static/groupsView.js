// Groups management module (tenant admin only, part of Settings page)

const GroupsView = {
    groups: [],
    selectedGroup: null,

    init() {
        const addBtn = document.getElementById('addGroupBtn');
        if (addBtn) addBtn.addEventListener('click', () => this.showCreateForm());

        const submitBtn = document.getElementById('submitCreateGroupBtn');
        if (submitBtn) submitBtn.addEventListener('click', () => this.createGroup());

        const cancelBtn = document.getElementById('cancelCreateGroupBtn');
        if (cancelBtn) cancelBtn.addEventListener('click', () => this.hideCreateForm());

        const nameInput = document.getElementById('newGroupName');
        if (nameInput) {
            nameInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') this.createGroup();
                if (e.key === 'Escape') this.hideCreateForm();
            });
        }

        const deleteBtn = document.getElementById('deleteGroupBtn');
        if (deleteBtn) deleteBtn.addEventListener('click', () => this.deleteGroup());

        // Inline editing of group name/description in the detail header
        const nameEl = document.getElementById('groupDetailName');
        if (nameEl) nameEl.addEventListener('click', () => this._startInlineEdit(nameEl, 'name', 'Group name'));

        const descEl = document.getElementById('groupDetailDescription');
        if (descEl) descEl.addEventListener('click', () => this._startInlineEdit(descEl, 'description', 'Add a description'));

        const addMemberBtn = document.getElementById('groupAddMemberBtn');
        if (addMemberBtn) addMemberBtn.addEventListener('click', () => this.showAddMembers());

        const submitAddBtn = document.getElementById('submitAddMembersBtn');
        if (submitAddBtn) submitAddBtn.addEventListener('click', () => this.submitAddMembers());

        const memberSearch = document.getElementById('memberPickerSearch');
        if (memberSearch) memberSearch.addEventListener('input', () => this.renderMemberPicker());

        const grantType = document.getElementById('permGrantType');
        if (grantType) grantType.addEventListener('change', () => this.toggleGrantType());

        const grantPermBtn = document.getElementById('grantPermissionBtn');
        if (grantPermBtn) grantPermBtn.addEventListener('click', () => this.showGrantForm());

        const submitGrantBtn = document.getElementById('submitGrantPermBtn');
        if (submitGrantBtn) submitGrantBtn.addEventListener('click', () => this.grantPermission());

        const cancelGrantBtn = document.getElementById('cancelGrantPermBtn');
        if (cancelGrantBtn) cancelGrantBtn.addEventListener('click', () => this.hideGrantForm());
    },

    // Groups CRUD

    async loadGroups() {
        try {
            const resp = await fetch('/api/v1/groups', { credentials: 'include' });
            const data = await resp.json();
            if (data.success) {
                this.groups = data.data || [];
                this.renderGroups();
            }
        } catch (err) {
            console.error('[GroupsView] Failed to load groups:', err);
        }
    },

    renderGroups() {
        const container = document.getElementById('groupsList');
        if (!container) return;

        if (this.groups.length === 0) {
            container.innerHTML = '<div class="no-data" style="padding: 1rem; text-align: center; color: var(--text-muted);">No groups created yet.</div>';
            return;
        }

        let html = '<table class="users-table"><thead><tr>';
        html += '<th>Group</th><th>Members</th><th>Created</th><th></th>';
        html += '</tr></thead><tbody>';

        this.groups.forEach(g => {
            html += `<tr class="group-row" data-group-id="${Utils.escapeHtml(g.id)}" onclick="GroupsView.openDetail('${Utils.escapeJs(g.id)}')">
                <td>
                    <div class="group-cell">
                        <div class="group-name">${Utils.escapeHtml(g.name)}</div>
                        ${g.description ? `<div class="text-muted" style="font-size:0.75rem;">${Utils.escapeHtml(g.description)}</div>` : ''}
                    </div>
                </td>
                <td><span class="role-badge">${g.member_count || 0}</span></td>
                <td class="text-muted">${new Date(g.created_at).toLocaleDateString()}</td>
                <td class="kebab-cell" onclick="event.stopPropagation()">
                    <div class="kebab-wrapper">
                        <button class="kebab-btn" onclick="KebabMenu.toggle(event,this)">⋮</button>
                        <div class="kebab-menu">
                            <button class="kebab-item danger" onclick="GroupsView.deleteGroupById('${Utils.escapeJs(g.id)}')">Delete</button>
                        </div>
                    </div>
                </td>
            </tr>`;
        });

        html += '</tbody></table>';
        container.innerHTML = html;
    },

    showCreateForm() {
        const modal = document.getElementById('createGroupModal');
        if (modal) modal.style.display = 'flex';
        setTimeout(() => document.getElementById('newGroupName')?.focus(), 100);
    },

    hideCreateForm() {
        const modal = document.getElementById('createGroupModal');
        if (modal) modal.style.display = 'none';
        const nameInput = document.getElementById('newGroupName');
        const descInput = document.getElementById('newGroupDescription');
        if (nameInput) nameInput.value = '';
        if (descInput) descInput.value = '';
    },

    async createGroup() {
        const name = document.getElementById('newGroupName')?.value.trim();
        const description = document.getElementById('newGroupDescription')?.value.trim();

        if (!name) {
            if (window.Toast) Toast.error('Error', 'Group name is required');
            return;
        }

        try {
            const resp = await fetch('/api/v1/groups', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ name, description })
            });
            const data = await resp.json();
            if (data.success) {
                this.hideCreateForm();
                await this.loadGroups();
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to create group');
            }
        } catch (err) {
            console.error('[GroupsView] createGroup error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    async openDetail(groupId, fromRoute = false) {
        this.selectedGroup = this.groups.find(g => g.id === groupId);
        if (!this.selectedGroup) return;

        // Push a history entry so browser/mouse/keyboard back returns to the list.
        // Suppressed when restoring from the URL (popstate/deep-link) to avoid dupes.
        if (!fromRoute) window.App?.pushSubPath('groups/' + groupId);

        const panel = document.getElementById('groupDetailPanel');
        const list = document.getElementById('groupsListView');
        if (panel) panel.style.display = 'block';
        if (list) list.style.display = 'none';

        this._renderDetailHeader();

        await this.loadMembers();
    },

    // Group state for the add-members modal
    _availableUsers: [],
    _selectedToAdd: null,

    closeDetail() {
        const panel = document.getElementById('groupDetailPanel');
        const list = document.getElementById('groupsListView');
        if (panel) panel.style.display = 'none';
        if (list) list.style.display = '';
        this.selectedGroup = null;
    },

    // Renders the detail header name/description as plain text (placeholder when empty).
    _renderDetailHeader() {
        if (!this.selectedGroup) return;
        const nameEl = document.getElementById('groupDetailName');
        const descEl = document.getElementById('groupDetailDescription');
        if (nameEl) nameEl.textContent = this.selectedGroup.name;
        if (descEl) {
            const d = this.selectedGroup.description;
            descEl.textContent = d || 'Add a description';
            descEl.classList.toggle('is-empty', !d);
        }
    },

    // Click-to-edit: swaps the header text for an input that saves on Enter/blur.
    _startInlineEdit(el, field, placeholder) {
        if (!this.selectedGroup || el.classList.contains('editing')) return;
        el.classList.add('editing');

        const current = field === 'name' ? this.selectedGroup.name : (this.selectedGroup.description || '');
        const input = document.createElement('input');
        input.type = 'text';
        input.className = field === 'name' ? 'inline-edit-input inline-edit-name' : 'inline-edit-input inline-edit-desc';
        input.value = current;
        input.placeholder = placeholder || '';
        el.textContent = '';
        el.appendChild(input);
        input.focus();
        input.select();

        let done = false;
        const finish = async (save) => {
            if (done) return;
            done = true;
            el.classList.remove('editing');
            const val = input.value.trim();
            if (save && val !== current && (field !== 'name' || val)) {
                await this._saveGroupField(field, val);
            }
            this._renderDetailHeader();
        };

        input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') { e.preventDefault(); finish(true); }
            else if (e.key === 'Escape') { e.preventDefault(); finish(false); }
        });
        input.addEventListener('blur', () => finish(true));
    },

    async _saveGroupField(field, val) {
        const name = field === 'name' ? val : this.selectedGroup.name;
        const description = field === 'description' ? val : (this.selectedGroup.description || '');
        if (!name) {
            if (window.Toast) Toast.error('Error', 'Group name is required');
            return;
        }
        try {
            const resp = await fetch(`/api/v1/groups/${this.selectedGroup.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ name, description })
            });
            const data = await resp.json();
            if (data.success) {
                this.selectedGroup.name = name;
                this.selectedGroup.description = description;
                await this.loadGroups();
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to update group');
            }
        } catch (err) {
            console.error('[GroupsView] saveGroupField error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    async deleteGroup() {
        if (!this.selectedGroup) return;
        const ok = await this._deleteGroup(this.selectedGroup.id, this.selectedGroup.name);
        if (ok) {
            this.closeDetail();
            window.App?.pushSubPath('groups');
        }
    },

    async deleteGroupById(id) {
        const g = this.groups.find(x => x.id === id);
        if (!g) return;
        await this._deleteGroup(id, g.name);
    },

    async _deleteGroup(id, name) {
        if (!confirm(`Delete group "${name}"? This will remove all members and fractal permissions associated with this group.`)) {
            return false;
        }
        try {
            const resp = await fetch(`/api/v1/groups/${id}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await resp.json();
            if (data.success) {
                await this.loadGroups();
                return true;
            }
            if (window.Toast) Toast.error('Error', data.error || 'Failed to delete group');
            return false;
        } catch (err) {
            console.error('[GroupsView] deleteGroup error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
            return false;
        }
    },

    // Group Members

    async loadMembers() {
        if (!this.selectedGroup) return;

        const container = document.getElementById('groupMembersList');
        if (!container) return;

        try {
            const resp = await fetch(`/api/v1/groups/${this.selectedGroup.id}/members`, { credentials: 'include' });
            const data = await resp.json();
            if (data.success) {
                this.renderMembers(data.data || []);
            }
        } catch (err) {
            console.error('[GroupsView] loadMembers error:', err);
            container.innerHTML = '<div class="text-muted">Failed to load members</div>';
        }
    },

    renderMembers(members) {
        const container = document.getElementById('groupMembersList');
        if (!container) return;

        if (members.length === 0) {
            container.innerHTML = '<div class="no-data" style="padding: 0.75rem; text-align: center; color: var(--text-muted);">No members yet. Add users above.</div>';
            return;
        }

        let html = '<table class="users-table"><thead><tr><th>User</th><th>Added</th><th></th></tr></thead><tbody>';
        members.forEach(m => {
            html += `<tr>
                <td>
                    <div class="user-cell">
                        <div class="gravatar" style="background-color: ${m.gravatar_color || '#666'}">
                            ${m.gravatar_initial || m.username[0].toUpperCase()}
                        </div>
                        <div class="user-info">
                            <div class="user-name">${Utils.escapeHtml(m.display_name || m.username)}</div>
                            <div class="user-username">@${Utils.escapeHtml(m.username)}</div>
                        </div>
                    </div>
                </td>
                <td class="text-muted">${new Date(m.added_at).toLocaleDateString()}</td>
                <td><button class="btn-delete-user btn-sm" onclick="GroupsView.removeMember('${Utils.escapeJs(m.username)}')">Remove</button></td>
            </tr>`;
        });
        html += '</tbody></table>';
        container.innerHTML = html;
    },

    showAddMembers() {
        if (!this.selectedGroup) return;
        this._selectedToAdd = new Set();
        const search = document.getElementById('memberPickerSearch');
        if (search) search.value = '';
        const modal = document.getElementById('addMembersModal');
        if (modal) modal.style.display = 'flex';
        this.loadAvailableUsers();
        setTimeout(() => search?.focus(), 100);
    },

    hideAddMembers() {
        const modal = document.getElementById('addMembersModal');
        if (modal) modal.style.display = 'none';
    },

    async loadAvailableUsers() {
        const listEl = document.getElementById('memberPickerList');
        if (!listEl || !this.selectedGroup) return;
        listEl.innerHTML = '<div class="loading">Loading...</div>';

        try {
            const resp = await fetch('/api/v1/users', { credentials: 'include' });
            const data = await resp.json();
            if (!data.success) {
                listEl.innerHTML = '<div class="text-muted" style="padding:0.75rem;">Failed to load users</div>';
                return;
            }

            // Exclude users already in the group
            const membersResp = await fetch(`/api/v1/groups/${this.selectedGroup.id}/members`, { credentials: 'include' });
            const membersData = await membersResp.json();
            const currentMembers = (membersData.success && membersData.data) ? membersData.data.map(m => m.username) : [];

            this._availableUsers = (data.data || []).filter(u => !currentMembers.includes(u.username));
            this.renderMemberPicker();
        } catch (err) {
            console.error('[GroupsView] loadAvailableUsers error:', err);
            listEl.innerHTML = '<div class="text-muted" style="padding:0.75rem;">Failed to load users</div>';
        }
    },

    renderMemberPicker() {
        const listEl = document.getElementById('memberPickerList');
        if (!listEl) return;

        const q = (document.getElementById('memberPickerSearch')?.value || '').toLowerCase().trim();
        const users = this._availableUsers.filter(u =>
            !q || (u.display_name || '').toLowerCase().includes(q) || u.username.toLowerCase().includes(q));

        if (users.length === 0) {
            const msg = this._availableUsers.length === 0 ? 'All users are already members.' : 'No matching users.';
            listEl.innerHTML = `<div class="no-data" style="padding:0.75rem;text-align:center;color:var(--text-muted);">${msg}</div>`;
            return;
        }

        listEl.innerHTML = users.map(u => {
            const checked = this._selectedToAdd.has(u.username) ? 'checked' : '';
            const initial = (u.display_name || u.username)[0].toUpperCase();
            return `<label class="member-picker-item">
                <div class="gravatar" style="background-color:${u.gravatar_color || '#666'}">${Utils.escapeHtml(initial)}</div>
                <div class="user-info">
                    <div class="user-name">${Utils.escapeHtml(u.display_name || u.username)}</div>
                    <div class="user-username">@${Utils.escapeHtml(u.username)}</div>
                </div>
                <input type="checkbox" value="${Utils.escapeHtml(u.username)}" ${checked} onchange="GroupsView.toggleMemberSelection('${Utils.escapeJs(u.username)}', this.checked)">
            </label>`;
        }).join('');
    },

    toggleMemberSelection(username, checked) {
        if (!this._selectedToAdd) this._selectedToAdd = new Set();
        if (checked) this._selectedToAdd.add(username);
        else this._selectedToAdd.delete(username);
    },

    async submitAddMembers() {
        if (!this.selectedGroup) return;
        const usernames = this._selectedToAdd ? Array.from(this._selectedToAdd) : [];
        if (usernames.length === 0) {
            if (window.Toast) Toast.error('Error', 'Select at least one user');
            return;
        }

        let added = 0;
        for (const username of usernames) {
            try {
                const resp = await fetch(`/api/v1/groups/${this.selectedGroup.id}/members`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({ username })
                });
                const data = await resp.json();
                if (data.success) added++;
                else if (window.Toast) Toast.error('Error', `${username}: ${data.error || 'failed to add'}`);
            } catch (err) {
                console.error('[GroupsView] addMember error:', err);
            }
        }

        if (added > 0) {
            this.selectedGroup.member_count = (this.selectedGroup.member_count || 0) + added;
            await this.loadMembers();
            await this.loadGroups();
        }
        this.hideAddMembers();
    },

    async removeMember(username) {
        if (!this.selectedGroup) return;

        if (!confirm(`Remove ${username} from this group?`)) return;

        try {
            const resp = await fetch(`/api/v1/groups/${this.selectedGroup.id}/members/${encodeURIComponent(username)}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await resp.json();
            if (data.success) {
                await this.loadMembers();
                await this.loadGroups();
                if (this.selectedGroup) {
                    this.selectedGroup.member_count = Math.max(0, (this.selectedGroup.member_count || 1) - 1);
                }
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to remove member');
            }
        } catch (err) {
            console.error('[GroupsView] removeMember error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    // Fractal Permissions (used in fractal manage tab)

    async loadPermissions(fractalId) {
        const container = document.getElementById('fractalPermissionsList');
        if (!container) return;

        try {
            const resp = await fetch(`/api/v1/fractals/${fractalId}/permissions`, { credentials: 'include' });
            const data = await resp.json();
            if (data.success) {
                this.renderPermissions(data.data || [], fractalId);
            } else {
                container.innerHTML = '<div class="text-muted">Failed to load permissions</div>';
            }
        } catch (err) {
            console.error('[GroupsView] loadPermissions error:', err);
            container.innerHTML = '<div class="text-muted">Failed to load permissions</div>';
        }
    },

    renderPermissions(permissions, fractalId) {
        const container = document.getElementById('fractalPermissionsList');
        if (!container) return;

        if (permissions.length === 0) {
            container.innerHTML = '<div class="access-empty-state">No permissions granted yet. Only tenant admins can access this fractal.</div>';
            return;
        }

        let html = '<table class="access-table"><thead><tr><th>User / Group</th><th>Role</th><th class="col-actions">Actions</th></tr></thead><tbody>';
        permissions.forEach(p => {
            const isGroup = !!p.group_id;
            const label = isGroup
                ? `<span class="perm-type-badge perm-type-group">Group</span> ${Utils.escapeHtml(p.display_name || p.group_id)}`
                : `<span class="perm-type-badge perm-type-user">User</span> ${Utils.escapeHtml(p.display_name || p.username)}`;

            html += `<tr>
                <td>${label}</td>
                <td>
                    <select class="setting-select perm-role-select" data-perm-id="${Utils.escapeHtml(p.id)}" data-fractal-id="${Utils.escapeHtml(fractalId)}" onchange="GroupsView.updatePermissionRole(this)">
                        <option value="viewer" ${p.role === 'viewer' ? 'selected' : ''}>viewer</option>
                        <option value="analyst" ${p.role === 'analyst' ? 'selected' : ''}>analyst</option>
                        <option value="admin" ${p.role === 'admin' ? 'selected' : ''}>admin</option>
                    </select>
                </td>
                <td class="col-actions"><button class="btn-delete-user btn-sm" onclick="GroupsView.revokePermission('${Utils.escapeJs(p.id)}', '${Utils.escapeJs(fractalId)}')">Revoke</button></td>
            </tr>`;
        });
        html += '</tbody></table>';
        container.innerHTML = html;
    },

    async updatePermissionRole(selectEl) {
        const permId = selectEl.dataset.permId;
        const fractalId = selectEl.dataset.fractalId;
        const role = selectEl.value;

        try {
            const resp = await fetch(`/api/v1/fractals/${fractalId}/permissions/${permId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ role })
            });
            const data = await resp.json();
            if (data.success) {
                // Role updated visually via the select
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to update role');
                await this.loadPermissions(fractalId);
            }
        } catch (err) {
            console.error('[GroupsView] updatePermissionRole error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    async revokePermission(permId, fractalId) {
        if (!confirm('Revoke this permission?')) return;

        try {
            const resp = await fetch(`/api/v1/fractals/${fractalId}/permissions/${permId}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await resp.json();
            if (data.success) {
                await this.loadPermissions(fractalId);
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to revoke permission');
            }
        } catch (err) {
            console.error('[GroupsView] revokePermission error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    // Grant permission form

    showGrantForm() {
        const form = document.getElementById('grantPermissionForm');
        if (form) form.style.display = 'block';
        this.loadGrantFormOptions();
    },

    hideGrantForm() {
        const form = document.getElementById('grantPermissionForm');
        if (form) form.style.display = 'none';
        const typeSelect = document.getElementById('permGrantType');
        if (typeSelect) typeSelect.value = 'user';
        const roleSelect = document.getElementById('permRoleSelect');
        if (roleSelect) roleSelect.value = 'viewer';
        this.toggleGrantType();
    },

    toggleGrantType() {
        const type = document.getElementById('permGrantType')?.value;
        const userGroup = document.getElementById('permUserSelectGroup');
        const groupGroup = document.getElementById('permGroupSelectGroup');
        if (userGroup) userGroup.style.display = type === 'user' ? '' : 'none';
        if (groupGroup) groupGroup.style.display = type === 'group' ? '' : 'none';
    },

    async loadGrantFormOptions() {
        const fractalId = this.getCurrentFractalId();
        if (!fractalId) return;

        // Load users
        const userSelect = document.getElementById('permUserSelect');
        if (userSelect) {
            try {
                const resp = await fetch('/api/v1/users', { credentials: 'include' });
                const data = await resp.json();
                userSelect.innerHTML = '<option value="">Select user...</option>';
                if (data.success) {
                    (data.data || []).forEach(u => {
                        const opt = document.createElement('option');
                        opt.value = u.username;
                        opt.textContent = `${u.display_name} (@${u.username})`;
                        userSelect.appendChild(opt);
                    });
                }
            } catch (err) {
                console.error('[GroupsView] loadGrantFormOptions users error:', err);
            }
        }

        // Load groups
        const groupSelect = document.getElementById('permGroupSelect');
        if (groupSelect) {
            try {
                const resp = await fetch('/api/v1/groups', { credentials: 'include' });
                const data = await resp.json();
                groupSelect.innerHTML = '<option value="">Select group...</option>';
                if (data.success) {
                    (data.data || []).forEach(g => {
                        const opt = document.createElement('option');
                        opt.value = g.id;
                        opt.textContent = `${g.name} (${g.member_count || 0} members)`;
                        groupSelect.appendChild(opt);
                    });
                }
            } catch (err) {
                console.error('[GroupsView] loadGrantFormOptions groups error:', err);
            }
        }
    },

    async grantPermission() {
        const fractalId = this.getCurrentFractalId();
        if (!fractalId) return;

        const type = document.getElementById('permGrantType')?.value;
        const role = document.getElementById('permRoleSelect')?.value;

        let body = { role };
        if (type === 'user') {
            const username = document.getElementById('permUserSelect')?.value;
            if (!username) {
                if (window.Toast) Toast.error('Error', 'Select a user');
                return;
            }
            body.username = username;
        } else {
            const groupId = document.getElementById('permGroupSelect')?.value;
            if (!groupId) {
                if (window.Toast) Toast.error('Error', 'Select a group');
                return;
            }
            body.group_id = groupId;
        }

        try {
            const resp = await fetch(`/api/v1/fractals/${fractalId}/permissions`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(body)
            });
            const data = await resp.json();
            if (data.success) {
                this.hideGrantForm();
                await this.loadPermissions(fractalId);
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to grant permission');
            }
        } catch (err) {
            console.error('[GroupsView] grantPermission error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    getCurrentFractalId() {
        if (window.FractalContext && window.FractalContext.currentFractal) {
            return window.FractalContext.currentFractal.id;
        }
        return null;
    },

    // Prism Permissions

    getCurrentPrismId() {
        if (window.FractalManageTab && window.FractalManageTab.currentPrismId) {
            return window.FractalManageTab.currentPrismId;
        }
        return null;
    },

    async loadPrismPermissions(prismId) {
        const container = document.getElementById('prismPermissionsList');
        if (!container) return;

        try {
            const resp = await fetch(`/api/v1/prisms/${prismId}/permissions`, { credentials: 'include' });
            const data = await resp.json();
            if (data.success) {
                this.renderPrismPermissions((data.data && data.data.permissions) || [], prismId);
            } else {
                container.innerHTML = '<div class="text-muted">Failed to load permissions</div>';
            }
        } catch (err) {
            console.error('[GroupsView] loadPrismPermissions error:', err);
            container.innerHTML = '<div class="text-muted">Failed to load permissions</div>';
        }
    },

    renderPrismPermissions(permissions, prismId) {
        const container = document.getElementById('prismPermissionsList');
        if (!container) return;

        if (permissions.length === 0) {
            container.innerHTML = '<div class="no-data" style="padding: 0.75rem; text-align: center; color: var(--text-muted);">No permissions granted. Only tenant admins can access this prism.</div>';
            return;
        }

        let html = '<table class="users-table"><thead><tr><th>User / Group</th><th>Role</th><th></th></tr></thead><tbody>';
        permissions.forEach(p => {
            const isGroup = !!p.group_id;
            const label = isGroup
                ? `<span class="perm-type-badge perm-type-group">Group</span> ${Utils.escapeHtml(p.display_name || p.group_id)}`
                : `<span class="perm-type-badge perm-type-user">User</span> ${Utils.escapeHtml(p.display_name || p.username)}`;

            html += `<tr>
                <td>${label}</td>
                <td>
                    <select class="setting-select perm-role-select" data-perm-id="${Utils.escapeHtml(p.id)}" data-prism-id="${Utils.escapeHtml(prismId)}" onchange="GroupsView.updatePrismPermissionRole(this)">
                        <option value="viewer" ${p.role === 'viewer' ? 'selected' : ''}>viewer</option>
                        <option value="analyst" ${p.role === 'analyst' ? 'selected' : ''}>analyst</option>
                        <option value="admin" ${p.role === 'admin' ? 'selected' : ''}>admin</option>
                    </select>
                </td>
                <td><button class="btn-delete-user btn-sm" onclick="GroupsView.revokePrismPermission('${Utils.escapeJs(p.id)}', '${Utils.escapeJs(prismId)}')">Revoke</button></td>
            </tr>`;
        });
        html += '</tbody></table>';
        container.innerHTML = html;
    },

    async updatePrismPermissionRole(selectEl) {
        const permId = selectEl.dataset.permId;
        const prismId = selectEl.dataset.prismId;
        const role = selectEl.value;

        try {
            const resp = await fetch(`/api/v1/prisms/${prismId}/permissions/${permId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ role })
            });
            const data = await resp.json();
            if (data.success) {
                // Role updated visually via the select
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to update role');
                await this.loadPrismPermissions(prismId);
            }
        } catch (err) {
            console.error('[GroupsView] updatePrismPermissionRole error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    async revokePrismPermission(permId, prismId) {
        if (!confirm('Revoke this permission?')) return;

        try {
            const resp = await fetch(`/api/v1/prisms/${prismId}/permissions/${permId}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await resp.json();
            if (data.success) {
                await this.loadPrismPermissions(prismId);
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to revoke permission');
            }
        } catch (err) {
            console.error('[GroupsView] revokePrismPermission error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    showPrismGrantForm() {
        const form = document.getElementById('grantPrismPermissionForm');
        if (form) form.style.display = 'block';
        this.loadPrismGrantFormOptions();
    },

    hidePrismGrantForm() {
        const form = document.getElementById('grantPrismPermissionForm');
        if (form) form.style.display = 'none';
        const typeSelect = document.getElementById('prismPermGrantType');
        if (typeSelect) typeSelect.value = 'user';
        const roleSelect = document.getElementById('prismPermRoleSelect');
        if (roleSelect) roleSelect.value = 'viewer';
        this.togglePrismGrantType();
    },

    togglePrismGrantType() {
        const type = document.getElementById('prismPermGrantType')?.value;
        const userGroup = document.getElementById('prismPermUserSelectGroup');
        const groupGroup = document.getElementById('prismPermGroupSelectGroup');
        if (userGroup) userGroup.style.display = type === 'user' ? '' : 'none';
        if (groupGroup) groupGroup.style.display = type === 'group' ? '' : 'none';
    },

    async loadPrismGrantFormOptions() {
        const userSelect = document.getElementById('prismPermUserSelect');
        if (userSelect) {
            try {
                const resp = await fetch('/api/v1/users', { credentials: 'include' });
                const data = await resp.json();
                userSelect.innerHTML = '<option value="">Select user...</option>';
                if (data.success) {
                    (data.data || []).forEach(u => {
                        const opt = document.createElement('option');
                        opt.value = u.username;
                        opt.textContent = `${u.display_name} (@${u.username})`;
                        userSelect.appendChild(opt);
                    });
                }
            } catch (err) {
                console.error('[GroupsView] loadPrismGrantFormOptions users error:', err);
            }
        }

        const groupSelect = document.getElementById('prismPermGroupSelect');
        if (groupSelect) {
            try {
                const resp = await fetch('/api/v1/groups', { credentials: 'include' });
                const data = await resp.json();
                groupSelect.innerHTML = '<option value="">Select group...</option>';
                if (data.success) {
                    (data.data || []).forEach(g => {
                        const opt = document.createElement('option');
                        opt.value = g.id;
                        opt.textContent = `${g.name} (${g.member_count || 0} members)`;
                        groupSelect.appendChild(opt);
                    });
                }
            } catch (err) {
                console.error('[GroupsView] loadPrismGrantFormOptions groups error:', err);
            }
        }
    },

    async grantPrismPermission() {
        const prismId = this.getCurrentPrismId();
        if (!prismId) return;

        const type = document.getElementById('prismPermGrantType')?.value;
        const role = document.getElementById('prismPermRoleSelect')?.value;

        let body = { role };
        if (type === 'user') {
            const username = document.getElementById('prismPermUserSelect')?.value;
            if (!username) {
                if (window.Toast) Toast.error('Error', 'Select a user');
                return;
            }
            body.username = username;
        } else {
            const groupId = document.getElementById('prismPermGroupSelect')?.value;
            if (!groupId) {
                if (window.Toast) Toast.error('Error', 'Select a group');
                return;
            }
            body.group_id = groupId;
        }

        try {
            const resp = await fetch(`/api/v1/prisms/${prismId}/permissions`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(body)
            });
            const data = await resp.json();
            if (data.success) {
                this.hidePrismGrantForm();
                await this.loadPrismPermissions(prismId);
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to grant permission');
            }
        } catch (err) {
            console.error('[GroupsView] grantPrismPermission error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    }
};

window.GroupsView = GroupsView;

document.addEventListener('DOMContentLoaded', () => {
    GroupsView.init();
});
