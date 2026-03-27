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

        const closeBtn = document.getElementById('closeGroupDetailBtn');
        if (closeBtn) closeBtn.addEventListener('click', () => this.closeDetail());

        const editBtn = document.getElementById('editGroupBtn');
        if (editBtn) editBtn.addEventListener('click', () => this.editGroup());

        const deleteBtn = document.getElementById('deleteGroupBtn');
        if (deleteBtn) deleteBtn.addEventListener('click', () => this.deleteGroup());

        const addMemberBtn = document.getElementById('groupAddMemberBtn');
        if (addMemberBtn) addMemberBtn.addEventListener('click', () => this.addMember());

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
            html += `<tr class="group-row" data-group-id="${Utils.escapeHtml(g.id)}">
                <td>
                    <div class="group-cell">
                        <div class="group-name">${Utils.escapeHtml(g.name)}</div>
                        ${g.description ? `<div class="text-muted" style="font-size:0.75rem;">${Utils.escapeHtml(g.description)}</div>` : ''}
                    </div>
                </td>
                <td><span class="role-badge">${g.member_count || 0}</span></td>
                <td class="text-muted">${new Date(g.created_at).toLocaleDateString()}</td>
                <td><button class="btn-secondary btn-sm" onclick="GroupsView.openDetail('${Utils.escapeJs(g.id)}')">Manage</button></td>
            </tr>`;
        });

        html += '</tbody></table>';
        container.innerHTML = html;
    },

    showCreateForm() {
        const form = document.getElementById('createGroupForm');
        if (form) form.style.display = 'block';
        document.getElementById('newGroupName')?.focus();
    },

    hideCreateForm() {
        const form = document.getElementById('createGroupForm');
        if (form) form.style.display = 'none';
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

    async openDetail(groupId) {
        this.selectedGroup = this.groups.find(g => g.id === groupId);
        if (!this.selectedGroup) return;

        const panel = document.getElementById('groupDetailPanel');
        const list = document.getElementById('groupsList');
        if (panel) panel.style.display = 'block';
        if (list) list.style.display = 'none';

        document.getElementById('groupDetailName').textContent = this.selectedGroup.name;
        document.getElementById('groupDetailDescription').textContent = this.selectedGroup.description || '';

        await this.loadMembers();
        await this.loadUsersForMemberSelect();
    },

    closeDetail() {
        const panel = document.getElementById('groupDetailPanel');
        const list = document.getElementById('groupsList');
        if (panel) panel.style.display = 'none';
        if (list) list.style.display = '';
        this.selectedGroup = null;
    },

    async editGroup() {
        if (!this.selectedGroup) return;

        const newName = prompt('Group name:', this.selectedGroup.name);
        if (newName === null) return;
        const newDesc = prompt('Description:', this.selectedGroup.description || '');
        if (newDesc === null) return;

        if (!newName.trim()) {
            if (window.Toast) Toast.error('Error', 'Group name is required');
            return;
        }

        try {
            const resp = await fetch(`/api/v1/groups/${this.selectedGroup.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ name: newName.trim(), description: newDesc.trim() })
            });
            const data = await resp.json();
            if (data.success) {
                this.selectedGroup.name = newName.trim();
                this.selectedGroup.description = newDesc.trim();
                document.getElementById('groupDetailName').textContent = this.selectedGroup.name;
                document.getElementById('groupDetailDescription').textContent = this.selectedGroup.description || '';
                await this.loadGroups();
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to update group');
            }
        } catch (err) {
            console.error('[GroupsView] editGroup error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
    },

    async deleteGroup() {
        if (!this.selectedGroup) return;

        if (!confirm(`Delete group "${this.selectedGroup.name}"? This will remove all members and fractal permissions associated with this group.`)) {
            return;
        }

        try {
            const resp = await fetch(`/api/v1/groups/${this.selectedGroup.id}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await resp.json();
            if (data.success) {
                this.closeDetail();
                await this.loadGroups();
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to delete group');
            }
        } catch (err) {
            console.error('[GroupsView] deleteGroup error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
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

    async loadUsersForMemberSelect() {
        const select = document.getElementById('groupAddMemberSelect');
        if (!select) return;

        try {
            const resp = await fetch('/api/v1/users', { credentials: 'include' });
            const data = await resp.json();
            if (!data.success) return;

            // Also get current members to exclude them
            const membersResp = await fetch(`/api/v1/groups/${this.selectedGroup.id}/members`, { credentials: 'include' });
            const membersData = await membersResp.json();
            const currentMembers = (membersData.success && membersData.data) ? membersData.data.map(m => m.username) : [];

            select.innerHTML = '<option value="">Select user...</option>';
            (data.data || []).forEach(u => {
                if (!currentMembers.includes(u.username)) {
                    const opt = document.createElement('option');
                    opt.value = u.username;
                    opt.textContent = `${u.display_name} (@${u.username})`;
                    select.appendChild(opt);
                }
            });
        } catch (err) {
            console.error('[GroupsView] loadUsersForMemberSelect error:', err);
        }
    },

    async addMember() {
        if (!this.selectedGroup) return;
        const select = document.getElementById('groupAddMemberSelect');
        const username = select?.value;
        if (!username) {
            if (window.Toast) Toast.error('Error', 'Select a user to add');
            return;
        }

        try {
            const resp = await fetch(`/api/v1/groups/${this.selectedGroup.id}/members`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ username })
            });
            const data = await resp.json();
            if (data.success) {
                await this.loadMembers();
                await this.loadUsersForMemberSelect();
                // Update member count in list
                if (this.selectedGroup) {
                    this.selectedGroup.member_count = (this.selectedGroup.member_count || 0) + 1;
                }
            } else {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to add member');
            }
        } catch (err) {
            console.error('[GroupsView] addMember error:', err);
            if (window.Toast) Toast.error('Error', 'Network error');
        }
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
                await this.loadUsersForMemberSelect();
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
