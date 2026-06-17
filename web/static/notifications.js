const Notifications = {
    _pollInterval: null,
    _open: false,

    init() {
        document.addEventListener('click', (e) => {
            if (this._open) {
                const bell = document.getElementById('notificationBell');
                if (bell && !bell.contains(e.target)) {
                    this._closeDropdown();
                }
            }
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this._open) this._closeDropdown();
        });

        this._startPolling();
    },

    _startPolling() {
        this._stopPolling();
        this.pollCount();
        this._pollInterval = setInterval(() => this.pollCount(), 60000);
    },

    _stopPolling() {
        if (this._pollInterval) {
            clearInterval(this._pollInterval);
            this._pollInterval = null;
        }
    },

    async pollCount() {
        try {
            const resp = await fetch('/api/v1/notifications/count', { credentials: 'include' });
            if (!resp.ok) return;
            const data = await resp.json();
            if (data.success && data.data != null) {
                this._updateBadge(data.data.unread_count);
            }
        } catch (_) {}
    },

    _updateBadge(count) {
        const badge = document.getElementById('notificationBadge');
        const btn   = document.getElementById('notificationBellBtn');
        if (!badge || !btn) return;
        if (count > 0) {
            badge.textContent = count > 99 ? '99+' : String(count);
            badge.style.display = 'block';
            btn.classList.add('has-unread');
        } else {
            badge.style.display = 'none';
            btn.classList.remove('has-unread');
        }
    },

    async toggle() {
        if (this._open) {
            this._closeDropdown();
        } else {
            this._openDropdown();
        }
    },

    _openDropdown() {
        this._open = true;
        const dd = document.getElementById('notificationDropdown');
        if (dd) dd.style.display = 'flex';
        this._loadList();
    },

    _closeDropdown() {
        this._open = false;
        const dd = document.getElementById('notificationDropdown');
        if (dd) dd.style.display = 'none';
    },

    async _loadList() {
        const listEl = document.getElementById('notificationList');
        if (!listEl) return;
        listEl.innerHTML = '<div class="notification-empty">Loading…</div>';
        try {
            const resp = await fetch('/api/v1/notifications', { credentials: 'include' });
            if (!resp.ok) throw new Error('request failed');
            const data = await resp.json();
            if (!data.success) throw new Error(data.error || 'unknown error');
            const { notifications, unread_count } = data.data;
            this._renderList(notifications || [], listEl);
            this._updateBadge(unread_count || 0);
        } catch (_) {
            listEl.innerHTML = '<div class="notification-empty">Failed to load notifications.</div>';
        }
    },

    _renderList(items, listEl) {
        if (!items.length) {
            listEl.innerHTML = '<div class="notification-empty">No notifications in the last 24 hours</div>';
            return;
        }
        listEl.innerHTML = items.map(item => {
            const unreadClass = item.read ? '' : ' unread';
            const severity    = Utils.escapeHtml(item.severity || 'info');
            const title       = Utils.escapeHtml(item.title || '');
            const message     = item.message ? Utils.escapeHtml(item.message) : '';
            const timeStr     = this._relativeTime(new Date(item.created_at));
            return `<div class="notification-item${unreadClass}">
                <div class="notification-severity-dot ${severity}"></div>
                <div class="notification-item-body">
                    <div class="notification-item-title">${title}</div>
                    ${message ? `<div class="notification-item-message">${message}</div>` : ''}
                    <div class="notification-item-time">${timeStr}</div>
                </div>
            </div>`;
        }).join('');
    },

    async markAllRead() {
        try {
            await fetch('/api/v1/notifications/read', {
                method: 'POST',
                credentials: 'include',
            });
        } catch (_) {}
        this._updateBadge(0);
        document.querySelectorAll('#notificationList .notification-item').forEach(el => {
            el.classList.remove('unread');
        });
    },

    _relativeTime(date) {
        const diffMs  = Date.now() - date.getTime();
        const diffMin = Math.floor(diffMs / 60000);
        if (diffMin < 1)  return 'just now';
        if (diffMin < 60) return `${diffMin}m ago`;
        const diffHr = Math.floor(diffMin / 60);
        if (diffHr < 24)  return `${diffHr}h ago`;
        return `${Math.floor(diffHr / 24)}d ago`;
    },
};

window.Notifications = Notifications;
