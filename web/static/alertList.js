// Shared chrome for the two alert tables (manual and feed). Both render the
// same header, row shell, pagination and empty state so they read as one
// surface; only the middle columns differ.
const AlertList = {
    SEVERITY_ORDER: ['critical', 'high', 'medium', 'low', 'informational'],

    severityRank(sev) {
        const i = this.SEVERITY_ORDER.indexOf((sev || 'medium').toLowerCase());
        return i === -1 ? this.SEVERITY_ORDER.length : i;
    },

    // Count line plus page-size selector.
    renderTableHeader({ shown, total, unfiltered, pageSize, onPageSize }) {
        const filteredNote = unfiltered && unfiltered !== total
            ? ` (filtered from ${unfiltered} total)`
            : '';
        return `
            <div class="alerts-table-header">
                <div class="alerts-count">Showing ${shown} of ${total} alerts${filteredNote}</div>
                <div class="alerts-page-size">
                    <label>Show:</label>
                    <select onchange="${onPageSize}(this.value)">
                        ${[10, 25, 50, 100].map(n =>
                            `<option value="${n}" ${pageSize === n ? 'selected' : ''}>${n}</option>`
                        ).join('')}
                    </select>
                </div>
            </div>`;
    },

    // cols: [{ key, label, sortable }] - key is passed back to onSort.
    renderColumns(cols, { onSort, sortColumn, sortDirection }) {
        return cols.map(c => {
            if (!c.sortable) return `<th>${c.label}</th>`;
            const indicator = sortColumn === c.key
                ? (sortDirection === 'asc' ? ' &#9650;' : ' &#9660;')
                : '';
            return `<th class="sortable-th" onclick="${onSort}('${c.key}')">${c.label}${indicator}</th>`;
        }).join('');
    },

    // The name cell is identical in both tables; cells are the columns after it.
    renderRow(alert, cells) {
        const isAutoDisabled = !alert.enabled && alert.disabled_reason;
        const statusClass = isAutoDisabled ? 'auto-disabled' : (alert.enabled ? 'enabled' : 'disabled');
        const statusText = isAutoDisabled ? 'Auto-disabled' : (alert.enabled ? 'Enabled' : 'Disabled');

        return `
            <tr class="alert-row ${statusClass}" data-alert-id="${alert.id}">
                <td class="alert-name">
                    <div class="alert-name-row">
                        <span class="status-dot status-${statusClass}" title="${statusText}"></span>
                        <strong>${Utils.escapeHtml(alert.name)}</strong>
                        ${isAutoDisabled ? '<span class="alert-auto-disabled-badge">timeout</span>' : ''}
                    </div>
                    ${alert.description ? `<div class="alert-description-preview">${Utils.escapeHtml(alert.description.substring(0, 60))}${alert.description.length > 60 ? '...' : ''}</div>` : ''}
                </td>
                ${cells.join('')}
            </tr>`;
    },

    severityCell(alert, onFilter) {
        const sev = (alert.severity || 'medium').toLowerCase();
        const cls = sev === 'info' ? 'informational' : sev;
        return `<td class="alert-severity-cell"><span class="severity-pill severity-${cls}" onclick="event.stopPropagation(); ${onFilter}('${sev}')" title="Filter by ${sev}">${sev}</span></td>`;
    },

    // Three pills then an overflow count, so a rule with a long ATT&CK tag list
    // cannot blow out the row height.
    labelsCell(labels, onFilter, truncate = 24) {
        if (!labels || labels.length === 0) {
            return '<td class="alert-labels-cell"><span class="alert-labels-wrap"><span class="text-muted">-</span></span></td>';
        }
        const max = 3;
        const short = l => l.length > truncate ? l.substring(0, truncate - 2) + '..' : l;
        let html = labels.slice(0, max).map(l =>
            `<span class="label-pill" style="--chip-color:${Utils.tagColorFor(l)}" onclick="event.stopPropagation(); ${onFilter}('${Utils.escapeHtml(l).replace(/'/g, "\\'")}')" title="${Utils.escapeHtml(l)}">${Utils.escapeHtml(short(l))}</span>`
        ).join('');
        if (labels.length > max) {
            html += `<span class="label-pill label-pill-more" title="${Utils.escapeHtml(labels.slice(max).join(', '))}">+${labels.length - max}</span>`;
        }
        return `<td class="alert-labels-cell"><span class="alert-labels-wrap">${html}</span></td>`;
    },

    execTimeCell(alert) {
        const ms = alert.last_execution_time_ms;
        const text = ms != null ? (ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${ms}ms`) : '-';
        const slow = ms != null && ms >= 3000 ? 'alert-run-time-slow' : '';
        return `<td class="alert-run-time ${slow}">${text}</td>`;
    },

    lastTriggeredCell(alert) {
        return `<td class="alert-triggered">${alert.last_triggered ? TZ.format(alert.last_triggered, 'friendly') : 'Never'}</td>`;
    },

    // Numbered pages with ellipsis, driven by a total so it serves the manual
    // table's client-side paging and the feed table's server-side paging alike.
    renderPagination({ current, totalPages, onPage }) {
        if (totalPages <= 1) return '<div class="alerts-pagination" style="display: none;"></div>';

        const maxVisible = 5;
        let start = Math.max(1, current - Math.floor(maxVisible / 2));
        let end = Math.min(totalPages, start + maxVisible - 1);
        if (end - start + 1 < maxVisible) start = Math.max(1, end - maxVisible + 1);

        const btn = (page, label, extra = '') =>
            `<button onclick="${onPage}(${page})" class="pagination-btn${extra}">${label}</button>`;
        const dead = label => `<button class="pagination-btn disabled">${label}</button>`;

        let html = '<div class="alerts-pagination">';
        html += current > 1 ? btn(current - 1, '&lsaquo;') : dead('&lsaquo;');

        if (start > 1) {
            html += btn(1, '1');
            if (start > 2) html += '<span class="pagination-ellipsis">...</span>';
        }
        for (let i = start; i <= end; i++) {
            html += i === current ? `<button class="pagination-btn active">${i}</button>` : btn(i, String(i));
        }
        if (end < totalPages) {
            if (end < totalPages - 1) html += '<span class="pagination-ellipsis">...</span>';
            html += btn(totalPages, String(totalPages));
        }

        html += current < totalPages ? btn(current + 1, '&rsaquo;') : dead('&rsaquo;');
        return html + '</div>';
    },

    renderEmptyState({ filtered, noun, onClear, hint }) {
        return `
            <div class="alerts-table-container">
                <div class="empty-state">
                    <div class="empty-text">${filtered ? `No ${noun} match your filters` : `No ${noun} found`}</div>
                    ${filtered
                        ? `<div class="empty-actions"><button onclick="${onClear}()" class="btn-secondary">Clear Filters</button></div>`
                        : (hint ? `<div class="empty-hint">${hint}</div>` : '')}
                </div>
            </div>`;
    }
};

window.AlertList = AlertList;
