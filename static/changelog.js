function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function parseChangeColumn(col) {
    if (col.startsWith('Release Date ')) return { change: 'Date Changed', detail: col.slice(13).replace(' -> ', ' → ') };
    if (col.startsWith('Release Type ')) return { change: 'Type Changed', detail: col.slice(13).replace(' -> ', ' → ') };
    if (col.startsWith('Release Status ')) return { change: 'Status Changed', detail: col.slice(15).replace(' -> ', ' → ') };
    if (col.startsWith('Name ')) return { change: 'Name Changed', detail: col.slice(5).replace(' -> ', ' → ') };
    if (col.startsWith('Semester ')) return { change: 'Semester Changed', detail: col.slice(9).replace(' -> ', ' → ') };
    if (col.startsWith('Workload ')) return { change: 'Workload Changed', detail: col.slice(9).replace(' -> ', ' → ') };
    if (col === 'Removed from Roadmap') return { change: 'Removed', detail: 'Removed from Roadmap', removed: true };
    if (col === 'Restored to Roadmap') return { change: 'Restored', detail: 'Restored to Roadmap' };
    if (col === 'Added to roadmap') return { change: 'Added', detail: 'Added to roadmap' };
    return { change: col, detail: '' };
}

class Changelog {
    constructor() {
        this.dataVersion = '';
        this.init();
    }

    async init() {
        await this.fetchVersion();
        await this.loadFilterOptions();
        this.restoreFiltersFromUrl();
        this.bindEvents();
        await this.loadChangelog();
    }

    async fetchVersion() {
        try {
            const response = await fetch('/api/version');
            const data = await response.json();
            this.dataVersion = data.version || '';
        } catch (e) {
            this.dataVersion = '';
        }
    }

    async loadFilterOptions() {
        try {
            const response = await fetch('/api/filter-options');
            const data = await response.json();
            this.populateSelect('product-filter', data.product_names);
            this.populateSelect('type-filter', data.release_types);
            this.populateSelect('status-filter', data.release_statuses);
            this.restoreFiltersFromUrl();
        } catch (e) {
            console.error('Error loading filter options:', e);
        }
    }

    populateSelect(id, values) {
        const select = document.getElementById(id);
        if (!select) return;
        values.forEach(val => {
            const option = document.createElement('option');
            option.value = val;
            option.textContent = val;
            select.appendChild(option);
        });
    }

    getActiveFilters() {
        return {
            days: document.getElementById('days-select').value,
            product_name: document.getElementById('product-filter').value,
            release_type: document.getElementById('type-filter').value,
            release_status: document.getElementById('status-filter').value,
        };
    }

    syncFiltersToUrl() {
        const filters = this.getActiveFilters();
        const params = new URLSearchParams();
        if (filters.days && filters.days !== '30') params.set('days', filters.days);
        if (filters.product_name) params.set('product_name', filters.product_name);
        if (filters.release_type) params.set('release_type', filters.release_type);
        if (filters.release_status) params.set('release_status', filters.release_status);
        const qs = params.toString();
        history.replaceState(null, '', qs ? `/changelog?${qs}` : '/changelog');
    }

    restoreFiltersFromUrl() {
        const params = new URLSearchParams(location.search);
        const map = {
            'days': 'days-select',
            'product_name': 'product-filter',
            'release_type': 'type-filter',
            'release_status': 'status-filter',
        };
        for (const [param, elId] of Object.entries(map)) {
            const val = params.get(param);
            const el = document.getElementById(elId);
            if (val && el) el.value = val;
        }
    }

    bindEvents() {
        const selects = ['days-select', 'product-filter', 'type-filter', 'status-filter'];
        selects.forEach(id => {
            const el = document.getElementById(id);
            if (!el) return;
            el.addEventListener('change', () => {
                this.syncFiltersToUrl();
                this.loadChangelog();
            });
        });

        const clearBtn = document.getElementById('clear-filters');
        if (clearBtn) {
            clearBtn.addEventListener('click', (e) => {
                e.preventDefault();
                document.getElementById('days-select').value = '30';
                document.getElementById('product-filter').value = '';
                document.getElementById('type-filter').value = '';
                document.getElementById('status-filter').value = '';
                this.syncFiltersToUrl();
                this.loadChangelog();
            });
        }
    }

    async loadChangelog() {
        const container = document.getElementById('changelog-content');
        container.innerHTML = '<div class="loading-indicator"><div class="spinner"></div><span>Loading changelog...</span></div>';

        const filters = this.getActiveFilters();
        const params = new URLSearchParams();
        params.set('days', filters.days || '30');
        params.set('include_inactive', 'true');
        if (filters.product_name) params.set('product_name', filters.product_name);
        if (filters.release_type) params.set('release_type', filters.release_type);
        if (filters.release_status) params.set('release_status', filters.release_status);
        if (this.dataVersion) params.set('v', this.dataVersion);

        try {
            const response = await fetch(`/api/changelog?${params.toString()}`);
            const data = await response.json();
            this.render(data.days || []);
        } catch (e) {
            container.innerHTML = '<div class="no-results"><p>Error loading changelog. Please try again later.</p></div>';
        }
    }

    render(days) {
        const container = document.getElementById('changelog-content');

        if (!days.length) {
            container.innerHTML = '<div class="no-results"><p>No changes found in the selected time period.</p></div>';
            return;
        }

        let html = '';
        for (const day of days) {
            const dateObj = new Date(day.date);
            const dateStr = dateObj.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });

            html += `<div class="changelog-day">
                <div class="changelog-day-header">
                    <h2 class="changelog-date">${escapeHtml(dateStr)}</h2>
                    <span class="changelog-count">· ${day.count} change${day.count !== 1 ? 's' : ''}</span>
                </div>`;

            // Group by workload
            const workloads = {};
            for (const item of day.items) {
                const wl = item.product_name || 'Unknown';
                if (!workloads[wl]) workloads[wl] = [];
                workloads[wl].push(item);
            }

            for (const [workload, items] of Object.entries(workloads).sort((a, b) => a[0].localeCompare(b[0]))) {
                html += `<div class="changelog-workload">
                    <h3 class="changelog-workload-name">${escapeHtml(workload)}</h3>`;

                for (const item of items) {
                    const inactiveClass = item.active === false ? ' changelog-item-inactive' : '';
                    const cols = item.changed_columns && item.changed_columns.length ? item.changed_columns : null;

                    if (cols) {
                        for (const col of cols) {
                            const parsed = parseChangeColumn(col);
                            const removedClass = parsed.removed ? ' changelog-cell-removed' : '';
                            html += `<a href="/release/${escapeHtml(item.release_item_id)}" class="changelog-row${inactiveClass}">
                                <div class="changelog-cell-name">${escapeHtml(item.feature_name)}</div>
                                <div class="changelog-cell-change${removedClass}">${escapeHtml(parsed.change)}</div>
                                <div class="changelog-cell-detail">${escapeHtml(parsed.detail)}</div>
                            </a>`;
                        }
                    } else {
                        html += `<a href="/release/${escapeHtml(item.release_item_id)}" class="changelog-row${inactiveClass}">
                            <div class="changelog-cell-name">${escapeHtml(item.feature_name)}</div>
                            <div class="changelog-cell-change">Updated</div>
                            <div class="changelog-cell-detail"></div>
                        </a>`;
                    }
                }

                html += '</div>';
            }

            html += '</div>';
        }

        container.innerHTML = html;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new Changelog();
});
