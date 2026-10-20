class Changelog {
    constructor() {
        this.dataVersion = '';
        this.init();
    }

    async init() {
        await this.fetchVersion();
        await this.loadFilterOptions();
        this.views = new RoadmapViews.Controller('/changelog', () => this.loadChangelog());
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
        return this.views.capture();
    }

    syncFiltersToUrl() {
        this.views.sync();
    }

    restoreFiltersFromUrl() {
        this.views.restore(location.search);
    }

    bindEvents() {
        const selects = ['days-select', 'product-filter', 'type-filter', 'status-filter', 'include-inactive'];
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
                document.getElementById('include-inactive').checked = true;
                this.syncFiltersToUrl();
                this.loadChangelog();
            });
        }
    }

    async loadChangelog() {
        if (this.request) this.request.abort();
        const request = new AbortController();
        this.request = request;
        const container = document.getElementById('changelog-content');
        container.innerHTML = '<div class="loading-indicator"><div class="spinner"></div><span>Loading changelog...</span></div>';

        const filters = this.getActiveFilters();
        const params = new URLSearchParams();
        params.set('days', filters.days || '30');
        params.set('include_inactive', filters.include_inactive);
        if (filters.product_name) params.set('product_name', filters.product_name);
        if (filters.release_type) params.set('release_type', filters.release_type);
        if (filters.release_status) params.set('release_status', filters.release_status);
        if (this.dataVersion) params.set('v', this.dataVersion);

        try {
            const response = await fetch(`/api/changelog?${params.toString()}`, { signal: request.signal });
            const data = await response.json();
            if (request !== this.request) return;
            this.render(data.days || []);
        } catch (e) {
            if (request !== this.request || e.name === 'AbortError') return;
            container.innerHTML = '<div class="no-results"><p>Error loading changelog. Please try again later.</p></div>';
        }
    }

    render(days) {
        const container = document.getElementById('changelog-content');

        if (!days.length) {
            container.innerHTML = '<div class="no-results"><p>No changes found in the selected time period.</p></div>';
            return;
        }

        container.innerHTML = renderChangelogDays(days, { dayLinks: true });
        attachCopyHandlers(container);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new Changelog();
});
