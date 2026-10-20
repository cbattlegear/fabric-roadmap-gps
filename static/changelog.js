class Changelog {
    constructor() {
        this.requestId = 0;
        this.snapshot = null;
        try {
            this.visit = new ChangelogVisit(window.localStorage, navigator.locks);
        } catch (error) {
            this.visit = new ChangelogVisit(null, null);
        }
        this.init();
    }

    async init() {
        await this.loadFilterOptions();
        this.views = new RoadmapViews.Controller('/changelog', () => {
            this.restoreFiltersFromUrl();
            return this.loadChangelog();
        });
        this.restoreFiltersFromUrl();
        this.bindEvents();
        await this.loadChangelog();
    }

    async loadFilterOptions() {
        try {
            const response = await fetch('/api/filter-options');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            this.populateSelect('product-filter', data.product_names);
            this.populateSelect('type-filter', data.release_types);
            this.populateSelect('status-filter', data.release_statuses);
        } catch (error) {
            this.filterError = 'Filter choices could not be loaded. Reload to retry.';
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
        for (const id of ['days-select', 'product-filter', 'type-filter', 'status-filter', 'include-inactive']) {
            document.getElementById(id).addEventListener('change', () => {
                this.syncFiltersToUrl();
                this.loadChangelog();
            });
        }
        document.getElementById('clear-filters').addEventListener('click', event => {
            event.preventDefault();
            document.getElementById('days-select').value = '30';
            document.getElementById('product-filter').value = '';
            document.getElementById('type-filter').value = '';
            document.getElementById('status-filter').value = '';
            document.getElementById('include-inactive').checked = true;
            this.syncFiltersToUrl();
            this.loadChangelog();
        });
    }

    filteredDays(snapshot, filters) {
        const firstDay = new Date(snapshot.cutoff.slice(0, 10) + 'T00:00:00Z');
        firstDay.setUTCDate(firstDay.getUTCDate() - Number(filters.days || 30));
        const start = firstDay.toISOString().slice(0, 10);
        return snapshot.data.days.filter(day => day.date >= start).map(day => ({
            ...day,
            items: day.items.filter(item =>
                (filters.include_inactive !== 'false' || item.active !== false) &&
                (!filters.product_name || item.product_name === filters.product_name) &&
                (!filters.release_type || item.release_type === filters.release_type) &&
                (!filters.release_status || item.release_status === filters.release_status))
                .map(item => ({ ...item, unread: this.visit.isNew(item) })),
        })).filter(day => day.items.length).map(day => ({ ...day, count: day.items.length }));
    }

    async loadChangelog() {
        const requestId = ++this.requestId;
        const container = document.getElementById('changelog-content');
        const status = document.getElementById('visit-status');
        status.textContent = this.filterError || this.visit.error || '';
        if (this.snapshot) {
            this.render(this.filteredDays(this.snapshot, this.getActiveFilters()), true);
            return;
        }
        container.innerHTML = '<div class="loading-indicator"><div class="spinner"></div><span>Loading changelog...</span></div>';
        try {
            // One complete, unfiltered snapshot per document, even if filters
            // change while the first request is still in flight.
            this.snapshotRequest ||= fetch('/api/changelog?visit=1', { cache: 'no-store', credentials: 'omit' })
                .then(async response => {
                    if (!response.ok) throw new Error(`HTTP ${response.status}`);
                    return this.visit.snapshot(await response.json());
                });
            const snapshot = await this.snapshotRequest;
            if (requestId !== this.requestId) return;
            this.render(this.filteredDays(snapshot, this.getActiveFilters()), true);
            this.snapshot = snapshot;
            if (!this.filterError) await this.visit.acknowledge(snapshot);
            status.textContent = this.filterError || this.visit.error || '';
        } catch (error) {
            if (requestId !== this.requestId) return;
            this.snapshotRequest = null;
            status.textContent = 'Visit tracking could not be updated. Reload to retry.';
            // The ordinary API remains usable if a complete manifest cannot
            // be fetched/validated. Never acknowledge this partial fallback.
            try {
                const response = await fetch(`/api/changelog?${new URLSearchParams(this.getActiveFilters())}`);
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                const data = await response.json();
                if (!Array.isArray(data.days)) throw new Error('Invalid changelog response');
                if (requestId === this.requestId) this.render(data.days);
            } catch (fallbackError) {
                if (requestId === this.requestId) {
                    container.innerHTML = '<div class="no-results"><p>Error loading changelog. Please reload to retry.</p></div>';
                }
            }
        }
    }

    render(days, events = false) {
        const container = document.getElementById('changelog-content');
        if (!days.length) {
            container.innerHTML = '<div class="no-results"><p>No changes found in the selected time period.</p></div>';
            return;
        }
        container.innerHTML = renderChangelogDays(days, { dayLinks: true, events });
        attachCopyHandlers(container);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new Changelog();
});
