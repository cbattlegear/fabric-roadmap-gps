class Changelog {
    constructor() {
        this.dataVersion = '';
        this.catchUp = false;
        this.requestId = 0;
        this.loadedCheckpoint = null;
        try {
            this.checkpoint = new CatchUpCheckpoint(window.localStorage, window.sessionStorage);
        } catch (error) {
            this.checkpoint = { available: false, baseline: null, error: 'Browser storage is disabled. The normal changelog still works.' };
        }
        this.init();
    }

    async init() {
        await this.fetchVersion();
        await this.loadFilterOptions();
        this.views = new RoadmapViews.Controller('/changelog', () => {
            this.restoreFiltersFromUrl();
            return this.loadChangelog();
        });
        this.restoreFiltersFromUrl();
        this.bindEvents();
        this.showCheckpoint();
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
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            this.populateSelect('product-filter', data.product_names);
            this.populateSelect('type-filter', data.release_types);
            this.populateSelect('status-filter', data.release_statuses);
        } catch (e) {
            console.error('Error loading filter options:', e);
            this.filterError = 'Filter choices could not be loaded. Showing all products, types, and statuses; reload to retry.';
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
        const params = new URLSearchParams(location.search);
        if (this.catchUp) params.set('catch_up', '1');
        const qs = params.toString();
        history.replaceState(null, '', qs ? `/changelog?${qs}` : '/changelog');
    }

    restoreFiltersFromUrl() {
        const params = new URLSearchParams(location.search);
        this.catchUp = params.get('catch_up') === '1' && !!this.checkpoint.baseline;
        this.views.restore(location.search);
    }

    bindEvents() {
        document.getElementById('catch-up-toggle').addEventListener('click', () => {
            this.catchUp = !this.catchUp;
            this.syncFiltersToUrl();
            this.showCheckpoint();
            this.loadChangelog();
        });
        document.getElementById('checkpoint-reset').addEventListener('click', () => {
            try {
                this.checkpoint.reset();
                location.reload();
            } catch (error) {
                document.getElementById('checkpoint-status').textContent = 'Could not reset browser storage. Enable storage and try again.';
            }
        });
        document.getElementById('checkpoint-mark').addEventListener('click', () => {
            const status = document.getElementById('checkpoint-status');
            try {
                if (!this.loadedCheckpoint) throw new Error('Load the complete catch-up view first.');
                const warning = this.checkpoint.acknowledge(this.loadedCheckpoint);
                this.showCheckpoint();
                status.textContent = warning || `Saved ${this.loadedCheckpoint} UTC for your next session. This tab keeps the baseline shown above. Close this tab to start a new session.`;
            } catch (error) {
                status.textContent = `Checkpoint not saved: ${error.message}`;
            }
        });
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
        this.showCheckpoint();
        const requestId = ++this.requestId;
        this.loadedCheckpoint = null;
        document.getElementById('checkpoint-mark').disabled = true;
        document.getElementById('catch-up-summary').textContent = '';
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
        const catchUp = this.catchUp;
        const initial = !this.checkpoint.baseline && this.checkpoint.available && !this.checkpoint.error;
        if (catchUp || initial) {
            params.set('since', this.checkpoint.baseline || this.checkpoint.today);
            params.set('include_inactive', 'true');
            // Retrieve the entire checkpoint range before allowing acknowledgement.
            for (const key of ['product_name', 'release_type', 'release_status', 'days', 'v']) params.delete(key);
        }

        try {
            const response = await fetch(`/api/changelog?${params.toString()}`, { signal: request.signal });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            if (requestId !== this.requestId) return;
            if (!Array.isArray(data.days)) throw new Error('Invalid changelog response');
            if (catchUp || initial) {
                if (data.complete !== true || !this.checkpoint.validDay(data.through) || data.since !== params.get('since')) {
                    throw new Error('Incomplete catch-up response');
                }
                this.loadedCheckpoint = data.through;
            }
            if (catchUp) {
                const days = data.days.map(day => ({
                    ...day,
                    items: day.items.filter(item =>
                        (filters.include_inactive !== 'false' || item.active !== false) &&
                        (!filters.product_name || item.product_name === filters.product_name) &&
                        (!filters.release_type || item.release_type === filters.release_type) &&
                        (!filters.release_status || item.release_status === filters.release_status))
                })).filter(day => day.items.length);
                for (const day of days) day.count = day.items.length;
                const items = days.flatMap(day => day.items);
                const features = new Set(items.map(item => item.release_item_id)).size;
                document.getElementById('catch-up-summary').textContent = `${features} features changed in ${items.length} change events with these filters. Mark as seen acknowledges all products, types, and statuses through ${data.through} UTC, not just these filters.`;
                this.render(days, true);
            } else if (initial) {
                const normal = await fetch(`/api/changelog?${new URLSearchParams(filters)}`, { signal: request.signal });
                if (!normal.ok) throw new Error(`HTTP ${normal.status}`);
                const normalData = await normal.json();
                if (requestId !== this.requestId) return;
                if (!Array.isArray(normalData.days)) throw new Error('Invalid changelog response');
                this.render(normalData.days);
            } else {
                this.render(data.days);
            }
            document.getElementById('checkpoint-mark').disabled = !this.loadedCheckpoint;
        } catch (e) {
            if (requestId !== this.requestId || e.name === 'AbortError') return;
            this.loadedCheckpoint = null;
            document.getElementById('checkpoint-mark').disabled = true;
            container.innerHTML = '<div class="no-results"><p>Error loading changelog. Please try again later.</p></div>';
            document.getElementById('checkpoint-status').textContent = 'Load failed. Your checkpoint has not changed. Reload or change a filter to retry.';
        }
    }

    showCheckpoint() {
        const cp = this.checkpoint;
        document.getElementById('checkpoint-baseline').textContent = cp.baseline
            ? `Session baseline: ${cp.baseline} UTC (included).`
            : 'No saved baseline. Start from today after the changelog loads; existing features are not all new.';
        document.getElementById('checkpoint-status').textContent = cp.error || this.filterError ||
            (cp.baseline && !this.catchUp ? 'Choose Changes since my last visit to load the full catch-up range before marking it as seen.' : '');
        const toggle = document.getElementById('catch-up-toggle');
        toggle.disabled = !cp.baseline;
        toggle.setAttribute('aria-pressed', String(this.catchUp));
        document.getElementById('days-select').disabled = this.catchUp;
        document.getElementById('checkpoint-mark').textContent = cp.baseline ? 'Mark all changes as seen' : 'Start from today';
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
