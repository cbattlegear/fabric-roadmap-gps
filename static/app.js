const md = new markdownit({html: true});

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function descriptionPrepareHtml(text) {
    if (!text) return 'No description available.';
    let escaped = escapeHtml(text);
    let unescapeStr = escaped.replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'");

    unescapeStr = unescapeStr.replace('###', '');

    // Convert line breaks to <br>
    unescapeStr = unescapeStr.replace(/\n/g, '<br>');
    unescapeStr = md.render(unescapeStr);
    return unescapeStr;
}

// Redirect old hash-based release URLs to new route
function checkHashRedirect() {
    if (location.hash.startsWith('#release/')) {
        const id = location.hash.split('#release/')[1];
        if (id) window.location.replace(`/release/${id}`);
    }
}

class RecentChanges {
    constructor() {
        this.filterOptions = {};
        this.currentData = [];
        this.debounceTimer = null;
        this.minSearchLength = 2;
        this.page = 1;
        this.pageSize = 15;
        this.pagination = null;
        this.loadingMore = false;
        this.allPagesLoaded = false;
        this.observer = null;
        this.hasLoadedOnce = false;
        this.dataVersion = '';
        this.init();
    }

    async init() {
        await this.fetchVersion();
        await this.loadFilterOptions();
        this.restoreFiltersFromUrl();
        await this.loadRecentChanges({ reset: true });
        this.bindEvents();
        this.setupInfiniteScroll();
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
            this.filterOptions = await response.json();
            this.populateFilterSelects();
        } catch (error) {
            console.error('Error loading filter options:', error);
        }
    }

    populateFilterSelects() {
        const productSelect = document.getElementById('product-filter');
        const typeSelect = document.getElementById('type-filter');
        const statusSelect = document.getElementById('status-filter');

        this.filterOptions.product_names.forEach(product => {
            const option = document.createElement('option');
            option.value = product;
            option.textContent = product;
            productSelect.appendChild(option);
        });

        this.filterOptions.release_types.forEach(type => {
            const option = document.createElement('option');
            option.value = type;
            option.textContent = type;
            typeSelect.appendChild(option);
        });

        this.filterOptions.release_statuses.forEach(status => {
            const option = document.createElement('option');
            option.value = status;
            option.textContent = status;
            statusSelect.appendChild(option);
        });
    }

    async loadRecentChanges(options = {}) {
        const { reset = false } = options;
        const filters = this.getActiveFilters();

        if (reset) {
            this.page = 1;
            this.currentData = [];
            this.allPagesLoaded = false;
        }

        if (this.loadingMore || this.allPagesLoaded) return;

        const isInitialLoad = this.page === 1 && !this.hasLoadedOnce;

        this.loadingMore = true;

        if (this.page > 1) {
            this.toggleBottomLoading(true);
        } else if (isInitialLoad) {
            this.showLoading(true);
        } else {
            this.toggleBottomLoading(true);
        }

        try {
            const params = new URLSearchParams();
            if (filters.product_name) params.append('product_name', filters.product_name);
            if (filters.release_type) params.append('release_type', filters.release_type);
            if (filters.release_status) params.append('release_status', filters.release_status);
            if (filters.modified_within_days) params.append('modified_within_days', filters.modified_within_days);
            if (filters.sort && filters.sort !== 'last_modified') params.append('sort', filters.sort);
            const hasSearchQuery = filters.q && String(filters.q).trim().length >= this.minSearchLength;
            if (hasSearchQuery) {
                params.append('q', String(filters.q).trim());
                this.toggleSearchSpinner(true);
            }
            params.append('page', String(this.page));
            params.append('page_size', String(this.pageSize));
            params.append('include_inactive', 'true');
            if (this.dataVersion) params.append('v', this.dataVersion);

            const response = await fetch(`/api/releases?${params.toString()}`);

            const payload = await response.json();
            const items = Array.isArray(payload) ? payload : (payload.data || []);

            if (this.page === 1) {
                this.currentData = items;
            } else {
                this.currentData = this.currentData.concat(items);
            }

            this.pagination = payload.pagination || null;

            if (this.pagination && this.page > this.pagination.total_pages) {
                this.page = this.pagination.total_pages || 1;
                this.loadingMore = false;
                return this.loadRecentChanges({ reset: true });
            }

            if (this.pagination && !this.pagination.has_next) {
                this.allPagesLoaded = true;
            }

            this.renderChanges();

            if (this.pagination && this.pagination.has_next) {
                this.page += 1;
            }

            if (!this.hasLoadedOnce && this.currentData.length > 0) {
                this.hasLoadedOnce = true;
            }
        } catch (error) {
            console.error('Error loading recent changes:', error);
            if (isInitialLoad) {
                this.showError();
            }
        } finally {
            this.toggleSearchSpinner(false);
            if (isInitialLoad) {
                this.showLoading(false);
            }
            this.toggleBottomLoading(false);
            this.loadingMore = false;
            this.updateLoadMoreButton();
        }
    }

    renderChanges() {
        const container = document.getElementById('changes-grid');
        const noResults = document.getElementById('no-results');

        if (this.currentData.length === 0) {
            container.innerHTML = '';
            noResults.style.display = 'block';
            return;
        }

        noResults.style.display = 'none';
        container.innerHTML = this.currentData.map(item => this.createChangeItem(item)).join('');
        container.querySelectorAll('.change-item').forEach(card => {
            card.addEventListener('click', () => {
                const id = card.getAttribute('data-id');
                window.location.href = `/release/${id}`;
            });
        });
    }

    createChangeItem(item) {
        const date = new Date(item.last_modified).toLocaleDateString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric'
        });

        // release_date is delivered as a "Q# YYYY" quarter token from the
        // API (post-2026 source change). Display it verbatim.
        const releaseDate = item.release_date || 'TBD';

        const releaseTypeClass = item.release_type == 'General availability' ? 'badge-success' : 'badge-warning';
        const releaseStatusClass = item.release_status == 'Shipped' ? 'badge-success' : 'badge-warning';
        const isInactive = item.active === false;
        const inactiveClass = isInactive ? ' change-item-inactive' : '';
        const removedBadge = isInactive ? '<span class="change-badge badge-removed">Removed</span>' : '';

        return `
            <div class="change-item${inactiveClass}" data-id="${escapeHtml(item.release_item_id)}" role="button" tabindex="0" aria-label="View details for ${escapeHtml(item.feature_name || 'feature')}">
                <div class="change-header">
                    <h3 class="change-title">${escapeHtml(item.feature_name || 'Unnamed Feature')}</h3>
                    <div class="change-badges">
                        ${removedBadge}
                        <span class="change-badge badge-product">${escapeHtml(item.product_name || 'Unknown')}</span>
                        <span class="change-badge ${releaseTypeClass}">${escapeHtml(item.release_type || 'Unknown')}</span>
                        <span class="change-badge ${releaseStatusClass}">${escapeHtml(item.release_status || 'Unknown')}</span>
                    </div>
                </div>
                <div class="change-meta">
                    <span class="change-date">Last Modified: ${date}</span>
                    <span class="release-date">Release Date: ${releaseDate}</span>
                </div>
                <div class="change-description">${descriptionPrepareHtml(item.feature_description || 'No description available.')}</div>
                <div style="margin-top:.5rem; font-size:.75rem; display: flex; justify-content: space-between; align-items: center;">
                    <div class="change-link-inline" style="color: var(--fabric-primary);">Click for details ▸</div>
                    ${item.blog_title && item.blog_url ? `<a href="${escapeHtml(item.blog_url)}" target="_blank" rel="noopener noreferrer" style="color: var(--fabric-secondary); text-decoration: none; display: flex; align-items: center; gap: .25rem;" onclick="event.stopPropagation();" title="${escapeHtml(item.blog_title)}">📚 Related blog</a>` : ''}
                </div>
            </div>`;
    }

    bindEvents() {
        const applyBtn = document.getElementById('apply-filters');
        const clearBtn = document.getElementById('clear-filters');
        const loadMoreBtn = document.getElementById('load-more-btn');

        const searchInput = document.getElementById('search-input');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.debounceSearch();
            });
        }

        const autoSelectIds = ['product-filter','type-filter','status-filter','days-filter','sort-select'];
        autoSelectIds.forEach(id => {
            const el = document.getElementById(id);
            if (!el) return;
            el.addEventListener('change', () => {
                if (this.loadingMore) return;
                if (this._filterChangeTimer) clearTimeout(this._filterChangeTimer);
                this._filterChangeTimer = setTimeout(() => {
                    this.syncFiltersToUrl();
                    this.loadRecentChanges({ reset: true });
                }, 60);
            });
        });

        clearBtn.addEventListener('click', () => {
            this.clearFilters();
            this.loadRecentChanges({ reset: true });
        });

        if (loadMoreBtn) {
            loadMoreBtn.addEventListener('click', () => {
                this.loadRecentChanges({ reset: false });
            });
        }

        document.querySelectorAll('.filter-select').forEach(select => {
            select.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.loadRecentChanges({ reset: true });
                }
            });
        });
    }

    getActiveFilters() {
        return {
            product_name: document.getElementById('product-filter').value,
            release_type: document.getElementById('type-filter').value,
            release_status: document.getElementById('status-filter').value,
            modified_within_days: document.getElementById('days-filter').value,
            sort: document.getElementById('sort-select').value,
            q: (document.getElementById('search-input') && document.getElementById('search-input').value) || ''
        };
    }

    clearFilters() {
        document.getElementById('product-filter').value = '';
        document.getElementById('type-filter').value = '';
        document.getElementById('status-filter').value = '';
        document.getElementById('days-filter').value = '';
        document.getElementById('sort-select').value = 'last_modified';
        const si = document.getElementById('search-input');
        if (si) si.value = '';
        this.page = 1;
        this.allPagesLoaded = false;
        this.currentData = [];
        this.syncFiltersToUrl();
    }

    syncFiltersToUrl() {
        const filters = this.getActiveFilters();
        const params = new URLSearchParams();
        if (filters.product_name) params.set('product_name', filters.product_name);
        if (filters.release_type) params.set('release_type', filters.release_type);
        if (filters.release_status) params.set('release_status', filters.release_status);
        if (filters.modified_within_days) params.set('modified_within_days', filters.modified_within_days);
        if (filters.sort && filters.sort !== 'last_modified') params.set('sort', filters.sort);
        if (filters.q && filters.q.trim().length >= this.minSearchLength) params.set('q', filters.q.trim());
        const qs = params.toString();
        history.replaceState(null, '', qs ? `/?${qs}` : '/');
    }

    restoreFiltersFromUrl() {
        const params = new URLSearchParams(location.search);
        const map = {
            'product_name': 'product-filter',
            'release_type': 'type-filter',
            'release_status': 'status-filter',
            'modified_within_days': 'days-filter',
            'sort': 'sort-select',
            'q': 'search-input'
        };
        for (const [param, elId] of Object.entries(map)) {
            const val = params.get(param);
            const el = document.getElementById(elId);
            if (val && el) el.value = val;
        }
    }

    updateLoadMoreButton() {
        const btn = document.getElementById('load-more-btn');
        if (!btn) return;
        if (this.observer) {
            btn.style.display = this.allPagesLoaded ? 'none' : 'none';
        } else {
            btn.style.display = this.allPagesLoaded ? 'none' : 'inline-block';
            btn.disabled = this.loadingMore;
        }
    }

    toggleBottomLoading(show) {
        const el = document.getElementById('loading-more-indicator');
        if (!el) return;
        el.style.display = show ? 'block' : 'none';
    }

    toggleSearchSpinner(show) {
        const el = document.getElementById('search-spinner');
        if (!el) return;
        el.classList.toggle('active', show);
    }

    showLoading(show) {
        const loader = document.getElementById('loading-indicator');
        const grid = document.getElementById('changes-grid');
        const noResults = document.getElementById('no-results');

        if (show) {
            loader.style.display = 'flex';
            if (!this.hasLoadedOnce) {
                grid.style.display = 'none';
                noResults.style.display = 'none';
            }
        } else {
            loader.style.display = 'none';
            grid.style.display = 'grid';
        }
    }

    showError() {
        const container = document.getElementById('changes-grid');
        const noResults = document.getElementById('no-results');

        container.innerHTML = '';
        noResults.innerHTML = '<p>Error loading changes. Please try again later.</p>';
        noResults.style.display = 'block';
    }

    debounceSearch(delay = 300) {
        if (this.debounceTimer) clearTimeout(this.debounceTimer);
        this.debounceTimer = setTimeout(() => {
            const filters = this.getActiveFilters();
            if (filters.q && String(filters.q).trim().length > 0 && String(filters.q).trim().length < this.minSearchLength) {
                return;
            }
            this.syncFiltersToUrl();
            this.loadRecentChanges({ reset: true });
        }, delay);
    }

    setupInfiniteScroll() {
        const sentinel = document.getElementById('infinite-scroll-sentinel');
        if (!('IntersectionObserver' in window) || !sentinel) return;
        this.observer = new IntersectionObserver(entries => {
            for (const entry of entries) {
                if (entry.isIntersecting) {
                    this.loadRecentChanges({ reset: false });
                }
            }
        }, { root: null, rootMargin: '0px 0px 300px 0px', threshold: 0 });
        this.observer.observe(sentinel);
        this.updateLoadMoreButton();
    }

    maybePreloadNext() {
        if (this.allPagesLoaded || this.loadingMore) return;
        const sentinel = document.getElementById('infinite-scroll-sentinel');
        if (!sentinel) return;
        const rect = sentinel.getBoundingClientRect();
        if (rect.top < window.innerHeight + 300) {
            this.loadRecentChanges({ reset: false });
        }
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    checkHashRedirect();
    new RecentChanges();
});
