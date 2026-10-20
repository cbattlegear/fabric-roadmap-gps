(function (root) {
    'use strict';

    const STORAGE_KEY = 'fabric-gps.saved-views';
    const MAX_VIEWS = 20;
    const MAX_NAME = 80;
    const common = {
        product_name: ['product-filter', ''],
        release_type: ['type-filter', ''],
        release_status: ['status-filter', ''],
        include_inactive: ['include-inactive', 'true', ['true', 'false']],
    };
    const contracts = {
        '/': {
            ...common,
            modified_within_days: ['days-filter', '', ['', '7', '14', '30']],
            sort: ['sort-select', 'last_modified', ['last_modified', 'release_date']],
            q: ['search-input', ''],
        },
        '/changelog': {
            ...common,
            days: ['days-select', '30', ['7', '14', '30', '60', '90']],
        },
    };

    function normalize(path, state) {
        if (!Object.hasOwn(contracts, path)) throw new Error('Unsupported view page.');
        const filters = {};
        const invalid = [];
        for (const [key, [, fallback, allowed]] of Object.entries(contracts[path])) {
            const value = state[key] === undefined ? fallback : state[key];
            if (typeof value !== 'string' || (allowed && !allowed.includes(value))) {
                filters[key] = fallback;
                invalid.push(key);
            } else {
                filters[key] = value;
            }
        }
        return { filters, invalid };
    }

    function parse(path, search) {
        return normalize(path, Object.fromEntries(new URLSearchParams(search)));
    }

    function serialize(path, state) {
        const { filters } = normalize(path, state);
        const params = new URLSearchParams();
        for (const [key, value] of Object.entries(filters)) {
            if (value !== contracts[path][key][1]) params.set(key, value);
        }
        const query = params.toString();
        return path + (query ? `?${query}` : '');
    }

    function validateRecord(raw) {
        const record = JSON.parse(raw);
        if (!record || record.version !== 1 || !Array.isArray(record.views) ||
            record.views.length > MAX_VIEWS) {
            throw new Error('Unsupported saved-view data.');
        }
        const ids = new Set();
        return record.views.map(view => {
            if (!view || typeof view.id !== 'string' || !view.id || view.id.length > 100 ||
                ids.has(view.id) || typeof view.name !== 'string' || !view.name.trim() ||
                view.name.length > MAX_NAME || !Object.hasOwn(contracts, view.path) ||
                !view.filters || typeof view.filters !== 'object' || Array.isArray(view.filters)) {
                throw new Error('Invalid saved view.');
            }
            const { filters, invalid } = normalize(view.path, view.filters);
            if (invalid.length || Object.keys(view.filters).some(key =>
                !Object.hasOwn(contracts[view.path], key))) {
                throw new Error('Invalid saved-view filters.');
            }
            ids.add(view.id);
            return { id: view.id, name: view.name.trim(), path: view.path, filters };
        });
    }

    class Controller {
        constructor(path, reload) {
            this.path = path;
            this.reload = reload;
            this.views = [];
            this.selected = '';
            this.chooser = document.getElementById('saved-view-select');
            this.name = document.getElementById('view-name');
            this.status = document.getElementById('view-status');
            this.error = document.getElementById('view-error');
            this.recovery = document.getElementById('copy-view-recovery');
            this.disclosure = root.RoadmapFilters?.create();
            this.refresh();
            this.bindEvents();
        }

        read() {
            const raw = root.localStorage.getItem(STORAGE_KEY);
            return raw === null ? [] : validateRecord(raw);
        }

        storageError() {
            this.error.textContent = 'Saved views could not be read or written. Allow browser storage, or reset saved views if the data is damaged. Copy view link still works.';
            document.getElementById('reset-saved-views').hidden = false;
            document.getElementById('saved-views-panel')?.showPopover?.();
        }

        refresh() {
            try {
                this.views = this.read();
                this.error.textContent = '';
                document.getElementById('reset-saved-views').hidden = true;
            } catch (error) {
                this.views = [];
                this.storageError();
            }
            this.render();
        }

        render() {
            this.chooser.replaceChildren();
            const empty = document.createElement('option');
            empty.value = '';
            empty.textContent = 'Choose a saved view';
            this.chooser.appendChild(empty);
            for (const view of this.views.filter(view => view.path === this.path)) {
                const option = document.createElement('option');
                option.value = view.id;
                option.textContent = view.name;
                this.chooser.appendChild(option);
            }
            if (!this.views.some(view => view.id === this.selected && view.path === this.path)) {
                this.selected = '';
            }
            this.chooser.value = this.selected;
            document.getElementById('rename-view').disabled = !this.selected;
            document.getElementById('delete-view').disabled = !this.selected;
        }

        capture() {
            const filters = {};
            for (const [key, [id]] of Object.entries(contracts[this.path])) {
                const el = document.getElementById(id);
                filters[key] = el.type === 'checkbox' ? String(el.checked) : el.value;
            }
            return normalize(this.path, filters).filters;
        }

        restore(search) {
            const { filters, invalid } = parse(this.path, search);
            for (const [key, [id, fallback]] of Object.entries(contracts[this.path])) {
                const el = document.getElementById(id);
                let value = filters[key];
                if (el.tagName === 'SELECT' &&
                    !Array.from(el.options).some(option => option.value === value)) {
                    value = fallback;
                    invalid.push(key);
                }
                if (el.type === 'checkbox') el.checked = value === 'true';
                else el.value = value;
            }
            document.getElementById('filter-status').textContent = invalid.length ?
                `Some filters are no longer supported and were reset: ${invalid.join(', ')}.` : '';
            this.disclosure?.update();
        }

        sync() {
            this.disclosure?.update();
            const filters = this.capture();
            root.history.replaceState(null, '', serialize(this.path, filters));
            const selected = this.views.find(view => view.id === this.selected);
            if (selected && serialize(this.path, selected.filters) !== serialize(this.path, filters)) {
                this.selected = '';
                this.render();
            }
        }

        change(operation) {
            this.status.textContent = '';
            try {
                const views = this.read();
                const result = operation(views);
                if (!result) return;
                root.localStorage.setItem(STORAGE_KEY, JSON.stringify({ version: 1, views }));
                this.selected = result.selected;
                this.refresh();
                this.status.textContent = result.message;
            } catch (error) {
                this.storageError();
            }
        }

        validName() {
            const name = this.name.value.trim();
            if (!name || name.length > MAX_NAME) {
                this.status.textContent = `Enter a view name between 1 and ${MAX_NAME} characters.`;
                this.name.focus();
                return null;
            }
            return name;
        }

        async copy() {
            const input = document.getElementById('copy-view-url');
            const link = new URL(serialize(this.path, this.capture()), root.location.origin).href;
            this.status.textContent = '';
            this.recovery.hidden = true;
            try {
                await root.navigator.clipboard.writeText(link);
                this.status.textContent = 'View link copied.';
            } catch (error) {
                this.recovery.hidden = false;
                input.value = link;
                input.focus();
                input.select();
                this.status.textContent = 'Could not copy automatically. Copy the selected link below.';
            }
        }

        bindEvents() {
            document.getElementById('save-view').addEventListener('click', () => {
                const name = this.validName();
                if (!name) return;
                this.change(views => {
                    if (views.length >= MAX_VIEWS) {
                        this.status.textContent = `You can save up to ${MAX_VIEWS} views across both pages. Delete a view first.`;
                        return null;
                    }
                    const id = root.crypto.randomUUID();
                    views.push({ id, name, path: this.path, filters: this.capture() });
                    return { selected: id, message: 'View saved in this browser.' };
                });
            });
            this.chooser.addEventListener('change', () => {
                this.selected = this.chooser.value;
                const view = this.views.find(view => view.id === this.selected);
                this.render();
                if (!view) return;
                this.name.value = view.name;
                this.restore(serialize(this.path, view.filters).split('?')[1] || '');
                const target = serialize(this.path, this.capture());
                if (target !== root.location.pathname + root.location.search) {
                    root.history.pushState(null, '', target);
                }
                this.reload();
                this.status.textContent = 'Saved view opened.';
            });
            document.getElementById('rename-view').addEventListener('click', () => {
                const name = this.validName();
                if (!name) return;
                this.change(views => {
                    const view = views.find(view => view.id === this.selected && view.path === this.path);
                    if (!view) {
                        this.refresh();
                        this.status.textContent = 'That view was removed in another tab. Choose a view again.';
                        return null;
                    }
                    view.name = name;
                    return { selected: view.id, message: 'View renamed.' };
                });
            });
            document.getElementById('delete-view').addEventListener('click', () => {
                this.change(views => {
                    const index = views.findIndex(view => view.id === this.selected && view.path === this.path);
                    if (index === -1) {
                        this.refresh();
                        this.status.textContent = 'That view was already removed.';
                        return null;
                    }
                    views.splice(index, 1);
                    return { selected: '', message: 'View deleted.' };
                });
            });
            document.getElementById('reset-saved-views').addEventListener('click', () => {
                if (!root.confirm('Delete all saved views in this browser, on both pages?')) return;
                try {
                    root.localStorage.removeItem(STORAGE_KEY);
                    this.refresh();
                    this.status.textContent = 'Saved views reset.';
                } catch (error) {
                    this.storageError();
                }
            });
            document.getElementById('copy-view-link').addEventListener('click', () => this.copy());
            root.addEventListener('storage', event => {
                if (event.key === STORAGE_KEY || event.key === null) this.refresh();
            });
            root.addEventListener('popstate', () => {
                this.selected = '';
                this.render();
                this.restore(root.location.search);
                this.reload();
            });
        }
    }

    root.RoadmapViews = { normalize, parse, serialize, validateRecord, Controller };
})(globalThis);
