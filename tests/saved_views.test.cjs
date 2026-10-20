const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const key = 'fabric-gps.saved-views';
const source = name => fs.readFileSync(path.join(__dirname, '..', 'static', name), 'utf8');

class Element {
    constructor(tagName = 'INPUT', values = [], value = '') {
        this.tagName = tagName;
        this.type = 'text';
        this.options = values.map(value => ({ value, textContent: value }));
        this.children = [];
        this.value = value;
        this.textContent = '';
        this.listeners = {};
        this.checked = true;
        this.hidden = false;
        this.style = {};
        this.classList = { toggle() {} };
    }
    addEventListener(event, fn) { this.listeners[event] = fn; }
    setAttribute() {}
    emit(event) { return this.listeners[event]?.({ preventDefault() {} }); }
    appendChild(child) { this.children.push(child); this.options.push(child); }
    replaceChildren() { this.children = []; this.options = []; }
    focus() { this.focused = true; }
    select() { this.selected = true; }
}

function setup(page = '/') {
    const elements = {};
    const ids = [
        'saved-view-select', 'view-name', 'view-status', 'view-error', 'filter-status',
        'copy-view-recovery', 'copy-view-url', 'save-view', 'rename-view', 'delete-view',
        'copy-view-link', 'reset-saved-views', 'search-input', 'include-inactive',
        'clear-filters', 'load-more-btn', 'changes-grid', 'no-results', 'loading-indicator',
        'loading-more-indicator', 'search-spinner', 'changelog-content',
        'catch-up-toggle', 'checkpoint-reset', 'checkpoint-mark',
        'checkpoint-baseline', 'checkpoint-status', 'catch-up-summary',
    ];
    ids.forEach(id => { elements[id] = new Element(); });
    elements['include-inactive'].type = 'checkbox';
    elements['saved-view-select'].tagName = 'SELECT';
    elements['product-filter'] = new Element('SELECT', ['', 'Data & AI', '数据'], '');
    elements['type-filter'] = new Element('SELECT', ['', 'Preview'], '');
    elements['status-filter'] = new Element('SELECT', ['', 'Shipped'], '');
    elements['days-filter'] = new Element('SELECT', ['', '7', '14', '30'], '');
    elements['days-select'] = new Element('SELECT', ['7', '14', '30', '60', '90'], '30');
    elements['sort-select'] = new Element('SELECT', ['last_modified', 'release_date'], 'last_modified');
    const values = new Map();
    const events = {};
    const location = { origin: 'https://fabric.example', search: '', pathname: page, hash: '' };
    const entries = [page];
    let position = 0;
    const navigate = target => {
        const url = new URL(target, location.origin);
        location.search = url.search;
        location.pathname = url.pathname;
    };
    let sequence = 0;
    const sandbox = {
        URL, URLSearchParams, AbortController, console, setTimeout, clearTimeout,
        location,
        crypto: { randomUUID: () => `view-${++sequence}` },
        localStorage: {
            getItem: key => values.get(key) ?? null,
            setItem: (key, value) => values.set(key, value),
            removeItem: key => values.delete(key),
        },
        history: {
            replaceState(_state, _unused, target) {
                entries[position] = target;
                navigate(target);
            },
            pushState(_state, _unused, target) {
                entries.splice(++position, entries.length, target);
                navigate(target);
            },
            back() {
                if (position > 0) {
                    navigate(entries[--position]);
                    events.popstate();
                }
            },
            forward() {
                if (position + 1 < entries.length) {
                    navigate(entries[++position]);
                    events.popstate();
                }
            },
        },
        navigator: { clipboard: { writeText: async text => { sandbox.copied = text; } } },
        addEventListener: (event, fn) => { events[event] = fn; },
        confirm: () => true,
        document: {
            getElementById: id => elements[id] || null,
            createElement: tag => new Element(tag.toUpperCase()),
            addEventListener() {},
            querySelectorAll: () => [],
        },
    };
    sandbox.window = sandbox;
    const context = vm.createContext(sandbox);
    vm.runInContext(source('saved-views.js'), context);
    const api = sandbox.RoadmapViews;
    let reloads = 0;
    const controller = () => new api.Controller(page, () => { reloads++; });
    return { api, sandbox, context, elements, values, events, controller, reloads: () => reloads };
}

function record(views) { return JSON.stringify({ version: 1, views }); }

test('storage errors reveal the compact Views panel instead of hiding recovery', () => {
    const env = setup();
    let opened = 0;
    env.elements['saved-views-panel'] = { showPopover() { opened++; } };
    env.sandbox.localStorage.getItem = () => { throw new Error('denied'); };
    env.controller();
    assert.equal(opened, 1);
    assert.match(env.elements['view-error'].textContent, /could not be read or written/);
    assert.equal(env.elements['reset-saved-views'].hidden, false);
});

function view(overrides = {}) {
    return { id: 'one', name: 'A view', path: '/', filters: { q: 'lake' }, ...overrides };
}

test('allowlisted legacy URL round trips Unicode, special characters, sort, windows and removed state', () => {
    const { api } = setup();
    for (const page of ['/', '/changelog']) {
        const query = '?product_name=%E6%95%B0%E6%8D%AE&release_type=Preview&release_status=Shipped' +
            '&q=KQL+%26+%23+%2B+%3F+%E2%98%83&sort=release_date&modified_within_days=14' +
            '&days=90&include_inactive=false&token=secret&v=123&page=7&page_size=15';
        const state = api.parse(page, query).filters;
        const link = api.serialize(page, state);
        assert.deepEqual(api.parse(page, link.split('?')[1]).filters, state);
        for (const forbidden of ['token', 'secret', 'v=', 'page=', 'page_size', '#']) {
            assert.ok(!link.includes(forbidden), link);
        }
        assert.equal(state.product_name, '数据');
        assert.equal(state.include_inactive, 'false');
        if (page === '/') {
            assert.equal(state.q, 'KQL & # + ? ☃');
            assert.equal(state.sort, 'release_date');
            assert.equal(state.modified_within_days, '14');
        } else {
            assert.ok(!link.includes('q='));
            assert.equal(state.days, '90');
        }
    }
    assert.equal(api.serialize('/', {}), '/');
    assert.equal(api.serialize('/changelog', {}), '/changelog');
    assert.equal(api.parse('/', '').filters.include_inactive, 'true');
});

test('restoring invalid or stale values reports defaults and resets absent fields', () => {
    const { api, elements, controller } = setup();
    const ctl = controller();
    ctl.restore('?product_name=retired&sort=invalid&modified_within_days=-4&include_inactive=bad&q=雪');
    assert.match(elements['filter-status'].textContent, /product_name/);
    assert.match(elements['filter-status'].textContent, /sort/);
    assert.equal(elements['product-filter'].value, '');
    assert.equal(elements['sort-select'].value, 'last_modified');
    assert.equal(elements['days-filter'].value, '');
    assert.equal(elements['include-inactive'].checked, true);
    assert.equal(elements['search-input'].value, '雪');
    ctl.restore('');
    assert.equal(elements['search-input'].value, '');
    assert.equal(elements['filter-status'].textContent, '');
    assert.equal(api.parse('/changelog', '?days=999').filters.days, '30');
    assert.throws(() => api.parse('/preferences', '?token=private'));
});

test('records reject malformed, future, excessive, duplicate, private and invalid state', () => {
    const { api } = setup();
    for (const raw of [
        '{', 'null', '[]', '{"version":2,"views":[]}', record([null]),
        record([view({ name: '' })]), record([view({ name: 'x'.repeat(81) })]),
        record([view({ path: '//evil.example' })]), record([view({ path: '/preferences' })]),
        record([view({ filters: null })]), record([view({ filters: [] })]),
        record([view({ filters: { sort: 'unknown' } })]),
        record([view({ filters: { token: 'secret' } })]),
        record([view({ filters: { q: 7 } })]),
        record([view(), view()]),
        record(Array.from({ length: 21 }, (_, i) => view({ id: String(i) }))),
    ]) assert.throws(() => api.validateRecord(raw), raw);
    assert.equal(api.validateRecord(record([view()]))[0].filters.q, 'lake');
});

test('save survives reload; rename/delete preserve other pages and render names as text', () => {
    const env = setup();
    const { elements, values, controller } = env;
    values.set(key, record([view({ id: 'changelog', path: '/changelog', filters: { days: '60' } })]));
    let ctl = controller();
    elements['view-name'].value = '<img src=x onerror=alert(1)> 雪';
    ctl.restore('?q=lake&sort=release_date&include_inactive=false');
    elements['save-view'].emit('click');
    const savedId = ctl.selected;
    assert.equal(ctl.views.length, 2);
    assert.equal(elements['saved-view-select'].children[1].textContent, elements['view-name'].value);
    assert.equal(elements['saved-view-select'].children[1].innerHTML, undefined);
    ctl = controller();
    elements['saved-view-select'].value = savedId;
    ctl.restore('?q=different&modified_within_days=30');
    elements['saved-view-select'].emit('change');
    assert.equal(elements['search-input'].value, 'lake');
    assert.equal(elements['days-filter'].value, '');
    assert.equal(elements['include-inactive'].checked, false);
    assert.equal(env.reloads(), 1);
    elements['view-name'].value = 'Renamed';
    elements['rename-view'].emit('click');
    assert.equal(JSON.parse(values.get(key)).views[1].name, 'Renamed');
    assert.equal(JSON.parse(values.get(key)).views[1].filters.q, 'lake');
    elements['delete-view'].emit('click');
    assert.equal(JSON.parse(values.get(key)).views.length, 1);
    assert.equal(JSON.parse(values.get(key)).views[0].id, 'changelog');
    assert.equal(elements['rename-view'].disabled, true);
});

test('empty/long names and collection limit fail visibly without changing storage', () => {
    const { values, controller, elements } = setup();
    const ctl = controller();
    for (const name of ['  ', 'x'.repeat(81)]) {
        elements['view-name'].value = name;
        elements['save-view'].emit('click');
        assert.match(elements['view-status'].textContent, /between 1 and 80/);
        assert.equal(values.size, 0);
    }
    values.set(key, record(Array.from({ length: 20 }, (_, i) => view({ id: String(i) }))));
    elements['view-name'].value = 'Another';
    elements['save-view'].emit('click');
    assert.match(elements['view-status'].textContent, /up to 20/);
    assert.equal(ctl.read().length, 20);
});

test('storage corruption, denied reads/writes and reset failures are explicit and never overwrite', async () => {
    const { values, sandbox, controller, elements } = setup();
    values.set(key, 'corrupt');
    const ctl = controller();
    assert.match(elements['view-error'].textContent, /could not be read or written/);
    elements['view-name'].value = 'New';
    elements['save-view'].emit('click');
    assert.equal(values.get(key), 'corrupt');
    await ctl.copy();
    assert.equal(sandbox.copied, 'https://fabric.example/');
    values.set('unrelated', 'keep');
    sandbox.confirm = () => false;
    elements['reset-saved-views'].emit('click');
    assert.equal(values.get(key), 'corrupt');
    sandbox.confirm = () => true;
    elements['reset-saved-views'].emit('click');
    assert.equal(values.get(key), undefined);
    assert.equal(values.get('unrelated'), 'keep');
    sandbox.localStorage.setItem = () => { throw Error('quota'); };
    elements['save-view'].emit('click');
    assert.match(elements['view-error'].textContent, /could not be read or written/);
    assert.ok(!elements['view-status'].textContent.includes('saved in'));
    sandbox.localStorage.getItem = () => { throw Error('denied'); };
    ctl.refresh();
    assert.equal(elements['reset-saved-views'].hidden, false);
    sandbox.localStorage.removeItem = () => { throw Error('denied'); };
    elements['reset-saved-views'].emit('click');
    assert.match(elements['view-error'].textContent, /could not be read or written/);
});

test('clipboard success waits for resolution; rejection and unavailable API expose a manual link', async () => {
    const { sandbox, controller, elements } = setup();
    const ctl = controller();
    ctl.restore('?product_name=Data+%26+AI&q=%E9%9B%AA%23%26&sort=release_date');
    sandbox.location.search += '&token=private&page=10&v=7';
    let finish;
    sandbox.navigator.clipboard.writeText = () => new Promise(resolve => { finish = resolve; });
    const pending = ctl.copy();
    assert.equal(elements['view-status'].textContent, '');
    finish();
    await pending;
    assert.equal(elements['view-status'].textContent, 'View link copied.');
    for (const clipboard of [
        { writeText: async () => { throw Error('denied'); } }, undefined,
    ]) {
        sandbox.navigator.clipboard = clipboard;
        await ctl.copy();
        assert.equal(elements['copy-view-recovery'].hidden, false);
        assert.equal(elements['copy-view-url'].selected, true);
        const url = new URL(elements['copy-view-url'].value);
        assert.equal(url.searchParams.get('q'), '雪#&');
        assert.equal(url.searchParams.get('product_name'), 'Data & AI');
        assert.equal(url.searchParams.get('token'), null);
        assert.equal(url.searchParams.get('page'), null);
        assert.match(elements['view-status'].textContent, /Could not copy automatically/);
    }
});

test('back/forward restores defaults and reloads without overwriting history', () => {
    for (const page of ['/', '/changelog']) {
        const { sandbox, events, elements, controller, reloads } = setup(page);
        controller();
        sandbox.location.search = '?product_name=Data+%26+AI&days=90&q=lake&include_inactive=false';
        events.popstate();
        assert.equal(elements['product-filter'].value, 'Data & AI');
        assert.equal(elements['include-inactive'].checked, false);
        if (page === '/changelog') assert.equal(elements['days-select'].value, '90');
        sandbox.location.search = '';
        events.popstate();
        assert.equal(elements['product-filter'].value, '');
        assert.equal(elements['include-inactive'].checked, true);
        assert.equal(elements['search-input'].value, '');
        assert.equal(elements['days-select'].value, '30');
        assert.equal(sandbox.location.search, '');
        assert.equal(reloads(), 2);
    }
});

test('storage events refresh chooser and edits merge other-tab records', () => {
    const { values, controller, elements, events } = setup();
    const ctl = controller();
    values.set(key, record([view()]));
    elements['view-name'].value = 'Second';
    elements['save-view'].emit('click');
    assert.equal(ctl.read().length, 2);
    values.set(key, record([view({ id: 'remote', name: 'Other tab' })]));
    events.storage({ key });
    assert.equal(elements['saved-view-select'].children[1].textContent, 'Other tab');
    assert.equal(elements['rename-view'].disabled, true);
    values.delete(key);
    events.storage({ key: null });
    assert.equal(elements['saved-view-select'].children.length, 1);
});

test('opening saved views creates traversable history entries and edited views can reopen', () => {
    for (const page of ['/', '/changelog']) {
        const { values, sandbox, elements, controller, reloads } = setup(page);
        values.set(key, record([
            view({ id: 'first', path: page, filters: { product_name: 'Data & AI' } }),
            view({ id: 'second', path: page, filters: { product_name: '数据' } }),
        ]));
        const ctl = controller();
        const open = id => {
            elements['saved-view-select'].value = id;
            elements['saved-view-select'].emit('change');
        };
        open('first');
        open('second');
        assert.equal(elements['product-filter'].value, '数据');
        sandbox.history.back();
        assert.equal(elements['product-filter'].value, 'Data & AI');
        sandbox.history.back();
        assert.equal(elements['product-filter'].value, '');
        sandbox.history.forward();
        assert.equal(elements['product-filter'].value, 'Data & AI');
        sandbox.history.forward();
        assert.equal(elements['product-filter'].value, '数据');
        assert.equal(reloads(), 6);
        open('second');
        elements['product-filter'].value = '';
        ctl.sync();
        assert.equal(ctl.selected, '');
        assert.equal(elements['saved-view-select'].value, '');
        assert.equal(elements['rename-view'].disabled, true);
        open('second');
        assert.equal(elements['product-filter'].value, '数据');
    }
});

function pageInstance(env, page) {
    const filename = page === '/' ? 'app.js' : 'changelog.js';
    const className = page === '/' ? 'RecentChanges' : 'Changelog';
    vm.runInContext(source(filename), env.context);
    const proto = vm.runInContext(`${className}.prototype`, env.context);
    const instance = Object.create(proto);
    Object.assign(instance, {
        filterOptions: {}, currentData: [], minSearchLength: 2, page: 8, pageSize: 15,
        allPagesLoaded: false, loadingMore: false, hasLoadedOnce: true, dataVersion: '',
        checkpoint: { available: false, baseline: null, error: '' },
        catchUp: false, requestId: 0,
    });
    instance.renderChanges = () => {};
    instance.render = days => { instance.rendered = days; };
    return instance;
}

test('saved changelog views and browser history restore checkpoint mode without sharing it', async () => {
    const env = setup('/changelog');
    const instance = pageInstance(env, '/changelog');
    instance.checkpoint = { baseline: '2026-10-17', available: true, error: '' };
    env.sandbox.location.search = '?catch_up=1&include_inactive=false';
    env.sandbox.history.replaceState(null, '', '/changelog?catch_up=1&include_inactive=false');
    instance.fetchVersion = async () => {};
    instance.loadFilterOptions = async () => {};
    const modes = [];
    instance.loadChangelog = async () => { modes.push(instance.catchUp); };
    await instance.init();
    assert.equal(instance.catchUp, true);
    env.elements['view-name'].value = 'My view';
    env.elements['save-view'].emit('click');
    const id = instance.views.selected;
    const stored = env.values.get(key);
    assert.ok(!stored.includes('catch_up'));
    assert.ok(!stored.includes('2026-10-17'));
    env.elements['saved-view-select'].value = id;
    env.elements['saved-view-select'].emit('change');
    assert.equal(instance.catchUp, false);
    assert.ok(!env.sandbox.location.search.includes('catch_up'));
    env.sandbox.history.back();
    assert.equal(instance.catchUp, true);
    assert.deepEqual(modes, [true, false, true]);
});

test('both page initializers wait for filter options before restoring bookmarks', async () => {
    for (const page of ['/', '/changelog']) {
        const env = setup(page);
        const instance = pageInstance(env, page);
        env.elements['product-filter'].options = [{ value: '' }];
        env.sandbox.location.search = '?product_name=Future+product&q=lake&days=60';
        const requests = [];
        env.sandbox.fetch = async url => {
            requests.push(url);
            if (url === '/api/version') return { json: async () => ({ version: '123' }) };
            if (url === '/api/filter-options') {
                return { ok: true, json: async () => ({
                    product_names: ['Future product'], release_types: ['Preview'],
                    release_statuses: ['Shipped'],
                }) };
            }
            return { ok: true, json: async () => ({ data: [], days: [] }) };
        };
        await instance.init();
        assert.equal(env.elements['product-filter'].value, 'Future product');
        assert.equal(env.elements['filter-status'].textContent, '');
        assert.equal(requests[0], '/api/version');
        assert.equal(requests[1], '/api/filter-options');
        const query = new URL(requests[2], env.sandbox.location.origin).searchParams;
        assert.equal(query.get('product_name'), 'Future product');
        assert.equal(query.get('include_inactive'), 'true');
        if (page === '/') {
            assert.equal(query.get('page'), '1');
            assert.equal(query.get('q'), 'lake');
        } else assert.equal(query.get('days'), '60');
    }
});

test('opening saved roadmap view resets pagination and cancels pending search', async () => {
    const env = setup();
    const instance = pageInstance(env, '/');
    instance.views = new env.api.Controller('/', () => instance.reloadView());
    env.elements['view-name'].value = 'Lake';
    instance.views.restore('?q=lake&sort=release_date&include_inactive=false');
    env.elements['save-view'].emit('click');
    const id = instance.views.selected;
    instance.page = 8;
    instance.allPagesLoaded = true;
    instance.currentData = ['old'];
    instance.debounceTimer = setTimeout(() => assert.fail('stale search fired'), 50);
    instance._filterChangeTimer = setTimeout(() => assert.fail('stale filter fired'), 50);
    let requested;
    env.sandbox.fetch = async url => {
        requested = url;
        return { json: async () => ({ data: [], pagination: { has_next: false } }) };
    };
    instance.views.restore('?q=different');
    env.elements['saved-view-select'].value = id;
    env.elements['saved-view-select'].emit('change');
    await new Promise(resolve => setTimeout(resolve, 60));
    const params = new URL(requested, env.sandbox.location.origin).searchParams;
    assert.equal(params.get('page'), '1');
    assert.equal(params.get('q'), 'lake');
    assert.equal(params.get('sort'), 'release_date');
    assert.equal(params.get('include_inactive'), 'false');
    assert.equal(instance.currentData.length, 0);
});

test('latest filter restoration wins even if older responses finish afterward', async () => {
    for (const page of ['/', '/changelog']) {
        const env = setup(page);
        const instance = pageInstance(env, page);
        instance.views = new env.api.Controller(page, () => {});
        const requests = [];
        env.sandbox.fetch = (_url, options) => new Promise(resolve => {
            requests.push({ resolve, signal: options.signal });
        });
        const load = () => page === '/' ?
            instance.loadRecentChanges({ reset: true }) : instance.loadChangelog();
        const old = load();
        instance.views.restore('?product_name=Data+%26+AI');
        const latest = load();
        assert.equal(requests[0].signal.aborted, true);
        requests[1].resolve({ ok: true, json: async () => ({
            data: ['latest'], days: ['latest'], pagination: { has_next: false },
        }) });
        await latest;
        requests[0].resolve({ ok: true, json: async () => ({
            data: ['stale'], days: ['stale'], pagination: { has_next: false },
        }) });
        await old;
        assert.equal((page === '/' ? instance.currentData : instance.rendered)[0], 'latest');
    }
});
