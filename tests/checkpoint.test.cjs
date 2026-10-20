const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const source = name => fs.readFileSync(path.join(__dirname, '..', 'static', name), 'utf8');
const context = vm.createContext({});
vm.runInContext(source('checkpoint.js'), context);
const Visit = vm.runInContext('ChangelogVisit', context);
const key = 'fabric-gps.changelog-visit.v2';
const oldKey = 'fabric-gps.checkpoint.v1';
const locks = { request: async (_key, work) => work() };
const storage = () => {
    const data = new Map();
    return {
        getItem: key => data.has(key) ? data.get(key) : null,
        setItem: (key, value) => data.set(key, value),
        removeItem: key => data.delete(key),
    };
};
const at = '2026-10-19T00:00:00.1234567Z';
const cutoff = '2026-10-19T00:00:01.1234567Z';
const id = number => number.toString(16).padStart(64, '0');
const event = (number, extra = {}) => ({
    event_id: id(number), observed_at: at, release_item_id: 'feature',
    product_name: 'Power BI', active: true, ...extra,
});
const complete = (items = [], through = cutoff) => ({
    mode: 'visit', complete: true, cutoff: through,
    window_start: Visit.windowStart(Visit.timestamp(through)),
    days: items.length ? [{ date: '2026-10-19', count: items.length, items }] : [],
    total_items: items.length,
});
const response = data => ({ ok: true, json: async () => data });
const saved = (ids = [id(1)], through = at) =>
    JSON.stringify({ version: 2, cutoff: through, event_ids: ids,
        event_times: ids.map(() => '2026-10-18T23:59:59.9999999Z') });

test('first visit saves automatically only after complete display, with no new labels', async () => {
    const local = storage(), visit = new Visit(local, locks);
    assert.equal(local.getItem(key), null);
    const snapshot = visit.snapshot(complete([event(1)]));
    assert.equal(visit.isNew(event(1)), false);
    await visit.acknowledge(snapshot);
    assert.equal(JSON.parse(local.getItem(key)).cutoff, cutoff);
    assert.equal(visit.isNew(event(1)), false);
    assert.equal(new Visit(local, locks).isNew(event(1)), false);
});

test('seven-digit UTC event times and UTC midnight do not collapse to dates or milliseconds', () => {
    assert.equal(Visit.timestamp(at), at);
    assert.equal(Visit.timestamp('2026-10-19T00:00:00Z'), '2026-10-19T00:00:00.0000000Z');
    for (const invalid of ['2026-10-19', '2026-02-30T00:00:00Z', '2026-10-19T00:00:00-01:00', 'bad']) {
        assert.equal(Visit.timestamp(invalid), null);
    }
    const visit = new Visit(storage(), locks);
    const data = complete([event(1, { observed_at: '2026-10-18T23:59:59.9999999Z' }),
        event(2, { observed_at: '2026-10-19T00:00:00.0000001Z' })]);
    data.days = [
        { date: '2026-10-18', count: 1, items: [data.days[0].items[0]] },
        { date: '2026-10-19', count: 1, items: [data.days[0].items[1]] },
    ];
    assert.equal(visit.snapshot(data).ids.size, 2);
});

test('same-day and older-started late commits are new by event identity, not timestamp', async () => {
    const local = storage();
    local.setItem(key, saved());
    const visit = new Visit(local, locks);
    const late = event(2, { observed_at: '2026-10-19T00:00:00.0000001Z' });
    assert.equal(visit.isNew(event(1)), false);
    assert.equal(visit.isNew(late), true);
    await visit.acknowledge(visit.snapshot(complete([event(1), late])));
    assert.equal(visit.isNew(late), true, 'Current document highlights remain stable');
    assert.equal(new Visit(local, locks).isNew(late), false, 'Next visit sees the acknowledged event');
});

test('multi-tab saves are locked and monotonic; open tabs retain their prior event sets', async () => {
    const local = storage();
    local.setItem(key, saved());
    const older = new Visit(local, locks), newer = new Visit(local, locks);
    await newer.acknowledge(newer.snapshot(complete([event(1), event(2)], '2026-10-20T00:00:00Z')));
    await older.acknowledge(older.snapshot(complete([event(1)])));
    assert.equal(JSON.parse(local.getItem(key)).cutoff, '2026-10-20T00:00:00.0000000Z');
    assert.equal(older.isNew(event(2)), true);
    assert.equal(new Visit(local, locks).isNew(event(2)), false);
});

test('equal-cutoff snapshots union visible event identities rather than losing late commits', async () => {
    const local = storage(), a = new Visit(local, locks), b = new Visit(local, locks);
    await a.acknowledge(a.snapshot(complete([event(1), event(2)])));
    await b.acknowledge(b.snapshot(complete([event(1)])));
    assert.equal(JSON.parse(local.getItem(key)).event_ids.length, 2);
});

test('overlapping tabs preserve late commits regardless of which cutoff completed last', async () => {
    for (const olderLast of [false, true]) {
        const local = storage(), a = new Visit(local, locks), b = new Visit(local, locks);
        const older = a.snapshot(complete([event(1), event(2)]));
        const newer = b.snapshot(complete([event(1)], '2026-10-20T00:00:00Z'));
        if (olderLast) {
            await b.acknowledge(newer);
            await a.acknowledge(older);
        } else {
            await a.acknowledge(older);
            await b.acknowledge(newer);
        }
        const state = new Visit(local, locks);
        assert.equal(state.isNew(event(2)), false);
        assert.equal(state.baseline.cutoff, '2026-10-20T00:00:00.0000000Z');
    }
});

test('per-event expiry prevents old manifests growing beyond retained history', async () => {
    const local = storage();
    local.setItem(key, saved([id(1)], '2026-10-18T23:59:59.9999999Z'));
    // Use a strict valid predecessor for the saved event.
    const raw = JSON.parse(local.getItem(key));
    raw.event_times = ['2026-10-18T23:59:59.9999998Z'];
    local.setItem(key, JSON.stringify(raw));
    const visit = new Visit(local, locks);
    await visit.acknowledge(visit.snapshot(complete([], '2027-02-01T00:00:00Z')));
    assert.equal(JSON.parse(local.getItem(key)).event_ids.length, 0);
});

test('a merged manifest over the storage limit fails explicitly without a partial save', async () => {
    const local = storage();
    const raw = saved(Array.from({ length: 10000 }, (_, i) => id(i)));
    local.setItem(key, raw);
    const visit = new Visit(local, locks);
    await visit.acknowledge(visit.snapshot(complete([event(10000)])));
    assert.equal(local.getItem(key), raw);
    assert.match(visit.error, /too large/);
});

test('old day storage safely restarts and is removed only after a successful exact save', async () => {
    const local = storage();
    local.setItem(oldKey, '{"version":1,"day":"2026-10-18"}');
    const visit = new Visit(local, locks);
    assert.equal(visit.baseline, null);
    assert.equal(visit.error, '');
    assert.notEqual(local.getItem(oldKey), null);
    await visit.acknowledge(visit.snapshot(complete()));
    assert.equal(local.getItem(oldKey), null);
});

test('damaged state is explicit; storage/lock failures do not claim success', async () => {
    for (const raw of ['bad', '{}', '{"version":2,"cutoff":"invalid","event_ids":[]}']) {
        const local = storage();
        local.setItem(key, raw);
        const visit = new Visit(local, locks);
        assert.equal(visit.baseline, null);
        assert.match(visit.error, /could not be read/);
    }
    const local = storage(), visit = new Visit(local, locks);
    local.setItem = () => { throw new Error('quota'); };
    await visit.acknowledge(visit.snapshot(complete()));
    assert.match(visit.error, /could not be saved/);
    assert.equal(local.getItem(key), null);
    const unavailable = new Visit(storage(), null);
    await unavailable.acknowledge(unavailable.snapshot(complete()));
    assert.match(unavailable.error, /unavailable/);
});

test('partial, invalid, stale and overflow manifests cannot be acknowledged', () => {
    const local = storage();
    local.setItem(key, saved());
    const visit = new Visit(local, locks);
    const invalid = [
        { ...complete(), complete: false }, { ...complete(), days: null },
        { ...complete(), total_items: 1 }, { ...complete(), cutoff: '2026-10-18T00:00:00Z' },
        complete([event(1), event(1)]),
        complete([event(1, { observed_at: cutoff })]),
        complete([event(1, { observed_at: '2026-10-18T23:59:59Z' })]),
        complete(Array.from({ length: 10001 }, (_, i) => event(i))),
    ];
    for (const data of invalid) assert.throws(() => visit.snapshot(data));
    assert.equal(JSON.parse(local.getItem(key)).cutoff, at);
    assert.equal(visit.snapshot(complete(Array.from({ length: 10000 }, (_, i) => event(i)))).ids.size, 10000);
});

function changelog(fetch, existing = true, clock = Date) {
    const elements = new Map();
    const get = id => {
        if (!elements.has(id)) elements.set(id, {
            value: '', textContent: '', innerHTML: '', disabled: false, checked: true,
            listeners: {}, setAttribute() {},
            addEventListener(name, handler) { this.listeners[name] = handler; },
        });
        return elements.get(id);
    };
    const local = storage();
    if (existing) local.setItem(key, saved());
    const sandbox = vm.createContext({
        document: { getElementById: get, addEventListener() {} }, URLSearchParams, fetch, Date: clock,
    });
    vm.runInContext(source('changelog.js'), sandbox);
    const view = vm.runInContext('Object.create(Changelog.prototype)', sandbox);
    Object.assign(view, {
        visit: new Visit(local, locks), snapshot: null, requestId: 0,
        views: { capture: () => ({
            days: get('days-select').value || '30',
            product_name: get('product-filter').value,
            release_type: get('type-filter').value,
            release_status: get('status-filter').value,
            include_inactive: String(get('include-inactive').checked),
        }) },
        render(days) { this.rendered = days; },
    });
    return { view, get, local };
}

test('one unfiltered snapshot precedes acknowledgement; every filter is display-only', async () => {
    const requests = [];
    const { view, get, local } = changelog(async url => {
        requests.push(url);
        return response(complete([event(1), event(2, { active: false }), event(3, { product_name: 'Other' })]));
    });
    get('product-filter').value = 'Power BI';
    get('include-inactive').checked = false;
    await view.loadChangelog();
    assert.deepEqual(requests, ['/api/changelog?visit=1']);
    assert.equal(view.rendered[0].count, 1);
    assert.equal(JSON.parse(local.getItem(key)).event_ids.length, 3);
    get('include-inactive').checked = true;
    await view.loadChangelog();
    assert.equal(requests.length, 1);
    assert.equal(view.rendered[0].items[1].unread, true);
    get('product-filter').value = '';
    await view.loadChangelog();
    assert.equal(view.rendered[0].count, 3);
    assert.equal(view.rendered[0].items[0].unread, false);
    assert.equal(view.rendered[0].items[2].unread, true);
});

test('failed, partial and malformed loads keep old state and use unacknowledged ordinary fallback', async () => {
    for (const fetchSnapshot of [
        async () => { throw new Error('network'); },
        async () => ({ ok: false, status: 503 }),
        async () => response({ ...complete(), complete: false }),
        async () => response({ ...complete(), total_items: 1 }),
    ]) {
        const { view, get, local } = changelog(async url =>
            url.includes('visit=1') ? fetchSnapshot() : response({ days: [] }));
        await view.loadChangelog();
        assert.equal(view.snapshot, null);
        assert.equal(JSON.parse(local.getItem(key)).cutoff, at);
        assert.match(get('visit-status').textContent, /could not be updated/);
    }
});

test('filter-option or render failures do not acknowledge even a complete snapshot', async () => {
    for (const renderFailure of [false, true]) {
        const { view, local } = changelog(async () => response(complete([event(2)])));
        if (renderFailure) view.render = () => { throw new Error('render'); };
        else view.filterError = 'Filter choices could not be loaded. Reload to retry.';
        await view.loadChangelog();
        assert.equal(JSON.parse(local.getItem(key)).cutoff, at);
    }
});

test('filters changing during an in-flight load share one snapshot and only newest display saves', async () => {
    let finish, calls = 0;
    const { view, get, local } = changelog(() => {
        calls++;
        return new Promise(resolve => { finish = resolve; });
    });
    const first = view.loadChangelog();
    get('product-filter').value = 'Other';
    const second = view.loadChangelog();
    assert.equal(local.getItem(key), saved());
    finish(response(complete([event(2), event(3, { product_name: 'Other' })])));
    await Promise.all([first, second]);
    assert.equal(calls, 1);
    assert.equal(view.rendered[0].items.length, 1);
    assert.equal(view.rendered[0].items[0].event_id, id(3));
    assert.equal(JSON.parse(local.getItem(key)).event_ids.length, 3);
});

test('browser clock skew is irrelevant; first display is not new and next visit uses prior manifest', async () => {
    class SkewedDate extends Date {
        constructor(value) { super(value === undefined ? '2099-01-01T00:00:00Z' : value); }
        static now() { return Date.parse('2099-01-01T00:00:00Z'); }
    }
    context.Date = SkewedDate;
    try {
        const { view, local } = changelog(async () => response(complete([event(1)])), false, SkewedDate);
        await view.loadChangelog();
        assert.equal(view.rendered[0].items[0].unread, false);
        assert.equal(JSON.parse(local.getItem(key)).cutoff, cutoff);
        assert.equal(new Visit(local, locks).isNew(event(2)), true);
    } finally {
        context.Date = Date;
    }
});

test('bad filter options remain explicit after data loads', async () => {
    const { view, get, local } = changelog(async url =>
        url.includes('filter-options') ? { ok: false, status: 500 } : response(complete()));
    await view.loadFilterOptions();
    await view.loadChangelog();
    assert.match(get('visit-status').textContent, /Filter choices/);
    assert.equal(local.getItem(key), saved());
});

test('unread badge is escaped and confined to new temporal events', () => {
    const sandbox = vm.createContext({
        document: { createElement: () => ({
            set textContent(text) { this.innerHTML = String(text).replaceAll('<', '&lt;').replaceAll('>', '&gt;'); },
        }) },
        window: { location: { origin: 'https://example.test' } },
    });
    vm.runInContext(source('changelog-render.js'), sandbox);
    const days = [{ date: '2026-10-19', count: 2, items: [
        { release_item_id: 'one', feature_name: '<unsafe>', unread: true },
        { release_item_id: 'two', feature_name: 'Seen', unread: false },
    ] }];
    sandbox.days = days;
    const html = vm.runInContext('renderChangelogDays(days, {events:true})', sandbox);
    assert.equal((html.match(/changelog-new-badge/g) || []).length, 1);
    assert.match(html, /changelog-unread/);
    assert.match(html, /&lt;unsafe&gt;/);
    assert.doesNotMatch(html, /<unsafe>/);
});

test('daily permalink requests exact UTC events, retains repeated edits/removal and never accesses checkpoints', async () => {
    const requests = [];
    const container = { innerHTML: '' };
    const data = {
        mode: 'events', event_date: '2026-10-19',
        days: [{ date: '2026-10-19', count: 3, items: [
            { release_item_id: 'one', feature_name: 'Feature', changed_columns: ['Description updated'] },
            { release_item_id: 'one', feature_name: 'Feature', changed_columns: ['Name old -> new'] },
            { release_item_id: 'removed', feature_name: 'Removed feature', active: false,
                changed_columns: ['Removed from Roadmap'] },
        ] }],
    };
    const sandbox = vm.createContext({
        URLSearchParams,
        localStorage: { getItem: () => assert.fail('daily read checkpoint'),
            setItem: () => assert.fail('daily acknowledged checkpoint') },
        document: {
            addEventListener() {},
            createElement: () => ({
                set textContent(value) { this.innerHTML = String(value).replaceAll('<', '&lt;').replaceAll('>', '&gt;'); },
            }),
        },
        fetch: async (url, options) => {
            requests.push({ url, options });
            return { ok: true, json: async () => data };
        },
    });
    vm.runInContext(source('changelog-render.js'), sandbox);
    vm.runInContext(source('changelog-day.js'), sandbox);
    const proto = vm.runInContext('ChangelogDay.prototype', sandbox);
    const view = Object.assign(Object.create(proto), { container, date: '2026-10-19' });
    await view.load('fixture-version');
    const query = new URL(requests[0].url, 'https://example.test').searchParams;
    assert.equal(query.get('event_date'), '2026-10-19');
    assert.equal(query.get('include_inactive'), 'true');
    assert.equal(query.get('v'), 'fixture-version');
    assert.equal(query.has('date'), false);
    assert.equal(query.has('visit'), false);
    assert.equal(requests[0].options.cache, 'no-store');
    assert.equal(requests[0].options.credentials, 'omit');
    assert.match(container.innerHTML, /3 change events/);
    assert.equal((container.innerHTML.match(/href="\/release\/one"/g) || []).length, 2);
    assert.match(container.innerHTML, /Removed from Roadmap/);
    assert.doesNotMatch(container.innerHTML, /changelog-new-badge|changelog-copy-link/);

    for (const bad of [
        { ok: false, json: async () => ({ error: 'failed' }) },
        { ok: true, json: async () => { throw new SyntaxError('invalid JSON'); } },
        { ok: true, json: async () => ({ days: [] }) },  // Old API ignoring event_date.
        { ok: true, json: async () => ({ mode: 'events', event_date: '2026-10-18', days: [] }) },
        { ok: true, json: async () => ({ ...data, days: {} }) },
        { ok: true, json: async () => ({ ...data, days: [{ date: '2026-10-18', items: [] }] }) },
    ]) {
        sandbox.fetch = async () => bad;
        await view.load('');
        assert.match(container.innerHTML, /Error loading changes/);
        assert.doesNotMatch(container.innerHTML, /No roadmap changes/);
    }
});
