const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');

const source = name => fs.readFileSync(path.join(__dirname, '..', 'static', name), 'utf8');
const context = vm.createContext({});
vm.runInContext(source('checkpoint.js'), context);
const Checkpoint = vm.runInContext('CatchUpCheckpoint', context);
const key = 'fabric-gps.checkpoint.v1';
const storage = () => {
    const data = new Map();
    return {
        getItem: key => data.has(key) ? data.get(key) : null,
        setItem: (key, value) => data.set(key, value),
        removeItem: key => data.delete(key),
    };
};
const saved = day => JSON.stringify({ version: 1, day });
const october19 = () => '2026-10-19';
const october20 = () => '2026-10-20';

test('first visit is not persisted until explicit acknowledgement', () => {
    const local = storage(), session = storage();
    const cp = new Checkpoint(local, session, october19);
    assert.equal(cp.baseline, null);
    assert.equal(local.getItem(key), null);
    cp.acknowledge('2026-10-19');
    assert.equal(JSON.parse(local.getItem(key)).day, '2026-10-19');
    assert.equal(new Checkpoint(local, session, october19).baseline, '2026-10-19');
});

test('reloads and parallel tabs retain their snapshot, fresh tabs see saved date', () => {
    const local = storage(), first = storage(), second = storage();
    local.setItem(key, saved('2026-10-17'));
    const a = new Checkpoint(local, first, october19);
    new Checkpoint(local, second, october19);
    a.acknowledge('2026-10-19');
    assert.equal(a.baseline, '2026-10-17');
    assert.equal(new Checkpoint(local, first, october19).baseline, '2026-10-17');
    assert.equal(new Checkpoint(local, second, october19).baseline, '2026-10-17');
    assert.equal(new Checkpoint(local, storage(), october19).baseline, '2026-10-19');
    a.acknowledge('2026-10-18');
    assert.equal(JSON.parse(local.getItem(key)).day, '2026-10-19');
});

test('same-day acknowledgements remain inclusive across repeated refreshes', () => {
    const local = storage(), session = storage();
    local.setItem(key, saved('2026-10-19'));
    const cp = new Checkpoint(local, session, october19);
    cp.acknowledge('2026-10-19');
    cp.acknowledge('2026-10-19');
    assert.equal(cp.baseline, '2026-10-19');
    assert.equal(new Checkpoint(local, storage(), october20).baseline, '2026-10-19');
});

test('an older tab cannot overwrite a checkpoint saved after UTC midnight', () => {
    const local = storage();
    local.setItem(key, saved('2026-10-17'));
    const older = new Checkpoint(local, storage(), october19);
    const newer = new Checkpoint(local, storage(), october20);
    newer.acknowledge('2026-10-20');
    assert.match(older.acknowledge('2026-10-19'), /newer checkpoint/);
    assert.equal(JSON.parse(local.getItem(key)).day, '2026-10-20');
    assert.equal(older.baseline, '2026-10-17');
});

test('invalid version, corrupt, future and expired values are explicit until reset', () => {
    for (const raw of ['bad', 'null', '{}', saved('2026-10-20'), saved('2026-07-20'), saved('2026-02-30')]) {
        const local = storage(), session = storage();
        local.setItem(key, raw);
        let cp = new Checkpoint(local, session, october19);
        assert.ok(cp.error);
        assert.equal(cp.baseline, null);
        cp = new Checkpoint(local, session, october19);
        assert.ok(cp.error);
        cp.reset();
        assert.equal(local.getItem(key), null);
        assert.equal(new Checkpoint(local, session, october19).error, '');
    }
});

test('disabled storage and write failures cannot claim a saved checkpoint', () => {
    const broken = { getItem() { throw new Error('disabled'); } };
    const cp = new Checkpoint(broken, storage(), october19);
    assert.equal(cp.available, false);
    assert.match(cp.error, /storage/);
    const local = storage();
    const writable = new Checkpoint(local, storage(), october19);
    local.setItem = () => { throw new Error('quota'); };
    assert.throws(() => writable.acknowledge('2026-10-19'), /quota/);
    assert.equal(writable.baseline, null);
});

test('a session-write failure after local save explicitly reports partial success', () => {
    const local = storage(), session = storage();
    const cp = new Checkpoint(local, session, october19);
    session.setItem = () => { throw new Error('quota'); };
    assert.match(cp.acknowledge('2026-10-19'), /saved, but/);
    assert.equal(JSON.parse(local.getItem(key)).day, '2026-10-19');
    assert.equal(cp.baseline, null);
});

function changelog(fetch, clock = october19) {
    const elements = new Map();
    const get = id => {
        if (!elements.has(id)) elements.set(id, {
            value: '', textContent: '', innerHTML: '', disabled: false,
            listeners: {},
            setAttribute() {}, addEventListener(name, handler) { this.listeners[name] = handler; },
        });
        return elements.get(id);
    };
    const local = storage();
    local.setItem(key, saved('2026-10-17'));
    const cp = new Checkpoint(local, storage(), clock);
    const sandbox = vm.createContext({
        document: { getElementById: get, addEventListener() {} },
        URLSearchParams, AbortController, fetch,
    });
    vm.runInContext(source('changelog.js'), sandbox);
    const view = vm.runInContext('Object.create(Changelog.prototype)', sandbox);
    Object.assign(view, { checkpoint: cp, catchUp: true, requestId: 0, dataVersion: '',
        views: { capture: () => ({
            days: get('days-select').value || '30',
            product_name: get('product-filter').value,
            release_type: get('type-filter').value,
            release_status: get('status-filter').value,
            include_inactive: String(get('include-inactive').checked !== false),
        }) },
        render(days) { this.rendered = days; } });
    return { view, get, local };
}
const complete = () => ({
    days: [], complete: true, since: '2026-10-17', through: '2026-10-19',
});
const response = data => ({ ok: true, json: async () => data });

test('saved inactive filter cannot omit removals before checkpoint acknowledgement', async () => {
    let requested;
    const { view, get } = changelog(async url => {
        requested = new URLSearchParams(url.split('?')[1]);
        return response({
            ...complete(),
            days: [{ date: '2026-10-19', count: 2, items: [
                { release_item_id: 'removed', active: false },
                { release_item_id: 'restored', active: true },
            ] }],
        });
    });
    get('include-inactive').checked = false;
    await view.loadChangelog();
    assert.equal(requested.get('include_inactive'), 'true');
    assert.equal(view.loadedCheckpoint, '2026-10-19');
    assert.equal(view.rendered[0].items.length, 1);
    assert.equal(view.rendered[0].items[0].release_item_id, 'restored');
});

test('failed, partial, and invalid catch-up responses disable acknowledgement', async () => {
    for (const fetch of [
        async () => { throw new Error('network'); },
        async () => ({ ok: false, status: 500 }),
        async () => response({ ...complete(), complete: false }),
        async () => response({ ...complete(), since: '2026-10-18' }),
        async () => response({ ...complete(), through: '2026-10-20' }),
        async () => response({ ...complete(), days: null }),
    ]) {
        const { view, get, local } = changelog(fetch);
        await view.loadChangelog();
        assert.equal(view.loadedCheckpoint, null);
        assert.equal(get('checkpoint-mark').disabled, true);
        assert.match(get('checkpoint-status').textContent, /not changed/);
        assert.equal(JSON.parse(local.getItem(key)).day, '2026-10-17');
    }
});

test('empty successful load enables only explicit acknowledgement', async () => {
    const { view, get, local } = changelog(async () => response(complete()));
    await view.loadChangelog();
    assert.equal(view.loadedCheckpoint, '2026-10-19');
    assert.equal(get('checkpoint-mark').disabled, false);
    assert.equal(JSON.parse(local.getItem(key)).day, '2026-10-17');
    view.bindEvents();
    await get('checkpoint-mark').listeners.click();
    assert.equal(JSON.parse(local.getItem(key)).day, '2026-10-19');
    assert.equal(view.checkpoint.baseline, '2026-10-17');
    assert.match(get('checkpoint-status').textContent, /Saved/);
});

test('a tab crossing UTC midnight accepts a current response without moving its baseline', async () => {
    let day = '2026-10-19';
    const { view, get, local } = changelog(
        async () => response({ ...complete(), through: day }), () => day);
    await view.loadChangelog();
    day = '2026-10-20';
    await view.loadChangelog();
    assert.equal(view.loadedCheckpoint, '2026-10-20');
    assert.equal(get('checkpoint-mark').disabled, false);
    assert.equal(view.checkpoint.baseline, '2026-10-17');
    assert.equal(JSON.parse(local.getItem(key)).day, '2026-10-17');
    view.bindEvents();
    await get('checkpoint-mark').listeners.click();
    assert.equal(JSON.parse(local.getItem(key)).day, '2026-10-20');
    assert.equal(view.checkpoint.baseline, '2026-10-17');
});

test('first visit spanning UTC midnight accepts the next UTC day and saves only on action', async () => {
    let day = '2026-10-19';
    const { view, get, local } = changelog(async () => {
        day = '2026-10-20';
        return response({ ...complete(), since: '2026-10-19', through: day });
    }, () => day);
    view.catchUp = false;
    view.checkpoint.baseline = null;
    local.removeItem(key);
    await view.loadChangelog();
    assert.equal(view.loadedCheckpoint, '2026-10-20');
    assert.equal(get('checkpoint-mark').disabled, false);
    assert.equal(local.getItem(key), null);
    view.bindEvents();
    await get('checkpoint-mark').listeners.click();
    assert.equal(JSON.parse(local.getItem(key)).day, '2026-10-20');
    assert.equal(view.checkpoint.baseline, '2026-10-20');
});

test('first visit requires both successful initial and normal changelog retrieval', async () => {
    let calls = 0;
    const { view, get, local } = changelog(async () => {
        if (++calls === 1) return response({ ...complete(), since: '2026-10-19' });
        return { ok: false, status: 500 };
    });
    view.catchUp = false;
    view.checkpoint.baseline = null;
    local.removeItem(key);
    await view.loadChangelog();
    assert.equal(calls, 2);
    assert.equal(view.loadedCheckpoint, null);
    assert.equal(get('checkpoint-mark').disabled, true);
    assert.equal(local.getItem(key), null);
});

test('switching to normal browsing cannot acknowledge catch-up results', async () => {
    const { view, get, local } = changelog(async () => response(complete()));
    view.catchUp = false;
    await view.loadChangelog();
    assert.equal(view.loadedCheckpoint, null);
    assert.equal(get('checkpoint-mark').disabled, true);
    view.bindEvents();
    await get('checkpoint-mark').listeners.click();
    assert.equal(JSON.parse(local.getItem(key)).day, '2026-10-17');
});

test('filter loading failures remain visible after a successful changelog load', async () => {
    let calls = 0;
    const { view, get } = changelog(async () => {
        if (++calls === 1) return { ok: false, status: 500 };
        return response(complete());
    });
    await view.loadFilterOptions();
    await view.loadChangelog();
    assert.match(get('checkpoint-status').textContent, /Filter choices could not be loaded/);
});

test('all events load before client filters; features and events have distinct counts', async () => {
    let url;
    const data = complete();
    data.days = [{ date: '2026-10-19', items: [
        { release_item_id: 'one', product_name: 'Power BI' },
        { release_item_id: 'one', product_name: 'Power BI' },
        { release_item_id: 'two', product_name: 'Other' },
    ] }];
    const { view, get } = changelog(async request => { url = request; return response(data); });
    get('product-filter').value = 'Power BI';
    await view.loadChangelog();
    assert.ok(url.includes('since=2026-10-17'));
    assert.ok(!url.includes('product_name'));
    assert.equal(view.rendered[0].count, 2);
    assert.match(get('catch-up-summary').textContent, /1 features changed in 2 change events/);
    assert.match(get('catch-up-summary').textContent, /all products/);
});

test('a late earlier response cannot enable acknowledgement after a newer failed load', async () => {
    let resolve;
    let calls = 0;
    const { view, get } = changelog(() => {
        if (++calls === 1) return new Promise(done => { resolve = done; });
        return Promise.resolve({ ok: false, status: 500 });
    });
    const earlier = view.loadChangelog();
    await view.loadChangelog();
    resolve(response(complete()));
    await earlier;
    assert.equal(view.loadedCheckpoint, null);
    assert.equal(get('checkpoint-mark').disabled, true);
});
