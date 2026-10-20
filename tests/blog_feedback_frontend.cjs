// Runs under pytest using Node's built-in assertions and a bounded DOM fake.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const { webcrypto } = require('node:crypto');

const listeners = {};
const storage = new Map();
const calls = [];
let mountedGroups = [];
let groupId = 0;
let response = { ok: true, status: 200, json: async () => ({ status: 'recorded', vote: 'helpful' }) };
const document = {
    addEventListener: (name, handler) => { (listeners[name] ||= []).push(handler); },
    querySelectorAll: () => mountedGroups,
    createElement: () => ({
        set textContent(value) {
            this.innerHTML = String(value).replace(/[&<>"']/g, char => ({
                '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
            })[char]);
        }
    })
};
const context = vm.createContext({
    document, window: {}, URL, Uint8Array, crypto: webcrypto, Date, console,
    localStorage: {
        getItem: key => storage.get(key) || null,
        setItem: (key, value) => storage.set(key, value)
    },
    fetch: async (url, options) => {
        calls.push({ url, ...options });
        if (response instanceof Error) throw response;
        if (typeof response === 'function') return response();
        return response;
    }
});
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'static', 'blog-feedback.js'), 'utf8'), context);
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'static', 'app.js'), 'utf8')
    + '\nwindow.RecentChanges = RecentChanges;', context);

const { BlogFeedback } = context.window;
const url = 'https://example.com/blog?x=1&y=2';
const card = Object.create(context.window.RecentChanges.prototype);
const item = {
    release_item_id: 'r1', feature_name: 'Feature', last_modified: '2026-10-19',
    blog_title: 'Article', blog_url: url
};

function group(releaseId = `test-${++groupId}`, blogUrl = url) {
    const attributes = {};
    const buttons = ['helpful', 'not_related'].map(vote => ({
        dataset: { feedbackVote: vote }, disabled: false,
        setAttribute: (name, value) => { attributes[vote + name] = value; }
    }));
    const status = { textContent: '' };
    return {
        dataset: { releaseId, blogUrl }, buttons, status, attributes, hidden: true,
        getAttribute: name => attributes[name],
        setAttribute: (name, value) => { attributes[name] = value; },
        querySelectorAll: () => buttons,
        querySelector: () => status
    };
}

function activate(target, type = 'click', button = 0, modifiers = {}) {
    const link = { dataset: target.dataset };
    const event = {
        button,
        ...modifiers,
        target: { closest: selector => selector === '[data-feedback-article]' ? link : null },
        preventDefault: () => assert.fail('Article navigation must not be prevented'),
        stopPropagation: () => assert.fail('Article activation must not be swallowed'),
    };
    listeners[type].forEach(listener => listener(event));
}

function click(target, choice = 'helpful') {
    activate(target);
    let stopped = false;
    const button = { dataset: { feedbackVote: choice }, closest: () => target };
    listeners.click[0]({
        target: { closest: selector => selector === '[data-feedback-vote]' ? button : null },
        stopPropagation: () => { stopped = true; }
    });
    assert.equal(stopped, true);
}

const settle = () => new Promise(resolve => setImmediate(resolve));

(async () => {
    assert.equal(storage.size, 0, 'loading a cacheable page must not create identity');
    assert.equal(calls.length, 0);
    assert.match(BlogFeedback.markup('r"1', url), /data-release-id="r&quot;1"/);
    assert.match(BlogFeedback.markup('r1', url), /aria-live="polite"/);
    assert.match(BlogFeedback.markup('r1', url), /class="blog-feedback" hidden/);
    const visited = group('visited');
    const rematch = group('visited', 'https://example.com/rematch');
    mountedGroups = [visited, rematch];
    activate(visited);
    assert.equal(visited.hidden, false);
    assert.equal(rematch.hidden, true);
    assert.equal(storage.size, 0, 'Article activation must not create persistent click tracking or voter identity');
    assert.equal(calls.length, 0, 'Article activation must not call API or analytics');
    assert.doesNotMatch(BlogFeedback.markup('visited', url), /class="blog-feedback" hidden/);
    assert.match(BlogFeedback.markup('visited', rematch.dataset.blogUrl), /class="blog-feedback" hidden/);
    const middle = group('middle');
    mountedGroups = [middle];
    activate(middle, 'auxclick', 2);
    assert.equal(middle.hidden, true);
    activate(middle, 'auxclick', 1);
    assert.equal(middle.hidden, false);
    for (const modifiers of [{ ctrlKey: true }, { metaKey: true }, { shiftKey: true }, { detail: 0 }]) {
        const modified = group();
        mountedGroups = [modified];
        activate(modified, 'click', 0, modifiers);
        assert.equal(modified.hidden, false, 'Modified and keyboard clicks reveal without swallowing navigation');
    }
    mountedGroups = [];
    for (const invalid of [null, '', 'javascript:alert(1)', 'https://user:pw@example.com', 'https://[bad', 'https:example.com', 'https://example.com\\bad']) {
        assert.equal(BlogFeedback.markup('r1', invalid), '');
    }
    assert.match(card.createChangeItem(item), /blog-feedback/);
    assert.match(card.createChangeItem(item), />Article<\/span>/);
    assert.match(card.createChangeItem(item), /target="_blank" rel="noopener noreferrer"/);
    assert.doesNotMatch(card.createChangeItem(item), /onclick=|Related blog/);
    const hostile = card.createChangeItem({ ...item, blog_title: '<img src=x onerror="bad">' });
    assert.match(hostile, /&lt;img src=x onerror=&quot;bad&quot;&gt;/);
    assert.doesNotMatch(hostile, /<img src=x/);
    assert.match(card.createChangeItem(item), /<a href="\/release\/r1">Feature<\/a>/);
    assert.doesNotMatch(card.createChangeItem(item), /role="button"/);
    assert.doesNotMatch(card.createChangeItem({ ...item, blog_title: null }), /blog-feedback/);
    assert.doesNotMatch(card.createChangeItem({ ...item, blog_url: 'javascript:bad' }), /blog-feedback/);

    let resolveResponse;
    response = () => new Promise(resolve => { resolveResponse = resolve; });
    const first = group('r1');
    click(first);
    assert.equal(first.status.textContent, 'Saving feedback...');
    assert.ok(first.buttons.every(button => button.disabled));
    click(first);
    assert.equal(calls.length, 1, 'double clicks while pending must not submit again');
    const rerendered = card.createChangeItem(item);
    assert.match(rerendered, /Saving feedback/);
    assert.match(rerendered, /aria-busy="true"/);
    assert.match(rerendered, /data-feedback-vote="not_related"[^>]*disabled/);
    assert.doesNotMatch(card.createChangeItem({ ...item, blog_url: 'https://example.com/other' }), /aria-busy="true"/);
    assert.match(card.createChangeItem({ ...item, blog_url: 'https://example.com/other' }), /class="blog-feedback" hidden/);
    const replacement = group('r1');
    mountedGroups = [replacement];
    click(replacement, 'not_related');
    assert.equal(calls.length, 1, 'replacement controls must not overlap an outstanding vote');
    assert.ok(replacement.buttons.every(button => button.disabled));
    resolveResponse({ ok: true, json: async () => ({ status: 'recorded', vote: 'helpful' }) });
    await settle();
    assert.match(first.status.textContent, /recorded/);
    assert.equal(first.attributes['helpfularia-pressed'], 'true');
    assert.equal(first.attributes['not_relatedaria-pressed'], 'false');
    assert.ok(first.buttons.every(button => !button.disabled));
    assert.match(replacement.status.textContent, /recorded/);
    assert.equal(replacement.attributes['helpfularia-pressed'], 'true');
    assert.ok(replacement.buttons.every(button => !button.disabled));
    assert.match(card.createChangeItem(item), /data-feedback-vote="helpful" aria-pressed="true"/);
    assert.doesNotMatch(card.createChangeItem({ ...item, blog_url: 'https://example.com/other' }), /aria-pressed="true"/);
    assert.equal(calls[0].credentials, 'omit');
    assert.equal(calls[0].cache, 'no-store');
    const key = calls[0].headers['X-Feedback-Voter'];
    assert.match(key, /^[0-9a-f]{64}$/);
    assert.deepEqual(JSON.parse(calls[0].body), { release_item_id: 'r1', blog_url: url, vote: 'helpful' });

    response = { ok: true, json: async () => ({ status: 'changed', vote: 'not_related' }) };
    click(replacement, 'not_related');
    await settle();
    assert.match(replacement.status.textContent, /changed/);
    assert.equal(replacement.attributes['helpfularia-pressed'], 'false');
    assert.equal(replacement.attributes['not_relatedaria-pressed'], 'true');
    assert.equal(calls.at(-1).headers['X-Feedback-Voter'], key, 'changes and page instances share identity');

    response = { ok: false, status: 503 };
    click(replacement, 'helpful');
    await settle();
    assert.match(replacement.status.textContent, /could not be saved/);
    assert.equal(replacement.attributes['not_relatedaria-pressed'], 'true', 'failed changes preserve the confirmed choice');
    assert.match(card.createChangeItem(item), /data-feedback-vote="not_related" aria-pressed="true"/);
    assert.ok(replacement.buttons.every(button => !button.disabled));

    const permalink = 'https://community.fabric.microsoft.com/t5/Fabric-Updates-Blog/post/ba-p/12345';
    const canonical = 'https://community.fabric.microsoft.com/blog/fbc_fabricupdatesblogs/post/12345?source=rss';
    response = () => new Promise(resolve => { resolveResponse = resolve; });
    click(group('community-release', permalink));
    const aliasReplacement = group('community-release', canonical);
    mountedGroups = [aliasReplacement];
    const aliasCount = calls.length;
    click(aliasReplacement, 'not_related');
    assert.equal(calls.length, aliasCount, 'canonical aliases share the pending guard');
    assert.match(BlogFeedback.markup('community-release', canonical), /aria-busy="true"/);
    resolveResponse({ ok: true, json: async () => ({ status: 'recorded', vote: 'helpful' }) });
    await settle();
    assert.equal(aliasReplacement.attributes['helpfularia-pressed'], 'true');
    assert.match(aliasReplacement.status.textContent, /recorded/);
    assert.doesNotMatch(BlogFeedback.markup('different-release', canonical), /aria-pressed="true"/);

    for (const [status, message] of [[403, /blocked/], [404, /no longer exists/], [409, /match has changed/], [429, /Too many votes/], [503, /could not be saved/]]) {
        response = { ok: false, status };
        const target = group();
        click(target);
        await settle();
        assert.match(target.status.textContent, message);
        assert.equal(target.dataset.state, 'error');
        assert.equal(target.buttons[0].disabled, [404, 409].includes(status));
        if ([404, 409].includes(status)) {
            const count = calls.length;
            click(target);
            assert.equal(calls.length, count, 'stale groups require a refresh');
            const staleReplacement = group(target.dataset.releaseId);
            click(staleReplacement);
            assert.equal(calls.length, count, 'rerendered stale groups still require a refresh');
            assert.ok(staleReplacement.buttons.every(button => button.disabled));
            assert.match(BlogFeedback.markup(target.dataset.releaseId, url), /data-stale="true"/);
        }
    }
    response = { ok: true, json: async () => ({ unexpected: true }) };
    const invalidReply = group();
    click(invalidReply);
    await settle();
    assert.match(invalidReply.status.textContent, /could not be confirmed/);
    assert.notEqual(invalidReply.attributes['helpfularia-pressed'], 'true');

    response = new Error('Network failed');
    const failed = group();
    click(failed);
    await settle();
    assert.equal(failed.dataset.state, 'error');
    assert.ok(failed.buttons.every(button => !button.disabled));

    storage.set('fabric-gps-blog-feedback', JSON.stringify({ key, created: Date.now() - 181 * 86400000 }));
    response = { ok: true, json: async () => ({ status: 'recorded', vote: 'helpful' }) };
    click(group());
    await settle();
    assert.notEqual(calls.at(-1).headers['X-Feedback-Voter'], key, 'expired identifiers rotate');

    context.localStorage.getItem = () => { throw new Error('storage blocked'); };
    const unavailable = group();
    const before = calls.length;
    click(unavailable);
    assert.match(unavailable.status.textContent, /storage is unavailable/);
    assert.equal(calls.length, before);

    // Card listeners must not navigate for a feedback descendant, even before
    // the delegated document listener receives the event.
    let cardClick;
    const fakeCard = {
        addEventListener: (_, handler) => { cardClick = handler; },
        getAttribute: () => 'r1'
    };
    context.document.getElementById = id => id === 'changes-grid'
        ? { innerHTML: '', querySelectorAll: () => [fakeCard] } : { style: {} };
    card.currentData = [item];
    card.renderChanges();
    cardClick({ target: { closest: selector => {
        assert.equal(selector, 'a, button, .blog-feedback');
        return {};
    } } });
    assert.equal(context.window.location, undefined, 'feedback must not trigger card navigation');
    context.window.location = { href: '' };
    cardClick({ target: { closest: () => null } });
    assert.equal(context.window.location.href, '/release/r1');
    console.log('Feedback frontend states and navigation passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
