const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function setup(writeText) {
    const status = { textContent: '' };
    const input = {
        value: '', focus() { this.focused = true; }, select() { this.selected = true; },
    };
    const recovery = { hidden: true, querySelector: () => input };
    const block = { querySelector: selector => selector === '[role="status"]' ? status : recovery };
    const button = {
        disabled: false,
        dataset: { copyTarget: 'command', copyLabel: 'Claude Code command' },
        closest: () => block,
    };
    const text = 'claude mcp add --transport http fabric-gps https://gps.example/mcp?x=1&y=2';
    let click;
    const sandbox = {
        navigator: { clipboard: writeText ? { writeText } : undefined },
        document: {
            getElementById: () => ({ textContent: text }),
            addEventListener(_name, handler) { click = handler; },
        },
    };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '..', 'static', 'endpoints.js'), 'utf8'), sandbox);
    return { click: () => click({ target: { closest: () => button } }), button, status, input, recovery, text };
}

test('copy writes exact command and explicitly announces success without shifting focus', async () => {
    let copied;
    const env = setup(async value => { copied = value; });
    await env.click();
    assert.equal(copied, env.text);
    assert.equal(env.status.textContent, 'Copied Claude Code command.');
    assert.equal(env.recovery.hidden, true);
    assert.equal(env.input.focused, undefined);
    assert.equal(env.button.disabled, false);
});

test('denied and missing clipboard expose selected manual copy with an explicit error', async () => {
    for (const writeText of [undefined, async () => { throw new Error('denied'); }]) {
        const env = setup(writeText);
        await env.click();
        assert.match(env.status.textContent, /Could not copy automatically/);
        assert.equal(env.recovery.hidden, false);
        assert.equal(env.input.value, env.text);
        assert.equal(env.input.focused, true);
        assert.equal(env.input.selected, true);
        assert.equal(env.button.disabled, false);
    }
});

test('pending guard prevents duplicate writes and a successful retry clears manual recovery', async () => {
    let finish;
    let calls = 0;
    const env = setup(() => { calls++; return new Promise(resolve => { finish = resolve; }); });
    env.recovery.hidden = false;
    env.status.textContent = 'Old failure';
    const pending = env.click();
    assert.equal(env.button.disabled, true);
    assert.equal(env.status.textContent, '');
    assert.equal(env.recovery.hidden, true);
    await env.click();
    assert.equal(calls, 1);
    finish();
    await pending;
    assert.equal(env.button.disabled, false);
    assert.match(env.status.textContent, /^Copied/);
});
