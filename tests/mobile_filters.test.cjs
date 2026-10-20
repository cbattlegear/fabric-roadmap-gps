const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

class Element {
    constructor(tagName) {
        this.tagName = tagName.toUpperCase();
        this.children = [];
        this.listeners = {};
        this.value = '';
        this.checked = true;
        this.open = false;
    }
    append(...nodes) {
        for (const node of nodes) {
            if (node.parentNode) {
                node.parentNode.children = node.parentNode.children.filter(child => child !== node);
            }
            node.parentNode = this;
            this.children.push(node);
        }
    }
    insert(node, reference, offset) {
        if (node.parentNode) {
            node.parentNode.children = node.parentNode.children.filter(child => child !== node);
        }
        node.parentNode = this;
        this.children.splice(this.children.indexOf(reference) + offset, 0, node);
    }
    before(node) { this.parentNode.insert(node, this, 0); }
    after(node) { this.parentNode.insert(node, this, 1); }
    contains(node) { return node === this || this.children.some(child => child.contains(node)); }
    addEventListener(type, listener) { this.listeners[type] = listener; }
    descendants() { return this.children.flatMap(child => [child, ...child.descendants()]); }
    get selectedOptions() { return this.options.filter(option => option.value === this.value); }
    querySelectorAll(selector) {
        return this.descendants().filter(node => selector === '[data-secondary-filter]' && node.secondary);
    }
    querySelector(selector) {
        return this.descendants().find(node =>
            selector === '.filter-actions' ? node.actions :
            selector === 'label' ? node.tagName === 'LABEL' :
            selector === 'select' ? node.tagName === 'SELECT' :
            ['INPUT', 'SELECT'].includes(node.tagName));
    }
}

function setup(mobile = true) {
    const container = new Element('div');
    const fields = {};
    const groups = {};
    const document = {
        activeElement: null,
        createElement: tag => new Element(tag),
        createComment: () => new Element('#comment'),
        querySelector: () => container,
    };
    const ids = [
        'product-filter', 'type-filter', 'status-filter', 'days-filter',
        'sort-select', 'search-input', 'include-inactive',
    ];
    for (const id of ids) {
        const group = groups[id] = new Element('div');
        const field = fields[id] = new Element(id === 'search-input' || id === 'include-inactive' ? 'input' : 'select');
        field.type = id === 'include-inactive' ? 'checkbox' : 'text';
        field.value = id === 'sort-select' ? 'last_modified' : '';
        field.focus = () => { document.activeElement = field; };
        group.secondary = !['product-filter', 'search-input'].includes(id);
        group.append(field);
        const labels = {
            'type-filter': 'Release Type:', 'status-filter': 'Status:',
            'days-filter': 'Modified Within:', 'sort-select': 'Sort By:',
            'include-inactive': 'Removed items',
        };
        const label = new Element('label');
        label.textContent = labels[id] || id;
        group.append(label);
        field.options = ['', 'Preview', 'Shipped', 'General availability', '14', 'release_date', 'last_modified']
            .map(value => ({ value, textContent: ({
                '14': 'Last 14 days', release_date: 'Release Date', last_modified: 'Last Modified',
            })[value] || value }));
        container.append(group);
    }
    const actions = new Element('div');
    actions.actions = true;
    container.append(actions);
    const original = [...container.children];
    const media = { matches: mobile, addEventListener(type, fn) { this[type] = fn; } };
    const sandbox = { document, matchMedia: () => media };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '..', 'static', 'mobile-filters.js'), 'utf8'), sandbox);
    const ctl = sandbox.RoadmapFilters.create();
    ctl.summary.focus = () => { document.activeElement = ctl.summary; };
    return { ctl, media, document, container, fields, groups, original };
}

test('mobile disclosure keeps the original live fields and only Product/Search upfront', () => {
    const { ctl, container, fields, groups } = setup();
    assert.equal(ctl.details.open, false);
    assert.equal(ctl.details.hidden, false);
    assert.equal(ctl.summary.textContent, 'More filters');
    assert.equal(ctl.applied.hidden, true);
    assert.equal(ctl.panel.children.length, 5);
    for (const [id, field] of Object.entries(fields)) {
        assert.equal(container.contains(field), true);
        assert.equal(groups[id].children[0], field);
        assert.equal(ctl.panel.contains(field), !['product-filter', 'search-input'].includes(id));
    }
});

test('active secondary count includes sort/window/removed but not upfront Search/Product', () => {
    const { ctl, fields, container } = setup();
    fields['product-filter'].value = 'Data & AI';
    fields['search-input'].value = 'lake';
    ctl.update();
    assert.equal(ctl.summary.textContent, 'More filters');
    fields['type-filter'].value = 'Preview';
    fields['status-filter'].value = 'Shipped';
    fields['sort-select'].value = 'release_date';
    fields['days-filter'].value = '14';
    fields['include-inactive'].checked = false;
    container.listeners.change();
    assert.equal(ctl.summary.textContent, 'More filters (5 active)');
    assert.equal(ctl.applied.textContent,
        'Release Type: Preview; Status: Shipped; Modified Within: Last 14 days; Sort By: Release Date; Removed items: Excluded');
    assert.equal(ctl.applied.hidden, false);
    fields['type-filter'].value = fields['status-filter'].value = fields['days-filter'].value = '';
    fields['sort-select'].value = 'last_modified';
    fields['include-inactive'].checked = true;
    ctl.update();
    assert.equal(ctl.summary.textContent, 'More filters');
    assert.equal(ctl.applied.textContent, '');
    assert.equal(ctl.applied.hidden, true);
});

test('named criteria use literal option labels without HTML, truncation or disclosure expansion', () => {
    const { ctl, fields, media } = setup();
    const label = '<img src=x onerror=alert(1)> & a very long release type '.repeat(20);
    fields['type-filter'].options.push({ value: 'long', textContent: label });
    fields['type-filter'].value = 'long';
    ctl.update();
    assert.equal(ctl.applied.textContent, `Release Type: ${label}`);
    assert.equal(ctl.applied.children.length, 0);
    assert.equal(ctl.details.open, false);
    for (const mobile of [false, true]) {
        media.matches = mobile;
        media.change();
        assert.equal(ctl.applied.textContent, `Release Type: ${label}`);
    }
});

test('desktop restores the exact original field/tab order and mobile round trips without clones', () => {
    const env = setup(false);
    const { ctl, fields, container, original, media } = env;
    const visible = () => container.children.filter(node => node.tagName !== '#COMMENT' && node !== ctl.details);
    assert.deepEqual(visible(), original);
    assert.equal(ctl.details.hidden, true);
    fields['status-filter'].value = 'Shipped';
    media.matches = true;
    media.change();
    ctl.details.open = true;
    media.matches = false;
    media.change();
    assert.deepEqual(visible(), original);
    assert.equal(fields['status-filter'].value, 'Shipped');
    media.matches = true;
    media.change();
    assert.equal(ctl.details.open, true);
    assert.equal(ctl.summary.textContent, 'More filters (1 active)');
});

test('Escape returns focus to summary; a focused field stays reachable across breakpoints', () => {
    const { ctl, fields, media, document } = setup(false);
    fields['type-filter'].focus();
    media.matches = true;
    media.change();
    assert.equal(ctl.details.open, true);
    assert.equal(document.activeElement, fields['type-filter']);
    let prevented = false;
    ctl.details.listeners.keydown({ key: 'Enter', preventDefault() { prevented = true; } });
    assert.equal(ctl.details.open, true);
    ctl.details.listeners.keydown({ key: 'Escape', preventDefault() { prevented = true; } });
    assert.equal(prevented, true);
    assert.equal(ctl.details.open, false);
    assert.equal(document.activeElement, ctl.summary);
    media.matches = false;
    media.change();
    assert.equal(document.activeElement, fields['product-filter']);
});
