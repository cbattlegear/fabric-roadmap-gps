// Per-day changelog page. Reads the target date from the #changelog-content
// data attribute and loads only that day's changes from the API.

class ChangelogDay {
    constructor() {
        this.container = document.getElementById('changelog-content');
        this.date = this.container ? this.container.dataset.date : '';
        this.init();
    }

    async init() {
        if (!this.container || !this.date) return;
        let version = '';
        try {
            const resp = await fetch('/api/version');
            const data = await resp.json();
            version = data.version || '';
        } catch (e) {
            version = '';
        }
        await this.load(version);
    }

    async load(version) {
        const params = new URLSearchParams();
        params.set('date', this.date);
        params.set('include_inactive', 'true');
        if (version) params.set('v', version);

        try {
            const response = await fetch(`/api/changelog?${params.toString()}`);
            const data = await response.json();
            this.render(data.days || []);
        } catch (e) {
            this.container.innerHTML = '<div class="no-results"><p>Error loading changes for this day. Please try again later.</p></div>';
        }
    }

    render(days) {
        if (!days.length) {
            this.container.innerHTML = '<div class="no-results"><p>No roadmap changes were recorded on this day.</p></div>';
            return;
        }
        this.container.innerHTML = renderChangelogDays(days, { dayLinks: false });
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new ChangelogDay();
});
