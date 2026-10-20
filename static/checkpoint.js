// A visit is one document lifetime. Filters/history reuse its complete snapshot.
class ChangelogVisit {
    constructor(local, locks) {
        this.local = local;
        this.locks = locks;
        this.key = 'fabric-gps.changelog-visit.v2';
        this.baseline = null;
        this.error = '';
        try {
            const raw = local.getItem(this.key);
            if (raw !== null) this.baseline = this.read(raw);
        } catch (error) {
            this.error = 'Visit history could not be read. Highlights will restart after this visit.';
        }
    }

    static timestamp(value) {
        if (typeof value !== 'string') return null;
        const match = value.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,7}))?Z$/);
        if (!match) return null;
        const parsed = new Date(match[1] + 'Z');
        if (!Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 19) !== match[1]) return null;
        return match[1] + '.' + (match[2] || '').padEnd(7, '0') + 'Z';
    }

    static windowStart(cutoff) {
        const day = new Date(cutoff.slice(0, 19) + 'Z');
        day.setUTCDate(day.getUTCDate() - 90);
        return day.toISOString().slice(0, 19) + cutoff.slice(19);
    }

    read(raw) {
        const saved = JSON.parse(raw);
        const cutoff = ChangelogVisit.timestamp(saved?.cutoff);
        if (saved?.version !== 2 || !cutoff || !Array.isArray(saved.event_ids) ||
            saved.event_ids.length > 10000 ||
            !Array.isArray(saved.event_times) || saved.event_times.length !== saved.event_ids.length ||
            saved.event_ids.some(id => typeof id !== 'string' || !/^[a-f0-9]{64}$/.test(id))) {
            throw new Error('Invalid visit history');
        }
        const events = new Map();
        saved.event_ids.forEach((id, index) => {
            const observed = ChangelogVisit.timestamp(saved.event_times[index]);
            if (!observed || observed >= cutoff || events.has(id)) throw new Error('Invalid visit event time');
            events.set(id, observed);
        });
        return { cutoff, event_ids: new Set(events.keys()), events };
    }

    snapshot(data) {
        const cutoff = ChangelogVisit.timestamp(data?.cutoff);
        const start = ChangelogVisit.timestamp(data?.window_start);
        if (data?.mode !== 'visit' || data.complete !== true || !cutoff || !start ||
            start !== ChangelogVisit.windowStart(cutoff) ||
            (this.baseline && cutoff < this.baseline.cutoff) || !Array.isArray(data.days)) {
            throw new Error('Incomplete or stale visit snapshot');
        }
        const ids = new Set();
        const events = new Map();
        for (const day of data.days) {
            if (!Array.isArray(day.items) || day.count !== day.items.length) throw new Error('Partial visit snapshot');
            for (const item of day.items) {
                const observed = ChangelogVisit.timestamp(item.observed_at);
                if (!observed || observed < start || observed >= cutoff || observed.slice(0, 10) !== day.date ||
                    !/^[a-f0-9]{64}$/.test(item.event_id) || ids.has(item.event_id)) {
                    throw new Error('Invalid visit event');
                }
                ids.add(item.event_id);
                events.set(item.event_id, observed);
            }
        }
        if (ids.size > 10000 || data.total_items !== ids.size) throw new Error('Incomplete visit manifest');
        return { data, cutoff, ids, events };
    }

    isNew(item) {
        return Boolean(this.baseline && !this.baseline.event_ids.has(item.event_id));
    }

    async acknowledge(snapshot) {
        if (!this.locks?.request) {
            this.error = 'Visit tracking is unavailable in this browser.';
            return;
        }
        try {
            await this.locks.request(this.key, () => {
                const raw = this.local.getItem(this.key);
                let saved = null;
                if (raw !== null) {
                    try {
                        saved = this.read(raw);
                    } catch (error) {
                        // A complete displayed snapshot can replace damaged old history.
                    }
                }
                const cutoff = saved && saved.cutoff > snapshot.cutoff ? saved.cutoff : snapshot.cutoff;
                const start = ChangelogVisit.windowStart(cutoff);
                // A slower older request may contain a late commit that a
                // newer-cutoff request did not see. Union completed displays,
                // then expire individual events outside retained coverage.
                const events = new Map([...(saved?.events || []), ...snapshot.events]);
                for (const [id, observed] of events) {
                    if (observed < start) events.delete(id);
                }
                if (events.size > 10000) throw new RangeError('Visit history limit exceeded');
                this.local.setItem(this.key, JSON.stringify({
                    version: 2, cutoff, event_ids: [...events.keys()], event_times: [...events.values()],
                }));
                // A day-only manual baseline cannot be upgraded into an exact event manifest.
                this.local.removeItem('fabric-gps.checkpoint.v1');
            });
        } catch (error) {
            this.error = error instanceof RangeError
                ? 'Visit history is too large to save. Changes were not marked seen.'
                : 'Visit tracking could not be saved. Allow browser storage, then reload.';
        }
        // Deliberately retain the original baseline while this document is open.
    }
}
