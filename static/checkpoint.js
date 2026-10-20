// Dates are inclusive UTC days, never content-event timestamps.
class CatchUpCheckpoint {
    constructor(local, session, utcDay = () => new Date().toISOString().slice(0, 10)) {
        this.local = local;
        this.session = session;
        this.utcDay = utcDay;
        this.key = 'fabric-gps.checkpoint.v1';
        this.baseline = null;
        this.error = '';
        this.available = true;
        try {
            const snapshot = session.getItem(this.key);
            const raw = snapshot === null ? local.getItem(this.key) : snapshot;
            if (raw !== null) {
                let value;
                try {
                    value = JSON.parse(raw);
                } catch (error) {
                    value = null;
                }
                if (!value || value.version !== 1 || (value.day !== null && !this.validDay(value.day))) {
                    this.error = 'Your checkpoint is invalid, future-dated, or older than 90 days. Reset it to start again.';
                } else {
                    this.baseline = value.day;
                }
            }
            // A null snapshot is intentional: first visits stay first visits on reload.
            session.setItem(this.key, this.error ? raw : JSON.stringify({ version: 1, day: this.baseline }));
        } catch (error) {
            this.available = false;
            this.error = 'Browser storage is unavailable or unreadable. Catch-up cannot save a checkpoint; the normal changelog still works.';
        }
    }

    get today() {
        return this.utcDay();
    }

    canonicalDay(day) {
        if (typeof day !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(day)) return false;
        const parsed = new Date(`${day}T00:00:00Z`);
        return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === day;
    }

    validDay(day) {
        if (!this.canonicalDay(day)) return false;
        const parsed = new Date(`${day}T00:00:00Z`);
        const age = (Date.parse(`${this.today}T00:00:00Z`) - parsed.getTime()) / 86400000;
        return age >= 0 && age <= 90;
    }

    acknowledge(day) {
        if (!this.available || !this.validDay(day)) throw new Error('Cannot save this checkpoint.');
        const raw = this.local.getItem(this.key);
        let saved = null;
        if (raw !== null) {
            const value = JSON.parse(raw);
            // A newer tab may have crossed UTC midnight since this tab opened.
            if (value.version === 1 && this.canonicalDay(value.day)) saved = value.day;
        }
        // An older tab must not move a newer saved checkpoint backwards.
        this.local.setItem(this.key, JSON.stringify({ version: 1, day: saved && saved > day ? saved : day }));
        if (this.baseline === null) {
            try {
                this.session.setItem(this.key, JSON.stringify({ version: 1, day }));
            } catch (error) {
                this.error = 'The checkpoint was saved, but this tab could not update its session snapshot. Open a new tab to use it.';
                return this.error;
            }
            this.baseline = day;
        }
        if (saved && saved > day) return `A newer checkpoint (${saved} UTC) was already saved and has been kept. This tab keeps its session baseline.`;
        // Keep this tab's session baseline fixed, including across reloads.
    }

    reset() {
        this.local.removeItem(this.key);
        this.session.removeItem(this.key);
    }
}
