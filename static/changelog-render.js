// Shared rendering helpers for the changelog pages (full changelog + per-day
// permalink page). Exposed as globals so both page scripts can reuse them
// without a build step.

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Parse a stored roadmap date for display. `last_modified` is a UTC calendar
// date with no time component (e.g. "2026-06-03"). Passing that straight to
// `new Date()` parses it as UTC midnight, which `toLocaleDateString` then
// shifts into the viewer's timezone — showing the previous day for anyone west
// of UTC. Constructing the Date from explicit components yields local midnight
// of the exact stored day, so the displayed date always matches what's stored.
function parseRoadmapDate(value) {
    if (typeof value === 'string') {
        const m = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
    }
    return new Date(value);
}

function parseChangeColumn(col) {
    if (col.startsWith('Release Date ')) return { change: 'Date Changed', detail: col.slice(13).replace(' -> ', ' → ') };
    if (col.startsWith('Release Type ')) return { change: 'Type Changed', detail: col.slice(13).replace(' -> ', ' → ') };
    if (col.startsWith('Release Status ')) return { change: 'Status Changed', detail: col.slice(15).replace(' -> ', ' → ') };
    if (col.startsWith('Name ')) return { change: 'Name Changed', detail: col.slice(5).replace(' -> ', ' → ') };
    if (col.startsWith('Semester ')) return { change: 'Semester Changed', detail: col.slice(9).replace(' -> ', ' → ') };
    if (col.startsWith('Workload ')) return { change: 'Workload Changed', detail: col.slice(9).replace(' -> ', ' → ') };
    if (col === 'Removed from Roadmap') return { change: 'Removed', detail: 'Removed from Roadmap', removed: true };
    if (col === 'Restored to Roadmap') return { change: 'Restored', detail: 'Restored to Roadmap' };
    if (col === 'Added to roadmap') return { change: 'Added', detail: 'Added to roadmap' };
    return { change: col, detail: '' };
}

// Build the HTML for a list of changelog "days". When opts.dayLinks is true,
// each day's header links to its permanent /changelog/<date> page and exposes
// a copy-link button (used on the full changelog). On the per-day page that
// chrome is redundant, so it's omitted.
function renderChangelogDays(days, opts) {
    opts = opts || {};
    const dayLinks = !!opts.dayLinks;
    let html = '';

    for (const day of days) {
        const dateObj = parseRoadmapDate(day.date);
        const dateStr = dateObj.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });
        const countLabel = `· ${day.count} change${day.count !== 1 ? 's' : ''}`;

        let header;
        if (dayLinks) {
            const permalink = `/changelog/${encodeURIComponent(day.date)}`;
            const absUrl = `${window.location.origin}${permalink}`;
            header = `<div class="changelog-day-header">
                    <a class="changelog-date-link" href="${escapeHtml(permalink)}"><h2 class="changelog-date">${escapeHtml(dateStr)}</h2></a>
                    <span class="changelog-count">${escapeHtml(countLabel)}</span>
                    <button type="button" class="changelog-copy-link" data-copy-url="${escapeHtml(absUrl)}" title="Copy permalink to this day" aria-label="Copy permalink to changes on ${escapeHtml(dateStr)}">Copy link</button>
                </div>`;
        } else {
            header = `<div class="changelog-day-header">
                    <h2 class="changelog-date">${escapeHtml(dateStr)}</h2>
                    <span class="changelog-count">${escapeHtml(countLabel)}</span>
                </div>`;
        }

        html += `<div class="changelog-day">${header}`;

        // Group by workload
        const workloads = {};
        for (const item of day.items) {
            const wl = item.product_name || 'Unknown';
            if (!workloads[wl]) workloads[wl] = [];
            workloads[wl].push(item);
        }

        for (const [workload, items] of Object.entries(workloads).sort((a, b) => a[0].localeCompare(b[0]))) {
            html += `<div class="changelog-workload">
                    <h3 class="changelog-workload-name">${escapeHtml(workload)}</h3>`;

            for (const item of items) {
                const inactiveClass = item.active === false ? ' changelog-item-inactive' : '';
                const cols = item.changed_columns && item.changed_columns.length ? item.changed_columns : null;

                if (cols) {
                    for (const col of cols) {
                        const parsed = parseChangeColumn(col);
                        const removedClass = parsed.removed ? ' changelog-cell-removed' : '';
                        html += `<a href="/release/${escapeHtml(item.release_item_id)}" class="changelog-row${inactiveClass}">
                                <div class="changelog-cell-name">${escapeHtml(item.feature_name)}</div>
                                <div class="changelog-cell-change${removedClass}">${escapeHtml(parsed.change)}</div>
                                <div class="changelog-cell-detail">${escapeHtml(parsed.detail)}</div>
                            </a>`;
                    }
                } else {
                    html += `<a href="/release/${escapeHtml(item.release_item_id)}" class="changelog-row${inactiveClass}">
                            <div class="changelog-cell-name">${escapeHtml(item.feature_name)}</div>
                            <div class="changelog-cell-change">Updated</div>
                            <div class="changelog-cell-detail"></div>
                        </a>`;
                }
            }

            html += '</div>';
        }

        html += '</div>';
    }

    return html;
}

// Wire up "Copy link" buttons inside a container using event delegation, so it
// keeps working after the container's innerHTML is replaced on each render.
function attachCopyHandlers(container) {
    if (!container || container.dataset.copyHandlersBound === 'true') return;
    container.dataset.copyHandlersBound = 'true';
    container.addEventListener('click', async (e) => {
        const btn = e.target.closest('.changelog-copy-link');
        if (!btn) return;
        e.preventDefault();
        const url = btn.getAttribute('data-copy-url');
        if (!url) return;
        const original = btn.textContent;
        try {
            if (navigator.clipboard && navigator.clipboard.writeText) {
                await navigator.clipboard.writeText(url);
            } else {
                const tmp = document.createElement('textarea');
                tmp.value = url;
                tmp.style.position = 'fixed';
                tmp.style.opacity = '0';
                document.body.appendChild(tmp);
                tmp.select();
                document.execCommand('copy');
                document.body.removeChild(tmp);
            }
            btn.textContent = 'Copied!';
            btn.classList.add('changelog-copy-link-done');
        } catch (err) {
            btn.textContent = 'Copy failed';
        }
        setTimeout(() => {
            btn.textContent = original;
            btn.classList.remove('changelog-copy-link-done');
        }, 2000);
    });
}
