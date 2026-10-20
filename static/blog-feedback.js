/* Feedback stays browser-local until an explicit vote; cached pages are anonymous. */
(() => {
    const storageKey = 'fabric-gps-blog-feedback';
    const retentionMs = 180 * 24 * 60 * 60 * 1000;
    const states = new Map();

    function validUrl(value) {
        if (typeof value !== 'string' || value.length > 1000
            || !/^https?:\/\//i.test(value) || /[\\\s\u0000-\u001f]/.test(value)) return false;
        try {
            const url = new URL(value);
            return ['http:', 'https:'].includes(url.protocol) && Boolean(url.hostname) && !url.username && !url.password;
        } catch {
            return false;
        }
    }

    function attribute(value) {
        return String(value).replace(/[&<>"']/g, char => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        })[char]);
    }

    function pairKey(releaseId, blogUrl) {
        const url = new URL(blogUrl);
        const messageId = url.pathname.match(/\/(\d+)\/?$/);
        const communityArticle = url.hostname === 'community.fabric.microsoft.com'
            && (!url.port || url.port === '443')
            && /^\/(t5|blog)\//.test(url.pathname) && messageId;
        const identity = communityArticle
            ? `community:${messageId[1]}`
            : url.origin + url.pathname + url.search;
        return JSON.stringify([releaseId, identity]);
    }

    function refreshControls(group, state) {
        const key = pairKey(group.dataset.releaseId, group.dataset.blogUrl);
        const render = target => {
            target.hidden = !state.revealed;
            target.setAttribute('aria-busy', String(state.pending));
            target.dataset.state = state.phase;
            target.dataset.stale = String(state.stale);
            target.querySelector('.blog-feedback-status').textContent = state.message;
            target.querySelectorAll('button').forEach(button => {
                button.disabled = state.pending || state.stale;
                button.setAttribute('aria-pressed', String(button.dataset.feedbackVote === state.vote));
            });
        };
        render(group);
        document.querySelectorAll('.blog-feedback').forEach(target => {
            if (target !== group && pairKey(target.dataset.releaseId, target.dataset.blogUrl) === key) {
                render(target);
            }
        });
    }

    function markup(releaseId, blogUrl) {
        if (!validUrl(blogUrl)) return '';
        const state = states.get(pairKey(releaseId, blogUrl));
        const disabled = state?.pending || state?.stale ? ' disabled' : '';
        return `<div class="blog-feedback"${state?.revealed ? '' : ' hidden'} data-release-id="${attribute(releaseId)}" data-blog-url="${attribute(blogUrl)}" role="group" aria-label="Feedback on this related article" aria-busy="${Boolean(state?.pending)}" data-state="${attribute(state?.phase || '')}" data-stale="${Boolean(state?.stale)}">
            <span class="blog-feedback-label">Is this article related?</span>
            <div class="blog-feedback-actions">
                <button type="button" data-feedback-vote="helpful" aria-pressed="${state?.vote === 'helpful'}"${disabled}>Helpful</button>
                <button type="button" data-feedback-vote="not_related" aria-pressed="${state?.vote === 'not_related'}"${disabled}>Not related</button>
            </div>
            <span class="blog-feedback-status" role="status" aria-live="polite">${attribute(state?.message || '')}</span>
        </div>`;
    }

    function voterKey() {
        const stored = localStorage.getItem(storageKey);
        if (stored) {
            const data = JSON.parse(stored);
            if (/^[0-9a-f]{64}$/.test(data.key) && Number.isFinite(data.created)
                && data.created <= Date.now() && Date.now() - data.created < retentionMs) return data.key;
        }
        const bytes = crypto.getRandomValues(new Uint8Array(32));
        const key = Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
        localStorage.setItem(storageKey, JSON.stringify({ key, created: Date.now() }));
        return key;
    }

    function revealArticle(event) {
        const link = event.target.closest('[data-feedback-article]');
        if (!link || !validUrl(link.dataset.blogUrl)) return;
        const identity = pairKey(link.dataset.releaseId, link.dataset.blogUrl);
        const state = states.get(identity) || {
            pending: false, stale: false, vote: null, phase: '', message: ''
        };
        state.revealed = true;
        states.set(identity, state);
        document.querySelectorAll('.blog-feedback').forEach(group => {
            if (pairKey(group.dataset.releaseId, group.dataset.blogUrl) === identity) {
                refreshControls(group, state);
            }
        });
    }

    async function submit(group, vote) {
        const identity = pairKey(group.dataset.releaseId, group.dataset.blogUrl);
        const state = states.get(identity) || {
            pending: false, stale: false, vote: null, phase: '', message: ''
        };
        states.set(identity, state);
        if (!state.revealed) return;
        if (state.pending || state.stale) {
            refreshControls(group, state);
            return;
        }
        let key;
        try {
            key = voterKey();
        } catch {
            state.message = 'Browser storage is unavailable. Enable it, or clear this site\'s local storage, then try again.';
            state.phase = 'error';
            refreshControls(group, state);
            return;
        }
        state.pending = true;
        state.phase = 'pending';
        state.message = 'Saving feedback...';
        refreshControls(group, state);
        try {
            const response = await fetch('/api/blog-feedback', {
                method: 'POST',
                credentials: 'omit',
                cache: 'no-store',
                headers: { 'Content-Type': 'application/json', 'X-Feedback-Voter': key },
                body: JSON.stringify({
                    release_item_id: group.dataset.releaseId,
                    blog_url: group.dataset.blogUrl,
                    vote
                })
            });
            if (!response.ok) {
                const messages = {
                    403: 'Feedback was blocked. Open this page on the main site and try again.',
                    404: 'This release no longer exists. Refresh the page.',
                    409: 'This article match has changed. Refresh the page before voting.',
                    429: 'Too many votes. Please wait before trying again.'
                };
                if (response.status === 409 || response.status === 404) state.stale = true;
                throw new Error(messages[response.status] || 'Feedback could not be saved. Please try again later.');
            }
            const result = await response.json();
            if (!['recorded', 'changed', 'unchanged'].includes(result.status) || result.vote !== vote) {
                throw new Error('Feedback could not be confirmed. Please try again.');
            }
            state.vote = vote;
            state.phase = 'recorded';
            state.message = result.status === 'changed'
                ? 'Feedback changed. Thank you.'
                : 'Feedback recorded. Thank you. You can change your choice.';
        } catch (error) {
            state.phase = 'error';
            state.message = error instanceof TypeError
                ? 'Could not connect. Check your connection and try again.'
                : error.message;
        } finally {
            state.pending = false;
            refreshControls(group, state);
        }
    }

    document.addEventListener('click', event => {
        revealArticle(event);
        const button = event.target.closest('[data-feedback-vote]');
        if (!button) return;
        const group = button.closest('.blog-feedback');
        if (!group) return;
        event.stopPropagation();
        submit(group, button.dataset.feedbackVote);
    });
    document.addEventListener('auxclick', event => {
        if (event.button === 1) revealArticle(event);
    });

    window.BlogFeedback = { markup, validUrl };
})();
