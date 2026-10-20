(function (root) {
    'use strict';
    document.addEventListener('click', async event => {
        const button = event.target.closest('.api-docs [data-copy-target]');
        if (!button || button.disabled) return;
        const block = button.closest('[data-copy-block]');
        const status = block.querySelector('[role="status"]');
        const recovery = block.querySelector('[data-copy-recovery]');
        const text = document.getElementById(button.dataset.copyTarget).textContent;
        button.disabled = true;
        status.textContent = '';
        recovery.hidden = true;
        try {
            await root.navigator.clipboard.writeText(text);
            status.textContent = `Copied ${button.dataset.copyLabel}.`;
        } catch (error) {
            const input = recovery.querySelector('input');
            recovery.hidden = false;
            input.value = text;
            input.focus();
            input.select();
            status.textContent = 'Could not copy automatically. Copy the selected text below.';
        } finally {
            button.disabled = false;
        }
    });
})(globalThis);
