(function (root) {
    'use strict';

    class Controller {
        constructor(container) {
            this.container = container;
            this.groups = Array.from(container.querySelectorAll('[data-secondary-filter]'));
            this.fields = this.groups.map(group => group.querySelector('input, select'));
            this.defaults = this.fields.map(field => this.value(field));
            this.positions = this.groups.map(group => {
                const marker = document.createComment('filter position');
                group.before(marker);
                return marker;
            });
            this.details = document.createElement('details');
            this.details.className = 'mobile-filter-details';
            this.summary = document.createElement('summary');
            this.panel = document.createElement('div');
            this.panel.className = 'mobile-filter-fields';
            this.details.append(this.summary, this.panel);
            container.querySelector('.filter-actions').before(this.details);
            this.media = root.matchMedia('(max-width: 768px)');
            this.media.addEventListener('change', () => this.adapt());
            container.addEventListener('change', () => this.update());
            this.details.addEventListener('keydown', event => {
                if (event.key === 'Escape' && this.details.open) {
                    this.details.open = false;
                    this.summary.focus();
                    event.preventDefault();
                }
            });
            this.adapt();
        }

        value(field) {
            return field.type === 'checkbox' ? String(field.checked) : field.value;
        }

        update() {
            const count = this.fields.filter((field, index) =>
                this.value(field) !== this.defaults[index]).length;
            this.summary.textContent = 'More filters' + (count ? ` (${count} active)` : '');
        }

        adapt() {
            const focused = document.activeElement;
            const inFields = this.groups.some(group => group.contains(focused));
            // Reuse the live fields; markers preserve the original desktop DOM/tab order.
            if (this.media.matches) {
                this.details.hidden = false;
                if (inFields) this.details.open = true;
                this.panel.append(...this.groups);
            } else {
                this.groups.forEach((group, index) => this.positions[index].after(group));
                this.details.hidden = true;
            }
            if (inFields) focused.focus({ preventScroll: true });
            else if (focused === this.summary && !this.media.matches) {
                this.container.querySelector('select').focus({ preventScroll: true });
            }
            this.update();
        }
    }

    root.RoadmapFilters = {
        Controller,
        create() {
            const container = document.querySelector('[data-mobile-filters]');
            return container ? new Controller(container) : null;
        },
    };
})(globalThis);
