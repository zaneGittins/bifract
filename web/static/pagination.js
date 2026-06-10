// Pagination management
const Pagination = {
    currentPage: 1,
    pageSize: 50,
    totalResults: 0,
    allResults: [],
    renderCallback: null,

    init(renderCallback) {
        this.renderCallback = renderCallback;

        document.addEventListener('click', (e) => {
            const sizeBtn = e.target.closest('.page-size-btn');
            if (sizeBtn) {
                this.pageSize = parseInt(sizeBtn.dataset.size);
                this.currentPage = 1;
                document.querySelectorAll('.page-size-btn').forEach(b => b.classList.remove('active'));
                sizeBtn.classList.add('active');
                this.updateDisplay();
            }

            const numBtn = e.target.closest('.page-num-btn:not(.ellipsis)');
            if (numBtn && numBtn.dataset.page) {
                this.currentPage = parseInt(numBtn.dataset.page);
                this.updateDisplay();
            }
        });
    },

    setResults(results) {
        this.allResults = results;
        this.totalResults = results.length;
        this.currentPage = 1;
        this.updateDisplay();
    },

    getCurrentPageResults() {
        const start = (this.currentPage - 1) * this.pageSize;
        return this.allResults.slice(start, start + this.pageSize);
    },

    getTotalPages() {
        return Math.ceil(this.totalResults / this.pageSize);
    },

    updateDisplay() {
        const bar = document.getElementById('paginationBar');
        const pageNumbers = document.getElementById('pageNumbers');
        const totalPages = this.getTotalPages();

        if (totalPages > 1) {
            bar.style.display = 'flex';
            pageNumbers.innerHTML = this._renderPageNumbers(totalPages);
        } else {
            bar.style.display = 'none';
        }

        if (this.renderCallback) {
            this.renderCallback(this.getCurrentPageResults());
        }
    },

    _renderPageNumbers(totalPages) {
        const cur = this.currentPage;
        let pages;

        if (totalPages <= 9) {
            pages = Array.from({ length: totalPages }, (_, i) => i + 1);
        } else {
            const set = new Set([1, totalPages, cur]);
            for (let i = Math.max(2, cur - 1); i <= Math.min(totalPages - 1, cur + 1); i++) set.add(i);
            pages = Array.from(set).sort((a, b) => a - b);
        }

        let html = '';
        let prev = 0;
        for (const p of pages) {
            if (p - prev > 1) html += `<button class="page-num-btn ellipsis" disabled>...</button>`;
            html += `<button class="page-num-btn${p === cur ? ' active' : ''}" data-page="${p}">${p}</button>`;
            prev = p;
        }
        return html;
    },

    reset() {
        this.currentPage = 1;
        this.allResults = [];
        this.totalResults = 0;
        const bar = document.getElementById('paginationBar');
        if (bar) bar.style.display = 'none';
    }
};

window.Pagination = Pagination;
