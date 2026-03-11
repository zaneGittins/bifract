// Pagination management
const Pagination = {
    currentPage: 1,
    pageSize: 50,
    totalResults: 0,
    allResults: [],
    renderCallback: null,

    init(renderCallback) {
        this.renderCallback = renderCallback;

        const pageSizeSelect = document.getElementById('pageSizeSelect');
        const prevBtn = document.getElementById('prevPageBtn');
        const nextBtn = document.getElementById('nextPageBtn');

        if (pageSizeSelect) {
            pageSizeSelect.addEventListener('change', (e) => {
                this.pageSize = parseInt(e.target.value);
                this.currentPage = 1;
                this.updateDisplay();
            });
        }

        if (prevBtn) {
            prevBtn.addEventListener('click', () => this.prevPage());
        }

        if (nextBtn) {
            nextBtn.addEventListener('click', () => this.nextPage());
        }
    },

    setResults(results) {
        this.allResults = results;
        this.totalResults = results.length;
        this.currentPage = 1;
        this.updateDisplay();
    },

    getCurrentPageResults() {
        const start = (this.currentPage - 1) * this.pageSize;
        const end = start + this.pageSize;
        return this.allResults.slice(start, end);
    },

    getTotalPages() {
        return Math.ceil(this.totalResults / this.pageSize);
    },

    prevPage() {
        if (this.currentPage > 1) {
            this.currentPage--;
            this.updateDisplay();
        }
    },

    nextPage() {
        if (this.currentPage < this.getTotalPages()) {
            this.currentPage++;
            this.updateDisplay();
        }
    },

    updateDisplay() {
        const paginationControls = document.getElementById('paginationControls');
        const prevBtn = document.getElementById('prevPageBtn');
        const nextBtn = document.getElementById('nextPageBtn');
        const pageInfo = document.getElementById('pageInfo');

        const totalPages = this.getTotalPages();

        if (totalPages > 1) {
            paginationControls.style.display = 'flex';
            prevBtn.disabled = this.currentPage === 1;
            nextBtn.disabled = this.currentPage === totalPages;
            pageInfo.textContent = `Page ${this.currentPage} of ${totalPages}`;
        } else {
            paginationControls.style.display = 'none';
        }

        // Trigger results re-render using callback
        if (this.renderCallback && typeof this.renderCallback === 'function') {
            this.renderCallback(this.getCurrentPageResults());
        }
    },

    reset() {
        this.currentPage = 1;
        this.allResults = [];
        this.totalResults = 0;
        const paginationControls = document.getElementById('paginationControls');
        if (paginationControls) {
            paginationControls.style.display = 'none';
        }
    }
};

// Make globally available
window.Pagination = Pagination;
