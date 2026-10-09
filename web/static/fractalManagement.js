const FractalManagement = {
    currentEditFractal: null,
    createType: 'fractal',

    init() {
        this.setupEventListeners();
        // Don't load fractals immediately - only when view is shown
    },

    setupEventListeners() {
        // Create fractal button
        const createFractalBtn = document.getElementById('createFractalBtn');
        if (createFractalBtn) {
            createFractalBtn.addEventListener('click', () => this.showCreateFractalModal());
        }

        // Modal close handlers
        this.setupModalHandlers();
    },

    setupModalHandlers() {
        // Create Fractal Modal
        const createModal = document.getElementById('createFractalModal');
        const createForm = document.getElementById('createFractalForm');
        const cancelCreateBtn = document.getElementById('cancelCreateBtn');

        if (createModal) {
            createModal.addEventListener('click', (e) => {
                if (e.target === createModal) {
                    this.hideCreateFractalModal();
                }
            });
        }

        const createCloseBtn = createModal?.querySelector('.close-btn');
        if (createCloseBtn) {
            createCloseBtn.addEventListener('click', () => this.hideCreateFractalModal());
        }

        if (cancelCreateBtn) {
            cancelCreateBtn.addEventListener('click', () => this.hideCreateFractalModal());
        }

        if (createForm) {
            createForm.addEventListener('submit', (e) => this.handleCreateFractal(e));
        }

        // Edit Fractal Modal
        const editModal = document.getElementById('editFractalModal');
        const editForm = document.getElementById('editFractalForm');
        const cancelEditBtn = document.getElementById('cancelEditBtn');

        if (editModal) {
            editModal.addEventListener('click', (e) => {
                if (e.target === editModal) {
                    this.hideEditFractalModal();
                }
            });
        }

        const editCloseBtn = editModal?.querySelector('.close-btn');
        if (editCloseBtn) {
            editCloseBtn.addEventListener('click', () => this.hideEditFractalModal());
        }

        if (cancelEditBtn) {
            cancelEditBtn.addEventListener('click', () => this.hideEditFractalModal());
        }

        if (editForm) {
            editForm.addEventListener('submit', (e) => this.handleEditFractal(e));
        }

        // Close modals on escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this.hideCreateFractalModal();
                this.hideEditFractalModal();
            }
        });
    },











    // Modal Management
    showCreateFractalModal() {
        const modal = document.getElementById('createFractalModal');
        if (modal) {
            modal.style.display = 'flex';

            const form = document.getElementById('createFractalForm');
            if (form) form.reset();

            this.setCreateType('fractal');

            const nameInput = document.getElementById('newFractalName');
            if (nameInput) setTimeout(() => nameInput.focus(), 100);
        }
    },

    setCreateType(type) {
        this.createType = type;
        const isFractal = type === 'fractal';
        const title = document.getElementById('createModalTitle');
        const submitBtn = document.getElementById('createSubmitBtn');
        const nameInput = document.getElementById('newFractalName');
        const fractalBtn = document.getElementById('createTypeFractal');
        const prismBtn = document.getElementById('createTypePrism');
        if (title) title.textContent = isFractal ? 'Create New Fractal' : 'Create New Prism';
        if (submitBtn) submitBtn.textContent = isFractal ? 'Create Fractal' : 'Create Prism';
        if (nameInput) nameInput.placeholder = isFractal ? 'Enter fractal name' : 'Enter prism name';
        if (fractalBtn) fractalBtn.classList.toggle('active', isFractal);
        if (prismBtn) prismBtn.classList.toggle('active', !isFractal);
    },

    hideCreateFractalModal() {
        const modal = document.getElementById('createFractalModal');
        if (modal) {
            modal.style.display = 'none';
        }
    },

    showEditFractalModal() {
        const modal = document.getElementById('editFractalModal');
        if (modal) {
            modal.style.display = 'flex';

            // Focus on name input
            const nameInput = document.getElementById('editFractalName');
            if (nameInput) {
                setTimeout(() => nameInput.focus(), 100);
            }
        }
    },

    hideEditFractalModal() {
        const modal = document.getElementById('editFractalModal');
        if (modal) {
            modal.style.display = 'none';
        }
        this.currentEditFractal = null;
    },

    // CRUD Operations
    async handleCreateFractal(event) {
        event.preventDefault();

        const form = event.target;
        const formData = new FormData(form);
        const name = formData.get('name').trim();
        const description = formData.get('description').trim();
        const isPrism = this.createType === 'prism';
        const label = isPrism ? 'Prism' : 'Fractal';

        if (!name) {
            Toast.show(`${label} name is required`, 'error');
            return;
        }

        if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(name)) {
            Toast.show('Name can only contain letters, numbers, hyphens, and underscores, and must start with a letter.', 'error');
            return;
        }

        try {
            const url = isPrism ? '/api/v1/prisms' : '/api/v1/fractals';
            const response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include',
                body: JSON.stringify({ name, description })
            });

            const data = await response.json();

            if (!response.ok || !data.success) {
                throw new Error(data.error || `HTTP ${response.status}`);
            }

            Toast.show(`${label} "${name}" created successfully`, 'success');
            this.hideCreateFractalModal();

            if (window.FractalSelector) window.FractalSelector.loadAvailableFractals();
            if (window.FractalListing) window.FractalListing.loadFractals();

        } catch (error) {
            console.error(`Failed to create ${label.toLowerCase()}:`, error);
            Toast.show(`Failed to create ${label.toLowerCase()}: ${error.message}`, 'error');
        }
    },


    async handleEditFractal(event) {
        event.preventDefault();

        if (!this.currentEditFractal) {
            Toast.show('No fractal selected for editing', 'error');
            return;
        }

        const form = event.target;
        const formData = new FormData(form);

        const fractalData = {
            name: formData.get('name').trim(),
            description: formData.get('description').trim()
        };

        if (!fractalData.name) {
            Toast.show('Fractal name is required', 'error');
            return;
        }

        if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(fractalData.name)) {
            Toast.show('Name can only contain letters, numbers, hyphens, and underscores, and must start with a letter.', 'error');
            return;
        }

        try {
            const response = await fetch(`/api/v1/fractals/${this.currentEditFractal.id}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include',
                body: JSON.stringify(fractalData)
            });

            const data = await response.json();

            if (!response.ok || !data.success) {
                throw new Error(data.error || `HTTP ${response.status}`);
            }

            Toast.show(`Fractal "${fractalData.name}" updated successfully`, 'success');
            this.hideEditFractalModal();

            // Update fractal selector if available
            if (window.FractalSelector) {
                window.FractalSelector.loadAvailableFractals();
            }

            // Update fractal listing if available
            if (window.FractalListing) {
                window.FractalListing.loadFractals();
            }

        } catch (error) {
            console.error('Failed to update fractal:', error);
            Toast.show(`Failed to update fractal: ${error.message}`, 'error');
        }
    },


















};

// Make globally available
window.FractalManagement = FractalManagement;