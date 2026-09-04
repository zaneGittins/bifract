/**
 * Chat module - LLM-powered log analysis assistant.
 * Provides a conversational interface to query and explore fractal log data.
 */

// What each tool did, past tense, for the trace. A name absent here is shown
// with its underscores stripped, which reads well enough for the rest.
const TOOL_LABELS = {
    query_logs: 'Ran query',
    validate_bql: 'Checked query',
    get_fields: 'Read fields',
    get_field_stats: 'Profiled fields',
    get_recent_logs: 'Read recent logs',
    get_bql_reference: 'Read BQL reference',
    list_alerts: 'Read alerts',
    get_alert: 'Read alert',
    get_alert_executions: 'Read alert history',
    get_attack_coverage: 'Checked ATT&CK coverage',
    find_processes: 'Found processes',
    get_provenance_graph: 'Built process tree',
    list_dictionaries: 'Read watchlists',
    get_dictionary: 'Read watchlist',
    search_dictionary: 'Checked watchlist',
    list_models: 'Read models',
    get_model: 'Read model',
    get_model_data: 'Read model data',
    list_comments: 'Read comments',
    get_log_comments: 'Read comments on this log',
    list_comment_tags: 'Read comment tags',
    list_notebooks: 'Read notebooks',
    get_notebook: 'Read notebook',
    list_saved_queries: 'Read saved queries',
    list_dashboards: 'Read dashboards',
    get_dashboard: 'Read dashboard',
    list_instruction_libraries: 'Read libraries',
    get_instruction_library: 'Read library',
    read_instruction_page: 'Read library page',
    search_archive: 'Searched the archive',
    get_archive_search: 'Read archive results',
    cancel_archive_search: 'Cancelled archive search',
    add_comment: 'Added a comment',
    add_tag: 'Tagged comments',
    remove_tag: 'Untagged comments',
    create_alert: 'Created an alert',
    update_alert: 'Updated an alert',
    create_notebook: 'Created a notebook',
    add_notebook_section: 'Added a notebook section',
};

// What approving a proposed action will actually do, in the user's terms. The
// arguments are shown verbatim beside it: this is only the headline.
const CONFIRM_TITLES = {
    add_comment: 'Add a comment to a log',
    add_tag: 'Add a tag to comments',
    remove_tag: 'Remove a tag from comments',
    create_alert: 'Create a detection alert',
    update_alert: 'Change a detection alert',
    create_notebook: 'Create a notebook',
    add_notebook_section: 'Add a section to a notebook',
    search_archive: 'Search the archive (slow, and it costs storage reads)',
};

// What became of a proposed action, in the card's own words. The keys are the
// statuses the server records for an offered call.
const CONFIRM_OUTCOMES = {
    approve: 'Approved',
    deny: 'Declined',
    superseded: 'Superseded',
    expired: 'Expired',
};

// How often the streaming answer repaints. Every token would reparse the whole
// reply; this is short enough to read as continuous.
const STREAM_PAINT_MS = 50;

const Chat = {
    currentConversationId: null,
    conversations: [],
    instructionLibraries: [],
    isStreaming: false,
    currentReader: null,
    loadingInterval: null,
    initialized: false,
    chatCharts: [],
    autoScroll: true,
    scrollPending: false,
    statusLabel: null,
    lastUserMessage: null,
    conversationFilter: '',

    init() {
        if (this.initialized) return;
        this.initialized = true;
        this.bindEvents();
    },

    bindEvents() {
        const newBtn = document.getElementById('newConversationBtn');
        if (newBtn) newBtn.addEventListener('click', () => this.createConversation());

        const newEmptyBtn = document.getElementById('newConversationEmptyBtn');
        if (newEmptyBtn) newEmptyBtn.addEventListener('click', () => this.createConversation());

        const sendBtn = document.getElementById('chatSendBtn');
        if (sendBtn) sendBtn.addEventListener('click', () => this.handleSend());

        const input = document.getElementById('chatInput');
        if (input) {
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    this.handleSend();
                }
            });
            // Auto-resize textarea
            input.addEventListener('input', () => {
                input.style.height = 'auto';
                input.style.height = Math.min(input.scrollHeight, 150) + 'px';
            });
        }

        const clearBtn = document.getElementById('clearChatBtn');
        if (clearBtn) clearBtn.addEventListener('click', () => this.clearMessages());

        const deleteBtn = document.getElementById('deleteChatBtn');
        if (deleteBtn) deleteBtn.addEventListener('click', () => this.deleteCurrentConversation());

        const deleteAllBtn = document.getElementById('deleteAllChatsBtn');
        if (deleteAllBtn) deleteAllBtn.addEventListener('click', () => this.deleteAllConversations());


        const instructionSelect = document.getElementById('chatInstructionSelect');
        if (instructionSelect) instructionSelect.addEventListener('change', () => this.onInstructionSelectChange());

        const convSearch = document.getElementById('chatConvSearch');
        if (convSearch) convSearch.addEventListener('input', () => {
            this.conversationFilter = convSearch.value.trim().toLowerCase();
            this.renderConversationList();
        });

        // Sidebar collapse/expand
        const collapseBtn = document.getElementById('chatCollapseBtn');
        if (collapseBtn) collapseBtn.addEventListener('click', () => this.toggleSidebar(true));
        const expandBtn = document.getElementById('chatExpandBtn');
        if (expandBtn) expandBtn.addEventListener('click', () => this.toggleSidebar(false));
        const railNewBtn = document.getElementById('chatRailNewBtn');
        if (railNewBtn) railNewBtn.addEventListener('click', () => this.createConversation());
        this.applySidebarState();

        // Auto-scroll detection: pause when user scrolls up
        const scrollEl = document.querySelector('.chat-thread-scroll');
        if (scrollEl) {
            scrollEl.addEventListener('scroll', () => {
                const atBottom = scrollEl.scrollHeight - scrollEl.scrollTop - scrollEl.clientHeight < 40;
                this.autoScroll = atBottom;
            }, { passive: true });
        }
    },

    SIDEBAR_KEY: 'bifract-chat-sidebar-collapsed',

    applySidebarState() {
        const layout = document.querySelector('.chat-layout');
        if (!layout) return;
        // Default to collapsed for a focused first impression; remember the choice after.
        const stored = localStorage.getItem(this.SIDEBAR_KEY);
        const collapsed = stored === null ? true : stored === '1';
        layout.classList.toggle('sidebar-collapsed', collapsed);
    },

    toggleSidebar(collapsed) {
        const layout = document.querySelector('.chat-layout');
        if (!layout) return;
        if (collapsed === undefined) collapsed = !layout.classList.contains('sidebar-collapsed');
        layout.classList.toggle('sidebar-collapsed', collapsed);
        try { localStorage.setItem(this.SIDEBAR_KEY, collapsed ? '1' : '0'); } catch (e) {}
    },

    show(subPath = '') {
        const fractal = window.FractalContext?.currentFractal;
        if (!fractal) return;
        if (subPath) this.currentConversationId = subPath;
        this.loadConversations();
        this.loadInstructions();
    },

    hide() {
        this.stopStreaming();
    },

    onFractalChange() {
        this.stopStreaming();
        this.destroyCharts();
        this.currentConversationId = null;
        this.conversations = [];
        this.instructionLibraries = [];
        this.showEmptyState();
        if (window.FractalContext?.hasScope()) {
            this.loadConversations();
            this.loadInstructions();
        }
    },

    // ---- Conversations ----

    async loadConversations() {
        try {
            const res = await HttpUtils.safeFetch('/api/v1/chat/conversations', {
                credentials: 'include',
            });
            this.conversations = HttpUtils.list(res);
            this.renderConversationList();

            // Reselect current conversation if still valid
            if (this.currentConversationId) {
                const still = this.conversations.find(c => c.id === this.currentConversationId);
                if (still) {
                    this.selectConversation(this.currentConversationId);
                } else {
                    this.currentConversationId = null;
                    this.showEmptyState();
                }
            }
        } catch (err) {
            console.error('[Chat] Failed to load conversations:', err);
        }
    },

    async createConversation() {
        try {
            const res = await HttpUtils.safeFetch('/api/v1/chat/conversations', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ title: 'New conversation' }),
                credentials: 'include',
            });
            const conv = res.data;
            this.conversations.unshift(conv);
            this.renderConversationList();
            this.selectConversation(conv.id);
        } catch (err) {
            console.error('[Chat] Failed to create conversation:', err);
            if (window.Toast) Toast.error('Chat', 'Failed to create conversation');
        }
    },

    async deleteCurrentConversation() {
        if (!this.currentConversationId) return;
        const id = this.currentConversationId;
        try {
            await HttpUtils.safeFetch(`/api/v1/chat/conversations/${id}`, {
                method: 'DELETE',
                credentials: 'include',
            });
            this.conversations = this.conversations.filter(c => c.id !== id);
            this.currentConversationId = null;
            this.renderConversationList();
            this.showEmptyState();
        } catch (err) {
            console.error('[Chat] Failed to delete conversation:', err);
            if (window.Toast) Toast.error('Chat', 'Failed to delete conversation');
        }
    },

    async deleteAllConversations() {
        if (this.conversations.length === 0) return;
        if (!confirm('Delete all conversations in this fractal?')) return;
        this.stopStreaming();
        try {
            await HttpUtils.safeFetch('/api/v1/chat/conversations', {
                method: 'DELETE',
                credentials: 'include',
            });
            this.conversations = [];
            this.currentConversationId = null;
            this.renderConversationList();
            this.showEmptyState();
        } catch (err) {
            console.error('[Chat] Failed to delete all conversations:', err);
            if (window.Toast) Toast.error('Chat', 'Failed to delete conversations');
        }
    },

    async clearMessages() {
        if (!this.currentConversationId) return;
        this.stopStreaming();
        this.destroyCharts();
        try {
            await HttpUtils.safeFetch(`/api/v1/chat/conversations/${this.currentConversationId}/messages`, {
                method: 'DELETE',
                credentials: 'include',
            });
            const msgs = document.getElementById('chatMessages');
            if (msgs) msgs.innerHTML = '';
        } catch (err) {
            console.error('[Chat] Failed to clear messages:', err);
        }
    },

    selectConversation(id) {
        window.App?.pushSubPath(id);
        this.stopStreaming();
        this.destroyCharts();
        this.currentConversationId = id;
        this.renderConversationList();
        this.showActiveArea();
        this.loadMessages(id);

        // Update title
        const conv = this.conversations.find(c => c.id === id);
        const titleEl = document.getElementById('chatConversationTitle');
        if (titleEl && conv) titleEl.textContent = conv.title;

        // Sync library selector with conversation
        this.syncLibrarySelect();
    },

    renderConversationList() {
        const list = document.getElementById('conversationList');
        if (!list) return;

        const filtered = this.conversationFilter
            ? this.conversations.filter(c => c.title.toLowerCase().includes(this.conversationFilter))
            : this.conversations;

        if (filtered.length === 0) {
            list.innerHTML = `<div class="conv-empty">${this.conversationFilter ? 'No matches' : 'No conversations yet'}</div>`;
            return;
        }

        list.innerHTML = filtered.map(conv => {
            const active = conv.id === this.currentConversationId ? ' active' : '';
            const date = this.formatRelativeTime(conv.updated_at);
            return `
                <div class="conv-item${active}" data-id="${Utils.escapeHtml(conv.id)}">
                    <div class="conv-item-title" title="${Utils.escapeHtml(conv.title)}">${Utils.escapeHtml(conv.title)}</div>
                    <div class="conv-item-date">${date}</div>
                </div>
            `;
        }).join('');

        list.querySelectorAll('.conv-item').forEach(el => {
            el.addEventListener('click', () => this.selectConversation(el.dataset.id));
        });
    },

    // ---- Messages ----

    async loadMessages(conversationId) {
        const msgs = document.getElementById('chatMessages');
        if (!msgs) return;
        msgs.innerHTML = '';

        try {
            const [res, offers] = await Promise.all([
                HttpUtils.safeFetch(`/api/v1/chat/conversations/${conversationId}/messages`, { credentials: 'include' }),
                // A conversation that never proposed a write has nothing here,
                // and a failure to read it must not blank the thread.
                HttpUtils.safeFetch(`/api/v1/chat/conversations/${conversationId}/tool-calls`, { credentials: 'include' })
                    .catch(() => null),
            ]);
            this.renderHistory(HttpUtils.list(res), HttpUtils.list(offers));
            this.scrollToBottom();
        } catch (err) {
            console.error('[Chat] Failed to load messages:', err);
        }
    },

    handleSend() {
        const input = document.getElementById('chatInput');
        if (!input) return;
        const text = input.value.trim();
        if (!text || this.isStreaming) return;

        // Handle slash commands
        if (text.startsWith('/')) {
            this.handleCommand(text);
            input.value = '';
            input.style.height = 'auto';
            return;
        }

        if (!this.currentConversationId) {
            this.createConversation().then(() => {
                input.value = text;
                this.handleSend();
            });
            return;
        }

        input.value = '';
        input.style.height = 'auto';
        this.streamMessage(text);
    },

    handleCommand(cmd) {
        if (cmd === '/clear') {
            this.clearMessages();
        }
    },

    async streamMessage(userText) {
        if (!this.currentConversationId || this.isStreaming) return;
        this.lastUserMessage = userText;
        this.appendUserMessage(userText);
        await this._streamToAssistant(userText);
    },

    async retryLastMessage() {
        if (!this.lastUserMessage || !this.currentConversationId || this.isStreaming) return;
        // Remove the last error bubble and its separator
        const msgs = document.getElementById('chatMessages');
        if (msgs && msgs.lastElementChild) {
            msgs.lastElementChild.remove(); // assistant bubble
            if (msgs.lastElementChild?.classList?.contains('chat-separator')) {
                msgs.lastElementChild.remove(); // separator
            }
        }
        await this._streamToAssistant(this.lastUserMessage);
    },

    async _streamToAssistant(content, displayText) {
        // The server closes anything still waiting when a new message arrives,
        // so the cards for those actions stop being answerable here too.
        document.querySelectorAll('.chat-confirm:not(.is-answered)')
            .forEach(card => this._markConfirmAnswered(card, 'superseded'));

        const assistantBubble = this.createAssistantBubble();
        const msgs = document.getElementById('chatMessages');
        if (msgs) {
            msgs.appendChild(this.createSeparator());
            msgs.appendChild(assistantBubble);
        }

        const contentEl = assistantBubble.querySelector('.chat-msg-content');
        this.startLoadingAnimation(contentEl);
        await this._runStream(
            `/api/v1/chat/conversations/${this.currentConversationId}/stream`,
            { content, time_range: this._timeRange() },
            contentEl,
            false,
        );
    },

    _timeRange() {
        return document.getElementById('chatTimeRange')?.value || '24h';
    },

    // Reads one SSE reply into contentEl. The reply to a confirmed action
    // continues the same bubble, so hasContent says whether anything is in it.
    async _runStream(url, body, contentEl, hasContent) {
        this.autoScroll = true;
        this.scrollToBottom();
        this.isStreaming = true;
        this.updateInputState(true);
        let hadError = false;

        try {
            const response = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
                credentials: 'include',
            });

            if (!response.ok) throw new Error(`Server error: ${response.status}`);

            const reader = response.body.getReader();
            this.currentReader = reader;
            const decoder = new TextDecoder();
            let buffer = '';

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split('\n');
                buffer = lines.pop();

                for (const line of lines) {
                    if (!line.startsWith('data: ')) continue;
                    const data = line.slice(6).trim();
                    if (!data) continue;
                    let event;
                    try { event = JSON.parse(data); } catch { continue; }
                    hasContent = this._handleSSEEvent(contentEl, event, hasContent);
                    if (event.type === 'error') hadError = true;
                }
            }
        } catch (err) {
            if (err.name !== 'AbortError') {
                console.error('[Chat] Stream error:', err);
                this.hideStatusIndicator();
                hadError = true;
                this.clearBubbleLoading(contentEl);
                this._finalizeStreamingText(contentEl);
                this.appendError(contentEl, `Connection error: ${err.message}`);
            }
        } finally {
            this.isStreaming = false;
            this.currentReader = null;
            this.hideStatusIndicator();
            this.updateInputState(false);
            this._finalizeStreamingText(contentEl);
            this.clearBubbleLoading(contentEl);
            if (contentEl && !contentEl.childNodes.length) {
                contentEl.innerHTML = '<span class="chat-tool-empty">The assistant returned nothing.</span>';
            }
            this.scrollToBottom();
            if (hadError) this._appendRetryButton(contentEl);
        }
    },

    // Shared SSE event handler used by all stream methods
    _handleSSEEvent(contentEl, event, hasContent) {
        switch (event.type) {
            case 'token':
                if (!hasContent) { this.clearBubbleLoading(contentEl); contentEl.innerHTML = ''; hasContent = true; }
                this.setStatus('Writing');
                this.appendToken(contentEl, event.content);
                break;
            case 'tool_call':
                // present_results and render_chart draw below the trace, from
                // their own events, once the steps that fed them are in.
                if (event.tool_name === 'present_results' || event.tool_name === 'render_chart') break;
                if (!hasContent) { this.clearBubbleLoading(contentEl); contentEl.innerHTML = ''; hasContent = true; }
                if (event.tool_name === 'think') {
                    this.setStatus('Thinking');
                    this.renderThinkBlock(contentEl, event.tool_args, event.tool_call_id);
                    break;
                }
                this.setStatus(this._toolStatus(event.tool_name));
                this.renderToolCall(contentEl, event.tool_name, event.tool_args, event.tool_call_id);
                break;
            case 'tool_result':
                if (event.tool_name === 'render_chart' || event.tool_name === 'think') break;
                this.renderToolResult(contentEl, event.tool_name, event.tool_result, event.tool_call_id);
                break;
            case 'tool_confirm':
                if (!hasContent) { this.clearBubbleLoading(contentEl); contentEl.innerHTML = ''; hasContent = true; }
                this.clearBubbleLoading(contentEl);
                this._finalizeStreamingText(contentEl);
                this.renderToolConfirm(contentEl, event.tool_name, event.tool_args, event.pending_id);
                break;
            case 'think':
                this.setStatus('Thinking');
                break;
            case 'chart':
                if (!hasContent) { this.clearBubbleLoading(contentEl); contentEl.innerHTML = ''; hasContent = true; }
                this.discardStreamingText(contentEl);
                this.renderChart(contentEl, event.tool_args);
                break;
            case 'present':
                this.discardStreamingText(contentEl);
                this.clearBubbleLoading(contentEl);
                this.renderPresentation(contentEl, event.tool_args);
                hasContent = true;
                break;
            case 'error':
                this.hideStatusIndicator();
                this.clearBubbleLoading(contentEl);
                this._finalizeStreamingText(contentEl);
                this.appendError(contentEl, event.content || 'Unknown error');
                hasContent = true;
                break;
            case 'title':
                if (event.content) {
                    const conv = this.conversations.find(c => c.id === this.currentConversationId);
                    if (conv) conv.title = event.content;
                    this.renderConversationList();
                    const titleEl = document.getElementById('chatConversationTitle');
                    if (titleEl) titleEl.textContent = event.content;
                }
                break;
            case 'done':
                this.hideStatusIndicator();
                break;
        }
        if (this.autoScroll) this.scrollToBottom();
        return hasContent;
    },

    // The steps and results already on screen are in the stored conversation
    // either way, so replacing the bubble with the error made the live view
    // disagree with the one you come back to.
    appendError(contentEl, message) {
        if (!contentEl) return;
        const el = document.createElement('div');
        el.className = 'chat-error';
        el.textContent = message;
        contentEl.appendChild(el);
    },

    _appendRetryButton(contentEl) {
        if (!contentEl) return;
        const btn = document.createElement('button');
        btn.className = 'chat-retry-btn';
        btn.textContent = 'Retry';
        btn.addEventListener('click', () => this.retryLastMessage());
        contentEl.appendChild(btn);
    },

    // Settle the streamed answer: cancel the pending repaint, render what
    // arrived, and stop treating the element as the live buffer. The element
    // stays where it is, so nothing moves as the stream ends.
    _finalizeStreamingText(contentEl) {
        const el = contentEl?.querySelector('.chat-streaming-text');
        if (!el) return;
        clearTimeout(el.paintTimer);
        el.paintTimer = null;
        const raw = (el.raw || '').replace(/\s+$/, '');
        if (!raw) {
            el.remove();
            return;
        }
        el.raw = raw;
        el.innerHTML = this._renderMarkdown(raw);
        el.classList.remove('chat-streaming-text');
    },

    // A chart or a presentation supersedes the prose the model streamed ahead
    // of it. Replaying stored messages drops the same text at the same point,
    // so a reloaded turn reads exactly as it did live.
    discardStreamingText(contentEl) {
        const el = contentEl?.querySelector('.chat-streaming-text');
        if (el) {
            clearTimeout(el.paintTimer);
            el.remove();
        }
    },

    stopStreaming() {
        if (this.currentReader) {
            this.currentReader.cancel().catch(() => {});
            this.currentReader = null;
        }
        this.isStreaming = false;
        this.stopLoadingAnimation();
        this.updateInputState(false);
    },

    destroyCharts() {
        for (const c of this.chatCharts) {
            try { c.destroy(); } catch {}
        }
        this.chatCharts = [];
    },

    // ---- Rendering ----

    // Stored rows are replayed through the same primitives the live stream uses,
    // so a conversation looks the same coming back to it as it did while it ran.
    // The unit is the turn, not the row: the stream writes a turn's think steps,
    // tool calls, results and answer into one bubble, while the turn is stored as
    // an assistant row per tool round.
    renderHistory(messages, offers = []) {
        if (!document.getElementById('chatMessages')) return;
        const offered = new Map((offers || []).map((o) => [o.tool_call_id, o]));
        let turn = null;
        for (const msg of messages) {
            if (msg.role === 'user') {
                this.flushTurnText(turn);
                turn = null;
                this.appendUserMessage(msg.content, true);
                continue;
            }
            if (msg.role === 'assistant') {
                if (!turn) turn = this.openAssistantTurn();
                this.replayAssistantMessage(turn, msg, offered);
            } else if (msg.role === 'tool' && turn) {
                this.replayToolResult(turn, msg);
            }
        }
        this.flushTurnText(turn);
    },

    // A turn buffers its answer text because the stream does: tokens accumulate
    // in one span that a chart or a presentation replaces, so the text is only
    // known to have survived once the turn is over.
    openAssistantTurn() {
        const msgs = document.getElementById('chatMessages');
        if (!msgs) return null;
        if (msgs.children.length > 0) msgs.appendChild(this.createSeparator());
        const bubble = this.createAssistantBubble();
        msgs.appendChild(bubble);
        const contentEl = bubble.querySelector('.chat-msg-content');
        this.clearBubbleLoading(contentEl);
        return { contentEl, text: '' };
    },

    // Mirrors _finalizeStreamingText: trailing blank goes, and nothing is
    // written for an answer that turned out to be only whitespace.
    flushTurnText(turn) {
        if (!turn) return;
        const raw = turn.text.replace(/\s+$/, '');
        turn.text = '';
        if (!raw) return;
        const rendered = document.createElement('div');
        rendered.className = 'chat-msg-text chat-markdown';
        rendered.innerHTML = this._renderMarkdown(raw);
        turn.contentEl.appendChild(rendered);
    },

    replayAssistantMessage(turn, msg, offered) {
        // Text precedes the round's tool calls in the stream, and a chart or a
        // presentation supersedes it.
        if (msg.content) turn.text += msg.content;

        for (const tc of msg.tool_calls || []) {
            const name = tc.function?.name;
            if (!name) continue;
            let args = {};
            try { args = JSON.parse(tc.function.arguments || '{}'); } catch {}
            // A step goes in above the answer text, and the live path trims the
            // buffer as it does so.
            turn.text = turn.text.replace(/\s+$/, '');
            switch (name) {
                case 'think':
                    this.renderThinkBlock(turn.contentEl, args, tc.id);
                    break;
                case 'render_chart':
                    turn.text = '';
                    this.renderChart(turn.contentEl, args);
                    break;
                case 'present_results':
                    turn.text = '';
                    this.renderPresentation(turn.contentEl, args);
                    break;
                default:
                    this.renderToolCall(turn.contentEl, name, args, tc.id);
            }
        }

        // The stream offers a card once the round's steps and text are in, so
        // the replay flushes the text before putting one back.
        for (const tc of msg.tool_calls || []) {
            const offer = offered?.get(tc.id);
            if (!offer) continue;
            this.flushTurnText(turn);
            const card = this.renderToolConfirm(turn.contentEl, offer.tool_name, offer.arguments, offer.id);
            if (offer.status !== 'open') this._markConfirmAnswered(card, offer.status);
        }
    },

    replayToolResult(turn, msg) {
        const stored = msg.tool_results?.[0];
        if (!stored) return;
        // Display-only calls carry no result worth showing, as in the live stream.
        if (stored.tool_name === 'think' || stored.tool_name === 'render_chart') return;
        this.renderToolResult(turn.contentEl, stored.tool_name, stored.result, stored.tool_call_id);
    },

    createSeparator() {
        const sep = document.createElement('div');
        sep.className = 'chat-separator';
        sep.innerHTML = '<div class="chat-sep-line"></div><div class="chat-sep-dot"></div><div class="chat-sep-line"></div>';
        return sep;
    },

    appendUserMessage(text, skipScroll = false) {
        const msgs = document.getElementById('chatMessages');
        if (!msgs) return;
        // Add separator if there are existing messages
        if (msgs.children.length > 0) {
            msgs.appendChild(this.createSeparator());
        }
        const div = document.createElement('div');
        div.className = 'chat-message chat-message-user';
        div.innerHTML = `<div class="chat-msg-content">${Utils.escapeHtml(text)}</div>`;
        msgs.appendChild(div);
        if (!skipScroll) this.scrollToBottom();
    },

    createAssistantBubble() {
        const div = document.createElement('div');
        div.className = 'chat-message chat-message-assistant';
        div.innerHTML = `<div class="chat-msg-content"><span class="chat-loading-text"></span></div>`;
        return div;
    },

    // Tokens accumulate on the element and it repaints on a timer. Appending to
    // textContent rebuilt the whole answer per token, and formatting only once
    // the stream ended made the finished reply jump as it reflowed.
    appendToken(contentEl, token) {
        let el = contentEl.querySelector('.chat-streaming-text');
        if (!el) {
            el = document.createElement('div');
            el.className = 'chat-streaming-text chat-msg-text chat-markdown';
            el.raw = '';
            contentEl.appendChild(el);
        }
        el.raw += token;
        if (el.paintTimer) return;
        el.paintTimer = setTimeout(() => {
            el.paintTimer = null;
            el.innerHTML = this._renderMarkdown(el.raw);
            if (this.autoScroll) this.scrollToBottom();
        }, STREAM_PAINT_MS);
    },

    _renderMarkdown(text) {
        if (!text) return '';
        if (window.marked) {
            try {
                marked.setOptions({ breaks: true, gfm: true, headerIds: false, mangle: false });
                const html = marked.parse(text);
                return DOMPurify ? DOMPurify.sanitize(html) : html;
            } catch {}
        }
        // Fallback: escape and basic formatting
        return Utils.escapeHtml(text)
            .replace(/`([^`]+)`/g, '<code class="chat-inline-code">$1</code>')
            .replace(/\n/g, '<br>');
    },

    // A trace step goes in above the answer text, so drop the blank the tokens
    // left behind. Only the pending buffer is touched: rewriting the rendered
    // element's textContent would flatten its markup.
    trimTrailingWhitespace(contentEl) {
        const el = contentEl?.querySelector('.chat-streaming-text');
        if (el && typeof el.raw === 'string') el.raw = el.raw.replace(/\s+$/, '');
    },

    // Find or create the unified investigation trace for an assistant turn.
    // Steps are inserted above the streamed answer text.
    _traceEl(contentEl) {
        let trace = contentEl.querySelector(':scope > .chat-trace');
        if (!trace) {
            trace = document.createElement('div');
            trace.className = 'chat-trace';
            const firstText = contentEl.querySelector('.chat-streaming-text, .chat-msg-text');
            if (firstText) contentEl.insertBefore(trace, firstText);
            else contentEl.appendChild(trace);
        }
        return trace;
    },

    _traceStep(label, type, callId) {
        const step = document.createElement('div');
        step.className = 'chat-trace-step collapsed';
        if (type) step.dataset.type = type;
        if (callId) step.dataset.callId = callId;
        step.innerHTML = `
            <div class="chat-trace-step-header">
                <span class="chat-trace-node"></span>
                <span class="chat-trace-chevron">&#9656;</span>
                <span class="chat-trace-label"></span>
                <span class="chat-trace-summary"></span>
            </div>
            <div class="chat-trace-step-body"></div>
        `;
        step.querySelector('.chat-trace-label').textContent = label;
        step.querySelector('.chat-trace-step-header').addEventListener('click', () => step.classList.toggle('collapsed'));
        return step;
    },

    renderThinkBlock(contentEl, args, callId) {
        this.trimTrailingWhitespace(contentEl);
        const trace = this._traceEl(contentEl);
        const reasoning = args?.reasoning || '';
        const step = this._traceStep('Thinking', 'think', callId);
        step.querySelector('.chat-trace-summary').textContent = reasoning.length > 90 ? reasoning.slice(0, 90) + '…' : reasoning;
        const think = document.createElement('div');
        think.className = 'chat-trace-think';
        think.textContent = reasoning;
        step.querySelector('.chat-trace-step-body').appendChild(think);
        trace.appendChild(step);
    },

    renderToolCall(contentEl, toolName, args, callId) {
        this.trimTrailingWhitespace(contentEl);
        const trace = this._traceEl(contentEl);

        const label = TOOL_LABELS[toolName] || toolName.replace(/_/g, ' ');
        const query = args?.query || '';
        // A tool with no query of its own is summarized by what it was pointed at.
        const summary = query || args?.search || args?.name || args?.image || args?.host
            || args?.value || args?.page_name || args?.process_guid || '';

        const step = this._traceStep(label, toolName, callId);
        step.querySelector('.chat-trace-summary').textContent = summary;
        const header = step.querySelector('.chat-trace-step-header');
        const body = step.querySelector('.chat-trace-step-body');

        if (query) {
            const meta = document.createElement('span');
            meta.className = 'chat-trace-meta';
            meta.textContent = document.getElementById('chatTimeRange')?.value || '24h';
            header.appendChild(meta);

            const open = document.createElement('span');
            open.className = 'chat-trace-open';
            open.title = 'Open in search';
            open.innerHTML = '<svg width="11" height="11" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="6.5" cy="6.5" r="5.5" stroke="currentColor" stroke-width="2"/><line x1="10.5" y1="10.5" x2="15" y2="15" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>';
            open.addEventListener('click', (e) => { e.stopPropagation(); this.openInSearch(query); });
            header.appendChild(open);

            const pre = document.createElement('pre');
            pre.className = 'chat-trace-query';
            pre.textContent = query;
            body.appendChild(pre);
        }
        trace.appendChild(step);
    },

    // A write the assistant wants to make. Nothing runs until the user answers,
    // and only the id travels back: the arguments stay on the server, so what is
    // shown here is what will run.
    renderToolConfirm(contentEl, toolName, args, pendingId) {
        this.trimTrailingWhitespace(contentEl);

        const card = document.createElement('div');
        card.className = 'chat-confirm';

        const head = document.createElement('div');
        head.className = 'chat-confirm-head';
        head.textContent = CONFIRM_TITLES[toolName] || ('Run ' + toolName.replace(/_/g, ' '));
        card.appendChild(head);

        const note = document.createElement('div');
        note.className = 'chat-confirm-note';
        note.textContent = 'This changes what other people see. Nothing happens until you approve it.';
        card.appendChild(note);

        const detail = document.createElement('pre');
        detail.className = 'chat-confirm-args';
        detail.textContent = JSON.stringify(args ?? {}, null, 2);
        card.appendChild(detail);

        const actions = document.createElement('div');
        actions.className = 'chat-confirm-actions';

        const approve = document.createElement('button');
        approve.className = 'chat-confirm-btn chat-confirm-approve';
        approve.textContent = 'Approve';

        const decline = document.createElement('button');
        decline.className = 'chat-confirm-btn';
        decline.textContent = 'Decline';

        const answer = (decision) => {
            if (this.isStreaming) return;
            this._markConfirmAnswered(card, decision);
            this._resolveToolCall(pendingId, decision, contentEl);
        };
        approve.addEventListener('click', () => answer('approve'));
        decline.addEventListener('click', () => answer('deny'));

        actions.appendChild(decline);
        actions.appendChild(approve);
        card.appendChild(actions);
        contentEl.appendChild(card);
        this.scrollToBottom();
        return card;
    },

    // The answered state of a card, applied whether the user just clicked, the
    // turn was superseded, or a stored decision is being replayed. One place
    // decides what it looks like so the three cannot drift apart.
    _markConfirmAnswered(card, status) {
        if (!card || card.classList.contains('is-answered')) return;
        card.classList.add('is-answered', status === 'approve' ? 'is-approved' : 'is-declined');
        card.querySelector('.chat-confirm-actions')?.remove();
        const outcome = document.createElement('div');
        outcome.className = 'chat-confirm-outcome';
        outcome.textContent = CONFIRM_OUTCOMES[status] || 'Answered';
        card.appendChild(outcome);
    },

    // Answers a proposed action and reads the rest of the reply into the same
    // bubble, so the turn continues where it paused.
    async _resolveToolCall(pendingId, decision, contentEl) {
        await this._runStream(
            `/api/v1/chat/conversations/${this.currentConversationId}/tool-calls/${encodeURIComponent(pendingId)}/stream`,
            { decision, time_range: this._timeRange() },
            contentEl,
            true,
        );
    },

    // The result goes under the step for its own call. A round that batches a
    // think with a real call announces them in one order and dispatches them in
    // another, so "the step that went in last" was regularly the wrong one.
    renderToolResult(contentEl, toolName, result, callId) {
        const trace = contentEl.querySelector(':scope > .chat-trace');
        const steps = trace ? trace.querySelectorAll('.chat-trace-step') : [];
        const own = callId && trace ? trace.querySelector(`.chat-trace-step[data-call-id="${CSS.escape(callId)}"]`) : null;
        const step = own || (steps.length ? steps[steps.length - 1] : null);
        const targetCall = step ? step.querySelector('.chat-trace-step-body') : contentEl;

        const resultDiv = document.createElement('div');
        resultDiv.className = 'chat-trace-result';
        resultDiv.innerHTML = this._toolResultHtml(toolName, result);
        targetCall.appendChild(resultDiv);
    },

    // Tools answer in the API's own shapes, so this reads the shape rather than
    // the tool name. Only the two with a presentation worth special-casing get
    // one; everything else falls through to a table or a short summary.
    _toolResultHtml(toolName, result) {
        if (result?.error) {
            return `<span class="chat-error">Error: ${Utils.escapeHtml(result.error)}</span>`;
        }
        if (result?.declined) {
            return '<span class="chat-tool-empty">Declined</span>';
        }
        if (toolName === 'get_fields') {
            const fields = result?.fields || [];
            if (!fields.length) return '<span class="chat-tool-empty">No fields found</span>';
            const names = fields.map(f => typeof f === 'string'
                ? Utils.escapeHtml(f)
                : `${Utils.escapeHtml(f.name)} <span style="opacity:0.5">(${f.count})</span>`);
            return `<span class="chat-tool-fields">${names.join(', ')}</span>`;
        }
        if (result && typeof result === 'object' && 'results' in result) {
            const rows = result.results || [];
            if (!rows.length) return '<span class="chat-tool-empty">No results</span>';
            const order = result.field_order?.length ? result.field_order : this._columnsOf(rows);
            return this.renderMiniTable(rows, order, rows.length, false);
        }

        // A page the server truncated says so; keep the note with the rows.
        const rows = Array.isArray(result) ? result : (Array.isArray(result?.items) ? result.items : null);
        if (rows) {
            if (!rows.length) return '<span class="chat-tool-empty">Nothing found</span>';
            const table = this.renderMiniTable(rows, this._columnsOf(rows), rows.length, !!result?.more);
            const showing = result?.showing ? `<div class="chat-tool-note">Showing ${Utils.escapeHtml(String(result.showing))}</div>` : '';
            return table + showing;
        }
        if (result && typeof result === 'object') {
            const pairs = Object.entries(result)
                .filter(([, v]) => v !== null && v !== undefined && typeof v !== 'object')
                .slice(0, 8)
                .map(([k, v]) => `${Utils.escapeHtml(k)}: ${Utils.escapeHtml(String(v))}`);
            if (pairs.length) return `<span class="chat-tool-fields">${pairs.join(' · ')}</span>`;
            return '<span class="chat-tool-empty">Done</span>';
        }
        return `<span class="chat-tool-fields">${Utils.escapeHtml(String(result ?? ''))}</span>`;
    },

    // Columns for a table built from rows whose shape nothing declared.
    _columnsOf(rows) {
        const seen = [];
        for (const row of rows.slice(0, 5)) {
            if (!row || typeof row !== 'object') continue;
            for (const key of Object.keys(row)) {
                if (!seen.includes(key)) seen.push(key);
            }
        }
        return seen.slice(0, 6);
    },

    renderMiniTable(rows, fieldOrder, totalCount, truncated) {
        const displayRows = rows.slice(0, 5);
        const cols = fieldOrder.length > 0 ? fieldOrder : Object.keys(displayRows[0] || {});
        const displayCols = cols.slice(0, 6); // Max 6 columns

        const header = displayCols.map(c => `<th>${Utils.escapeHtml(c)}</th>`).join('');
        const body = displayRows.map(row => {
            const cells = displayCols.map(col => {
                const val = row[col];
                const str = val === null || val === undefined ? '' : String(val);
                const truncVal = str.length > 80 ? str.slice(0, 80) + '...' : str;
                return `<td title="${Utils.escapeHtml(str)}">${Utils.escapeHtml(truncVal)}</td>`;
            }).join('');
            return `<tr>${cells}</tr>`;
        }).join('');

        const note = truncated || rows.length > 5
            ? `<div class="chat-table-note">Showing ${displayRows.length} of ${totalCount} rows</div>`
            : `<div class="chat-table-note">${totalCount} row${totalCount !== 1 ? 's' : ''}</div>`;

        return `
            <div class="chat-mini-table-wrap">
                <table class="chat-mini-table">
                    <thead><tr>${header}</tr></thead>
                    <tbody>${body}</tbody>
                </table>
            </div>
            ${note}
        `;
    },

    renderPresentation(contentEl, args) {
        if (!args) return;
        const severity = args.severity || 'info';
        const summary = args.summary || '';
        const findings = args.findings || [];

        const div = document.createElement('div');
        div.className = 'chat-presentation';

        let html = '';

        // Severity bubble
        if (severity && severity !== 'info') {
            html += `<div class="chat-severity-bubble chat-severity-${severity}">${Utils.escapeHtml(severity)}</div>`;
        }

        // Summary text
        html += `<div class="chat-present-summary">${Utils.escapeHtml(summary)}</div>`;

        // Findings table
        if (findings.length > 0) {
            html += '<div class="chat-findings-table">';
            findings.forEach(f => {
                html += `<div class="chat-finding-row">
                    <span class="chat-finding-label">${Utils.escapeHtml(f.label)}</span>
                    <span class="chat-finding-value">${Utils.escapeHtml(f.value)}</span>
                </div>`;
            });
            html += '</div>';
        }

        div.innerHTML = html;
        contentEl.appendChild(div);

        // Inline chart (if provided)
        if (args.chart && args.chart.labels && args.chart.datasets) {
            this.renderChart(contentEl, args.chart);
        }
    },

    renderChart(contentEl, args) {
        if (!args || !args.labels || !args.datasets) return;

        const container = document.createElement('div');
        container.className = 'chat-chart-container';

        if (args.title) {
            const title = document.createElement('div');
            title.className = 'chat-chart-title';
            title.textContent = args.title;
            container.appendChild(title);
        }

        const wrapper = document.createElement('div');
        wrapper.className = 'chat-chart-wrapper';
        container.appendChild(wrapper);

        const canvas = document.createElement('canvas');
        wrapper.appendChild(canvas);
        contentEl.appendChild(container);

        const chart = BifractCharts.renderFromPreprocessed(canvas, args);
        if (chart) this.chatCharts.push(chart);
    },

    // ---- Loading / status indicator ----

    startLoadingAnimation(contentEl) {
        if (!contentEl) return;
        const textEl = contentEl.querySelector('.chat-loading-text');
        if (textEl) textEl.textContent = '';
        this.setStatus('Thinking');
    },

    // Remove the in-bubble loading marker but keep the status indicator running
    clearBubbleLoading(contentEl) {
        const el = contentEl?.querySelector('.chat-loading-text');
        if (el) el.remove();
    },

    // Map a tool name to an honest, present-tense status label.
    _toolStatus(name) {
        switch (name) {
            case 'get_fields': return 'Reading fields';
            case 'get_field_stats': return 'Profiling fields';
            case 'get_bql_reference': return 'Reading BQL reference';
            case 'get_recent_logs': return 'Reading recent logs';
            case 'validate_bql': return 'Checking query';
            case 'query_logs': return 'Running query';
            case 'list_alerts': case 'get_alert': return 'Reading alerts';
            case 'get_alert_executions': return 'Reading alert history';
            case 'get_attack_coverage': return 'Checking ATT&CK coverage';
            case 'find_processes': return 'Finding processes';
            case 'get_provenance_graph': return 'Building the process tree';
            case 'list_dictionaries': case 'get_dictionary': case 'search_dictionary': return 'Checking watchlists';
            case 'list_models': case 'get_model': case 'get_model_data': return 'Reading models';
            case 'list_comments': case 'get_log_comments': case 'list_comment_tags': return 'Reading comments';
            case 'list_notebooks': case 'get_notebook': return 'Reading notebooks';
            case 'list_saved_queries': return 'Reading saved queries';
            case 'list_dashboards': case 'get_dashboard': return 'Reading dashboards';
            case 'list_instruction_libraries': case 'get_instruction_library': case 'read_instruction_page': return 'Reading library';
            case 'search_archive': case 'get_archive_search': return 'Searching the archive';
            case 'render_chart': return 'Building chart';
            case 'present_results': return 'Summarizing';
            case 'think': return 'Thinking';
            default: return 'Working';
        }
    },

    // Show the status line with the actual current step (no random phrases).
    setStatus(text) {
        if (this.statusLabel === text) return;
        this.statusLabel = text;
        const indicator = document.getElementById('chatStatusIndicator');
        const statusText = document.getElementById('chatStatusText');
        if (indicator) indicator.style.display = 'flex';
        if (statusText) statusText.textContent = text + '…';
    },

    // Back-compat alias.
    showStatusIndicator() { this.setStatus('Working'); },

    hideStatusIndicator() {
        this.statusLabel = null;
        const indicator = document.getElementById('chatStatusIndicator');
        if (indicator) indicator.style.display = 'none';
    },

    // Legacy alias used in stopStreaming
    stopLoadingAnimation() {
        this.hideStatusIndicator();
    },

    // ---- UI state helpers ----

    showEmptyState() {
        const empty = document.getElementById('chatEmptyState');
        const active = document.getElementById('chatActiveArea');
        if (empty) empty.style.display = 'flex';
        if (active) active.style.display = 'none';
    },

    showActiveArea() {
        const empty = document.getElementById('chatEmptyState');
        const active = document.getElementById('chatActiveArea');
        if (empty) empty.style.display = 'none';
        if (active) active.style.display = 'flex';
    },

    updateInputState(streaming) {
        const input = document.getElementById('chatInput');
        const sendBtn = document.getElementById('chatSendBtn');
        const stopBtn = document.getElementById('chatStopBtn');
        if (input) input.disabled = streaming;
        if (sendBtn) sendBtn.style.display = streaming ? 'none' : '';
        if (stopBtn) stopBtn.style.display = streaming ? '' : 'none';
        // One answer at a time: each resumes the turn on its own stream, and two
        // at once would interleave into the same bubble.
        document.querySelectorAll('.chat-confirm:not(.is-answered) .chat-confirm-btn')
            .forEach(btn => { btn.disabled = streaming; });
    },

    // Coalesced to one write per frame: reading scrollHeight forces layout, and
    // a fast reply asked for it on every token.
    scrollToBottom() {
        if (this.scrollPending) return;
        this.scrollPending = true;
        requestAnimationFrame(() => {
            this.scrollPending = false;
            const scrollEl = document.querySelector('.chat-thread-scroll');
            if (scrollEl) scrollEl.scrollTop = scrollEl.scrollHeight;
        });
    },

    async analyzeLog(logData) {
        if (!logData) return;

        // Navigate to chat view
        if (window.App) App.showFractalViewTab('chat');

        // Create a new conversation
        try {
            const res = await HttpUtils.safeFetch('/api/v1/chat/conversations', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ title: 'Log Analysis' }),
                credentials: 'include',
            });
            const conv = res.data;
            this.conversations.unshift(conv);
            this.renderConversationList();
            this.selectConversation(conv.id);

            // Wait for the UI to settle, then stream the analysis
            await new Promise(r => setTimeout(r, 100));
            this.streamLogAnalysis(logData);
        } catch (err) {
            console.error('[Chat] Failed to create log analysis conversation:', err);
            if (window.Toast) Toast.error('Chat', 'Failed to start log analysis');
        }
    },

    async streamLogAnalysis(logData) {
        if (!this.currentConversationId || this.isStreaming) return;
        const logJSON = JSON.stringify(logData, null, 2);
        const fullContent = `Analyze this log entry. Explain the key fields, highlight anything notable or suspicious, and ask if I have questions.\n\n<log>\n${logJSON}\n</log>`;
        this.lastUserMessage = fullContent;
        this.appendUserMessage('Analyze this log');
        await this._streamToAssistant(fullContent);
    },


    // ---- Instructions ----

    async loadInstructions() {
        try {
            const res = await HttpUtils.safeFetch('/api/v1/instruction-libraries', { credentials: 'include' });
            this.instructionLibraries = res.data || [];
            this.renderInstructionSelect();
        } catch (err) {
            console.error('[Chat] Failed to load instruction libraries:', err);
        }
    },

    renderInstructionSelect() {
        const sel = document.getElementById('chatInstructionSelect');
        if (!sel) return;

        const libs = this.instructionLibraries || [];
        const defaultLib = libs.find(l => l.is_default);
        const defaultLabel = defaultLib ? `Default (${defaultLib.name})` : 'Default (none)';

        sel.innerHTML = `<option value="">${Utils.escapeHtml(defaultLabel)}</option>` +
            libs.map(lib =>
                `<option value="${Utils.escapeHtml(lib.id)}">${Utils.escapeHtml(lib.name)} (${lib.page_count} pages)${lib.is_default ? ' *' : ''}</option>`
            ).join('');

        // Sync with current conversation libraries
        this.syncLibrarySelect();
    },

    async syncLibrarySelect() {
        const sel = document.getElementById('chatInstructionSelect');
        if (!sel || !this.currentConversationId) return;

        try {
            const res = await HttpUtils.safeFetch(`/api/v1/chat/conversations/${this.currentConversationId}/libraries`, { credentials: 'include' });
            const libs = res.data || [];
            if (libs.length > 0) {
                sel.value = libs[0].id;
            } else {
                sel.value = '';
            }
        } catch (err) {
            sel.value = '';
        }
    },

    async onInstructionSelectChange() {
        if (!this.currentConversationId) return;
        const sel = document.getElementById('chatInstructionSelect');
        if (!sel) return;
        const libraryId = sel.value;
        const libraryIds = libraryId ? [libraryId] : [];

        try {
            await HttpUtils.safeFetch(`/api/v1/chat/conversations/${this.currentConversationId}/libraries`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ library_ids: libraryIds }),
                credentials: 'include',
            });
        } catch (err) {
            console.error('[Chat] Failed to set conversation libraries:', err);
            if (window.Toast) Toast.error('Chat', 'Failed to update libraries');
        }
    },

    openInSearch(query) {
        const input = document.getElementById('queryInput');
        if (input) input.value = query;
        if (window.App) App.showFractalViewTab('search');
        if (window.QueryExecutor) QueryExecutor.execute();
    },

    formatRelativeTime(isoStr) {
        if (!isoStr) return '';
        const date = new Date(isoStr);
        const now = new Date();
        const diff = Math.floor((now - date) / 1000);
        if (diff < 60) return 'just now';
        if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
        if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
        return Math.floor(diff / 86400) + 'd ago';
    },
};

window.Chat = Chat;
