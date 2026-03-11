/**
 * Chat module - LLM-powered log analysis assistant.
 * Provides a conversational interface to query and explore fractal log data.
 */

const Chat = {
    currentConversationId: null,
    conversations: [],
    instructions: [],
    isStreaming: false,
    currentReader: null,
    loadingInterval: null,
    loadingMsgIndex: 0,
    initialized: false,

    loadingMessages: [
        'Scanning threat vectors...',
        'Correlating IOCs...',
        'Querying SIEM...',
        'Hunting APT patterns...',
        'Analyzing anomalies...',
        'Pivoting on indicators...',
        'Enriching with threat intel...',
        'De-obfuscating payload...',
        'Checking lateral movement...',
        'Parsing PCAP...',
        'Tracing beaconing behavior...',
        'Fingerprinting signatures...',
        'Running heuristics...',
        'Inspecting egress traffic...',
    ],

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

        const instructionsBtn = document.getElementById('chatInstructionsBtn');
        if (instructionsBtn) instructionsBtn.addEventListener('click', () => this.showInstructionsPanel());

        const instructionSelect = document.getElementById('chatInstructionSelect');
        if (instructionSelect) instructionSelect.addEventListener('change', () => this.onInstructionSelectChange());
    },

    show() {
        const fractal = window.FractalContext?.currentFractal;
        if (!fractal) return;
        this.loadConversations();
        this.loadInstructions();
    },

    hide() {
        this.stopStreaming();
    },

    onFractalChange() {
        this.stopStreaming();
        this.currentConversationId = null;
        this.conversations = [];
        this.instructions = [];
        this.showEmptyState();
        if (window.FractalContext?.currentFractal) {
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
            this.conversations = res.data?.conversations || [];
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
        this.stopStreaming();
        this.currentConversationId = id;
        this.renderConversationList();
        this.showActiveArea();
        this.loadMessages(id);

        // Update title
        const conv = this.conversations.find(c => c.id === id);
        const titleEl = document.getElementById('chatConversationTitle');
        if (titleEl && conv) titleEl.textContent = conv.title;

        // Sync instruction selector
        const sel = document.getElementById('chatInstructionSelect');
        if (sel && conv) sel.value = conv.instruction_id || '';
    },

    renderConversationList() {
        const list = document.getElementById('conversationList');
        if (!list) return;

        if (this.conversations.length === 0) {
            list.innerHTML = '<div class="conv-empty">No conversations yet</div>';
            return;
        }

        list.innerHTML = this.conversations.map(conv => {
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
            const res = await HttpUtils.safeFetch(`/api/v1/chat/conversations/${conversationId}/messages`, {
                credentials: 'include',
            });
            const messages = res.data?.messages || [];
            messages.forEach(msg => this.renderMessage(msg));
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

        // Render user message immediately
        this.appendUserMessage(userText);

        // Create assistant message bubble with loading state
        const assistantBubble = this.createAssistantBubble();
        const msgs = document.getElementById('chatMessages');
        if (msgs) {
            msgs.appendChild(this.createSeparator());
            msgs.appendChild(assistantBubble);
        }
        this.scrollToBottom();

        this.isStreaming = true;
        this.updateInputState(true);
        this.startLoadingAnimation(assistantBubble.querySelector('.chat-msg-content'));

        try {
            const timeRange = document.getElementById('chatTimeRange')?.value || '24h';
            const response = await fetch(`/api/v1/chat/conversations/${this.currentConversationId}/stream`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ content: userText, time_range: timeRange }),
                credentials: 'include',
            });

            if (!response.ok) {
                throw new Error(`Server error: ${response.status}`);
            }

            const reader = response.body.getReader();
            this.currentReader = reader;
            const decoder = new TextDecoder();
            let buffer = '';
            let contentEl = assistantBubble.querySelector('.chat-msg-content');
            let hasContent = false;

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split('\n');
                buffer = lines.pop(); // Keep incomplete line

                for (const line of lines) {
                    if (!line.startsWith('data: ')) continue;
                    const data = line.slice(6).trim();
                    if (!data) continue;

                    let event;
                    try {
                        event = JSON.parse(data);
                    } catch {
                        continue;
                    }

                    switch (event.type) {
                        case 'token':
                            if (!hasContent) {
                                this.clearBubbleLoading(contentEl);
                                contentEl.innerHTML = '';
                                hasContent = true;
                            }
                            this.appendToken(contentEl, event.content);
                            break;

                        case 'tool_call':
                            if (event.tool_name === 'present_results') break; // Handled by 'present' event
                            if (!hasContent) {
                                this.clearBubbleLoading(contentEl);
                                contentEl.innerHTML = '';
                                hasContent = true;
                            }
                            this.renderToolCall(contentEl, event.tool_name, event.tool_args);
                            break;

                        case 'tool_result':
                            this.renderToolResult(contentEl, event.tool_name, event.tool_result);
                            break;

                        case 'present':
                            // Remove streaming text but keep tool call blocks
                            const textSpan = contentEl.querySelector('.chat-streaming-text');
                            if (textSpan) textSpan.remove();
                            this.clearBubbleLoading(contentEl);
                            this.renderPresentation(contentEl, event.tool_args);
                            hasContent = true;
                            break;

                        case 'error':
                            this.hideStatusIndicator();
                            contentEl.innerHTML = `<span class="chat-error">${Utils.escapeHtml(event.content || 'Unknown error')}</span>`;
                            hasContent = true;
                            break;

                        case 'title':
                            // Update conversation title in sidebar and header
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
                    this.scrollToBottom();
                }
            }
        } catch (err) {
            if (err.name !== 'AbortError') {
                console.error('[Chat] Stream error:', err);
                const contentEl = assistantBubble.querySelector('.chat-msg-content');
                this.hideStatusIndicator();
                if (contentEl) {
                    contentEl.innerHTML = `<span class="chat-error">Connection error: ${Utils.escapeHtml(err.message)}</span>`;
                }
            }
        } finally {
            this.isStreaming = false;
            this.currentReader = null;
            this.hideStatusIndicator();
            this.updateInputState(false);
            this.scrollToBottom();
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

    // ---- Rendering ----

    renderMessage(msg) {
        const msgs = document.getElementById('chatMessages');
        if (!msgs) return;

        if (msg.role === 'tool') return; // Tool results rendered inline

        if (msg.role === 'user') {
            this.appendUserMessage(msg.content, true);
        } else if (msg.role === 'assistant') {
            // Add separator before assistant reply
            if (msgs.children.length > 0) {
                msgs.appendChild(this.createSeparator());
            }
            const bubble = this.createAssistantBubble();
            const contentEl = bubble.querySelector('.chat-msg-content');

            // Check if this message ends with present_results
            const hasPresentation = this.renderPresentFromHistory(contentEl, msg.tool_calls);

            if (!hasPresentation) {
                contentEl.innerHTML = this.formatAssistantContent(msg.content);

                // Render any tool calls that were part of this message
                if (msg.tool_calls && msg.tool_calls.length > 0) {
                    msg.tool_calls.forEach(tc => {
                        let args = {};
                        try { args = JSON.parse(tc.function?.arguments || '{}'); } catch {}
                        this.renderToolCall(contentEl, tc.function?.name, args);
                    });
                }
            }

            msgs.appendChild(bubble);
        }
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
        div.innerHTML = `<div class="chat-msg-label">Bot</div><div class="chat-msg-content"><span class="chat-loading-text"></span></div>`;
        return div;
    },

    appendToken(contentEl, token) {
        let textSpan = contentEl.querySelector('.chat-streaming-text');
        if (!textSpan) {
            textSpan = document.createElement('span');
            textSpan.className = 'chat-streaming-text chat-msg-text';
            contentEl.appendChild(textSpan);
        }
        textSpan.textContent += token;
    },

    formatAssistantContent(text) {
        if (!text) return '';
        return '<span class="chat-msg-text">' + Utils.escapeHtml(text)
            .replace(/`([^`]+)`/g, '<code class="chat-inline-code">$1</code>')
            .replace(/\n/g, '<br>') + '</span>';
    },

    trimTrailingWhitespace(contentEl) {
        const textSpan = contentEl.querySelector('.chat-streaming-text, .chat-msg-text');
        if (textSpan) {
            textSpan.textContent = textSpan.textContent.replace(/\s+$/, '');
        }
    },

    renderToolCall(contentEl, toolName, args) {
        this.trimTrailingWhitespace(contentEl);
        const div = document.createElement('div');
        div.className = 'chat-tool-call collapsed';

        if (toolName === 'get_fields') {
            div.innerHTML = `
                <div class="chat-tool-header">
                    <span class="chat-tool-chevron">&#9656;</span>
                    <span class="chat-tool-name">fields</span>
                    <span class="chat-tool-summary">discovering available fields</span>
                </div>
            `;
        } else if (toolName === 'search_alerts') {
            const search = args?.search || '';
            div.innerHTML = `
                <div class="chat-tool-header">
                    <span class="chat-tool-chevron">&#9656;</span>
                    <span class="chat-tool-name">alerts</span>
                    <span class="chat-tool-summary">${search ? 'searching: ' + Utils.escapeHtml(search) : 'listing all alerts'}</span>
                </div>
            `;
        } else {
            const query = args?.query || '';
            const timeMeta = Utils.escapeHtml(document.getElementById('chatTimeRange')?.value || '24h');
            div.innerHTML = `
                <div class="chat-tool-header">
                    <span class="chat-tool-chevron">&#9656;</span>
                    <span class="chat-tool-name">query</span>
                    <span class="chat-tool-summary">${Utils.escapeHtml(query)}</span>
                    <span class="chat-tool-meta">${timeMeta}</span>
                    <span class="chat-tool-search" title="Open in search">
                        <svg width="10" height="10" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <circle cx="6.5" cy="6.5" r="5.5" stroke="currentColor" stroke-width="2"/>
                            <line x1="10.5" y1="10.5" x2="15" y2="15" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                        </svg>
                    </span>
                </div>
                <pre class="chat-tool-query">${Utils.escapeHtml(query)}</pre>
            `;
            // Search icon click -> open in search view
            div.querySelector('.chat-tool-search').addEventListener('click', (e) => {
                e.stopPropagation();
                this.openInSearch(query);
            });
        }
        div.querySelector('.chat-tool-header').addEventListener('click', () => {
            div.classList.toggle('collapsed');
        });
        contentEl.appendChild(div);
    },

    renderToolResult(contentEl, toolName, result) {
        // Find the last tool-call block and attach result to it
        const toolCalls = contentEl.querySelectorAll('.chat-tool-call');
        const targetCall = toolCalls[toolCalls.length - 1] || contentEl;

        const resultDiv = document.createElement('div');
        resultDiv.className = 'chat-tool-result';

        if (result?.error) {
            resultDiv.innerHTML = `<span class="chat-error">Error: ${Utils.escapeHtml(result.error)}</span>`;
        } else if (toolName === 'get_fields') {
            const fields = result?.fields || [];
            if (fields.length === 0) {
                resultDiv.innerHTML = '<span class="chat-tool-empty">No fields found</span>';
            } else {
                const fieldStrs = fields.map(f => {
                    if (typeof f === 'string') return Utils.escapeHtml(f);
                    return `${Utils.escapeHtml(f.name)} <span style="opacity:0.5">(${f.count})</span>`;
                });
                resultDiv.innerHTML = `<span class="chat-tool-fields">${fieldStrs.join(', ')}</span>`;
            }
        } else if (toolName === 'search_alerts') {
            const alerts = result?.alerts || [];
            if (alerts.length === 0) {
                resultDiv.innerHTML = '<span class="chat-tool-empty">No alerts found</span>';
            } else {
                const rows = alerts.map(a => ({
                    name: a.name,
                    type: a.alert_type,
                    enabled: a.enabled ? 'yes' : 'no',
                    query: a.query,
                    ...(a.feed_name ? { feed: a.feed_name } : {})
                }));
                const fieldOrder = ['name', 'type', 'enabled', 'query'];
                if (alerts.some(a => a.feed_name)) fieldOrder.push('feed');
                resultDiv.innerHTML = this.renderMiniTable(rows, fieldOrder, alerts.length, false);
            }
        } else {
            const rows = result?.rows || [];
            const count = result?.count || 0;
            const truncated = result?.is_truncated || false;
            const fieldOrder = result?.field_order || [];

            if (rows.length === 0) {
                resultDiv.innerHTML = '<span class="chat-tool-empty">No results</span>';
            } else {
                resultDiv.innerHTML = this.renderMiniTable(rows, fieldOrder, count, truncated);
            }
        }

        targetCall.appendChild(resultDiv);
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
        div.className = `chat-presentation chat-severity-${severity}`;

        let html = '';

        // Severity badge
        if (severity !== 'info') {
            html += `<span class="chat-severity-badge chat-severity-${severity}">${Utils.escapeHtml(severity)}</span> `;
        }

        // Summary text
        html += `<span class="chat-present-summary">${Utils.escapeHtml(summary)}</span>`;

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
    },

    // Also render present_results from history
    renderPresentFromHistory(contentEl, toolCalls) {
        if (!toolCalls || toolCalls.length === 0) return false;
        for (const tc of toolCalls) {
            if (tc.function?.name === 'present_results') {
                let args = {};
                try { args = JSON.parse(tc.function.arguments || '{}'); } catch {}
                this.renderPresentation(contentEl, args);
                return true;
            }
        }
        return false;
    },

    // ---- Loading / status indicator ----

    startLoadingAnimation(contentEl) {
        if (!contentEl) return;
        this.loadingMsgIndex = Math.floor(Math.random() * this.loadingMessages.length);

        // Show in-bubble loading dot
        const textEl = contentEl.querySelector('.chat-loading-text');
        if (textEl) textEl.textContent = '...';

        // Show bottom-left status indicator with cycling messages
        this.showStatusIndicator();
    },

    // Remove in-bubble "..." but keep the status indicator running
    clearBubbleLoading(contentEl) {
        const el = contentEl?.querySelector('.chat-loading-text');
        if (el) el.remove();
    },

    showStatusIndicator() {
        const indicator = document.getElementById('chatStatusIndicator');
        const statusText = document.getElementById('chatStatusText');
        if (indicator) indicator.style.display = 'flex';

        this.loadingMsgIndex = Math.floor(Math.random() * this.loadingMessages.length);
        const update = () => {
            if (statusText) {
                statusText.textContent = this.loadingMessages[this.loadingMsgIndex % this.loadingMessages.length];
                this.loadingMsgIndex++;
            }
        };
        update();
        if (!this.loadingInterval) {
            this.loadingInterval = setInterval(update, 2200);
        }
    },

    hideStatusIndicator() {
        if (this.loadingInterval) {
            clearInterval(this.loadingInterval);
            this.loadingInterval = null;
        }
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
        const btn = document.getElementById('chatSendBtn');
        if (input) input.disabled = streaming;
        if (btn) {
            btn.disabled = streaming;
            btn.classList.toggle('loading', streaming);
        }
    },

    scrollToBottom() {
        const msgs = document.getElementById('chatMessages');
        if (msgs) msgs.scrollTop = msgs.scrollHeight;
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

        // Build the full message for the LLM with log context
        const logJSON = JSON.stringify(logData, null, 2);
        const fullContent = `Analyze this log entry. Explain the key fields, highlight anything notable or suspicious, and ask if I have questions.\n\n<log>\n${logJSON}\n</log>`;

        // Show a compact user message (not the full log dump)
        this.appendUserMessage('Analyze this log');

        // Create assistant bubble
        const assistantBubble = this.createAssistantBubble();
        const msgs = document.getElementById('chatMessages');
        if (msgs) {
            msgs.appendChild(this.createSeparator());
            msgs.appendChild(assistantBubble);
        }
        this.scrollToBottom();

        this.isStreaming = true;
        this.updateInputState(true);
        this.startLoadingAnimation(assistantBubble.querySelector('.chat-msg-content'));

        try {
            const timeRange = document.getElementById('chatTimeRange')?.value || '24h';
            const response = await fetch(`/api/v1/chat/conversations/${this.currentConversationId}/stream`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ content: fullContent, time_range: timeRange }),
                credentials: 'include',
            });

            if (!response.ok) {
                throw new Error(`Server error: ${response.status}`);
            }

            const reader = response.body.getReader();
            this.currentReader = reader;
            const decoder = new TextDecoder();
            let buffer = '';
            let contentEl = assistantBubble.querySelector('.chat-msg-content');
            let hasContent = false;

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

                    switch (event.type) {
                        case 'token':
                            if (!hasContent) { this.clearBubbleLoading(contentEl); contentEl.innerHTML = ''; hasContent = true; }
                            this.appendToken(contentEl, event.content);
                            break;
                        case 'tool_call':
                            if (event.tool_name === 'present_results') break;
                            if (!hasContent) { this.clearBubbleLoading(contentEl); contentEl.innerHTML = ''; hasContent = true; }
                            this.renderToolCall(contentEl, event.tool_name, event.tool_args);
                            break;
                        case 'tool_result':
                            this.renderToolResult(contentEl, event.tool_name, event.tool_result);
                            break;
                        case 'present':
                            const textSpan = contentEl.querySelector('.chat-streaming-text');
                            if (textSpan) textSpan.remove();
                            this.clearBubbleLoading(contentEl);
                            this.renderPresentation(contentEl, event.tool_args);
                            hasContent = true;
                            break;
                        case 'error':
                            this.hideStatusIndicator();
                            contentEl.innerHTML = `<span class="chat-error">${Utils.escapeHtml(event.content || 'Unknown error')}</span>`;
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
                    this.scrollToBottom();
                }
            }
        } catch (err) {
            if (err.name !== 'AbortError') {
                console.error('[Chat] Log analysis stream error:', err);
                const contentEl = assistantBubble.querySelector('.chat-msg-content');
                this.hideStatusIndicator();
                if (contentEl) {
                    contentEl.innerHTML = `<span class="chat-error">Connection error: ${Utils.escapeHtml(err.message)}</span>`;
                }
            }
        } finally {
            this.isStreaming = false;
            this.currentReader = null;
            this.hideStatusIndicator();
            this.updateInputState(false);
            this.scrollToBottom();
        }
    },

    /**
     * Start an AI chat session with notebook content as context
     */
    async analyzeNotebook(notebook) {
        if (!notebook || !notebook.sections) return;

        // Build context from notebook sections
        const parts = [];
        if (notebook.name) parts.push(`# Notebook: ${notebook.name}`);
        if (notebook.description) parts.push(notebook.description);

        for (const section of notebook.sections) {
            if (section.section_type === 'markdown' && section.content) {
                parts.push(section.content);
            } else if (section.section_type === 'query' && section.content) {
                parts.push('```quandrix\n' + section.content + '\n```');
                if (section.last_results) {
                    try {
                        let results = section.last_results;
                        if (typeof results === 'string') results = JSON.parse(results);
                        if (results.results && results.results.length > 0) {
                            const preview = results.results.slice(0, 20);
                            parts.push('Query results (' + results.count + ' rows):\n```json\n' + JSON.stringify(preview, null, 2) + '\n```');
                        }
                    } catch (e) { /* skip unparseable results */ }
                }
            } else if (section.section_type === 'comment_context') {
                try {
                    const data = JSON.parse(section.content || '{}');
                    if (data.comment_text) parts.push('Comment: ' + data.comment_text);
                    if (data.query) parts.push('Query context: `' + data.query + '`');
                } catch (e) { /* skip */ }
            }
        }

        const context = parts.join('\n\n');
        const fullContent = `I have a notebook with the following content. Use it as context for our conversation. Summarize key findings and ask what I want to explore further.\n\n<notebook>\n${context}\n</notebook>`;

        // Navigate to chat view
        if (window.App) App.showFractalViewTab('chat');

        try {
            const res = await HttpUtils.safeFetch('/api/v1/chat/conversations', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ title: notebook.name || 'Notebook Analysis' }),
                credentials: 'include',
            });
            const conv = res.data;
            this.conversations.unshift(conv);
            this.renderConversationList();
            this.selectConversation(conv.id);

            await new Promise(r => setTimeout(r, 100));

            // Show compact user message
            this.appendUserMessage('Analyze this notebook');

            const assistantBubble = this.createAssistantBubble();
            const msgs = document.getElementById('chatMessages');
            if (msgs) {
                msgs.appendChild(this.createSeparator());
                msgs.appendChild(assistantBubble);
            }
            this.scrollToBottom();

            this.isStreaming = true;
            this.updateInputState(true);
            this.startLoadingAnimation(assistantBubble.querySelector('.chat-msg-content'));

            const timeRange = document.getElementById('chatTimeRange')?.value || '24h';
            const response = await fetch(`/api/v1/chat/conversations/${this.currentConversationId}/stream`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ content: fullContent, time_range: timeRange }),
                credentials: 'include',
            });

            if (!response.ok) throw new Error(`Server error: ${response.status}`);

            const reader = response.body.getReader();
            this.currentReader = reader;
            const decoder = new TextDecoder();
            let buffer = '';
            let contentEl = assistantBubble.querySelector('.chat-msg-content');
            let hasContent = false;

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
                    switch (event.type) {
                        case 'token':
                            if (!hasContent) { this.clearBubbleLoading(contentEl); contentEl.innerHTML = ''; hasContent = true; }
                            this.appendToken(contentEl, event.content);
                            break;
                        case 'tool_call':
                            if (event.tool_name === 'present_results') break;
                            if (!hasContent) { this.clearBubbleLoading(contentEl); contentEl.innerHTML = ''; hasContent = true; }
                            this.renderToolCall(contentEl, event.tool_name, event.tool_args);
                            break;
                        case 'tool_result':
                            this.renderToolResult(contentEl, event.tool_name, event.tool_result);
                            break;
                        case 'present':
                            const textSpan = contentEl.querySelector('.chat-streaming-text');
                            if (textSpan) textSpan.remove();
                            this.clearBubbleLoading(contentEl);
                            this.renderPresentation(contentEl, event.tool_args);
                            hasContent = true;
                            break;
                        case 'error':
                            this.hideStatusIndicator();
                            contentEl.innerHTML = `<span class="chat-error">${Utils.escapeHtml(event.content || 'Unknown error')}</span>`;
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
                    this.scrollToBottom();
                }
            }
        } catch (err) {
            if (err.name !== 'AbortError') {
                console.error('[Chat] Notebook analysis stream error:', err);
                if (window.Toast) Toast.error('Chat', 'Failed to analyze notebook');
            }
        } finally {
            this.isStreaming = false;
            this.currentReader = null;
            this.hideStatusIndicator();
            this.updateInputState(false);
            this.scrollToBottom();
        }
    },

    // ---- Instructions ----

    async loadInstructions() {
        try {
            const res = await HttpUtils.safeFetch('/api/v1/chat/instructions', { credentials: 'include' });
            this.instructions = res.data?.instructions || [];
            this.renderInstructionSelect();
        } catch (err) {
            console.error('[Chat] Failed to load instructions:', err);
        }
    },

    renderInstructionSelect() {
        const sel = document.getElementById('chatInstructionSelect');
        if (!sel) return;

        const defaultInst = this.instructions.find(i => i.is_default);
        const defaultLabel = defaultInst ? `Default (${defaultInst.name})` : 'Default';

        sel.innerHTML = `<option value="">${Utils.escapeHtml(defaultLabel)}</option>` +
            this.instructions.map(inst =>
                `<option value="${Utils.escapeHtml(inst.id)}">${Utils.escapeHtml(inst.name)}${inst.is_default ? ' *' : ''}</option>`
            ).join('');

        // Sync with current conversation
        const conv = this.conversations.find(c => c.id === this.currentConversationId);
        if (conv) sel.value = conv.instruction_id || '';
    },

    async onInstructionSelectChange() {
        if (!this.currentConversationId) return;
        const sel = document.getElementById('chatInstructionSelect');
        if (!sel) return;
        const instructionId = sel.value || null;

        try {
            await HttpUtils.safeFetch(`/api/v1/chat/conversations/${this.currentConversationId}/instruction`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ instruction_id: instructionId }),
                credentials: 'include',
            });
            // Update local state
            const conv = this.conversations.find(c => c.id === this.currentConversationId);
            if (conv) conv.instruction_id = instructionId;
        } catch (err) {
            console.error('[Chat] Failed to set instruction:', err);
            if (window.Toast) Toast.error('Chat', 'Failed to update instruction');
        }
    },

    showInstructionsPanel() {
        // Create panel overlay inside chat-main
        const main = document.querySelector('.chat-main');
        if (!main) return;

        // Remove existing panel if open
        const existing = main.querySelector('.chat-instructions-panel');
        if (existing) { existing.remove(); return; }

        const panel = document.createElement('div');
        panel.className = 'chat-instructions-panel';
        panel.innerHTML = `
            <div class="chat-instructions-panel-header">
                <h3>Instructions</h3>
                <button class="chat-instructions-close-btn" title="Close">&times;</button>
            </div>
            <div class="chat-instructions-list" id="chatInstructionsList"></div>
            <div class="chat-instructions-panel-footer">
                <button class="chat-instructions-add-btn" id="chatAddInstructionBtn">+ New instruction</button>
            </div>
        `;

        main.appendChild(panel);
        panel.querySelector('.chat-instructions-close-btn').addEventListener('click', () => panel.remove());
        panel.querySelector('#chatAddInstructionBtn').addEventListener('click', () => this.showInstructionEditor(panel));
        this.renderInstructionCards(panel);
    },

    renderInstructionCards(panel) {
        const list = panel.querySelector('#chatInstructionsList');
        if (!list) return;

        if (this.instructions.length === 0) {
            list.innerHTML = '<div class="chat-instructions-empty">No custom instructions yet.<br>Create one to customize how the AI assistant behaves for this fractal.</div>';
            return;
        }

        list.innerHTML = this.instructions.map(inst => `
            <div class="chat-instruction-card" data-id="${Utils.escapeHtml(inst.id)}">
                <div class="chat-instruction-card-header">
                    <span class="chat-instruction-card-name">
                        ${Utils.escapeHtml(inst.name)}
                        ${inst.is_default ? '<span class="chat-instruction-default-badge">default</span>' : ''}
                    </span>
                    <div class="chat-instruction-card-actions">
                        <button class="edit-btn" title="Edit">Edit</button>
                        <button class="danger delete-btn" title="Delete">Delete</button>
                    </div>
                </div>
                <div class="chat-instruction-card-preview">${Utils.escapeHtml(inst.content || '(empty)')}</div>
            </div>
        `).join('');

        list.querySelectorAll('.edit-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const id = e.target.closest('.chat-instruction-card').dataset.id;
                const inst = this.instructions.find(i => i.id === id);
                if (inst) this.showInstructionEditor(panel, inst);
            });
        });

        list.querySelectorAll('.delete-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const id = e.target.closest('.chat-instruction-card').dataset.id;
                this.deleteInstruction(panel, id);
            });
        });
    },

    showInstructionEditor(panel, existing = null) {
        const list = panel.querySelector('#chatInstructionsList');
        const footer = panel.querySelector('.chat-instructions-panel-footer');
        if (!list) return;

        if (footer) footer.style.display = 'none';

        list.innerHTML = `
            <div class="chat-instruction-editor">
                <label>Name</label>
                <input type="text" id="instEditorName" placeholder="e.g. Security Analyst" value="${existing ? Utils.escapeHtml(existing.name) : ''}" />
                <label>Instructions</label>
                <textarea id="instEditorContent" placeholder="Tell the AI how to behave, what to focus on, what format to use...">${existing ? Utils.escapeHtml(existing.content) : ''}</textarea>
                <label class="chat-instruction-default-toggle">
                    <input type="checkbox" id="instEditorDefault" ${existing?.is_default ? 'checked' : ''} />
                    Use as default for new conversations
                </label>
                <div class="chat-instruction-editor-actions">
                    <button class="chat-instruction-cancel-btn" id="instEditorCancel">Cancel</button>
                    <button class="chat-instruction-save-btn" id="instEditorSave">${existing ? 'Save' : 'Create'}</button>
                </div>
            </div>
        `;

        panel.querySelector('#instEditorCancel').addEventListener('click', () => {
            if (footer) footer.style.display = '';
            this.renderInstructionCards(panel);
        });

        panel.querySelector('#instEditorSave').addEventListener('click', () => {
            this.saveInstruction(panel, existing?.id);
        });
    },

    async saveInstruction(panel, existingId = null) {
        const name = document.getElementById('instEditorName')?.value.trim();
        const content = document.getElementById('instEditorContent')?.value.trim();
        const isDefault = document.getElementById('instEditorDefault')?.checked || false;

        if (!name) {
            if (window.Toast) Toast.error('Chat', 'Instruction name is required');
            return;
        }

        try {
            if (existingId) {
                await HttpUtils.safeFetch(`/api/v1/chat/instructions/${existingId}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, content, is_default: isDefault }),
                    credentials: 'include',
                });
            } else {
                await HttpUtils.safeFetch('/api/v1/chat/instructions', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, content, is_default: isDefault }),
                    credentials: 'include',
                });
            }
            await this.loadInstructions();
            const footer = panel.querySelector('.chat-instructions-panel-footer');
            if (footer) footer.style.display = '';
            this.renderInstructionCards(panel);
        } catch (err) {
            console.error('[Chat] Failed to save instruction:', err);
            if (window.Toast) Toast.error('Chat', 'Failed to save instruction');
        }
    },

    async deleteInstruction(panel, id) {
        if (!confirm('Delete this instruction?')) return;
        try {
            await HttpUtils.safeFetch(`/api/v1/chat/instructions/${id}`, {
                method: 'DELETE',
                credentials: 'include',
            });
            await this.loadInstructions();
            this.renderInstructionCards(panel);
        } catch (err) {
            console.error('[Chat] Failed to delete instruction:', err);
            if (window.Toast) Toast.error('Chat', 'Failed to delete instruction');
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
