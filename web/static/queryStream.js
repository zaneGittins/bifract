// Reads a /query/stream response, for the search page and the alert editor.
//
// A done or error frame is the run's answer. The server keeps the stream open after
// done to drain histogram chunks, so losing it then costs only the timeline.
const QueryStream = {
    INTERRUPTED: 'The query ended unexpectedly. Run it again.',

    // Calls onFrame with each frame. Rejects with INTERRUPTED when the stream ends
    // before an answer, and with AbortError when the run is aborted.
    async read(res, onFrame) {
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        let answered = false;
        try {
            for (;;) {
                let chunk;
                try {
                    chunk = await reader.read();
                } catch (err) {
                    if (err.name === 'AbortError') throw err;
                    if (answered) return;
                    throw new Error(this.INTERRUPTED);
                }
                if (chunk.done) break;
                buf += decoder.decode(chunk.value, { stream: true });
                const lines = buf.split('\n');
                buf = lines.pop();
                for (const line of lines) {
                    if (!line.trim()) continue;
                    let frame;
                    try { frame = JSON.parse(line); } catch (e) { continue; }
                    if (frame.type === 'done' || frame.type === 'error') answered = true;
                    onFrame(frame);
                }
            }
            if (!answered) throw new Error(this.INTERRUPTED);
        } finally {
            reader.releaseLock();
        }
    },
};

window.QueryStream = QueryStream;
