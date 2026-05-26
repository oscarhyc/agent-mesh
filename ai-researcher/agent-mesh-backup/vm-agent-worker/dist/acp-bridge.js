export class AcpBridge {
    state = "idle";
    sessionId = null;
    currentTaskId = null;
    pendingRpcId = 1;
    pendingRpc = new Map();
    taskTimeout = null;
    pendingTaskInterval = null;
    idleTimer = null;
    idleTimeoutMs;
    ws;
    subprocess;
    constructor(ws, subprocess, idleTimeoutMs = 30000) {
        this.idleTimeoutMs = idleTimeoutMs;
        this.ws = ws;
        this.subprocess = subprocess;
        // Incoming messages from subprocess stdout
        this.subprocess.onMessage = (msg) => {
            const raw = JSON.stringify(msg);
            console.log("[AcpBridge] <- subprocess: " + raw.slice(0, 200));
            try {
                const parsed = JSON.parse(raw);
                this.handleSubprocessMessage(parsed);
            }
            catch {
                console.log("[AcpBridge] <- subprocess: [raw " + typeof msg + "]");
            }
        };
        subprocess.onExit = (code, signal) => {
            console.log("[AcpBridge] subprocess exited -- code=" + code + " signal=" + signal);
            this.state = "done";
        };
        // Incoming messages from WebSocket
        ws.on("message", (data) => {
            const raw = data.toString();
            console.log("[AcpBridge] WS msg: " + raw.slice(0, 200));
            try {
                const msg = JSON.parse(raw);
                const type = msg["type"];
                if (type === "task" || type === "execute") {
                    const taskId = msg["taskId"];
                    const prompt = msg["prompt"];
                    const params = msg["params"];
                    const effectivePrompt = params ? params["prompt"] : undefined;
                    const finalPrompt = effectivePrompt ?? prompt ?? "";
                    this.handleTask({ taskId: taskId ?? "", prompt: finalPrompt });
                }
                else if (type === "cancel") {
                    const tid = msg["taskId"] ?? "";
                    this.handleCancel(tid);
                }
                else {
                    // Forward ACP method to hermes subprocess
                    const method = msg['method'];
                    const params = msg['params'];
                    if (method && params) {
                        // Include id if present for response routing
                        const id = msg['id'];
                        this.sendRpc(method, params)
                            .then((res) => {
                            this.ws.send(JSON.stringify({ jsonrpc: '2.0', id: res.id ?? id, result: res.result }));
                        })
                            .catch((err) => {
                            const errRes = { jsonrpc: '2.0', id: id ?? null, error: { code: -32600, message: String(err) } };
                            this.ws.send(JSON.stringify(errRes));
                        });
                    }
                }
            }
            catch (err) {
                console.error("[AcpBridge] bad WS message: " + err);
            }
        });
        ws.on("close", () => {
            console.log("[AcpBridge] WS client disconnected -- keeping hermes alive for reuse");
            if (this.taskTimeout)
                clearTimeout(this.taskTimeout);
            if (this.pendingTaskInterval)
                clearInterval(this.pendingTaskInterval);
            // Start idle timer; if no new task arrives, kill hermes
            this.startIdleTimer();
        });
        ws.on("error", (err) => {
            console.error("[AcpBridge] WS error: " + err);
        });
    }
    async start() {
        console.log("[AcpBridge] start() -- starting subprocess and ACP handshake");
        this.subprocess.start();
        await this.performAcpHandshake();
    }
    async performAcpHandshake() {
        this.state = "initializing";
        try {
            // Step 1: send initialize (may be needed before authenticate)
            console.log("[AcpBridge] -> sending initialize");
            const initRes = await this.sendRpc("initialize", {
                protocolVersion: "1.0",
                agentType: "agent-mesh",
                capabilities: { tools: true },
            });
            console.log("[AcpBridge] <- initialize response: " + JSON.stringify(initRes).slice(0, 200));
            // Step 2: authenticate
            console.log("[AcpBridge] -> sending authenticate");
            const methodId = "agent-" + Date.now();
            const authRes = await this.sendRpc("authenticate", { methodId });
            console.log("[AcpBridge] <- authenticate response: " + JSON.stringify(authRes).slice(0, 200));
            // Bug fix: check auth result - throw if auth failed
            if (authRes["error"]) {
                const errObj = authRes["error"];
                throw new Error("authenticate failed: " + (errObj["message"] ?? JSON.stringify(errObj)));
            }
            // Step 3: new_session with MCP servers
            this.state = "authenticating";
            console.log("[AcpBridge] -> sending new_session");
            const mcpServers = [
                {
                    name: "filesystem",
                    command: "node",
                    args: ["/home/oscar/.npm-global/lib/node_modules/@modelcontextprotocol/server-filesystem/dist/index.js", "/"],
                    env: [],
                },
            ];
            const sessionRes = await this.sendRpc("session/new", {
                cwd: "/tmp",
                mcpServers,
            });
            const sid = sessionRes["result"] &&
                typeof sessionRes["result"] === "object"
                ? sessionRes["result"]["sessionId"]
                : undefined;
            if (!sid) {
                throw new Error("new_session returned no sessionId: " + JSON.stringify(sessionRes));
            }
            this.sessionId = sid;
            console.log("[AcpBridge] <- new_session ok, sessionId=" + this.sessionId);
            this.state = "ready";
            console.log("[AcpBridge] ACP handshake complete -- ready for tasks");
        }
        catch (err) {
            console.error("[AcpBridge] ACP handshake failed: " + err);
            this.ws.send(JSON.stringify({ type: "error", message: "ACP handshake failed: " + err }));
            this.subprocess.kill();
            throw err;
        }
    }
    sendRpc(method, params) {
        return new Promise((resolve, reject) => {
            const id = this.pendingRpcId++;
            const msg = { jsonrpc: "2.0", method, params, id };
            this.pendingRpc.set(id, (res) => {
                if (res["error"]) {
                    reject(new Error("RPC " + method + " error: " + JSON.stringify(res["error"])));
                }
                else {
                    resolve(res);
                }
            });
            const wire = JSON.stringify(msg);
            console.log("[AcpBridge] -> RPC " + method + " id=" + id + ": " + wire.slice(0, 120));
            const sent = this.subprocess.send({ jsonrpc: "2.0", method, params, id });
            console.log("[AcpBridge] subprocess.send returned: " + sent);
            setTimeout(() => {
                if (this.pendingRpc.delete(id)) {
                    reject(new Error("RPC " + method + " timed out after 30s"));
                }
            }, 30_000);
        });
    }
    handleSubprocessMessage(msg) {
        const isNotification = msg.id === undefined && (msg.type === "session_update" ||
            msg["method"] === "session/update");
        if (isNotification) {
            this.handleSessionUpdate(msg);
            return;
        }
        if (msg.id !== undefined) {
            // Bug fix: parseInt can return NaN for non-numeric strings - handle it safely
            let id;
            if (typeof msg.id === "string") {
                const parsed = parseInt(msg.id, 10);
                if (isNaN(parsed)) {
                    console.log("[AcpBridge] ignoring RPC response with non-numeric id: " + msg.id);
                    return;
                }
                id = parsed;
            }
            else {
                id = msg.id;
            }
            const resolver = this.pendingRpc.get(id);
            if (resolver) {
                this.pendingRpc.delete(id);
                resolver(msg);
            }
            else {
                console.log("[AcpBridge] unexpected RPC response id=" + id + ": " + JSON.stringify(msg).slice(0, 120));
            }
        }
    }
    handleSessionUpdate(msg) {
        const params = msg["params"];
        if (!params)
            return;
        const update = params["update"];
        if (!update)
            return;
        const sessionUpdate = update["sessionUpdate"];
        if (sessionUpdate === "agent_message_chunk" || sessionUpdate === "agent_thought_chunk") {
            const content = update["content"];
            const text = content ? content["text"] : update["text"];
            if (text) {
                this.taskTextBuffer += text;
                this.ws.send(JSON.stringify({ type: "chunk", taskId: this.currentTaskId, text }));
            }
        }
        if (sessionUpdate === "agent_message") {
            const content = update["content"];
            if (content) {
                let finalText = "";
                for (const block of content) {
                    if (block["type"] === "text" && typeof block["text"] === "string") {
                        finalText = block["text"];
                    }
                }
                if (finalText)
                    this.taskTextBuffer = finalText;
            }
        }
        if (sessionUpdate === "usage_update") {
            const size = update["size"];
            const used = update["used"];
            if (size !== undefined && used !== undefined) {
                console.log("[AcpBridge] usage: " + used + "/" + size + " tokens");
            }
        }
    }
    taskTextBuffer = "";
    handleTask(payload) {
        if (this.state !== "ready" || !this.sessionId) {
            if (this.state === "initializing" || this.state === "authenticating") {
                console.log("[AcpBridge] task received while " + this.state + " -- waiting...");
                // Bug fix: clear any existing pending task interval before creating a new one
                if (this.pendingTaskInterval) {
                    clearInterval(this.pendingTaskInterval);
                    this.pendingTaskInterval = null;
                }
                this.pendingTaskInterval = setInterval(() => {
                    if (this.state === "ready") {
                        if (this.pendingTaskInterval) {
                            clearInterval(this.pendingTaskInterval);
                            this.pendingTaskInterval = null;
                        }
                        this.handleTask(payload);
                    }
                    else if (this.state === "done" || this.state === "idle") {
                        if (this.pendingTaskInterval) {
                            clearInterval(this.pendingTaskInterval);
                            this.pendingTaskInterval = null;
                        }
                        this.ws.send(JSON.stringify({ type: "error", message: "bridge closed during handshake", taskId: payload.taskId }));
                    }
                }, 1000);
                // Safety timeout
                setTimeout(() => {
                    if (this.pendingTaskInterval) {
                        clearInterval(this.pendingTaskInterval);
                        this.pendingTaskInterval = null;
                    }
                    if (this.state !== "ready") {
                        this.ws.send(JSON.stringify({ type: "error", message: "handshake timeout", taskId: payload.taskId }));
                    }
                }, 60000);
                return;
            }
            this.ws.send(JSON.stringify({ type: "error", message: "not ready (state=" + this.state + ")", taskId: payload.taskId }));
            return;
        }
        this.clearTaskTimeout();
        this.currentTaskId = payload.taskId ?? "mesh-" + Date.now();
        const prompt = payload.prompt ?? "";
        console.log("[AcpBridge] task dispatch -- taskId=" + this.currentTaskId + " prompt=" + prompt.slice(0, 80));
        this.taskTimeout = setTimeout(() => {
            console.log("[AcpBridge] task " + this.currentTaskId + " timed out after 60s");
            this.ws.send(JSON.stringify({ type: "error", message: "task timeout after 60s", taskId: this.currentTaskId }));
            this.subprocess.kill();
        }, 60_000);
        this.sendTaskPrompt(prompt).catch((err) => {
            console.error("[AcpBridge] task prompt error: " + err);
            this.ws.send(JSON.stringify({ type: "error", message: String(err), taskId: this.currentTaskId }));
            this.clearTaskTimeout();
        });
    }
    async sendTaskPrompt(prompt) {
        this.taskTextBuffer = "";
        const rpcRes = await this.sendRpc("session/prompt", {
            sessionId: this.sessionId,
            prompt: [{ type: "text", text: prompt }],
        });
        const res = rpcRes;
        const text = this.extractFinalText(res);
        console.log("[AcpBridge] task complete -- final text length=" + text.length + ", buffer had " + this.taskTextBuffer.length + " chars");
        this.clearTaskTimeout();
        this.clearIdleTimer();
        this.ws.send(JSON.stringify({ type: "result", taskId: this.currentTaskId, final: text, done: true }));
        await this.endCurrentSession();
        this.currentTaskId = null;
    }
    async endCurrentSession() {
        if (!this.sessionId)
            return;
        try {
            const res = await this.sendRpc("session/end", {
                sessionId: this.sessionId,
                reason: "task_complete",
            });
            console.log("[AcpBridge] session/end ok: " + JSON.stringify(res).slice(0, 100));
        }
        catch (err) {
            console.warn("[AcpBridge] session/end failed (non-fatal): " + err);
        }
        this.sessionId = null;
        this.startIdleTimer();
    }
    startIdleTimer() {
        if (this.idleTimer)
            clearTimeout(this.idleTimer);
        this.idleTimer = setTimeout(() => {
            console.log("[AcpBridge] idle timeout reached -- killing hermes");
            this.subprocess.kill();
        }, this.idleTimeoutMs);
    }
    clearIdleTimer() {
        if (this.idleTimer) {
            clearTimeout(this.idleTimer);
            this.idleTimer = null;
        }
    }
    // Bug fix: require exact taskId match, don't match empty string
    handleCancel(taskId) {
        if (taskId && this.currentTaskId === taskId) {
            console.log("[AcpBridge] cancelling task " + this.currentTaskId);
            this.clearTaskTimeout();
            if (this.pendingTaskInterval) {
                clearInterval(this.pendingTaskInterval);
                this.pendingTaskInterval = null;
            }
            this.subprocess.send({
                jsonrpc: "2.0",
                method: "session/cancel",
                params: { sessionId: this.sessionId },
                id: undefined,
            });
            this.ws.send(JSON.stringify({ type: "cancelled", taskId: this.currentTaskId }));
            this.currentTaskId = null;
        }
    }
    clearTaskTimeout() {
        if (this.taskTimeout) {
            clearTimeout(this.taskTimeout);
            this.taskTimeout = null;
        }
    }
    extractFinalText(rpcRes) {
        if (typeof rpcRes["text"] === "string")
            return rpcRes["text"];
        const content = rpcRes["content"];
        if (Array.isArray(content)) {
            for (const block of content) {
                if (block["type"] === "text" && typeof block["text"] === "string") {
                    return block["text"];
                }
            }
        }
        const result = rpcRes["result"];
        if (typeof result === "object" && result !== null) {
            const r = result;
            if (typeof r["text"] === "string")
                return r["text"];
            if (Array.isArray(r["content"])) {
                for (const block of r["content"]) {
                    if (block["type"] === "text" && typeof block["text"] === "string") {
                        return block["text"];
                    }
                }
            }
            if (typeof r["stopReason"] === "string") {
                return this.taskTextBuffer || "";
            }
        }
        return this.taskTextBuffer || JSON.stringify(rpcRes).slice(0, 500);
    }
}
