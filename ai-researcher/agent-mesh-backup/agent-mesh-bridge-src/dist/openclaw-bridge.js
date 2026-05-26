// @ts-nocheck
/**
 * OpenClaw Mesh Bridge — AcpRuntime backend for the agent mesh.
 * Registers as "mesh" backend via registerAcpRuntimeBackend.
 */
// ---------------------------------------------------------------------------
// Registry Client — same LAN WebSocket discovery
// ---------------------------------------------------------------------------
import WebSocket from "ws";
export class RegistryClient {
    registryUrl;
    ws = null;
    pending = new Map();
    nextId = 1;
    reconnectTimer = null;
    constructor(registryUrl) {
        this.registryUrl = registryUrl;
    }
    async connect() {
        return new Promise((resolve, reject) => {
            this.ws = new WebSocket(this.registryUrl);
            this.ws.on("open", () => resolve());
            this.ws.on("error", reject);
            this.ws.on("message", (data) => {
                try {
                    const msg = JSON.parse(data.toString());
                    const cb = this.pending.get(msg.id);
                    if (cb) {
                        cb(msg);
                        this.pending.delete(msg.id);
                    }
                }
                catch { }
            });
            this.ws.on("close", () => this.scheduleReconnect());
        });
    }
    scheduleReconnect() {
        if (this.reconnectTimer)
            return;
        this.reconnectTimer = setTimeout(async () => {
            this.reconnectTimer = null;
            try {
                await this.connect();
            }
            catch { }
        }, 5000);
    }
    async call(method, params) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN)
            await this.connect();
        return new Promise((resolve, reject) => {
            const id = this.nextId++;
            const msg = { jsonrpc: "2.0", method, params, id };
            this.pending.set(id, resolve);
            this.ws.send(JSON.stringify(msg));
            setTimeout(() => {
                if (this.pending.has(id)) {
                    this.pending.delete(id);
                    reject(new Error(`RPC ${method} timed out`));
                }
            }, 15000);
        });
    }
    async discover(criteria, limit = 1) {
        const res = await this.call("discover", { capabilities: criteria, limit });
        return res.result?.agents ?? [];
    }
    close() {
        this.ws?.close();
        if (this.reconnectTimer)
            clearTimeout(this.reconnectTimer);
        this.pending.clear();
    }
}
// ---------------------------------------------------------------------------
// Bridge Client — direct WS connection to a worker
// ---------------------------------------------------------------------------
export class BridgeClient {
    host;
    port;
    onMessage;
    onClose;
    ws = null;
    reconnectAttempts = 0;
    maxReconnectAttempts = 5;
    reconnectDelayMs = 1000;
    constructor(host, port, onMessage, onClose) {
        this.host = host;
        this.port = port;
        this.onMessage = onMessage;
        this.onClose = onClose;
    }
    async connect() {
        return new Promise((resolve, reject) => {
            this.ws = new WebSocket(`ws://${this.host}:${this.port}`);
            this.ws.on("open", () => { this.reconnectAttempts = 0; resolve(); });
            this.ws.on("error", reject);
            this.ws.on("message", (data) => { try {
                this.onMessage(JSON.parse(data.toString()));
            }
            catch { } });
            this.ws.on("close", () => { this.onClose(); this.attemptReconnect(); });
        });
    }
    send(msg) {
        if (this.ws?.readyState === WebSocket.OPEN)
            this.ws.send(JSON.stringify(msg));
    }
    attemptReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts)
            return;
        this.reconnectAttempts++;
        setTimeout(() => this.connect().catch(() => { }), this.reconnectDelayMs * this.reconnectAttempts);
    }
    close() { this.ws?.close(); }
}
export class MeshRuntime {
    config;
    registry;
    sessions = new Map();
    eventQueues = new Map();
    eventQueueListeners = new Map();
    constructor(config) {
        this.config = config;
        this.registry = new RegistryClient(config.registry);
    }
    async spawn(agentType, taskId) {
        const agents = await this.registry.discover({ agentType }, 1);
        if (!agents.length) {
            console.log(`[MeshRuntime] No agents for type=${agentType}`);
            return null;
        }
        const agent = agents[0];
        console.log(`[MeshRuntime] Spawning task=${taskId} on ${agent.agentId} (${agent.host}:${agent.port})`);
        return new Promise((resolve, reject) => {
            const client = new BridgeClient(agent.host, agent.port, (msg) => this.pushEvent(taskId, msg), () => { this.respawn(taskId, agentType).then(resolve).catch(reject); });
            client.connect().then(() => {
                this.sessions.set(taskId, { agentId: agent.agentId, taskId, host: agent.host, port: agent.port, client });
                resolve({ agentId: agent.agentId, taskId, host: agent.host, port: agent.port, client });
            }).catch(reject);
        });
    }
    async respawn(taskId, agentType) {
        return this.spawn(agentType, taskId);
    }
    pushEvent(sk, msg) {
        const queue = this.eventQueues.get(sk) ?? [];
        queue.push(msg);
        this.eventQueues.set(sk, queue);
        const listener = this.eventQueueListeners.get(sk);
        if (listener) {
            this.eventQueueListeners.delete(sk);
            listener(msg);
        }
    }
    /** AsyncGenerator that yields MeshEvents as they arrive from the worker */
    async *events(sk) {
        const queue = this.eventQueues.get(sk) ?? [];
        this.eventQueues.set(sk, queue);
        while (true) {
            while (queue.length > 0)
                yield queue.shift();
            yield await new Promise((resolve) => { this.eventQueueListeners.set(sk, resolve); });
        }
    }
    send(sk, msg) { this.sessions.get(sk)?.client.send(msg); }
    close(sk) {
        this.sessions.get(sk)?.client.close();
        this.sessions.delete(sk);
        this.eventQueues.delete(sk);
        this.eventQueueListeners.delete(sk);
    }
}
// ---------------------------------------------------------------------------
// Event conversion: MeshEvent → OpenClaw AcpRuntimeEvent
// ---------------------------------------------------------------------------
function meshEventToAcpEvents(msg) {
    const events = [];
    switch (msg.type) {
        case "result": {
            const r = msg;
            if (r.error)
                events.push({ type: "error", message: r.error, code: "agent_error", retryable: false });
            else if (r.chunk)
                events.push({ type: "text_delta", text: r.chunk });
            else if (r.final)
                events.push({ type: "text_delta", text: r.final });
            if (r.done)
                events.push({ type: "done", stopReason: "stop" });
            break;
        }
        case "agent_message":
        case "message": {
            const am = msg;
            if (am.content?.length) {
                for (const b of am.content) {
                    if (b.type === "text" && b.text)
                        events.push({ type: "text_delta", text: b.text });
                }
            }
            break;
        }
        case "error":
        case "error_response": {
            const e = msg;
            events.push({ type: "error", message: e.message ?? "Unknown", code: e.code ?? "agent_error", retryable: false });
            break;
        }
        case "done":
        case "stop": {
            events.push({ type: "done", stopReason: "stop" });
            break;
        }
    }
    return events;
}
// ---------------------------------------------------------------------------
// AcpRuntime implementation
// ---------------------------------------------------------------------------
const BACKEND_ID = "mesh";
class MeshBridgeRuntime {
    meshBySession = new Map();
    async ensureSession(input) {
        const { sessionKey, agent: agentType } = input;
        const mesh = new MeshRuntime({ registry: process.env.MESH_REGISTRY_URL ?? "ws://192.168.1.206:9000" });
        const session = await mesh.spawn(agentType, sessionKey);
        if (!session)
            throw new Error(`[mesh] no agent for type=${agentType}`);
        this.meshBySession.set(sessionKey, mesh);
        return {
            sessionKey,
            backend: BACKEND_ID,
            runtimeSessionName: sessionKey,
            agentSessionId: session.agentId,
        };
    }
    async *runTurn(input) {
        const sk = input.handle?.sessionKey ?? "default";
        const mesh = this.meshBySession.get(sk);
        if (!mesh) {
            yield { type: "error", message: `No mesh session: ${sk}` };
            return;
        }
        const msg = { type: "execute", taskId: sk, sessionKey: sk, prompt: input.text ?? "" };
        mesh.send(sk, msg);
        try {
            for await (const ev of mesh.events(sk)) {
                for (const e of meshEventToAcpEvents(ev))
                    yield e;
                if (ev.type === "done" || ev.done)
                    return;
            }
        }
        finally {
            mesh.close(sk);
            this.meshBySession.delete(sk);
        }
    }
    async cancel(input) {
        const m = this.meshBySession.get(input.handle.sessionKey);
        if (m) {
            m.close(input.handle.sessionKey);
            this.meshBySession.delete(input.handle.sessionKey);
        }
    }
    async close(input) {
        const m = this.meshBySession.get(input.handle.sessionKey);
        if (m) {
            m.close(input.handle.sessionKey);
            this.meshBySession.delete(input.handle.sessionKey);
        }
    }
}
// ---------------------------------------------------------------------------
// Registration — call this once to activate the mesh backend
// ---------------------------------------------------------------------------
export async function registerMeshBridge(config) {
    if (config?.registry)
        process.env.MESH_REGISTRY_URL = config.registry;
    if (config?.defaultAgentType)
        process.env.MESH_DEFAULT_AGENT_TYPE = config.defaultAgentType;
    // Dynamic import to avoid TS path resolution issues with openclaw/plugin-sdk
    const mod = await import("openclaw/plugin-sdk/acp-runtime");
    const register = mod.registerAcpRuntimeBackend ?? mod.default?.registerAcpRuntimeBackend;
    if (!register)
        throw new Error("Cannot find registerAcpRuntimeBackend in openclaw/plugin-sdk/acp-runtime");
    register({ id: BACKEND_ID, runtime: new MeshBridgeRuntime(), healthy: () => true });
    console.log(`[agent-mesh-bridge] Registered AcpRuntimeBackend "${BACKEND_ID}" (registry=${process.env.MESH_REGISTRY_URL})`);
}
