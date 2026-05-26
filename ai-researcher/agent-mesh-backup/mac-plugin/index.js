/**
 * OpenClaw plugin entry point for agent-mesh bridge.
 * Discovery: ~/.openclaw/agent-mesh-plugin/src/index.js
 * Enable: openclaw.json → plugins.entries.agent-mesh.enabled: true
 */
const { createRequire } = require("module");
// Use openclaw's dist/index.js as base so ws and openclaw/plugin-sdk resolve correctly
const require_ = createRequire(require.resolve("/opt/homebrew/lib/node_modules/openclaw/dist/index.js"));

const WebSocket = require_("ws");
const { registerAcpRuntimeBackend } = require_("openclaw/plugin-sdk/acp-runtime");

// ---------------------------------------------------------------------------
// Registry Client
// ---------------------------------------------------------------------------

/**
 * @param {string} registryUrl
 * @param {object} criteria
 * @param {number} limit
 * @returns {Promise<{ agents: Array<{ agentId: string, host: string, port: number, capabilities?: { agentType?: string } }> }>}
 */
async function rpcDiscover(registryUrl, criteria, limit = 1) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(registryUrl);
    let timer;
    const cleanup = () => { clearTimeout(timer); ws.close(); };
    ws.on("open", () => {
      const id = Date.now() % 99999;
      ws.send(JSON.stringify({ jsonrpc: "2.0", method: "discover", params: { capabilities: criteria, limit }, id }));
      timer = setTimeout(() => { cleanup(); reject(new Error("discover timeout")); }, 15000);
    });
    ws.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.result?.agents) { cleanup(); resolve(msg.result); }
        else if (msg.error) { cleanup(); reject(new Error(msg.error.message)); }
      } catch {}
    });
    ws.on("error", (err) => { cleanup(); reject(err); });
    ws.on("close", () => { clearTimeout(timer); reject(new Error("registry closed")); });
  });
}

// ---------------------------------------------------------------------------
// Bridge Client
// ---------------------------------------------------------------------------

class BridgeClient {
  /**
   * @param {string} host
   * @param {number} port
   * @param {(msg: any) => void} onMessage
   * @param {() => void} onClose
   */
  constructor(host, port, onMessage, onClose) {
    this.host = host; this.port = port;
    this.onMessage = onMessage; this.onClose = onClose;
    this.ws = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.reconnectDelayMs = 1000;
  }

  async connect() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(`ws://${this.host}:${this.port}`);
      this.ws.on("open", () => { this.reconnectAttempts = 0; resolve(); });
      this.ws.on("error", reject);
      this.ws.on("message", (data) => { try { this.onMessage(JSON.parse(data.toString())); } catch {} });
      this.ws.on("close", () => { this.onClose(); this._attemptReconnect(); });
    });
  }

  /** @param {any} msg */
  send(msg) { if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(msg)); }

  _attemptReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) return;
    this.reconnectAttempts++;
    setTimeout(() => this.connect().catch(() => {}), this.reconnectDelayMs * this.reconnectAttempts);
  }

  close() { this.ws?.close(); }
}

// ---------------------------------------------------------------------------
// Mesh Runtime
// ---------------------------------------------------------------------------

class MeshRuntime {
  /**
   * @param {{ registry: string, defaultAgentType?: string }} config
   */
  constructor(config) {
    this.config = config;
    /** @type {Map<string, any>} */
    this.sessions = new Map();
    /** @type {Map<string, any[]>} */
    this.eventQueues = new Map();
    /** @type {Map<string, (ev: any) => void>} */
    this.eventQueueListeners = new Map();
  }

  /** @param {string} agentType @param {string} taskId @returns {Promise<any|null>} */
  async spawn(agentType, taskId) {
    try {
      const result = await rpcDiscover(this.config.registry, { agentType }, 1);
      const agents = result.agents;
      if (!agents?.length) { console.log(`[MeshRuntime] No agents for type=${agentType}`); return null; }
      const agent = agents[0];
      console.log(`[MeshRuntime] Spawning task=${taskId} on ${agent.agentId} (${agent.host}:${agent.port})`);
      const self = this;
      const client = new BridgeClient(agent.host, agent.port,
        (msg) => self._pushEvent(taskId, msg),
        () => { self._respawn(taskId, agentType).catch(console.error); }
      );
      await client.connect();
      const session = { agentId: agent.agentId, taskId, host: agent.host, port: agent.port, client };
      this.sessions.set(taskId, session);
      return session;
    } catch (err) { console.error(`[MeshRuntime] Spawn failed: ${err}`); return null; }
  }

  /** @param {string} taskId @param {string} agentType */
  async _respawn(taskId, agentType) {
    const s = await this.spawn(agentType, taskId);
    if (!s) console.log(`[MeshRuntime] Respawn failed for ${taskId}`);
  }

  /** @param {string} sk @param {any} msg */
  _pushEvent(sk, msg) {
    const queue = this.eventQueues.get(sk) ?? [];
    queue.push(msg); this.eventQueues.set(sk, queue);
    const listener = this.eventQueueListeners.get(sk);
    if (listener) { this.eventQueueListeners.delete(sk); listener(msg); }
  }

  /** @param {string} sk @returns {AsyncGenerator} */
  async *events(sk) {
    const queue = this.eventQueues.get(sk) ?? [];
    this.eventQueues.set(sk, queue);
    while (true) {
      while (queue.length > 0) yield queue.shift();
      yield await new Promise((resolve) => { this.eventQueueListeners.set(sk, resolve); });
    }
  }

  /** @param {string} sk @param {any} msg */
  send(sk, msg) { this.sessions.get(sk)?.client.send(msg); }
  /** @param {string} sk */
  close(sk) {
    this.sessions.get(sk)?.client.close();
    this.sessions.delete(sk);
    this.eventQueues.delete(sk);
    this.eventQueueListeners.delete(sk);
  }
}

// ---------------------------------------------------------------------------
// Event conversion
// ---------------------------------------------------------------------------

/** @param {any} msg @returns {any[]} */
function meshEventToAcpEvents(msg) {
  const events = [];
  switch (msg.type) {
    case "result": {
      if (msg.error) events.push({ type: "error", message: msg.error, code: "agent_error", retryable: false });
      else if (msg.chunk) events.push({ type: "text_delta", text: msg.chunk });
      else if (msg.final) events.push({ type: "text_delta", text: msg.final });
      // Always emit done for result — fixes turn completion race when worker doesn't send done flag
      events.push({ type: "done", stopReason: "stop" });
      break;
    }
    case "agent_message":
    case "message": {
      if (msg.content?.length) {
        for (const b of msg.content) {
          if (b.type === "text" && b.text) events.push({ type: "text_delta", text: b.text });
        }
      }
      break;
    }
    case "error":
    case "error_response": {
      events.push({ type: "error", message: msg.message ?? "Unknown", code: msg.code ?? "agent_error", retryable: false });
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
  constructor() {
    /** @type {Map<string, MeshRuntime>} */
    this.meshBySession = new Map();
  }

  /** @param {{ sessionKey: string, agent: string, mode?: string }} input */
  async ensureSession(input) {
    const { sessionKey, agent: agentType } = input;
    const registryUrl = process.env.MESH_REGISTRY_URL ?? "ws://192.168.1.206:9000";
    const mesh = new MeshRuntime({ registry: registryUrl });
    const session = await mesh.spawn(agentType, sessionKey);
    if (!session) throw new Error(`[mesh] no agent for type=${agentType}`);
    this.meshBySession.set(sessionKey, mesh);
    return {
      sessionKey,
      backend: BACKEND_ID,
      runtimeSessionName: sessionKey,
      agentSessionId: session.agentId,
    };
  }

  /** @param {{ handle?: { sessionKey?: string }, text?: string, attachments?: any[], mode?: string }} input */
  async *runTurn(input) {
    const sk = input.handle?.sessionKey ?? "default";
    const mesh = this.meshBySession.get(sk);
    if (!mesh) { yield { type: "error", message: `No mesh session: ${sk}` }; return; }
    const msg = { type: "execute", taskId: sk, sessionKey: sk, prompt: input.text ?? "" };
    mesh.send(sk, msg);
    try {
      for await (const ev of mesh.events(sk)) {
        for (const e of meshEventToAcpEvents(ev)) yield e;
        if (ev.type === "done" || ev.done) return;
      }
    } finally {
      mesh.close(sk);
      this.meshBySession.delete(sk);
    }
  }

  /** @param {{ handle: { sessionKey: string } }} input */
  async cancel(input) {
    const m = this.meshBySession.get(input.handle.sessionKey);
    if (m) { m.close(input.handle.sessionKey); this.meshBySession.delete(input.handle.sessionKey); }
  }

  /** @param {{ handle: { sessionKey: string }, reason: string }} input */
  async close(input) {
    const m = this.meshBySession.get(input.handle.sessionKey);
    if (m) { m.close(input.handle.sessionKey); this.meshBySession.delete(input.handle.sessionKey); }
  }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

function registerMeshRuntime(config) {
  if (config?.registry) process.env.MESH_REGISTRY_URL = config.registry;
  if (config?.defaultAgentType) process.env.MESH_DEFAULT_AGENT_TYPE = config.defaultAgentType;
  registerAcpRuntimeBackend({ id: BACKEND_ID, runtime: new MeshBridgeRuntime(), healthy: () => true });
  console.log(`[agent-mesh] Registered AcpRuntimeBackend "${BACKEND_ID}" (registry=${process.env.MESH_REGISTRY_URL})`);
}

// ---------------------------------------------------------------------------
// Plugin entry point
// ---------------------------------------------------------------------------

const plugin = {
  id: "agent-mesh",
  name: "Agent Mesh Bridge",
  description: "Routes ACP sessions through the agent mesh to remote workers.",

  configSchema() {
    return {
      type: "object",
      properties: {
        registry: { type: "string", default: "ws://192.168.1.206:9000" },
        defaultAgentType: { type: "string", default: "mock-hermes" },
      },
    };
  },

  register(api) {
    const cfg = api.pluginConfig ?? {};
    const registry = cfg.registry ?? process.env.MESH_REGISTRY_URL ?? "ws://192.168.1.206:9000";
    const defaultAgentType = cfg.defaultAgentType ?? process.env.MESH_DEFAULT_AGENT_TYPE ?? "mock-hermes";

    api.registerService({
      id: "agent-mesh-runtime",

      async start(ctx) {
        ctx.logger?.info(`[agent-mesh] Starting (registry=${registry})`);
        try {
          registerMeshRuntime({ registry, defaultAgentType });
          ctx.logger?.info("[agent-mesh] Mesh backend registered as 'mesh'");
        } catch (err) {
          ctx.logger?.error(`[agent-mesh] Registration failed: ${err}`);
        }
      },

      async stop() {},
    });
  },
};

export default plugin;