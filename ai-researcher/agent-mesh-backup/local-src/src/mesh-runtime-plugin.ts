// @ts-nocheck
/**
 * Mesh Runtime — OpenClaw AcpRuntime backend for the agent mesh.
 * Registers as "mesh" AcpRuntimeBackend via registerMeshBridgeRuntime().
 * Used by agent-mesh-plugin as a service inside the OpenClaw gateway process.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Minimal AcpMessage - sent to/received from worker WebSocket */
interface AcpMessage { type: string; [key: string]: unknown }
/** Agent info returned from registry discover */
interface AgentInfo { agentId: string; host: string; port: number; capabilities?: { agentType?: string } }
/** Internal event for async generator queue */
interface MeshEvent { type: string; [key: string]: unknown }

// ---------------------------------------------------------------------------
// Registry Client — connects to LAN registry WebSocket
// ---------------------------------------------------------------------------

import WebSocket from "ws";

interface JsonRpcRequest { jsonrpc: "2.0"; method: string; params: Record<string, unknown>; id: number }
interface JsonRpcResponse { jsonrpc: string; id?: number; result?: unknown; error?: { code: number; message: string } }

export class RegistryClient {
  private ws: WebSocket | null = null;
  private pending = new Map<number, (res: JsonRpcResponse) => void>();
  private nextId = 1;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(private registryUrl: string) {}

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.registryUrl);
      this.ws.on("open", () => resolve());
      this.ws.on("error", reject);
      this.ws.on("message", (data) => {
        try {
          const msg = JSON.parse(data.toString()) as JsonRpcResponse;
          const cb = this.pending.get(msg.id as number);
          if (cb) { cb(msg); this.pending.delete(msg.id as number); }
        } catch {}
      });
      this.ws.on("close", () => this.scheduleReconnect());
    });
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(async () => {
      this.reconnectTimer = null;
      try { await this.connect(); } catch {}
    }, 5000);
  }

  private async call(method: string, params: Record<string, unknown>): Promise<JsonRpcResponse> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) await this.connect();
    return new Promise((resolve, reject) => {
      const id = this.nextId++;
      const msg: JsonRpcRequest = { jsonrpc: "2.0", method, params, id };
      this.pending.set(id, resolve as (res: JsonRpcResponse) => void);
      this.ws!.send(JSON.stringify(msg));
      setTimeout(() => {
        if (this.pending.has(id)) { this.pending.delete(id); reject(new Error(`RPC ${method} timed out`)); }
      }, 15000);
    });
  }

  async discover(criteria: Partial<{ agentType: string }>, limit = 1): Promise<AgentInfo[]> {
    const res = await this.call("discover", { capabilities: criteria, limit }) as JsonRpcResponse & { result: { agents: AgentInfo[] } };
    return res.result?.agents ?? [];
  }

  close(): void {
    this.ws?.close();
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.pending.clear();
  }
}

// ---------------------------------------------------------------------------
// Bridge Client — direct WS connection to a worker
// ---------------------------------------------------------------------------

export class BridgeClient {
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private readonly maxReconnectAttempts = 5;
  private readonly reconnectDelayMs = 1000;

  constructor(
    private host: string,
    private port: number,
    private onMessage: (msg: AcpMessage) => void,
    private onClose: () => void,
  ) {}

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(`ws://${this.host}:${this.port}`);
      this.ws.on("open", () => { this.reconnectAttempts = 0; resolve(); });
      this.ws.on("error", reject);
      this.ws.on("message", (data) => { try { this.onMessage(JSON.parse(data.toString())); } catch {} });
      this.ws.on("close", () => { this.onClose(); this.attemptReconnect(); });
    });
  }

  send(msg: AcpMessage): void {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(msg));
  }

  private attemptReconnect(): void {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) return;
    this.reconnectAttempts++;
    setTimeout(() => this.connect().catch(() => {}), this.reconnectDelayMs * this.reconnectAttempts);
  }

  close(): void { this.ws?.close(); }
}

// ---------------------------------------------------------------------------
// Mesh Runtime
// ---------------------------------------------------------------------------

interface MeshSession {
  agentId: string;
  taskId: string;
  host: string;
  port: number;
  client: BridgeClient;
}

export class MeshRuntime {
  private registry: RegistryClient;
  private sessions = new Map<string, MeshSession>();
  private eventQueues = new Map<string, MeshEvent[]>();
  private eventQueueListeners = new Map<string, (ev: MeshEvent) => void>();

  constructor(private config: { registry: string; defaultAgentType?: string }) {
    this.registry = new RegistryClient(config.registry);
  }

  async spawn(agentType: string, taskId: string): Promise<MeshSession | null> {
    const agents = await this.registry.discover({ agentType }, 1);
    if (!agents.length) { console.log(`[MeshRuntime] No agents for type=${agentType}`); return null; }
    const agent = agents[0];
    console.log(`[MeshRuntime] Spawning task=${taskId} on ${agent.agentId} (${agent.host}:${agent.port})`);
    return new Promise((resolve, reject) => {
      const client = new BridgeClient(
        agent.host, agent.port,
        (msg) => this.pushEvent(taskId, msg),
        () => { this.respawn(taskId, agentType).then(resolve).catch(reject); }
      );
      client.connect().then(() => {
        this.sessions.set(taskId, { agentId: agent.agentId, taskId, host: agent.host, port: agent.port, client });
        resolve({ agentId: agent.agentId, taskId, host: agent.host, port: agent.port, client });
      }).catch(reject);
    });
  }

  private async respawn(taskId: string, agentType: string): Promise<MeshSession | null> {
    return this.spawn(agentType, taskId);
  }

  private pushEvent(sk: string, msg: AcpMessage): void {
    const queue = this.eventQueues.get(sk) ?? [];
    queue.push(msg as MeshEvent);
    this.eventQueues.set(sk, queue);
    const listener = this.eventQueueListeners.get(sk);
    if (listener) { this.eventQueueListeners.delete(sk); listener(msg as MeshEvent); }
  }

  async *events(sk: string): AsyncGenerator<MeshEvent> {
    const queue = this.eventQueues.get(sk) ?? [];
    this.eventQueues.set(sk, queue);
    while (true) {
      while (queue.length > 0) yield queue.shift()!;
      yield await new Promise<MeshEvent>((resolve) => { this.eventQueueListeners.set(sk, resolve); });
    }
  }

  send(sk: string, msg: AcpMessage): void { this.sessions.get(sk)?.client.send(msg); }
  close(sk: string): void {
    this.sessions.get(sk)?.client.close();
    this.sessions.delete(sk);
    this.eventQueues.delete(sk);
    this.eventQueueListeners.delete(sk);
  }
}

// ---------------------------------------------------------------------------
// AcpRuntime interface implementation
// ---------------------------------------------------------------------------

const BACKEND_ID = "mesh";

export class MeshBridgeRuntime {
  private meshBySession = new Map<string, MeshRuntime>();

  async ensureSession(input: { sessionKey: string; agent: string; mode?: string }): Promise<any> {
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

  async *runTurn(input: { handle?: { sessionKey?: string }; text?: string; attachments?: any[]; mode?: string }): AsyncIterable<any> {
    const sk = input.handle?.sessionKey ?? "default";
    const mesh = this.meshBySession.get(sk);
    if (!mesh) { yield { type: "error", message: `No mesh session: ${sk}` }; return; }

    const msg: AcpMessage = { type: "execute", taskId: sk, sessionKey: sk, prompt: input.text ?? "" };
    mesh.send(sk, msg);

    try {
      for await (const ev of mesh.events(sk)) {
        for (const e of meshEventToAcpEvents(ev)) yield e;
        if (ev.type === "done" || (ev as any).done) return;
      }
    } finally {
      mesh.close(sk);
      this.meshBySession.delete(sk);
    }
  }

  async cancel(input: { handle: { sessionKey: string } }): Promise<void> {
    const m = this.meshBySession.get(input.handle.sessionKey);
    if (m) { m.close(input.handle.sessionKey); this.meshBySession.delete(input.handle.sessionKey); }
  }

  async close(input: { handle: { sessionKey: string }; reason: string }): Promise<void> {
    const m = this.meshBySession.get(input.handle.sessionKey);
    if (m) { m.close(input.handle.sessionKey); this.meshBySession.delete(input.handle.sessionKey); }
  }
}

// ---------------------------------------------------------------------------
// Event conversion: MeshEvent → OpenClaw AcpRuntimeEvent
// ---------------------------------------------------------------------------

function meshEventToAcpEvents(msg: MeshEvent): any[] {
  const events = [];
  switch (msg.type) {
    case "result": {
      const r = msg as { done?: boolean; final?: string; chunk?: string; error?: string };
      if (r.error) events.push({ type: "error", message: r.error, code: "agent_error", retryable: false });
      else if (r.chunk) events.push({ type: "text_delta", text: r.chunk });
      else if (r.final) events.push({ type: "text_delta", text: r.final });
      if (r.done) events.push({ type: "done", stopReason: "stop" });
      break;
    }
    case "agent_message":
    case "message": {
      const am = msg as { content?: Array<{ type: string; text?: string }> };
      if (am.content?.length) {
        for (const b of am.content) {
          if (b.type === "text" && b.text) events.push({ type: "text_delta", text: b.text });
        }
      }
      break;
    }
    case "error":
    case "error_response": {
      const e = msg as { message?: string; code?: string };
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
// Registration function — call this to register as an AcpRuntimeBackend
// ---------------------------------------------------------------------------

export async function registerMeshBridgeRuntime(config?: { registry?: string; defaultAgentType?: string }): Promise<void> {
  if (config?.registry) process.env.MESH_REGISTRY_URL = config.registry;
  if (config?.defaultAgentType) process.env.MESH_DEFAULT_AGENT_TYPE = config.defaultAgentType;

  // Dynamic import to avoid path resolution issues
  const mod = await import("openclaw/plugin-sdk/acp-runtime") as any;
  const register: (b: any) => void = mod.registerAcpRuntimeBackend ?? mod.default?.registerAcpRuntimeBackend;
  if (!register) throw new Error("Cannot find registerAcpRuntimeBackend in openclaw/plugin-sdk/acp-runtime");

  register({ id: BACKEND_ID, runtime: new MeshBridgeRuntime(), healthy: () => true });
  console.log(`[agent-mesh-bridge] Registered AcpRuntimeBackend "${BACKEND_ID}" (registry=${process.env.MESH_REGISTRY_URL})`);
}