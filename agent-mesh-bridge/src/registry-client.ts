import WebSocket from "ws";
import type { JsonRpcRequest, JsonRpcResponse, AgentInfo } from "@agent-mesh/shared/src/protocol.js";

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
      this.ws.on("close", () => { this.scheduleReconnect(); });
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
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      await this.connect();
    }
    return new Promise((resolve, reject) => {
      const id = this.nextId++;
      const msg: JsonRpcRequest = { jsonrpc: "2.0", method, params, id };
      this.pending.set(id, resolve);
      this.ws!.send(JSON.stringify(msg));
      setTimeout(() => {
        if (this.pending.has(id)) {
          this.pending.delete(id);
          reject(new Error(`RPC ${method} timed out`));
        }
      }, 15_000);
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
