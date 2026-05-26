import type { WebSocket } from "ws";
import type { AcpMessage } from "@agent-mesh/shared/src/protocol.js";
import { AgentSubprocess } from "./subprocess.js";

/**
 * ACP JSON-RPC message types.
 */
interface JsonRpcMessage {
  jsonrpc: "2.0";
  method?: string;
  params?: Record<string, unknown>;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
  id?: number | string;
  type?: string;
}

type AcpBridgeState = "idle" | "initializing" | "authenticating" | "ready" | "done";

interface TaskPayload {
  taskId: string;
  prompt: string;
  sessionKey?: string;
}

export class AcpBridge {
  private state: AcpBridgeState = "idle";
  private sessionId: string | null = null;
  private currentTaskId: string | null = null;
  private pendingRpcId = 1;
  private pendingRpc = new Map<number, (res: JsonRpcMessage) => void>();
  private taskTimeout: ReturnType<typeof setTimeout> | null = null;
  private pendingTaskInterval: ReturnType<typeof setInterval> | null = null;
  private idleTimer: ReturnType<typeof setTimeout> | null = null;
  private idleTimeoutMs: number;
  private ws: WebSocket;
  private subprocess: AgentSubprocess;

  constructor(ws: WebSocket, subprocess: AgentSubprocess, idleTimeoutMs = 30000) {
    this.idleTimeoutMs = idleTimeoutMs;
    this.ws = ws;
    this.subprocess = subprocess;

    // Incoming messages from subprocess stdout
    this.subprocess.onMessage = (msg: AcpMessage) => {
      const raw = JSON.stringify(msg);
      console.log("[AcpBridge] <- subprocess: " + raw.slice(0, 200));
      try {
        const parsed = JSON.parse(raw) as JsonRpcMessage;
        this.handleSubprocessMessage(parsed);
      } catch {
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
        const msg = JSON.parse(raw) as Record<string, unknown>;
        const type = msg["type"] as string | undefined;
        if (type === "task" || type === "execute") {
          const taskId = (msg as Record<string, unknown>)["taskId"] as string | undefined;
          const prompt = (msg as Record<string, unknown>)["prompt"] as string | undefined;
          const params = (msg as Record<string, unknown>)["params"] as Record<string, unknown> | undefined;
          const effectivePrompt = params ? (params["prompt"] as string | undefined) : undefined;
          const finalPrompt = effectivePrompt ?? prompt ?? "";
          this.handleTask({ taskId: taskId ?? "", prompt: finalPrompt });
        } else if (type === "cancel") {
          const tid = (msg as Record<string, unknown>)["taskId"] as string | undefined ?? "";
          this.handleCancel(tid);
        } else {
          // Forward ACP method to hermes subprocess
          const method = msg['method'] as string | undefined;
          const params = msg['params'] as Record<string, unknown> | undefined;
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
      } catch (err) {
        console.error("[AcpBridge] bad WS message: " + err);
      }
    });

    ws.on("close", () => {
      console.log("[AcpBridge] WS client disconnected -- keeping hermes alive for reuse");
      if (this.taskTimeout) clearTimeout(this.taskTimeout);
      if (this.pendingTaskInterval) clearInterval(this.pendingTaskInterval);
      // Start idle timer; if no new task arrives, kill hermes
      this.startIdleTimer();
    });

    ws.on("error", (err) => {
      console.error("[AcpBridge] WS error: " + err);
    });
  }

  async start(): Promise<void> {
    console.log("[AcpBridge] start() -- starting subprocess and ACP handshake");
    this.subprocess.start();
    await this.performAcpHandshake();
  }

  private async performAcpHandshake(): Promise<void> {
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
      if ((authRes as Record<string, unknown>)["error"]) {
        const errObj = (authRes as Record<string, unknown>)["error"] as Record<string, unknown>;
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
      const sid = (sessionRes as Record<string, unknown>)["result"] &&
        typeof (sessionRes as Record<string, unknown>)["result"] === "object"
          ? ((sessionRes as Record<string, unknown>)["result"] as Record<string, unknown>)["sessionId"] as string
          : undefined;
      if (!sid) {
        throw new Error("new_session returned no sessionId: " + JSON.stringify(sessionRes));
      }
      this.sessionId = sid;
      console.log("[AcpBridge] <- new_session ok, sessionId=" + this.sessionId);

      this.state = "ready";
      console.log("[AcpBridge] ACP handshake complete -- ready for tasks");
    } catch (err) {
      console.error("[AcpBridge] ACP handshake failed: " + err);
      this.ws.send(JSON.stringify({ type: "error", message: "ACP handshake failed: " + err }));
      this.subprocess.kill();
      throw err;
    }
  }

  private sendRpc(method: string, params: Record<string, unknown>): Promise<JsonRpcMessage> {
    return new Promise((resolve, reject) => {
      const id = this.pendingRpcId++;
      const msg: JsonRpcMessage = { jsonrpc: "2.0", method, params, id };
      this.pendingRpc.set(id, (res) => {
        if ((res as Record<string, unknown>)["error"]) {
          reject(new Error("RPC " + method + " error: " + JSON.stringify((res as Record<string, unknown>)["error"])));
        } else {
          resolve(res);
        }
      });
      const wire = JSON.stringify(msg);
      console.log("[AcpBridge] -> RPC " + method + " id=" + id + ": " + wire.slice(0, 120));
      const sent = this.subprocess.send({ jsonrpc: "2.0", method, params, id } as unknown as AcpMessage);
      console.log("[AcpBridge] subprocess.send returned: " + sent);
      setTimeout(() => {
        if (this.pendingRpc.delete(id)) {
          reject(new Error("RPC " + method + " timed out after 30s"));
        }
      }, 30_000);
    });
  }

  private handleSubprocessMessage(msg: JsonRpcMessage): void {
    const isNotification = msg.id === undefined && (
      msg.type === "session_update" ||
      (msg as Record<string, unknown>)["method"] as string | undefined === "session/update"
    );
    if (isNotification) {
      this.handleSessionUpdate(msg);
      return;
    }
    if (msg.id !== undefined) {
      // Bug fix: parseInt can return NaN for non-numeric strings - handle it safely
      let id: number;
      if (typeof msg.id === "string") {
        const parsed = parseInt(msg.id, 10);
        if (isNaN(parsed)) {
          console.log("[AcpBridge] ignoring RPC response with non-numeric id: " + msg.id);
          return;
        }
        id = parsed;
      } else {
        id = msg.id as number;
      }
      const resolver = this.pendingRpc.get(id);
      if (resolver) {
        this.pendingRpc.delete(id);
        resolver(msg);
      } else {
        console.log("[AcpBridge] unexpected RPC response id=" + id + ": " + JSON.stringify(msg).slice(0, 120));
      }
    }
  }

  private handleSessionUpdate(msg: JsonRpcMessage): void {
    const params = (msg as Record<string, unknown>)["params"] as Record<string, unknown> | undefined;
    if (!params) return;
    const update = params["update"] as Record<string, unknown> | undefined;
    if (!update) return;
    const sessionUpdate = update["sessionUpdate"] as string | undefined;

    if (sessionUpdate === "agent_message_chunk" || sessionUpdate === "agent_thought_chunk") {
      const content = update["content"] as Record<string, unknown> | undefined;
      const text = content ? (content["text"] as string | undefined) : (update["text"] as string | undefined);
      if (text) {
        this.taskTextBuffer += text;
        this.ws.send(JSON.stringify({ type: "chunk", taskId: this.currentTaskId, text }));
      }
    }

    if (sessionUpdate === "agent_message") {
      const content = update["content"] as Array<Record<string, unknown>> | undefined;
      if (content) {
        let finalText = "";
        for (const block of content) {
          if (block["type"] === "text" && typeof block["text"] === "string") {
            finalText = block["text"] as string;
          }
        }
        if (finalText) this.taskTextBuffer = finalText;
      }
    }

    if (sessionUpdate === "usage_update") {
      const size = update["size"] as number | undefined;
      const used = update["used"] as number | undefined;
      if (size !== undefined && used !== undefined) {
        console.log("[AcpBridge] usage: " + used + "/" + size + " tokens");
      }
    }
  }

  private taskTextBuffer = "";

  private handleTask(payload: TaskPayload): void {
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
            if (this.pendingTaskInterval) { clearInterval(this.pendingTaskInterval); this.pendingTaskInterval = null; }
            this.handleTask(payload);
          } else if (this.state === "done" || this.state === "idle") {
            if (this.pendingTaskInterval) { clearInterval(this.pendingTaskInterval); this.pendingTaskInterval = null; }
            this.ws.send(JSON.stringify({ type: "error", message: "bridge closed during handshake", taskId: payload.taskId }));
          }
        }, 1000);
        // Safety timeout
        setTimeout(() => {
          if (this.pendingTaskInterval) { clearInterval(this.pendingTaskInterval); this.pendingTaskInterval = null; }
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

  private async sendTaskPrompt(prompt: string): Promise<void> {
    this.taskTextBuffer = "";
    const rpcRes = await this.sendRpc("session/prompt", {
      sessionId: this.sessionId,
      prompt: [{ type: "text", text: prompt }],
    });
    const res = rpcRes as Record<string, unknown>;
    const text = this.extractFinalText(res);
    console.log("[AcpBridge] task complete -- final text length=" + text.length + ", buffer had " + this.taskTextBuffer.length + " chars");
    this.clearTaskTimeout();
    this.clearIdleTimer();
    this.ws.send(JSON.stringify({ type: "result", taskId: this.currentTaskId, final: text, done: true }));
    await this.endCurrentSession();
    this.currentTaskId = null;
  }

  private async endCurrentSession(): Promise<void> {
    if (!this.sessionId) return;
    try {
      const res = await this.sendRpc("session/end", {
        sessionId: this.sessionId,
        reason: "task_complete",
      });
      console.log("[AcpBridge] session/end ok: " + JSON.stringify(res).slice(0, 100));
    } catch (err) {
      console.warn("[AcpBridge] session/end failed (non-fatal): " + err);
    }
    this.sessionId = null;
    this.startIdleTimer();
  }

  private startIdleTimer(): void {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = setTimeout(() => {
      console.log("[AcpBridge] idle timeout reached -- killing hermes");
      this.subprocess.kill();
    }, this.idleTimeoutMs);
  }

  private clearIdleTimer(): void {
    if (this.idleTimer) { clearTimeout(this.idleTimer); this.idleTimer = null; }
  }

  // Bug fix: require exact taskId match, don't match empty string
  private handleCancel(taskId: string): void {
    if (taskId && this.currentTaskId === taskId) {
      console.log("[AcpBridge] cancelling task " + this.currentTaskId);
      this.clearTaskTimeout();
      if (this.pendingTaskInterval) { clearInterval(this.pendingTaskInterval); this.pendingTaskInterval = null; }
      this.subprocess.send({
        jsonrpc: "2.0",
        method: "session/cancel",
        params: { sessionId: this.sessionId },
        id: undefined,
      } as unknown as AcpMessage);
      this.ws.send(JSON.stringify({ type: "cancelled", taskId: this.currentTaskId }));
      this.currentTaskId = null;
    }
  }

  private clearTaskTimeout(): void {
    if (this.taskTimeout) { clearTimeout(this.taskTimeout); this.taskTimeout = null; }
  }

  private extractFinalText(rpcRes: Record<string, unknown>): string {
    if (typeof rpcRes["text"] === "string") return rpcRes["text"] as string;
    const content = rpcRes["content"] as Array<Record<string, unknown>> | undefined;
    if (Array.isArray(content)) {
      for (const block of content) {
        if (block["type"] === "text" && typeof block["text"] === "string") {
          return block["text"] as string;
        }
      }
    }
    const result = rpcRes["result"];
    if (typeof result === "object" && result !== null) {
      const r = result as Record<string, unknown>;
      if (typeof r["text"] === "string") return r["text"] as string;
      if (Array.isArray(r["content"])) {
        for (const block of r["content"] as Array<Record<string, unknown>>) {
          if (block["type"] === "text" && typeof block["text"] === "string") {
            return block["text"] as string;
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