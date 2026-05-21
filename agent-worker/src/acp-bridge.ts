import type { WebSocket } from "ws";
import type { AcpMessage } from "@agent-mesh/shared/src/protocol.js";
import { AgentSubprocess } from "./subprocess.js";

interface TaskPayload {
  taskId: string;
  prompt: string;
  sessionKey?: string;
}

export class AcpBridge {
  private taskTimeout: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private ws: WebSocket,
    private subprocess: AgentSubprocess
  ) {
    console.log("[AcpBridge] created, starting subprocess");

    // Incoming ACP messages from subprocess stdout → forward to WS client
    const origHandler = this.subprocess.onMessage;
    this.subprocess.onMessage = (msg: AcpMessage) => {
      console.log(`[AcpBridge] subprocess.onMessage: ${JSON.stringify(msg).slice(0, 200)}`);
      // Forward to WS client as-is
      this.ws.send(JSON.stringify(msg));
      origHandler(msg);
    };

    subprocess.onExit = (code, signal) => {
      console.log(`[AcpBridge] subprocess exited — code=${code} signal=${signal}`);
    };

    // Incoming messages from WebSocket client → route to subprocess
    ws.on("message", (data) => {
      const raw = data.toString();
      console.log(`[AcpBridge] WS message received (${Buffer.byteLength(raw, 'utf8')}B): ${raw.slice(0, 200)}`);
      try {
        const msg = JSON.parse(raw) as Record<string, unknown>;

        if (msg["type"] === "task") {
          const payload = msg as unknown as TaskPayload;
          const taskId = payload.taskId ?? `mesh-${Date.now()}`;
          const prompt = payload.prompt ?? "";

          console.log(`[AcpBridge] task dispatch — taskId=${taskId} prompt="${prompt.slice(0, 80)}"`);

          // Clear any stale timeout
          if (this.taskTimeout) { clearTimeout(this.taskTimeout); this.taskTimeout = null; }

          // Set response timeout (60s)
          this.taskTimeout = setTimeout(() => {
            console.log(`[AcpBridge] task ${taskId} timed out after 60s`);
          }, 60_000);

          // Format as ACP execute message and send to subprocess stdin
          const acpMsg: AcpMessage = {
            type: "execute",
            taskId,
            prompt,
            sessionKey: payload.sessionKey ?? "mesh-test",
          };
          console.log(`[AcpBridge] sending to subprocess stdin: ${JSON.stringify(acpMsg)}`);
          this.subprocess.send(acpMsg);
        } else {
          console.log(`[AcpBridge] raw ACP forwarding: type=${msg["type"]}`);
          const acpMsg = msg as unknown as AcpMessage;
          this.subprocess.send(acpMsg);
        }
      } catch (err) {
        console.error(`[AcpBridge] bad message from WS: ${err}`);
      }
    });

    ws.on("close", () => {
      console.log("[AcpBridge] WS client disconnected");
      if (this.taskTimeout) clearTimeout(this.taskTimeout);
      subprocess.kill();
    });

    ws.on("error", (err) => {
      console.error("[AcpBridge] WS error:", err);
    });
  }

  start(): void {
    console.log("[AcpBridge] start() called");
    this.subprocess.start();
  }
}