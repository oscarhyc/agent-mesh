import type { WebSocket } from "ws";
import type { AcpMessage } from "@agent-mesh/shared/src/protocol.js";
import { AgentSubprocess } from "./subprocess.js";

export class AcpBridge {
  constructor(
    private ws: WebSocket,
    private subprocess: AgentSubprocess
  ) {
    subprocess.onMessage = (msg: AcpMessage) => {
      this.ws.send(JSON.stringify(msg));
    };

    ws.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString()) as AcpMessage;
        subprocess.send(msg);
      } catch {}
    });

    ws.on("close", () => {
      console.log("[AcpBridge] WebSocket closed, killing subprocess");
      subprocess.kill();
    });
  }

  start(): void {
    this.subprocess.start();
  }
}