import WebSocket from "ws";
import type { AcpMessage } from "@agent-mesh/shared/src/protocol.js";

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
      this.ws.on("message", (data) => {
        try {
          const msg = JSON.parse(data.toString()) as AcpMessage;
          this.onMessage(msg);
        } catch {}
      });
      this.ws.on("close", () => { this.onClose(); this.attemptReconnect(); });
    });
  }

  send(msg: AcpMessage): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  private attemptReconnect(): void {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.log(`[BridgeClient] Max reconnect attempts reached for ${this.host}:${this.port}`);
      return;
    }
    this.reconnectAttempts++;
    const delay = this.reconnectDelayMs * this.reconnectAttempts;
    console.log(`[BridgeClient] Reconnecting to ${this.host}:${this.port} in ${delay}ms (attempt ${this.reconnectAttempts})`);
    setTimeout(() => this.connect().catch(() => {}), delay);
  }

  close(): void {
    this.ws?.close();
  }
}
