import WebSocket from "ws";
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
            this.ws.on("message", (data) => {
                try {
                    const msg = JSON.parse(data.toString());
                    this.onMessage(msg);
                }
                catch { }
            });
            this.ws.on("close", () => { this.onClose(); this.attemptReconnect(); });
        });
    }
    send(msg) {
        if (this.ws?.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(msg));
        }
    }
    attemptReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.log(`[BridgeClient] Max reconnect attempts reached for ${this.host}:${this.port}`);
            return;
        }
        this.reconnectAttempts++;
        const delay = this.reconnectDelayMs * this.reconnectAttempts;
        console.log(`[BridgeClient] Reconnecting to ${this.host}:${this.port} in ${delay}ms (attempt ${this.reconnectAttempts})`);
        setTimeout(() => this.connect().catch(() => { }), delay);
    }
    close() {
        this.ws?.close();
    }
}
