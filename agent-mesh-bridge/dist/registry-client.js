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
            this.ws.on("close", () => { this.scheduleReconnect(); });
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
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            await this.connect();
        }
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
            }, 15_000);
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
    }
}
