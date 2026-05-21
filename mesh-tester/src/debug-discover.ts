import WebSocket from "ws";
import type { JsonRpcRequest, JsonRpcResponse, AgentInfo } from "../shared/src/protocol.js";

const REGISTRY_URL = process.env.REGISTRY_URL ?? "ws://localhost:9000";

async function rpc(ws: WebSocket, method: string, params: Record<string, unknown>): Promise<JsonRpcResponse> {
  return new Promise((resolve, reject) => {
    const id = Date.now();
    const msg: JsonRpcRequest = { jsonrpc: "2.0", method, params, id };
    const timeout = setTimeout(() => reject(new Error(`RPC ${method} timed out`)), 10_000);
    const handler = (data: Buffer) => {
      try {
        const res = JSON.parse(data.toString()) as JsonRpcResponse;
        if (res.id === id) { clearTimeout(timeout); ws.off("message", handler); resolve(res); }
      } catch {}
    };
    ws.on("message", handler);
    ws.send(JSON.stringify(msg));
  });
}

const ws = await new Promise<WebSocket>((resolve, reject) => {
  const ws = new WebSocket(REGISTRY_URL);
  ws.on("open", () => resolve(ws));
  ws.on("error", reject);
});

console.log("=== Raw discover call ===");
const res = await rpc(ws, "discover", { capabilities: { agentType: "mock-hermes" }, limit: 10 }) as JsonRpcResponse & { result: { agents: AgentInfo[] } };
console.log("Full response:", JSON.stringify(res, null, 2));
console.log("\nAgents found:", res.result?.agents?.length ?? 0);
for (const a of res.result?.agents ?? []) {
  console.log(`  - ${a.agentId} | type=${a.capabilities.agentType} | host=${a.host}:${a.port} | load=${a.load}`);
}

ws.close();