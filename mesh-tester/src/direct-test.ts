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

const res = await rpc(ws, "discover", { capabilities: { agentType: "mock-hermes" }, limit: 10 }) as JsonRpcResponse & { result: { agents: AgentInfo[] } };
const agents: AgentInfo[] = res.result?.agents ?? [];
console.log(`Found ${agents.length} agent(s)`);

if (!agents.length) {
  console.error("No agents found!");
  process.exit(1);
}

const agent = agents[0];
console.log(`Connecting to ${agent.agentId} @ ${agent.host}:${agent.port}...`);

const taskId = `test-${Date.now()}`;
const prompt = "hello from direct test";

// Connect directly to worker
const workerWs = await new Promise<WebSocket>((resolve, reject) => {
  const ws = new WebSocket(`ws://${agent.host}:${agent.port}`);
  ws.on("open", () => resolve(ws));
  ws.on("error", reject);
});

console.log(`Sending task dispatch: taskId=${taskId} prompt="${prompt}"`);
workerWs.send(JSON.stringify({ type: "task", taskId, prompt, sessionKey: "direct-test" }));

const timeout = setTimeout(() => {
  console.log("TIMEOUT — no response in 30s");
  workerWs.close();
  process.exit(1);
}, 30_000);

workerWs.on("message", (data) => {
  const raw = data.toString();
  console.log(`[recv] ${raw.slice(0, 400)}`);
  try {
    const msg = JSON.parse(raw);
    if (msg.final !== undefined) {
      console.log(`\n✅ FINAL RESULT: ${msg.final}`);
      clearTimeout(timeout);
      workerWs.close();
    }
  } catch {}
});

workerWs.on("close", () => {
  console.log("Worker WebSocket closed");
  clearTimeout(timeout);
});