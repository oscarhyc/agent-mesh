import { WebSocketServer, WebSocket } from "ws";
import { createServer } from "http";
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { Registry } from "./registry.js";
import type {
  JsonRpcRequest,
  JsonRpcResponse,
  WorkerRegisterParams,
  TaskResultParams,
  DeregisterParams,
  DiscoverParams,
  PingRequest,
} from "@agent-mesh/shared/src/protocol.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.REGISTRY_PORT ?? 9000);
const HTTP_PORT = Number(process.env.REGISTRY_HTTP_PORT ?? 9001);

const registry = new Registry();
const clients = new Set<WebSocket>();
const wsToAgent = new Map<WebSocket, string>();

// --- WebSocket server ---
const wss = new WebSocketServer({ port: PORT });
wss.on("connection", (ws: WebSocket, req) => {
  const ip = req.socket.remoteAddress ?? "unknown";
  const clientId = Math.floor(Math.random() * 999999);
  console.log(`[Registry] WS connect from ${ip}, id=${clientId}, total=${clients.size + 1}`);
  clients.add(ws);
  (ws as any).clientId = clientId;
  console.log(`[Registry] wsToAgent map size=${wsToAgent.size}`);

  ws.on("message", (data) => {
    const raw = data.toString();
    let method = "unknown";
    let isRequest = false;
    try {
      const parsed = JSON.parse(raw);
      // JsonRpcRequest has "method" field, JsonRpcResponse has "result" or "error"
      if (parsed.method !== undefined) {
        isRequest = true;
        method = String(parsed.method);
      } else {
        method = parsed.result ? "response" : "error";
      }
    } catch {}
    console.log(`[Registry] MSG from client_id=${(ws as any).clientId} isRequest=${isRequest} method=${method} len=${raw.length}`);
    try {
      const msg = JSON.parse(raw);
      if (isRequest) {
        handleRequest(ws, msg as JsonRpcRequest);
      } else {
        handleResponse(ws, msg as JsonRpcResponse);
      }
    } catch (err) {
      console.error("[Registry] bad message:", err);
    }
  });

  ws.on("close", () => { console.log(`[Registry] WS closed, removing agent=${wsToAgent.get(ws)}`); clients.delete(ws); wsToAgent.delete(ws); });
  ws.on("error", (err) => { console.error("[Registry] WS error:", err); clients.delete(ws); wsToAgent.delete(ws); });
});

function send(ws: WebSocket, msg: JsonRpcResponse): void {
  if (ws.readyState === WebSocket.OPEN) {
    try {
      ws.send(JSON.stringify(msg));
    } catch (err) {
      console.error(`[Registry] send error on client_id=${(ws as any).clientId}: ${err}`);
    }
  } else {
    console.log(`[Registry] send skipped — ws closed, client_id=${(ws as any).clientId}, state=${ws.readyState}`);
  }
}

function handleRequest(ws: WebSocket, msg: JsonRpcRequest): void {
  switch (msg.method) {
    case "register": {
      const params = msg.params as WorkerRegisterParams;
      const info = registry.register(params);
      registry.recordTaskStart(params.agentId); // set load=1
      registry.recordTaskDone(params.agentId);   // back to 0 — means registered and ready
      wsToAgent.set(ws, params.agentId);
      console.log(`[Registry] wsToAgent mapped ws=${ws} -> agentId=${params.agentId} (total ${wsToAgent.size})`);
      send(ws, { jsonrpc: "2.0", result: info, id: msg.id });
      break;
    }
    case "deregister": {
      const params = msg.params as DeregisterParams;
      const agentId = wsToAgent.get(ws);
      if (agentId) {
        registry.deregister(agentId, params.reason);
        wsToAgent.delete(ws);
      }
      send(ws, { jsonrpc: "2.0", result: { deregistered: true }, id: msg.id });
      break;
    }
    case "discover": {
      const params = msg.params as DiscoverParams;
      const agents = registry.discover(params.capabilities ?? {}, params.limit ?? 1);
      send(ws, { jsonrpc: "2.0", result: { agents }, id: msg.id });
      break;
    }
    case "pong": {
      const agentId = wsToAgent.get(ws);
      const cid = (ws as any).clientId ?? "unknown";
      console.log(`[Registry] pong from client_id=${cid} ws=${wsToAgent.has(ws) ? wsToAgent.get(ws) : "UNMAPPED"} (clients=${clients.size})`);
      if (agentId) {
        registry.refreshHeartbeat(agentId);
        console.log(`[Registry] heartbeat refreshed for ${agentId}`);
      } else {
        console.log(`[Registry] pong from unmapped client — not in wsToAgent (size=${wsToAgent.size})`);
      }
      send(ws, { jsonrpc: "2.0", result: { ok: true }, id: msg.id });
      break;
    }
    case "result": {
      // Task result reporting — decrement agent load
      const agentId = wsToAgent.get(ws);
      if (agentId) registry.recordTaskDone(agentId);
      send(ws, { jsonrpc: "2.0", result: { received: true }, id: msg.id });
      break;
    }
    default:
      send(ws, {
        jsonrpc: "2.0",
        error: { code: -32601, message: `Method not found: ${msg.method}` },
        id: msg.id,
      });
  }
}

// --- Ping all connected workers every 10s ---
setInterval(() => {
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      const ping: PingRequest = { jsonrpc: "2.0", method: "ping", params: {}, id: Date.now() };
      client.send(JSON.stringify(ping));
    }
  }
}, 10_000);

// --- HTTP server (serves dashboard at /) ---
const httpServer = createServer((req, res) => {
  if (req.url === "/" || req.url === "/index.html") {
    try {
      const html = readFileSync(join(__dirname, "../public/index.html"));
      res.writeHead(200, { "Content-Type": "text/html" });
      res.end(html);
    } catch {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("Agent Mesh Registry running. Dashboard not found.");
    }
  } else {
    res.writeHead(404);
    res.end("Not found");
  }
});

function handleResponse(ws: WebSocket, msg: JsonRpcResponse): void {
  // Handle async responses. For now, detect pong responses and refresh heartbeat.
  if (msg.result && typeof msg.result === "object") {
    const r = msg.result as Record<string, unknown>;
    if (r.pong === true) {
      const agentId = wsToAgent.get(ws);
      const cid = (ws as any).clientId ?? "unknown";
      if (agentId) {
        registry.refreshHeartbeat(agentId);
        console.log(`[Registry] heartbeat refreshed for ${agentId} (from pong response client_id=${cid})`);
      } else {
        console.log(`[Registry] pong from unmapped client_id=${cid} — not in wsToAgent`);
      }
      return;
    }
  }
  // Ignore other responses (e.g. discover results, result acks)
  const resp = msg.result ?? msg.error ?? {};
  console.log(`[Registry] unhandled response: ${JSON.stringify(resp).slice(0, 100)}`);
}

httpServer.listen(HTTP_PORT, () => {
  console.log(`[Registry] HTTP dashboard: http://localhost:${HTTP_PORT}/`);
  console.log(`[Registry] WebSocket port: ${PORT}`);
});
