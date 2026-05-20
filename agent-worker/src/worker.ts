import { WebSocket } from "ws";
import { WebSocketServer } from "ws";
import { networkInterfaces } from "os";
import { AgentSubprocess } from "./subprocess.js";
import { AcpBridge } from "./acp-bridge.js";
import type {
  JsonRpcRequest,
  JsonRpcResponse,
  WorkerRegisterParams,
} from "@agent-mesh/shared/src/protocol.js";

// --- CLI argument parsing ---
function getArg(name: string, fallback: string): string {
  const idx = process.argv.indexOf(`--${name}`);
  if (idx !== -1 && process.argv[idx + 1] !== undefined) {
    return process.argv[idx + 1];
  }
  return fallback;
}

const REGISTRY_URL = getArg("registry", "ws://localhost:9000");
const WORKER_PORT = Number(getArg("worker-port", "9001"));
const AGENT_TYPE = getArg("agent-type", "hermes");
const AGENT_COMMAND = getArg("agent-command", "hermes");
const AGENT_ARGS_STR = getArg("agent-args", "acp");
const AGENT_ARGS = AGENT_ARGS_STR.split(" ").filter(Boolean);
const AGENT_ID = getArg("agent-id", `${AGENT_TYPE}-${Date.now()}`);

console.log(`[Worker] Starting — registry=${REGISTRY_URL} port=${WORKER_PORT} type=${AGENT_TYPE}`);

// --- Discover local IP ---
function getLocalIp(): string {
  const nets = networkInterfaces();
  for (const name of Object.keys(nets)) {
    for (const iface of nets[name]) {
      if (iface.family === "IPv4" && !iface.internal) {
        return iface.address;
      }
    }
  }
  return "127.0.0.1";
}

const LOCAL_IP = getLocalIp();
console.log(`[Worker] Local IP: ${LOCAL_IP}`);

// --- Active bridges ---
const bridges = new Map<WebSocket, AcpBridge>();

function createSubprocess(): AgentSubprocess {
  return new AgentSubprocess(AGENT_COMMAND, AGENT_ARGS);
}

// --- WebSocket server (bridge connects directly here) ---
// Use host 0.0.0.0 to listen on all interfaces (works on both Linux and macOS).
// Linux defaults to localhost-only if not specified; macOS is more permissive.
const workerWss = new WebSocketServer({ host: "0.0.0.0", port: WORKER_PORT });
console.log(`[Worker] WebSocket server listening on ${WORKER_PORT}`);

workerWss.on("connection", (ws: WebSocket) => {
  console.log(`[Worker] Bridge connected from ${ws.remoteAddress}`);
  const subprocess = createSubprocess();
  const bridge = new AcpBridge(ws, subprocess);
  bridges.set(ws, bridge);
  bridge.start();

  ws.on("close", () => { bridges.delete(ws); });
});

// --- Registry event handlers (extracted for re-attach on reconnect) ---
function onRegistryOpen() {
  console.log(`[Worker] Connected to registry ${REGISTRY_URL}`);
  const registerParams: WorkerRegisterParams = {
    agentId: AGENT_ID,
    host: LOCAL_IP,
    port: WORKER_PORT,
    capabilities: {
      agentType: AGENT_TYPE,
      maxConcurrent: 1,
      skills: [],
    },
  };
  const registerMsg: JsonRpcRequest = {
    jsonrpc: "2.0",
    method: "register",
    params: registerParams,
    id: 1,
  };
  registryWs.send(JSON.stringify(registerMsg));
}

function onRegistryMessage(data: Buffer) {
  try {
    const raw = data.toString();
    const msg = JSON.parse(raw) as JsonRpcRequest | JsonRpcResponse;
    if ("method" in msg && msg.method === "ping") {
      try {
        console.log(`[Worker] ping received, id=${msg.id}, ws readyState=${registryWs.readyState}`);
        const pong: JsonRpcResponse = {
          jsonrpc: "2.0",
          result: { pong: true },
          id: msg.id,
        };
        console.log(`[Worker] sending pong, buffered=${registryWs.bufferedAmount}, state=${registryWs.readyState}`);
        if (registryWs.readyState === WebSocket.OPEN) {
          registryWs.send(JSON.stringify(pong));
          console.log(`[Worker] pong sent`);
        } else {
          console.log(`[Worker] skipped pong — ws state=${registryWs.readyState}`);
        }
      } catch (err) {
        console.error(`[Worker] pong send failed: ${err}`);
      }
    }
  } catch (err) {
    console.error(`[Worker] bad registry message: ${err}`);
  }
}

function onRegistryClose() {
  console.log("[Worker] Registry connection lost, retrying in 5s...");
  setTimeout(() => {
    registryWs = new WebSocket(REGISTRY_URL);
    registryWs.on("open", onRegistryOpen);
    registryWs.on("message", onRegistryMessage);
    registryWs.on("close", onRegistryClose);
    registryWs.on("error", onRegistryError);
  }, 5000);
}

function onRegistryError(err: Error) {
  console.error("[Worker] Registry WS error:", err);
}

// --- Connect to registry ---
let registryWs: WebSocket = new WebSocket(REGISTRY_URL);
registryWs.on("open", onRegistryOpen);
registryWs.on("message", onRegistryMessage);
registryWs.on("close", onRegistryClose);
registryWs.on("error", onRegistryError);

// --- Graceful shutdown ---
process.on("SIGTERM", () => {
  console.log("[Worker] SIGTERM, shutting down...");
  registryWs.close();
  workerWss.close();
  process.exit(0);
});