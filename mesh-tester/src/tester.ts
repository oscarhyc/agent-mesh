#!/usr/bin/env node
import WebSocket from "ws";
import type { JsonRpcRequest, JsonRpcResponse, AgentInfo } from "../shared/src/protocol.js";

const REGISTRY_URL = process.env.REGISTRY_URL ?? "ws://localhost:9000";

type Command = "ping" | "health" | "test-task" | "trace-session" | "inject-fault";

interface ParsedArgs {
  command: Command;
  options: Record<string, string | boolean>;
}

function parseArgs(args: string[]): ParsedArgs {
  const command = args[0] as Command;
  const options: Record<string, string | boolean> = {};
  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    if (arg.startsWith("--")) {
      const key = arg.slice(2);
      const val = args[i + 1];
      if (val && !val.startsWith("--")) {
        options[key] = val;
        i++;
      } else {
        options[key] = true;
      }
    }
  }
  return { command, options };
}

async function rpc(ws: WebSocket, method: string, params: Record<string, unknown>): Promise<JsonRpcResponse> {
  return new Promise((resolve, reject) => {
    const id = Date.now();
    const msg: JsonRpcRequest = { jsonrpc: "2.0", method, params, id };
    const timeout = setTimeout(() => reject(new Error(`RPC ${method} timed out`)), 10_000);

    const handler = (data: Buffer) => {
      try {
        const res = JSON.parse(data.toString()) as JsonRpcResponse;
        if (res.id === id) {
          clearTimeout(timeout);
          ws.off("message", handler);
          resolve(res);
        }
      } catch {}
    };
    ws.on("message", handler);
    ws.send(JSON.stringify(msg));
  });
}

async function main() {
  const { command, options } = parseArgs(process.argv.slice(2));

  if (!command) {
    console.log(`Usage: mesh-tester <command> [options]
Commands:
  ping --all                   Ping all registered workers
  health                       Show health report for all agents
  test-task --agent <id> --prompt <text>   Send a test task
  trace-session --task-id <id> Stream messages for a session
  inject-fault --agent <id>   Simulate agent crash`);
    return;
  }

  const ws = await new Promise<WebSocket>((resolve, reject) => {
    const ws = new WebSocket(REGISTRY_URL);
    ws.on("open", () => resolve(ws));
    ws.on("error", reject);
  });

  console.log(`[mesh-tester] Connected to ${REGISTRY_URL}`);

  switch (command) {
    case "ping": {
      const res = await rpc(ws, "discover", { capabilities: {}, limit: 100 }) as JsonRpcResponse & { result: { agents: AgentInfo[] } };
      const agents: AgentInfo[] = res.result?.agents ?? [];
      console.log(`\nPinging ${agents.length} agent(s)...\n`);
      for (const agent of agents) {
        const start = Date.now();
        try {
          const workerWs = new WebSocket(`ws://${agent.host}:${agent.port}`);
          await new Promise<void>((res, rej) => {
            workerWs.on("open", () => res());
            workerWs.on("error", rej);
            setTimeout(rej, 3000);
          });
          const latency = Date.now() - start;
          console.log(`  ✅ ${agent.agentId} @ ${agent.host}:${agent.port} — latency: ${latency}ms`);
          workerWs.close();
        } catch {
          console.log(`  ❌ ${agent.agentId} @ ${agent.host}:${agent.port} — unreachable`);
        }
      }
      break;
    }

    case "health": {
      const res = await rpc(ws, "discover", { capabilities: {}, limit: 100 }) as JsonRpcResponse & { result: { agents: AgentInfo[] } };
      const agents: AgentInfo[] = res.result?.agents ?? [];
      console.log(`\n=== Agent Health Report ===`);
      console.log(`Total agents: ${agents.length}`);
      console.log(`Online: ${agents.filter(a => a.status !== "offline").length}`);
      console.log(`Offline: ${agents.filter(a => a.status === "offline").length}\n`);
      for (const agent of agents) {
        const age = Math.round((Date.now() - agent.lastHeartbeat) / 1000);
        const statusIcon = agent.status === "idle" ? "🟢" : agent.status === "busy" ? "🟡" : "🔴";
        console.log(`  ${statusIcon} ${agent.agentId} | type=${agent.capabilities.agentType} | load=${agent.load} | heartbeat=${age}s ago | status=${agent.status}`);
      }
      break;
    }

    case "test-task": {
      const agentId = options["agent"] as string ?? "";
      const prompt = (options["prompt"] as string) ?? "hello";
      if (!agentId) { console.error("--agent required"); return; }
      const res = await rpc(ws, "discover", { capabilities: { agentType: agentId }, limit: 1 }) as JsonRpcResponse & { result: { agents: AgentInfo[] } };
      const agents = res.result?.agents ?? [];
      if (!agents.length) { console.error(`No agent found for type=${agentId}`); return; }
      const agent = agents[0];
      console.log(`Sending test task to ${agent.agentId}@${agent.host}:${agent.port}...`);
      const taskId = `test-${Date.now()}`;
      const workerWs = new WebSocket(`ws://${agent.host}:${agent.port}`);
      await new Promise<void>((res, rej) => { workerWs.on("open", () => res()); workerWs.on("error", rej); });
      workerWs.send(JSON.stringify({ type: "task", taskId, prompt, sessionKey: "test-session" }));
      const result = await new Promise<string>((resolve) => {
        workerWs.on("message", (data) => {
          const msg = JSON.parse(data.toString());
          if (msg.final) resolve(msg.final);
        });
        setTimeout(() => resolve("timeout"), 30_000);
      });
      console.log(`Result: ${result}`);
      workerWs.close();
      break;
    }

    case "trace-session": {
      console.log(`[mesh-tester] trace-session — session tracing requires worker-side support (v2)`);
      break;
    }

    case "inject-fault": {
      console.log(`[mesh-tester] inject-fault — fault injection requires worker-side support (v2)`);
      break;
    }

    default:
      console.error(`Unknown command: ${command}`);
  }

  ws.close();
}

main().catch((err) => {
  console.error(`[mesh-tester] Error: ${err.message}`);
  process.exit(1);
});