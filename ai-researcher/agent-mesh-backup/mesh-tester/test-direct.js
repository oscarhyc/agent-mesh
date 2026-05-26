#!/usr/bin/env node
import WebSocket from 'ws';

const REGISTRY_URL = "ws://localhost:9000";
const taskId = `test-${Date.now()}`;
const sk = `test-${Date.now()}`;

async function run() {
  // Discover agent
  const discoverWs = new WebSocket(REGISTRY_URL);
  const agents = await new Promise((resolve, reject) => {
    discoverWs.on("open", () => {
      discoverWs.send(JSON.stringify({ jsonrpc: "2.0", method: "discover", params: { capabilities: { agentType: "hermes" }, limit: 1 }, id: 1 }));
    });
    discoverWs.on("message", (data) => {
      const msg = JSON.parse(data.toString());
      if (msg.result?.agents) {
        discoverWs.close();
        resolve(msg.result.agents);
      }
    });
    discoverWs.on("error", reject);
    setTimeout(() => reject(new Error("discover timeout")), 10000);
  });

  console.log("Discovered:", agents.map(a => a.agentId));
  const agent = agents[0];

  // Connect directly to worker
  const workerWs = new WebSocket(`ws://${agent.host}:${agent.port}`);
  await new Promise((res, rej) => { workerWs.on("open", res); workerWs.on("error", rej); });

  // Send task
  workerWs.send(JSON.stringify({ type: "task", taskId, prompt: "Return exactly: PONG", sessionKey: sk }));
  console.log("Task sent, waiting for result...");

  const result = await new Promise((resolve) => {
    workerWs.on("message", (data) => {
      const msg = JSON.parse(data.toString());
      console.log("MSG type:", msg.type, "keys:", Object.keys(msg));
      if (msg.type === "result" && msg.final) {
        workerWs.close();
        resolve(msg.final);
      }
      if (msg.type === "chunk") process.stdout.write(msg.text ?? "");
    });
    setTimeout(() => { workerWs.close(); resolve("timeout"); }, 30000);
  });

  console.log("\nResult:", result);
  process.exit(0);
}

run().catch(err => { console.error(err); process.exit(1); });