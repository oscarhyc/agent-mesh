import WebSocket from "ws";

const ws = new WebSocket("ws://192.168.1.112:9001");

ws.on("open", () => {
  console.log("[diag] connected");
  const taskId = `diag-${Date.now()}`;
  ws.send(JSON.stringify({ type: "task", taskId, prompt: "echo hello diag", sessionKey: "test" }));
  setTimeout(() => { console.log("timeout"); ws.close(); process.exit(0); }, 10_000);
});

ws.on("message", (data) => {
  console.log(`[diag] recv: ${data.toString().slice(0, 300)}`);
});

ws.on("error", (err) => { console.error(`error: ${err.message}`); process.exit(1); });