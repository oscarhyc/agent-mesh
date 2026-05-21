import WebSocket from "ws";

const ws = new WebSocket("ws://192.168.1.206:9002");

ws.on("open", () => {
  console.log("[connect-test] connected");
  // Immediately send a task message before waiting
  const taskId = `connect-${Date.now()}`;
  console.log(`[connect-test] sending: ${JSON.stringify({ type: "task", taskId, prompt: "hello", sessionKey: "test" })}`);
  ws.send(JSON.stringify({ type: "task", taskId, prompt: "hello", sessionKey: "test" }));
});

ws.on("message", (data) => {
  console.log(`[connect-test] recv: ${data.toString().slice(0, 300)}`);
});

ws.on("close", (code, reason) => {
  console.log(`[connect-test] closed code=${code}`);
});

ws.on("error", (err) => {
  console.error(`[connect-test] error: ${err.message}`);
});

setTimeout(() => {
  console.log("[connect-test] timeout (10s)");
  ws.close();
  process.exit(0);
}, 10_000);