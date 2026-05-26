import WebSocket from "ws";

const prompt = process.argv[2] ?? "Return exactly: hi";
const taskId = `test-${Date.now()}`;

const ws = await new Promise((resolve, reject) => {
  const s = new WebSocket("ws://192.168.1.206:9000");
  s.on("open", () => resolve(s));
  s.on("error", reject);
});

ws.send(JSON.stringify({ jsonrpc: "2.0", method: "discover", params: { capabilities: { agentType: "hermes" }, limit: 1 }, id: 1 }));

ws.on("message", (data) => {
  const msg = JSON.parse(data.toString());
  if (msg.id !== 1) return;
  ws.close();
  const { host, port } = msg.result.agents[0];
  console.log(`Worker: ${host}:${port}`);

  const wws = new WebSocket(`ws://${host}:${port}`);
  wws.on("open", () => {
    console.log("Sending task...");
    wws.send(JSON.stringify({ type: "task", taskId, prompt, sessionKey: "test" }));
  });
  wws.on("message", (d) => {
    const m = JSON.parse(d.toString());
    console.log("MSG type=" + m.type);
    if (m.type === "chunk") process.stdout.write(m.text ?? "");
    if (m.type === "result") { console.log("\n✅ Final:", (m.final ?? "").slice(0, 300)); wws.close(); process.exit(0); }
    if (m.type === "error") { console.error("\n❌ Error:", JSON.stringify(m)); wws.close(); process.exit(1); }
  });
  wws.on("error", (e) => { console.error("WS err:", e.message); process.exit(1); });
});

setTimeout(() => { console.log("timeout"); process.exit(1); }, 60000);