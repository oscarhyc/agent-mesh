#!/usr/bin/env node
// Mock Hermes ACP agent — direct pipe test with stdin end guard

console.error("[mock-hermes] starting");

let buffer = "";

// Only enable resume if stdin is a TTY or has data
if (process.stdin.isTTY) {
  process.stdin.resume();
  console.error("[mock-hermes] resumed (TTY mode)");
}

process.stdin.on("data", (chunk) => {
  console.error(`[mock-hermes] DATA: ${chunk.toString().slice(0, 100)}`);
  buffer += chunk.toString();
  const lines = buffer.split("\n");
  buffer = lines.pop() ?? "";
  for (const line of lines) {
    if (!line.trim()) continue;
    try {
      const msg = JSON.parse(line);
      console.error(`[mock-hermes] received: ${JSON.stringify(msg)}`);
      const response = { type: "result", taskId: msg.taskId ?? "unknown", final: "ok" };
      process.stdout.write(JSON.stringify(response) + "\n");
      console.error(`[mock-hermes] sent response`);
    } catch (e) {
      console.error(`[mock-hermes] bad json: ${line.slice(0, 80)}`);
    }
  }
});

process.stdin.on("end", () => {
  console.error("[mock-hermes] stdin ENDED");
});

process.stdin.on("error", (e) => {
  console.error(`[mock-hermes] stdin error: ${e.message}`);
});

console.error("[mock-hermes] waiting for input...");
setTimeout(() => {
  console.error("[mock-hermes] timeout, exiting");
  process.exit(0);
}, 30_000);