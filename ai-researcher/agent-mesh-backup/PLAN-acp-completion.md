# Plan: Complete ACP Worker Implementation

## What "incomplete" means

The worker's ACP bridge (`acp-bridge.ts`) currently supports **only these ACP methods**:

| Method | Status |
|--------|--------|
| `initialize` | ✅ Handled in `performAcpHandshake()` |
| `authenticate` | ✅ Handled in `performAcpHandshake()` |
| `session/new` | ✅ Handled in `performAcpHandshake()` |
| `session/prompt` | ✅ Handled in `sendTaskPrompt()` |
| `session/cancel` | ✅ Handled in `handleCancel()` |
| Session update notifications | ✅ Handled in `handleSessionUpdate()` |

Missing methods that a full ACP backend should support:

| Method | Status | Purpose |
|--------|--------|---------|
| `session/end` | ❌ Not implemented | Close session gracefully (needed for persistence) |
| `session/load` | ❌ Not implemented | Resume existing session by id |
| `session/set_mode` | ❌ Not implemented | Set session mode (planning, fast, etc.) |
| `session/set_model` | ❌ Not implemented | Switch model mid-session |
| `session/set_config_option` | ❌ Not implemented — **caused the spawn error** | Generic session config write |
| `session/info` | ❌ Not implemented | Query session capabilities / state |

---

## Two layers that need updating

### Layer 1: Worker-side ACP bridge (`acp-bridge.ts`)
This is the code that runs on the Linux VM and bridges WebSocket → hermes stdin/stdout.

**What's needed:**
1. Add a `sendRpc()` call for `session/end` to close the conversation session without killing hermes
2. Handle `session/set_mode`, `session/set_model`, `session/set_config_option` — these are forwarded as-is to hermes and the response is forwarded back over WebSocket
3. Handle `session/load` for session resume
4. For session persistence specifically: add `endCurrentSession()` that calls `session/end`, keeps hermes alive, and resets `this.sessionId = null`

### Layer 2: OpenClaw plugin (`index.js` / `MeshRuntime`)
This is the `MeshRuntime` on the Mac mini side. It handles the ACP runtime events from the worker.

**What's needed:**
- Nothing ACP-method-specific here — it already converts mesh events to ACP events generically
- The `done` event mapping works correctly for `session/end`
- The bridge closes the WebSocket after `done`, which is fine for ephemeral sessions

---

## Implementation: Worker ACP Bridge Changes

### 1. Add `session/end` support

```typescript
// In acp-bridge.ts, add method:

private async endCurrentSession(): Promise<void> {
  if (!this.sessionId) return;
  try {
    const res = await this.sendRpc("session/end", {
      sessionId: this.sessionId,
      reason: "task_complete",
    });
    console.log("[AcpBridge] session/end ok: " + JSON.stringify(res).slice(0, 100));
  } catch (err) {
    console.warn("[AcpBridge] session/end failed (non-fatal): " + err);
  }
  this.sessionId = null;
}
```

Call `endCurrentSession()` after `sendTaskPrompt()` completes successfully — this closes the ACP session but keeps hermes alive for the next task.

### 2. Route new ACP methods through `sendRpc()`

In `handleSubprocessMessage()`, the worker already handles RPC responses by id. The missing methods just need to be forwarded to hermes.

Add a handler for these message types coming from the WebSocket side (not from subprocess). Currently `handleTask()` handles `task`/`execute` and `handleCancel()` handles `cancel`. All other ACP methods should be forwarded directly to hermes via `sendRpc()`:

```typescript
// In ws.on("message") handler, after the task/execute/cancel branches:

else {
  // Forward ACP method to hermes subprocess
  const method = msg["method"] as string | undefined;
  const params = msg["params"] as Record<string, unknown> | undefined;
  if (method && params) {
    this.sendRpc(method, params)
      .then((res) => {
        this.ws.send(JSON.stringify({ jsonrpc: "2.0", id: res.id, result: res.result }));
      })
      .catch((err) => {
        const errRes = { jsonrpc: "2.0", id: null, error: { code: -32600, message: String(err) } };
        this.ws.send(JSON.stringify(errRes));
      });
  }
}
```

**Note:** `msg["id"]` may be undefined for notifications — handle that case.

### 3. Methods to forward directly

These should be sent to hermes and the response sent back over WS:

| Method | Params |
|--------|--------|
| `session/set_mode` | `{ sessionId, mode }` |
| `session/set_model` | `{ sessionId, model }` |
| `session/set_config_option` | `{ sessionId, key, value }` |
| `session/load` | `{ sessionId }` |
| `session/info` | `{ sessionId }` |
| `session/end` | `{ sessionId, reason? }` |

For `session/end`, the bridge should also set `this.sessionId = null` after a successful call so it can start a fresh `session/new` for the next task.

---

## Persistence Flow (after implementation)

```
Task 1:
  WS connects → performAcpHandshake() → session/new → sessionId=A
  → handleTask() → sendTaskPrompt() → session/prompt → done
  → endCurrentSession() → session/end (hermes) → this.sessionId = null
  → WS closes

Task 2:
  WS connects → performAcpHandshake() → [skip if hermes already running]
  → session/new → sessionId=B
  → handleTask() → sendTaskPrompt() → ...
```

If hermes is kept alive between tasks (subprocess not killed), the `initialize`/`authenticate` handshake only happens once at startup. The bridge can be modified to do `initialize`+`authenticate` once in `start()`, then skip them for subsequent tasks.

---

## Testing checklist after implementation

- [ ] `session/end` — task completes, WS closes, hermes stays alive
- [ ] Second task reuses same hermes subprocess (check `ps aux | grep hermes` on VM)
- [ ] `session/set_config_option` — forwarded to hermes, response returned
- [ ] `session/set_mode` — forwarded to hermes, response returned
- [ ] `session/load` — works for session resume
- [ ] Worker survives 5+ sequential tasks without restart
- [ ] Worker handles hermes crash mid-session gracefully

---

## File to modify

`/Users/oscar/.openclaw/workspace/ai-researcher/agent-mesh/agent-worker/src/acp-bridge.ts`

Key changes:
1. Add `endCurrentSession()` method
2. Call `endCurrentSession()` after task completes in `sendTaskPrompt()`
3. Add ACP method forwarding in `ws.on("message")` for `session/set_mode`, `session/set_model`, `session/set_config_option`, `session/load`, `session/end`, `session/info`
4. Consider skipping `initialize`/`authenticate` on subsequent tasks (optional optimization)

---

## Optional: Kill hermes vs keep alive

Current behavior: worker calls `subprocess.kill()` when WS closes (in `ws.on("close")`).

For persistence, you need to change this — keep hermes alive. But you still need to kill it eventually (worker shutdown, idle timeout, crash).

Options:
1. **Idle timeout**: after `session/end`, start a timer; if no new task within N seconds, kill hermes
2. **Config flag**: `--persistent` on worker command line; only enable persistence when set
3. **Graceful shutdown**: on SIGTERM, call `session/end` then kill hermes

Recommendation: use option 1 — a configurable idle timeout (e.g. `--idle-timeout-ms 300000` = 5 minutes). Default can be short (30s) to preserve the current ephemeral behavior unless persistence is explicitly wanted.