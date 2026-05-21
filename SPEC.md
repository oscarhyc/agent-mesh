# Agent Mesh — Distributed ACP Agent Pool

## Status
Live — OpenClaw plugin integrated and mesh backend registered 2026-05-21.

---

## 1. Overview

A WebSocket-based agent mesh that lets OpenClaw dynamically discover, connect to, and use remote ACP-compatible agents (Hermes, Claude Code, etc.) running on any machine in the same subnet. Agents can be added or removed at runtime with zero OpenClaw restarts.

**Current deployment:** Registry runs on Mac mini at `ws://192.168.1.206:9000`. Hermes worker (`hermes-remote`) runs on a Linux VM (VMware, IP `192.168.1.112`). OpenClaw gateway on Mac mini integrates via the `agent-mesh` plugin at `~/.openclaw/extensions/agent-mesh/`.

**Goal:** Enable dynamic capacity scaling, heterogeneous agents, and fault tolerance without modifying OpenClaw core or the agent binaries.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     SAME SUBNET (LAN)                           │
│                                                           │
│   ┌──────────────┐     ┌──────────────────────────────────┐  │
│   │  Registry    │◄───►│  Worker 1 (Hermes Agent)          │  │
│   │  :9000      │     │  ws://worker1:9001                 │  │
│   │             │◄───►│  wrapped by agent-worker.js        │  │
│   │  (Node.js)  │     └──────────────────────────────────┘  │
│   │             │◄───►┌──────────────────────────────────┐  │
│   │  - register │     │  Worker 2 (Claude Code)           │  │
│   │  - heartbeat│     │  ws://worker2:9001                │  │
│   │  - discover  │     │  wrapped by agent-worker.js        │  │
│   │  - track     │     └──────────────────────────────────┘  │
│   │    capabilities│                                           │
│   └──────────────┘     ┌──────────────────────────────────┐  │
│          ▲              │  Worker N (any ACP agent)         │  │
│          │              │  ws://workerN:9001                │  │
│          │              └──────────────────────────────────┘  │
│          │                                                   │
└──────────┼───────────────────────────────────────────────────┘
           │
           │ WebSocket
           ▼
┌──────────────────────────────────────────────────────────────┐
│  OpenClaw Gateway (Mac mini, 192.168.1.206)                 │
│                                                             │
│  agent-mesh plugin (~/extensions/agent-mesh/)               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ MeshBridgeRuntime (AcpRuntimeBackend "mesh")         │   │
│  │   - queries Registry for available agents           │   │
│  │   - maintains persistent WS to selected agent       │   │
│  │   - pipes ACP messages over WebSocket               │   │
│  │   - handles reconnect on agent drop                 │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Existing ACPX interface unchanged — tools/skill layer      │
│  talks to MeshBridgeRuntime the same way it talks to a      │
│  local subprocess today.                                    │
└─────────────────────────────────────────────────────────────┘
```

**Three components:**
1. **Registry** — lightweight service broker (Node.js)
2. **Agent Worker** — per-machine wrapper that registers an agent and bridges ACP ↔ WebSocket
3. **Mesh Bridge (OpenClaw plugin)** — registers `mesh` AcpRuntimeBackend; replaces local `child_process.spawn` with registry discovery + WebSocket connection

---

## 3. Protocol

### 3.1 Registry Port & Protocol

**Port:** `9000` (configurable)
**Transport:** Plain WebSocket (no TLS — same subnet)
**Encoding:** JSON-RPC 2.0 messages over WebSocket frames

### 3.2 Registry → Worker Messages (server-initiated)

Workers connect to registry and receive control messages:

```
// Server registers this worker
{ "jsonrpc": "2.0", "method": "register", "params": { "agentId": "hermes-1", "capabilities": { "agentType": "hermes", "skills": ["coding", "research"], "maxConcurrent": 3 } }, "id": 1 }

// Server sends a task to this worker
{ "jsonrpc": "2.0", "method": "task", "params": { "taskId": "abc123", "sessionKey": "...", "prompt": "..." }, "id": 2 }

// Server cancels a task
{ "jsonrpc": "2.0", "method": "cancel", "params": { "taskId": "abc123" }, "id": 3 }

// Server pings — worker must respond with pong
{ "jsonrpc": "2.0", "method": "ping", "params": {}, "id": 4 }
```

### 3.3 Worker → Registry Messages (client-initiated)

```
// Worker registers on connect
{ "jsonrpc": "2.0", "method": "register", "params": { "agentId": "hermes-1", "host": "192.168.1.101", "port": 9001, "capabilities": { "agentType": "hermes", "skills": ["coding"], "maxConcurrent": 3 } }, "id": 1 }

// Worker sends task result
{ "jsonrpc": "2.0", "method": "result", "params": { "taskId": "abc123", "done": false, "chunk": "..." }, "id": 2 }
{ "jsonrpc": "2.0", "method": "result", "params": { "taskId": "abc123", "done": true, "final": "..." }, "id": 3 }

// Worker sends pong in response to ping
{ "jsonrpc": "2.0", "result": { "pong": true }, "id": 4 }

// Worker signals it's shutting down
{ "jsonrpc": "2.0", "method": "deregister", "params": { "reason": "shutdown" }, "id": 5 }
```

### 3.4 Mesh Bridge ↔ Registry Messages

```
// Bridge queries for agents matching criteria
{ "jsonrpc": "2.0", "method": "discover", "params": { "capabilities": { "agentType": "hermes" }, "limit": 1 }, "id": 10 }

// Registry responds with available agents
{ "jsonrpc": "2.0", "result": { "agents": [{ "agentId": "hermes-1", "host": "192.168.1.101", "port": 9001, "load": 0 }] }, "id": 10 }

// Bridge asks worker to start a task (bridged through registry redirect)
{ "jsonrpc": "2.0", "method": "task", "params": { "taskId": "...", "sessionKey": "...", "prompt": "..." }, "id": 11 }
```

### 3.5 Mesh Bridge ↔ Worker (direct WebSocket — after discovery)

Once the bridge has `host:port` of a worker, it connects **directly** to that worker's WebSocket server and pipes ACP messages. The registry is only used for discovery and routing — the actual ACP session is peer-to-peer between bridge and worker.

```
Bridge                         Worker
  │                               │
  │──── WS connect to :9001 ─────►│
  │                               │
  │◄─── ACP handshake JSON ──────│
  │                               │
  │──── ACP prompt message ──────►│  (forwarded to agent)
  │◄─── ACP response chunks ──────│  (streaming)
  │◄─── ACP final response ──────│
  │                               │
  │──── WS close ───────────────►│
```

---

## 4. Component Specifications

### 4.1 Registry (`registry/`)

**Purpose:** Service discovery and health tracking only. Does NOT proxy ACP traffic.

**State:**
```
agents: Map<agentId, {
  agentId: string
  host: string          // IP of worker machine
  port: number          // worker's direct WS port
  capabilities: { agentType, skills[], maxConcurrent }
  load: number          // current active task count
  lastHeartbeat: number // timestamp
  status: 'idle' | 'busy' | 'offline'
}>
```

**API endpoints (WebSocket JSON-RPC):**

| Method | Direction | Description |
|--------|-----------|-------------|
| `register` | Worker→Registry | Worker registers, provides host:port of its direct WS |
| `discover` | Bridge→Registry | Bridge queries for agents matching criteria |
| `result` | Worker→Registry | Worker reports task completion |
| `deregister` | Worker→Registry | Worker goes offline |
| `ping/pong` | Registry→Worker | Liveness check every 10s |
| `task` (redirect) | Registry→Worker | Initial task routing (before bridge connects direct) |

**Load balancing:** Registry returns agent with lowest `load` among matching agents. Bridge is responsible for direct connection after discovery.

**Timeout policy:** If a worker misses 3 consecutive pings (30s), registry marks it `offline` and removes it from discovery. Tasks assigned to it are implicitly cancelled — bridge must re-discover.

**Persistence:** In-memory only. Workers re-register on reconnect. No disk I/O.

### 4.2 Agent Worker (`agent-worker/`)

**Purpose:** Per-machine process that wraps an ACP agent binary (Hermes, Claude Code, etc.) and bridges it to WebSocket.

> ⚠️ **Architecture note:** The worker's WebSocket server (port :9001) lives on the **orchestrator/mesh host only**. Subagents do NOT run a WebSocket service — the orchestrator-side bridge handles all WS routing. A subagent machine needs only the agent binary; it does not need to expose any port.

**Usage on each worker machine (subagent host):**
```bash
node agent-worker.js \
  --registry ws://<registry-host>:9000 \
  --agent-type hermes \
  --agent-command "hermes" \
  --agent-args "acp" \
  --worker-port 9001
```

**Behavior:**
1. Connects to Registry WebSocket on startup
2. Registers with capabilities (agent type, skills, max concurrent sessions)
3. Accepts task dispatch messages from the WebSocket bridge (orchestrator side)
4. Spawns agent subprocess (`--agent-command --agent-args`)
5. Pipes ACP JSON-RPC between WebSocket and agent subprocess stdio
6. Streams results back via WebSocket to the orchestrator
7. On SIGTERM: graceful deregister + subprocess kill

**Concurrency:** Default 1 task at a time. If `--max-concurrent N` is set, worker queues tasks sequentially.

**Agent subprocess lifecycle:**
- Spawned on first task, kept alive between tasks (configurable idle timeout)
- Killed on: worker shutdown, agent crash, or `deregister`
- **⚠️ Subprocess stdin — macOS vs Linux:** On macOS, use direct `spawn(command, args, { stdio: ["pipe", "pipe", "pipe"] })`. Do NOT use `nohup bash -c "command &"` with `detached: true` — that closes stdin and makes `stdin.write()` silently fail. On Linux, use `setsid` to detach from controlling TTY (avoids SIGTTIN/SIGTTOU for Python asyncio).

### 4.3 OpenClaw Plugin (`agent-mesh-plugin/` / `~/.openclaw/extensions/agent-mesh/`)

**Purpose:** Registers the mesh AcpRuntimeBackend so `sessions_spawn` can route tasks through the registry to remote workers.

**Plugin path:** `~/.openclaw/extensions/agent-mesh/index.js`

**Manifest:** `~/.openclaw/extensions/agent-mesh/openclaw.plugin.json`

**How it works:**
1. Plugin starts → `registerMeshRuntime()` is called
2. `MeshBridgeRuntime` registers as `AcpRuntimeBackend id="mesh"` in the global ACP runtime registry
3. `sessions_spawn` with `backend: "mesh"` or `agentRuntime.id: "mesh"` routes through the mesh
4. `MeshBridgeRuntime.ensureSession()` calls registry `discover()` → connects to worker WS → returns session handle
5. `MeshBridgeRuntime.runTurn()` pipes prompt over WS → yields response chunks → closes on done

**Config (`~/.openclaw/openclaw.json`):**
```json
{
  "plugins": {
    "entries": {
      "agent-mesh": {
        "enabled": true,
        "config": {
          "registry": "ws://192.168.1.206:9000",
          "defaultAgentType": "mock-hermes"
        }
      }
    }
  }
}
```

**Agent configuration example:**
```json
{
  "agents": {
    "list": [{
      "id": "hermes-remote",
      "agentRuntime": { "id": "mesh" },
      "runtimeConfig": { "acp": { "backend": "mesh" } }
    }]
  }
}
```

**Implementation:** Single-file CJS plugin (`index.js`) — no build step. Uses `createRequire(require.resolve("/opt/homebrew/lib/node_modules/openclaw/dist/index.js"))` to resolve `ws` and `openclaw/plugin-sdk/acp-runtime` from OpenClaw's own node_modules. Contains `MeshBridgeRuntime` + `BridgeClient` + `rpcDiscover` (same pattern as standalone bridge).

### 4.4 Registry WebSocket Server (`registry/src/server.ts`)

**Port:** `9000`
**Auth:** None (same subnet trust)
**Heartbeat interval:** 10 seconds
**Health threshold:** 3 missed pings → agent marked offline

---

## 5. Data Flow: Full Task Lifecycle

```
1. Worker N starts
   → connects to Registry WS :9000
   → sends register(agentId="hermes-n", host, port, capabilities)
   → Registry adds to agents map

2. OpenClaw routes sessions_spawn(agentId="hermes", agentRuntime.id="mesh")
   → MeshBridgeRuntime receives the spawn request

3. MeshBridgeRuntime.ensureSession()
   → rpcDiscover(registry, { agentType: "hermes" }, 1)
   → Registry responds with { agents: [{ agentId, host, port }] }
   → BridgeClient connects directly to ws://<host>:<port>

4. MeshBridgeRuntime.runTurn()
   → sends { type: "execute", taskId, sessionKey, prompt } over WS
   → yields response chunks as AcpRuntimeEvents

5. Worker receives task
   → spawns "hermes acp" subprocess
   → pipes ACP messages to subprocess stdio
   → streams subprocess stdout back over WebSocket

6. MeshBridgeRuntime receives response chunks
   → forwards to OpenClaw ACP runtime
   → OpenClaw delivers to user

7. Task completes
   → Worker sends result to Registry (idempotent)
   → Registry increments agent load on start, decrements on done

8. If Worker goes offline mid-task:
   → Registry marks agent offline after 3 missed pings
   → MeshBridgeRuntime re-queries Registry for another worker
   → reconnects to alternative worker
   → if session is resumable, resumes; otherwise reports failure
```

---

## 6. Fault Tolerance

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Worker goes offline | 3 missed pings (30s) | Registry removes from pool; bridge reconnects to another worker |
| Worker crashes mid-task | WebSocket connection drops | Bridge re-discovers; task marked failed or resumed |
| Registry goes down | Workers can't connect | Workers keep trying to reconnect; tasks queue locally |
| Bridge loses registry | Mesh can't discover | `reconnectAttempts` with backoff; existing sessions continue |
| Network blip | WebSocket auto-reconnect | Workers reconnect with same agentId; registry restores state |

**Session resumption:** ACP sessions have an ID. If a worker dies and another picks up the task, it can send the session ID to the new worker. Whether the new worker can resume depends on the agent's session support (Hermes supports session resume; others may not).

**No HA registry needed** for v1. If registry goes down, existing sessions continue (they're peer-to-peer). New sessions wait or fail until registry is back.

---

## 7. Project Structure

```
agent-mesh/
├── registry/
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── server.ts         # Registry WS server + HTTP static serving
│       ├── types.ts          # Shared types
│       └── registry.ts       # In-memory agent registry + logic
│   └── public/
│       └── index.html        # Admin dashboard (served at :9000/)
├── agent-worker/
│   ├── package.json
│   ├── tsconfig.json
│   ├── onboarding-prompt.md  # Agent self-joining prompt
│   └── src/
│       ├── worker.ts         # Worker entry point
│       ├── acp-bridge.ts    # ACP stdio ↔ WebSocket bridge
│       └── subprocess.ts    # Agent subprocess manager
├── agent-mesh-plugin/        # OpenClaw plugin source (develop here)
│   ├── src/
│   │   ├── index.ts          # Plugin entry point
│   │   └── mesh-runtime-plugin.ts  # Runtime + registry clients
│   └── package.json
├── agent-mesh-bridge/        # Standalone bridge package (published)
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── bridge-client.ts  # Mesh Bridge WS client
│       ├── registry-client.ts # Registry discovery client
│       ├── mesh-runtime.ts   # ACPX runtime replacement
│       └── index.ts          # Plugin entry point
├── mesh-tester/
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       └── tester.ts         # CLI diagnostic tool
├── shared/
│   └── src/
│       ├── protocol.ts       # JSON-RPC message types
│       └── capabilities.ts   # Capability schema
├── mock-hermes.js            # Standalone mock Hermes for local testing
├── SPEC.md                   # This file
├── README.md
└── ONBOARDING.md
```

### Plugin Deployment

The `agent-mesh-plugin/` directory is the development source. The deployed plugin lives at:

```
~/.openclaw/extensions/agent-mesh/
├── index.js                  # Compiled single-file plugin
└── openclaw.plugin.json      # Manifest
```

The `index.js` is a self-contained CJS file that includes all runtime code (MeshRuntime, BridgeClient, rpcDiscover) and imports `ws` + `openclaw/plugin-sdk/acp-runtime` via `createRequire(require.resolve("/opt/homebrew/lib/node_modules/openclaw/dist/index.js"))`.

---

## 8. Registry Admin Dashboard (`registry/public/`)

**Purpose:** Web UI served by the Registry process for operators to monitor and manage the mesh.

**Served at:** `http://<registry-host>:9001/` — the Registry HTTP server (separate from the WebSocket port 9000).

**Features:**
- Live agent list: agentId, type, host, status (idle/busy/offline), current load, last heartbeat
- Active task count total
- Registry stats: uptime, total tasks processed, connected workers count
- Per-agent actions: force deregister, view recent task history
- Auto-refresh every 5 seconds via WebSocket subscription

**Implementation:** Single HTML file + vanilla JS WebSocket client. No build step, no external deps. Registry serves it as a static file.

**Auth:** None for v1. Same-subnet trust assumption.

---

## 9. Agent Self-Joining Prompt (`agent-worker/onboarding-prompt.md`)

**Purpose:** A config-driven prompt file that any ACP-compatible agent reads to generate its own worker startup script and systemd service.

**Prompt content:**
```
You are being onboarded to the Agent Mesh.

Your task: generate a worker startup command and systemd service file for your agent type.

Steps:
1. Identify your agent type (hermes / claude-code / etc.)
2. Identify your machine's LAN IP: hostname -I | awk '{print $1}'
3. Generate a worker startup command with these flags:
   - --registry ws://<registry-host>:9000
   - --worker-port 9001
   - --agent-type <your-type>
   - --agent-command <path to your agent binary>
   - --agent-args acp
4. Output a systemd service file (Linux) or launchd plist (macOS)
5. Save the service file to the appropriate system directory

Return exactly:
COMMAND: <the node command to run>
SERVICE: <full service file content>
```

**Usage:** Human asks their agent to "join the mesh" and pastes this prompt. The agent generates the command + service file. Human runs one command. Worker starts, self-registers.

---

## 10. Mesh Tester / Debug Agent (`mesh-tester/`)

**Purpose:** Built-in CLI diagnostic tool running on the registry host for verifying mesh health and tracing failures.

**Commands:**

```bash
# Verify all registered workers are reachable
node mesh-tester.js ping --all

# Send a test task to a specific agent, verify response
node mesh-tester.js test-task --agent hermes-1 --prompt "echo hello"

# Stream a live task's messages through the registry for debugging
node mesh-tester.js trace-session <taskId>

# Health report: pings, success rate, last error per agent
node mesh-tester.js health

# Simulate worker crash to test fault recovery
node mesh-tester.js inject-fault --agent hermes-1
```

**Implementation:** CLI tool that connects to Registry WebSocket and issues test/subscribe RPCs. Not exposed to OpenClaw chat — purely an ops tool.

**Not exposed to OpenClaw chat** — CLI only on registry host machine.

---

## 11. Current Host Configuration

| Machine | IP | Role |
|---------|-----|------|
| Mac mini (OpenClaw host) | 192.168.1.206 | Registry (WS :9000, HTTP :9001), OpenClaw Gateway |
| Linux VM (VMware, bridge) | 192.168.1.112 | Hermes worker, hermes agent binary |
| Windows (Hermes) | (same subnet) | Hermes native, connects to registry |

Registry dashboard: `http://192.168.1.206:9001/`

---

## 12. Technology

| Layer | Technology | Notes |
|-------|------------|-------|
| Registry | TypeScript + Node.js `ws` | Plain WebSocket. HTTP dashboard on port 9001. |
| Agent Worker | TypeScript + Node.js `ws` + `child_process` | `setsid` on Linux; direct spawn on macOS. |
| OpenClaw Plugin | Vanilla JS (CJS, no build) | Single `index.js` using OpenClaw's own `node_modules`. |
| Mesh Tester | TypeScript + Node.js `ws` | CLI diagnostic tool. |
| Shared | TypeScript | Protocol types only. No runtime deps. |

Node.js 22+. Registry is in-memory (no DB, no Redis).

---

## 13. Security

**No auth on v1** — all machines assumed trusted on same subnet.

For future consideration (v2):
- TLS mutual auth (mTLS) between all components
- Registry authentication (bridge must register with a secret)
- Agent capability signing
- Dashboard login
- Auth on registry WebSocket protocol

---

## 14. Open Questions / v1 Scope

**In scope v1:**
- Registry + Worker + Bridge for Hermes only
- Registry Admin Dashboard (simple HTML)
- Agent self-joining via prompt
- Mesh Tester CLI (ping, test-task, trace, health)
- Single subnet, no TLS
- Static agent type (worker configured as "hermes" or "claude-code")
- Manual agent registration
- Basic load balancing (lowest active task count)
- Graceful shutdown

**Deferred to v2:**
- Automatic discovery (mDNS/Bonjour)
- Multiple concurrent sessions per worker
- Cross-NAT support (relay mode)
- HA Registry (multiple instances)
- TLS
- Session migration
- Auth on dashboard
- Web-based admin dashboard with auth