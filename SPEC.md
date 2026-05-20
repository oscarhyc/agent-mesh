# Agent Mesh — Distributed ACP Agent Pool

## Status
Draft — for review before implementation.

---

## 1. Overview

A WebSocket-based agent mesh that lets OpenClaw dynamically discover, connect to, and use remote ACP-compatible agents (Hermes, Claude Code, etc.) running on any machine in the same subnet. Agents can be added or removed at runtime with zero OpenClaw restarts.

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
│  OpenClaw Gateway                                          │
│                                                             │
│  Modified ACPX bridge (agent-mesh-bridge/)                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ MeshClient                                           │   │
│  │   - queries Registry for available agents           │   │
│  │   - maintains persistent WS to selected agent       │   │
│  │   - pipes ACP messages over WebSocket               │   │
│  │   - handles reconnect on agent drop                 │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Existing ACPX interface unchanged — tools/skill layer      │
│  talks to MeshClient the same way it talks to a local       │
│  subprocess today.                                          │
└─────────────────────────────────────────────────────────────┘
```

**Three components:**
1. **Registry** — lightweight service broker (Node.js)
2. **Agent Worker** — per-machine wrapper that registers an agent and bridges ACP ↔ WebSocket
3. **Mesh Bridge** — OpenClaw ACPX plugin extension that replaces local subprocess spawning with registry discovery + WebSocket connection

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

**Usage on each worker machine:**
```bash
node agent-worker.js \
  --registry ws://192.168.1.100:9000 \
  --agent-type hermes \
  --agent-command "hermes" \
  --agent-args "acp" \
  --worker-port 9001
```

**Behavior:**
1. Connects to Registry WebSocket on `--worker-port`
2. Registers with capabilities (agent type, skills, max concurrent sessions)
3. Accepts task messages from Registry or Bridge
4. Spawns agent subprocess (`--agent-command --agent-args`)
5. Pipes ACP JSON-RPC between WebSocket and agent subprocess stdio
6. Streams results back via WebSocket
7. On SIGTERM: graceful deregister + subprocess kill

**Concurrency:** If `--max-concurrent N` is set and the underlying agent doesn't support multiplexing, worker queues tasks and processes them sequentially. Default: 1.

**Agent subprocess lifecycle:**
- Spawned on first task
- Kept alive between tasks (configurable idle timeout)
- Killed on: worker shutdown, agent crash, or Registry `deregister`

### 4.3 Mesh Bridge (`agent-mesh-bridge/`)

**Purpose:** OpenClaw ACPX plugin extension. Replaces local `child_process.spawn` with registry discovery + WebSocket connect.

**Implementation:** A modified version of the `acpx` runtime that:
1. On `sessions_spawn({ runtime: "acp", agentId: "hermes" })`: queries the Registry for an available Hermes worker
2. Establishes direct WebSocket to that worker's `:9001`
3. Pipes ACP JSON-RPC over WebSocket (same as it would over stdio)
4. On agent disconnect: re-queries Registry for another worker, reconnects, resumes session if possible

**Config (`~/.openclaw/openclaw.json`):**
```json
{
  "plugins": {
    "entries": {
      "agent-mesh-bridge": {
        "enabled": true,
        "config": {
          "registry": "ws://192.168.1.100:9000",
          "defaultAgentType": "hermes",
          "reconnectAttempts": 5,
          "reconnectDelayMs": 1000
        }
      }
    }
  }
}
```

**Existing ACPX interface:** `sessions_spawn({ runtime: "acp", agentId: "hermes" })` works exactly as before — the Mesh Bridge handles the routing transparently.

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

2. User sends message to OpenClaw
   → OpenClaw routes to ACP runtime
   → Mesh Bridge receives sessions_spawn(agentId="hermes")

3. Mesh Bridge queries Registry
   → discover(capabilities.agentType="hermes")
   → Registry responds with lowest-load agent (e.g. hermes-1, load=0)

4. Mesh Bridge connects direct to Worker 1
   → WS to ws://192.168.1.101:9001
   → sends ACP prompt message as JSON-RPC

5. Worker 1 receives task
   → spawns "hermes acp" subprocess
   → pipes ACP messages to subprocess stdio
   → streams subprocess stdout back over WebSocket

6. Mesh Bridge receives response chunks
   → forwards to OpenClaw ACP runtime
   → OpenClaw delivers to user

7. Task completes
   → Worker sends result to Registry (idempotent)
   → Registry increments agent load on start, decrements on done

8. If Worker 1 goes offline mid-task:
   → Registry marks hermes-1 offline after 3 missed pings
   → Mesh Bridge re-queries Registry for hermes-2
   → reconnects to hermes-2
   → if session is resumable (ACP session), resumes; otherwise reports failure
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
│       ├── server.ts         # Registry WS server
│       ├── types.ts          # Shared types
│       └── registry.ts       # In-memory agent registry + logic
├── agent-worker/
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── worker.ts         # Worker entry point
│       ├── acp-bridge.ts    # ACP stdio ↔ WebSocket bridge
│       └── subprocess.ts    # Agent subprocess manager
├── agent-mesh-bridge/
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── bridge-client.ts  # Mesh Bridge WS client
│       ├── registry-client.ts # Registry discovery client
│       ├── mesh-runtime.ts   # ACPX runtime replacement
│       └── index.ts          # Plugin entry point
├── shared/
│   └── src/
│       ├── protocol.ts       # JSON-RPC message types
│       └── capabilities.ts   # Capability schema
├── SPEC.md                   # This file
└── README.md
```

---

## 7. Registry Admin Dashboard (`registry/dashboard/`)

**Purpose:** Web UI served by the Registry process for operators to monitor and manage the mesh.

**Served at:** `http://<registry-host>:9000/` — the Registry HTTP server (port 9000) also serves the dashboard.

**Features:**
- Live agent list: agentId, type, host, status (idle/busy/offline), current load, last heartbeat
- Active task count total
- Registry stats: uptime, total tasks processed, connected workers count
- Per-agent actions: force deregister, view recent task history
- Auto-refresh every 5 seconds via WebSocket subscription

**Implementation:** Single HTML file + vanilla JS WebSocket client. No build step, no external deps. Registry serves it as a static file.

**Auth:** None for v1. Same-subnet trust assumption.

---

## 8. Agent Self-Joining Prompt (`agent-worker/onboarding-prompt.md`)

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

## 9. Mesh Tester / Debug Agent (`mesh-tester/`)

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

## 10. Project Structure

```
agent-mesh/
├── registry/
│   ├── package.json
│   ├── tsconfig.json
│   ├── src/
│   │   ├── server.ts         # Registry WS server + HTTP static serving
│   │   ├── types.ts          # Shared types
│   │   └── registry.ts       # In-memory agent registry + logic
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
├── agent-mesh-bridge/
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
├── SPEC.md                   # This file
└── README.md
```

---

## 11. Open Questions / v1 Scope

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

---

## 12. Technology

| Layer | Language | Key Libraries |
|-------|----------|---------------|
| Registry | TypeScript | `ws` (WebSocket), `tsx` (runner) |
| Agent Worker | TypeScript | `ws`, `child_process`, `tsx` |
| Mesh Bridge | TypeScript | `ws`, hooks into OpenClaw plugin system |
| Mesh Tester | TypeScript | `ws`, `tsx` |
| Shared | TypeScript | — |

Node.js 22+ on all components. No database, no Redis — registry is in-memory state only.

---

## 13. Security

**No auth on v1** — all machines assumed trusted on same subnet.

For future consideration (v2):
- TLS mutual auth (mTLS) between all components
- Registry authentication (bridge must register with a secret)
- Agent capability signing
- Dashboard login
