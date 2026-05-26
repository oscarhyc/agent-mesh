# Agent Mesh — Distributed ACP Agent Pool

**Backup location.** Complete source for the agent mesh project, spanning two machines.

---

## Machine Map

| Machine | Role | IP | Port |
|---------|------|-----|------|
| Mac mini | Registry + OpenClaw plugin host | `192.168.1.206` | `:9000` (registry), `:18789` (gateway) |
| Linux VM | Worker (runs Hermes subprocess) | `192.168.1.112` | `:9001` (worker WS) |

---

## Project Structure

```
agent-mesh-backup/
├── vm-agent-worker/              # WORKER source from Linux VM (192.168.1.112)
│   ├── src/
│   │   ├── acp-bridge.ts        # ★ MODIFIED: ACP↔WS bridge with persistence
│   │   ├── subprocess.ts         # Spawns/kills Hermes subprocess
│   │   └── worker.ts            # Entry point, WS server on :9001
│   └── dist/                     # Compiled JS output
│
├── mac-registry/                 # REGISTRY on Mac mini (192.168.1.206)
│   ├── src/
│   │   ├── server.ts            # WS service broker, discovery, heartbeat
│   │   └── registry.ts          # In-memory agent registry logic
│   └── public/
│       └── index.html           # Admin dashboard (served at :9000/)
│
├── mac-plugin/                   # OPENCLAW PLUGIN on Mac mini
│   ├── index.js                 # MeshBridgeRuntime (AcpRuntimeBackend "mesh")
│   └── openclaw.plugin.json
│
├── local-src/                   # All TypeScript source (both machines)
│   ├── agent-mesh-plugin/src/   # Plugin source (alternative)
│   ├── agent-mesh-bridge/src/   # Bridge source (alternative)
│   ├── agent-worker/src/        # Worker source (pre-VM local version)
│   └── registry/src/            # Registry source
│
├── agent-mesh-bridge-src/       # Compiled bridge plugin (TypeScript source)
│   ├── src/                      # mesh-runtime.ts, bridge-client.ts, etc.
│   └── dist/                     # Compiled JS
│
├── shared/                       # Shared types across components
│   └── src/
│       ├── protocol.ts          # JSON-RPC message types
│       └── capabilities.ts      # Agent capability schema
│
├── mesh-tester/                 # CLI diagnostic tool
│   └── src/tester.ts
│
├── ONBOARDING.md                # Agent self-joining guide
├── PLAN-acp-completion.md       # Persistence implementation plan
├── SPEC.md                      # Full system specification
├── README.md                    # This file
├── mock-hermes.js               # Mock Hermes for local testing
├── start-worker.sh              # Worker startup script
└── test-direct.js               # Direct Hermes test script
```

---

## Data Flow

```
OpenClaw (Mac mini 192.168.1.206)
    │
    │  1. RPC discover { agentType: hermes }
    ▼
Registry (:9000)  ← mac-registry/
    │  2. responds: { agents: [{ host: 192.168.1.112, port: 9001 }] }
    ▼
OpenClaw connects direct to Worker
    │
    │  ACP JSON-RPC over WebSocket (peer-to-peer, registry out of loop)
    ▼
Worker (:9001)  ← vm-agent-worker/src/acp-bridge.ts
    │
    │  stdin / stdout pipes
    ▼
Hermes subprocess (on Linux VM)
```

---

## Key File: acp-bridge.ts

**Location on VM:** `/home/oscar/agent-mesh/agent-worker/src/acp-bridge.ts`

This is the most-modified file. It bridges WebSocket ↔ Hermes stdio and handles the ACP protocol.

**Persistence enhancements (2026-05-24):**

| Feature | How it works |
|---------|-------------|
| `session/end` after task | `endCurrentSession()` calls ACP `session/end`, clears `sessionId` |
| Hermes survives WS close | `ws.on("close")` calls `startIdleTimer()` instead of `subprocess.kill()` |
| Idle timeout | 30s default — kills Hermes if no new task arrives. Set `idleTimeoutMs=0` to disable |
| ACP method forwarding | `session/set_mode`, `set_model`, `set_config_option`, `load`, `info` routed via `sendRpc()` |
| Task queuing | If task arrives while not ready, polls every 1s until state=ready |

---

## Starting / Restarting Components

### Registry (Mac mini)
```bash
cd /Users/oscar/.openclaw/workspace/ai-researcher/agent-mesh/registry
npx ts-node src/server.ts
```

### Worker (Linux VM)
```bash
# Build first after any changes
cd /home/oscar/agent-mesh/agent-worker
npm run build

# Start/restart
pkill -f "agent-worker" 2>/dev/null
node dist/worker.js \
  --registry ws://192.168.1.206:9000 \
  --agent-type hermes \
  --agent-command "hermes" \
  --agent-args "acp" \
  --worker-port 9001 &
```

### OpenClaw Plugin
```bash
# Plugin auto-loads when openclaw.json has:
# plugins.entries.agent-mesh.enabled: true
openclaw gateway restart
```

---

## Testing

```bash
# From Mac mini — test full mesh
cd /Users/oscar/.openclaw/workspace/ai-researcher/agent-mesh/mesh-tester
npx ts-node src/tester.ts \
  --registry ws://192.168.1.206:9000 \
  --prompt "Return exactly: hello" \
  --agent-type hermes

# Direct Hermes test (no mesh)
node /Users/oscar/.openclaw/workspace/ai-researcher/agent-mesh/test-direct.js
```

---

## Sync Commands

```bash
# Pull latest worker from VM to this backup
rsync -az 192.168.1.112:/home/oscar/agent-mesh/agent-worker/ \
  /Users/oscar/.openclaw/workspace/ai-researcher/agent-mesh-backup/vm-agent-worker/

# Check if worker/Hermes is running on VM
ssh 192.168.1.112 "ps aux | grep -E 'agent-worker|hermes'"

# View worker logs on VM
ssh 192.168.1.112 "tail -100 /tmp/agent-worker.log"

# Restart worker on VM
ssh 192.168.1.112 "pkill -f agent-worker; cd /home/oscar/agent-mesh/agent-worker && node dist/worker.js --registry ws://192.168.1.206:9000 --agent-type hermes --agent-command hermes --agent-args acp --worker-port 9001 &"
```

---

## Architecture Notes

- **Registry is not in the data path** — after discovery, OpenClaw connects directly to worker. Registry only does discovery + heartbeat tracking.
- **Persistence is configurable** — default 30s idle timeout. Pass `idleTimeoutMs` to `AcpBridge` constructor to change, or `0` to disable (keeps Hermes alive indefinitely).
- **ACP methods forwarded to Hermes** — `session/set_mode`, `session/set_model`, `session/set_config_option`, `session/load`, `session/info` are routed via `sendRpc()` with proper response routing.
- **subprocess.ts and worker.ts have pre-existing TS type errors** — these do not affect runtime.

---

*Last updated: 2026-05-24*