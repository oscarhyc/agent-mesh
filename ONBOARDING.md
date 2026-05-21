# Agent Mesh — Join Protocol

## What This Is

The Agent Mesh is a distributed worker pool. Your machine joins as a subagent — it runs an agent binary and communicates through the orchestrator's WebSocket bridge. You do NOT run a WebSocket server yourself.

## What You Need

1. The agent binary you plan to run (e.g. `hermes`, `claude-code`, etc.)
2. Node.js 22+ installed
3. Access to `ws://192.168.1.206:9000` (registry)

## How It Works

```
Your machine                    Orchestrator (192.168.1.206)
─────────────                   ──────────────────────────
agent-worker → connects to registry (ws://192.168.1.206:9000)
           → registers as available
           ← receives tasks via stdin (from orchestrator's WS bridge)
           → sends responses via stdout (to orchestrator's WS bridge)
You only handle the agent binary — the WebSocket bridge lives on the orchestrator.
```

## How To Join

### Step 1 — Identify your machine's LAN IP

```bash
hostname -I | awk '{print $1}'
# e.g. 192.168.1.112
```

### Step 2 — Start the worker

```bash
cd ~/agent-mesh/agent-worker

# Install deps if needed
npm install

# Start (replace hermes / acp with your agent's command and args)
./node_modules/.bin/tsx src/worker.ts \
  --registry ws://192.168.1.206:9000 \
  --worker-port 9001 \
  --agent-type hermes \
  --agent-command hermes \
  --agent-args "acp" \
  --agent-id hermes-remote
```

### Step 3 — Verify you're connected

Check the dashboard: `http://192.168.1.206:9001/`

You should see your agent listed as 🟢 ONLINE with status=idle.

## Customization

| Flag | What it does |
|------|-------------|
| `--agent-type` | How the mesh identifies your agent (e.g. "hermes", "claude-code") |
| `--agent-command` | Path to your agent binary |
| `--agent-args` | Arguments to pass to the agent (usually "acp") |
| `--agent-id` | Unique name for your instance (defaults to `{type}-{timestamp}`) |
| `--worker-port` | Local port worker listens on (orchestrator uses this to route tasks) |

## To leave the mesh

```bash
# Ctrl+C the worker process — it deregisters automatically
```

## Troubleshooting

**Worker shows offline in dashboard?**
- Check registry URL is correct: `ws://192.168.1.206:9000`
- Check your machine can reach port 9000 on the orchestrator
- Look at worker logs for "ping received" / "pong sent" — if you see those, it's connected

**Tasks sent but no response?**
- Verify your agent binary is in PATH or use full path to `--agent-command`
- Check worker logs for "spawned: hermes acp" — if missing, the binary path is wrong
- Verify your agent reads JSON from stdin and writes JSON to stdout (ACP format)