# Agent Mesh

A WebSocket-based distributed ACP agent pool for dynamic agent registration, discovery, and task routing across machines on the same subnet.

## Components

| Package | Description |
|---------|-------------|
| `@agent-mesh/shared` | Protocol types and capability schema |
| `@agent-mesh/registry` | Service discovery broker + HTTP dashboard |
| `@agent-mesh/agent-worker` | Per-machine sidecar wrapping ACP agents |
| `@agent-mesh/agent-mesh-bridge` | OpenClaw ACPX plugin extension |
| `@agent-mesh/mesh-tester` | CLI diagnostic tool |

## Quick Start

### 1. Install all packages

```bash
cd agent-mesh
npm install
```

### 2. Start the Registry

```bash
npm run dev:registry
# WebSocket: ws://localhost:9000
# Dashboard: http://localhost:9001/
```

### 3. Start a Worker (on another machine)

```bash
cd agent-mesh/agent-worker
npm start -- \
  --registry ws://<registry-host>:9000 \
  --worker-port 9001 \
  --agent-type hermes \
  --agent-command hermes \
  --agent-args "acp"
```

### 4. Use the Mesh Tester

```bash
npm run dev:tester -- ping --all
npm run dev:tester -- health
```

## Architecture

- **Registry** (port 9000): WebSocket server for worker registration and discovery
- **Workers**: Connect to registry, expose direct WebSocket for ACP sessions
- **Bridge**: OpenClaw ACPX plugin that queries registry and connects directly to workers

See [SPEC.md](./SPEC.md) for full architecture documentation.

## Project Structure

```
agent-mesh/
├── shared/           # Protocol types
├── registry/          # Discovery broker + dashboard
├── agent-worker/      # Per-machine sidecar
├── agent-mesh-bridge/ # OpenClaw plugin
├── mesh-tester/      # CLI diagnostic tool
├── SPEC.md           # Full specification
└── README.md
```
