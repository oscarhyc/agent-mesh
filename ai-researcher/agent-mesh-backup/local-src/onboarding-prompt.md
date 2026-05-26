You are being onboarded to the Agent Mesh.

Your task: generate a worker startup command and systemd service file for your agent type.

## Steps

1. Identify your agent type (hermes / claude-code / opencode / etc.)
2. Identify your machine's LAN IP: run `hostname -I | awk '{print $1}'`
3. Determine the path to your agent binary (e.g. `hermes`, `claude`, `/usr/local/bin/codex`)
4. Generate a worker startup command with these required flags:
   - `--registry ws://<registry-host>:9000` — replace with the registry machine's LAN IP
   - `--worker-port 9001`
   - `--agent-type <your-type>`
   - `--agent-command <path to your agent binary>`
   - `--agent-args acp`

## Output Format

Return exactly two sections:

### COMMAND
The full node command to run. Example:
```
node /path/to/agent-mesh/agent-worker/dist/worker.js \
  --registry ws://192.168.1.100:9000 \
  --worker-port 9001 \
  --agent-type hermes \
  --agent-command hermes \
  --agent-args "acp"
```

### SERVICE FILE
A systemd service file (Linux) or launchd plist (macOS) that:
- Starts the worker command above
- Restarts on failure
- Logs to syslog or a file