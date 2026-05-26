#!/bin/bash
cd /home/oscar/agent-mesh/agent-worker
source /home/oscar/.hermes/hermes-agent/venv/bin/activate
exec node_modules/.bin/tsx src/worker.ts --registry ws://192.168.1.206:9000 >> /tmp/worker.log 2>&1