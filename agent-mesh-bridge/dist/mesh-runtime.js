import { RegistryClient } from "./registry-client.js";
import { BridgeClient } from "./bridge-client.js";
export class MeshRuntime {
    config;
    registry;
    sessions = new Map();
    messageHandlers = new Map();
    constructor(config) {
        this.config = config;
        this.registry = new RegistryClient(config.registry);
    }
    async spawn(agentId, taskId) {
        const agents = await this.registry.discover({ agentType: agentId }, 1);
        if (agents.length === 0) {
            console.log(`[MeshRuntime] No agents available for type=${agentId}`);
            return null;
        }
        const agent = agents[0];
        console.log(`[MeshRuntime] Spawning task=${taskId} on agent=${agent.agentId} (${agent.host}:${agent.port})`);
        return new Promise((resolve, reject) => {
            const client = new BridgeClient(agent.host, agent.port, (msg) => {
                const handler = this.messageHandlers.get(taskId);
                handler?.(msg);
            }, () => {
                this.respawn(taskId, agentId).then(resolve).catch(reject);
            });
            client.connect()
                .then(() => {
                this.sessions.set(taskId, { agentId: agent.agentId, taskId, host: agent.host, port: agent.port, client });
                resolve({ agentId: agent.agentId, taskId, host: agent.host, port: agent.port, client });
            })
                .catch(reject);
        });
    }
    async respawn(taskId, agentType) {
        console.log(`[MeshRuntime] Respawning task=${taskId}`);
        return this.spawn(agentType, taskId);
    }
    send(taskId, msg) {
        this.sessions.get(taskId)?.client.send(msg);
    }
    onMessage(taskId, handler) {
        this.messageHandlers.set(taskId, handler);
    }
    close(taskId) {
        const session = this.sessions.get(taskId);
        session?.client.close();
        this.sessions.delete(taskId);
        this.messageHandlers.delete(taskId);
    }
}
