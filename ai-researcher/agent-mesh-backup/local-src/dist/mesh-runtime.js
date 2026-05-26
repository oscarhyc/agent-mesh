import { RegistryClient } from "./registry-client.js";
import { BridgeClient } from "./bridge-client.js";
export class MeshRuntime {
    config;
    registry;
    sessions = new Map();
    messageHandlers = new Map();
    // AsyncIterable event queues per session — buffers messages from BridgeClient
    eventQueues = new Map();
    eventQueueListeners = new Map();
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
                // Push to event queue AND call legacy handler
                this.pushEvent(taskId, msg);
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
        return this.spawn(agentType, taskId); // spawn(agentType, taskId) — correct arg order
    }
    /**
     * AsyncGenerator of MeshEvents for a session — yields as messages arrive
     * from the worker via BridgeClient.onMessage.
     */
    async *events(sessionKey) {
        const queue = this.eventQueues.get(sessionKey) ?? [];
        this.eventQueues.set(sessionKey, queue);
        while (true) {
            // Yield immediately if there are queued events
            while (queue.length > 0) {
                yield queue.shift();
            }
            // Wait for next event via listener
            yield await new Promise((resolve) => {
                this.eventQueueListeners.set(sessionKey, (event) => resolve(event));
            });
        }
    }
    /**
     * Internal: called by BridgeClient when a message arrives from the worker.
     * Routes it into the event queue for the session.
     */
    pushEvent(sessionKey, msg) {
        const queue = this.eventQueues.get(sessionKey) ?? [];
        queue.push(msg);
        this.eventQueues.set(sessionKey, queue);
        const listener = this.eventQueueListeners.get(sessionKey);
        if (listener) {
            this.eventQueueListeners.delete(sessionKey);
            listener(msg);
        }
        // Also call the legacy callback handler if set
        const legacyHandler = this.messageHandlers.get(sessionKey);
        legacyHandler?.(msg);
    }
    send(sessionKey, msg) {
        this.sessions.get(sessionKey)?.client.send(msg);
    }
    onMessage(sessionKey, handler) {
        this.messageHandlers.set(sessionKey, handler);
    }
    close(sessionKey) {
        const session = this.sessions.get(sessionKey);
        session?.client.close();
        this.sessions.delete(sessionKey);
        this.messageHandlers.delete(sessionKey);
        this.eventQueues.delete(sessionKey);
        this.eventQueueListeners.delete(sessionKey);
    }
}
