import { RegistryClient } from "./registry-client.js";
import { BridgeClient } from "./bridge-client.js";
import type { AcpMessage, AgentInfo } from "@agent-mesh/shared/src/protocol.js";

export interface MeshConfig {
  registry: string;
  defaultAgentType?: string;
  reconnectAttempts?: number;
  reconnectDelayMs?: number;
}

export interface MeshSession {
  agentId: string;
  taskId: string;
  host: string;
  port: number;
  client: BridgeClient;
}

export interface MeshEvent {
  type: string;
  [key: string]: unknown;
}

export class MeshRuntime {
  private registry: RegistryClient;
  private sessions = new Map<string, MeshSession>();
  private messageHandlers = new Map<string, (msg: AcpMessage) => void>();
  // AsyncIterable event queues per session — buffers messages from BridgeClient
  private eventQueues = new Map<string, Array<MeshEvent>>();
  private eventQueueListeners = new Map<string, (event: MeshEvent) => void>();

  constructor(private config: MeshConfig) {
    this.registry = new RegistryClient(config.registry);
  }

  async spawn(agentId: string, taskId: string): Promise<MeshSession | null> {
    const agents = await this.registry.discover({ agentType: agentId }, 1);
    if (agents.length === 0) {
      console.log(`[MeshRuntime] No agents available for type=${agentId}`);
      return null;
    }

    const agent: AgentInfo = agents[0];
    console.log(`[MeshRuntime] Spawning task=${taskId} on agent=${agent.agentId} (${agent.host}:${agent.port})`);

    return new Promise((resolve, reject) => {
      const client = new BridgeClient(
        agent.host,
        agent.port,
        (msg: AcpMessage) => {
          // Push to event queue AND call legacy handler
          this.pushEvent(taskId, msg);
        },
        () => {
          this.respawn(taskId, agentId).then(resolve).catch(reject);
        }
      );

      client.connect()
        .then(() => {
          this.sessions.set(taskId, { agentId: agent.agentId, taskId, host: agent.host, port: agent.port, client });
          resolve({ agentId: agent.agentId, taskId, host: agent.host, port: agent.port, client });
        })
        .catch(reject);
    });
  }

  private async respawn(taskId: string, agentType: string): Promise<MeshSession | null> {
    console.log(`[MeshRuntime] Respawning task=${taskId}`);
    return this.spawn(agentType, taskId); // spawn(agentType, taskId) — correct arg order
  }

  /**
   * AsyncGenerator of MeshEvents for a session — yields as messages arrive
   * from the worker via BridgeClient.onMessage.
   */
  async *events(sessionKey: string): AsyncGenerator<MeshEvent> {
    const queue = this.eventQueues.get(sessionKey) ?? [];
    this.eventQueues.set(sessionKey, queue);

    while (true) {
      // Yield immediately if there are queued events
      while (queue.length > 0) {
        yield queue.shift()!;
      }
      // Wait for next event via listener
      yield await new Promise<MeshEvent>((resolve) => {
        this.eventQueueListeners.set(sessionKey, (event) => resolve(event));
      });
    }
  }

  /**
   * Internal: called by BridgeClient when a message arrives from the worker.
   * Routes it into the event queue for the session.
   */
  private pushEvent(sessionKey: string, msg: AcpMessage): void {
    const queue = this.eventQueues.get(sessionKey) ?? [];
    queue.push(msg as MeshEvent);
    this.eventQueues.set(sessionKey, queue);

    const listener = this.eventQueueListeners.get(sessionKey);
    if (listener) {
      this.eventQueueListeners.delete(sessionKey);
      listener(msg as MeshEvent);
    }

    // Also call the legacy callback handler if set
    const legacyHandler = this.messageHandlers.get(sessionKey);
    legacyHandler?.(msg);
  }

  send(sessionKey: string, msg: AcpMessage): void {
    this.sessions.get(sessionKey)?.client.send(msg);
  }

  onMessage(sessionKey: string, handler: (msg: AcpMessage) => void): void {
    this.messageHandlers.set(sessionKey, handler);
  }

  close(sessionKey: string): void {
    const session = this.sessions.get(sessionKey);
    session?.client.close();
    this.sessions.delete(sessionKey);
    this.messageHandlers.delete(sessionKey);
    this.eventQueues.delete(sessionKey);
    this.eventQueueListeners.delete(sessionKey);
  }
}