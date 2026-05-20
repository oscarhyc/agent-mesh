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

export class MeshRuntime {
  private registry: RegistryClient;
  private sessions = new Map<string, MeshSession>();
  private messageHandlers = new Map<string, (msg: AcpMessage) => void>();

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
          const handler = this.messageHandlers.get(taskId);
          handler?.(msg);
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
    return this.spawn(agentType, taskId);
  }

  send(taskId: string, msg: AcpMessage): void {
    this.sessions.get(taskId)?.client.send(msg);
  }

  onMessage(taskId: string, handler: (msg: AcpMessage) => void): void {
    this.messageHandlers.set(taskId, handler);
  }

  close(taskId: string): void {
    const session = this.sessions.get(taskId);
    session?.client.close();
    this.sessions.delete(taskId);
    this.messageHandlers.delete(taskId);
  }
}