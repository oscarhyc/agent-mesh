import type { AgentCapabilities, AgentInfo, WorkerRegisterParams } from "@agent-mesh/shared/src/protocol.js";

export class Registry {
  private agents = new Map<string, AgentInfo>();
  private pingTimers = new Map<string, ReturnType<typeof setTimeout>>();
  private readonly PING_INTERVAL_MS = 10_000;
  private readonly PING_MISS_THRESHOLD = 3;

  // Track how many tasks each agent is currently running
  private activeTasks = new Map<string, number>();

  register(params: WorkerRegisterParams): AgentInfo {
    const info: AgentInfo = {
      agentId: params.agentId,
      host: params.host,
      port: params.port,
      capabilities: params.capabilities,
      load: this.activeTasks.get(params.agentId) ?? 0,
      status: "idle",
      lastHeartbeat: Date.now(),
    };
    this.agents.set(params.agentId, info);
    this.schedulePing(params.agentId);
    console.log(`[Registry] registered: ${params.agentId} (${params.host}:${params.port})`);
    return info;
  }

  deregister(agentId: string, reason: string): void {
    this.agents.delete(agentId);
    this.activeTasks.delete(agentId);
    this.clearPingTimer(agentId);
    console.log(`[Registry] ${agentId} deregistered: ${reason}`);
  }

  recordTaskStart(agentId: string): void {
    const current = this.activeTasks.get(agentId) ?? 0;
    this.activeTasks.set(agentId, current + 1);
    this.refreshAgent(agentId, { status: "busy", load: current + 1 });
  }

  recordTaskDone(agentId: string): void {
    const current = this.activeTasks.get(agentId) ?? 0;
    const newLoad = Math.max(0, current - 1);
    this.activeTasks.set(agentId, newLoad);
    this.refreshAgent(agentId, { status: "idle", load: newLoad });
  }

  discover(criteria: Partial<AgentCapabilities>, limit = 1): AgentInfo[] {
    const candidates = Array.from(this.agents.values())
      .filter(a => a.status !== "offline")
      .filter(a => {
        if (criteria.agentType && a.capabilities.agentType !== criteria.agentType) return false;
        if (criteria.skills && criteria.skills.length > 0) {
          const hasAllSkills = criteria.skills.every(skill => a.capabilities.skills?.includes(skill));
          if (!hasAllSkills) return false;
        }
        return true;
      })
      .sort((a, b) => a.load - b.load); // lowest load first

    return candidates.slice(0, limit);
  }

  listAgents(): AgentInfo[] {
    return Array.from(this.agents.values());
  }

  getAgent(agentId: string): AgentInfo | undefined {
    return this.agents.get(agentId);
  }

  refreshHeartbeat(agentId: string): void {
    const agent = this.agents.get(agentId);
    if (agent) {
      agent.lastHeartbeat = Date.now();
      if (agent.status === "offline") {
        agent.status = "idle";
      }
    }
  }

  private refreshAgent(agentId: string, patch: Partial<AgentInfo>): void {
    const agent = this.agents.get(agentId);
    if (agent) Object.assign(agent, patch);
  }

  private schedulePing(agentId: string): void {
    this.clearPingTimer(agentId);
    const timer = setTimeout(() => this.handlePingTimeout(agentId), this.PING_INTERVAL_MS * this.PING_MISS_THRESHOLD);
    this.pingTimers.set(agentId, timer);
  }

  private clearPingTimer(agentId: string): void {
    const t = this.pingTimers.get(agentId);
    if (t) { clearTimeout(t); this.pingTimers.delete(agentId); }
  }

  private handlePingTimeout(agentId: string): void {
    const agent = this.agents.get(agentId);
    if (agent && agent.status !== "offline") {
      agent.status = "offline";
      console.log(`[Registry] ${agentId} marked OFFLINE (missed ${this.PING_MISS_THRESHOLD} pings)`);
    }
  }
}
