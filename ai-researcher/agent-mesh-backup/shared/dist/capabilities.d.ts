export interface AgentCapabilities {
    agentType: string;
    skills?: string[];
    maxConcurrent: number;
    version?: string;
    metadata?: Record<string, string>;
}
