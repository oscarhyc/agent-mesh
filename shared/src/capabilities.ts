export interface AgentCapabilities {
  agentType: string;         // "hermes" | "claude-code" | "opencode" | etc.
  skills?: string[];         // e.g. ["coding", "research", "writing"]
  maxConcurrent: number;     // max simultaneous tasks (default 1)
  version?: string;          // agent version string
  metadata?: Record<string, string>;
}