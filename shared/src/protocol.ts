import type { AgentCapabilities } from './capabilities.js';

// ─── JSON-RPC Base Types ──────────────────────────────────────────────────────

export interface JsonRpcRequest {
  jsonrpc: "2.0";
  method: string;
  params?: object;
  id: number | string;
}

export interface JsonRpcResponse {
  jsonrpc: "2.0";
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
  id: number | string;
}

// ─── Worker ↔ Registry Messages ───────────────────────────────────────────────

export interface WorkerRegisterParams {
  agentId: string;
  host: string;
  port: number;
  capabilities: AgentCapabilities;
}

export interface WorkerRegisterRequest extends JsonRpcRequest {
  method: "register";
  params: WorkerRegisterParams;
}

export interface TaskResultParams {
  taskId: string;
  done: boolean;
  chunk?: string;
  final?: string;
}

export interface TaskResultRequest extends JsonRpcRequest {
  method: "result";
  params: TaskResultParams;
}

export interface DeregisterParams {
  reason: "shutdown" | "crash" | "kicked";
}

export interface DeregisterRequest extends JsonRpcRequest {
  method: "deregister";
  params: DeregisterParams;
}

// ─── Registry → Worker Messages ───────────────────────────────────────────────

export interface TaskParams {
  taskId: string;
  sessionKey: string;
  prompt: string;
}

export interface TaskRequest extends JsonRpcRequest {
  method: "task";
  params: TaskParams;
}

export interface CancelParams {
  taskId: string;
}

export interface CancelRequest extends JsonRpcRequest {
  method: "cancel";
  params: CancelParams;
}

// ─── Heartbeat ────────────────────────────────────────────────────────────────

export interface PingRequest extends JsonRpcRequest {
  method: "ping";
  params: Record<string, never>;
}

export interface PongResponse extends JsonRpcResponse {
  result: { pong: true };
}

// ─── Discovery ────────────────────────────────────────────────────────────────

export interface DiscoverParams {
  capabilities: Partial<AgentCapabilities>;
  limit?: number;
}

export interface DiscoverRequest extends JsonRpcRequest {
  method: "discover";
  params: DiscoverParams;
}

export interface AgentInfo {
  agentId: string;
  host: string;
  port: number;
  capabilities: AgentCapabilities;
  load: number;
  status: "idle" | "busy" | "offline";
  lastHeartbeat: number;
}

export interface DiscoverResult {
  agents: AgentInfo[];
}

export interface DiscoverResponse extends JsonRpcResponse {
  result: DiscoverResult;
}

// ─── ACP Raw Message ──────────────────────────────────────────────────────────

export interface AcpMessage {
  type: string;
  [key: string]: unknown;
}