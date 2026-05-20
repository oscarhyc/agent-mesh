import { MeshRuntime, type MeshConfig } from "./mesh-runtime.js";

let meshRuntime: MeshRuntime | null = null;

export function registerMeshRuntime(config: MeshConfig): void {
  meshRuntime = new MeshRuntime(config);
  console.log(`[agent-mesh-bridge] Registered with registry=${config.registry}`);
}

export function getMeshRuntime(): MeshRuntime | null {
  return meshRuntime;
}

export { MeshRuntime } from "./mesh-runtime.js";
export type { MeshConfig, MeshSession } from "./mesh-runtime.js";