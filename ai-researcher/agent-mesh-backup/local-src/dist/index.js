import { MeshRuntime } from "./mesh-runtime.js";
let meshRuntime = null;
export function registerMeshRuntime(config) {
    meshRuntime = new MeshRuntime(config);
    console.log(`[agent-mesh-bridge] Registered with registry=${config.registry}`);
}
export function getMeshRuntime() {
    return meshRuntime;
}
export { MeshRuntime } from "./mesh-runtime.js";
