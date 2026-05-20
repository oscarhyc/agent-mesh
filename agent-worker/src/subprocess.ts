import { spawn, type ChildProcess } from "child_process";
import type { AcpMessage } from "@agent-mesh/shared/src/protocol.js";

export class AgentSubprocess {
  private proc: ChildProcess | null = null;
  private messageBuffer = "";

  constructor(
    private command: string,
    private args: string[]
  ) {}

  start(): void {
    if (this.proc) return;
    this.proc = spawn(this.command, this.args, {
      stdio: ["pipe", "pipe", "pipe"],
      env: { ...process.env },
    });

    this.proc.stdout?.on("data", (chunk: Buffer) => {
      this.messageBuffer += chunk.toString();
      const lines = this.messageBuffer.split("\n");
      this.messageBuffer = lines.pop() ?? "";
      for (const line of lines) {
        if (line.trim()) {
          try {
            const msg = JSON.parse(line) as AcpMessage;
            this.onMessage(msg);
          } catch {}
        }
      }
    });

    this.proc.stderr?.on("data", (chunk: Buffer) => {
      console.error(`[Worker:stderr] ${chunk.toString().trim()}`);
    });

    this.proc.on("exit", (code, signal) => {
      console.log(`[Worker] subprocess exited code=${code} signal=${signal}`);
      this.onExit(code, signal);
    });

    this.proc.on("error", (err) => {
      console.error(`[Worker] subprocess error: ${err.message}`);
    });

    console.log(`[Worker] spawned: ${this.command} ${this.args.join(" ")}`);
  }

  send(msg: AcpMessage): void {
    if (!this.proc || !this.proc.stdin) {
      console.error("[Worker] subprocess not running, cannot send message");
      return;
    }
    this.proc.stdin.write(JSON.stringify(msg) + "\n");
  }

  kill(): void {
    if (!this.proc) return;
    this.proc.kill("SIGTERM");
    this.proc = null;
  }

  isRunning(): boolean {
    return this.proc !== null && this.proc.exitCode === null;
  }

  onMessage(_msg: AcpMessage): void {}
  onExit(_code: number | null, _signal: string | null): void {}
}