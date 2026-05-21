import { spawn, type ChildProcess } from "child_process";
import type { AcpMessage } from "@agent-mesh/shared/src/protocol.js";

export class AgentSubprocess {
  private proc: ChildProcess | null = null;
  private messageBuffer = "";
  public onMessage: (msg: AcpMessage) => void = () => {};
  public onExit: (code: number | null, signal: string | null) => void = () => {};

  constructor(
    private command: string,
    private args: string[]
  ) {}

  start(): void {
    if (this.proc) return;

    // Cross-platform: setsid only exists on Linux, not macOS.
    // Linux: use setsid to fully detach from controlling TTY.
    // macOS: use nohup + bash background to achieve same effect.
    const isLinux = process.platform === "linux";

    if (isLinux) {
      // setsid creates a new session with no controlling terminal —
      // prevents SIGTTIN/SIGTTOU when Python asyncio reads stdin in a PTY.
      const cmd = "/usr/bin/setsid";
      const cmdArgs = [this.command, ...this.args];
      this.proc = spawn(cmd, cmdArgs, {
        stdio: ["ignore", "pipe", "pipe", "pipe"],
        env: { ...process.env },
      });
    } else {
      // macOS: spawn directly with stdin connected (no nohup/bash wrapper that breaks pipe)
      this.proc = spawn(this.command, this.args, {
        stdio: ["pipe", "pipe", "pipe"],
        env: { ...process.env },
      });
    }

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
    const data = JSON.stringify(msg) + "\n";
    const written = this.proc.stdin.write(data);
    console.log(`[Worker] stdin.write(${written}): ${data.slice(0, 80)}`);
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