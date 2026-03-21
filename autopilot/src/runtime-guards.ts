import { existsSync } from "fs";
import { join } from "path";

import { saveState } from "./session.js";
import type { AgentRunResult, RunPhase, RunnerOptions } from "./types.js";

export function failTask(
  sessionDir: string,
  taskNum: number,
  phase: RunPhase,
  message: string,
  cycle?: number,
  implThreadId?: string | null,
): never {
  saveState(sessionDir, {
    current_task: taskNum,
    phase,
    cycle,
    impl_thread_id: implThreadId ?? null,
    last_runtime_error: message,
  });
  throw new Error(message);
}

export function assertPhaseCompleted(
  result: AgentRunResult,
  sessionDir: string,
  taskNum: number,
  phase: RunPhase,
  label: string,
  cycle?: number,
  implThreadId?: string | null,
): void {
  if (result.status === "completed" || result.status === "dry_run") return;
  failTask(
    sessionDir,
    taskNum,
    phase,
    `${label} failed: ${result.error ?? result.status}`,
    cycle,
    result.threadId ?? implThreadId,
  );
}

export function assertArtifacts(
  taskDir: string,
  sessionDir: string,
  taskNum: number,
  phase: RunPhase,
  label: string,
  files: string[],
  runner: RunnerOptions,
  cycle?: number,
  implThreadId?: string | null,
): void {
  if (runner.dryRun) return;
  const missing = files.filter((file) => !existsSync(join(taskDir, file)));
  if (missing.length === 0) return;
  failTask(
    sessionDir,
    taskNum,
    phase,
    `${label} is missing required artifacts: ${missing.join(", ")}`,
    cycle,
    implThreadId,
  );
}
