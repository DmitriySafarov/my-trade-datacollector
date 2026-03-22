import { existsSync, rmSync } from "fs";
import { join } from "path";

import { Codex } from "@openai/codex-sdk";

import { callAgent } from "./agent.js";
import type { AgentRunResult, RunnerOptions, RuntimeConfig } from "./types.js";

const TRANSIENT_AGENT_RETRIES = 2;
const RETRYABLE_ERROR_PATTERNS = [
  "reconnecting",
  "timeout waiting for child process to exit",
  "connection closed",
  "socket hang up",
  "econnreset",
];

interface PhaseRunnerOptions {
  artifactsToReset?: string[];
  taskDir: string;
  phaseName: string;
  label: string;
  threadId?: string | null;
  sandboxMode?: RuntimeConfig["sandboxMode"];
}

function isRetryableAgentFailure(result: AgentRunResult): boolean {
  if (result.status !== "failed") return false;
  const message = (result.error ?? "").toLowerCase();
  return RETRYABLE_ERROR_PATTERNS.some((pattern) => message.includes(pattern));
}

function delayFor(attempt: number): Promise<void> {
  const delayMs = attempt * 5000;
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}

function resetArtifacts(taskDir: string, files: string[] | undefined): void {
  if (!files?.length) return;
  for (const file of files) {
    const path = join(taskDir, file);
    if (existsSync(path)) rmSync(path, { force: true });
  }
}

export async function runPhaseWithRetry(
  codex: Codex,
  prompt: string,
  config: RuntimeConfig,
  runner: RunnerOptions,
  opts: PhaseRunnerOptions,
): Promise<AgentRunResult> {
  let threadId = opts.threadId ?? null;
  let lastResult: AgentRunResult | null = null;

  for (let attempt = 0; attempt <= TRANSIENT_AGENT_RETRIES; attempt++) {
    resetArtifacts(opts.taskDir, opts.artifactsToReset);
    if (attempt > 0) {
      console.log(
        `  \x1b[33mRetrying ${opts.label} after transient agent failure (${attempt}/${TRANSIENT_AGENT_RETRIES})…\x1b[0m`,
      );
      await delayFor(attempt);
    }

    const result = await callAgent(codex, prompt, config, runner, {
      taskDir: opts.taskDir,
      phaseName: opts.phaseName,
      threadId: threadId ?? undefined,
      sandboxMode: opts.sandboxMode,
    });
    threadId = result.threadId ?? threadId;
    lastResult = { ...result, threadId };

    if (!isRetryableAgentFailure(result)) return lastResult;
    if (attempt === TRANSIENT_AGENT_RETRIES) return lastResult;

    console.log(
      `  \x1b[33mTransient agent failure during ${opts.label}: ${result.error ?? "unknown error"}\x1b[0m`,
    );
  }

  return lastResult ?? { status: "failed", threadId, turnCount: 0, error: "Phase failed before starting" };
}
