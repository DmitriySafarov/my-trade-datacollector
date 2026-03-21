import { writeFileSync } from "fs";
import { join } from "path";

import { Codex } from "@openai/codex-sdk";

import { PROJECT_ROOT } from "./paths.js";
import type { AgentRunResult, RunnerOptions, RuntimeConfig } from "./types.js";

interface AgentCallOptions {
  taskDir: string;
  phaseName: string;
  threadId?: string;
  sandboxMode?: RuntimeConfig["sandboxMode"];
}

function previewMessage(text: string): string {
  return text.slice(0, 120).replace(/\n/g, " ").trim();
}

export async function callAgent(
  codex: Codex,
  prompt: string,
  config: RuntimeConfig,
  runner: RunnerOptions,
  opts: AgentCallOptions,
): Promise<AgentRunResult> {
  if (runner.dryRun) {
    writeFileSync(join(opts.taskDir, `${opts.phaseName}-prompt.md`), prompt);
    console.log(`  Dry run: wrote ${opts.phaseName}-prompt.md`);
    return {
      status: "dry_run",
      threadId: opts.threadId ?? null,
      turnCount: 0,
    };
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), config.timeoutSeconds * 1000);
  let threadId: string | null = null;
  let turnCount = 0;
  let failureMessage: string | null = null;

  try {
    const threadOpts = {
      model: config.model,
      modelReasoningEffort: config.reasoningEffort,
      workingDirectory: PROJECT_ROOT,
      approvalPolicy: config.approvalPolicy,
      sandboxMode: opts.sandboxMode ?? config.sandboxMode,
      webSearchMode: config.webSearchMode,
    };
    const thread = opts.threadId
      ? codex.resumeThread(opts.threadId, threadOpts)
      : codex.startThread(threadOpts);
    const streamedTurn = await thread.runStreamed(prompt, { signal: controller.signal });

    for await (const event of streamedTurn.events) {
      const eventType = (event as Record<string, unknown>).type as string ?? "";
      const item = (event as Record<string, unknown>).item as Record<string, unknown> ?? {};
      const itemType = item.type as string ?? "";

      if (eventType === "thread.started") {
        threadId = ((event as Record<string, unknown>).thread_id as string | undefined) ?? threadId;
      }

      if (eventType === "turn.failed") {
        const error = (event as Record<string, unknown>).error as Record<string, unknown> | undefined;
        failureMessage = (error?.message as string | undefined) ?? "Agent turn failed";
        break;
      }

      if (eventType === "error") {
        failureMessage = ((event as Record<string, unknown>).message as string | undefined) ?? "Agent stream failed";
        break;
      }

      if (eventType === "item.completed" && itemType === "agent_message") {
        const text = item.text as string ?? "";
        const preview = previewMessage(text);
        if (preview) {
          turnCount++;
          console.log(`  \x1b[2m${String(turnCount).padStart(3)}\x1b[0m  ${preview}${text.length > 120 ? "…" : ""}`);
        }
      }

      if (eventType === "item.completed" && itemType === "command_execution") {
        const command = (item.command as string ?? "").slice(0, 60);
        const exitCode = item.exit_code as number ?? "?";
        const color = exitCode === 0 ? "32" : "31";
        console.log(`  \x1b[2m${String(turnCount).padStart(3)}\x1b[0m  \x1b[${color}m$ ${command} → exit=${exitCode}\x1b[0m`);
      }

      if (eventType === "turn.completed") {
        const usage = (event as Record<string, unknown>).usage as Record<string, unknown> ?? {};
        console.log(`  \x1b[2m─── turn ${turnCount}: in=${usage.input_tokens ?? "?"} out=${usage.output_tokens ?? "?"}\x1b[0m`);
      }
    }

    threadId = thread.id ?? threadId;
    if (controller.signal.aborted) {
      console.error(`  \x1b[31mPhase timed out after ${config.timeoutSeconds}s\x1b[0m`);
      return {
        status: "timed_out",
        threadId,
        turnCount,
        error: `Phase timed out after ${config.timeoutSeconds}s`,
      };
    }
    if (failureMessage) {
      console.error(`  \x1b[31mAgent error: ${failureMessage}\x1b[0m`);
      return {
        status: "failed",
        threadId,
        turnCount,
        error: failureMessage,
      };
    }
    console.log(`  \x1b[2m─── phase ${opts.phaseName} done (${turnCount} turns)\x1b[0m`);
    return {
      status: "completed",
      threadId,
      turnCount,
    };
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    if (controller.signal.aborted) {
      console.error(`  \x1b[31mPhase timed out after ${config.timeoutSeconds}s\x1b[0m`);
      return {
        status: "timed_out",
        threadId,
        turnCount,
        error: `Phase timed out after ${config.timeoutSeconds}s`,
      };
    } else {
      console.error(`  \x1b[31mAgent error: ${message}\x1b[0m`);
      return {
        status: "failed",
        threadId,
        turnCount,
        error: message,
      };
    }
  } finally {
    clearTimeout(timer);
  }
}
