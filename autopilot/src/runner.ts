import { Codex } from "@openai/codex-sdk";

import { runPhaseWithRetry } from "./phase-runner.js";
import {
  createTaskDir,
  evaluateVerifyGate,
  hasNoActionableTasks,
  readTaskLabel,
  saveState,
} from "./session.js";
import { assertArtifacts, assertPhaseCompleted, failTask } from "./runtime-guards.js";
import {
  buildFinishPrompt,
  buildFixPrompt,
  buildStartPrompt,
  buildVerifyPrompt,
} from "./prompts.js";
import type { RunnerOptions, RuntimeConfig } from "./types.js";

export async function runTask(
  codex: Codex,
  taskNum: number,
  sessionDir: string,
  config: RuntimeConfig,
  runner: RunnerOptions,
  shutdownRequested: () => boolean,
): Promise<boolean | null> {
  const taskDir = createTaskDir(sessionDir, taskNum);
  const resumeState = runner.resumeState?.current_task === taskNum ? runner.resumeState : null;
  const resumePhase = runner.phase;
  const startCycle = resumeState?.cycle && (resumePhase === "verify" || resumePhase === "fix")
    ? Math.max(1, resumeState.cycle)
    : 1;
  let implThreadId = resumeState?.impl_thread_id ?? null;

  console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`Task ${taskNum}`);
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

  if (!resumePhase || resumePhase === "start") {
    console.log("\n\x1b[1;34m▸ IMPLEMENT\x1b[0m");
    saveState(sessionDir, { current_task: taskNum, phase: "start", cycle: undefined, last_runtime_error: null });
    const startResult = await runPhaseWithRetry(
      codex,
      buildStartPrompt(taskDir, config),
      config,
      runner,
      {
        taskDir,
        phaseName: "start",
        label: "IMPLEMENT",
        threadId: implThreadId ?? undefined,
        artifactsToReset: ["context.md", "start.md"],
      },
    );
    assertPhaseCompleted(startResult, sessionDir, taskNum, "start", "IMPLEMENT", undefined, implThreadId);
    implThreadId = startResult.threadId ?? implThreadId;
    saveState(sessionDir, { current_task: taskNum, impl_thread_id: implThreadId, last_runtime_error: null });
    assertArtifacts(taskDir, sessionDir, taskNum, "start", "IMPLEMENT", ["context.md"], runner, undefined, implThreadId);

    if (shutdownRequested()) {
      console.log("\x1b[33mStopped after IMPLEMENT phase.\x1b[0m");
      return false;
    }
    if (hasNoActionableTasks(taskDir)) {
      console.log("  \x1b[33mNo actionable tasks — stopping autopilot\x1b[0m");
      saveState(sessionDir, {
        current_task: taskNum,
        phase: "skipped",
        cycle: undefined,
        impl_thread_id: null,
        last_runtime_error: null,
      });
      return null;
    }
    assertArtifacts(taskDir, sessionDir, taskNum, "start", "IMPLEMENT", ["start.md"], runner, undefined, implThreadId);
  }

  if (resumePhase === "start") return true;
  const taskLabel = readTaskLabel(taskDir);
  if (taskLabel) console.log(`  \x1b[2m${taskLabel}\x1b[0m`);

  let clean = false;
  if (resumePhase !== "finish") {
    for (let cycle = startCycle; cycle <= config.maxCycles; cycle++) {
      const resumeIntoFix = resumePhase === "fix" && cycle === startCycle;
      if (shutdownRequested()) {
        console.log("\x1b[33mStopped before VERIFY.\x1b[0m");
        break;
      }

      if (!resumeIntoFix) {
        console.log(`\n\x1b[1;35m▸ VERIFY (${cycle}/${config.maxCycles})\x1b[0m`);
        saveState(sessionDir, { current_task: taskNum, phase: "verify", cycle, impl_thread_id: implThreadId, last_runtime_error: null });
        const verifyResult = await runPhaseWithRetry(
          codex,
          buildVerifyPrompt(taskDir, cycle, config),
          config,
          runner,
          {
            taskDir,
            phaseName: `verify-${cycle}`,
            label: `VERIFY (${cycle})`,
            sandboxMode: config.verifySandboxMode,
            artifactsToReset: [`verify-${cycle}.md`, `verify-${cycle}-result.json`],
          },
        );
        assertPhaseCompleted(verifyResult, sessionDir, taskNum, "verify", `VERIFY (${cycle})`, cycle, implThreadId);
        assertArtifacts(
          taskDir,
          sessionDir,
          taskNum,
          "verify",
          `VERIFY (${cycle})`,
          [`verify-${cycle}.md`, `verify-${cycle}-result.json`],
          runner,
          cycle,
          implThreadId,
        );

        if (runner.dryRun) {
          console.log("  \x1b[2mDry run: skipping verify evaluation and fix loop\x1b[0m");
          clean = true;
          break;
        }

        const gate = evaluateVerifyGate(taskDir, cycle);
        if (gate === null) {
          failTask(sessionDir, taskNum, "verify", `VERIFY (${cycle}) produced an unreadable result file`, cycle, implThreadId);
        }
        if (gate.invalidReason) {
          failTask(sessionDir, taskNum, "verify", `VERIFY (${cycle}) produced an invalid gate result: ${gate.invalidReason}`, cycle, implThreadId);
        }
        if (gate.clean) {
          console.log(`  \x1b[1;32m✓ CLEAN\x1b[0m on cycle ${cycle}`);
          clean = true;
          break;
        }
      }

      if (shutdownRequested()) {
        console.log("\x1b[33mStopped before FIX.\x1b[0m");
        break;
      }

      console.log(`\n\x1b[1;34m▸ FIX (${cycle})\x1b[0m`);
      saveState(sessionDir, { current_task: taskNum, phase: "fix", cycle, impl_thread_id: implThreadId, last_runtime_error: null });
      const fixResult = await runPhaseWithRetry(
        codex,
        buildFixPrompt(taskDir, cycle, config),
        config,
        runner,
        {
          taskDir,
          phaseName: `implement-${cycle}`,
          label: `FIX (${cycle})`,
          threadId: implThreadId ?? undefined,
          artifactsToReset: [`implement-${cycle}.md`],
        },
      );
      assertPhaseCompleted(fixResult, sessionDir, taskNum, "fix", `FIX (${cycle})`, cycle, implThreadId);
      implThreadId = fixResult.threadId ?? implThreadId;
      saveState(sessionDir, { current_task: taskNum, impl_thread_id: implThreadId, last_runtime_error: null });
      assertArtifacts(taskDir, sessionDir, taskNum, "fix", `FIX (${cycle})`, [`implement-${cycle}.md`], runner, cycle, implThreadId);
    }

    if (!clean && !shutdownRequested() && !runner.dryRun) {
      console.error("  \x1b[31mMax cycles reached — proceeding to finish\x1b[0m");
    }
  }

  if (resumePhase === "verify") return clean;
  if (!shutdownRequested()) {
    console.log("\n\x1b[1;32m▸ FINISH\x1b[0m");
    saveState(sessionDir, { current_task: taskNum, phase: "finish", impl_thread_id: implThreadId, last_runtime_error: null });
    const finishResult = await runPhaseWithRetry(
      codex,
      buildFinishPrompt(taskDir, config),
      config,
      runner,
      {
        taskDir,
        phaseName: "finish",
        label: "FINISH",
        threadId: implThreadId ?? undefined,
        artifactsToReset: ["finish.md"],
      },
    );
    assertPhaseCompleted(finishResult, sessionDir, taskNum, "finish", "FINISH", undefined, implThreadId);
    assertArtifacts(taskDir, sessionDir, taskNum, "finish", "FINISH", ["finish.md"], runner, undefined, implThreadId);
  }

  console.log(`\n  Task ${taskNum} → ${clean ? "\x1b[32mclean\x1b[0m" : "\x1b[31mdirty\x1b[0m"}`);
  return clean;
}
