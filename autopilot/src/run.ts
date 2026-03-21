import { existsSync, mkdirSync } from "fs";
import { join } from "path";
import { parseArgs } from "util";

import { Codex } from "@openai/codex-sdk";

import { loadRuntimeConfig } from "./config.js";
import { AUTOPILOT_ROOT, PROJECT_ROOT } from "./paths.js";
import { verifyProjectAgentProfiles, verifyProjectSkillsInstalled } from "./preflight.js";
import { buildResumePlan, getTaskResumeState } from "./resume.js";
import { runTask } from "./runner.js";
import { createSessionDir, readState, saveState, saveSummary } from "./session.js";
import type { CliOverrides, RunPhase } from "./types.js";

const { values } = parseArgs({
  options: {
    tasks: { type: "string" },
    "max-cycles": { type: "string" },
    timeout: { type: "string" },
    model: { type: "string" },
    "dry-run": { type: "boolean", default: false },
    "preflight-only": { type: "boolean", default: false },
    resume: { type: "string" },
    phase: { type: "string" },
  },
});

function parseNumber(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? undefined : parsed;
}

function buildCliOverrides(): CliOverrides {
  return {
    tasks: parseNumber(values.tasks),
    maxCycles: parseNumber(values["max-cycles"]),
    timeoutSeconds: parseNumber(values.timeout),
    model: values.model,
    dryRun: values["dry-run"] ?? false,
    preflightOnly: values["preflight-only"] ?? false,
    resume: values.resume,
    phase: values.phase as RunPhase | undefined,
  };
}

let shutdown = false;
process.on("SIGINT", () => {
  if (shutdown) process.exit(1);
  shutdown = true;
  console.log("\nCtrl+C — finishing current turn, then stopping…");
});
process.on("SIGTERM", () => {
  shutdown = true;
});

function sessionDirFor(resume: string | undefined): string {
  return resume ? join(AUTOPILOT_ROOT, "sessions", resume) : createSessionDir();
}

function runPreflightChecks(): { skills: string[]; agents: string[] } {
  return {
    skills: verifyProjectSkillsInstalled(),
    agents: verifyProjectAgentProfiles(),
  };
}

function buildCodexEnv(): Record<string, string> {
  return Object.fromEntries(
    Object.entries(process.env).filter((entry): entry is [string, string] => typeof entry[1] === "string"),
  );
}

async function main(): Promise<void> {
  delete process.env.SSH_AUTH_SOCK;
  delete process.env.OPENAI_API_KEY;
  delete process.env.CODEX_API_KEY;
  process.env.GIT_CONFIG_COUNT = "3";
  process.env.GIT_CONFIG_KEY_0 = "commit.gpgsign";
  process.env.GIT_CONFIG_VALUE_0 = "false";
  process.env.GIT_CONFIG_KEY_1 = "tag.gpgsign";
  process.env.GIT_CONFIG_VALUE_1 = "false";
  process.env.GIT_CONFIG_KEY_2 = "gpg.format";
  process.env.GIT_CONFIG_VALUE_2 = "";

  const cli = buildCliOverrides();
  const config = loadRuntimeConfig(PROJECT_ROOT, cli);

  if (cli.preflightOnly) {
    console.log("\x1b[1mAutopilot Preflight\x1b[0m");
    console.log(
      `  timeout=${config.timeoutSeconds}s  model=${config.model}  sandbox=${config.sandboxMode}  verify=${config.verifySandboxMode}  multi_agent=${config.multiAgent}  web_search=${config.webSearch}`,
    );
    for (const warning of config.warnings) {
      console.log(`  \x1b[33mwarning:\x1b[0m ${warning}`);
    }
    const verified = runPreflightChecks();
    console.log(`  project_skills=${verified.skills.join(", ")}`);
    console.log(`  project_agents=${verified.agents.join(", ")}`);
    console.log("  status=ok");
    process.exit(0);
  }

  const sessionDir = sessionDirFor(cli.resume);

  if (cli.resume && !existsSync(sessionDir)) {
    console.error(`\x1b[31mSession not found: ${sessionDir}\x1b[0m`);
    process.exit(1);
  }
  const resumeState = cli.resume ? readState(sessionDir) : null;
  if (cli.resume && !resumeState) {
    console.error(`\x1b[31mSession state not found or unreadable: ${join(sessionDir, "session.json")}\x1b[0m`);
    process.exit(1);
  }
  if (!cli.resume) mkdirSync(sessionDir, { recursive: true });
  const resumePlan = buildResumePlan(resumeState, cli.phase);

  console.log(`\x1b[1mAutopilot\x1b[0m  ${sessionDir.split("/").pop()}`);
  console.log(
    `  tasks=${config.taskLimit || "∞"}  cycles=${config.maxCycles}  timeout=${config.timeoutSeconds}s  model=${config.model}${resumePlan.phase ? `  phase=${resumePlan.phase}` : ""}${cli.resume ? `  resume=${cli.resume}` : ""}`,
  );
  console.log(
    `  sandbox=${config.sandboxMode}  verify=${config.verifySandboxMode}  multi_agent=${config.multiAgent}  web_search=${config.webSearch}`,
  );
  for (const warning of config.warnings) {
    console.log(`  \x1b[33mwarning:\x1b[0m ${warning}`);
  }
  const verified = runPreflightChecks();
  console.log(`  project_skills=${verified.skills.join(", ")}`);
  console.log(`  project_agents=${verified.agents.join(", ")}`);
  console.log("  Ctrl+C to stop gracefully\n");

  const codex = new Codex({ env: buildCodexEnv() });
  let completedCount = resumePlan.completed;
  let cleanCount = resumePlan.clean;
  let dirtyCount = resumePlan.dirty;

  for (let taskNum = resumePlan.startTaskNum; !shutdown; taskNum++) {
    if (config.taskLimit > 0 && taskNum > config.taskLimit) break;
    const success = await runTask(
      codex,
      taskNum,
      sessionDir,
      config,
      { dryRun: cli.dryRun, phase: taskNum === resumePlan.startTaskNum ? resumePlan.phase : undefined, resumeState: getTaskResumeState(taskNum, resumeState) },
      () => shutdown,
    );
    if (success === null) break;
    completedCount++;
    if (success) cleanCount++;
    else dirtyCount++;
    saveState(sessionDir, {
      current_task: taskNum,
      phase: "done",
      cycle: undefined,
      impl_thread_id: null,
      total_tasks: config.taskLimit,
      completed: completedCount,
      clean: cleanCount,
      dirty: dirtyCount,
      last_runtime_error: null,
    });
  }

  console.log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log(`\x1b[1mSummary\x1b[0m  ${cleanCount}/${completedCount} clean, ${dirtyCount} dirty`);
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  saveSummary(sessionDir, completedCount, cleanCount, config.model);
  process.exit(dirtyCount === 0 ? 0 : 1);
}

main().catch((error) => {
  console.error("Autopilot error:", error);
  process.exit(1);
});
