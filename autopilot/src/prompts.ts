import { buildPromptContext } from "./context.js";
import {
  buildAutopilotPreamble,
  buildFinishInstructions,
  buildFixInstructions,
  buildStartInstructions,
  buildVerifyInstructions,
} from "./instructions.js";
import { readSessionLogs, readVerifyFindings } from "./session.js";
import type { RuntimeConfig } from "./types.js";

function buildPreamble(kind: "start" | "verify" | "fix" | "finish", config: RuntimeConfig): string {
  return buildAutopilotPreamble(buildPromptContext(kind, config));
}

export function buildStartPrompt(taskDir: string, config: RuntimeConfig): string {
  return `${buildPreamble("start", config)}

# Session Log Directory
Write ALL logs and results to: ${taskDir}/

## Required artifacts
1. \`${taskDir}/context.md\` — chosen task, dependencies, blockers, rationale
2. \`${taskDir}/start.md\` — agent consultation summary, plan, changed files, decisions, issues

## Do not stop after planning
Implement the entire task. Run ruff and tests before stopping. Do not update tracker or commit in this phase.

---

${buildStartInstructions(config)}
`;
}

export function buildVerifyPrompt(taskDir: string, attempt: number, config: RuntimeConfig): string {
  return `${buildPreamble("verify", config)}

# Session Log Directory
Write ALL logs and results to: ${taskDir}/

## Verify iteration ${attempt}
- You are logically read-only for source code
- You may only create verification artifacts inside ${taskDir}/
- Do not read previous verify reports for this task

## Required artifacts
1. \`${taskDir}/verify-${attempt}.md\` — narrative review, checks run, findings
2. \`${taskDir}/verify-${attempt}-result.json\` — valid JSON with findings and pass/fail flags

## Required JSON keys
\`clean\`, \`critical\`, \`important\`, \`suggestion\`, \`ruff_passed\`, \`syntax_passed\`, \`tests_passed\`, \`findings\`

Set \`clean=true\` only if findings are empty and ruff, syntax, and tests all pass.

---

${buildVerifyInstructions(config)}
`;
}

export function buildFixPrompt(taskDir: string, attempt: number, config: RuntimeConfig): string {
  const findings = readVerifyFindings(taskDir, attempt);
  return `${buildPreamble("fix", config)}

# Session Log Directory
Write ALL logs and results to: ${taskDir}/

${findings}

## Required artifacts
1. Read the verify findings from iteration ${attempt}
2. Fix every finding
3. Write \`${taskDir}/implement-${attempt}.md\` — what changed and how the findings were resolved

---

${buildFixInstructions()}
`;
}

export function buildFinishPrompt(taskDir: string, config: RuntimeConfig): string {
  return `${buildPreamble("finish", config)}

# Session Log Directory
Write ALL logs and results to: ${taskDir}/

## Required artifact
1. \`${taskDir}/finish.md\` — tracker updates, commits, next-task suggestion

# Full Session Context

${readSessionLogs(taskDir)}

---

${buildFinishInstructions(config)}
`;
}
