import { randomUUID } from "crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "fs";
import { join } from "path";

import { AUTOPILOT_ROOT } from "./paths.js";
import type { SessionState, VerifyResult } from "./types.js";

export function createSessionDir(): string {
  const now = new Date();
  const ts = [
    now.getFullYear(),
    String(now.getMonth() + 1).padStart(2, "0"),
    String(now.getDate()).padStart(2, "0"),
  ].join("-") + "_" + [
    String(now.getHours()).padStart(2, "0"),
    String(now.getMinutes()).padStart(2, "0"),
    String(now.getSeconds()).padStart(2, "0"),
  ].join("-");
  const dir = join(AUTOPILOT_ROOT, "sessions", `${ts}_${randomUUID().slice(0, 8)}`);
  mkdirSync(dir, { recursive: true });
  return dir;
}

export function createTaskDir(sessionDir: string, taskNum: number): string {
  const dir = join(sessionDir, `task-${String(taskNum).padStart(3, "0")}`);
  mkdirSync(dir, { recursive: true });
  return dir;
}

export function readState(sessionDir: string): SessionState | null {
  const path = join(sessionDir, "session.json");
  if (!existsSync(path)) return null;

  try {
    return JSON.parse(readFileSync(path, "utf-8")) as SessionState;
  } catch {
    return null;
  }
}

export function saveState(sessionDir: string, state: Partial<SessionState>): SessionState {
  const nextState = { ...(readState(sessionDir) ?? {}), ...state } as SessionState;
  writeFileSync(join(sessionDir, "session.json"), JSON.stringify(nextState, null, 2));
  return nextState;
}

export function readSessionLogs(taskDir: string, excludePrefixes: string[] = []): string {
  if (!existsSync(taskDir)) return "";
  return readdirSync(taskDir)
    .filter((file) => file.endsWith(".md") && !file.endsWith("-prompt.md"))
    .filter((file) => !excludePrefixes.some((prefix) => file.startsWith(prefix)))
    .sort()
    .map((file) => `## File: ${file}\n${readFileSync(join(taskDir, file), "utf-8")}`)
    .join("\n\n");
}

export function readVerifyFindings(taskDir: string, attempt: number): string {
  const path = join(taskDir, `verify-${attempt}-result.json`);
  if (!existsSync(path)) return "";

  try {
    const data = JSON.parse(readFileSync(path, "utf-8")) as VerifyResult;
    if (!data.findings.length) return "";
    const lines = ["# Findings From Verify", ""];
    for (const finding of data.findings) {
      lines.push(`### [${finding.severity}] ${finding.id}: ${finding.title}`);
      lines.push(`**File:** ${finding.file}:${finding.line ?? "?"}`);
      lines.push(`**Description:** ${finding.description}`);
      lines.push(`**Fix:** ${finding.recommended_fix}`);
      lines.push("");
    }
    return lines.join("\n");
  } catch {
    return "";
  }
}

export function isVerifyClean(taskDir: string, attempt: number): boolean | null {
  const path = join(taskDir, `verify-${attempt}-result.json`);
  if (!existsSync(path)) return null;

  try {
    const result = JSON.parse(readFileSync(path, "utf-8")) as VerifyResult;
    if (result.clean) return true;
    return (
      result.findings.length === 0
      && result.ruff_passed
      && result.syntax_passed
      && result.tests_passed
    );
  } catch {
    return null;
  }
}

export function readTaskLabel(taskDir: string): string | null {
  const contextPath = join(taskDir, "context.md");
  if (!existsSync(contextPath)) return null;

  for (const line of readFileSync(contextPath, "utf-8").split("\n")) {
    if (line.startsWith("# ") || line.startsWith("## Task")) {
      return line.replace(/^#+\s*/, "");
    }
  }
  return null;
}

export function hasNoActionableTasks(taskDir: string): boolean {
  const contextPath = join(taskDir, "context.md");
  if (!existsSync(contextPath)) return false;
  return readFileSync(contextPath, "utf-8").includes("no_actionable_tasks");
}

export function saveSummary(
  sessionDir: string,
  totalTasks: number,
  cleanTasks: number,
  model: string,
): void {
  const summary = `# Autopilot Session Summary

- **Date**: ${new Date().toISOString()}
- **Tasks**: ${totalTasks}
- **Clean**: ${cleanTasks}
- **Dirty**: ${totalTasks - cleanTasks}
- **Model**: ${model}
`;
  writeFileSync(join(sessionDir, "summary.md"), summary);
}
