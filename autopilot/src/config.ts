import { existsSync, readFileSync } from "fs";
import { join } from "path";

import type { CliOverrides, ReasoningEffort, RuntimeConfig, SandboxMode, WebSearchMode } from "./types.js";

type TomlValue = boolean | number | string;
type TomlTable = Record<string, TomlValue>;

interface ParsedToml {
  root: TomlTable;
  sections: Record<string, TomlTable>;
}

function parseTomlValue(rawValue: string): TomlValue {
  const value = rawValue.trim();
  if (value.startsWith("\"") && value.endsWith("\"")) {
    return value.slice(1, -1).replace(/\\"/g, "\"");
  }
  if (value === "true") return true;
  if (value === "false") return false;
  if (/^-?\d+$/.test(value)) return Number.parseInt(value, 10);
  return value;
}

function parseToml(text: string): ParsedToml {
  const parsed: ParsedToml = { root: {}, sections: {} };
  let current = parsed.root;

  for (const originalLine of text.split("\n")) {
    const line = originalLine.trim();
    if (!line || line.startsWith("#")) continue;
    if (line.startsWith("[") && line.endsWith("]")) {
      const section = line.slice(1, -1).trim();
      current = parsed.sections[section] ?? {};
      parsed.sections[section] = current;
      continue;
    }

    const separator = line.indexOf("=");
    if (separator === -1) continue;
    const key = line.slice(0, separator).trim();
    const rawValue = line.slice(separator + 1);
    current[key] = parseTomlValue(rawValue);
  }

  return parsed;
}

function getString(table: TomlTable | undefined, key: string): string | undefined {
  const value = table?.[key];
  return typeof value === "string" ? value : undefined;
}

function getNumber(table: TomlTable | undefined, key: string): number | undefined {
  const value = table?.[key];
  return typeof value === "number" ? value : undefined;
}

function getBoolean(table: TomlTable | undefined, key: string): boolean | undefined {
  const value = table?.[key];
  return typeof value === "boolean" ? value : undefined;
}

function getFeatureFlag(table: TomlTable | undefined, key: string): boolean | undefined {
  const value = table?.[key];
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    if (value === "false" || value === "off" || value === "disabled") return false;
    return true;
  }
  return undefined;
}

function parseSandboxMode(value: string | undefined, fallback: SandboxMode): SandboxMode {
  return value === "read-only" || value === "workspace-write" ? value : fallback;
}

function parseWebSearchMode(value: string | undefined, enabled: boolean): WebSearchMode {
  if (value === "disabled" || value === "cached" || value === "live") return value;
  return enabled ? "cached" : "disabled";
}

function parseReasoningEffort(value: string | undefined): ReasoningEffort {
  if (
    value === "minimal"
    || value === "low"
    || value === "medium"
    || value === "high"
    || value === "xhigh"
  ) {
    return value;
  }
  return "medium";
}

function positiveNumber(value: number | undefined, fallback: number): number {
  return value && value > 0 ? value : fallback;
}

function readCodexConfig(projectRoot: string): ParsedToml {
  const path = join(projectRoot, ".codex/config.toml");
  if (!existsSync(path)) return { root: {}, sections: {} };
  return parseToml(readFileSync(path, "utf-8"));
}

export function loadRuntimeConfig(projectRoot: string, cli: CliOverrides): RuntimeConfig {
  const codex = readCodexConfig(projectRoot);
  const autopilot = codex.sections.autopilot;
  const features = codex.sections.features;
  const agents = codex.sections.agents;
  const webSearch = getFeatureFlag(features, "web_search") ?? true;

  const warnings: string[] = [];
  const requestedApproval = getString(autopilot, "approval_policy");
  if (requestedApproval && requestedApproval !== "never") {
    warnings.push(
      `autopilot.approval_policy=${requestedApproval} is not supported by this runner; using never.`,
    );
  }

  const requestedVerifyMode = parseSandboxMode(
    getString(autopilot, "verify_sandbox_mode"),
    "workspace-write",
  );
  const verifySandboxMode = requestedVerifyMode === "read-only"
    ? "workspace-write"
    : requestedVerifyMode;
  if (requestedVerifyMode === "read-only") {
    warnings.push(
      "autopilot.verify_sandbox_mode=read-only was coerced to workspace-write so verify can write session artifacts.",
    );
  }

  return {
    model: cli.model
      ?? getString(autopilot, "model")
      ?? getString(codex.root, "model")
      ?? "gpt-5.4",
    taskLimit: cli.tasks ?? getNumber(autopilot, "task_limit") ?? 0,
    maxCycles: positiveNumber(cli.maxCycles ?? getNumber(autopilot, "max_cycles"), 10),
    timeoutSeconds: positiveNumber(
      cli.timeoutSeconds
      ?? getNumber(autopilot, "timeout_seconds")
      ?? getNumber(agents, "job_max_runtime_seconds"),
      1800,
    ),
    reasoningEffort: parseReasoningEffort(
      getString(autopilot, "reasoning_effort")
      ?? getString(codex.root, "reasoning_effort"),
    ),
    approvalPolicy: "never",
    sandboxMode: parseSandboxMode(getString(autopilot, "sandbox_mode"), "workspace-write"),
    verifySandboxMode,
    pushEnabled: getBoolean(autopilot, "push_enabled") ?? false,
    includeProjectRules: getBoolean(autopilot, "include_project_rules") ?? true,
    includeSkillGuides: getBoolean(autopilot, "include_skill_guides") ?? true,
    includeAgentProfiles: getBoolean(autopilot, "include_agent_profiles") ?? true,
    multiAgent: getFeatureFlag(features, "multi_agent") ?? true,
    webSearch,
    webSearchMode: parseWebSearchMode(getString(autopilot, "web_search_mode"), webSearch),
    agentMaxThreads: getNumber(agents, "max_threads") ?? 6,
    agentMaxDepth: getNumber(agents, "max_depth") ?? 1,
    warnings,
  };
}
