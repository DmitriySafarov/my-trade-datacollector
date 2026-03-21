import { readFileSync } from "fs";
import { join } from "path";

import { PROJECT_ROOT } from "./paths.js";
import { PROJECT_AGENTS } from "./project-agents.js";
import {
  getSkillGuidePath,
  IMPLEMENT_SKILL_NAMES,
  PROJECT_SKILLS,
  VERIFY_SKILL_NAMES,
} from "./project-skills.js";
import type { PromptKind, RuntimeConfig } from "./types.js";

function readFileSafe(relativePath: string): string {
  try {
    return readFileSync(join(PROJECT_ROOT, relativePath), "utf-8").trim();
  } catch {
    return `[file not found: ${relativePath}]`;
  }
}

function renderRuntimeCapabilities(config: RuntimeConfig): string {
  const lines = [
    "## Runtime Capabilities",
    "- approval_policy: `never`",
    `- implementation sandbox: \`${config.sandboxMode}\``,
    `- verify sandbox: \`${config.verifySandboxMode}\``,
    `- required project skills: ${PROJECT_SKILLS.map((skill) => `\`${skill.invocationName}\``).join(", ")}`,
    `- required agent profiles: ${PROJECT_AGENTS.map((agent) => `\`${agent.name}\``).join(", ")}`,
    `- multi-agent: ${config.multiAgent ? `enabled (budget ${config.agentMaxThreads} threads, depth ${config.agentMaxDepth})` : "disabled"}`,
    `- web search: ${config.webSearch ? `${config.webSearchMode} when necessary for unstable external facts/docs` : "disabled"}`,
    "- required project skills and required agent profiles are fail-fast runtime contracts; do not silently continue without them",
    "- optional capabilities may fall back locally only when they are not required for the current task",
  ];
  return lines.join("\n");
}

function renderFileBlock(relativePath: string): string {
  return [
    `## File: \`${relativePath}\``,
    "```text",
    readFileSafe(relativePath),
    "```",
  ].join("\n");
}

function selectSkillFiles(kind: PromptKind): string[] {
  if (kind === "verify") return VERIFY_SKILL_NAMES.map(getSkillGuidePath);
  if (kind === "start" || kind === "fix") return IMPLEMENT_SKILL_NAMES.map(getSkillGuidePath);
  return [];
}

function renderProjectRules(config: RuntimeConfig): string[] {
  if (!config.includeProjectRules) return [];
  return [renderFileBlock("AGENTS.md")];
}

function renderSkillGuides(kind: PromptKind, config: RuntimeConfig): string[] {
  if (!config.includeSkillGuides) return [];
  return selectSkillFiles(kind).map(renderFileBlock);
}

function renderAgentProfiles(kind: PromptKind, config: RuntimeConfig): string[] {
  if (!config.includeAgentProfiles || kind === "finish") return [];
  return PROJECT_AGENTS.map((agent) => renderFileBlock(agent.profilePath));
}

export function buildPromptContext(kind: PromptKind, config: RuntimeConfig): string {
  const sections = [
    renderRuntimeCapabilities(config),
    ...renderProjectRules(config),
    ...renderSkillGuides(kind, config),
    ...renderAgentProfiles(kind, config),
  ];
  return sections.join("\n\n");
}
