import { createHash } from "crypto";
import { existsSync, readdirSync, readFileSync } from "fs";
import { join, relative } from "path";

import { PROJECT_ROOT } from "./paths.js";
import {
  PROJECT_AGENTS,
  REQUIRED_AGENT_FAILURE_PHRASE,
} from "./project-agents.js";
import { getInstalledSkillDir, PROJECT_SKILLS } from "./project-skills.js";

function walkFiles(root: string, current = root): string[] {
  const entries = readdirSync(current, { withFileTypes: true });
  const files: string[] = [];

  for (const entry of entries) {
    const fullPath = join(current, entry.name);
    if (entry.isDirectory()) {
      files.push(...walkFiles(root, fullPath));
      continue;
    }
    files.push(relative(root, fullPath));
  }

  return files.sort();
}

function readFrontmatterName(skillMdPath: string): string | null {
  const text = readFileSync(skillMdPath, "utf-8");
  const match = text.match(/^---\n([\s\S]*?)\n---/);
  if (!match) return null;
  const nameMatch = match[1].match(/^name:\s*(.+)$/m);
  return nameMatch?.[1]?.trim() ?? null;
}

function directorySignature(root: string): string {
  const hash = createHash("sha256");
  for (const file of walkFiles(root)) {
    hash.update(file);
    hash.update("\0");
    hash.update(readFileSync(join(root, file)));
    hash.update("\0");
  }
  return hash.digest("hex");
}

function verifyInstalledSkill(sourceDir: string, installedDir: string, expectedName: string): void {
  if (!existsSync(installedDir)) {
    throw new Error(`Missing installed skill '${expectedName}' at ${installedDir}`);
  }

  const sourceSkillMd = join(sourceDir, "SKILL.md");
  const installedSkillMd = join(installedDir, "SKILL.md");
  const installedYaml = join(installedDir, "agents", "openai.yaml");

  if (!existsSync(installedSkillMd) || !existsSync(installedYaml)) {
    throw new Error(`Installed skill '${expectedName}' is incomplete at ${installedDir}`);
  }

  const sourceName = readFrontmatterName(sourceSkillMd);
  const installedName = readFrontmatterName(installedSkillMd);
  if (sourceName !== expectedName || installedName !== expectedName) {
    throw new Error(`Skill name mismatch for '${expectedName}'`);
  }

  const sourceHash = directorySignature(sourceDir);
  const installedHash = directorySignature(installedDir);
  if (sourceHash !== installedHash) {
    throw new Error(`Installed skill '${expectedName}' is stale or diverged from repo source`);
  }
}

export function verifyProjectSkillsInstalled(): string[] {
  const verified: string[] = [];
  for (const skill of PROJECT_SKILLS) {
    const sourceDir = join(PROJECT_ROOT, skill.sourceDir);
    verifyInstalledSkill(sourceDir, getInstalledSkillDir(skill.invocationName), skill.invocationName);
    verified.push(skill.invocationName);
  }
  return verified;
}

function verifyAgentProfile(agentName: string, profilePath: string, requiredSkills: string[]): void {
  if (!existsSync(profilePath)) {
    throw new Error(`Missing required agent profile '${agentName}' at ${profilePath}`);
  }

  const text = readFileSync(profilePath, "utf-8");
  if (!text.includes(`name = "${agentName}"`)) {
    throw new Error(`Agent profile '${agentName}' has mismatched or missing name field`);
  }
  if (!text.includes("## Project Skills")) {
    throw new Error(`Agent profile '${agentName}' is missing the Project Skills section`);
  }
  for (const skillName of requiredSkills) {
    if (!text.includes(`$${skillName}`)) {
      throw new Error(`Agent profile '${agentName}' is missing required skill '${skillName}'`);
    }
  }
  if (!text.includes(REQUIRED_AGENT_FAILURE_PHRASE)) {
    throw new Error(
      `Agent profile '${agentName}' is missing the required fail-fast phrase '${REQUIRED_AGENT_FAILURE_PHRASE}'`,
    );
  }
}

export function verifyProjectAgentProfiles(): string[] {
  const verified: string[] = [];
  for (const agent of PROJECT_AGENTS) {
    verifyAgentProfile(agent.name, join(PROJECT_ROOT, agent.profilePath), agent.requiredSkills);
    verified.push(agent.name);
  }
  return verified;
}
