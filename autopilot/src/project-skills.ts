import { homedir } from "os";
import { join } from "path";

export interface ProjectSkill {
  invocationName: string;
  sourceDir: string;
}

export const PROJECT_SKILLS: ProjectSkill[] = [
  {
    invocationName: "dc-write-tests",
    sourceDir: ".agents/skills/write-tests",
  },
  {
    invocationName: "dc-code-reviewer",
    sourceDir: ".agents/skills/code-reviewer",
  },
  {
    invocationName: "dc-simplify",
    sourceDir: ".agents/skills/simplify",
  },
];

const SKILLS_BY_NAME = new Map(
  PROJECT_SKILLS.map((skill) => [skill.invocationName, skill]),
);

export const IMPLEMENT_SKILL_NAMES = [
  "dc-write-tests",
  "dc-code-reviewer",
  "dc-simplify",
];

export const VERIFY_SKILL_NAMES = ["dc-code-reviewer"];

export function getProjectSkill(name: string): ProjectSkill {
  const skill = SKILLS_BY_NAME.get(name);
  if (!skill) throw new Error(`Unknown project skill: ${name}`);
  return skill;
}

export function getSkillGuidePath(name: string): string {
  return join(getProjectSkill(name).sourceDir, "SKILL.md");
}

export function getInstalledSkillDir(name: string): string {
  return join(homedir(), ".codex", "skills", name);
}
