export interface ProjectAgent {
  name: string;
  profilePath: string;
  requiredSkills: string[];
}

export const REQUIRED_AGENT_SKILLS = [
  "dc-write-tests",
  "dc-code-reviewer",
  "dc-simplify",
];

export const REQUIRED_AGENT_FAILURE_PHRASE = "runtime contract failure";

export const PROJECT_AGENTS: ProjectAgent[] = [
  {
    name: "data-engineer",
    profilePath: ".codex/agents/data-engineer.toml",
    requiredSkills: REQUIRED_AGENT_SKILLS,
  },
  {
    name: "python-async-pro",
    profilePath: ".codex/agents/python-async-pro.toml",
    requiredSkills: REQUIRED_AGENT_SKILLS,
  },
  {
    name: "timescaledb-pro",
    profilePath: ".codex/agents/timescaledb-pro.toml",
    requiredSkills: REQUIRED_AGENT_SKILLS,
  },
  {
    name: "ws-specialist",
    profilePath: ".codex/agents/ws-specialist.toml",
    requiredSkills: REQUIRED_AGENT_SKILLS,
  },
  {
    name: "api-integrator",
    profilePath: ".codex/agents/api-integrator.toml",
    requiredSkills: REQUIRED_AGENT_SKILLS,
  },
];
