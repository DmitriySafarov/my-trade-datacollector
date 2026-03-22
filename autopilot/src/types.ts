export type Severity = "critical" | "important" | "suggestion";
export type SandboxMode = "read-only" | "workspace-write" | "danger-full-access";
export type WebSearchMode = "disabled" | "cached" | "live";
export type ReasoningEffort = "minimal" | "low" | "medium" | "high" | "xhigh";
export type PromptKind = "start" | "verify" | "fix" | "finish";
export type RunPhase = "start" | "verify" | "fix" | "finish";
export type SessionPhase = RunPhase | "done" | "skipped";
export type AgentRunStatus = "completed" | "dry_run" | "timed_out" | "failed";

export interface Finding {
  id: string;
  severity: Severity;
  blocking?: boolean;
  source?: string;
  file: string;
  line?: number;
  title: string;
  description: string;
  recommended_fix: string;
}

export interface VerifyResult {
  clean: boolean;
  critical: number;
  important: number;
  suggestion: number;
  ruff_passed: boolean;
  syntax_passed: boolean;
  tests_passed: boolean;
  tests_total?: number;
  tests_failed?: number;
  findings: Finding[];
}

export interface VerifyGateEvaluation {
  clean: boolean;
  invalidReason?: string;
}

export interface SessionState {
  current_task: number;
  phase: SessionPhase;
  cycle?: number;
  total_tasks?: number;
  completed?: number;
  clean?: number;
  dirty?: number;
  impl_thread_id?: string | null;
  last_runtime_error?: string | null;
}

export interface CliOverrides {
  tasks?: number;
  maxCycles?: number;
  timeoutSeconds?: number;
  model?: string;
  dryRun: boolean;
  preflightOnly: boolean;
  resume?: string;
  phase?: RunPhase;
}

export interface RuntimeConfig {
  model: string;
  taskLimit: number;
  maxCycles: number;
  timeoutSeconds: number;
  reasoningEffort: ReasoningEffort;
  approvalPolicy: "never";
  sandboxMode: SandboxMode;
  verifySandboxMode: SandboxMode;
  pushEnabled: boolean;
  includeProjectRules: boolean;
  includeSkillGuides: boolean;
  includeAgentProfiles: boolean;
  multiAgent: boolean;
  webSearch: boolean;
  webSearchMode: WebSearchMode;
  agentMaxThreads: number;
  agentMaxDepth: number;
  warnings: string[];
}

export interface AgentRunResult {
  status: AgentRunStatus;
  threadId: string | null;
  turnCount: number;
  error?: string;
}

export interface RunnerOptions {
  dryRun: boolean;
  phase?: RunPhase;
  resumeState?: SessionState | null;
}
