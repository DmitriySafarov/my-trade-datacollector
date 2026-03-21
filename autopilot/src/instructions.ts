import type { RuntimeConfig } from "./types.js";

const PYTEST_CMD = ".venv/bin/python -m pytest tests/ -v";

export function buildAutopilotPreamble(context: string): string {
  return `
# Autopilot Mode

You are running inside an automated sprint loop. Work autonomously and finish the task end-to-end.

## Non-negotiable rules
- NEVER ask questions or request approval
- NEVER stop after analysis if code changes are required
- Use local source code, tracker files, AGENTS.md, and the inlined skill guides as the primary source of truth
- If direct skill activation is available, invoke the named skills explicitly
- Required project skills and required expert-agent profiles are fail-fast runtime contracts; if they are unavailable when needed, record a runtime contract failure and stop
- Optional capabilities may fall back locally only when they are not required for the current task
- Verify is logically read-only for source code; write access exists only so you can persist session artifacts under \`autopilot/sessions/\`
- Use web search only if it is available and only for unstable external facts, official docs, or live APIs

## Environment
- Docker Desktop is installed and expected to work
- PostgreSQL 16 + TimescaleDB are available via Docker
- Python 3.12 is the project runtime
- Ruff and pytest are the expected validation tools
- "Docker not available" or "skipping DB tests" is not acceptable for collector work

## Ownership
- You own the whole project state, not just your diff
- Fix pre-existing bugs or failing tests that block the current task
- If something truly cannot be completed, write a concrete backlog entry with what you tried and why it is blocked

## Project Context

${context}
`.trim();
}

export function buildStartInstructions(config: RuntimeConfig): string {
  const agentStep = config.multiAgent
    ? `
## Step 5: Consult expert agents
Launch relevant expert agents in parallel when the capability is available.

- Use at least 2 expert agents for non-trivial tasks when spawning works
- Stay within the configured budget: up to ${config.agentMaxThreads} threads, max depth ${config.agentMaxDepth}
- Prefer \`python-async-pro\` for implementation, plus domain agents that match the task
- If required expert-agent spawning is unavailable for a non-trivial task, record a runtime contract failure and stop
`.trim()
    : `
## Step 5: Expert consultation fallback
Multi-agent consultation is disabled in runtime config. Perform the design review locally and record that no expert agents were spawned.
`.trim();

  return `
# Start Session

## Step 1: Read the spec
- Skim \`../plans/data-collector-spec-v2.md\`
- Read \`../plans/data-collector-reliability.md\`

## Step 2: Find current work
- Read \`autopilot/tracker/backlog.md\` first
- Read \`autopilot/tracker/feature-tracker.md\`
- Check \`git log --oneline -10\` and \`git status --short\`

## Step 3: Pick exactly one actionable task
- Take the first pending tracker task unless backlog has a higher-priority actionable item
- Skip tasks that need human decisions, unavailable credentials, or blocked earlier dependencies
- If nothing actionable exists, write \`no_actionable_tasks\` to \`context.md\` and stop

## Step 4: Understand the task
- Read all relevant existing code
- Read the matching spec section before making implementation choices
- Decide which skill guides and expert agents are relevant

${agentStep}

## Step 6: Implement
- Write \`context.md\` and \`start.md\`
- Implement the full task, not just a plan
- Follow AGENTS.md precisely: async only, Bronze-only writes, required \`source\` field, UTC timestamps, files under 200 lines

## Step 7: Skills and self-review
- Activate \`$dc-write-tests\` whenever code paths or collectors change; if it is unavailable, record a runtime contract failure and stop
- Activate \`$dc-code-reviewer\` on the changed code and any adjacent execution paths needed for an honest review before stopping; if it is unavailable, record a runtime contract failure and stop
- Activate \`$dc-simplify\` after fixes to reduce unnecessary complexity; if it is unavailable when needed, record a runtime contract failure and stop
- Run \`ruff check . && ruff format --check .\`
- Run \`${PYTEST_CMD}\`
`.trim();
}

export function buildVerifyInstructions(config: RuntimeConfig): string {
  const reviewStep = config.multiAgent
    ? `
## Step 1: Expert review
Launch relevant review agents sequentially when the capability is available.

- Prefer \`python-async-pro\` plus domain agents relevant to the changed files
- Record which agents ran and what each one found
- If required review-agent spawning is unavailable for the task, record a runtime contract failure and stop
`.trim()
    : `
## Step 1: Local review
Multi-agent review is disabled in runtime config. Perform the full review locally and note that no expert agents were spawned.
`.trim();

  return `
# Verify Session

You are the REVIEWER. Do not modify source code. You may only write verification artifacts inside the session task directory.

${reviewStep}

## Step 2: Skills
- Activate \`$dc-code-reviewer\` in structured report mode for the verify artifacts
- Use it as a real code reviewer first, then as a collector-invariant checker
- If direct skill activation is unavailable, record a runtime contract failure and stop

## Step 3: Run checks
- \`ruff check . && ruff format --check .\`
- Syntax-check changed Python files
- \`${PYTEST_CMD}\`

## Step 4: Report
- Write \`verify-*.md\` and \`verify-*-result.json\`
- Report every critical, important, and suggestion finding
- Report failing tests and pre-existing bugs too
- Set \`clean=true\` only if findings are empty and ruff, syntax, and tests all pass
`.trim();
}

export function buildFixInstructions(): string {
  return `
# Implement Fixes

## Step 1: Read the previous verify findings
- Fix every critical and important finding
- Fix suggestions unless they conflict with project conventions
- Fix failing tests even if they were pre-existing

## Step 2: Skills and cleanup
- Activate \`$dc-code-reviewer\` after the fixes; if it is unavailable, record a runtime contract failure and stop
- Activate \`$dc-simplify\` after review fixes; if it is unavailable when needed, record a runtime contract failure and stop

## Step 3: Validate
- Run \`ruff check . && ruff format --check .\`
- Run \`${PYTEST_CMD}\`
- Stop only when the code and findings are reconciled
`.trim();
}

export function buildFinishInstructions(config: RuntimeConfig): string {
  const gitStep = config.pushEnabled
    ? `
## Step 3: Commit and push
- Stage relevant code, tests, migrations, tracker updates, and progress log
- Do not commit session logs under \`autopilot/sessions/\`
- Create clear commit messages in imperative mood
- Push to remote after committing
`.trim()
    : `
## Step 3: Commit locally
- Stage relevant code, tests, migrations, tracker updates, and progress log
- Do not commit session logs under \`autopilot/sessions/\`
- Create clear local commits in imperative mood
- Do NOT push in this run
`.trim();

  return `
# Finish Session

## Step 1: Update tracker
- Read \`autopilot/tracker/feature-tracker.md\`
- Mark completed tasks as done

## Step 2: Update progress log
- Append a session entry to \`autopilot/tracker/progress.md\`
- Include completed task, changed files, decisions, issues, and next step

${gitStep}

## Step 4: Write finish log
- Record tracker changes
- Record commit hashes and messages
- Suggest the next task
`.trim();
}
