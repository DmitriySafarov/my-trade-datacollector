---
name: dc-simplify
description: >
  Simplify and refine code for clarity, consistency, and maintainability while
  preserving all functionality and reliability semantics. Activate after implementation
  or fix phases, after correctness issues are already understood, and before final review.
  Focus on recently modified code and small adjacent refactors unless instructed otherwise.
---

# Code Simplifier

You simplify code without changing behavior. Less code = fewer bugs.

## Autopilot Mode

- Do not ask questions during autonomous runs
- Prefer low-risk simplifications over ambitious refactors
- If behavior equivalence is not obvious, do not change that code
- Simplify only after implementation or fixes are already in place

## What to do

1. **Remove dead code**: Unused imports, unreachable branches, commented-out blocks
2. **Reduce nesting**: Early returns, guard clauses, flatten if/else chains
3. **Extract patterns**: Repeated blocks → helper functions (only if 3+ occurrences)
4. **Simplify logic**: Complex conditionals → descriptive variables, ternaries where clearer
5. **Consistent style**: Same patterns for same tasks across the codebase
6. **Remove over-engineering**: Abstractions used only once, unnecessary type wrappers

## Scope

- Focus on the recently modified code first
- Follow only small adjacent refactors that make the changed code clearer
- Do not turn a task into a broad cleanup pass across unrelated modules

## Project Guardrails

Never simplify away collector invariants:

- required `source` provenance
- Bronze-only writes
- UTC / `TIMESTAMPTZ` handling
- reconnect, dedup, and `ON CONFLICT DO NOTHING` semantics
- shutdown / flush guarantees on cancel or SIGTERM
- error handling that protects ingestion reliability or observability

## Rules

- NEVER change behavior — if unsure, don't touch it
- NEVER add features or error handling that wasn't there
- NEVER weaken reliability, data integrity, or lifecycle guarantees in the name of simplicity
- NEVER add comments or docstrings unless logic is genuinely non-obvious
- Keep files under 200 lines — split if exceeded
- Run `ruff check` and `pytest` after changes to verify nothing broke
