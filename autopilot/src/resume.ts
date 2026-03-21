import type { RunPhase, SessionState } from "./types.js";

export interface ResumePlan {
  startTaskNum: number;
  phase?: RunPhase;
  completed: number;
  clean: number;
  dirty: number;
}

function count(value: number | undefined): number {
  return value && value > 0 ? value : 0;
}

function toRunPhase(phase: SessionState["phase"] | undefined): RunPhase | undefined {
  return phase === "start" || phase === "verify" || phase === "fix" || phase === "finish"
    ? phase
    : undefined;
}

export function buildResumePlan(state: SessionState | null, cliPhase?: RunPhase): ResumePlan {
  if (!state) {
    return {
      startTaskNum: 1,
      phase: cliPhase,
      completed: 0,
      clean: 0,
      dirty: 0,
    };
  }

  const completed = count(state.completed);
  const clean = count(state.clean);
  const dirty = count(state.dirty);
  const taskIsDone = state.phase === "done" || state.phase === "skipped";

  return {
    startTaskNum: taskIsDone ? state.current_task + 1 : state.current_task,
    phase: cliPhase ?? (taskIsDone ? undefined : toRunPhase(state.phase)),
    completed,
    clean,
    dirty,
  };
}

export function getTaskResumeState(taskNum: number, state: SessionState | null): SessionState | null {
  if (!state) return null;
  if (state.current_task !== taskNum) return null;
  if (state.phase === "done" || state.phase === "skipped") return null;
  return state;
}
