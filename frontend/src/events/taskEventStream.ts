import type { RepairStatusView } from "../api/client";
import type { components } from "../api/schema";

export type TaskEventEnvelope = components["schemas"]["TaskEventEnvelope"];
export type TaskEventState = {
  events: TaskEventEnvelope[];
  needsReplay: boolean;
  repairs?: Record<string, RepairStatusView>;
};

export function reduceTaskEvents(
  state: TaskEventState,
  incoming: TaskEventEnvelope,
): TaskEventState {
  if (
    state.events.some((event) => event.eventId === incoming.eventId) ||
    incoming.taskSequence <= (state.events.at(-1)?.taskSequence ?? 0)
  ) {
    return state;
  }
  const repair = repairEvent(incoming);
  const expected = (state.events.at(-1)?.taskSequence ?? 0) + 1;
  return {
    events: [...state.events, incoming],
    repairs: repair
      ? {
          ...state.repairs,
          [String(incoming.payload.repair_id)]: repair,
        }
      : state.repairs,
    needsReplay: state.needsReplay || incoming.taskSequence > expected,
  };
}

export interface EventStream {
  close(): void;
}

export type EventStreamFactory = (
  taskId: string,
  onEvent: (event: TaskEventEnvelope) => void,
  onOpen?: () => void,
) => EventStream;

export const openTaskEventStream: EventStreamFactory = (
  taskId,
  onEvent,
  onOpen,
) => {
  const source = new EventSource(`/api/tasks/${taskId}/events`, {
    withCredentials: true,
  });
  source.addEventListener("task_event", (event) => {
    onEvent(
      JSON.parse((event as MessageEvent<string>).data) as TaskEventEnvelope,
    );
  });
  if (onOpen) source.addEventListener("open", onOpen);
  return source;
};

function repairEvent(event: TaskEventEnvelope): RepairStatusView | undefined {
  if (event.type !== "repair.updated" || event.schemaVersion !== 1) return;
  const p = event.payload;
  if (
    typeof p.repair_id !== "string" ||
    typeof p.status !== "string" ||
    typeof p.stage !== "string"
  )
    return;
  const statuses = [
    "queued",
    "running",
    "stopped",
    "interrupted",
    "retryable_error",
    "completed",
    "exhausted",
    "closed",
    "invalidated",
  ];
  const stages = [
    "intake",
    "diagnosis",
    "eval_review",
    "baseline_reproduction",
    "proposal",
    "candidate_validation",
    "commit_acceptance",
    "publication",
  ];
  if (!statuses.includes(p.status) || !stages.includes(p.stage)) return;
  const attempts: Record<string, number> = {};
  if (
    p.attempts &&
    typeof p.attempts === "object" &&
    !Array.isArray(p.attempts)
  ) {
    for (const role of ["diagnosis", "review", "proposal"]) {
      const n = (p.attempts as Record<string, unknown>)[role];
      if (typeof n === "number" && Number.isSafeInteger(n) && n >= 0)
        attempts[role] = n;
    }
  }
  return {
    repairId: p.repair_id,
    status: p.status as RepairStatusView["status"],
    stage: p.stage as RepairStatusView["stage"],
    attempts,
    outcomeCode: typeof p.outcome_code === "string" ? p.outcome_code : null,
    detail: typeof p.detail === "string" ? p.detail : null,
    prUrl: typeof p.pr_url === "string" ? p.pr_url : null,
  };
}
