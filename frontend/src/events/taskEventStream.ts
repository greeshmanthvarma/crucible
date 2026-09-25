import type { components } from "../api/schema";

export type TaskEventEnvelope = components["schemas"]["TaskEventEnvelope"];
export type TaskEventState = {
  events: TaskEventEnvelope[];
  needsReplay: boolean;
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
  const expected = (state.events.at(-1)?.taskSequence ?? 0) + 1;
  return {
    events: [...state.events, incoming],
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
