import { afterEach, describe, expect, it, vi } from "vitest";

import { reduceTaskEvents } from "./taskEventStream";
import type { TaskEventEnvelope } from "./taskEventStream";
import { openTaskEventStream } from "./taskEventStream";

afterEach(() => vi.unstubAllGlobals());

const event = (eventId: string, taskSequence: number): TaskEventEnvelope => ({
  eventId,
  taskId: "task",
  runId: null,
  taskSequence,
  runSequence: null,
  type: "task.provisioning_started",
  schemaVersion: 1,
  payload: { schema_version: 1 },
  createdAt: "2026-09-20T00:00:00Z",
});

it("opens SSE with cookie credentials", () => {
  const source = { addEventListener: vi.fn(), close: vi.fn() };
  const EventSource = vi.fn(function () {
    return source;
  });
  vi.stubGlobal("EventSource", EventSource);

  openTaskEventStream("task", vi.fn());

  expect(EventSource).toHaveBeenCalledWith("/api/tasks/task/events", {
    withCredentials: true,
  });
});

describe("reduceTaskEvents", () => {
  it("deduplicates, orders, and detects gaps", () => {
    const first = event("one", 1);
    const duplicate = event("one", 1);
    const third = event("three", 3);

    const state = reduceTaskEvents({ events: [], needsReplay: false }, first);
    expect(reduceTaskEvents(state, duplicate)).toEqual(state);
    expect(reduceTaskEvents(state, third).needsReplay).toBe(true);
  });

  it("ignores older events and appends contiguous events", () => {
    const initial = reduceTaskEvents(
      reduceTaskEvents({ events: [], needsReplay: false }, event("one", 1)),
      event("two", 2),
    );
    expect(reduceTaskEvents(initial, event("old", 1))).toEqual(initial);
    expect(initial.events.map(({ taskSequence }) => taskSequence)).toEqual([
      1, 2,
    ]);
  });
});
