import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { expect, it, vi } from "vitest";

import type { CrucibleClient } from "../../api/client";
import type {
  EventStreamFactory,
  TaskEventEnvelope,
} from "../../events/taskEventStream";
import { TaskView } from "./TaskView";

const task = {
  id: "task",
  repositoryId: "repository",
  sourceRef: "HEAD",
  baseRevision: "a".repeat(40),
  workspacePath: "/workspace",
  status: "active",
  failureCode: null,
  failureDetail: null,
  createdAt: "2026-09-20T00:00:00Z",
  updatedAt: "2026-09-20T00:00:00Z",
};

it("renders canonical messages, sends, and refreshes once for a deduplicated completion", async () => {
  let listener: (event: TaskEventEnvelope) => void = () => undefined;
  let opened: () => void = () => undefined;
  const streamFactory: EventStreamFactory = (_taskId, onEvent, onOpen) => {
    listener = onEvent;
    opened = onOpen ?? (() => undefined);
    return { close: vi.fn() };
  };
  const client = {
    getTask: vi.fn().mockResolvedValue(task),
    getMessages: vi.fn().mockResolvedValue([
      {
        id: "message",
        taskId: "task",
        runId: "run",
        conversationSequence: 1,
        role: "user",
        status: "completed",
        parts: [
          { id: "part", partSequence: 1, kind: "text", textContent: "hello" },
        ],
        createdAt: task.createdAt,
        completedAt: task.createdAt,
      },
    ]),
    getTaskTrace: vi.fn().mockResolvedValue([]),
    getWorkspaceState: vi.fn().mockResolvedValue({
      status: "",
      diff: "",
      statusTruncated: false,
      diffTruncated: false,
    }),
    sendMessage: vi.fn().mockResolvedValue({
      messageId: "message-2",
      runId: "run-2",
      runStatus: "queued",
    }),
  } as unknown as CrucibleClient;
  render(
    <TaskView taskId="task" client={client} streamFactory={streamFactory} />,
  );
  await screen.findByText(/hello/);

  fireEvent.change(screen.getByLabelText("Message"), {
    target: { value: "next" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Send" }));
  await waitFor(() => expect(client.sendMessage).toHaveBeenCalled());
  const completed = {
    eventId: "event",
    taskId: "task",
    runId: "run-2",
    taskSequence: 4,
    runSequence: 2,
    type: "message.completed",
    schemaVersion: 1,
    payload: { schema_version: 1 },
    createdAt: task.createdAt,
  };
  listener({
    ...completed,
    eventId: "started",
    taskSequence: 3,
    runSequence: 1,
    type: "run.started",
  });
  listener(completed);
  listener(completed);
  await waitFor(() => expect(client.getMessages).toHaveBeenCalledTimes(4));
  opened();
  opened();
  await waitFor(() => expect(client.getMessages).toHaveBeenCalledTimes(5));
  expect(screen.getByText("Run: running")).toBeInTheDocument();
});
