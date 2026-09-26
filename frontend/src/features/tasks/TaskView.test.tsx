import {
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
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
    getApprovals: vi.fn().mockResolvedValue([]),
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

it("keeps the draft available when submission fails", async () => {
  const client = {
    getTask: vi.fn().mockResolvedValue(task),
    getMessages: vi.fn().mockResolvedValue([]),
    getTaskTrace: vi.fn().mockResolvedValue([]),
    getWorkspaceState: vi.fn().mockResolvedValue({
      status: "",
      diff: "",
      statusTruncated: false,
      diffTruncated: false,
    }),
    getApprovals: vi.fn().mockResolvedValue([]),
    sendMessage: vi.fn().mockRejectedValue(new Error("offline")),
  } as unknown as CrucibleClient;
  const streamFactory: EventStreamFactory = () => ({ close: vi.fn() });
  const rendered = render(
    <TaskView taskId="task" client={client} streamFactory={streamFactory} />,
  );
  const view = within(rendered.container);
  await view.findByText("Status: active");
  const input = view.getByLabelText("Message");
  fireEvent.change(input, { target: { value: "keep this" } });
  fireEvent.click(view.getByRole("button", { name: "Send" }));

  await view.findByRole("alert");
  expect(input).toHaveValue("keep this");
});

it("reconstructs a pending approval and submits the displayed digest", async () => {
  const approval = {
    id: "approval",
    taskId: "task",
    runId: "run",
    stepId: "step",
    toolCallId: "call",
    spec: {
      executable: "python",
      arguments: ["-V"],
      cwd: ".",
      timeoutSeconds: 30,
      network: "none",
      environmentNames: ["CI"],
      image: "runner@sha256:digest",
      reason: "Check Python",
      limits: {
        cpus: 1,
        memoryBytes: 1024,
        pids: 16,
        outputBytes: 100,
      },
    },
    specDigest: "digest",
    status: "pending",
    decisionReason: null,
    decidedBy: null,
    createdAt: task.createdAt,
    decidedAt: null,
  };
  const client = {
    getTask: vi.fn().mockResolvedValue(task),
    getMessages: vi.fn().mockResolvedValue([]),
    getTaskTrace: vi.fn().mockResolvedValue([]),
    getWorkspaceState: vi.fn().mockResolvedValue({
      status: "",
      diff: "",
      statusTruncated: false,
      diffTruncated: false,
    }),
    getApprovals: vi.fn().mockResolvedValue([approval]),
    decideApproval: vi.fn().mockResolvedValue({
      ...approval,
      status: "approved",
    }),
  } as unknown as CrucibleClient;

  render(
    <TaskView
      taskId="task"
      client={client}
      streamFactory={() => ({ close: vi.fn() })}
    />,
  );

  expect(await screen.findByText("Network: none")).toBeVisible();
  expect(screen.getByText("Environment names: CI")).toBeVisible();
  fireEvent.click(screen.getByRole("button", { name: "Approve" }));
  await waitFor(() =>
    expect(client.decideApproval).toHaveBeenCalledWith(
      "approval",
      "approved",
      "digest",
      expect.any(String),
    ),
  );
});

it("places a tool result and its command approval in the conversation", async () => {
  const approval = {
    id: "approval-inline", taskId: "task", runId: "run", stepId: "step", toolCallId: "call-inline",
    spec: { executable: "python", arguments: ["-m", "unittest"], cwd: ".", timeoutSeconds: 30,
      network: "none", environmentNames: [], image: "python@sha256:digest", reason: "Run tests",
      limits: { cpus: 1, memoryBytes: 1024, pids: 16, outputBytes: 100 } },
    specDigest: "digest-inline", status: "pending", decisionReason: null, decidedBy: null,
    createdAt: task.createdAt, decidedAt: null,
  };
  const client = {
    getTask: vi.fn().mockResolvedValue(task),
    getMessages: vi.fn().mockResolvedValue([
      { id: "assistant", role: "assistant", parts: [
        { id: "call-part", kind: "tool_call", toolCallId: "call-inline", textContent: null },
        { id: "diff-part", kind: "tool_call", toolCallId: "diff-call", textContent: null },
      ] },
      { id: "tool", role: "tool", parts: [{ id: "result-part", kind: "tool_result", textContent: "raw tool reply" }] },
    ]),
    getTaskTrace: vi.fn().mockResolvedValue([{ id: "step", calls: [
      { id: "call-inline", name: "execute_command", arguments: { executable: "python", arguments: ["-m", "unittest"] }, status: "pending" },
      { id: "diff-call", name: "workspace_diff", arguments: {}, status: "completed" },
    ], results: [{ id: "diff-result", toolCallId: "diff-call", status: "succeeded", displayText: "diff --git a/file.py b/file.py\n@@ -1 +1 @@\n-old\n+new", artifactId: null }] }]),
    getWorkspaceState: vi.fn().mockResolvedValue({ status: "", diff: "", statusTruncated: false, diffTruncated: false }),
    getApprovals: vi.fn().mockResolvedValue([approval]),
    decideApproval: vi.fn().mockResolvedValue({ ...approval, status: "approved" }),
  } as unknown as CrucibleClient;
  const rendered = render(<TaskView taskId="task" client={client} streamFactory={() => ({ close: vi.fn() })} />);
  const view = within(rendered.container);

  const command = await view.findByRole("region", { name: "Tool call: execute_command" });
  expect(within(command).getByText("python -m unittest")).toBeVisible();
  expect(view.getByRole("region", { name: "Command approval" })).toBeVisible();
  expect(view.getAllByText("-old")).toHaveLength(1);
  expect(view.getAllByText("+new")).toHaveLength(1);
  expect(view.queryByText("raw tool reply")).not.toBeInTheDocument();
  fireEvent.click(view.getByRole("button", { name: "Approve" }));
  await waitFor(() => expect(client.decideApproval).toHaveBeenCalledWith("approval-inline", "approved", "digest-inline", expect.any(String)));
});
