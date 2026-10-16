import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { RepairStatus } from "./RepairStatus";
import { TaskView } from "./TaskView";
import type { CrucibleClient } from "../../api/client";

afterEach(cleanup);

const repair = {
  repairId: "repair",
  status: "running" as const,
  stage: "proposal" as const,
  attempts: { proposal: 2 },
  outcomeCode: null,
  detail: null,
  prUrl: null,
};
it("renders one current stage and legal controls with safe PR links", async () => {
  const act = vi.fn().mockResolvedValue(undefined);
  const view = render(
    <RepairStatus repair={repair} stop={act} command={act} />,
  );
  expect(screen.getByText(/proposal: 2/)).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Stop repair" }));
  await waitFor(() => expect(act).toHaveBeenCalled());
  view.rerender(
    <RepairStatus
      repair={{
        ...repair,
        status: "interrupted",
        prUrl: "https://github.com/example/repo/pull/1",
      }}
      stop={act}
      command={act}
    />,
  );
  expect(
    screen.queryByRole("button", { name: "Stop repair" }),
  ).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Resume repair" }));
  await waitFor(() =>
    expect(act).toHaveBeenCalledWith("/repair resume", expect.any(String)),
  );
  expect(
    screen.getByRole("button", { name: "Close repair" }),
  ).toBeInTheDocument();
  expect(
    screen.getByRole("link", { name: "Review repair PR" }),
  ).toHaveAttribute("href", "https://github.com/example/repo/pull/1");
  view.rerender(
    <RepairStatus
      repair={{ ...repair, status: "completed" }}
      stop={act}
      command={act}
    />,
  );
  expect(screen.queryByRole("button")).not.toBeInTheDocument();
});
it("loads repairs on refresh and handles a repair response without a coding run", async () => {
  const client = {
    getTask: vi
      .fn()
      .mockResolvedValue({ id: "task", status: "active", sourceRef: "main" }),
    getMessages: vi.fn().mockResolvedValue([]),
    getTaskTrace: vi.fn().mockResolvedValue([]),
    getApprovals: vi.fn().mockResolvedValue([]),
    getWorkspaceState: vi.fn().mockResolvedValue({ status: "", diff: "" }),
    getRepairs: vi.fn().mockResolvedValue([repair]),
    sendMessage: vi.fn().mockResolvedValue({
      kind: "repair_command",
      status: "blocked",
      repairId: null,
      code: "open_pr",
      detail: "An open repair PR needs review.",
      prUrls: ["https://github.com/example/repo/pull/2"],
    }),
  } as unknown as CrucibleClient;
  render(
    <TaskView
      taskId="task"
      client={client}
      streamFactory={() => ({ close: vi.fn() })}
    />,
  );
  await screen.findByText(/proposal: 2/);
  fireEvent.change(screen.getByLabelText("Message"), {
    target: { value: "/repair" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Send" }));
  await screen.findByText("An open repair PR needs review.");
  expect(screen.getByText("Run: idle")).toBeInTheDocument();
  expect(
    screen.getByRole("link", { name: "Review existing repair PR" }),
  ).toHaveAttribute("href", "https://github.com/example/repo/pull/2");
  expect(client.getRepairs).toHaveBeenCalled();
});

it("preserves the stop idempotency key on retry and rejects unsafe PR links", async () => {
  const stop = vi
    .fn()
    .mockRejectedValueOnce(new Error("offline"))
    .mockResolvedValueOnce(undefined);
  render(
    <RepairStatus
      repair={{ ...repair, prUrl: "javascript:alert(1)" }}
      stop={stop}
      command={vi.fn()}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "Stop repair" }));
  await screen.findByRole("alert");
  fireEvent.click(screen.getByRole("button", { name: "Stop repair" }));
  await waitFor(() => expect(stop).toHaveBeenCalledTimes(2));
  expect(stop.mock.calls[0][0]).toBe(stop.mock.calls[1][0]);
  expect(screen.queryByRole("link")).not.toBeInTheDocument();
});

it("refreshes one current card for a repair event and ignores its replay", async () => {
  let listener: (
    event: import("../../events/taskEventStream").TaskEventEnvelope,
  ) => void = () => undefined;
  const getRepairs = vi.fn().mockResolvedValue([repair]);
  const client = {
    getTask: vi
      .fn()
      .mockResolvedValue({ id: "task", status: "active", sourceRef: "main" }),
    getMessages: vi.fn().mockResolvedValue([]),
    getTaskTrace: vi.fn().mockResolvedValue([]),
    getWorkspaceState: vi.fn().mockResolvedValue({ status: "" }),
    getApprovals: vi.fn().mockResolvedValue([]),
    getRepairs,
  } as unknown as CrucibleClient;
  render(
    <TaskView
      taskId="task"
      client={client}
      streamFactory={(_task, onEvent) => {
        listener = onEvent;
        return { close: vi.fn() };
      }}
    />,
  );
  await screen.findByText(/proposal: 2/);
  getRepairs.mockResolvedValue([{ ...repair, stage: "publication" }]);
  const event = {
    eventId: "repair-event",
    taskId: "task",
    runId: null,
    taskSequence: 1,
    runSequence: null,
    type: "repair.updated",
    schemaVersion: 1,
    payload: {
      repair_id: "repair",
      status: "running",
      stage: "publication",
      attempts: {},
    },
    createdAt: "2026-10-15T00:00:00Z",
  };
  act(() => {
    listener(event);
    listener(event);
  });
  await screen.findByText(/Repair · running · publication/);
  expect(screen.getAllByRole("region", { name: "Repair status" })).toHaveLength(
    1,
  );
  expect(getRepairs).toHaveBeenCalledTimes(2);
});
