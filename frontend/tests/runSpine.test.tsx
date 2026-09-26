import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { App } from "../src/App";

class FakeEventSource {
  addEventListener() {}
  close() {}
}

afterEach(() => vi.unstubAllGlobals());

it("registers, creates a task, reconstructs it, and submits a message", async () => {
  const repository = {
    id: "repository",
    rootPath: "/canonical/repository",
    headRevision: "a".repeat(40),
    createdAt: "2026-09-20T00:00:00Z",
  };
  const task = {
    id: "task",
    repositoryId: repository.id,
    sourceRef: "HEAD",
    baseRevision: repository.headRevision,
    workspacePath: "/workspace",
    status: "active",
    failureCode: null,
    failureDetail: null,
    createdAt: repository.createdAt,
    updatedAt: repository.createdAt,
  };
  const fetchMock = vi.fn(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      let body: object = {};
      if (url === "/api/repositories" && init?.method === "POST")
        body = repository;
      else if (url === "/api/repositories") body = [];
      else if (url === "/api/tasks") body = [];
      else if (url.endsWith("/tasks") && init?.method === "POST") body = task;
      else if (url === "/api/tasks/task") body = task;
      else if (url.endsWith("/approvals")) body = [];
      else if (url.endsWith("/review")) {
        body = {
          latestRunStatus: null,
          completionSummary: null,
          claimedFiles: [],
          validationAttempts: [],
          resultRevisions: [],
          integrations: [],
        };
      } else if (url.endsWith("/trace")) body = [];
      else if (url.endsWith("/workspace")) {
        body = {
          status: "",
          diff: "",
          statusTruncated: false,
          diffTruncated: false,
        };
      } else if (url.endsWith("/messages") && init?.method === "POST") {
        body = { messageId: "message", runId: "run", runStatus: "queued" };
      } else if (url.endsWith("/messages")) body = [];
      return new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    },
  );
  vi.stubGlobal("fetch", fetchMock);
  vi.stubGlobal("EventSource", FakeEventSource);

  render(<App />);
  fireEvent.click(screen.getByRole("button", { name: "Add repository" }));
  fireEvent.change(screen.getByLabelText("Repository path"), {
    target: { value: "/alias" },
  });
  fireEvent.click(screen.getByRole("button", { name: /^Add$/ }));
  await waitFor(() =>
    expect(
      fetchMock.mock.calls.some(
        ([url, init]) => url === "/api/repositories" && init?.method === "POST",
      ),
    ).toBe(true),
  );
  fireEvent.change(screen.getByLabelText("Message"), {
    target: { value: "Explain" },
  });
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "Send" })).toBeEnabled(),
  );
  fireEvent.click(screen.getByRole("button", { name: "Send" }));
  await screen.findByText("Status: active");

  await waitFor(() =>
    expect(
      fetchMock.mock.calls.some(([url, init]) => {
        const headers = new Headers(init?.headers);
        return (
          url === "/api/tasks/task/messages" &&
          init?.method === "POST" &&
          Boolean(headers.get("Idempotency-Key"))
        );
      }),
    ).toBe(true),
  );
});
