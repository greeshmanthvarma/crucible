import {
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { expect, it, vi } from "vitest";

import { AcceptancePanel } from "./AcceptancePanel";
import { IntegrationPanel } from "./IntegrationPanel";
import { ValidationTrace } from "./ValidationTrace";

it("renders validation evidence including pending approval and artifacts", () => {
  render(
    <ValidationTrace
      attempts={[
        {
          id: "attempt",
          runId: "run",
          attemptNumber: 1,
          status: "failed",
          createdAt: "2026-09-20T00:00:00Z",
          completedAt: "2026-09-20T00:01:00Z",
          commands: [
            {
              id: "command",
              commandSequence: 1,
              status: "pending",
              approvalId: "approval",
              toolCallId: null,
              artifactId: "artifact",
              exitCode: null,
              summary: "Awaiting approval",
              createdAt: "2026-09-20T00:00:00Z",
              completedAt: null,
            },
          ],
        },
      ]}
    />,
  );

  expect(screen.getByText("Validation attempt 1: failed")).toBeVisible();
  expect(screen.getByText(/Awaiting approval/)).toBeVisible();
  expect(screen.getByRole("link", { name: "Evidence" })).toHaveAttribute(
    "href",
    "/api/artifacts/artifact",
  );
});

it("keeps one acceptance retry key and disables acceptance when invalid", async () => {
  const accept = vi.fn().mockRejectedValueOnce(new Error("offline"));
  const { rerender } = render(
    <AcceptancePanel
      enabled={false}
      reason="Validation failed"
      accept={accept}
    />,
  );
  expect(screen.getByRole("button", { name: "Accept result" })).toBeDisabled();
  expect(screen.getByText("Validation failed")).toBeVisible();

  rerender(<AcceptancePanel enabled accept={accept} />);
  fireEvent.click(screen.getByRole("button", { name: "Accept result" }));
  fireEvent.click(screen.getByRole("button", { name: "Confirm acceptance" }));
  await screen.findByRole("alert");
  fireEvent.click(screen.getByRole("button", { name: "Retry acceptance" }));
  await waitFor(() => expect(accept).toHaveBeenCalledTimes(2));
  expect(accept.mock.calls[0][0]).toBe(accept.mock.calls[1][0]);
});

it("requires an explicit clean-target integration and displays conflicts", async () => {
  const integrate = vi.fn().mockResolvedValue(undefined);
  const inspectTarget = vi
    .fn()
    .mockResolvedValue({
      currentRef: "main",
      headRevision: "c".repeat(40),
      clean: true,
    });
  render(
    <IntegrationPanel
      repositoryId="repository"
      inspectTarget={inspectTarget}
      results={[
        {
          id: "result",
          taskId: "task",
          commitSha: "a".repeat(40),
          parentRevision: "b".repeat(40),
          previousResultRevisionId: null,
          diffArtifactId: "diff",
          validationSnapshot: {},
          summary: "Ready",
          createdBy: "user",
          createdAt: "2026-09-20T00:00:00Z",
        },
      ]}
      integrations={[
        {
          id: "integration",
          resultRevisionId: "result",
          repositoryId: "repository",
          targetRef: "main",
          expectedTargetRevision: "c".repeat(40),
          status: "conflict",
          observedBeforeRevision: "c".repeat(40),
          observedAfterRevision: null,
          failureCode: "integration_conflict",
          failureDetail: "README.md conflicted",
          createdAt: "2026-09-20T00:00:00Z",
          completedAt: "2026-09-20T00:01:00Z",
        },
      ]}
      integrate={integrate}
    />,
  );

  expect(
    screen.getByText(/clean and still at the expected revision/),
  ).toBeVisible();
  expect(screen.getByText(/README.md conflicted/)).toBeVisible();
  await screen.findByText(/Branch:/);
  fireEvent.click(screen.getByRole("button", { name: "Integrate result" }));
  fireEvent.click(screen.getByRole("button", { name: "Confirm integration" }));
  await waitFor(() =>
    expect(integrate).toHaveBeenCalledWith(
      "result",
      "repository",
      "main",
      "c".repeat(40),
      expect.any(String),
    ),
  );
});

it("blocks integration when the target checkout is dirty", async () => {
  const integrate = vi.fn();
  const rendered = render(
    <IntegrationPanel
      repositoryId="repository"
      inspectTarget={vi
        .fn()
        .mockResolvedValue({
          currentRef: "main",
          headRevision: "a".repeat(40),
          clean: false,
        })}
      results={[
        {
          id: "result",
          taskId: "task",
          commitSha: "b".repeat(40),
          parentRevision: "a".repeat(40),
          previousResultRevisionId: null,
          diffArtifactId: "diff",
          validationSnapshot: {},
          summary: "Ready",
          createdBy: "user",
          createdAt: "2026-09-20T00:00:00Z",
        },
      ]}
      integrations={[]}
      integrate={integrate}
    />,
  );
  const view = within(rendered.container);

  expect(await view.findByText("Has local changes")).toBeVisible();
  expect(view.getByRole("button", { name: "Integrate result" })).toBeDisabled();
  expect(view.getByText(/will not modify a dirty checkout/)).toBeVisible();
  expect(integrate).not.toHaveBeenCalled();
});
