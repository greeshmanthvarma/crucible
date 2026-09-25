import { fireEvent, render, screen, waitFor } from "@testing-library/react";
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
  await screen.findByRole("alert");
  fireEvent.click(screen.getByRole("button", { name: "Retry acceptance" }));
  await waitFor(() => expect(accept).toHaveBeenCalledTimes(2));
  expect(accept.mock.calls[0][0]).toBe(accept.mock.calls[1][0]);
});

it("requires an explicit clean-target integration and displays conflicts", async () => {
  const integrate = vi.fn().mockResolvedValue(undefined);
  render(
    <IntegrationPanel
      repositoryId="repository"
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
  fireEvent.change(screen.getByLabelText("Target ref"), {
    target: { value: "main" },
  });
  fireEvent.change(screen.getByLabelText("Expected revision"), {
    target: { value: "c".repeat(40) },
  });
  fireEvent.click(screen.getByRole("button", { name: "Integrate result" }));
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
