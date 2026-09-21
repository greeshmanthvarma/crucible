import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { expect, it, vi } from "vitest";

import type { CrucibleClient } from "../../api/client";
import { RepositoryForm } from "./RepositoryForm";

it("registers explicitly and renders the canonical root without creating a task", async () => {
  const client = {
    registerRepository: vi.fn().mockResolvedValue({
      id: "repository",
      rootPath: "/canonical/repository",
      headRevision: "a".repeat(40),
      createdAt: "2026-09-20T00:00:00Z",
    }),
    createTask: vi.fn(),
  } as unknown as CrucibleClient;
  render(<RepositoryForm client={client} onRegistered={vi.fn()} />);

  fireEvent.change(screen.getByLabelText("Repository path"), {
    target: { value: "/alias" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Register" }));

  await waitFor(() =>
    expect(
      screen.getByText("Registered: /canonical/repository"),
    ).toBeInTheDocument(),
  );
  expect(client.registerRepository).toHaveBeenCalledWith("/alias");
  expect(client.createTask).not.toHaveBeenCalled();
});
