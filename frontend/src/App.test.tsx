import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { App } from "./App";
import type { CrucibleClient } from "./api/client";

describe("App", () => {
  it("requires a repository before starting a chat and lists saved tasks", async () => {
    const client = {
      listRepositories: vi.fn().mockResolvedValue([]),
      listTasks: vi.fn().mockResolvedValue([
        {
          id: "task-one",
          repositoryId: "repo-one",
          createdAt: "2026-09-20T00:00:00Z",
        },
      ]),
      registerRepository: vi.fn().mockResolvedValue({
        id: "repo-one",
        rootPath: "/workspace/example",
        createdAt: "2026-09-20T00:00:00Z",
      }),
    } as unknown as CrucibleClient;
    render(<App client={client} />);

    expect(
      screen.getByRole("heading", { name: "What would you like to build?" }),
    ).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Message"), {
      target: { value: "Build this" },
    });
    expect(screen.getByRole("button", { name: "Send" })).toBeDisabled();
    await screen.findByRole("button", { name: /Repository ·/ });
    fireEvent.click(screen.getByRole("button", { name: "Add repository" }));
    fireEvent.change(screen.getByLabelText("Repository path"), {
      target: { value: "/workspace/example" },
    });
    fireEvent.click(screen.getByRole("button", { name: /^Add$/ }));
    await waitFor(() =>
      expect(client.registerRepository).toHaveBeenCalledWith(
        "/workspace/example",
      ),
    );
    expect(screen.getByRole("button", { name: "Send" })).toBeEnabled();
    expect(screen.getByText("example")).toBeInTheDocument();
  });
});
