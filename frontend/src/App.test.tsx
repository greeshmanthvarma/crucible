import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { App } from "./App";

describe("App", () => {
  it("identifies Crucible as a coding-agent harness", () => {
    render(<App />);

    expect(
      screen.getByRole("heading", { name: "Crucible" }),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Eval-driven coding-agent harness"),
    ).toBeInTheDocument();
  });
});
