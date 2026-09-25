import { afterEach, expect, it, vi } from "vitest";

import {
  bootstrapFromLocation,
  bootstrapSession,
  sessionFetch,
} from "./session";

afterEach(() => {
  vi.unstubAllGlobals();
  sessionStorage.clear();
});

it("keeps the session in an HttpOnly cookie and adds in-memory CSRF to mutations", async () => {
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce(
      new Response(JSON.stringify({ csrfToken: "csrf" }), {
        status: 201,
        headers: { "Content-Type": "application/json" },
      }),
    )
    .mockResolvedValueOnce(new Response("{}"));
  vi.stubGlobal("fetch", fetchMock);

  await bootstrapSession("one-time-secret");
  await sessionFetch("/api/repositories", { method: "POST" });

  expect(fetchMock).toHaveBeenNthCalledWith(
    1,
    "/api/auth/bootstrap",
    expect.objectContaining({ credentials: "include" }),
  );
  const mutation = fetchMock.mock.calls[1][1] as RequestInit;
  expect(mutation.credentials).toBe("include");
  expect(new Headers(mutation.headers).get("X-CSRF-Token")).toBe("csrf");
  expect(localStorage).toHaveLength(0);
  expect(sessionStorage.getItem("crucible.csrf")).toBe("csrf");
});

it("rotates the readable CSRF token after a browser reload", async () => {
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce(
      new Response(JSON.stringify({ csrfToken: "rotated" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    )
    .mockResolvedValueOnce(new Response("{}"));
  vi.stubGlobal("fetch", fetchMock);
  sessionStorage.setItem("crucible.csrf", "retained-csrf");

  await bootstrapFromLocation();
  await sessionFetch("/api/tasks/task/messages", { method: "POST" });

  expect(fetchMock).toHaveBeenNthCalledWith(1, "/api/auth/session", {
    method: "POST",
    credentials: "include",
    headers: { "X-CSRF-Token": "retained-csrf" },
  });
  const mutation = fetchMock.mock.calls[1][1] as RequestInit;
  expect(new Headers(mutation.headers).get("X-CSRF-Token")).toBe("rotated");
});
