import { beforeEach, expect, it, vi } from "vitest";

beforeEach(() => {
  vi.resetModules();
  vi.unstubAllGlobals();
  sessionStorage.clear();
  history.replaceState(null, "", "/");
});

it("recovers CSRF from an existing cookie when browser storage is empty", async () => {
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce(
      new Response(JSON.stringify({ csrfToken: "recovered" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    )
    .mockResolvedValueOnce(new Response("{}"));
  vi.stubGlobal("fetch", fetchMock);
  const { bootstrapFromLocation, sessionFetch } = await import("./session");

  await bootstrapFromLocation();
  await sessionFetch("/api/tasks/task/messages", { method: "POST" });

  expect(fetchMock.mock.calls[0][0]).toBe("/api/auth/session");
  const mutation = fetchMock.mock.calls[1][1] as RequestInit;
  expect(new Headers(mutation.headers).get("X-CSRF-Token")).toBe("recovered");
});

it("recovers CSRF before retrying a draft from an already-open page", async () => {
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce(
      new Response(JSON.stringify({ csrfToken: "recovered" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    )
    .mockResolvedValueOnce(new Response("{}"));
  vi.stubGlobal("fetch", fetchMock);
  const { sessionFetch } = await import("./session");

  await sessionFetch("/api/tasks/task/messages", { method: "POST" });

  expect(fetchMock.mock.calls[0][0]).toBe("/api/auth/session");
  const mutation = fetchMock.mock.calls[1][1] as RequestInit;
  expect(new Headers(mutation.headers).get("X-CSRF-Token")).toBe("recovered");
});

it("refreshes and retries once when another tab rotated CSRF", async () => {
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce(
      new Response(JSON.stringify({ csrfToken: "old" }), {
        status: 201,
        headers: { "Content-Type": "application/json" },
      }),
    )
    .mockResolvedValueOnce(
      new Response(JSON.stringify({ code: "csrf_rejected" }), {
        status: 403,
        headers: { "Content-Type": "application/json" },
      }),
    )
    .mockResolvedValueOnce(
      new Response(JSON.stringify({ csrfToken: "new" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    )
    .mockResolvedValueOnce(new Response("{}"));
  vi.stubGlobal("fetch", fetchMock);
  const { bootstrapSession, sessionFetch } = await import("./session");

  await bootstrapSession("bootstrap-secret");
  const response = await sessionFetch("/api/tasks/task/messages", {
    method: "POST",
  });

  expect(response.ok).toBe(true);
  expect(fetchMock.mock.calls[2][0]).toBe("/api/auth/session");
  const retried = fetchMock.mock.calls[3][1] as RequestInit;
  expect(new Headers(retried.headers).get("X-CSRF-Token")).toBe("new");
});
