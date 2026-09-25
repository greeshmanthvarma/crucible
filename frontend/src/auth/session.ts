let csrfToken: string | undefined;

export async function bootstrapSession(secret: string): Promise<void> {
  const response = await fetch("/api/auth/bootstrap", {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ secret }),
  });
  if (!response.ok) throw new Error("Bootstrap authentication failed");
  const body = (await response.json()) as { csrfToken: string };
  csrfToken = body.csrfToken;
}

export async function bootstrapFromLocation(): Promise<void> {
  const parameters = new URLSearchParams(window.location.hash.slice(1));
  const secret = parameters.get("bootstrap");
  if (secret) {
    await bootstrapSession(secret);
    history.replaceState(null, "", `${location.pathname}${location.search}`);
    return;
  }
  const response = await fetch("/api/auth/session", { credentials: "include" });
  if (!response.ok) return;
  const body = (await response.json()) as { csrfToken: string };
  csrfToken = body.csrfToken;
}

export function sessionFetch(
  input: RequestInfo | URL,
  init: RequestInit = {},
): Promise<Response> {
  const method = (init.method ?? "GET").toUpperCase();
  const headers = new Headers(init.headers);
  if (["POST", "PUT", "PATCH", "DELETE"].includes(method) && csrfToken) {
    headers.set("X-CSRF-Token", csrfToken);
  }
  return fetch(input, { ...init, credentials: "include", headers });
}
