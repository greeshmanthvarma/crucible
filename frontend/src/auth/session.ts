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
  sessionStorage.setItem("crucible.csrf", csrfToken);
}

export async function bootstrapFromLocation(): Promise<void> {
  const parameters = new URLSearchParams(window.location.hash.slice(1));
  const secret = parameters.get("bootstrap");
  if (secret) {
    await bootstrapSession(secret);
    history.replaceState(null, "", `${location.pathname}${location.search}`);
    return;
  }
  const retainedToken = sessionStorage.getItem("crucible.csrf");
  if (!retainedToken) return;
  const response = await fetch("/api/auth/session", {
    method: "POST",
    credentials: "include",
    headers: { "X-CSRF-Token": retainedToken },
  });
  if (!response.ok) {
    sessionStorage.removeItem("crucible.csrf");
    return;
  }
  const body = (await response.json()) as { csrfToken: string };
  csrfToken = body.csrfToken;
  sessionStorage.setItem("crucible.csrf", csrfToken);
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
