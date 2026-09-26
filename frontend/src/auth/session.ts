let csrfToken: string | undefined;
let refreshPromise: Promise<string | undefined> | undefined;

async function refreshSession(): Promise<string | undefined> {
  if (!refreshPromise) {
    refreshPromise = (async () => {
      const response = await fetch("/api/auth/session", {
        method: "POST",
        credentials: "include",
      });
      if (!response.ok) {
        csrfToken = undefined;
        sessionStorage.removeItem("crucible.csrf");
        return undefined;
      }
      const body = (await response.json()) as { csrfToken: string };
      csrfToken = body.csrfToken;
      sessionStorage.setItem("crucible.csrf", csrfToken);
      return csrfToken;
    })().finally(() => {
      refreshPromise = undefined;
    });
  }
  return refreshPromise;
}

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
  await refreshSession();
}

export async function sessionFetch(
  input: RequestInfo | URL,
  init: RequestInit = {},
): Promise<Response> {
  const method = (init.method ?? "GET").toUpperCase();
  const unsafe = ["POST", "PUT", "PATCH", "DELETE"].includes(method);
  if (unsafe && !csrfToken) {
    await refreshSession();
  }
  async function send(): Promise<Response> {
    const headers = new Headers(init.headers);
    if (unsafe && csrfToken) headers.set("X-CSRF-Token", csrfToken);
    return fetch(input, { ...init, credentials: "include", headers });
  }
  const response = await send();
  if (unsafe && response.status === 403) {
    const body = (await response.clone().json().catch(() => ({}))) as {
      code?: string;
    };
    if (body.code === "csrf_rejected" && (await refreshSession())) {
      return send();
    }
  }
  return response;
}
