import { FormEvent, useState } from "react";

import type { CrucibleClient, RepositoryResponse } from "../../api/client";

export function RepositoryForm({
  client,
  onRegistered,
}: {
  client: CrucibleClient;
  onRegistered(repository: RepositoryResponse): void;
}) {
  const [path, setPath] = useState("");
  const [pending, setPending] = useState(false);
  const [repository, setRepository] = useState<RepositoryResponse>();
  const [error, setError] = useState("");

  async function submit(event: FormEvent) {
    event.preventDefault();
    setPending(true);
    setError("");
    try {
      const result = await client.registerRepository(path);
      setRepository(result);
      onRegistered(result);
    } catch (reason) {
      setError(
        reason instanceof Error ? reason.message : "Registration failed",
      );
    } finally {
      setPending(false);
    }
  }

  return (
    <form onSubmit={submit}>
      <label>
        Repository path
        <input value={path} onChange={(event) => setPath(event.target.value)} />
      </label>
      <button disabled={pending || !path.trim()}>
        {pending ? "Registering…" : "Register"}
      </button>
      {repository && <p>Registered: {repository.rootPath}</p>}
      {error && <p role="alert">{error}</p>}
    </form>
  );
}
