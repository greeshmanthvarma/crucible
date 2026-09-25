import { useRef, useState } from "react";

import type {
  IntegrationResponse,
  ResultRevisionResponse,
} from "../../api/client";

export function IntegrationPanel({
  repositoryId,
  results,
  integrations,
  integrate,
}: {
  repositoryId: string;
  results: ResultRevisionResponse[];
  integrations: IntegrationResponse[];
  integrate: (
    resultId: string,
    repositoryId: string,
    targetRef: string,
    expectedRevision: string,
    key: string,
  ) => Promise<unknown>;
}) {
  const [selectedResult, setSelectedResult] = useState("");
  const selected = selectedResult || results.at(-1)?.id || "";
  const [targetRef, setTargetRef] = useState("");
  const [expected, setExpected] = useState("");
  const [error, setError] = useState("");
  const keys = useRef(new Map<string, string>());
  async function submit() {
    setError("");
    const request = `${selected}\0${targetRef}\0${expected}`;
    const key = keys.current.get(request) ?? crypto.randomUUID();
    keys.current.set(request, key);
    try {
      await integrate(selected, repositoryId, targetRef, expected, key);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Integration failed");
    }
  }
  return (
    <section aria-label="Integration">
      <h3>Integration</h3>
      <p>
        The target checkout must be clean and still at the expected revision.
      </p>
      <label>
        Result Revision
        <select
          value={selected}
          onChange={(event) => setSelectedResult(event.target.value)}
        >
          {results.map((result) => (
            <option key={result.id} value={result.id}>
              {result.commitSha}
            </option>
          ))}
        </select>
      </label>
      <label>
        Target ref
        <input
          value={targetRef}
          onChange={(event) => setTargetRef(event.target.value)}
        />
      </label>
      <label>
        Expected revision
        <input
          value={expected}
          onChange={(event) => setExpected(event.target.value)}
        />
      </label>
      <button
        disabled={!selected || !targetRef || !expected}
        onClick={() => void submit()}
      >
        Integrate result
      </button>
      {error && <p role="alert">{error}</p>}
      {integrations.map((item) => (
        <p key={item.id}>
          Integration: {item.status}
          {item.failureDetail && ` — ${item.failureDetail}`}
        </p>
      ))}
    </section>
  );
}
