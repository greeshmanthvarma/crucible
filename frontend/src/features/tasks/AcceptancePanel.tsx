import { useRef, useState } from "react";

export function AcceptancePanel({
  enabled,
  reason,
  accept,
}: {
  enabled: boolean;
  reason?: string;
  accept: (key: string) => Promise<unknown>;
}) {
  const key = useRef(crypto.randomUUID());
  const [error, setError] = useState("");
  const [pending, setPending] = useState(false);

  async function submit() {
    setPending(true);
    setError("");
    try {
      await accept(key.current);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Acceptance failed");
    } finally {
      setPending(false);
    }
  }

  return (
    <section aria-label="Acceptance">
      <h3>Acceptance</h3>
      {!enabled && <p>{reason ?? "Acceptance is not available"}</p>}
      <button disabled={!enabled || pending} onClick={() => void submit()}>
        Accept result
      </button>
      {error && (
        <p role="alert">
          {error}{" "}
          <button onClick={() => void submit()}>Retry acceptance</button>
        </p>
      )}
    </section>
  );
}
