import { useRef, useState } from "react";
import { repairLink } from "./repairLink";
import type { RepairStatusView } from "../../api/client";
import { Button } from "../../components/ui/button";

export function RepairStatus({
  repair,
  stop,
  command,
}: {
  repair: RepairStatusView;
  stop: (key: string) => Promise<unknown>;
  command: (text: string, key: string) => Promise<unknown>;
}) {
  const keys = useRef<Record<string, string>>({});
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const active = ["queued", "running"].includes(repair.status);
  const resumable = ["stopped", "interrupted", "retryable_error"].includes(
    repair.status,
  );
  const url = repairLink(repair.prUrl);
  async function act(
    action: string,
    operation: (key: string) => Promise<unknown>,
  ) {
    setBusy(true);
    setError("");
    try {
      const key = (keys.current[action] ??= crypto.randomUUID());
      await operation(key);
      delete keys.current[action];
    } catch (reason) {
      setError(
        reason instanceof Error ? reason.message : "Repair action failed",
      );
    } finally {
      setBusy(false);
    }
  }
  return (
    <section
      aria-label="Repair status"
      className="my-2 rounded-xl border p-3 text-sm"
      aria-live="polite"
    >
      <p className="font-medium">
        Repair · {repair.status} · {repair.stage.replaceAll("_", " ")}
      </p>
      <p className="text-xs text-muted-foreground">
        {Object.entries(repair.attempts)
          .map(([role, count]) => `${role}: ${count}`)
          .join(" · ")}
      </p>
      {repair.detail && <p>{repair.detail}</p>}
      {url && (
        <a className="underline" href={url} target="_blank" rel="noreferrer">
          Review repair PR
        </a>
      )}
      <div className="mt-2 flex gap-2">
        {active && (
          <Button
            disabled={busy}
            variant="outline"
            onClick={() => void act("stop", stop)}
          >
            Stop repair
          </Button>
        )}
        {resumable && (
          <>
            <Button
              disabled={busy}
              variant="outline"
              onClick={() =>
                void act("resume", (key) => command("/repair resume", key))
              }
            >
              Resume repair
            </Button>
            <Button
              disabled={busy}
              variant="ghost"
              onClick={() =>
                void act("close", (key) => command("/repair close", key))
              }
            >
              Close repair
            </Button>
          </>
        )}
      </div>
      {error && <p role="alert">{error}</p>}
    </section>
  );
}
