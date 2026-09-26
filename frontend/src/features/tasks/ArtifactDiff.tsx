import { useEffect, useState } from "react";

import { sessionFetch } from "../../auth/session";
import { UnifiedDiff } from "./UnifiedDiff";

export function ArtifactDiff({ artifactId }: { artifactId: string }) {
  const [diff, setDiff] = useState<string>();
  const [error, setError] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    void sessionFetch(`/api/artifacts/${artifactId}`, { signal: controller.signal })
      .then(async (response) => {
        if (!response.ok) throw new Error(`Could not load diff (${response.status})`);
        return response.text();
      })
      .then(setDiff)
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) setError(cause instanceof Error ? cause.message : "Could not load diff");
      });
    return () => controller.abort();
  }, [artifactId]);

  if (error) return <p role="alert" className="text-destructive">{error}</p>;
  if (diff === undefined) return <p className="text-sm text-muted-foreground">Loading accepted diff…</p>;
  return <UnifiedDiff diff={diff} />;
}
