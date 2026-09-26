import { useRef, useState } from "react";

import type { ApprovalResponse } from "../../api/client";
import { Button } from "../../components/ui/button";
import { Badge } from "../../components/ui/badge";

export function CommandApprovalCard({
  approval,
  decide,
}: {
  approval: ApprovalResponse;
  decide: (
    approvalId: string,
    decision: "approved" | "denied",
    digest: string,
    key: string,
  ) => Promise<void>;
}) {
  const key = useRef<string>(undefined);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");

  async function submit(decision: "approved" | "denied") {
    key.current ??= crypto.randomUUID();
    setSubmitting(true);
    setError("");
    try {
      await decide(approval.id, decision, approval.specDigest, key.current);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Decision failed");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <section aria-label="Command approval" className="space-y-3 rounded-xl border border-amber-500/40 bg-amber-500/5 p-4 text-sm">
      <div className="flex items-center justify-between gap-3">
        <h4 className="font-medium">Command approval</h4>
        <Badge variant={approval.status === "pending" ? "secondary" : "outline"}>
          {approval.status}
        </Badge>
      </div>
      <p>{approval.spec.reason}</p>
      <pre className="overflow-x-auto rounded-md bg-muted px-3 py-2 font-mono text-xs">{[approval.spec.executable, ...approval.spec.arguments].join(" ")}</pre>
      <div className="grid gap-x-4 gap-y-1 text-xs text-muted-foreground sm:grid-cols-2">
        <p>Working directory: {approval.spec.cwd}</p>
        <p>Network: {approval.spec.network}</p>
        <p>Timeout: {approval.spec.timeoutSeconds}s</p>
        <p>Environment names: {approval.spec.environmentNames.join(", ") || "none"}</p>
        <p className="col-span-full break-all">Image: {approval.spec.image}</p>
        <p className="col-span-full break-all">Digest: {approval.specDigest}</p>
      </div>
      {approval.status === "pending" && (
        <div className="flex gap-2">
          <Button disabled={submitting} onClick={() => void submit("approved")}>Approve</Button>
          <Button variant="outline" disabled={submitting} onClick={() => void submit("denied")}>Deny</Button>
        </div>
      )}
      {error && <p role="alert" className="text-destructive">{error}</p>}
    </section>
  );
}
