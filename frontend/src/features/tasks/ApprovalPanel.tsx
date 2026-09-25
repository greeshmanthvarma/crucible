import { useRef, useState } from "react";

import type { ApprovalResponse } from "../../api/client";

export function ApprovalPanel({
  approvals,
  decide,
}: {
  approvals: ApprovalResponse[];
  decide: (
    approvalId: string,
    decision: "approved" | "denied",
    digest: string,
    key: string,
  ) => Promise<void>;
}) {
  const keys = useRef(new Map<string, string>());
  const [submitting, setSubmitting] = useState<string>();

  function submit(approval: ApprovalResponse, decision: "approved" | "denied") {
    const existing = keys.current.get(approval.id);
    const key = existing ?? crypto.randomUUID();
    keys.current.set(approval.id, key);
    setSubmitting(approval.id);
    void decide(approval.id, decision, approval.specDigest, key)
      .catch(() => undefined)
      .finally(() => setSubmitting(undefined));
  }

  return (
    <section aria-label="Command approvals">
      <h3>Command approvals</h3>
      {approvals.map((approval) => (
        <article key={approval.id}>
          <h4>{approval.spec.reason}</h4>
          <p>
            Command: {approval.spec.executable}{" "}
            {approval.spec.arguments.join(" ")}
          </p>
          <p>Working directory: {approval.spec.cwd}</p>
          <p>Network: {approval.spec.network}</p>
          <p>Timeout: {approval.spec.timeoutSeconds}s</p>
          <p>Image: {approval.spec.image}</p>
          <p>
            Environment names:{" "}
            {approval.spec.environmentNames.join(", ") || "none"}
          </p>
          <p>Digest: {approval.specDigest}</p>
          <p>Status: {approval.status}</p>
          {approval.status === "pending" && (
            <div>
              <button
                disabled={submitting === approval.id}
                onClick={() => submit(approval, "approved")}
              >
                Approve
              </button>
              <button
                disabled={submitting === approval.id}
                onClick={() => submit(approval, "denied")}
              >
                Deny
              </button>
            </div>
          )}
        </article>
      ))}
    </section>
  );
}
