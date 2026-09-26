import { useRef, useState } from "react";
import { Check, GitCommitHorizontal } from "lucide-react";

import type { ResultRevisionResponse } from "../../api/client";
import { Badge } from "../../components/ui/badge";
import { Button } from "../../components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "../../components/ui/dialog";

export function AcceptancePanel({
  enabled,
  reason,
  validationStatus,
  acceptedResult,
  integrated = false,
  accept,
}: {
  enabled: boolean;
  reason?: string;
  validationStatus?: string;
  acceptedResult?: ResultRevisionResponse;
  integrated?: boolean;
  accept: (key: string) => Promise<unknown>;
}) {
  const key = useRef(crypto.randomUUID());
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [error, setError] = useState("");
  const [pending, setPending] = useState(false);

  async function submit() {
    setPending(true);
    setError("");
    try {
      await accept(key.current);
      setConfirmOpen(false);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Acceptance failed");
    } finally {
      setPending(false);
    }
  }

  return (
    <section
      aria-label="Acceptance"
      className="space-y-3 rounded-xl border bg-card p-4"
    >
      <div className="flex items-center justify-between gap-3">
        <div>
          <h3 className="font-medium">1. Accept changes</h3>
          <p className="text-xs text-muted-foreground">
            Create a result commit in the task workspace. The main checkout is
            unchanged.
          </p>
        </div>
        <Badge
          variant={
            acceptedResult ? "secondary" : enabled ? "outline" : "secondary"
          }
        >
          {acceptedResult ? "Accepted" : enabled ? "Ready" : "Waiting"}
        </Badge>
      </div>
      {acceptedResult ? (
        <p className="flex items-center gap-2 text-sm">
          <Check className="size-4" /> Result commit{" "}
          <code className="font-mono">
            {acceptedResult.commitSha.slice(0, 12)}
          </code>{" "}
          {integrated ? " was integrated." : " is ready to integrate."}
        </p>
      ) : (
        <>
          {!enabled && (
            <p className="text-sm text-muted-foreground">
              {reason ?? "Acceptance is not available"}
            </p>
          )}
          {validationStatus === "not_configured" && enabled && (
            <p className="rounded-md border border-amber-500/30 bg-amber-500/5 p-2 text-xs">
              No automatic validation was configured. Review the changes before
              accepting.
            </p>
          )}
          <Button
            disabled={!enabled || pending}
            onClick={() => setConfirmOpen(true)}
          >
            <GitCommitHorizontal className="size-4" /> Accept result
          </Button>
        </>
      )}
      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Accept these changes?</DialogTitle>
            <DialogDescription>
              This commits the task workspace as a Result Revision. It does not
              modify your main checkout.
            </DialogDescription>
          </DialogHeader>
          {validationStatus === "not_configured" && (
            <p className="text-sm text-amber-700 dark:text-amber-300">
              Automatic validation was not configured for this run.
            </p>
          )}
          {error && (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          )}
          <DialogFooter>
            <Button
              variant="outline"
              disabled={pending}
              onClick={() => setConfirmOpen(false)}
            >
              Cancel
            </Button>
            <Button disabled={pending} onClick={() => void submit()}>
              {pending
                ? "Accepting…"
                : error
                  ? "Retry acceptance"
                  : "Confirm acceptance"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  );
}
