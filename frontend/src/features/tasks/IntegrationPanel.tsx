import { useCallback, useEffect, useRef, useState } from "react";
import { ArrowRight, RefreshCw } from "lucide-react";

import type {
  IntegrationResponse,
  RepositoryTargetResponse,
  ResultRevisionResponse,
} from "../../api/client";
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

export function IntegrationPanel({
  repositoryId,
  results,
  integrations,
  inspectTarget,
  integrate,
}: {
  repositoryId: string;
  results: ResultRevisionResponse[];
  integrations: IntegrationResponse[];
  inspectTarget?: (repositoryId: string) => Promise<RepositoryTargetResponse>;
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
  const [target, setTarget] = useState<RepositoryTargetResponse>();
  const [loadingTarget, setLoadingTarget] = useState(false);
  const [targetError, setTargetError] = useState("");
  const [error, setError] = useState("");
  const [pending, setPending] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const keys = useRef(new Map<string, string>());

  const refreshTarget = useCallback(async () => {
    if (!inspectTarget) return;
    setLoadingTarget(true);
    setTargetError("");
    try {
      setTarget(await inspectTarget(repositoryId));
    } catch (cause) {
      setTarget(undefined);
      setTargetError(
        cause instanceof Error
          ? cause.message
          : "Could not inspect target checkout",
      );
    } finally {
      setLoadingTarget(false);
    }
  }, [inspectTarget, repositoryId]);

  useEffect(() => {
    if (results.length === 0 || !inspectTarget) return;
    let cancelled = false;
    void inspectTarget(repositoryId).then(
      (snapshot) => {
        if (!cancelled) setTarget(snapshot);
      },
      (cause: unknown) => {
        if (!cancelled)
          setTargetError(
            cause instanceof Error
              ? cause.message
              : "Could not inspect target checkout",
          );
      },
    );
    return () => {
      cancelled = true;
    };
  }, [results.length, inspectTarget, repositoryId]);

  const completed = integrations.some(
    (item) => item.resultRevisionId === selected && item.status === "completed",
  );
  const canIntegrate = Boolean(
    selected &&
    target?.currentRef &&
    target.clean &&
    !loadingTarget &&
    !completed &&
    !pending,
  );

  async function submit() {
    if (!canIntegrate || !target?.currentRef) return;
    setError("");
    setPending(true);
    const request = `${selected}\0${target.currentRef}\0${target.headRevision}`;
    const key = keys.current.get(request) ?? crypto.randomUUID();
    keys.current.set(request, key);
    try {
      await integrate(
        selected,
        repositoryId,
        target.currentRef,
        target.headRevision,
        key,
      );
      keys.current.delete(request);
      setConfirmOpen(false);
      await refreshTarget();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Integration failed");
    } finally {
      setPending(false);
    }
  }
  return (
    <section
      aria-label="Integration"
      className="space-y-4 rounded-xl border bg-card p-4"
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="font-medium">2. Integrate into repository</h3>
          <p className="text-xs text-muted-foreground">
            Apply the accepted commit to your local checkout. The target must be
            clean and still at the expected revision.
          </p>
        </div>
        <Badge
          variant={
            completed ? "secondary" : canIntegrate ? "outline" : "secondary"
          }
        >
          {completed ? "Integrated" : canIntegrate ? "Ready" : "Waiting"}
        </Badge>
      </div>
      {results.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          Accept changes before integrating.
        </p>
      ) : (
        <>
          <label className="block space-y-1 text-xs font-medium">
            <span>Result revision</span>
            <select
              aria-label="Result revision"
              className="h-9 w-full rounded-lg border bg-background px-2 font-mono text-xs"
              value={selected}
              onChange={(event) => setSelectedResult(event.target.value)}
            >
              {results.map((result) => (
                <option key={result.id} value={result.id}>
                  {result.commitSha.slice(0, 12)} —{" "}
                  {result.summary.slice(0, 60)}
                </option>
              ))}
            </select>
          </label>
          <div className="rounded-lg border bg-muted/30 p-3 text-sm">
            <div className="flex items-center justify-between gap-2">
              <p className="font-medium">Target checkout</p>
              <Button
                variant="ghost"
                size="sm"
                disabled={loadingTarget || !inspectTarget}
                onClick={() => void refreshTarget()}
              >
                <RefreshCw className="size-3.5" /> Refresh
              </Button>
            </div>
            {(loadingTarget || (!target && !targetError)) && (
              <p className="text-muted-foreground">
                Checking branch and working tree…
              </p>
            )}
            {targetError && (
              <p role="alert" className="text-destructive">
                {targetError}
              </p>
            )}
            {target && (
              <div className="mt-2 space-y-1 text-xs">
                <p>
                  Branch: <code>{target.currentRef ?? "Detached HEAD"}</code>
                </p>
                <p>
                  Current revision:{" "}
                  <code title={target.headRevision}>
                    {target.headRevision.slice(0, 12)}
                  </code>
                </p>
                <p>
                  Working tree:{" "}
                  <span
                    className={
                      target.clean
                        ? "text-emerald-700 dark:text-emerald-300"
                        : "text-destructive"
                    }
                  >
                    {target.clean ? "Clean" : "Has local changes"}
                  </span>
                </p>
              </div>
            )}
          </div>
          {!target?.clean && target && (
            <p className="text-xs text-destructive">
              Clear or commit local changes, then refresh. Crucible will not
              modify a dirty checkout.
            </p>
          )}
          {target?.currentRef === null && (
            <p className="text-xs text-destructive">
              Switch to a branch before integrating.
            </p>
          )}
          {completed && (
            <p className="text-sm">This result has already been integrated.</p>
          )}
          <Button disabled={!canIntegrate} onClick={() => setConfirmOpen(true)}>
            <ArrowRight className="size-4" /> Integrate result
          </Button>
        </>
      )}
      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}
      {integrations
        .filter((item) => item.resultRevisionId === selected)
        .map((item) => (
          <div key={item.id} className="rounded-lg border p-3 text-sm">
            <p className="font-medium">Integration: {item.status}</p>
            {item.failureDetail && (
              <p className="mt-1 text-destructive">{item.failureDetail}</p>
            )}
            {item.status === "recovery_required" && (
              <p className="mt-1 text-xs">
                Manual Git recovery is required before retrying.
              </p>
            )}
            {item.observedAfterRevision && (
              <p className="mt-1 text-xs">
                New revision:{" "}
                <code>{item.observedAfterRevision.slice(0, 12)}</code>
              </p>
            )}
          </div>
        ))}
      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              Integrate into {target?.currentRef ?? "repository"}?
            </DialogTitle>
            <DialogDescription>
              Crucible will cherry-pick the accepted result into your local
              checkout. It will verify the branch, revision, and clean working
              tree again before changing anything.
            </DialogDescription>
          </DialogHeader>
          <p className="font-mono text-xs">
            Expected target: {target?.headRevision.slice(0, 12)}
          </p>
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
            <Button disabled={!canIntegrate} onClick={() => void submit()}>
              {pending ? "Integrating…" : "Confirm integration"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  );
}
