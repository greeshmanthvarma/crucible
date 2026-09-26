import { FormEvent, useState } from "react";
import { ArrowUp, FolderGit2 } from "lucide-react";

import type { CrucibleClient, RepositoryResponse } from "../../api/client";
import { Button } from "../../components/ui/button";
import {
  MessageScroller,
  MessageScrollerButton,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerProvider,
  MessageScrollerViewport,
} from "../../components/ui/message-scroller";
import { Textarea } from "../../components/ui/textarea";
import {
  openTaskEventStream,
  type EventStreamFactory,
} from "../../events/taskEventStream";
import { CommandApprovalCard } from "./ApprovalPanel";
import { ToolCallCard } from "./ToolCallCard";
import { useTaskSession } from "./useTaskSession";
import { WorkspaceDiff } from "./WorkspaceDiff";
import { ValidationTrace } from "./ValidationTrace";
import { AcceptancePanel } from "./AcceptancePanel";
import { IntegrationPanel } from "./IntegrationPanel";
import { ArtifactDiff } from "./ArtifactDiff";
import { Badge } from "../../components/ui/badge";

export function TaskView({
  taskId,
  client,
  repository,
  streamFactory = openTaskEventStream,
}: {
  taskId: string;
  client: CrucibleClient;
  repository?: RepositoryResponse;
  streamFactory?: EventStreamFactory;
}) {
  const session = useTaskSession(taskId, client, streamFactory);
  const [text, setText] = useState("");

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!text.trim()) return;
    void session.send({ text, key: crypto.randomUUID() }).then(
      () => setText(""),
      () => undefined,
    );
  }

  const latestRunEvent = [...session.events.events]
    .reverse()
    .find((event) => event.type.startsWith("run."));
  const runStatus =
    latestRunEvent?.type === "run.started"
      ? "running"
      : latestRunEvent?.type === "run.interrupted"
        ? "failed"
        : (latestRunEvent?.type.replace("run.", "") ?? "idle");
  const canonicalRunStatus = session.review.latestRunStatus ?? runStatus;
  const latestValidation = session.review.validationAttempts.at(-1);
  const acceptedResult =
    session.task?.status === "active"
      ? undefined
      : session.review.resultRevisions.at(-1);
  const hasWorkspaceChanges = Boolean(session.workspace?.status.trim());
  const acceptanceEnabled =
    session.task?.status === "active" &&
    canonicalRunStatus === "completed" &&
    hasWorkspaceChanges &&
    (latestValidation?.status === "passed" ||
      latestValidation?.status === "not_configured");
  const acceptanceReason = ["queued", "running", "validating"].includes(
    canonicalRunStatus,
  )
    ? "Wait for the run to finish before accepting"
    : canonicalRunStatus !== "completed"
      ? "The latest run must complete before acceptance"
      : !hasWorkspaceChanges
        ? "There are no workspace changes to accept"
        : "Acceptance requires passed or not-configured validation";
  const calls = new Map(
    session.trace.flatMap((step) =>
      step.calls.map((call) => [call.id, call] as const),
    ),
  );
  const results = new Map(
    session.trace.flatMap((step) =>
      step.results.map((result) => [result.toolCallId, result] as const),
    ),
  );
  const approvals = new Map(
    session.approvals.map(
      (approval) => [approval.toolCallId, approval] as const,
    ),
  );
  const visibleCallIds = new Set(
    session.messages.flatMap((message) =>
      message.parts
        .map((part) => part.toolCallId)
        .filter((id): id is string => Boolean(id)),
    ),
  );
  return (
    <section className="mx-auto flex h-[calc(100svh-3.75rem)] min-h-0 min-w-0 w-full max-w-4xl flex-col overflow-x-hidden px-4 sm:px-8">
      <div className="shrink-0 pt-5">
        {session.task && (
          <header className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
            <h2 className="sr-only">Task</h2>
            <p>Status: {session.task.status}</p>
            <p>Source: {session.task.sourceRef}</p>
            <p className="truncate">
              Workspace base: {session.task.workspaceBaseRevision ?? session.task.baseRevision}
            </p>
          </header>
        )}
        <p className="py-2 text-xs text-muted-foreground">
          Run: {canonicalRunStatus}
        </p>
      </div>
      <MessageScrollerProvider
        autoScroll
        defaultScrollPosition="last-anchor"
        scrollPreviousItemPeek={64}
      >
        <MessageScroller className="min-h-0 flex-1">
          <MessageScrollerViewport aria-label="Conversation">
            <MessageScrollerContent
              role="list"
              className="min-w-0 gap-7 px-1 py-6"
            >
              {session.messages
                .filter(
                  (message) =>
                    message.role !== "tool" &&
                    message.parts.some(
                      (part) =>
                        part.kind === "text" || part.kind === "tool_call",
                    ),
                )
                .map((message) => (
                  <MessageScrollerItem
                    key={message.id}
                    messageId={message.id}
                    scrollAnchor={message.role === "user"}
                    role="listitem"
                    className={
                      message.role === "user"
                        ? "ml-auto max-w-[85%] rounded-2xl bg-muted px-4 py-3"
                        : "w-full max-w-[95%] space-y-3 px-1"
                    }
                  >
                    {message.parts.map((part) => {
                      if (part.kind === "text" && part.textContent)
                        return (
                          <div
                            key={part.id}
                            className="whitespace-pre-wrap break-words text-sm leading-6"
                          >
                            {part.textContent}
                          </div>
                        );
                      if (part.kind !== "tool_call" || !part.toolCallId)
                        return null;
                      const call = calls.get(part.toolCallId);
                      if (!call) return null;
                      const approval = approvals.get(call.id);
                      return (
                        <div key={part.id} className="space-y-2">
                          <ToolCallCard
                            call={call}
                            result={results.get(call.id)}
                          />
                          {approval && (
                            <CommandApprovalCard
                              approval={approval}
                              decide={session.decideApproval}
                            />
                          )}
                        </div>
                      );
                    })}
                  </MessageScrollerItem>
                ))}
              {session.approvals
                .filter(
                  (approval) =>
                    approval.status === "pending" &&
                    !visibleCallIds.has(approval.toolCallId),
                )
                .map((approval) => (
                  <MessageScrollerItem
                    key={approval.id}
                    messageId={approval.id}
                    role="listitem"
                    className="w-full max-w-[95%]"
                  >
                    <CommandApprovalCard
                      approval={approval}
                      decide={session.decideApproval}
                    />
                  </MessageScrollerItem>
                ))}
              {session.pending && session.task?.status === "active" && (
                <MessageScrollerItem
                  messageId={`pending-${session.pending.key}`}
                  scrollAnchor
                  role="listitem"
                  className="ml-auto max-w-[85%] rounded-2xl bg-muted px-4 py-3 text-sm"
                >
                  sending: {session.pending.text}
                </MessageScrollerItem>
              )}
            </MessageScrollerContent>
          </MessageScrollerViewport>
          <MessageScrollerButton />
        </MessageScroller>
      </MessageScrollerProvider>
      <details className="max-h-[42vh] shrink-0 overflow-y-auto border-t border-border/70 py-3 text-sm">
        <summary className="flex cursor-pointer items-center justify-between gap-3 font-medium">
          <span>Review & integrate</span>
          <Badge variant="outline">
            {session.task?.status === "integrated"
              ? "Integrated"
              : acceptedResult
                ? "Ready to integrate"
                : acceptanceEnabled
                  ? "Ready to accept"
                  : "Not ready"}
          </Badge>
        </summary>
        <div className="space-y-5 pt-4">
          <section aria-label="Completion review">
            <h3 className="font-medium">Completion</h3>
            <p className="mt-1 text-sm text-muted-foreground">
              {session.review.completionSummary ?? "No completion proposal"}
            </p>
          </section>
          <ValidationTrace attempts={session.review.validationAttempts} />
          {session.task?.status === "active" && hasWorkspaceChanges && (
            <details className="rounded-xl border p-4">
              <summary className="cursor-pointer font-medium">
                Review pending changes
              </summary>
              <div className="pt-4">
                <WorkspaceDiff workspace={session.workspace} />
              </div>
            </details>
          )}
          <AcceptancePanel
            enabled={acceptanceEnabled}
            reason={acceptanceReason}
            validationStatus={latestValidation?.status}
            acceptedResult={acceptedResult}
            integrated={session.task?.status === "integrated"}
            accept={session.accept}
          />
          {session.task && (
            <IntegrationPanel
              repositoryId={session.task.repositoryId}
              results={session.review.resultRevisions}
              integrations={session.review.integrations}
              inspectTarget={client.getRepositoryTarget}
              integrate={session.integrate}
            />
          )}
          {session.review.resultRevisions.map((result) => (
            <details key={result.id} className="rounded-xl border p-4">
              <summary className="cursor-pointer font-medium">
                View accepted diff · {result.commitSha.slice(0, 12)}
              </summary>
              <div className="space-y-3 pt-4">
                <ArtifactDiff artifactId={result.diffArtifactId} />
                <a
                  className="text-xs underline"
                  href={`/api/artifacts/${result.diffArtifactId}`}
                >
                  Download diff
                </a>
              </div>
            </details>
          ))}
        </div>
      </details>
      <div className="shrink-0 bg-background/95 pb-4 pt-3 backdrop-blur">
        <form
          onSubmit={submit}
          className="rounded-2xl border bg-card p-3 shadow-[0_12px_40px_-24px_rgba(0,0,0,.35)]"
        >
          <span className="flex items-center gap-1.5 px-2 pb-2 text-xs font-medium text-muted-foreground">
            <FolderGit2 className="size-3.5" />
            {repository?.rootPath.split(/[\\/]/).filter(Boolean).at(-1) ??
              "Repository"}
          </span>
          <Textarea
            aria-label="Message"
            id="task-message"
            className="min-h-24 resize-none border-0 bg-transparent px-2 py-2 shadow-none focus-visible:ring-0"
            placeholder={
              session.task?.status === "provisioning_failed"
                ? "This workspace could not be created."
                : "Message Crucible…"
            }
            value={text}
            disabled={Boolean(session.task && !["active", "accepted", "integrated", "continuing"].includes(session.task.status))}
            onChange={(event) => setText(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter" && !event.shiftKey) {
                event.preventDefault();
                event.currentTarget.form?.requestSubmit();
              }
            }}
          />
          <div className="flex items-center justify-end border-t border-border/60 pt-3">
            <Button
              type="submit"
              aria-label="Send"
              size="icon"
              disabled={
                Boolean(session.pending) ||
                Boolean(session.task && !["active", "accepted", "integrated", "continuing"].includes(session.task.status)) ||
                !text.trim()
              }
            >
              <ArrowUp className="size-4" />
            </Button>
          </div>
        </form>
        {session.error && (
          <p role="alert" className="mt-2 text-sm text-destructive">
            {session.error}{" "}
            {session.task?.status === "active" && session.failedRequest && (
              <button
                className="underline"
                onClick={() => void session.send(session.failedRequest!)}
              >
                Retry
              </button>
            )}
          </p>
        )}
      </div>
    </section>
  );
}
