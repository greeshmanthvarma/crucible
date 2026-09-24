import { FormEvent, useState } from "react";

import type { CrucibleClient } from "../../api/client";
import {
  openTaskEventStream,
  type EventStreamFactory,
} from "../../events/taskEventStream";
import { ToolTrace } from "./ToolTrace";
import { ApprovalPanel } from "./ApprovalPanel";
import { CommandEvidence } from "./CommandEvidence";
import { useTaskSession } from "./useTaskSession";
import { WorkspaceDiff } from "./WorkspaceDiff";
import { ValidationTrace } from "./ValidationTrace";
import { AcceptancePanel } from "./AcceptancePanel";
import { IntegrationPanel } from "./IntegrationPanel";

export function TaskView({
  taskId,
  client,
  streamFactory = openTaskEventStream,
}: {
  taskId: string;
  client: CrucibleClient;
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
  const acceptanceEnabled =
    session.task?.status === "active" &&
    canonicalRunStatus === "completed" &&
    (latestValidation?.status === "passed" ||
      latestValidation?.status === "not_configured");
  const acceptanceReason = ["queued", "running", "validating"].includes(
    canonicalRunStatus,
  )
    ? "Acceptance is disabled while the Run is active"
    : "Acceptance requires passed or not-configured Validation";
  return (
    <section>
      {session.task && (
        <header>
          <h2>Task</h2>
          <p>Status: {session.task.status}</p>
          <p>Source: {session.task.sourceRef}</p>
          <p>Base revision: {session.task.baseRevision}</p>
        </header>
      )}
      <p>Run: {canonicalRunStatus}</p>
      <ol aria-label="Conversation">
        {session.messages.map((message) => (
          <li key={message.id}>
            <strong>{message.role}</strong>{" "}
            {message.parts.map((part) => part.textContent ?? "").join("")}
          </li>
        ))}
        {session.pending && <li>sending: {session.pending.text}</li>}
      </ol>
      <ToolTrace steps={session.trace} />
      <ApprovalPanel
        approvals={session.approvals}
        decide={session.decideApproval}
      />
      <CommandEvidence steps={session.trace} />
      <WorkspaceDiff workspace={session.workspace} />
      <section aria-label="Completion review">
        <h3>Completion</h3>
        <p>{session.review.completionSummary ?? "No completion proposal"}</p>
        <p>
          Changed files:{" "}
          {session.review.claimedFiles.join(", ") || "None claimed"}
        </p>
      </section>
      <ValidationTrace attempts={session.review.validationAttempts} />
      {session.review.resultRevisions.map((result) => (
        <article key={result.id}>
          <h3>Result Revision</h3>
          <p>Commit: {result.commitSha}</p>
          <p>{result.summary}</p>
          <a href={`/api/artifacts/${result.diffArtifactId}`}>Accepted diff</a>
        </article>
      ))}
      <AcceptancePanel
        enabled={acceptanceEnabled}
        reason={acceptanceReason}
        accept={session.accept}
      />
      {session.task && (
        <IntegrationPanel
          repositoryId={session.task.repositoryId}
          results={session.review.resultRevisions}
          integrations={session.review.integrations}
          integrate={session.integrate}
        />
      )}
      <form onSubmit={submit}>
        <label>
          Message
          <textarea
            value={text}
            onChange={(event) => setText(event.target.value)}
          />
        </label>
        <button disabled={Boolean(session.pending) || !text.trim()}>
          Send
        </button>
      </form>
      {session.error && (
        <p role="alert">
          {session.error}{" "}
          <button
            onClick={() =>
              session.pending && void session.send(session.pending)
            }
          >
            Retry
          </button>
        </p>
      )}
    </section>
  );
}
