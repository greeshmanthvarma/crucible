import { FormEvent, useState } from "react";

import type { CrucibleClient } from "../../api/client";
import {
  openTaskEventStream,
  type EventStreamFactory,
} from "../../events/taskEventStream";
import { ToolTrace } from "./ToolTrace";
import { useTaskSession } from "./useTaskSession";
import { WorkspaceDiff } from "./WorkspaceDiff";

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
      <p>Run: {runStatus}</p>
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
      <WorkspaceDiff workspace={session.workspace} />
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
