import { FormEvent, useEffect, useRef, useState } from "react";

import type {
  CrucibleClient,
  MessageResponse,
  TaskResponse,
} from "../../api/client";
import {
  openTaskEventStream,
  reduceTaskEvents,
  type EventStreamFactory,
  type TaskEventState,
} from "../../events/taskEventStream";

export function TaskView({
  taskId,
  client,
  streamFactory = openTaskEventStream,
}: {
  taskId: string;
  client: CrucibleClient;
  streamFactory?: EventStreamFactory;
}) {
  const [task, setTask] = useState<TaskResponse>();
  const [messages, setMessages] = useState<MessageResponse[]>([]);
  const [text, setText] = useState("");
  const [pending, setPending] = useState<{ text: string; key: string }>();
  const [error, setError] = useState("");
  const [events, setEvents] = useState<TaskEventState>({
    events: [],
    needsReplay: false,
  });
  const eventState = useRef<TaskEventState>({ events: [], needsReplay: false });

  async function refreshMessages() {
    setMessages(await client.getMessages(taskId));
  }

  useEffect(() => {
    let disposed = false;
    let stream: ReturnType<EventStreamFactory> | undefined;
    void Promise.all([client.getTask(taskId), client.getMessages(taskId)]).then(
      ([loadedTask, loadedMessages]) => {
        if (disposed) return;
        setTask(loadedTask);
        setMessages(loadedMessages);
        let opened = false;
        stream = streamFactory(
          taskId,
          (event) => {
            const next = reduceTaskEvents(eventState.current, event);
            if (next === eventState.current) return;
            eventState.current = next;
            setEvents(next);
            if (event.type === "message.completed") {
              void client.getMessages(taskId).then(setMessages);
            }
          },
          () => {
            if (opened) void client.getMessages(taskId).then(setMessages);
            opened = true;
          },
        );
      },
    );
    return () => {
      disposed = true;
      stream?.close();
    };
  }, [client, streamFactory, taskId]);

  async function send(payload: { text: string; key: string }) {
    setPending(payload);
    setError("");
    try {
      await client.sendMessage(taskId, payload.text, payload.key);
      setText("");
      setPending(undefined);
      await refreshMessages();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Message failed");
    }
  }

  function submit(event: FormEvent) {
    event.preventDefault();
    if (text.trim()) void send({ text, key: crypto.randomUUID() });
  }

  const latestRunEvent = [...events.events]
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
      {task && (
        <header>
          <h2>Task</h2>
          <p>Status: {task.status}</p>
          <p>Source: {task.sourceRef}</p>
          <p>Base revision: {task.baseRevision}</p>
        </header>
      )}
      <p>Run: {runStatus}</p>
      <ol aria-label="Conversation">
        {messages.map((message) => (
          <li key={message.id}>
            <strong>{message.role}</strong>{" "}
            {message.parts.map((part) => part.textContent).join("")}
          </li>
        ))}
        {pending && <li>sending: {pending.text}</li>}
      </ol>
      <form onSubmit={submit}>
        <label>
          Message
          <textarea
            value={text}
            onChange={(event) => setText(event.target.value)}
          />
        </label>
        <button disabled={Boolean(pending) || !text.trim()}>Send</button>
      </form>
      {error && (
        <p role="alert">
          {error}{" "}
          <button onClick={() => pending && void send(pending)}>Retry</button>
        </p>
      )}
    </section>
  );
}
