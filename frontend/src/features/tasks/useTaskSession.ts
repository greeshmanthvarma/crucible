import { useCallback, useEffect, useRef, useState } from "react";

import type {
  CrucibleClient,
  MessageResponse,
  StepTraceResponse,
  TaskResponse,
  WorkspaceStateResponse,
} from "../../api/client";
import {
  openTaskEventStream,
  reduceTaskEvents,
  type EventStreamFactory,
  type TaskEventState,
} from "../../events/taskEventStream";

export function useTaskSession(
  taskId: string,
  client: CrucibleClient,
  streamFactory: EventStreamFactory = openTaskEventStream,
) {
  const [task, setTask] = useState<TaskResponse>();
  const [messages, setMessages] = useState<MessageResponse[]>([]);
  const [trace, setTrace] = useState<StepTraceResponse[]>([]);
  const [workspace, setWorkspace] = useState<WorkspaceStateResponse>();
  const [pending, setPending] = useState<{ text: string; key: string }>();
  const [error, setError] = useState("");
  const [events, setEvents] = useState<TaskEventState>({
    events: [],
    needsReplay: false,
  });
  const eventState = useRef<TaskEventState>({ events: [], needsReplay: false });

  const refresh = useCallback(async () => {
    const [loadedMessages, loadedTrace, loadedWorkspace] = await Promise.all([
      client.getMessages(taskId),
      client.getTaskTrace(taskId),
      client.getWorkspaceState(taskId),
    ]);
    setMessages(loadedMessages);
    setTrace(loadedTrace);
    setWorkspace(loadedWorkspace);
  }, [client, taskId]);

  useEffect(() => {
    let disposed = false;
    let stream: ReturnType<EventStreamFactory> | undefined;
    void Promise.all([
      client.getTask(taskId),
      client.getMessages(taskId),
      client.getTaskTrace(taskId),
      client.getWorkspaceState(taskId),
    ]).then(([loadedTask, loadedMessages, loadedTrace, loadedWorkspace]) => {
      if (disposed) return;
      setTask(loadedTask);
      setMessages(loadedMessages);
      setTrace(loadedTrace);
      setWorkspace(loadedWorkspace);
      let opened = false;
      stream = streamFactory(
        taskId,
        (event) => {
          const next = reduceTaskEvents(eventState.current, event);
          if (next === eventState.current) return;
          eventState.current = next;
          setEvents(next);
          if (
            event.type === "message.completed" ||
            event.type.startsWith("run.") ||
            event.type.startsWith("step.") ||
            event.type.startsWith("tool_call.") ||
            event.type === "context.prepared"
          ) {
            void refresh();
          }
        },
        () => {
          if (opened) void refresh();
          opened = true;
        },
      );
    });
    return () => {
      disposed = true;
      stream?.close();
    };
  }, [client, refresh, streamFactory, taskId]);

  async function send(payload: { text: string; key: string }) {
    setPending(payload);
    setError("");
    try {
      await client.sendMessage(taskId, payload.text, payload.key);
      setPending(undefined);
      await refresh();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Message failed");
      throw reason;
    }
  }

  return { task, messages, trace, workspace, pending, error, events, send };
}
