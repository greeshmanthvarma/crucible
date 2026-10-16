import { useCallback, useEffect, useRef, useState } from "react";

import type {
  CrucibleClient,
  RepairStatusView,
  RepairCommandResponse,
  ApprovalResponse,
  MessageResponse,
  StepTraceResponse,
  TaskResponse,
  TaskReviewResponse,
  WorkspaceStateResponse,
} from "../../api/client";
import {
  openTaskEventStream,
  reduceTaskEvents,
  type EventStreamFactory,
  type TaskEventState,
} from "../../events/taskEventStream";

const emptyReview: TaskReviewResponse = {
  latestRunStatus: null,
  completionSummary: null,
  claimedFiles: [],
  validationAttempts: [],
  resultRevisions: [],
  integrations: [],
};

export function useTaskSession(
  taskId: string,
  client: CrucibleClient,
  streamFactory: EventStreamFactory = openTaskEventStream,
) {
  const [repairs, setRepairs] = useState<RepairStatusView[]>([]);
  const [repairResult, setRepairResult] = useState<RepairCommandResponse>();
  const [task, setTask] = useState<TaskResponse>();
  const [messages, setMessages] = useState<MessageResponse[]>([]);
  const [trace, setTrace] = useState<StepTraceResponse[]>([]);
  const [workspace, setWorkspace] = useState<WorkspaceStateResponse>();
  const [approvals, setApprovals] = useState<ApprovalResponse[]>([]);
  const [review, setReview] = useState<TaskReviewResponse>(emptyReview);
  const [pending, setPending] = useState<{ text: string; key: string }>();
  const [failedRequest, setFailedRequest] = useState<{
    text: string;
    key: string;
  }>();
  const [error, setError] = useState("");
  const [events, setEvents] = useState<TaskEventState>({
    events: [],
    needsReplay: false,
  });
  const eventState = useRef<TaskEventState>({ events: [], needsReplay: false });

  const refreshVersion = useRef(0);
  const refresh = useCallback(async () => {
    const version = ++refreshVersion.current;
    const [
      loadedTask,
      loadedMessages,
      loadedTrace,
      loadedWorkspace,
      loadedApprovals,
      loadedReview,
      loadedRepairs,
    ] = await Promise.all([
      client.getTask(taskId),
      client.getMessages(taskId),
      client.getTaskTrace(taskId),
      client.getWorkspaceState(taskId),
      client.getApprovals(taskId),
      client.getTaskReview?.(taskId) ?? Promise.resolve(emptyReview),
      client.getRepairs?.(taskId) ?? Promise.resolve([]),
    ]);
    if (version !== refreshVersion.current) return;
    setTask(loadedTask);
    setMessages(loadedMessages);
    setTrace(loadedTrace);
    setWorkspace(loadedWorkspace);
    setApprovals(loadedApprovals);
    setReview(loadedReview);
    setRepairs(loadedRepairs);
  }, [client, taskId]);

  useEffect(() => {
    let disposed = false;
    const version = ++refreshVersion.current;
    eventState.current = { events: [], needsReplay: false };
    let stream: ReturnType<EventStreamFactory> | undefined;
    void Promise.all([
      client.getTask(taskId),
      client.getMessages(taskId),
      client.getTaskTrace(taskId),
      client.getWorkspaceState(taskId),
      client.getApprovals(taskId),
      client.getTaskReview?.(taskId) ?? Promise.resolve(emptyReview),
      client.getRepairs?.(taskId) ?? Promise.resolve([]),
    ]).then(
      ([
        loadedTask,
        loadedMessages,
        loadedTrace,
        loadedWorkspace,
        loadedApprovals,
        loadedReview,
        loadedRepairs,
      ]) => {
        if (disposed || version !== refreshVersion.current) return;
        setEvents(eventState.current);
        setRepairResult(undefined);
        setTask(loadedTask);
        setMessages(loadedMessages);
        setTrace(loadedTrace);
        setWorkspace(loadedWorkspace);
        setApprovals(loadedApprovals);
        setReview(loadedReview);
        setRepairs(loadedRepairs);
        let opened = false;
        stream = streamFactory(
          taskId,
          (event) => {
            const next = reduceTaskEvents(eventState.current, event);
            if (next === eventState.current) return;
            eventState.current = next;
            setEvents(next);
            void refresh().catch((reason) =>
              setError(
                reason instanceof Error ? reason.message : "Refresh failed",
              ),
            );
          },
          () => {
            if (opened)
              void refresh().catch((reason) =>
                setError(
                  reason instanceof Error ? reason.message : "Refresh failed",
                ),
              );
            opened = true;
          },
        );
      },
      (reason) => {
        if (!disposed)
          setError(
            reason instanceof Error ? reason.message : "Task load failed",
          );
      },
    );
    return () => {
      disposed = true;
      stream?.close();
    };
  }, [client, refresh, streamFactory, taskId]);

  async function send(payload: { text: string; key: string }) {
    setPending(payload);
    setFailedRequest(undefined);
    setError("");
    try {
      const response = await client.sendMessage(
        taskId,
        payload.text,
        payload.key,
      );
      setRepairResult(
        response && "prUrls" in response && response.kind === "repair_command"
          ? response
          : undefined,
      );
      setPending(undefined);
      await refresh();
    } catch (reason) {
      setPending(undefined);
      setFailedRequest(payload);
      setError(reason instanceof Error ? reason.message : "Message failed");
      throw reason;
    }
  }

  async function stopRepair(repairId: string, key: string) {
    await client.stopRepair(repairId, key);
    await refresh();
  }

  async function decideApproval(
    approvalId: string,
    decision: "approved" | "denied",
    specDigest: string,
    key: string,
  ) {
    await client.decideApproval(approvalId, decision, specDigest, key);
    await refresh();
  }

  async function accept(key: string) {
    await client.acceptTask(taskId, key);
    await refresh();
  }

  async function integrate(
    resultId: string,
    repositoryId: string,
    targetRef: string,
    expectedRevision: string,
    key: string,
  ) {
    await client.integrateResult(
      resultId,
      repositoryId,
      targetRef,
      expectedRevision,
      key,
    );
    await refresh();
  }

  return {
    task,
    repairs,
    repairResult,
    stopRepair,
    messages,
    trace,
    workspace,
    approvals,
    review,
    pending,
    failedRequest,
    error,
    events,
    send,
    decideApproval,
    accept,
    integrate,
  };
}
