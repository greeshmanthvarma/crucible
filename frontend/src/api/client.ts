import type { components } from "./schema";
import { sessionFetch } from "../auth/session";

export type RepositoryResponse = components["schemas"]["RepositoryResponse"];
export type RepositoryTargetResponse =
  components["schemas"]["RepositoryTargetResponse"];
export type TaskResponse = components["schemas"]["TaskResponse"];
export type MessageResponse = components["schemas"]["MessageResponse"];
export type SubmittedRunResponse =
  components["schemas"]["SubmittedRunResponse"];
export type StepTraceResponse = components["schemas"]["StepTraceResponse"];
export type WorkspaceStateResponse =
  components["schemas"]["WorkspaceStateResponse"];
export type ApprovalResponse = components["schemas"]["ApprovalResponse"];
export type ResultRevisionResponse =
  components["schemas"]["ResultRevisionResponse"];
export type IntegrationResponse = components["schemas"]["IntegrationResponse"];

export type ValidationCommandReview = {
  id: string;
  commandSequence: number;
  status: string;
  approvalId: string | null;
  toolCallId: string | null;
  artifactId: string | null;
  exitCode: number | null;
  summary: string;
  createdAt: string;
  completedAt: string | null;
};
export type ValidationAttemptReview = {
  id: string;
  runId: string;
  attemptNumber: number;
  status: string;
  createdAt: string;
  completedAt: string | null;
  commands: ValidationCommandReview[];
};
export type TaskReviewResponse = {
  latestRunStatus: string | null;
  completionSummary: string | null;
  claimedFiles: string[];
  validationAttempts: ValidationAttemptReview[];
  resultRevisions: ResultRevisionResponse[];
  integrations: IntegrationResponse[];
};

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await sessionFetch(path, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!response.ok) {
    const body = (await response.json().catch(() => ({}))) as {
      detail?: string;
    };
    throw new ApiError(
      response.status,
      body.detail ?? `Request failed (${response.status})`,
    );
  }
  return response.json() as Promise<T>;
}

export interface CrucibleClient {
  registerRepository(path: string): Promise<RepositoryResponse>;
  listRepositories(): Promise<RepositoryResponse[]>;
  getRepositoryTarget(repositoryId: string): Promise<RepositoryTargetResponse>;
  listTasks(): Promise<TaskResponse[]>;
  createTask(
    repositoryId: string,
    sourceRef: string,
    idempotencyKey: string,
  ): Promise<TaskResponse>;
  getTask(taskId: string): Promise<TaskResponse>;
  getMessages(taskId: string): Promise<MessageResponse[]>;
  getTaskTrace(taskId: string): Promise<StepTraceResponse[]>;
  getWorkspaceState(taskId: string): Promise<WorkspaceStateResponse>;
  getApprovals(taskId: string): Promise<ApprovalResponse[]>;
  getTaskReview(taskId: string): Promise<TaskReviewResponse>;
  acceptTask(
    taskId: string,
    idempotencyKey: string,
  ): Promise<ResultRevisionResponse>;
  integrateResult(
    resultRevisionId: string,
    repositoryId: string,
    targetRef: string,
    expectedRevision: string,
    idempotencyKey: string,
  ): Promise<IntegrationResponse>;
  decideApproval(
    approvalId: string,
    decision: "approved" | "denied",
    specDigest: string,
    idempotencyKey: string,
    reason?: string,
  ): Promise<ApprovalResponse>;
  sendMessage(
    taskId: string,
    text: string,
    idempotencyKey: string,
  ): Promise<SubmittedRunResponse>;
}

export const apiClient: CrucibleClient = {
  listRepositories: () => request("/api/repositories"),
  getRepositoryTarget: (repositoryId) =>
    request(`/api/repositories/${repositoryId}/target`),
  listTasks: () => request("/api/tasks"),
  registerRepository: (path) =>
    request("/api/repositories", {
      method: "POST",
      body: JSON.stringify({ path }),
    }),
  createTask: (repositoryId, sourceRef, idempotencyKey) =>
    request(`/api/repositories/${repositoryId}/tasks`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
      body: JSON.stringify({ sourceRef }),
    }),
  getTask: (taskId) => request(`/api/tasks/${taskId}`),
  getMessages: (taskId) => request(`/api/tasks/${taskId}/messages`),
  getTaskTrace: (taskId) => request(`/api/tasks/${taskId}/trace`),
  getWorkspaceState: (taskId) => request(`/api/tasks/${taskId}/workspace`),
  getApprovals: (taskId) => request(`/api/tasks/${taskId}/approvals`),
  getTaskReview: (taskId) => request(`/api/tasks/${taskId}/review`),
  acceptTask: (taskId, idempotencyKey) =>
    request(`/api/tasks/${taskId}/acceptances`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
      body: "{}",
    }),
  integrateResult: (
    resultRevisionId,
    repositoryId,
    targetRef,
    expectedRevision,
    idempotencyKey,
  ) =>
    request(`/api/result-revisions/${resultRevisionId}/integrations`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
      body: JSON.stringify({ repositoryId, targetRef, expectedRevision }),
    }),
  decideApproval: (approvalId, decision, specDigest, idempotencyKey, reason) =>
    request(`/api/approvals/${approvalId}/decision`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
      body: JSON.stringify({ decision, specDigest, reason }),
    }),
  sendMessage: (taskId, text, idempotencyKey) =>
    request(`/api/tasks/${taskId}/messages`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
      body: JSON.stringify({ text }),
    }),
};
