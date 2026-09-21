import type { components } from "./schema";

export type RepositoryResponse = components["schemas"]["RepositoryResponse"];
export type TaskResponse = components["schemas"]["TaskResponse"];
export type MessageResponse = components["schemas"]["MessageResponse"];
export type SubmittedRunResponse =
  components["schemas"]["SubmittedRunResponse"];

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
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
  createTask(repositoryId: string, sourceRef: string): Promise<TaskResponse>;
  getTask(taskId: string): Promise<TaskResponse>;
  getMessages(taskId: string): Promise<MessageResponse[]>;
  sendMessage(
    taskId: string,
    text: string,
    idempotencyKey: string,
  ): Promise<SubmittedRunResponse>;
}

export const apiClient: CrucibleClient = {
  registerRepository: (path) =>
    request("/api/repositories", {
      method: "POST",
      body: JSON.stringify({ path }),
    }),
  createTask: (repositoryId, sourceRef) =>
    request(`/api/repositories/${repositoryId}/tasks`, {
      method: "POST",
      body: JSON.stringify({ sourceRef }),
    }),
  getTask: (taskId) => request(`/api/tasks/${taskId}`),
  getMessages: (taskId) => request(`/api/tasks/${taskId}/messages`),
  sendMessage: (taskId, text, idempotencyKey) =>
    request(`/api/tasks/${taskId}/messages`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
      body: JSON.stringify({ text }),
    }),
};
