import { useState } from "react";

import { apiClient, type RepositoryResponse, type TaskResponse } from "./api/client";
import { RepositoryForm } from "./features/repositories/RepositoryForm";
import { TaskView } from "./features/tasks/TaskView";

export function App() {
  const [repository, setRepository] = useState<RepositoryResponse>();
  const [task, setTask] = useState<TaskResponse>();

  return (
    <main>
      <h1>Crucible</h1>
      <p>Eval-driven coding-agent harness</p>
      <RepositoryForm client={apiClient} onRegistered={setRepository} />
      {repository && !task && (
        <button onClick={() => void apiClient.createTask(repository.id, "HEAD").then(setTask)}>
          Create Task from HEAD
        </button>
      )}
      {task && <TaskView taskId={task.id} client={apiClient} />}
    </main>
  );
}
