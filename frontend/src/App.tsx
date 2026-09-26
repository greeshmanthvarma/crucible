import { useEffect, useState, type FormEvent } from "react";
import { ArrowUp, FolderGit2, Plus } from "lucide-react";

import {
  apiClient,
  type CrucibleClient,
  type RepositoryResponse,
  type TaskResponse,
} from "./api/client";
import { Button } from "./components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "./components/ui/dialog";
import { Input } from "./components/ui/input";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarTrigger,
} from "./components/ui/sidebar";
import { Textarea } from "./components/ui/textarea";
import { TaskView } from "./features/tasks/TaskView";

function repositoryName(repository: RepositoryResponse) {
  return (
    repository.rootPath.split(/[\\/]/).filter(Boolean).at(-1) ??
    repository.rootPath
  );
}

function taskLabel(task: TaskResponse, repositories: RepositoryResponse[]) {
  const repository = repositories.find((item) => item.id === task.repositoryId);
  return `${repository ? repositoryName(repository) : "Repository"} · ${new Date(task.createdAt).toLocaleDateString()}`;
}

export function App({ client = apiClient }: { client?: CrucibleClient }) {
  const [repositories, setRepositories] = useState<RepositoryResponse[]>([]);
  const [tasks, setTasks] = useState<TaskResponse[]>([]);
  const [repositoryId, setRepositoryId] = useState<string>();
  const [taskId, setTaskId] = useState<string>();
  const [draft, setDraft] = useState("");
  const [pendingSubmission, setPendingSubmission] = useState<{
    task: TaskResponse;
    text: string;
    key: string;
  }>();
  const [path, setPath] = useState("");
  const [repositoryDialogOpen, setRepositoryDialogOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    let active = true;
    void Promise.all([client.listRepositories(), client.listTasks()]).then(
      ([knownRepositories, knownTasks]) => {
        if (!active) return;
        setRepositories((current) => [
          ...current,
          ...knownRepositories.filter(
            (item) => !current.some((existing) => existing.id === item.id),
          ),
        ]);
        setTasks((current) => [
          ...current,
          ...knownTasks.filter(
            (item) => !current.some((existing) => existing.id === item.id),
          ),
        ]);
        setRepositoryId((current) => current ?? knownRepositories[0]?.id);
      },
      (reason: unknown) => {
        if (active)
          setError(
            reason instanceof Error
              ? reason.message
              : "Could not load workspace",
          );
      },
    );
    return () => {
      active = false;
    };
  }, [client]);

  const repository = repositories.find((item) => item.id === repositoryId);

  async function addRepository(event: FormEvent) {
    event.preventDefault();
    if (!path.trim() || busy) return;
    setBusy(true);
    setError("");
    try {
      const added = await client.registerRepository(path.trim());
      setRepositories((current) => [
        added,
        ...current.filter((item) => item.id !== added.id),
      ]);
      setRepositoryId(added.id);
      setPath("");
      setRepositoryDialogOpen(false);
    } catch (reason) {
      setError(
        reason instanceof Error ? reason.message : "Could not add repository",
      );
    } finally {
      setBusy(false);
    }
  }

  async function startTask(event: FormEvent) {
    event.preventDefault();
    if (!repository || !draft.trim() || busy) return;
    setBusy(true);
    setError("");
    try {
      const retry =
        pendingSubmission?.task.repositoryId === repository.id &&
        pendingSubmission.text === draft.trim()
          ? pendingSubmission
          : undefined;
      const created =
        retry?.task ??
        (await client.createTask(repository.id, "HEAD", crypto.randomUUID()));
      if (!retry) setTasks((current) => [created, ...current]);
      const submission = retry ?? {
        task: created,
        text: draft.trim(),
        key: crypto.randomUUID(),
      };
      setPendingSubmission(submission);
      await client.sendMessage(created.id, submission.text, submission.key);
      setTaskId(created.id);
      setPendingSubmission(undefined);
      setDraft("");
    } catch (reason) {
      setError(
        reason instanceof Error ? reason.message : "Could not start task",
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <SidebarProvider>
      <Sidebar
        className="border-r border-sidebar-border/70"
        collapsible="offcanvas"
      >
        <SidebarHeader className="gap-4 p-4">
          <div className="flex items-center gap-2.5 px-2 pt-1">
            <span className="text-base font-semibold tracking-tight">
              Crucible
            </span>
          </div>
          <Button
            variant="outline"
            className="h-10 w-full justify-start gap-2 rounded-xl bg-background shadow-xs"
            onClick={() => {
              setTaskId(undefined);
              setError("");
            }}
          >
            <Plus className="size-4" /> New chat
          </Button>
        </SidebarHeader>
        <SidebarContent className="px-2">
          <SidebarGroup>
            <SidebarGroupLabel>Chats</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu aria-label="Tasks">
                {tasks.map((task) => (
                  <SidebarMenuItem key={task.id}>
                    <SidebarMenuButton
                      isActive={task.id === taskId}
                      onClick={() => {
                        setTaskId(task.id);
                        setRepositoryId(task.repositoryId);
                        setError("");
                      }}
                      title={taskLabel(task, repositories)}
                    >
                      <span className="truncate">
                        {taskLabel(task, repositories)}
                      </span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                ))}
              </SidebarMenu>
              {tasks.length === 0 && (
                <p className="px-2 py-3 text-xs text-muted-foreground">
                  Your chats will appear here.
                </p>
              )}
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
      </Sidebar>

      <SidebarInset className="min-h-svh bg-background">
        <header className="flex h-15 items-center border-b border-border/70 px-4 sm:px-6">
          <SidebarTrigger aria-label="Toggle sidebar" />
        </header>

        {taskId ? (
          <TaskView
            key={taskId}
            taskId={taskId}
            client={client}
            repository={repository}
          />
        ) : (
          <div className="flex min-h-[calc(100svh-3.75rem)] flex-col items-center justify-center px-4 py-12">
            <div className="w-full max-w-3xl -translate-y-8">
              <div className="mb-10 text-center">
                <h1 className="text-3xl font-semibold tracking-tight sm:text-4xl">
                  What would you like to build?
                </h1>
                <p className="mt-3 text-sm text-muted-foreground">
                  Choose a repository, then ask Crucible to work on it.
                </p>
              </div>
              <form
                onSubmit={(event) => void startTask(event)}
                className="rounded-2xl border bg-card p-3 shadow-[0_12px_40px_-24px_rgba(0,0,0,.35)]"
              >
                <div className="flex min-w-0 items-center gap-2 pb-2">
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    aria-label="Add repository"
                    title="Add repository"
                    onClick={() => setRepositoryDialogOpen(true)}
                  >
                    <Plus className="size-4" />
                  </Button>
                  {repository ? (
                    <button
                      type="button"
                      onClick={() => setRepositoryDialogOpen(true)}
                      className="flex min-w-0 items-center gap-1.5 rounded-lg px-2 py-1 text-xs font-medium hover:bg-muted"
                    >
                      <FolderGit2 className="size-3.5 shrink-0" />
                      <span className="truncate">
                        {repositoryName(repository)}
                      </span>
                    </button>
                  ) : (
                    <span className="text-xs text-muted-foreground">
                      Add a repository to send
                    </span>
                  )}
                </div>
                <Textarea
                  aria-label="Message"
                  id="new-chat-message"
                  className="min-h-28 resize-none border-0 bg-transparent px-2 py-2 shadow-none focus-visible:ring-0"
                  placeholder="Ask a question or describe a coding task…"
                  value={draft}
                  onChange={(event) => setDraft(event.target.value)}
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
                    size="icon"
                    aria-label="Send"
                    disabled={!repository || !draft.trim() || busy}
                  >
                    <ArrowUp className="size-4" />
                  </Button>
                </div>
              </form>
              {error && (
                <p role="alert" className="mt-3 text-sm text-destructive">
                  {error}
                </p>
              )}
            </div>
          </div>
        )}
      </SidebarInset>

      <Dialog
        open={repositoryDialogOpen}
        onOpenChange={setRepositoryDialogOpen}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Choose a repository</DialogTitle>
            <DialogDescription>
              Attach a local Git repository to this chat.
            </DialogDescription>
          </DialogHeader>
          {repositories.length > 0 && (
            <div className="space-y-1" aria-label="Repositories">
              {repositories.map((item) => (
                <Button
                  key={item.id}
                  type="button"
                  variant={item.id === repositoryId ? "secondary" : "ghost"}
                  className="h-auto w-full justify-start gap-3 px-3 py-2 text-left"
                  onClick={() => {
                    setRepositoryId(item.id);
                    setRepositoryDialogOpen(false);
                  }}
                >
                  <FolderGit2 className="size-4 shrink-0" />
                  <span className="min-w-0">
                    <span className="block truncate font-medium">
                      {repositoryName(item)}
                    </span>
                    <span className="block truncate text-xs text-muted-foreground">
                      {item.rootPath}
                    </span>
                  </span>
                </Button>
              ))}
            </div>
          )}
          <form
            onSubmit={(event) => void addRepository(event)}
            className="space-y-3 border-t pt-4"
          >
            <label htmlFor="repository-path" className="text-sm font-medium">
              Repository path
            </label>
            <div className="flex gap-2">
              <Input
                id="repository-path"
                placeholder="/path/to/repository"
                value={path}
                onChange={(event) => setPath(event.target.value)}
              />
              <Button type="submit" disabled={!path.trim() || busy}>
                {busy ? "Adding…" : "Add"}
              </Button>
            </div>
          </form>
          {error && (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          )}
        </DialogContent>
      </Dialog>
    </SidebarProvider>
  );
}
