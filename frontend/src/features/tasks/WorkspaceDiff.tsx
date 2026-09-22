import type { WorkspaceStateResponse } from "../../api/client";

export function WorkspaceDiff({
  workspace,
}: {
  workspace?: WorkspaceStateResponse;
}) {
  if (!workspace) return null;
  return (
    <section aria-label="Workspace diff">
      <h3>Workspace</h3>
      <pre>{workspace.status || "Clean"}</pre>
      <pre>{workspace.diff || "No diff"}</pre>
      {(workspace.statusTruncated || workspace.diffTruncated) && (
        <p>Output truncated</p>
      )}
    </section>
  );
}
