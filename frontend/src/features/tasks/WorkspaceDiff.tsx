import type { WorkspaceStateResponse } from "../../api/client";
import { UnifiedDiff } from "./UnifiedDiff";

export function WorkspaceDiff({
  workspace,
}: {
  workspace?: WorkspaceStateResponse;
}) {
  if (!workspace) return null;
  return (
    <section aria-label="Workspace diff">
      <h3>Workspace</h3>
      <pre className="overflow-x-auto rounded-lg bg-muted/50 p-3 text-xs">{workspace.status || "Clean"}</pre>
      <UnifiedDiff diff={workspace.diff} />
      {(workspace.statusTruncated || workspace.diffTruncated) && (
        <p>Output truncated</p>
      )}
    </section>
  );
}
