import { Check, ChevronDown, CircleAlert, Clock3, Terminal } from "lucide-react";

import type { StepTraceResponse } from "../../api/client";
import { Badge } from "../../components/ui/badge";
import { UnifiedDiff } from "./UnifiedDiff";

type Call = StepTraceResponse["calls"][number];
type Result = StepTraceResponse["results"][number];

function stringArg(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function title(call: Call): string {
  const args = call.arguments;
  switch (call.name) {
    case "execute_command":
      return [stringArg(args.executable), ...(Array.isArray(args.arguments) ? args.arguments.filter((item): item is string => typeof item === "string") : [])].filter(Boolean).join(" ") || "Command";
    case "read_file": return `Read ${stringArg(args.path) ?? "file"}`;
    case "write_file": return `Wrote ${stringArg(args.path) ?? "file"}`;
    case "apply_patch": return "Applied patch";
    case "search_files": return `Searched ${stringArg(args.query) ?? "files"}`;
    case "list_files": return "Listed files";
    case "workspace_diff": return "Reviewed changes";
    case "workspace_status": return "Checked workspace";
    default: return call.name.replaceAll("_", " ");
  }
}

export function ToolCallCard({ call, result }: { call: Call; result?: Result }) {
  const status = result?.status ?? call.status;
  const failed = ["failed", "denied", "interrupted", "rejected"].includes(status);
  const diff = call.name === "workspace_diff" ? result?.displayText : undefined;
  return (
    <section aria-label={`Tool call: ${call.name}`} className="overflow-hidden rounded-xl border bg-card text-sm">
      <div className="flex items-start gap-3 px-4 py-3">
        <span className="mt-0.5 text-muted-foreground">
          {call.name === "execute_command" ? <Terminal className="size-4" /> : failed ? <CircleAlert className="size-4 text-destructive" /> : result ? <Check className="size-4" /> : <Clock3 className="size-4" />}
        </span>
        <div className="min-w-0 flex-1">
          <p className="truncate font-medium" title={title(call)}>{title(call)}</p>
          <p className="text-xs text-muted-foreground">{call.name.replaceAll("_", " ")}</p>
        </div>
        <Badge variant={failed ? "destructive" : result ? "secondary" : "outline"}>{status}</Badge>
      </div>
      {failed && result?.displayText && <p role="alert" className="border-t border-destructive/20 bg-destructive/5 px-4 py-2 text-xs text-destructive">{result.displayText}</p>}
      {diff && <div className="border-t p-3"><UnifiedDiff diff={diff} /></div>}
      {(result?.displayText && !diff || failed || call.name === "execute_command") && (
        <details className="border-t px-4 py-2">
          <summary className="flex cursor-pointer items-center gap-1 text-xs text-muted-foreground [&::-webkit-details-marker]:hidden">
            <ChevronDown className="size-3" /> Details
          </summary>
          {call.name === "execute_command" && <pre className="mt-2 overflow-x-auto rounded-md bg-muted p-3 text-xs">{JSON.stringify(call.arguments, null, 2)}</pre>}
          {result?.displayText && !failed && <pre className="mt-2 max-h-64 overflow-auto whitespace-pre-wrap break-words rounded-md bg-muted p-3 text-xs">{result.displayText}</pre>}
          {result?.artifactId && <a className="mt-2 inline-block text-xs underline" href={`/api/artifacts/${result.artifactId}`}>Full output</a>}
        </details>
      )}
    </section>
  );
}
