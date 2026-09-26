type DiffFile = { name: string; lines: string[] };

function splitDiff(diff: string): DiffFile[] {
  const files: DiffFile[] = [];
  let current: DiffFile | undefined;
  for (const line of diff.replace(/\n$/, "").split("\n")) {
    if (line.startsWith("diff --git ")) {
      current = { name: line.slice(11).split(" b/").at(-1) ?? line, lines: [] };
      files.push(current);
    } else {
      if (!current) {
        current = { name: "Changes", lines: [] };
        files.push(current);
      }
      current.lines.push(line);
    }
  }
  return files;
}

export function UnifiedDiff({ diff }: { diff: string }) {
  if (!diff.trim()) {
    return <p className="text-sm text-muted-foreground">No diff</p>;
  }
  return (
    <div className="space-y-3" aria-label="Code diff">
      {splitDiff(diff).map((file, index) => (
        <section key={`${file.name}-${index}`} className="overflow-hidden rounded-lg border">
          <h4 className="border-b bg-muted/60 px-3 py-2 font-mono text-xs font-medium">
            {file.name}
          </h4>
          <div className="overflow-x-auto py-1 font-mono text-xs leading-5">
            {file.lines.map((line, lineIndex) => {
              const kind = line.startsWith("+") && !line.startsWith("+++")
                ? "added"
                : line.startsWith("-") && !line.startsWith("---")
                  ? "removed"
                  : line.startsWith("@@")
                    ? "hunk"
                    : "context";
              return (
                <div
                  key={lineIndex}
                  className={
                    kind === "added"
                      ? "bg-emerald-500/10 text-emerald-800 dark:text-emerald-300"
                      : kind === "removed"
                        ? "bg-rose-500/10 text-rose-800 dark:text-rose-300"
                        : kind === "hunk"
                          ? "bg-sky-500/10 text-sky-800 dark:text-sky-300"
                          : "text-muted-foreground"
                  }
                >
                  <pre className="w-max min-w-full px-3">{line || " "}</pre>
                </div>
              );
            })}
          </div>
        </section>
      ))}
    </div>
  );
}
