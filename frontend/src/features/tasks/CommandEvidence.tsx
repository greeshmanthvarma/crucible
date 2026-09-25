import type { StepTraceResponse } from "../../api/client";

export function CommandEvidence({ steps }: { steps: StepTraceResponse[] }) {
  const evidence = steps.flatMap((step) =>
    step.calls
      .filter((call) => call.name === "execute_command")
      .map((call) => ({
        call,
        result: step.results.find((result) => result.toolCallId === call.id),
      })),
  );
  return (
    <section aria-label="Command evidence">
      <h3>Command evidence</h3>
      {evidence.map(({ call, result }) => (
        <article key={call.id}>
          <p>Status: {result?.status ?? call.status}</p>
          {result?.artifactId && <p>Artifact: {result.artifactId}</p>}
          {result?.result.truncated === true && <p>Output truncated</p>}
          {result?.displayText && <pre>{result.displayText}</pre>}
        </article>
      ))}
    </section>
  );
}
