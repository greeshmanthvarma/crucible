import type { StepTraceResponse } from "../../api/client";

export function ToolTrace({ steps }: { steps: StepTraceResponse[] }) {
  return (
    <section aria-label="Tool trace">
      <h3>Tool trace</h3>
      {steps.map((step) => (
        <article key={step.id}>
          <h4>Step {step.stepSequence}</h4>
          {step.manifest && (
            <small>
              {step.manifest.model} · context {step.manifest.estimatedTokens} ·
              instructions{" "}
              {Object.values(step.manifest.instructionDigests).join(", ")}
            </small>
          )}
          <ol>
            {step.calls.map((call) => {
              const result = step.results.find(
                (candidate) => candidate.toolCallId === call.id,
              );
              return (
                <li key={call.id}>
                  <strong>{call.name}</strong> {result?.status ?? call.status}
                  <pre>{JSON.stringify(call.arguments, null, 2)}</pre>
                  {result && <pre>{result.displayText}</pre>}
                </li>
              );
            })}
          </ol>
        </article>
      ))}
    </section>
  );
}
