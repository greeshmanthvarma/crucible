import type { ValidationAttemptReview } from "../../api/client";

export function ValidationTrace({
  attempts,
}: {
  attempts: ValidationAttemptReview[];
}) {
  return (
    <section aria-label="Validation evidence">
      <h3>Validation</h3>
      {attempts.length === 0 && <p>No Validation evidence</p>}
      {attempts.map((attempt) => (
        <article key={attempt.id}>
          <h4>
            Validation attempt {attempt.attemptNumber}: {attempt.status}
          </h4>
          <ol>
            {attempt.commands.map((command) => (
              <li key={command.id}>
                Command {command.commandSequence}: {command.status} —{" "}
                {command.summary}
                {command.exitCode !== null &&
                  ` (exit ${command.exitCode})`}{" "}
                {command.approvalId && <span>Approval required</span>}{" "}
                {command.artifactId && (
                  <a href={`/api/artifacts/${command.artifactId}`}>Evidence</a>
                )}
              </li>
            ))}
          </ol>
        </article>
      ))}
    </section>
  );
}
