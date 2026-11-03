# Test suite maintenance

The backend suite tests harness plumbing and safety. It is separate from the
development, validation, and final benchmark trials used to measure harness
improvements. Passing these tests is not evidence of improved model behavior.

## Keeping useful coverage

- Give each behavior one primary test at its module's public interface. Add a
  higher-level test when wiring, persistence, authority, or recovery can fail
  independently of that interface.
- Do not import a `test_*` function to collect it again or call it from another
  test. Run the feature-focused tests together instead. Shared setup belongs in
  fixtures or helpers that are not themselves collected as tests.
- Consolidate identical-input tests whose assertions describe one returned
  contract. Preserve distinct malformed inputs, parallel ordering, interrupted
  execution, exact size limits, and other independently meaningful scenarios.
- Parameterize scenarios that share setup, but retain separate cases when they
  exercise different behavior. Inline and standalone frontend approvals, for
  example, have different rendering paths even though their decision controls
  are the same.
- A smaller test count or unchanged line/branch coverage does not by itself
  justify removing a test. Map its assertions to the retained test and inspect
  the implementation path; coverage overlap is supporting evidence only.
- Keep direct tests of security and durable invariants even when broad workflow
  tests also encounter them. Different layers can independently violate path
  containment, credential scoping, transaction ordering, or terminal outcomes.
- Scripted gateways and Docker ports test orchestration, not live model quality
  or real sandbox isolation. Keep opted-in external proofs clearly marked.

## Local checks

For an edit confined to one module, run its focused unit/contract tests and the
integration tests for the affected workflow. Run the full offline suite before
claiming a complete suite result. Do not change CI selection or add skips just
to shorten a cleanup run.

From the repository root:

```sh
cd backend
CRUCIBLE_MODEL=fake LITELLM_LOCAL_MODEL_COST_MAP=true uv run pytest -q \
  -m 'not docker and not real_model and not benchmark_external' --durations=30
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy src/crucible
```

Model credentials, image builds/pulls, real Docker execution, and external
benchmark trials are not implied by an offline test run. The marked tests still
exist and must be authorized separately when those proofs are needed.
