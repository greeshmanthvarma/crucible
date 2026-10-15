from crucible.repair.reporting import public_report


def test_public_report_uses_only_bounded_aggregate_data():
    body = public_report(
        accepted=1,
        attempted=3,
        roles={"proposal": 3},
        elapsed_seconds=12,
        mechanisms=["tool behavior", "PRIVATE_CASE_SENTINEL"],
        usage={
            "reported_input": 11,
            "reported_output": 12,
            "estimated_input": 31,
            "estimated_output": 32,
        },
        observations=7,
    )
    assert "Accepted fixes: 1; diagnosed fixes: 3" in body
    assert "proposal: 3" in body and "wall time: 12 seconds" in body
    assert "reported input/output tokens: 11/12" in body
    assert "estimated input/output tokens: 31/32" in body
    assert "Accepted validation observations: 7" in body
    assert "PRIVATE_CASE_SENTINEL" not in body
    assert "manual" in body.lower() and "deterministic" in body.lower()
