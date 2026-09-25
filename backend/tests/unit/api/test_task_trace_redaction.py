from crucible.api.tasks import _public_tool_arguments


def test_command_trace_exposes_environment_names_but_not_values() -> None:
    public = _public_tool_arguments(
        "execute_command",
        {
            "executable": "tool",
            "environment": {"API_TOKEN": "super-secret", "CI": "1"},
        },
    )

    assert public == {
        "executable": "tool",
        "environmentNames": ["API_TOKEN", "CI"],
    }
    assert "super-secret" not in repr(public)
