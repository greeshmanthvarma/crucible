"""Shared operator guidance; execution still revalidates authority at admission."""

from collections.abc import Mapping


def recovery_actions(
    status: str,
    *,
    cleanup_confirmed: bool,
    unsettled_usage: bool,
    remaining: Mapping[str, int],
    retry_required: bool,
    work_complete: bool = False,
) -> list[str]:
    if status == "closed":
        return []
    if not cleanup_confirmed:
        return ["investigate", "recover"]
    if unsettled_usage:
        return ["acknowledge_accounting", "recover"]
    if status in ("error", "interrupted", "stopping"):
        return ["recover"]
    if status != "ready" or work_complete:
        return []
    if any(
        remaining[name] == 0
        for name in (
            "tokens",
            "requests",
            "active_seconds",
            "worker_wall_seconds",
            "trial_starts",
        )
    ):
        return ["remain_blocked"]
    return ["authorize_retry"] if retry_required else ["continue"]
