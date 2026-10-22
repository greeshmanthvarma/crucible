import json

import pytest

from crucible.improvement.worker_supervisor import remove_owned_effect


@pytest.mark.parametrize("kind", ["network", "worker"])
@pytest.mark.parametrize(
    "fault", [None, "absent", "list", "ownership", "remove", "retained"]
)
async def test_cleanup_requires_verified_absence(kind, fault):
    calls = []
    present = fault != "absent"

    class Docker:
        async def invoke(self, *args):
            nonlocal present
            calls.append(args)
            if args[:2] == ("network", "ls") or args[0] == "ps":
                if fault == "list":
                    raise OSError("daemon unavailable")
                return "resource-id" if present else ""
            if "inspect" in args:
                labels = {"owner": "other" if fault == "ownership" else "job"}
                return json.dumps(
                    [
                        {
                            "Labels": labels,
                            "Config": {"Labels": labels},
                            "State": {"Running": False},
                        }
                    ]
                )
            if "rm" in args:
                if fault == "remove":
                    raise ValueError("removal failed")
                if fault != "retained":
                    present = False
                return ""
            raise AssertionError(args)

    if fault in (None, "absent"):
        await remove_owned_effect(Docker(), kind, "owned-name", {"owner": "job"})
        assert not present
        # Repeated cleanup is harmless.
        await remove_owned_effect(Docker(), kind, "owned-name", {"owner": "job"})
    else:
        with pytest.raises((ValueError, OSError)):
            await remove_owned_effect(Docker(), kind, "owned-name", {"owner": "job"})
    if fault in ("absent", "ownership", "list"):
        assert not any("rm" in call for call in calls)


@pytest.mark.parametrize("caller", ["shutdown", "recovery"])
@pytest.mark.parametrize("present", [True, False])
async def test_job_cleanup_removes_network_or_confirms_failed_create(caller, present):
    from uuid import uuid4

    from crucible.improvement.lifecycle import ExperimentService
    from crucible.improvement.worker_supervisor import WorkerSupervisor, labels
    from tests.contract.test_improvement_worker import make_job

    job = make_job()
    effect = uuid4()
    states = []
    network_present = present

    class Store:
        async def effects(self, experiment):
            return [
                {
                    "id": str(effect),
                    "kind": "network",
                    "status": "uncertain",
                    "intent_json": json.dumps({"name": "owned-network"}),
                    "ownership_labels_json": json.dumps(labels(job)),
                }
            ]

        async def effect_state(self, identity, status):
            states.append(status)

    class Broker:
        async def stop(self, identity):
            pass

    class Docker:
        async def invoke(self, *args):
            nonlocal network_present
            if args[:2] == ("network", "ls"):
                return "network-id" if network_present else ""
            if args[:2] == ("network", "inspect"):
                return json.dumps([{"Labels": labels(job)}])
            assert args == ("network", "rm", "network-id")
            network_present = False
            return ""

    if caller == "shutdown":
        supervisor = object.__new__(WorkerSupervisor)
        supervisor.store, supervisor.broker, supervisor.docker = (
            Store(),
            Broker(),
            Docker(),
        )
        supervisor.owned = {job.job_id: [(effect, "network", "owned-network", job)]}
        await supervisor.stop(job.job_id)
        await supervisor.stop(job.job_id)
    else:
        service = object.__new__(ExperimentService)
        service.store, service.docker = Store(), Docker()
        await service._reconcile(job.experiment_id)
    assert not network_present
    assert all(status == "stopped" for status in states)
