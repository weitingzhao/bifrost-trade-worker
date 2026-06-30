"""W8 trade-k8s-native: K8s Lease leader election for trading daemon (R-DV3)."""

from __future__ import annotations

import asyncio
from typing import List, Optional

import pytest

from bifrost_worker.daemon.lease import (
    DEFAULT_LEASE_DURATION_SEC,
    InMemoryLeaseBackend,
    LeaderElector,
    LeaseAttempt,
    LeaseRecord,
    get_daemon_lease_settings,
    run_with_leadership,
)


class _Clock:
    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def _elector(
    backend: InMemoryLeaseBackend,
    identity: str,
    clock: _Clock,
    started: Optional[List[str]] = None,
    stopped: Optional[List[str]] = None,
) -> LeaderElector:
    return LeaderElector(
        backend=backend,
        identity=identity,
        lease_duration_sec=15.0,
        renew_deadline_sec=10.0,
        retry_period_sec=2.0,
        clock=clock,
        on_started_leading=(lambda: started.append(identity)) if started is not None else None,
        on_stopped_leading=(lambda: stopped.append(identity)) if stopped is not None else None,
    )


def test_daemon_lease_settings_disabled_by_default() -> None:
    s = get_daemon_lease_settings({})
    assert s.enabled is False
    assert s.name == "bifrost-daemon"
    assert s.lease_duration_sec == DEFAULT_LEASE_DURATION_SEC


def test_daemon_lease_settings_yaml_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BIFROST_DAEMON_LEASE_ENABLED", "1")
    monkeypatch.setenv("POD_NAMESPACE", "bifrost-stg")
    monkeypatch.setenv("POD_NAME", "daemon-abc123")
    monkeypatch.setenv("BIFROST_DAEMON_LEASE_NAME", "env-daemon-lease")
    cfg = {"daemon": {"lease": {"enabled": False, "lease_duration_sec": 20}}}
    s = get_daemon_lease_settings(cfg)
    assert s.enabled is True
    assert s.namespace == "bifrost-stg"
    assert s.identity == "daemon-abc123"
    assert s.name == "env-daemon-lease"
    assert s.lease_duration_sec == 20.0


@pytest.mark.asyncio
async def test_standby_never_starts_fsm() -> None:
    be = InMemoryLeaseBackend()
    be.create(
        LeaseRecord(
            holder_identity="other",
            lease_duration_sec=300.0,
            acquire_time=0.0,
            renew_time=1e12,
        )
    )
    el = LeaderElector(
        backend=be,
        identity="standby-pod",
        lease_duration_sec=10.0,
        renew_deadline_sec=5.0,
        retry_period_sec=0.01,
        clock=lambda: 1e12,
    )
    started: List[str] = []
    stop = asyncio.Event()

    async def fsm_loop() -> None:
        started.append("fsm")

    task = asyncio.create_task(run_with_leadership(el, fsm_loop, stop))
    await asyncio.sleep(0.1)
    assert el.is_leader is False
    assert started == []
    stop.set()
    await asyncio.wait_for(task, timeout=2.0)
    assert started == []


def test_two_daemon_pods_only_one_leader() -> None:
    """W8 verify bar: 2 daemon pods — only Lease holder runs FSM."""
    be = InMemoryLeaseBackend()
    clock = _Clock(0.0)
    a = _elector(be, "daemon-pod-a", clock)
    b = _elector(be, "daemon-pod-b", clock)
    for t in range(0, 60):
        clock.t = float(t)
        a.try_acquire_or_renew(float(t))
        b.try_acquire_or_renew(float(t))
        assert not (a.is_leader and b.is_leader), f"two leaders at t={t}"
        assert a.is_leader or b.is_leader, f"no leader at t={t}"
    assert a.is_leader and not b.is_leader


def test_takeover_after_expiry() -> None:
    be = InMemoryLeaseBackend()
    clock = _Clock(0.0)
    started_b: List[str] = []
    a = _elector(be, "daemon-pod-a", clock)
    b = _elector(be, "daemon-pod-b", clock, started=started_b)
    a.try_acquire_or_renew(0.0)
    assert b.try_acquire_or_renew(20.0) is LeaseAttempt.ACQUIRED
    assert b.is_leader is True
    assert started_b == ["daemon-pod-b"]
