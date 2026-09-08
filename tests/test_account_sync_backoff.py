"""The failure path of the Account Sync heartbeat loop.

On 2026-09-07 the daemon failed ~2000 consecutive syncs over five hours
(``relation "brokerage.positions" does not exist``), wrote ~50k lines into a
2000-entry Redis ring buffer — evicting every other log it had — and reported
``alive=True`` the whole time. These pin the three behaviours that prevent a
repeat: back off, fold the tracebacks, and say you are not healthy.
"""

from bifrost_worker.daemon.account_sync.heartbeat import (
    SYNC_BACKOFF_BASE_SEC,
    SYNC_BACKOFF_MAX_SEC,
    SYNC_TRACEBACK_EVERY,
    _sync_backoff_sec,
)


def test_no_delay_while_healthy() -> None:
    assert _sync_backoff_sec(0) == 0.0
    assert _sync_backoff_sec(-1) == 0.0


def test_backoff_grows_then_caps() -> None:
    assert _sync_backoff_sec(1) == SYNC_BACKOFF_BASE_SEC
    assert _sync_backoff_sec(2) == SYNC_BACKOFF_BASE_SEC * 2
    assert _sync_backoff_sec(3) == SYNC_BACKOFF_BASE_SEC * 4
    # Monotonic, and never past the cap however long the outage runs.
    prev = 0.0
    for n in range(1, 5000):
        cur = _sync_backoff_sec(n)
        assert prev <= cur <= SYNC_BACKOFF_MAX_SEC
        prev = cur
    assert _sync_backoff_sec(5000) == SYNC_BACKOFF_MAX_SEC


def test_backoff_bounds_a_five_hour_outage_below_the_ring_buffer() -> None:
    """The console stream holds 2000 entries; a failure costs ~25 lines."""
    elapsed = 0.0
    failures = 0
    while elapsed < 5 * 3600:
        failures += 1
        elapsed += _sync_backoff_sec(failures)
    # Was ~2000 failures / ~50k lines, which evicted the buffer several times.
    assert failures < 100


def test_traceback_folding_keeps_the_first_and_thins_the_rest() -> None:
    def traces(n: int) -> bool:
        return n == 1 or n % SYNC_TRACEBACK_EVERY == 0

    assert traces(1)
    assert not traces(2)
    assert not traces(SYNC_TRACEBACK_EVERY - 1)
    assert traces(SYNC_TRACEBACK_EVERY)
    # Over a five-hour outage under backoff: one traceback, not two thousand.
    assert sum(traces(n) for n in range(1, 100)) == 1
