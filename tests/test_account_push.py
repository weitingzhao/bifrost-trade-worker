"""account_push: raw_broker accounts are written on change, from a fresh plugin snapshot only."""

import time
from unittest.mock import MagicMock

import pytest

from bifrost_worker.daemon.app import account_push


def _acc(aid, nl="100", qty=1.0):
    return {
        "account_id": aid,
        "summary": {"NetLiquidation": nl},
        "positions": [{"symbol": "XYZ", "secType": "STK", "position": qty}],
    }


@pytest.fixture
def writes(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "bifrost_core.persistence.postgres.accounts_sync.sync_accounts_snapshot_to_tables",
        lambda conn, accounts: calls.append([a["account_id"] for a in accounts]),
    )
    monkeypatch.setattr("bifrost_core.core.daemon_flags.daemon_broker_writes_off", lambda: False)
    return calls


def _writer():
    w = account_push.AccountTablesWriter({})
    conn = MagicMock()
    conn.closed = 0
    w._conn = conn
    return w


def test_only_changed_accounts_are_written(writes):
    w = _writer()
    assert w.write([_acc("U1"), _acc("U2")]) == 2
    assert w.write([_acc("U1"), _acc("U2")]) == 0
    assert w.write([_acc("U1", nl="101"), _acc("U2")]) == 1
    assert w.write([_acc("U1", nl="101"), _acc("U2", qty=2.0)]) == 1
    assert writes == [["U1", "U2"], ["U1"], ["U2"]]


def test_force_writes_unchanged_accounts(writes):
    w = _writer()
    w.write([_acc("U1")])
    assert w.write([_acc("U1")], force=True) == 1
    assert writes == [["U1"], ["U1"]]


def test_broker_writes_off_writes_nothing(writes, monkeypatch):
    monkeypatch.setattr("bifrost_core.core.daemon_flags.daemon_broker_writes_off", lambda: True)
    assert _writer().write([_acc("U1")], force=True) == 0
    assert writes == []


def test_failed_write_is_retried_on_the_next_push(monkeypatch):
    monkeypatch.setattr("bifrost_core.core.daemon_flags.daemon_broker_writes_off", lambda: False)
    attempts = []

    def flaky(conn, accounts):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("lock timeout")

    monkeypatch.setattr(
        "bifrost_core.persistence.postgres.accounts_sync.sync_accounts_snapshot_to_tables", flaky
    )
    w = _writer()
    assert w.write([_acc("U1")]) == 0
    assert w.write([_acc("U1")]) == 1


def test_stale_or_missing_snapshot_is_not_written():
    now = time.time()
    assert account_push.fresh_accounts(None, now) is None
    old = {"updated_at": now - account_push.SNAPSHOT_MAX_AGE_SEC - 1, "accounts_snapshot": [_acc("U1")]}
    assert account_push.fresh_accounts(old, now) is None
    new = {"updated_at": now - 3, "accounts_snapshot": [_acc("U1"), "junk"]}
    assert [a["account_id"] for a in account_push.fresh_accounts(new, now)] == ["U1"]


def test_heartbeat_snapshots_no_longer_carry_accounts():
    from bifrost_worker.daemon.app import snapshot

    app = MagicMock()
    app.store.get_account_summary.return_value = None
    app.store.get_accounts_data.return_value = [_acc("U1")]
    assert "accounts_snapshot" not in snapshot.build_heartbeat_minimal_dict(app)
