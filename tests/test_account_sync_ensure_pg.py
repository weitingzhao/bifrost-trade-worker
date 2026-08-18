"""Account Sync reconnects PG when the connection is already closed."""

from __future__ import annotations

from types import SimpleNamespace

from bifrost_worker.daemon.account_sync.app import AccountSyncDaemon


class _ClosedConn:
    closed = 1

    def rollback(self) -> None:
        raise AssertionError("rollback should not run on a closed connection")

    def close(self) -> None:
        return None


class _OpenConn:
    closed = 0
    rolled_back = False

    def rollback(self) -> None:
        self.rolled_back = True


def test_ensure_pg_reconnects_when_connection_closed():
    app = AccountSyncDaemon({})
    app.pg_conn = _ClosedConn()
    fresh = object()
    app._connect_pg = lambda: fresh  # type: ignore[method-assign]

    assert app._ensure_pg() is True
    assert app.pg_conn is fresh


def test_ensure_pg_reuses_open_connection():
    app = AccountSyncDaemon({})
    open_conn = _OpenConn()
    app.pg_conn = open_conn
    app._connect_pg = lambda: (_ for _ in ()).throw(AssertionError("should not reconnect"))  # type: ignore[method-assign]

    assert app._ensure_pg() is True
    assert app.pg_conn is open_conn
    assert open_conn.rolled_back is True


def test_ensure_golden_reconnects_when_connection_closed():
    app = AccountSyncDaemon({})
    app.golden_conn = _ClosedConn()
    fresh = SimpleNamespace()
    app._connect_golden = lambda: fresh  # type: ignore[method-assign]

    assert app._ensure_golden() is True
    assert app.golden_conn is fresh
