# L4 compat layer cleanup (incremental)

## Gate (keep)

```python
# bifrost_worker.data.massive.celery_queues
MASSIVE_QUEUES_DISABLED = True  # do not flip
```

Celery: keep `stocks_ib`; Massive queues remain disabled.

## Current surface (2026-07-30)

| Artifact | Size / count |
|----------|----------------|
| `vendor/reader.py` | ~3351 lines |
| `job_massive_backfill` string hits (worker+api) | ~150+ across ~10 files |
| API importers of `vendor.reader` | massive routes, research (discovery/sepa/max_pain/data_readiness), ops job_queues |

Must-keep **market.\*** readers still live in `vendor/reader.py` (bridged SQL):

- `get_option_snapshots_latest` / eod helpers
- `get_option_open_interest_daily`
- `get_option_bars`
- `get_corporate_actions`
- `get_stock_day_series_for_sepa` / CRS helpers
- expiration / contract helpers
- SEPA fundamentals cache helpers

Dead / gated behind `MASSIVE_QUEUES_DISABLED`:

- All `*_job_massive_backfill*` mutators / claim / trim / dispatch

## Recommended incremental PRs

1. **Stub-strip**: keep function signatures used by API; delete SQL bodies behind early `if MASSIVE_QUEUES_DISABLED` (already return) — shrink file without breaking imports.
2. **Move readers**: extract market.* SELECT helpers → `bifrost_core.persistence.postgres.market_readers` (or plugin-published package); thin re-export from `vendor.reader`.
3. **API import flip**: update `bifrost_api.*` to import from core; leave worker re-exports for one release.
4. **Delete package last**: only after API+worker import count for `job_massive_*` is zero and Ops UI no longer lists Massive job queues as active.

## Verify

```bash
cd bifrost-trade-worker && pytest tests/test_massive_queues_disabled.py -q
cd bifrost-trade-api && pytest tests/test_massive_app.py -q
# broader:
cd bifrost-trade-worker && pytest -m 'not ib and not db' -q
cd bifrost-trade-api && pytest -q
```

## This pass

- Confirmed `MASSIVE_QUEUES_DISABLED is True`
- Full package deletion deferred (too large / high API coupling)
- No Massive Celery queues re-enabled
