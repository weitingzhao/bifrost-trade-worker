# P8 Sign-off Checklist — Market Data Subcontractor consumer switchover
#
# Agent does **not** auto sign-off. Owner confirms items below, then signs off
# via Ops Console / Platform API.
#
# Critical C1 (IB↔Polygon snapshot key) and C2 (Massive enqueue gate) must already
# be merged before using this checklist.

## 1. Automated gates (Agent)

```bash
cd bifrost-trade-api && make test
cd ../bifrost-trade-frontend && npm run build
cd ../bifrost-trade-worker && pytest -m 'not ib and not db' -q
cd ../bifrost-platform-plugin-market-data && make test && bash -n scripts/verify-market-data.sh
```

Expected: all green. `EXPECTED_CRONS` includes `market-data-option-bars` and `market-data-minute-bars`.

## 2. Celery Massive stopped

See [P8_MASSIVE_QUEUE_DRAIN.md](./P8_MASSIVE_QUEUE_DRAIN.md).

- [ ] `MASSIVE_QUEUES_DISABLED = True` in `celery_queues.py`
- [ ] No workers on `stocks_massive` / `options_massive` (inspect reserved/active empty or no workers)
- [ ] Ops Massive retry / Research Massive enqueue returns:
      `Massive queues disabled; use market-data plugin`
- [ ] No new `job_massive_backfill` pending rows after a refused enqueue

## 3. API data path (optional Agent / Dev DB)

With postgres pointing at a DB that has `market.option_snapshot` + `market.option_contract`:

- [ ] Call Option Discovery / Screener path that uses `get_option_snapshots_latest`
- [ ] Response rows use IB `contract_key` (`SYM|OPT|…`) and non-null IV/Greeks when data exists

## 4. Owner visual (required for program sign_off)

- [ ] Trade FE — Option Discovery: underlyings with plugin snapshots show IV / Greeks
- [ ] Trade FE — Screener (option-related): not empty solely due to key mismatch
- [ ] Data readiness inventory reads from `market.stock_financials` (no silent public-table zeros)
- [ ] ratios / short_* gaps show `ingest_status: pending_plugin` (known gap; not a sign-off blocker)

## 5. After Owner confirms

1. Sign off P8 via Platform program API / Console (`sign_off.required: true`)
2. Do **not** start P9 until Owner says continue

## Known deferred (not blocking P8)

- M1 full ratios / short plugin ingest
- M7 legacy `option_open_interest_daily` / expiration cache cleanup → P9
- IB writes still target `public.*` intentionally → P9
