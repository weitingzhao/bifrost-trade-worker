# P9 Sign-off Checklist — Cleanup + permission lockdown
#
# Agent does **not** auto sign-off. Owner confirms items below, then signs off
# via Ops Console / Platform API (`market-data-subcontractor` · P9).
#
# Prerequisite: P8 signed off (2026-07-30).

## 1. Automated gates (Agent)

```bash
cd bifrost-trade-core && make lint && make test
cd ../bifrost-trade-worker && make lint && make test
cd ../bifrost-trade-api && make test
cd ../bifrost-trade-frontend && npm run build
cd ../bifrost-platform-plugin-market-data && make test && make lint
```

Expected: all green (or only pre-existing lint debt unrelated to P9).

## 2. Legacy tables dropped

```bash
# Against target DB (DEV first):
psql -f bifrost-platform-plugin-market-data/scripts/p9_drop_legacy_tables.sql

psql -c "
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
    'stock_day','stock_min','option_day','option_min','option_contracts',
    'option_snapshots','option_expiration_cache','option_open_interest_daily',
    'tickers','ticker_overview','massive_corporate_action','job_massive_backfill'
  );
"
```

- [ ] Query returns **0 rows**
- [ ] Retained Trade tables still present: `watchlist`, `ticker_related_tickers` (symbol-keyed),
      `ticker_types`, `option_trades`, `job_bars_backfill`,
      `stock_readiness_daily`, `report_option_*`
- [x] `reference_us_holidays` dropped (migrated to `market.us_market_holiday`)

## 3. Role lockdown

Re-apply roles after DROP:

```bash
psql -f bifrost-platform-plugin-market-data/scripts/create_roles.sql
```

- [ ] `data_writer` can `SELECT` / write `market.*` and `data_ops.*`
- [ ] `data_writer` **cannot** `INSERT` into `public.watchlist` (SELECT only)
- [ ] `market_reader` is SELECT-only on `market.*`

## 4. Dead Celery / Massive code

- [ ] Massive ingest/gap/pool_fill modules removed or stubbed retired
- [ ] `MASSIVE_QUEUES_DISABLED = True`; enqueue returns plugin message
- [ ] Celery app still runs `stocks_ib` (bars backfill)
- [ ] `tradeCeleryK8sIdealCatalog.ts` marks Massive queues **superseded by market-data-subcontractor**

## 5. IB bars write path

- [ ] `write_ohlc_bars_to_db` / sink write `market.stock_daily` / `market.stock_minute` (no `public.stock_day`)
- [ ] Core DDL no longer creates legacy market tables (`stock_day`, `option_snapshots`, …)

## 6. Owner visual regression (required)

- [ ] Trade FE — Option Discovery: IV / Greeks still load for watchlist symbols
- [ ] Trade FE — Screener: not empty solely due to schema drop
- [ ] Stock Data Readiness: inventory reads `market.*` (no silent zeros from missing public tables)
- [ ] Celery Ops: Massive enqueue still refused with plugin message

## 7. After Owner confirms

1. Sign off P9 via Platform program API / Console (`sign_off.required: true`)
2. Program `market-data-subcontractor` complete
3. Optionally mark spine milestone `market-data-subcontractor` DONE

## Known deferred (not blocking P9)

- M1 full ratios / short plugin ingest
- Thin Massive API compatibility shims (reader re-exports) — remove when API imports fully point at core
- PROD/STG DB: run `p9_drop_legacy_tables.sql` + `create_roles.sql` after DEV Owner sign-off (DEV already applied 2026-07-30)
- After DROP on any env: if `market.ticker` empty, enqueue `ticker_sync` universe job
