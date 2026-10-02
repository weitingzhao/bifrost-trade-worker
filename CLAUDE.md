# CLAUDE.md — bifrost-trade-worker

> Legacy `bifrost-trader-engine` 已按 spine **D8**（2026-06-29）归档移出工作区。工作区事实基线见 `../AGENT_FACTS.md`。

与本项目用户对话一律使用中文回复（无论用户用何种语言提问）；UI 字符串与代码标识符使用 English。

## 工作区定位（2026-10-01）

| 项 | 值 |
|---|---|
| 域 / 载荷 | Trade (OLTP) · Satellite 执行载荷 · 只有 `src/bifrost_worker/daemon/`（Celery 已于 Wave 5 退役） |
| 运行位置 | K3s `bifrost-{dev,stg,prod}` 的 `daemon` Deployment（镜像 `bifrost-worker`）—— **D10：STG `replicas: 0`，PROD observe-safe**，不得为实盘扩容 |
| 发布链 | GitHub main → `bifrost-deliver-{stg,prod}`（mirror-sync → build → rollout）；Argo `bifrost-stg/prod` 手动同步 |
| 仓库可见性 | GitHub **PUBLIC**（12 个 repo 全部公开）—— `.env`、Secret YAML、dump、kubeconfig、账户内容永不入库 |
| 硬边界 | D10 交易执行冻结（BLOCKED）· D13 三域边界 · 平台/业务解耦（Flywheel A/B） |
| 事实基线 | `../AGENT_FACTS.md`（§8c 运行时与安全事实）· 规则 `../CLAUDE.md`（§8 Claude Code 运行配置） |

会话请在工作区根 `/stocks` 启动（加载治理层 hooks / auto mode / 共享记忆）；运行时与安全事实以 `../AGENT_FACTS.md` §8c 为准。

## D10：daemon 不下单

D10 BLOCKED 期间 daemon **不得下实盘单**，代码里也没有下单路径：

- 进程内没有 IB 连接（`GsTrading.connector = None`），行情与账户都从 Redis 读。
- `paper_trade` / `mock_hedging` 在 `GsTrading.__init__` 与 `_reload_config` 里写死为 `True`；
  `app/hedge_flow.py` 的对冲只记日志（`PAPER: would …`）并推进 HedgeFSM / guard 计数，不发任何订单。
- `daemon/execution/order_manager.py` 只跟踪执行状态（IDLE / ORDER_WORKING / …），不发单。
- 本 repo 没有 IB Operator RPC 下单调用；`lease.py` docstring 里的 "send orders via IB Operator RPC" 是旧描述。

把下单接上（Gateway RPC、`place_order`、实盘对冲）、为此扩容 daemon、移除
`k8s/overlays/{stg/daemon-scale-zero,prod/daemon-observe-safe}.patch.yaml`、写 `ib:operator:cmd`，都需要 Owner 明文解禁
**且** spine D10 → `UNLOCKED`（`../CLAUDE.md` §3）。**勿启动** `bifrost-trade-socket`（已归档）作 fallback。

## 进程

### 交易 daemon（GsTrading）— `scripts/run_daemon.py`

单进程 asyncio，三层 FSM：`DaemonFSM`（IDLE → CONNECTING → CONNECTED → RUNNING ⇄ RUNNING_SUSPENDED → STOPPING → STOPPED）、
`TradingFSM`、`HedgeFSM`。

| 子模块 | 职责 |
|--------|------|
| `daemon/app/` | `entry.py`（读配置、从 DB 注入 active gate set 与 structure、K8s Lease 选主）· `gs_trading.py`（主类）· `daemon_handlers.py`（各状态处理）· `control_heartbeat.py`（心跳、控制命令）· `hedge_flow.py`（模拟对冲）· `snapshot.py` · `contract_quote_live.py` · `ticker_redis.py` |
| `daemon/fsm/` | `daemon_fsm.py` · `trading_fsm.py` · `hedge_fsm.py` · `events.py` |
| `daemon/strategy/` | `gamma_scalper.py`（目标仓位 / 对冲意图）· `hedge_gate.py` |
| `daemon/guards/` | `execution_guard.py`（冷却、日内次数、仓位 / 亏损 / 价差上限、财报黑窗）· `trading_guard.py` |
| `daemon/execution/` | `order_manager.py`（执行状态，见上） |
| `daemon/core/` | `store.py` · `state/`（组合状态分类）· `metrics.py`（进程内计数）· `logging_utils.py` |
| `daemon/market/`、`daemon/pricing/` | 行情 store 包装、组合 Greeks |
| `daemon/lease.py` | K8s Lease 选主（R-DV3）：只有持有 Lease 的 Pod 跑 FSM 循环 |

数据流（读写都经 `bifrost_core`）：

- **读**：Redis 行情（IB ingestor 的 tick 键，`bifrost_core.core.realtime`）；Redis 账户快照 `ib:account:snapshot:v1`
  （`bifrost_core.portfolio.ib_edge`）。这些键的写方是 Platform IB Gateway Plugin（`redis-ib`）。启动时从 per-env
  `settings` 读 `active_gate_safety_strategy_id` / `active_strategy_structure_id` 与 host account。
- **控制**：monitor API 的 `/control/*` 写 per-env Redis 控制流，daemon 在心跳里消费（`stop`、`refresh_accounts`、
  ticker 订阅类等；`flatten` 未实现，只记 warning；`retry_ib` / `release_ib` 为空操作）。suspend / 心跳间隔也从 Redis 读。
- **写**（`bifrost_core.persistence.postgres.postgres_sink.PostgreSQLSink`）：
  - 交易状态快照、心跳 → per-env Redis HASH（`redis_daemon_state`；不在 PostgreSQL）。`write_operation` 是空操作。
  - `contract_quote_live`（来自 Redis 报价）→ Golden Source `raw_broker.contract_quote_live`。
  - 账户、持仓、未成交订单、TWS 成交 → Golden Source `raw_broker.*`，**除非** `core.daemon_flags.daemon_broker_writes_off()`
    为真（环境变量 `DAEMON_BROKER_WRITES_OFF=1`，旧名 `ACCOUNT_SYNC_DAEMON_ENABLED` 仍被认）。STG / PROD 不设，由 daemon 写；
    DEV 设（`overlays/dev/daemon-golden-writes-off.patch.yaml`），因为三环境共用同一个 Golden Source。

部署：base `replicas: 2`（Lease 热备）；DEV `replicas: 1`；STG `replicas: 0`；PROD `replicas: 2` + observe-safe。
initContainer 与 readiness 用 `scripts/wait_for_data.py`（等 CNPG + Redis 可连）。

### Account Sync daemon —— 已删除（2026-10-02，TD-22）

它从 2026-09-08 起 `replicas: 0`，且从未成功写入（`diff_engine` 用 `brokerage.*` 去写只有 `raw_broker.*` 的 Golden Source）。
包、脚本、测试、K8s Deployment、Ops 的 unit 与联动扩容、`/account-sync/control/*` 都已删除。账户与持仓一直由 daemon 的
`PostgreSQLSink` 写。

## 没有的东西

- 没有 Celery、CronJob 或 PG-as-broker 任务队列；后台数据任务在 Market Data / Flex Query Plugin。
- 不写 per-env `public.*` 的业务表，也不跑 DDL：core 0.35.0 起 `PostgreSQLSink` 连接时不再调 `_ensure_tables()` /
  `ensure_brokerage_schema()`，锁超时也不再 `pg_terminate_backend` 别的连接（TD-45）。表只由发布的 db-init Job 建；
  缺表时那次写入失败并记 error，daemon 不会自建。写入都去 Redis 或 Golden Source。

## 依赖

```
bifrost-core  ← 配置、Redis / PostgreSQL 读写层、组合模型（下限见 pyproject.toml）
```

## 命令

```bash
make install-dev                               # 可编辑安装兄弟目录的 core 等与本 repo（见 Makefile）

python scripts/run_daemon.py [config.yaml]     # 交易 daemon

make test                                      # pytest -m 'not ib and not db'
make lint                                      # ruff check .
```

## 测试标记

- `@pytest.mark.ib` — 需要 IB 实时连接
- `@pytest.mark.db` — 需要 PostgreSQL 连接
- 默认 CI 跑：`pytest -m 'not ib and not db'`
