# CLAUDE.md — bifrost-trade-worker

> 本项目是 bifrost-trader-engine 重构的一部分。迁移进度见 `bifrost-trade-infra/docs/MIGRATION_TRACKING.md`。

与本项目用户对话一律使用中文回复（无论用户用何种语言提问）；UI 字符串与代码标识符使用 English。

## 职责范围

本 repo 包含**交易 Daemon + Account Sync**（Celery / stocks_ib / Massive 队列已退役 — Wave 5）。

### 交易 Daemon (`src/bifrost_worker/daemon/`)

GsTrading 主进程 — 单进程 asyncio，所有交易状态通过三层 FSM 驱动：

| 子模块 | 职责 |
|--------|------|
| `daemon/app/` | GsTrading 主入口，进程启动与生命周期管理 |
| `daemon/fsm/` | 三层 FSM：`DaemonFSM → TradingFSM → HedgeFSM` |
| `daemon/strategy/` | 策略逻辑（开仓条件、Hedge 判断） |
| `daemon/guards/` | 安全守卫（风控限制、Gate 检查） |
| `daemon/execution/` | 订单执行（通过 ib_operator RPC 发单） |
| `daemon/sink/` | 状态快照写入 PostgreSQL |

**Daemon 架构约束**：
- 不直接连接 IB，通过 Redis 读取行情（ib-edge 写入）和账户数据
- 通过 `bifrost_core.ib_operator` RPC client 发送订单指令给 ib-edge Operator
- 所有可配置参数通过 `bifrost_core.config` 加载

入口：`scripts/run_daemon.py`

### Celery（已退役 — Wave 5）

`stocks_ib` Celery + beat + Flower 已从本 repo 移除。Polygon ingest → Market Data Plugin；IB bars → Plugin minute-bars。后台任务统一使用 K8s CronJob + PG-as-broker + asyncio worker。

`ops_audit_log` 保留由 `bifrost-core` `_ensure_tables()` 顺带 drop 旧分区，或手动：

```bash
# in bifrost-trade-core
python scripts/db/drop_ops_audit_partitions.py --months 3
```

## 依赖

```
bifrost-core  ← 配置、PostgreSQL 写入层、ib_operator RPC client
```

## 命令

```bash
pip install -e ".[dev]"

python scripts/run_daemon.py                   # 启动交易 daemon
python scripts/run_account_sync_daemon.py      # account sync

pytest -m 'not ib and not db'
```

## 测试标记

- `@pytest.mark.ib` — 需要 IB 实时连接
- `@pytest.mark.db` — 需要 PostgreSQL 连接
- 默认 CI 跑：`pytest -m 'not ib and not db'`
