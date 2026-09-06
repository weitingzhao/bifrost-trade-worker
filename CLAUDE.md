# CLAUDE.md — bifrost-trade-worker

> Legacy `bifrost-trader-engine` 已按 spine **D8**（2026-06-29）归档移出工作区。工作区事实基线见 `../AGENT_FACTS.md`。

与本项目用户对话一律使用中文回复（无论用户用何种语言提问）；UI 字符串与代码标识符使用 English。

## 工作区定位（2026-09-06）

| 项 | 值 |
|---|---|
| 域 / 载荷 | Trade (OLTP) · Satellite 执行载荷 · 仅 `daemon/`（Celery 已退役） |
| 运行位置 | K3s `bifrost-{dev,stg,prod}` 的 `daemon` Deployment —— **D10：STG `replicas: 0`，PROD observe-safe**，不得扩容 |
| 发布链 | GitHub main → `bifrost-deliver-{stg,prod}`（mirror-sync → build → rollout）；Argo `bifrost-stg/prod` 手动同步 |
| 仓库可见性 | GitHub **PUBLIC**（12 个 repo 全部公开）—— `.env`、Secret YAML、dump、kubeconfig、账户内容永不入库 |
| 硬边界 | D10 交易执行冻结（BLOCKED）· D13 三域边界 · 平台/业务解耦（Flywheel A/B） |
| 事实基线 | `../AGENT_FACTS.md`（§8c 运行时与安全事实）· 规则 `../CLAUDE.md`（§8 Claude Code 运行配置） |

会话请在工作区根 `/stocks` 启动（加载治理层 hooks / auto mode / 共享记忆）；运行时与安全事实以 `../AGENT_FACTS.md` §8c 为准。

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
- 不直接连接 IB，通过 Redis 读取行情和账户数据（**唯一写方**：Platform IB Gateway Plugin → `redis-ib`）
- 通过 `bifrost_core.ib_operator` RPC client 发送订单指令给 IB Gateway Operator（**D10 BLOCKED — 实盘发单冻结中**）
- **勿启动** `bifrost-trade-socket` 进程作 fallback（Wave 14G-F / D-14GF.4；双写=事故）
- 所有可配置参数通过 `bifrost_core.config` 加载

入口：`scripts/run_daemon.py`

### Celery（已退役 — Wave 5）

`stocks_ib` Celery + beat + Flower 已从本 repo 移除。Polygon ingest → Market Data Plugin；IB bars → Plugin minute-bars。后台任务统一使用 K8s CronJob + PG-as-broker + asyncio worker。

`ops_audit_log` 保留由 `bifrost-core` `_ensure_tables()` 顺带 drop 旧分区，或手动：

```bash
# Wave 6: ops_audit_log retired — no partition drop script
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
