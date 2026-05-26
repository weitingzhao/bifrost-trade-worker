# CLAUDE.md — bifrost-trade-worker

> 本项目是 bifrost-trader-engine 重构的一部分。迁移进度见 `bifrost-trade-infra/docs/MIGRATION_TRACKING.md`。

与本项目用户的所有对话一律使用中文。

## 职责范围

本 repo 包含所有**后台长进程和分布式任务**，分为两部分：

### 1. 交易 Daemon (`src/bifrost_worker/daemon/`)

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

---

### 2. Celery Workers (`src/bifrost_worker/celery/` + `src/bifrost_worker/data/`)

分布式后台任务，当前包含数据采集，未来可扩展任意 Celery 支持的异步任务：

| 子模块 | 职责 |
|--------|------|
| `celery/` | Celery app 初始化、beat 调度配置 |
| `data/` | Polygon 期权链/OHLCV 采集、IB 历史数据回填任务 |

#### Celery 队列

| 队列 | 用途 | Pool | 最大实例 |
|------|------|------|---------|
| `stocks_ib` | IB K 线回填 | prefork | 1 |
| `stocks_massive` | Polygon 股票数据 | solo | 2 |
| `stocks_massive_high` | 优先级股票数据 | solo | 1 |
| `options_massive` | Polygon 期权数据 | solo | 4 |
| `options_massive_high` | 优先级期权数据 | solo | 1 |

Flower 监控 UI 端口：**5555**

---

## 依赖

```
bifrost-core  ← 配置、PostgreSQL 写入层、ib_operator RPC client
celery[redis]
flower
ib_insync     ← 仅 data/ib 回填任务需要
```

## 命令

```bash
pip install -e ".[dev]"

python scripts/run_daemon.py                   # 启动交易 daemon

celery -A bifrost_worker.celery worker -Q stocks_massive,options_massive
celery -A bifrost_worker.celery beat            # 定时任务调度
celery -A bifrost_worker.celery flower          # 监控 UI（端口 5555）

pytest -m 'not ib and not db'
```

## 测试标记

- `@pytest.mark.ib` — 需要 IB 实时连接
- `@pytest.mark.db` — 需要 PostgreSQL 连接
- 默认 CI 跑：`pytest -m 'not ib and not db'`
