# 数据库计划

状态：规划中，未写代码。ClickHouse 已在 RedmiBook 上手动安装，见 [`deploy/clickhouse/clickhouse.md`](../deploy/clickhouse/clickhouse.md)。标 **【待定】** 的条目需要确认后再动手。

## 1. 数据范围

全部存在 RedmiBook。

| # | 数据 | 来源 | 规模 | 容量上限 |
|---|---|---|---|---|
| 1 | Osaka 进程监控：CPU、内存、网络（进程口径） | Osaka Celestia，每秒一次 | 每秒几个点 | 10 GB |
| 2 | Osaka 业务延迟数据：名字 + 数值，名字统一规划 | Osaka Celestia | 每秒十几个点 | 50 GB |
| 3 | Osaka 交易记录 | Osaka 业务程序 | 很小（小时–天级别持仓） | 不设 |
| 4 | 研究行情：单交易所约 50 个币种的逐笔成交；每 15 分钟一次、连续 10 秒的前 10 档 orderbook 快照 | RedmiBook 直连交易所 | 估算每天 0.3–0.6 GB，待实测校准 | 300 GB |

容量由人工监控，超限前扩容硬盘或上 NAS。程序不自动删除数据。

## 2. 选型

- **ClickHouse 单实例**存全部四类数据。只监听 `127.0.0.1`，systemd 管理，由 `deploy/` 安装和卸载。
- **Grafana** 用官方 ClickHouse 插件。跨周期对比用面板的 Time shift。
- Rust 端读写用官方 `clickhouse` crate。

## 3. 表设计

所有表按天分区（`PARTITION BY toYYYYMMDD(timestamp)`）。

| 数据 | 表结构 | 引擎 / 排序键 |
|---|---|---|
| 1、2 | 窄表 `(timestamp, name, value)`；延迟分位数用 `quantileTDigest` | `ReplacingMergeTree`，`ORDER BY (name, timestamp)`，补发的重复点按排序键去重 |
| 3 | 成交流水 | `ReplacingMergeTree`，按 (交易所, 成交 id) 去重 |
| 4 | 逐笔成交、orderbook 快照各一张表；每条存交易所时间和本地接收时间 | `MergeTree`，`ORDER BY (symbol, timestamp)` |
| 4 | 缺口表：记录采集断线区间 | `MergeTree`，`ORDER BY (symbol, start)` |

列编码：时间戳、id 用 `DoubleDelta`/`Delta`，价格用 `Gorilla`，统一叠加 `ZSTD`。

## 4. 回测

回测跑实盘 Rust 代码，只把数据源从交易所 websocket 换成 ClickHouse：

- 每个币种一条按时间排序的读取流，Rust 端多路归并成全局时间序后喂给策略。
- 回放时读缺口表，缺口区间要显式通知策略，不能当作无成交。

## 5. 行情采集

- 由 RedmiBook 直连交易所 websocket，作为 Phanes 的一个模块。
- 断线后指数退避重连，并把断线区间写入缺口表。
- 交易所：【待定 D-Q2】

## 6. 8 GB 内存调优

- 限制 `max_server_memory_usage_to_ram_ratio`、`mark_cache_size`、`uncompressed_cache_size`。
- 调小后台合并线程池。
- 关闭 `query_log`、`trace_log`、`metric_log` 等系统日志表，或给它们加短 TTL。

## 7. 待定问题

| # | 问题 | 推荐 |
|---|---|---|
| D-Q1 | 交易记录只存追加的成交流水，还是还要存订单状态变化、持仓、对账？ | 只存成交流水，状态由查询推导；若需要事务，这部分改用 SQLite |
| D-Q2 | 研究行情采集哪个交易所？ | — |
