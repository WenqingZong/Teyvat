# 行情服务计划

状态：以下各项均未实现。已实现部分见 [`architecture/market_data_provider.md`](../architecture/market_data_provider.md)。各项尚未展开设计，动手前需要先讨论。

## 功能

- **InstrumentManager**：统一管理各交易所的 symbol 拼写和交易规则。届时 `crates/market_data_provider/src/exchanges/binance/venue_stream.rs` 里的 `venue_symbol` 和 `stream_name` 移过去（见该文件的 TODO）。
- **Coindesk mapped / unmapped 检查**：具体内容待讨论。
- **跨重启保留订阅集合**：目前行情服务重启后，靠策略每 10 秒一次的 `/resubscribe` 恢复订阅；以后由行情服务自己持久化。
- **BBO 的 seqlock 最新值槽**：BBO 本来就只用最新值；与 ring 的关系待讨论。
- **OKX connector**：`OkxConnector` 目前是 dummy，每 10 ms 发合成数据；换成真实实现后删除 `exchanges/synthetic.rs`。

## 代码里的 TODO

| 位置（`crates/market_data_provider/` 下） | 内容 |
|---|---|
| `src/time.rs` | 给毫秒、微秒、纳秒各自独立的时间戳类型，让类型系统拒绝混用单位；在此之前，裸整数时间戳在名字里带单位 |
| `src/exchanges/worker.rs` | 每个交易所一个 worker 适合现在的 Oracle 免费实例；换更强的机器后，考虑按（交易所，行情类型）拆分 worker |
| `src/exchanges/mod.rs` | `ExchangeConnector::next_event` 的接口形状是临时的，需要详细讨论 |
