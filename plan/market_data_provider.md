# 行情服务计划

状态：以下各项均未实现。已实现部分见 [`architecture/market_data_provider.md`](../architecture/market_data_provider.md)。各项尚未展开设计，动手前需要先讨论。

## 功能

- **InstrumentManager**：统一管理各交易所的 symbol 拼写和交易规则。届时 `crates/market_data_provider/src/exchanges/binance/venue_stream.rs` 里的 `venue_symbol` 和 `stream_name` 移过去（见该文件的 TODO）。
- **Coindesk mapped / unmapped 检查**：具体内容待讨论。
- **跨重启保留订阅集合**：目前行情服务重启后，靠策略每 10 秒一次的 `/resubscribe` 恢复订阅；以后由行情服务自己持久化。
- **OKX connector**：`OkxConnector` 目前是 dummy，每 10 ms 发合成数据；换成真实实现后删除 `exchanges/synthetic.rs`。

## 长远规划（暂不实施）

以下各项工作量大、收益要靠实测确认，目前不做，只记录动机，供以后评估。

### 用自研的定长槽 ring 替换 bcast

**动机**：每个 ring 只装一种行情类型，`Bbo`、`Trade` 又都是不含变长字段的 `Copy` 结构，所以同一个 ring 里每条 rkyv 帧的长度本来就恒定（实际大小用 `size_of::<ArchivedBbo>()` 等实测）。bcast 是通用的字节 ring，没有利用这一点：每帧带长度头，绕回末尾时要插入 padding 帧，reader 还要先把帧拷进 `PayloadBuffer`、再 `rkyv::from_bytes` 反序列化（见 `crates/layline/src/shared_memory/reader.rs` 里 `MarketDataReader::poll` 上方的 PERFORMANCE 注释）。

**潜在收益**（类似 Disruptor 的定长槽设计）：

- **槽对齐到 cache line**（64 或 128 字节）：不需要长度头，也不需要 padding 帧；一条 BBO 正好占整数个 cache line。
- **每个槽自带 sequence**：reader 只需自旋读取下一个槽的 sequence，就能判断有没有新数据、有没有被覆盖，不用读全局的 producer position。空闲时检查的就是马上要读的那个 cache line。
- **零拷贝**：槽本身已对齐，可以用 rkyv 直接在共享内存里访问 archived 结构，省掉一次拷贝和一次反序列化。在读路径上，这部分开销可能比轮询本身更大。

**代价**：

- 全部是 `unsafe` 代码，还涉及内存序，需要专门的并发测试（如 loom），同时要满足 90% 的覆盖率。
- 要自己重新实现 bcast 现在提供的功能：writer 重启后从原有位置接上（`join_writer`）、元数据校验、多个 reader 各自独立读取。

**动手前提**：`poll_fan_out` 和 `codec` 两个 bench 证明拷贝加反序列化、或者轮询，是端到端延迟里的主要部分。

## 代码里的 TODO

| 位置（`crates/market_data_provider/` 下） | 内容 |
|---|---|
| `src/time.rs` | 给毫秒、微秒、纳秒各自独立的时间戳类型，让类型系统拒绝混用单位；在此之前，裸整数时间戳在名字里带单位 |
| `src/exchanges/worker.rs` | 每个交易所一个 worker 适合现在的 Oracle 免费实例；换更强的机器后，考虑按（交易所，行情类型）拆分 worker |
| `src/exchanges/mod.rs` | `ExchangeConnector::next_event` 的接口形状是临时的，需要详细讨论 |
