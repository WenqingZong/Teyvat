# 占位策略 dummy_strategy

开发用的占位策略，同时测量行情延迟，部署在 **Osaka**。代码在 `crates/dummy_strategy`（库），二进制是 `src/bin/dummy_strategy.rs`。它**只依赖 `layline`**，不依赖行情服务的 crate：类型、共享内存 reader 和 HTTP 契约都来自 layline。等真正的策略出现后，它会和真正的策略一起迁移到单独的私有仓库。

## 模块

- `config.rs`：`StrategyConfig::load`，读取 `strategy.toml`（权限 600，经 `layline::secrets::read_secret_toml`）。这是所有策略共用的格式：`strategy_name`、`provider_address`、`demo`（在交易所模拟盘交易，行情也来自模拟盘）、`topics`；`topics` 按交易所 → 品种 → 行情类型三层嵌套，直接复用 `layline::provider_api::topic_set`，与 HTTP API 的写法相同。`deny_unknown_fields`；拼错交易所、品种或行情类型时由 serde 报错，topics 为空也拒绝启动。等第二个策略出现，把它移进 layline。
- `client.rs`：策略侧的 HTTP 协议。`subscribe` 阻塞到所有 topic 都被接受（任何一个被拒就失败），然后留下一个分离线程，每 10 秒调用一次 `/resubscribe`，保持租约，并在行情服务重启后恢复订阅。连接失败时按 `layline::backoff::exponential_backoff` 重试。等真正的策略需要它时，把它移进 layline。
- `strategy.rs`：`Strategy` trait：`on_bbo`、`on_trade`，以及按类型分发的默认方法 `on_market_data`；参数都带上策略自己的接收时间 `received_at_nanoseconds`。
- `latency_bench.rs`：`LatencyBench`，实现了 `Strategy`，在内存里记录每条消息的延迟，由单独的报告线程定期输出，测量本身不会拖慢被测的消息。

## 运行

```bash
chmod 600 strategy.toml
cargo run --bin dummy_strategy -- --config strategy.toml --report-interval-seconds 60
```

`strategy.toml` 的模板和字段说明见 [`deploy/dummy_strategy/strategy.example.toml`](../deploy/dummy_strategy/strategy.example.toml)，部署见 [`deploy/README.md`](../deploy/README.md)。`--report-interval-seconds`（默认 60）是 dummy_strategy 专用的参数，正式策略不需要，所以不在 `strategy.toml` 里。

和行情服务的启动顺序无所谓。它以退避方式重试 `/subscribe`，订阅配置里的 topic（模板里是 Binance 上 BTC 和 ETH 的现货和永续的 BBO 和 Trade；现货需要行情服务从 Phanes 取到 SBE key，否则行情服务退出），在内存里记录每条消息的行情服务延迟、传输延迟和解析延迟；每隔 `--report-interval-seconds` 由报告线程输出每个 topic 和整体的 p50/p90/p99/p99.9/max（单位 µs；internal = 行情服务 + 传输）。`demo` 与行情服务配置的 `demo` 不一致时，每个请求都会被拒绝（400）。
