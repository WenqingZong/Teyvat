# 占位策略 dummy_strategy

开发用的占位策略，同时测量行情延迟，部署在 **Osaka**。代码在 `crates/dummy_strategy`（库），二进制是 `src/bin/dummy_strategy.rs`。它**只依赖 `layline`**，不依赖行情服务的 crate：类型、共享内存 reader 和 HTTP 契约都来自 layline。等真正的策略出现后，它会和真正的策略一起迁移到单独的私有仓库。

## 模块

- `config.rs`：`StrategyConfig::load`，读取 `strategy.toml`（权限 600，经 `layline::secrets::read_secret_toml`）。这是所有策略共用的格式：`strategy_name`、`provider_address`、`demo`（在交易所模拟盘交易，行情也来自模拟盘）、`topics`、`process_metrics_interval_seconds`（进程监控的间隔，必须大于 0）、`[celestia_client]`（上报进程监控用，和行情服务配置里的同一张表、同一把机器密钥，安装脚本每次都从行情服务的配置拷过来；上报的进程名就是 `strategy_name`）；`topics` 按交易所 → 品种 → 行情类型三层嵌套，直接复用 `layline::provider_api::topic_set`，与 HTTP API 的写法相同。`deny_unknown_fields`；拼错交易所、品种或行情类型时由 serde 报错，topics 为空也拒绝启动。等第二个策略出现，把它移进 layline。
- `client.rs`：策略侧的 HTTP 协议（async，reqwest）。`ProviderSession::subscribe` 一直重试到所有 topic 都被接受（任何一个被拒就失败）；`resubscribe_forever` 是后台 runtime 上的任务，每 10 秒调用一次 `/resubscribe`，保持租约，并在行情服务重启后恢复订阅。连接失败或 5xx 时按 `layline::backoff::exponential_backoff` 重试；4xx（如 `demo` 不一致）重试也没用，`subscribe` 直接失败，resubscribe 时则 panic；错误信息带上 provider 在响应体里给的原因。行情服务每次返回的 ring 路径都应该和第一次相同、每个 topic 都应该有结果，否则说明两边代码对不上，直接 panic。等真正的策略需要它时，把它移进 layline。
- `strategy.rs`：`Strategy` trait：`on_bbo`、`on_trade`，以及按类型分发的默认方法 `on_market_data`；参数都带上策略自己的接收时间 `received_at_nanoseconds`。
- `latency_bench.rs`：`LatencyBench`，实现了 `Strategy`，在内存里记录每条消息的延迟；每个窗口经 channel 交给 `LatencyBench::new` 一并返回的报告任务，由它排序并输出分位数，测量本身不会拖慢被测的消息。

## 运行

```bash
chmod 600 strategy.toml
cargo run --bin dummy_strategy -- --config strategy.toml --report-interval-seconds 60
```

`strategy.toml` 的模板和字段说明见 [`deploy/dummy_strategy/strategy.example.toml`](../deploy/dummy_strategy/strategy.example.toml)，部署见 [`deploy/README.md`](../deploy/README.md)。`--report-interval-seconds`（默认 60）是 dummy_strategy 专用的参数，正式策略不需要，所以不在 `strategy.toml` 里。

和行情服务的启动顺序无所谓。它以退避方式重试 `/subscribe`，订阅配置里的 topic（模板里是 Binance 上 BTC 和 ETH 的现货和永续的 BBO 和 Trade；现货需要行情服务从 Phanes 取到 SBE key，否则行情服务退出），在内存里记录每条消息的行情服务延迟、传输延迟和解析延迟；每隔 `--report-interval-seconds` 由报告任务输出每个 topic 和整体的 p50/p90/p99/p99.9/max（单位 µs；internal = 行情服务 + 传输）。`demo` 与行情服务配置的 `demo` 不一致时，请求会被拒绝（400），策略立即退出。线程只有三个：忙轮询 ring 的主线程、`tracing-appender`，以及只有一个 worker 的 `background` tokio runtime，订阅（主线程 `block_on`）、resubscribe、延迟报告、进程监控和 Celestia 上报都是它上面的任务（见 [`overview.md`](overview.md)「线程模型」）。加载配置后、订阅之前，它 spawn `layline::process_metrics::run_process_monitor`，每隔 `process_metrics_interval_seconds` 打一次本进程的 CPU、内存、线程、fd 和 TCP 流量（见 [`overview.md`](overview.md)「进程监控」）；主线程忙轮询 ring，所以进程 CPU 常在 100% 左右，按线程拆开的那一行才看得出其他线程的负载。二进制装了 panic hook：主线程或任何后台任务（resubscribe、报告、监控）panic 都会记日志、flush 后让整个进程退出，不会只死掉一个任务而进程看起来照常运行。
