# 占位策略 dummy_strategy

开发用的占位策略，同时测量行情延迟，部署在 **Osaka**。代码在 `crates/dummy_strategy`（库），二进制是 `src/bin/dummy_strategy.rs`。它**只依赖 `layline`**，不依赖行情服务的 crate：类型、共享内存 reader 和 HTTP 契约都来自 layline。等真正的策略出现后，它会和真正的策略一起迁移到单独的私有仓库。

## 模块

- `client.rs`：策略侧的 HTTP 协议。`subscribe` 阻塞到所有 topic 都被接受（任何一个被拒就失败），然后留下一个分离线程，每 10 秒调用一次 `/resubscribe`，保持租约，并在行情服务重启后恢复订阅。连接失败时按 `layline::backoff::exponential_backoff` 重试。等真正的策略需要它时，把它移进 layline。
- `strategy.rs`：`Strategy` trait：`on_bbo`、`on_trade`，以及按类型分发的默认方法 `on_market_data`；参数都带上策略自己的接收时间 `received_at_nanoseconds`。
- `latency_bench.rs`：`LatencyBench`，实现了 `Strategy`，在内存里记录每条消息的延迟，由单独的报告线程定期输出，测量本身不会拖慢被测的消息。

## 运行

```bash
cargo run --bin dummy_strategy -- --demo      # --demo 必须和行情服务配置里的 demo 一致
```

和行情服务的启动顺序无所谓。它以退避方式重试 `/subscribe`，订阅 Binance 上 BTC 和 ETH 的现货和永续的 BBO 和 Trade（现货需要行情服务从 Phanes 取到 SBE key，否则行情服务退出），在内存里记录每条消息的行情服务延迟、传输延迟和解析延迟；每隔 `--report-interval-seconds`（默认 60）由报告线程输出每个 topic 和整体的 p50/p90/p99/p99.9/max（单位 µs；internal = 行情服务 + 传输）。`--demo` 与配置的 `demo` 不一致时，每个请求都会被拒绝（400）。
