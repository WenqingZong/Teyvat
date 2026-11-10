# 系统总览

Teyvat 是一个个人量化交易系统的唯一代码库，命名取自原神。本文描述**已经实现**的整体结构；各组件的细节见同目录下的其他文档，尚未实现的功能见 [`plan/`](../plan/)。

## 机器

| 名称 | 硬件与网络 | 用途 |
|---|---|---|
| **Osaka** | Oracle Cloud 永久免费 AMD 实例，位于大阪：1/8 OCPU（AMD EPYC 7551，2 vCPU @ 2.0 GHz）、1 GB 内存、无 swap、44 GiB 硬盘，网络 0.48 Gbps，静态公网 IPv4, 静态公网IPv6，Ubuntu 26.04（x86_64） | **测试环境**，验证实现的正确性：现在运行行情服务和策略。**磁盘上不存 API Key**，启动时向 Phanes 取。 |
| **Tokyo** | Oracle Cloud 永久免费 ARM 实例，位于东京：4 核 Neoverse-N1、24 GB 内存、无 swap、192 GiB 硬盘，网络每核 0.48 Gbps，静态公网IPv4, 静态公网IPv6, Ubuntu 26.04（**aarch64**，部署时要按 aarch64 编译） | **生产环境**，暂未启用。 |
| **Xian** | 江苏安全云的廉价 NAT 云服务器，位于陕西西安：2 vCPU（Intel Xeon Platinum 8173M @ 2.0 GHz）、2 GB 内存，网速未知，每月流量上限 1000 GB，静态公网IPv4, 无IPv6, Ubuntu 24.04（x86_64） | **基准测试机**，只用于测量系统内部延迟，暂未启用。 |
| **RedmiBook** | 个人笔记本（i7-8565U、8 GB、Kali Linux），家用 WiFi，无公网 IP，经 Cloudflare Tunnel 对外提供服务 | Phanes（总控），以及以后的 ClickHouse 数据库。 |

代码在另一台笔记本（Intel Core Ultra 5 235U、Windows 11）的 WSL2（Ubuntu 24.04）里开发。

下图是现在的部署：交易进程在 Osaka。

```text
+-- Osaka ---------------------------------------------------+
| market_data_provider --shared memory rings--> strategies   |
| fetches API keys from Gnosis at startup                    |
| the provider and every strategy report their process       |
| metrics to Phanes Celestia                                 |
+----+-------------------------------------------------------+
     | HTTPS; Noise_IK end to end, Cloudflare sees only ciphertext
     v
 Cloudflare --tunnel--> cloudflared
                             |
+-- RedmiBook ---------------v-------------------------------+
| phanes (127.0.0.1:8888)                                    |
|   POST /gnosis    releases API keys                        |
|   GET  /celestia  receives process metrics                 |
|   GET  /info      liveness check                           |
+------------------------------------------------------------+
```

## 代码结构

一个 Cargo workspace（edition 2024，resolver 3）。根目录的 `Cargo.toml` 同时是 `teyvat` package，**定义系统里所有的二进制和 example**，`cargo build --release --bins --examples` 一条命令全部构建。构建其中任何一个二进制都会编译全部 crate。

| 目标 | 源码 | 部署位置 |
|---|---|---|
| bin `phanes` | `src/bin/phanes.rs` | RedmiBook |
| bin `market_data_provider` | `src/bin/market_data_provider.rs` | Osaka |
| bin `dummy_strategy` | `src/bin/dummy_strategy.rs` | Osaka |
| example `fetch` / `bark_push` / `celestia` | `examples/`：模拟 Osaka 取 key、发一条 Bark 推送、Celestia 上报的冒烟测试 | 在 RedmiBook 编译，按需拷贝 |

其余代码都在 `crates/` 下的库里：

| Crate | 说明 | 文档 |
|---|---|---|
| `phanes` | 总控：Gnosis 发放 API Key，Celestia 接收进程监控数据 | [`phanes.md`](phanes.md) |
| `market_data_provider` | 连接交易所，标准化行情，经共享内存发布给策略 | [`market_data_provider.md`](market_data_provider.md) |
| `dummy_strategy` | 开发用的占位策略，兼行情延迟测试 | [`dummy_strategy.md`](dummy_strategy.md) |
| `layline` | 地脉：各 crate 共用的代码 | 本文下一节 |

部署文件（脚本、systemd unit、配置模板）在顶层 [`deploy/`](../deploy/README.md)，crate 里只有代码。

## layline（地脉）

只有**当前**被两个及以上 crate 用到的代码才放进 layline；只有一个 crate 用到的代码留在那个 crate，等第二个使用者出现再移进来。例外是 Phanes 各服务客户端的配置表：`gnosis::client::GnosisClientConfig`（`[gnosis_client]`，目前只有 market_data_provider 用，以后的服务如 order entry 也会用）和 `celestia::client::CelestiaClientConfig`（`[celestia_client]`）放在一起，同样的东西写法一致。layline 是一个 crate，不用 feature 拆分。顶层模块：

- **行情契约**：`types`（一个类型一个文件：`Instrument`、`Topic`、`Bbo`、`Trade` 等）、`shared_memory`（每个策略自己的 ring：文件格式、`MarketDataReader`、`MarketDataWriter`、`SerializedMarketData`）、`provider_api`（行情服务对策略的 HTTP 契约）。
- **Phanes 协议**：`noise`（Noise_IK 加密层：客户端在消息 1 里带上自己的公钥，服务按公钥认出客户端；公钥和私钥一样放在 `SecretBox` 里）、`gnosis`（目录：`mod.rs` 是协议的服务端和客户端两半，两端不会走样，以及 Phanes 能发放的全部 gnosis 的 `GnosisId` 枚举；`client.rs` 是客户端的配置表 `GnosisClientConfig`（`[gnosis_client]`）；每个交易所一个文件放它的 key 类型，如 `gnosis/binance.rs`。每个 id 对应一个 `deny_unknown_fields` 的 key 类型，如 `BinanceApiKey { api_key, secret_key }`、`BinanceSbeApiKey { api_key }`；`Gnosis` trait 由类型给出它的实盘和 demo id，客户端按类型取 gnosis，不会配错 id，收到的 gnosis 直接按该类型解析。新增一种 gnosis 要改这里和 Phanes 的 `[gnoses]`，并重新部署 Phanes 和用到它的服务）。
- **Celestia**：`celestia`（`mod.rs` 是协议：Noise 握手的 `Initiator` / `Responder`、`Session`、`Batch`、`Ack`；`client.rs` 是上报端：`[celestia_client]` 配置表 `CelestiaClientConfig`、`ReporterConfig`、async 任务 `run_celestia_reporter`），Phanes 用协议的服务端那一半，market_data_provider 和策略用上报端。
- **进程监控**：`process_metrics`（`run_process_monitor(interval, sample_sender_option)` 是一个 async 任务，由各进程 spawn 到自己的 tokio runtime 上：每个间隔读一次本进程的计数器、打两行 info 日志，有 sender 时再把 `ProcessSample` 交出去，供上报 Phanes Celestia。见下文「进程监控」和「线程模型」）。
- **服务基础设施**：`bark`（Bark 加密推送和 `Notifier`：release 构建走 Bark，debug 构建打日志）、`secrets`（读取权限必须为 600 的密钥文件；`read_secret_toml` 读取并解析 600 的 TOML 配置，解析错误不引用原文）、`logging`（`init_tracing`）、`build`（`RELEASE_BUILD`、`build_profile`、`GIT_COMMIT_HASH`：layline 的 `build.rs` 在编译时执行 `git rev-parse HEAD` 写入，只在 HEAD、`refs/heads` 或 `packed-refs` 变化时重跑；不反映未提交的改动，不在 git 仓库里构建会失败）、`time`（墙钟时间、`NANOSECONDS_PER_SECOND`、`/info` 的时间格式）、`backoff`（指数退避）、`shutdown`（`shutdown_signal` 等待 SIGINT 或 SIGTERM，返回 `ShutdownSignal`；三个服务和 `examples/celestia.rs` 共用）。
- **`testing`**：所有 crate 的测试共用的 fixture 和检查（`test_keypair`、`test_instrument`、`test_topic`、`test_bbo`、`write_secret_file`、`assert_acyclic_modules`、`assert_grouped_imports` 等）。它是 public 的，因为其他 crate 的测试要用（`#[cfg(test)]` 只对 layline 自己的测试生效），所以生产代码不调用它只靠约定。`MarketDataWriter` 同理：只有行情服务的交易所 worker 会创建它，也是约定。

## 公共约定

- **配置**：每个服务的全部配置来自一个 TOML 文件（权限 600，经 `layline::secrets::read_secret_toml` 读取），不读环境变量。
- **日志**：只用 `tracing`；二进制和 example 调用一次 `layline::logging::init_tracing()`，并持有返回的 guard 直到退出（drop 时 flush）。每行是 `时间 级别 文件:行号: 消息`，不打印模块路径（文件和行号已经定位）。每个服务启动时先以 info 打印构建类型和 commit（`debug build of commit <hash>`），配置 `load` 成功后再打印整份配置（多行 `Debug`，`{:#?}`）；公钥、私钥、gnosis 和 Bark 凭据都是 `secrecy` 类型，打印为 `[REDACTED]`。含 `Url` 或 topic 集合的配置类型手写 `Debug`（解构 `Self` 列出全部字段，加字段时编译器会提醒）：URL 打印为一行，topic 按 `Display` 排序、每行一个。各 crate 的 config 测试用完整的期望输出检查这一点。
- **推送**：release 构建通过 Bark 推送到手机，缺少 `[bark]` 配置就拒绝启动；debug 构建不需要 Bark，推送改为 `info!` 日志。
- **时间**：墙钟时间统一来自 `layline::time::get_current_timestamp_nanoseconds()`（`SystemTime`，`NonZeroU64` 纳秒）；热路径只用整数，不格式化日期。

## 进程监控

`layline::process_metrics` 监控**本进程**。它不开线程：`run_process_monitor` 是一个 async 任务，由进程 spawn 到自己不在热路径上的 runtime 上（见「线程模型」）。间隔来自各服务配置文件的 `process_metrics_interval_seconds`（`NonZeroU64`）；第一次读数只打底，之后每个间隔打两行日志：

```text
process metrics: cpu 99.8%, memory anon 6.0 MiB / file 9.0 MiB / shmem 16.0 MiB, 3 threads, 12 fds, context switches 2.1/s voluntary / 0.4/s involuntary, tcp received 912 B/s / sent 300 B/s, sampled in 85 µs
thread metrics (cpu, voluntary/involuntary context switches per second): dummy_strategy#1234 99.7% 0.0/0.3; latency-report#1235 0.0% 0.1/0.0; ...
```

| 指标 | 来源 | 说明 |
|---|---|---|
| 进程 CPU | `/proc/self/stat` 的 utime + stime | 100% 为一个核；含已退出的线程。USER_HZ 固定 100，分辨率 10 ms |
| 内存 | `/proc/self/status` 的 `RssAnon` / `RssFile` / `RssShmem` | 以 KiB 存为 `u32`（与 `/proc` 的单位相同，不损失精度，上限 4 TiB）。三者之和即 RSS。分开报是因为 `/dev/shm` 里的 ring 是共享页，行情服务和每个映射它的策略都会再算一遍 |
| 线程 CPU、上下文切换 | `/proc/self/task/<tid>/stat` 和 `status` | 进程级的上下文切换是现存线程之和（`/proc/self/status` 里的只算主线程）。非自愿切换高说明线程被抢占 |
| fd 数 | `/proc/self/fd` | 含监控列目录时自己占的那一个 |
| TCP 收发 | netlink `sock_diag` 列出本网络命名空间的 TCP socket 及其 `tcp_info`，按 inode 与 `/proc/self/fd` 里的 socket 匹配 | 进程口径，不依赖 systemd，本地运行也有数。收取 `tcpi_bytes_received`，发送取 `tcpi_bytes_acked`（对端已确认，不重复计算重传；SYN 和 FIN 各多算 1）。两次采样之间建立又关闭的连接看不到；UDP 不统计 |
| 采样耗时 | 读数前后的 `Instant` | 监控自身每次的开销。sock_diag 每次 dump 要遍历内核的整张 TCP 哈希表，耗时随哈希表大小（随内存）而非 socket 数增长：31 GB 的开发机上约 1 ms，其余读取共约 0.1 ms |

速率和百分比都是 `f32`：监控用七位有效数字足够，发送时比 `f64` 省一半。

`ProcessSample`、`ThreadSample` 和 `MemoryUsage` 是公开的、可 serde 序列化，Celestia 直接发送它们。时间字段取 ClickHouse 存储它们的单位：`timestamp_seconds: NonZeroU32` 是读数结束时刻的 Unix 秒（UTC），对应 `DateTime('UTC')`（存储即 UInt32 Unix 秒，入库无需转换；`NonZero` 让 serde 拒绝全零的时间戳）；`sampling_duration_microseconds: u32` 对应 `UInt32`。

有 sender 时，`run_process_monitor` 用 `try_send` 交出样本，从不等待接收方：接收方没取走上一个样本，就丢弃这个并 warn。

这些读取由内核在内存里现场生成，不拿其他线程会持有的锁（不读 `smaps`，它要拿 `mmap_lock`）。每次读取会占住所在 runtime 的 worker 1–3 ms，所以监控任务不放在热路径上。读取失败只 `warn!` 并跳过这个间隔，不让进程退出。唯一的例外是两次读数之间单调时钟没有前进：这说明机器本身坏了，监控任务 panic；三个服务的 panic hook 都会让整个进程退出（tokio 只会结束 panic 的那个任务，但 hook 先于它运行）。线程在被列出后退出、fd 在被列出后关闭都属正常，直接跳过。

Phanes、market_data_provider 和 dummy_strategy 都在加载配置后立即启动监控；Phanes 在装好 panic hook 之后才启动，监控任务 panic 时和其他 panic 一样推送并退出。market_data_provider 和 dummy_strategy 还把每个样本交给 `layline::celestia::client` 上报给 Phanes（见 [`phanes.md`](phanes.md)「Celestia 简化版的现状」），Phanes 只打本地日志。

## 线程模型

每个进程只为两类工作单独开线程：**热路径**（dummy_strategy 的主线程忙轮询 ring；market_data_provider 每个交易所一个 worker 线程，bcast 的 `Writer` 是 `!Send`），以及 `tracing-appender` 的日志写线程。其余工作都是 tokio 任务，放在进程已有的、不在热路径上的 runtime 上：

| 进程 | 承载其余工作的 runtime | 上面的任务 |
|---|---|---|
| Phanes | 服务本身的多线程 runtime | HTTP 服务、推送、进程监控 |
| market_data_provider | `#[tokio::main]` 的 runtime | HTTP API、coordinator、租约、进程监控、Celestia 上报 |
| dummy_strategy | 一个只有一个 worker 的 `background` runtime | 订阅和 resubscribe、延迟报告、进程监控、Celestia 上报 |

layline 里的组件（如 `run_process_monitor`）只提供 async 任务，不自己开线程或 runtime，由进程决定放在哪里。加功能时加任务，不加线程。

## 质量检查

| 检查 | 方式 |
|---|---|
| 无警告 | `cargo clippy --workspace --all-targets`，CI 里 `RUSTFLAGS=-Dwarnings`；workspace 级 lint 禁止 `unwrap`、`print!`、`dbg!` |
| 无模块循环依赖 | 每个 crate 的 `tests/module_dependencies.rs` 调用 `layline::testing::assert_acyclic_modules` |
| import 分组 | 每个 package 的 `tests/import_groups.rs` 调用 `layline::testing::assert_grouped_imports`：标准库 → 第三方 → 本仓库，空行分隔，组内由 rustfmt 排序 |
| 格式串风格统一 | 每个 package 的 `tests/format_strings.rs` 调用 `layline::testing::assert_consistent_format_strings`：同一个字符串字面量里不能混用位置占位符（`{}`、`{:?}`、`{0}`）和具名占位符（`{name}`）；表达式先绑定成局部变量再内联。检查 `build.rs`、`src`、`tests`、`benches`、`examples`，跳过注释 |
| 测试与覆盖率 | `cargo test --workspace`；`cargo llvm-cov --workspace`，CI 要求行覆盖率 ≥ 90% |

CI 配置在 `.github/workflows/rust_ci.yml`，dependabot 每天检查依赖更新。
