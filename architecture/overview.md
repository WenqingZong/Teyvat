# 系统总览

Teyvat 是一个个人量化交易系统的唯一代码库，命名取自原神。本文描述**已经实现**的整体结构；各组件的细节见同目录下的其他文档，尚未实现的功能见 [`plan/`](../plan/)。

## 两台机器

| 名称 | 硬件与网络 | 运行什么 |
|---|---|---|
| **Osaka** | Oracle Cloud 永久免费 AMD 实例（1/8 OCPU、1 GB 内存、50 GB 硬盘），静态公网 IPv4，位于大阪 | 交易相关进程：行情服务、策略。**磁盘上不存 API Key**，启动时向 Phanes 取。 |
| **RedmiBook** | 个人笔记本（i7-8565U、8 GB、Kali Linux），家用 WiFi，无公网 IP，经 Cloudflare Tunnel 对外提供服务 | Phanes（总控） |

```text
+-- Osaka ---------------------------------------------------+
| market_data_provider --shared memory rings--> strategies   |
| fetches API keys from Gnosis at startup                    |
| process metrics (Celestia; only examples/celestia so far)  |
+----+-------------------------------------------------------+
     | HTTPS; Noise_KK end to end, Cloudflare sees only ciphertext
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
| example `fetch` / `bark_push` / `celestia` | `examples/`：模拟 Osaka 取 key、发一条 Bark 推送、模拟进程监控（`celestia` 自带测试，`test = true`） | 在 RedmiBook 编译，按需拷贝 |

其余代码都在 `crates/` 下的库里：

| Crate | 说明 | 文档 |
|---|---|---|
| `phanes` | 总控：Gnosis 发放 API Key，Celestia 接收进程监控数据 | [`phanes.md`](phanes.md) |
| `market_data_provider` | 连接交易所，标准化行情，经共享内存发布给策略 | [`market_data_provider.md`](market_data_provider.md) |
| `dummy_strategy` | 开发用的占位策略，兼行情延迟测试 | [`dummy_strategy.md`](dummy_strategy.md) |
| `layline` | 地脉：各 crate 共用的代码 | 本文下一节 |

部署文件（脚本、systemd unit、配置模板）在顶层 [`deploy/`](../deploy/README.md)，crate 里只有代码。

## layline（地脉）

只有**当前**被两个及以上 crate 用到的代码才放进 layline；只有一个 crate 用到的代码留在那个 crate，等第二个使用者出现再移进来。layline 是一个 crate，不用 feature 拆分。顶层模块：

- **行情契约**：`types`（一个类型一个文件：`Instrument`、`Topic`、`Bbo`、`Trade` 等）、`shared_memory`（每个策略自己的 ring：文件格式、`MarketDataReader`、`MarketDataWriter`、`SerializedMarketData`）、`provider_api`（行情服务对策略的 HTTP 契约）。
- **Phanes 协议**：`noise`（Noise_KK 加密层）、`gnosis`（API Key 协议的服务端和客户端两半都在这里，两端不会走样）、`api_keys`（Phanes 能发放的全部 key：`ApiKeyId` 枚举，每个 id 对应一个 key 类型，如 `BinanceApiKey { api_key, secret_key }`、`BinanceSbeApiKey { api_key }`，都 `deny_unknown_fields`，测试遍历全部 id 确认这一点；`ApiKey` trait 由类型给出它的实盘和 demo key id，客户端按类型取 key，不会配错 id；`parse_api_key` 的错误只列出文件里的字段名，从不引用值。新增一种 key 要改这里，并重新部署 Phanes 和用到它的服务）。
- **服务基础设施**：`bark`（Bark 加密推送和 `Notifier`：release 构建走 Bark，debug 构建打日志）、`secrets`（读取权限必须为 600 的密钥文件）、`logging`（`init_tracing`）、`build`（`RELEASE_BUILD`、`build_profile`）、`time`（墙钟时间、`/info` 的时间格式）、`backoff`（指数退避）。
- **`testing`**：所有 crate 的测试共用的 fixture 和检查（`test_keypair`、`test_instrument`、`test_topic`、`test_bbo`、`assert_acyclic_modules`、`assert_grouped_imports` 等）。它是 public 的，因为其他 crate 的测试要用（`#[cfg(test)]` 只对 layline 自己的测试生效），所以生产代码不调用它只靠约定。`MarketDataWriter` 同理：只有行情服务的交易所 worker 会创建它，也是约定。

## 公共约定

- **配置**：每个服务的全部配置来自一个 TOML 文件（权限 600），不读环境变量。
- **日志**：只用 `tracing`；二进制和 example 调用一次 `layline::logging::init_tracing()`，并持有返回的 guard 直到退出（drop 时 flush）。每行是 `时间 级别 文件:行号: 消息`，不打印模块路径（文件和行号已经定位）。
- **推送**：release 构建通过 Bark 推送到手机，缺少 `[bark]` 配置就拒绝启动；debug 构建不需要 Bark，推送改为 `info!` 日志。
- **时间**：墙钟时间统一来自 `layline::time::get_current_timestamp_nanoseconds()`（`SystemTime`，`NonZeroU64` 纳秒）；热路径只用整数，不格式化日期。

## 质量检查

| 检查 | 方式 |
|---|---|
| 无警告 | `cargo clippy --workspace --all-targets`，CI 里 `RUSTFLAGS=-Dwarnings`；workspace 级 lint 禁止 `unwrap`、`print!`、`dbg!` |
| 无模块循环依赖 | 每个 crate 的 `tests/module_dependencies.rs` 调用 `layline::testing::assert_acyclic_modules` |
| import 分组 | 每个 package 的 `tests/import_groups.rs` 调用 `layline::testing::assert_grouped_imports`：标准库 → 第三方 → 本仓库，空行分隔，组内由 rustfmt 排序 |
| 测试与覆盖率 | `cargo test --workspace`；`cargo llvm-cov --workspace`，CI 要求行覆盖率 ≥ 90% |

CI 配置在 `.github/workflows/rust_ci.yml`，dependabot 每天检查依赖更新。
