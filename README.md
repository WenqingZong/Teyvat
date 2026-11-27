# Teyvat

个人量化交易系统的唯一代码库，命名取自原神。交易所行情经共享内存低延迟交给策略；API Key 由总控按需发放；各进程的运行指标回传给总控。

## 全貌

```text
 Binance / OKX
      │ websocket
      ▼
 交易机器（Osaka / Tokyo）
   market_data_provider ── 共享内存 ring ──▶ 策略进程
      │                                        │
      │ 启动时取 API Key        进程指标上报（两者都经 Noise 加密）
      └──────────────────┬─────────────────────┘
                         │ HTTPS + Cloudflare Tunnel，Cloudflare 只见密文
                         ▼
  RedmiBook：Phanes（总控）
    Gnosis   发放 API Key
    Celestia 接收进程指标（目前只记日志，入库与看板未做）
```

- **行情**：`market_data_provider` 连交易所，把 BBO、逐笔成交和现货/永续的前 10 档盘口写进每个策略自己的共享内存 ring；策略忙轮询读取，不经过网络栈。
- **策略**：通过本机 HTTP 向行情服务订阅 topic，订阅带租约；行情服务与策略可以各自独立重启。
- **总控**：Phanes 持有所有 API Key 与 Bark 凭据，客户端凭 X25519 密钥认领；同时接收各机器、各进程的指标。

## 机器

| 机器 | 角色 |
|---|---|
| **Osaka** | Oracle 免费 AMD 实例（1/8 OCPU、1 GB），测试环境：验证正确性，不测延迟 |
| **Tokyo** | Oracle 免费 ARM 实例（4 核、24 GB），生产环境，也是延迟实测的基准 |
| **Xian** | 廉价 NAT 云服务器，基准测试机（只测系统内部延迟），暂未启用 |
| **RedmiBook** | 家用笔记本，无公网 IP，经 Cloudflare Tunnel 运行 Phanes；以后也放 ClickHouse |

细节（网络、内核、磁盘）见 [`architecture/overview.md`](architecture/overview.md)。

## 代码

一个 Cargo workspace。根 package `teyvat` 定义**全部**二进制，`cargo build --release --bins --examples` 一条命令构建完：

| 目标 | 作用 |
|---|---|
| `phanes` | 总控：Gnosis 发 key，Celestia 收指标 |
| `market_data_provider` | 交易所行情 → 共享内存 |
| `dummy_strategy` | 占位策略兼延迟测试 |
| `examples/` | 冒烟测试工具：`fetch`（取 key）、`bark_push`、`celestia`（模拟上报） |

库在 `crates/`：

| Crate | 作用 |
|---|---|
| `layline` | 地脉：各 crate 共用的行情类型、共享内存、Phanes 协议、日志、密钥 |
| `phanes` / `market_data_provider` / `dummy_strategy` | 三个服务的实现 |
| `layline-testing` | 只给测试用的源码检查（模块循环、import 分组、格式串） |

## 文档

| 位置 | 读者 | 内容 |
|---|---|---|
| 本文件 | 人 | 系统全貌，只讲“是什么” |
| [`architecture/`](architecture/README.md) | AI | 已实现组件的细节设计、不变量、代码结构 |
| [`plan/`](plan/README.md) | AI | 未实现部分的设计、决策、待定问题 |
| [`deploy/`](deploy/README.md) | 人 | 部署脚本、systemd unit、配置模板与步骤 |
| [`AGENTS.md`](AGENTS.md) | AI | 工作规则、命令、编码规范、夜间测试流程 |

## 开发

```bash
cargo build --bins --examples
cargo fmt --all --check
cargo clippy --workspace --all-targets
cargo test --workspace
cargo llvm-cov --workspace --summary-only   # CI 要求行覆盖率 ≥ 90%
```

## 许可证

[GPL-3.0](LICENSE)
