# Teyvat

个人量化交易系统的唯一代码库。命名取自原神。

所有二进制（生产服务和测试工具）都在顶层的 `teyvat` package 里定义，一条命令全部构建：

```bash
cargo build --release --bins --examples
```

| 二进制 | 说明 | 部署位置 |
|---|---|---|
| `phanes` | 天理法涅斯，系统总控：Gnosis 发放 API Key，Celestia 接收进程监控数据 | RedmiBook |
| `market_data_provider` | 连接交易所，经共享内存向策略发布行情 | Osaka |
| `dummy_strategy` | 开发用的占位策略，兼行情延迟测试 | Osaka |
| example `fetch` / `bark_push` / `celestia` | 测试工具：模拟 Osaka 取 key、发一条 Bark 推送、模拟进程监控 | 按需拷贝 |

其余代码都是 `crates/` 下的库：

| Crate | 说明 |
|---|---|
| `phanes` | 总控的实现 |
| `market_data_provider` | 行情服务的实现 |
| `dummy_strategy` | 占位策略的实现 |
| `layline` | 地脉，蕴含一切信息：各 crate 共用的代码（行情类型、共享内存、Phanes 协议、Bark 推送、日志、测试 fixture 等）。只有被两个及以上 crate 用到的代码才放进来 |

## 文档

| 目录 | 内容 |
|---|---|
| [`architecture/`](architecture/README.md) | 已实现功能的说明：系统总览、Phanes、行情服务、占位策略 |
| [`plan/`](plan/README.md) | 尚未实现的功能和设计讨论 |
| [`deploy/`](deploy/README.md) | 部署脚本、systemd unit、配置模板和部署指南 |

## 目录结构

```
Teyvat/
├── Cargo.toml        # workspace（统一依赖版本和 clippy lint）+ teyvat package（定义所有二进制）
├── Cargo.lock
├── src/bin/          # 生产二进制
├── examples/         # 测试工具
├── crates/           # 只放代码
│   ├── layline/
│   ├── phanes/
│   ├── market_data_provider/   # 含 benches/
│   └── dummy_strategy/
├── architecture/     # 已实现功能的文档
├── plan/             # 尚未实现的功能
├── deploy/           # 部署脚本、unit、配置模板、部署指南
└── gnoses/           # Phanes 发放的 key 文件（提交的是 dummy 值，部署前换成真实值，换后不要提交）
```

私有策略将来以 git submodule 的形式放在 `crates/` 之外，并列入 workspace 的 `exclude`，使用自己的 `Cargo.lock`。

## 开发

在仓库根目录：

```bash
cargo build --bins --examples
cargo fmt --all --check
cargo clippy --workspace --all-targets
cargo test --workspace
cargo llvm-cov --workspace --summary-only   # CI 要求行覆盖率 ≥ 90%
```

每个 crate 都有 `tests/module_dependencies.rs`（禁止顶层模块之间循环 import）和 `tests/import_groups.rs`（import 按标准库、第三方、本仓库分组）。编码规范见 [`CLAUDE.md`](CLAUDE.md)。

## 许可证

[GPL-3.0](LICENSE)
