# Teyvat

个人量化交易系统的唯一代码库。命名取自原神。

| Crate | 说明 | 部署位置 |
|---|---|---|
| [`phanes`](crates/phanes/README.md) | 天理法涅斯，系统总控：Gnosis 发放 API Key，Celestia 接收进程监控数据 | RedmiBook |
| [`market_data_provider`](crates/market_data_provider/README.md) | 连接交易所，经共享内存向策略发布行情 | Osaka |
| `dummy_strategy` | 开发用的占位策略，兼行情延迟测试 | Osaka |
| `layline` | 地脉，蕴含一切信息：各 crate 共用的代码（行情类型、共享内存、Phanes 协议、Bark 推送、日志等） | 库，不单独部署 |

只有被两个及以上 crate 用到的代码才放进 `layline`。

## 目录结构

```
Teyvat/
├── Cargo.toml        # workspace：统一依赖版本和 clippy lint
├── Cargo.lock
└── crates/
    ├── layline/
    ├── phanes/                 # 含 deploy/、examples/、plan/
    ├── market_data_provider/   # 含 deploy/、benches/
    └── dummy_strategy/
```

私有策略将来以 git submodule 的形式放在 `crates/` 之外，并列入 workspace 的 `exclude`，使用自己的 `Cargo.lock`。

## 开发

在仓库根目录：

```bash
cargo build
cargo fmt --all --check
cargo clippy --workspace --all-targets
cargo test --workspace
cargo llvm-cov --workspace --summary-only   # CI 要求行覆盖率 ≥ 90%
```

每个 crate 都有 `tests/module_dependencies.rs`，禁止顶层模块之间循环 import。编码规范见 [`CLAUDE.md`](CLAUDE.md)。

## 许可证

[GPL-3.0](LICENSE)
