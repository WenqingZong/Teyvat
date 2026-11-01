# 架构文档

本目录描述**已经实现**的功能，人和 AI 都读这里：改动某个组件之前，先读它的文档；改完之后，同步更新文档。尚未实现的功能在 [`plan/`](../plan/README.md)，实现之后把描述从 plan 移到这里。

| 文档 | 内容 |
|---|---|
| [`overview.md`](overview.md) | 两台机器、代码结构、二进制、layline、公共约定、质量检查 |
| [`phanes.md`](phanes.md) | 总控：Gnosis（API Key 发放）、Celestia（进程监控简化版）、IP 白名单、推送 |
| [`market_data_provider.md`](market_data_provider.md) | 行情服务：交易所连接、订阅协调、共享内存传输、基准测试 |
| [`dummy_strategy.md`](dummy_strategy.md) | 占位策略和延迟测试 |

部署步骤不在这里，见 [`deploy/README.md`](../deploy/README.md)。
