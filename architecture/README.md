# 架构文档

本目录描述**已经实现**的系统，给 AI 读：改动某个组件之前，先读它的文档；改完之后，在同一处更新。尚未实现的设计在 [`plan/`](../plan/README.md)，实现后把描述移到这里并从 plan 删除。面向人的系统全貌在 [`README.md`](../README.md)，部署步骤在 [`deploy/README.md`](../deploy/README.md)。

| 文档 | 内容 |
|---|---|
| [`overview.md`](overview.md) | 入口：机器、代码结构、layline 的模块、公共约定、进程监控、线程模型 |
| [`phanes.md`](phanes.md) | 总控：Gnosis（API Key 发放）、Celestia（进程监控）、IP 白名单、推送 |
| [`market_data_provider.md`](market_data_provider.md) | 行情服务：交易所连接、订阅协调、共享内存传输、基准测试 |
| [`dummy_strategy.md`](dummy_strategy.md) | 占位策略和延迟测试 |

`overview.md` 里的「公共约定」适用于所有组件；组件文档只写自己的设计，不重复总览和部署步骤。
