# 计划

本目录记录**尚未实现**的功能和设计讨论，给 AI 读。已实现的部分以 [`architecture/`](../architecture/README.md) 为准；这里只留设计决策、数字、约束和待定问题，标 **【待定】** 的条目需要确认后再动手。

| 文档 | 内容 |
|---|---|
| [`celestia.md`](celestia.md) | 进程监控的完整设计：磁盘 spool、ClickHouse 入库、Grafana 看板、限速补发 |
| [`database.md`](database.md) | 数据库：ClickHouse 表设计、回测数据读取、研究行情采集 |
| [`latency.md`](latency.md) | 延迟：本地、Osaka、Tokyo 三处实测对比，ring 预映射、绑核等提速方向 |
| [`market_data_provider.md`](market_data_provider.md) | 行情服务：Coindesk 检查、订阅持久化、OKX、替换 bcast（长远） |

## 部署后的手工步骤

- [ ] **Cloudflare WAF**：给 Phanes 的主机名加自定义规则 `(http.host eq "phanes.wenqing.dev" and not ip.src in {<Osaka IPv4> <Osaka IPv6>})` → Block（步骤见 [`deploy/README.md`](../deploy/README.md)「Cloudflare Tunnel」）。代码层已完成，见 [`architecture/phanes.md`](../architecture/phanes.md)「IP 白名单」。

## 面向用户的文档

- [ ] 编写面向用户的文档：`architecture/` 和 `plan/` 都是写给 AI 辅助开发看的，开源之前要另写一份给使用者看的——怎么部署、怎么订阅、每种行情类型的含义（例如 `TRADE` 与 `AGGREGATE_TRADE` 的区别，以及各交易所聚合口径的不同）。
