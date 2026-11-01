# 计划

本目录记录**尚未实现**的功能和设计讨论，人和 AI 都读这里。标 **【待定】** 的条目需要确认后再动手。某项实现之后，把它的描述移到 [`architecture/`](../architecture/README.md)，并在这里标记完成或删除。

| 文档 | 内容 |
|---|---|
| [`celestia.md`](celestia.md) | 进程监控的完整设计：磁盘 spool、ClickHouse 入库、Grafana 看板、`celestia_client` |
| [`database.md`](database.md) | 数据库：ClickHouse 表设计、回测数据读取、研究行情采集 |
| [`market_data_provider.md`](market_data_provider.md) | 行情服务：InstrumentManager、Coindesk 检查、订阅持久化、seqlock、OKX |
| [`todo.md`](todo.md) | 部署后的手工步骤 |
