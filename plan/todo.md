# TODO

## IP 白名单：部署后的手工步骤

代码层已完成（见 [`architecture/phanes.md`](../architecture/phanes.md)「已定方案」）。

- [ ] **Cloudflare WAF**：给 Phanes 的主机名加自定义规则 `(http.host eq "phanes.wenqing.dev" and not ip.src in {<Osaka IPv4> <Osaka IPv6>})` → Block（步骤见 [`deploy/README.md`](../deploy/README.md)「Cloudflare Tunnel」）。

## 面向用户的文档

现在 `architecture/`、`plan/` 等文档都是写给 AI 辅助开发看的。开源之前要另写一份给使用者看的文档：怎么部署、怎么订阅、每种行情类型的含义（例如 `TRADE` 与 `AGGREGATE_TRADE` 的区别，以及各交易所聚合口径的不同）。

- [ ] 编写面向用户的文档
