# TODO

## IP 白名单：部署后的手工步骤

代码层已完成（见 [`architecture/phanes.md`](../architecture/phanes.md)「已定方案」）。

- [ ] **Cloudflare WAF**：给 Phanes 的主机名加自定义规则 `(http.host eq "phanes.wenqing.dev" and not ip.src in {<Osaka IPv4> <Osaka IPv6>})` → Block（步骤见 [`deploy/README.md`](../deploy/README.md)「Cloudflare Tunnel」）。
