# MarketDataProvider

给策略提供高频行情：连接交易所，标准化行情，经共享内存发布给策略进程。

## 部署（在交易机上，如 Osaka）

在 Teyvat 仓库根目录、以策略进程所用的用户执行；脚本内部按需 sudo。

```bash
crates/market_data_provider/deploy/market_data_provider.sh install
```

首次运行会：编译 release 二进制、安装 systemd unit、创建 `/etc/market_data_provider/`（含 Gnosis 客户端密钥对，
以及从 `deploy/market_data_provider.example.toml` 复制的配置），然后停下，列出
`/etc/market_data_provider/market_data_provider.toml` 里待填的占位符：

- `[phanes]` 的 `url` 和 `gnosis_public_key`（即 Phanes 主机上 `/etc/phanes/gnosis.pub` 的内容）；
- `[bark]` 凭据（release 构建缺少它会拒绝启动）；
- `demo`：`false` 为实盘行情，`true` 为交易所的 demo 行情。

在 Phanes 主机上，把安装时打印的公钥填进 `/etc/phanes/phanes.toml` 的 `[gnosis_service_config] public_key`，
确认 `allowed_key_ids` 包含 `binance_sbe_real`（实盘）或 `binance_sbe_demo`（demo），然后重启 Phanes。
再跑一次 `crates/market_data_provider/deploy/market_data_provider.sh install`：这次会启动服务并检查 `/health`。

| 场景 | 命令 |
|---|---|
| 代码更新后重新部署 | `git pull && crates/market_data_provider/deploy/market_data_provider.sh install`（保留密钥和配置） |
| 看日志 | `journalctl --namespace=market_data_provider -u market_data_provider -f` |
| 状态 | `systemctl status market_data_provider`、`curl localhost:6666/subscribed` |
| 彻底删除 | `crates/market_data_provider/deploy/market_data_provider.sh uninstall`（先停掉策略：它会删除 ring 文件） |
| 从零重装 | `crates/market_data_provider/deploy/market_data_provider.sh reinstall`（**会生成新的 Gnosis 客户端密钥**，Phanes 要换公钥） |

安装时会为该用户开启 linger：否则 systemd-logind 会在该用户最后一个 SSH 会话结束时删掉其 `/dev/shm` 下的 ring 文件，
而策略还在读它们。
