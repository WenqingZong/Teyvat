# 部署

本目录存放所有部署相关的文件。代码在 `crates/`，二进制在顶层 `teyvat` package 中定义，见 [`architecture/overview.md`](../architecture/overview.md)。

每个服务一个子目录：

| 目录 | 内容 |
|---|---|
| `phanes/` | Phanes 的部署脚本 `phanes.sh`、systemd unit `phanes.service`、配置模板 `phanes.example.toml`（RedmiBook） |
| `market_data_provider/` | 行情服务的部署脚本 `market_data_provider.sh`、systemd unit `market_data_provider.service`、配置模板 `market_data_provider.example.toml`（Osaka） |
| `clickhouse/` | RedmiBook 上 ClickHouse 的手动安装记录 [`clickhouse.md`](clickhouse/clickhouse.md)（尚未接入代码，见 [`plan/database.md`](../plan/database.md)） |

所有命令都在 **Teyvat 仓库根目录**执行。部署脚本只编译自己的二进制（`cargo build --release -p teyvat --bin <名字>`），
但因为所有二进制都在同一个 package 里，构建时会编译全部 crate。

## Phanes（RedmiBook）

### 部署后的文件

| 路径 | 内容 |
|---|---|
| `/usr/local/bin/phanes` | 二进制 |
| `/etc/phanes/gnosis.key` / `gnosis.pub` | Gnosis 私钥（600）/ 公钥 |
| `/etc/phanes/celestia.key` / `celestia.pub` | Celestia 私钥（600）/ 公钥 |
| `/etc/phanes/phanes.toml` | 配置（600；监听地址、客户端 IP 白名单、Osaka 的两个公钥、允许的 key id、Bark 凭据） |
| `/etc/phanes/api_keys/<key_id>.json` | 待发放的 key 文件（600），从 `gnoses/` 同步 |
| `/etc/systemd/system/phanes.service` | systemd unit |

### RedmiBook 部署

以下命令都在 **Teyvat 仓库根目录**、以**普通用户**执行（脚本内部按需 sudo）。

#### 1. 准备

```bash
sudo apt install -y build-essential pkg-config cmake curl
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh   # 已装 Rust 可跳过
```

`gnoses/<key_id>.json` 是要发放的 key 文件，仓库里提交的是 dummy 值，字段名就是程序要读的字段（如 SBE 只要 `api_key`，交易 key 要 `api_key` 和 `secret_key`）。部署前把其中的 dummy 值换成真实的 key，**换过之后不要提交**。

#### 2. 安装

```bash
deploy/phanes/phanes.sh install
```

首次运行会：编译、安装二进制、生成 Gnosis 和 Celestia 密钥、同步 `gnoses/`（为空则报错退出）、生成配置模板、安装 systemd unit，
然后因为配置还没填而**暂不启动**，并打印 **Gnosis 公钥和 Celestia 公钥**（之后要给 Osaka，也可随时 `cat /etc/phanes/*.pub`）。

#### 3. 填配置

`/etc/phanes/phanes.toml`：

- `[clients.Osaka]` 填 Osaka 的公网 IP：`ipv4` 必填，`ipv6` 可选（Osaka 有 IPv6，访问 Cloudflare 时可能走 IPv6，记得填上）。
  各服务的 `allowed_clients` 引用这里的名字；引用不存在的名字、IP 无效或同一 IP 出现在两个名字下，Phanes 拒绝启动。
- `gnosis_service_config.public_key` 填 Osaka 的 Gnosis 公钥（见下文 Osaka 第 1 步）；`allowed_key_ids` 列出允许 Osaka 取的 key，只能是 `layline::api_keys::ApiKeyId` 里的 id（`binance_real`、`binance_demo`、`binance_sbe_real`、`binance_sbe_demo`），写错则 Phanes 拒绝启动；每个允许的 key 文件必须存在且字段与 `gnoses/` 模板一致。
- `celestia_service_config.public_key` 填 Osaka 的 Celestia 公钥。
- `[bark]` 的 `device_key`，以及 `encryption_key`（32 位，Bark App 里设为 AES-256-CBC）。

再跑一次 `deploy/phanes/phanes.sh install`，这次会启动服务并检查 `/info`，手机应收到"天理降临"。

#### 4. Cloudflare Tunnel

给 Phanes 一个单独的子域名 `phanes.wenqing.dev`，指向 `http://127.0.0.1:8888`（与 `phanes.toml` 的 `listen_address` 一致）：

- **本地 `config.yml` 管理的 tunnel**：在兜底规则前加
  ```yaml
  - hostname: phanes.wenqing.dev
    service: http://127.0.0.1:8888
  ```
  然后 `cloudflared tunnel route dns <tunnel 名> phanes.wenqing.dev && sudo systemctl restart cloudflared`。
- **Zero Trust 面板管理（token）的 tunnel**：Networks → Tunnels → Public Hostname 加同样的映射。

确认 `systemctl is-enabled cloudflared` 为 `enabled`。

再在 Cloudflare 面板给该主机名加 WAF 自定义规则（Security → WAF → Custom rules）：
表达式 `(http.host eq "phanes.wenqing.dev" and not ip.src in {<Osaka IPv4> <Osaka IPv6>})`，动作 Block。非白名单流量到不了隧道，与 Phanes 的白名单互为两层。

#### 5. 让笔记本保持在线

Osaka 每次重启都要取 key，RedmiBook 必须在线：

- 合盖不休眠：`/etc/systemd/logind.conf` 设 `HandleLidSwitch=ignore`、`HandleLidSwitchExternalPower=ignore`，`sudo systemctl restart systemd-logind`。
- 时钟同步（请求时间戳窗口 ±60s）：`timedatectl status` 应为 `synchronized: yes`，否则 `sudo timedatectl set-ntp true`。

#### 日常操作

| 场景 | 命令 |
|---|---|
| 代码更新后重新部署 | `git pull && deploy/phanes/phanes.sh install`（保留密钥和配置） |
| 修改 key | 改 `gnoses/`，必要时改 `allowed_key_ids`，再 `deploy/phanes/phanes.sh install` |
| 新增一种 key | 先在 `layline::api_keys` 加 id 和类型，在 `gnoses/` 加模板，再重新部署 Phanes 和用到它的服务 |
| 看日志 | `journalctl --namespace=phanes -u phanes -f`（Phanes 使用独立的 journal namespace） |
| 彻底删除 | `deploy/phanes/phanes.sh uninstall` |
| 从零重装 | `deploy/phanes/phanes.sh reinstall`（**会生成新 Gnosis 和 Celestia 密钥**，Osaka 要换公钥，`phanes.toml` 要重填） |

`uninstall` 删除上表"部署后的文件"里的所有东西以及 Phanes 的 journal 日志；cloudflared 的 hostname 映射保留。
`uninstall` / `reinstall` 会先确认，加 `--yes` 跳过。

### Osaka 配置

#### 1. 生成客户端密钥（只需 openssl）

Gnosis 和 Celestia 各一对：

```bash
sudo mkdir -p /etc/phanes-client && sudo chown $USER: /etc/phanes-client && chmod 700 /etc/phanes-client
cd /etc/phanes-client
for service in gnosis celestia; do
  openssl genpkey -algorithm X25519 -out $service.pem
  (umask 077; openssl pkey -in $service.pem -outform DER | tail -c 32 | base64 > ${service}_client.key)
  echo "$service: $(openssl pkey -in $service.pem -pubout -outform DER | tail -c 32 | base64)"   # → RedmiBook 的 phanes.toml
  shred -u $service.pem
done
```

> Ubuntu 新版默认的 sudo-rs 会对 Oracle Cloud Agent 的 sudoers 文件刷一堆解析警告，不影响执行；
> 嫌吵可 `sudo update-alternatives --config sudo` 选回 `sudo.ws`。

#### 2. RedmiBook 公钥

把 RedmiBook 的 `/etc/phanes/gnosis.pub`、`/etc/phanes/celestia.pub` 内容配给 Osaka 上的程序。

### 端到端验证（在 Osaka 上）

```bash
curl https://phanes.wenqing.dev/info        # → {"status":"ok",...}
```

在 RedmiBook 的 Teyvat 仓库根目录编译测试客户端并拷到 Osaka：

```bash
cargo build --release --example fetch
scp target/release/examples/fetch osaka:~/
```

```bash
./fetch --url https://phanes.wenqing.dev \
  --client-private-key-file /etc/phanes-client/gnosis_client.key \
  --gnosis-public-key "<gnosis.pub 内容>" --key-id binance_demo_trading
```

成功会打印 `granted <key_id>: <key>`（会打印 key 本身，只用于 dummy key），手机收到"神之心已发放"。

| 现象 | 原因 |
|---|---|
| `GLIBC_2.xx not found` | Kali 的 glibc 比 Osaka 新。改用 `sudo apt install musl-tools && rustup target add x86_64-unknown-linux-musl && cargo build --release --example fetch --target x86_64-unknown-linux-musl` |
| 403 + HTML 页面 | Cloudflare Bot Fight Mode / WAF 拦了数据中心 IP，给该 hostname 加 WAF Skip 规则 |
| 空 404，手机收到"白名单外的降临者被拦截" | 来源 IP 不在该服务的 `allowed_clients` 里。`journalctl` 里的 `blocked ... from cf-connecting-ip` 记录了 Cloudflare 看到的实际 IP，核对 `[clients.<name>]` 的 `ipv4` / `ipv6` |
| 空 404，手机收到"检测到未授权降临者" | Noise 握手失败：检查两边公钥是否配对、是否填反 |
| `Denied: timestamp outside allowed clock skew` | 两台机器时钟差超过 60s，检查 NTP |

#### 进程监控（简化版）

`examples/celestia/` 模拟 Osaka 端（`client.rs` 发送与补发、`sampler.rs` 采样）：每秒采样一个进程，发给 Phanes。
没收到 ack 的数据在内存里最多缓存 1 小时，断线后指数退避重连（1s → 60s）并补发。协议和实现见 [`architecture/phanes.md`](../architecture/phanes.md)。

```bash
cargo build --release --example celestia
scp target/release/examples/celestia osaka:~/
```

```bash
./celestia --url https://phanes.wenqing.dev \
  --client-private-key-file /etc/phanes-client/celestia_client.key \
  --celestia-public-key "<celestia.pub 内容>" --systemd-unit my_app.service   # 或 --process-id <pid>
```

- `--systemd-unit`：CPU、内存、网络都有。网络按进程口径统计，需要该 unit 开 `IPAccounting=yes`，否则网络为 0。
- `--process-id`：网络为 0。
- 两个参数必须且只能给一个。

RedmiBook 的日志里每秒一行 `Osaka #<sequence>: time .. ms, cpu ..%, memory .. MiB, received .. B/s, sent .. B/s`。
握手失败（公钥不配对）或某帧解密失败（伪造 / 篡改）属于安全事件：直接断开，手机收到"检测到未授权的魔神: 未授权的天空岛监控连接"（10 分钟节流）。
断线、长时间没数据等不推送，只记日志。

目前的简化：缓存在内存里，Osaka 端进程重启会丢掉未确认的数据；网络数据靠每秒调一次 `systemctl show`；Phanes 端只打日志、不去重。

## 行情服务 MarketDataProvider（Osaka）

### 安装

在 Teyvat 仓库根目录、以策略进程所用的用户执行；脚本内部按需 sudo。

```bash
deploy/market_data_provider/market_data_provider.sh install
```

首次运行会：编译 release 二进制、安装 systemd unit、创建 `/etc/market_data_provider/`（含 Gnosis 客户端密钥对，
以及从 `deploy/market_data_provider/market_data_provider.example.toml` 复制的配置），然后停下，列出
`/etc/market_data_provider/market_data_provider.toml` 里待填的占位符：

- `[phanes]` 的 `url` 和 `gnosis_public_key`（即 Phanes 主机上 `/etc/phanes/gnosis.pub` 的内容）；
- `[bark]` 凭据（release 构建缺少它会拒绝启动）；
- `demo`：`false` 为实盘行情，`true` 为交易所的 demo 行情。

在 Phanes 主机上，把安装时打印的公钥填进 `/etc/phanes/phanes.toml` 的 `[gnosis_service_config] public_key`，
确认 `allowed_key_ids` 包含 `binance_sbe_real`（实盘）或 `binance_sbe_demo`（demo），然后重启 Phanes。
再跑一次 `deploy/market_data_provider/market_data_provider.sh install`：这次会启动服务并检查 `/health`。

| 场景 | 命令 |
|---|---|
| 代码更新后重新部署 | `git pull && deploy/market_data_provider/market_data_provider.sh install`（保留密钥和配置） |
| 看日志 | `journalctl --namespace=market_data_provider -u market_data_provider -f` |
| 状态 | `systemctl status market_data_provider`、`curl localhost:6666/subscribed` |
| 彻底删除 | `deploy/market_data_provider/market_data_provider.sh uninstall`（先停掉策略：它会删除 ring 文件） |
| 从零重装 | `deploy/market_data_provider/market_data_provider.sh reinstall`（**会生成新的 Gnosis 客户端密钥**，Phanes 要换公钥） |

安装时会为该用户开启 linger：否则 systemd-logind 会在该用户最后一个 SSH 会话结束时删掉其 `/dev/shm` 下的 ring 文件，
而策略还在读它们。
