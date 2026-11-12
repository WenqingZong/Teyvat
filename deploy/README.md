# 部署

本目录存放所有部署相关的文件。代码在 `crates/`，二进制在顶层 `teyvat` package 中定义，见 [`architecture/overview.md`](../architecture/overview.md)。

每个服务一个子目录：

| 目录 | 内容 |
|---|---|
| `phanes/` | Phanes 的部署脚本 `phanes.sh`、systemd unit `phanes.service`、配置模板 `phanes.example.toml`（RedmiBook） |
| `market_data_provider/` | 行情服务的部署脚本 `market_data_provider.sh`、systemd unit `market_data_provider.service`、配置模板 `market_data_provider.example.toml`（Osaka） |
| `dummy_strategy/` | 占位策略的部署脚本 `dummy_strategy.sh`、systemd unit `dummy_strategy.service`、配置模板 `strategy.example.toml`（Osaka，和行情服务同机） |
| `common.sh` | 三个部署脚本共用的函数：日志和确认、编译安装二进制、准备配置目录、systemd unit 和 journal 上限、重启（先 `reset-failed`）、卸载、在配置里生成 X25519 私钥并推导公钥、`install` / `uninstall` / `reinstall` 分发 |
| `clickhouse/` | RedmiBook 上 ClickHouse 的手动安装记录 [`clickhouse.md`](clickhouse/clickhouse.md)（尚未接入代码，见 [`plan/database.md`](../plan/database.md)） |

**部署前先在目标机器上执行 `uname -r`，确认 Linux 内核 ≥ 4.2**：每个服务的进程监控（`layline::process_metrics`）从 `tcp_info` 读取 TCP 收发字节数，这两个字段是 4.2 加入的。内核过旧时服务照常运行，但每个监控间隔都会打一条 warn，说明 `tcp_info` 没有字节计数，监控数据缺失。

所有命令都在 **Teyvat 仓库根目录**执行。

每个服务的 systemd unit 都限制 600 秒内最多启动 5 次（`StartLimitIntervalSec=600`、`StartLimitBurst=5`）。配置有误时服务会连续启动失败，次数用满后 systemd 拒绝再启动（`Start request repeated too quickly`），修好配置也一样。各脚本的 `install` 在重启前会先 `reset-failed`，所以修好配置后再跑一次 `install` 即可；手动重启则要先 `sudo systemctl reset-failed <服务>`，再 `sudo systemctl restart <服务>`。

每个服务的日志写在自己的 journal namespace 里（unit 的 `LogNamespace=<服务>`），由一个独立的 journald 实例管理，它只读 `/etc/systemd/journald@<服务>.conf`，不读 `/etc/systemd/journald.conf`。`install` 会写这个文件：每个服务的 journal 最多 20 MB（`SystemMaxUse`、`RuntimeMaxUse`），最多保留 7 天（`MaxRetentionSec`），超出时 journald 先删最老的日志文件；`uninstall` 会删掉它。查看占用：`journalctl --namespace=<服务> --disk-usage`。部署脚本只编译自己的二进制（`cargo build --release -p teyvat --bin <名字>`），
但因为所有二进制都在同一个 package 里，构建时会编译全部 crate。

## Phanes（RedmiBook）

### 部署后的文件

| 路径 | 内容 |
|---|---|
| `/usr/local/bin/phanes` | 二进制 |
| `/etc/phanes/phanes.toml` | 配置（600；监听地址、Gnosis 和 Celestia 私钥、每个客户端的 IP、两个公钥和允许的 gnosis、待发放的 gnosis、Bark 凭据） |
| `/etc/systemd/system/phanes.service` | systemd unit |
| `/etc/systemd/journald@phanes.conf` | Phanes 的 journal 上限（20 MB、7 天） |

### RedmiBook 部署

以下命令都在 **Teyvat 仓库根目录**、以**普通用户**执行（脚本内部按需 sudo）。

#### 1. 准备

```bash
sudo apt install -y build-essential pkg-config cmake curl
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh   # 已装 Rust 可跳过
```

#### 2. 安装

```bash
deploy/phanes/phanes.sh install
```

首次运行会：编译、安装二进制、生成配置模板并用 openssl 生成 Gnosis 和 Celestia 密钥（私钥直接填进 `phanes.toml`，公钥不单独存文件，需要时由脚本从私钥推出）、安装 systemd unit，
然后因为配置还没填而**暂不启动**，并打印 **Gnosis 公钥和 Celestia 公钥**（之后要给每个客户端；每次 `install` 都会再打印一遍）。

#### 3. 填配置

`/etc/phanes/phanes.toml`：

- `process_metrics_interval_seconds`：多少秒打一次本进程的 CPU、内存、线程和 TCP 流量日志，必须大于 0（模板为 10）。
- 每个客户端一张 `[clients.<名字>]` 表（模板里是 `[clients.Osaka]`）：
  - `ipv4` 必填，`ipv6` 可选：Cloudflare 看到的公网 IP（Osaka 有 IPv6，访问 Cloudflare 时可能走 IPv6，记得填上）。IP 无效或同一 IP 出现在两个客户端下，Phanes 拒绝启动。
  - `client_gnosis_service_public_key`、`client_celestia_service_public_key`：该客户端的 Gnosis 和 Celestia 公钥：交易机器上由 `deploy/market_data_provider/market_data_provider.sh install` 打印（Celestia 公钥是整台机器的，行情服务和策略共用）；只跑 example 的机器见下文 Osaka 第 1 步。Phanes 按公钥认出客户端，所以同一个服务里两个客户端的公钥不能相同；公钥解不出来，Phanes 拒绝启动。
  - `allowed_gnosis_ids`：该客户端能取的 gnosis，只能是 `layline::gnosis::GnosisId` 里的 id（`binance_real`、`binance_demo`、`binance_sbe_real`、`binance_sbe_demo`），可以为空；写错 id，或引用了下面没有表的 gnosis，Phanes 拒绝启动。
- 每个要发放的 gnosis 一张 `[gnoses.<gnosis_id>]` 表，字段就是该 key 类型的字段：交易 key（`binance_real`、`binance_demo`）要 `api_key` 和 `secret_key`，SBE key（`binance_sbe_real`、`binance_sbe_demo`）只要 `api_key`。缺字段、多字段、未知 id，Phanes 都拒绝启动，报错只给行号和字段名，不会把 key 打进日志。
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
| 修改 gnosis 或授权 | 改 `/etc/phanes/phanes.toml` 的 `[gnoses]` 或客户端的 `allowed_gnosis_ids`，再 `sudo systemctl restart phanes` |
| 新增客户端 | 每个服务跑一次 `phanes keygen` 生成一对密钥：私钥填进该客户端自己的配置，公钥填进 `/etc/phanes/phanes.toml` 新加的 `[clients.<名字>]` 表，再 `sudo systemctl restart phanes`；Cloudflare WAF 规则也加上它的 IP |
| 新增一种 gnosis | 先在 `layline::gnosis` 加 id、在该交易所的文件（如 `layline/src/gnosis/binance.rs`，新交易所就新建一个）加类型，在 `phanes::gnosis::config::Gnoses` 加同名字段，在 `deploy/phanes/phanes.example.toml` 加示例，再重新部署 Phanes 和用到它的服务 |
| 看日志 | `journalctl --namespace=phanes -u phanes -f`（Phanes 使用独立的 journal namespace） |
| 彻底删除 | `deploy/phanes/phanes.sh uninstall` |
| 从零重装 | `deploy/phanes/phanes.sh reinstall`（**会生成新 Gnosis 和 Celestia 密钥**，Osaka 要换公钥，`phanes.toml` 要重填） |

`uninstall` 删除上表"部署后的文件"里的所有东西以及 Phanes 的 journal 日志；cloudflared 的 hostname 映射保留。
`uninstall` / `reinstall` 会先确认，加 `--yes` 跳过。

### Osaka 配置

#### 1. 生成客户端密钥（只需 openssl）

只给 `examples/` 用；部署了行情服务的机器不需要这一步，它的密钥由行情服务的安装生成（`celestia` example 也可以直接用那把 Celestia 私钥）。Gnosis 和 Celestia 各一对：

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

把 RedmiBook 上 `deploy/phanes/phanes.sh install` 打印的 Gnosis 和 Celestia 公钥配给 Osaka 上的程序。

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
  --gnosis-public-key "<Phanes 的 Gnosis 公钥>" --gnosis-id binance_demo
```

成功会打印 `granted <gnosis_id>: <gnosis>`（会打印 key 本身，只用于 dummy key），手机收到"神之心已发放"。

| 现象 | 原因 |
|---|---|
| `GLIBC_2.xx not found` | Kali 的 glibc 比 Osaka 新。改用 `sudo apt install musl-tools && rustup target add x86_64-unknown-linux-musl && cargo build --release --example fetch --target x86_64-unknown-linux-musl` |
| 403 + HTML 页面 | Cloudflare Bot Fight Mode / WAF 拦了数据中心 IP，给该 hostname 加 WAF Skip 规则 |
| 空 404，手机收到"检测到深渊入侵!!!" | 来源 IP 不属于任何客户端。`journalctl` 里的 `blocked request` 记录了全部 header，其中 `cf-connecting-ip` 是 Cloudflare 看到的实际 IP，核对 `[clients.<name>]` 的 `ipv4` / `ipv6` |
| 空 404，手机收到"检测到未授权的魔神" | 看 `journalctl` 里 `rejected unauthenticated Gnosis request:` 后面的原因：`public key belongs to no client`（客户端公钥没写进 `phanes.toml`，或填到了另一个服务的字段；公钥不写进日志，对照两边安装时打印的公钥检查）、`signed with the key of client A but sent from an address of client B`（公钥和 IP 不属于同一个客户端）、`noise: decrypt error`（客户端用的 Phanes 公钥不对） |
| `denied <gnosis_id>: gnosis not allowed for this client` | 该 id 不在这个客户端的 `allowed_gnosis_ids` 里 |
| `Denied: timestamp outside allowed clock skew` | 两台机器时钟差超过 60s，检查 NTP |

#### 进程监控（简化版）

`examples/celestia.rs` 是冒烟测试：不用部署任何服务，就能验证 Osaka 经 Cloudflare 到 Phanes Celestia 的整条链路。它像服务一样监控自己，每个间隔把样本发给 Phanes。
断线时样本在内存里最多缓存 1 小时，指数退避重连（1s → 60s）后补发。协议和实现见 [`architecture/phanes.md`](../architecture/phanes.md)。

```bash
cargo build --release --example celestia
scp target/release/examples/celestia osaka:~/
```

```bash
./celestia --url https://phanes.wenqing.dev \
  --client-private-key-file celestia_client.key \
  --celestia-public-key "<Phanes 的 Celestia 公钥>"   # 可选：--process-name（默认 celestia_example）、--process-metrics-interval-seconds（默认 10）
```

`celestia_client.key` 存这台机器的 base64 Celestia 私钥（行情服务配置里 `[celestia_client]` 的 `client_celestia_private_key`，或第 1 步生成的），权限必须是 600。

RedmiBook 的日志里每个间隔两行，以 `Osaka/celestia_example #<sequence>` 开头：第一行是进程指标和样本时间（Unix 秒），第二行是各线程的指标。
握手失败（公钥不配对）或某帧解密失败（伪造 / 篡改）属于安全事件：直接断开，手机收到"检测到未授权的魔神: 未授权的天空岛监控连接"（10 分钟节流）。
断线、长时间没数据等不推送，只记日志。

目前的简化：缓存在内存里，Osaka 端进程重启会丢掉未确认的数据；网络数据靠每秒调一次 `systemctl show`；Phanes 端只打日志、不去重。

## 行情服务 MarketDataProvider（Osaka）

### 安装

在 Teyvat 仓库根目录、以策略进程所用的用户执行；脚本内部按需 sudo。

```bash
deploy/market_data_provider/market_data_provider.sh install
```

首次运行会：编译 release 二进制、安装 systemd unit、创建 `/etc/market_data_provider/`（从
`deploy/market_data_provider/market_data_provider.example.toml` 复制的配置，以及用 openssl 生成的 Gnosis 客户端密钥和**这台机器的** Celestia 客户端密钥：
私钥直接填进配置的 `client_gnosis_private_key` 和 `[celestia_client]` 的 `client_celestia_private_key`；公钥不存文件，安装时由脚本从私钥推出并打印，只用来交给 Phanes，行情服务运行时用不到），然后停下，列出
`/etc/market_data_provider/market_data_provider.toml` 里待填的占位符：

- `[gnosis_client]` 的 `url` 和 `phanes_gnosis_public_key`（即 Phanes 主机上 `deploy/phanes/phanes.sh install` 打印的 Gnosis 公钥）；
- `[celestia_client]` 的 `url`（同上）和 `phanes_celestia_public_key`（同一处打印的 Celestia 公钥）。进程监控经它上报给 Phanes，进程名是 `market_data_provider`；这张表也是这台机器上每个策略的上报配置，策略的安装从这里拷走；
- `[bark]` 凭据（release 构建缺少它会拒绝启动）；
- `demo`：`false` 为实盘行情，`true` 为交易所的 demo 行情；
- `process_metrics_interval_seconds`：多少秒打一次本进程的 CPU、内存、线程和 TCP 流量日志，必须大于 0（模板为 10）。

在 Phanes 主机上，把安装时打印的两个公钥填进 `/etc/phanes/phanes.toml` 里这台机器的 `[clients.<名字>]` 表的 `client_gnosis_service_public_key` 和 `client_celestia_service_public_key`，
确认它的 `allowed_gnosis_ids` 包含 `binance_sbe_real`（实盘）或 `binance_sbe_demo`（demo），且 `[gnoses]` 里有这张表，然后重启 Phanes。
再跑一次 `deploy/market_data_provider/market_data_provider.sh install`：这次会启动服务并检查 `/health`。

| 场景 | 命令 |
|---|---|
| 代码更新后重新部署 | `git pull && deploy/market_data_provider/market_data_provider.sh install`（保留密钥和配置） |
| 看日志 | `journalctl --namespace=market_data_provider -u market_data_provider -f` |
| 状态 | `systemctl status market_data_provider`、`curl localhost:6666/subscribed` |
| 彻底删除 | `deploy/market_data_provider/market_data_provider.sh uninstall`（先停掉策略：它会删除 ring 文件） |
| 从零重装 | `deploy/market_data_provider/market_data_provider.sh reinstall`（**会生成新的 Gnosis 和 Celestia 客户端密钥**，Phanes 要换两个公钥，每个策略都要再 `install` 一次以拷走新的 Celestia 密钥） |

安装时会为该用户开启 linger：否则 systemd-logind 会在该用户最后一个 SSH 会话结束时删掉其 `/dev/shm` 下的 ring 文件，
而策略还在读它们。

## 占位策略 dummy_strategy（Osaka）

和行情服务在同一台机器、以**同一个用户**部署（它要读行情服务在 `/dev/shm` 里创建的 ring）。在 Teyvat 仓库根目录执行：

```bash
deploy/dummy_strategy/dummy_strategy.sh install
```

脚本会编译 release 二进制、安装 systemd unit，并把 `deploy/dummy_strategy/strategy.example.toml` 复制为
`/etc/dummy_strategy/strategy.toml`（600，已有则保留），**每次**都从行情服务的配置拷来 `[celestia_client]` 的设置（所以必须先装好行情服务、填好它的 `[celestia_client]`），然后重启服务，并在几秒后确认它仍在运行。配置无效时进程会退出，日志里有原因。

`strategy.toml` 是所有策略共用的格式：

- `strategy_name`：策略名，也是它在 `/dev/shm/market_data_provider/` 下的 ring 目录名；
- `provider_address`：行情服务的 HTTP 地址，即 `127.0.0.1:<market_data_provider.toml 的 port>`；
- `demo`：`true` 表示在交易所的模拟盘交易（行情也来自模拟盘），`false` 为实盘；必须与行情服务配置的 `demo` 一致，否则每个订阅请求都被拒绝（400）；
- `process_metrics_interval_seconds`：多少秒采样一次本进程的 CPU、内存、线程和 TCP 流量（打日志并上报 Phanes），必须大于 0（模板为 10）；
- `[celestia_client]`：上报进程监控用，由安装从行情服务的配置拷来，不用手填；上报的进程名就是 `strategy_name`；
- `[topics.<交易所>]`：每行一个品种（`BTC-USDT-SPOT`、`BTC-USDT-PERP`），值为行情类型列表（`BBO`、`TRADE`、`AGGREGATE_TRADE`，如 `["BBO", "TRADE"]`），与行情服务 HTTP API 的写法相同。

报告间隔是 dummy_strategy 自己的参数，不在 `strategy.toml` 里，写在 unit 的 `ExecStart`（`--report-interval-seconds 60`）。

| 场景 | 命令 |
|---|---|
| 代码更新后重新部署 | `git pull && deploy/dummy_strategy/dummy_strategy.sh install`（保留配置） |
| 修改订阅 | 编辑 `/etc/dummy_strategy/strategy.toml`，`sudo systemctl restart dummy_strategy` |
| 看延迟报告 | `journalctl --namespace=dummy_strategy -u dummy_strategy -f` |
| 彻底删除 | `deploy/dummy_strategy/dummy_strategy.sh uninstall`（ring 文件属于行情服务，保留） |
| 从零重装 | `deploy/dummy_strategy/dummy_strategy.sh reinstall`（配置恢复为模板） |
