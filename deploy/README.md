# 部署

本目录是给人看的部署文档和脚本。`phanes/`、`market_data_provider/`、`dummy_strategy/` 每个服务一个子目录（部署脚本、systemd unit、配置模板），`common.sh` 是三个脚本共用的函数，`clickhouse/` 是 RedmiBook 上 ClickHouse 的[手动安装记录](clickhouse/clickhouse.md)（尚未接入代码）。脚本都在 **Teyvat 仓库根目录**、以**普通用户**执行（内部按需 sudo）。

部署前在目标机器上执行 `uname -r` 确认内核 ≥ 4.2：进程监控从 `tcp_info` 读取 TCP 收发字节数；旧内核服务照常运行，但每个监控间隔打一条 warn，数据缺失。

每个 unit 限制 600 秒内最多启动 5 次；配置错误连续失败后 systemd 拒绝再启动（`Start request repeated too quickly`）。`install` 重启前会先 `reset-failed`，修好配置再跑一次即可；手动重启则要先 `sudo systemctl reset-failed <服务>`。日志在独立 journal namespace（`LogNamespace=<服务>`），journald 只读 `/etc/systemd/journald@<服务>.conf`，不读 `/etc/systemd/journald.conf`；`install` 写入 20 MB 上限、保留 7 天，`uninstall` 删除，查占用用 `journalctl --namespace=<服务> --disk-usage`。脚本只编译自己的二进制（`cargo build --release -p teyvat --bin <名字>`），但会编译全部 crate。

## 1. Phanes（RedmiBook）

`install` 留下 `/usr/local/bin/phanes`、`/etc/phanes/phanes.toml`（600，含监听地址、Gnosis 和 Celestia 私钥、客户端、gnoses、Bark 凭据）、`/etc/systemd/system/phanes.service` 和 `/etc/systemd/journald@phanes.conf`。

### 准备与安装

```bash
sudo apt install -y build-essential pkg-config cmake curl
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh   # 已装 Rust 可跳过
deploy/phanes/phanes.sh install
```

首次运行会：编译、安装二进制、从模板生成配置并用 openssl 生成 Gnosis 和 Celestia 密钥（私钥直接写进 `phanes.toml`，公钥不存文件、需要时从私钥推出）、安装 unit，然后因配置未填而**暂不启动**，并打印 **Gnosis 公钥和 Celestia 公钥**（之后要交给每个客户端；每次 `install` 都会再打印一遍）。

### 填配置

编辑 `/etc/phanes/phanes.toml`（字段语义和校验见 [`architecture/phanes.md`](../architecture/phanes.md)，模板里有注释）：

- `process_metrics_interval_seconds`：多少秒打一次本进程的指标日志，必须大于 0（模板为 10）。
- `[clients.<名字>]`：`ipv4` 必填、`ipv6` 可选，填 Cloudflare 看到的公网 IP（Osaka 有 IPv6，访问 Cloudflare 时可能走 IPv6）；`client_gnosis_service_public_key`、`client_celestia_service_public_key` 填该客户端安装时打印的公钥（Celestia 公钥按机器共用，只跑 example 的机器见下节）；`allowed_gnosis_ids` 只能是 `binance_real`、`binance_demo`、`binance_sbe_real`、`binance_sbe_demo`，可为空。IP 或公钥重复、无效，或引用了没有表的 gnosis 时，Phanes 拒绝启动。
- `[gnoses.<gnosis_id>]`：交易 key（`binance_real`、`binance_demo`）要 `api_key` 和 `secret_key`，SBE key（`binance_sbe_real`、`binance_sbe_demo`）只要 `api_key`；缺字段、多字段或未知 id 都拒绝启动，报错只给行号和字段名，不会把 key 打进日志。
- `[bark]`：`device_key`，以及 `encryption_key`（32 位，Bark App 里设为 AES-256-CBC）。

再跑一次 `deploy/phanes/phanes.sh install`：这次会启动服务并检查 `/info`，手机应收到"天理降临"。

### Cloudflare Tunnel 与 WAF

给 Phanes 一个单独的子域名 `phanes.wenqing.dev`，指向 `http://127.0.0.1:8888`（与 `listen_address` 一致）：

- **本地 `config.yml` 管理的 tunnel**：在兜底规则前加
  ```yaml
  - hostname: phanes.wenqing.dev
    service: http://127.0.0.1:8888
  ```
  然后 `cloudflared tunnel route dns <tunnel 名> phanes.wenqing.dev && sudo systemctl restart cloudflared`。
- **Zero Trust 面板管理（token）的 tunnel**：Networks → Tunnels → Public Hostname 加同样的映射。

确认 `systemctl is-enabled cloudflared` 为 `enabled`。再在 Cloudflare 面板给该主机名加 WAF 自定义规则（Security → WAF → Custom rules），表达式 `(http.host eq "phanes.wenqing.dev" and not ip.src in {<Osaka IPv4> <Osaka IPv6>})`，动作 Block；非白名单流量到不了隧道，与 Phanes 的白名单互为两层。

### 让笔记本保持在线

交易机器每次重启都要取 key，RedmiBook 必须在线：

- 合盖不休眠：`/etc/systemd/logind.conf` 设 `HandleLidSwitch=ignore`、`HandleLidSwitchExternalPower=ignore`，`sudo systemctl restart systemd-logind`。
- 时钟同步（请求时间戳窗口 ±60s）：`timedatectl status` 应为 `synchronized: yes`，否则 `sudo timedatectl set-ntp true`。

### 日常操作

| 场景 | 命令 |
|---|---|
| 代码更新后重新部署 | `git pull && deploy/phanes/phanes.sh install`（保留密钥和配置） |
| 修改 gnosis 或授权 | 改 `/etc/phanes/phanes.toml` 的 `[gnoses]` 或客户端的 `allowed_gnosis_ids`，再 `sudo systemctl restart phanes` |
| 新增客户端 | 在客户端为每个服务跑一次 `phanes keygen` 生成一对密钥：私钥填进该客户端自己的配置，公钥填进 `[clients.<名字>]` 新表，再 `sudo systemctl restart phanes`；WAF 规则也要加上它的 IP |
| 新增一种 gnosis | 在 `layline::gnosis` 加 id、在该交易所文件（如 `layline/src/gnosis/binance.rs`）加 key 类型、在 `phanes::gnosis::config::Gnoses` 加字段、更新 `deploy/phanes/phanes.example.toml`，再重新部署 Phanes 和用到它的服务 |
| 看日志 | `journalctl --namespace=phanes -u phanes -f` |
| 彻底删除 | `deploy/phanes/phanes.sh uninstall` |
| 从零重装 | `deploy/phanes/phanes.sh reinstall`（**会生成新 Gnosis 和 Celestia 密钥**，Osaka 要换公钥，`phanes.toml` 要重填） |

`uninstall` 删除上表所有文件以及 Phanes 的 journal 日志，保留 cloudflared 的 hostname 映射；`uninstall` / `reinstall` 会先确认，加 `--yes` 跳过。

### Osaka：只跑 example 时生成客户端密钥

部署了行情服务的机器不需要这一步（密钥由行情服务的安装生成，`celestia` example 也可直接用那把 Celestia 私钥）。Gnosis 和 Celestia 各一对，只需 openssl：

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

> Ubuntu 新版默认的 sudo-rs 会对 Oracle Cloud Agent 的 sudoers 文件刷一堆解析警告，不影响执行；嫌吵可 `sudo update-alternatives --config sudo` 选回 `sudo.ws`。

### 端到端验证

在 Osaka 上 `curl https://phanes.wenqing.dev/info` 应返回 `{"status":"ok",...}`。在 RedmiBook 的仓库根目录编译并拷贝取 key 的测试客户端：`cargo build --release --example fetch && scp target/release/examples/fetch osaka:~/`。

然后运行（`/etc/phanes-client/gnosis_client.key` 是 `Osaka：只跑 example 时生成客户端密钥` 一节生成的私钥）：

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
| 空 404，手机收到"检测到未授权的魔神" | 看 `journalctl` 里 `rejected unauthenticated Gnosis request:` 后面的原因：`public key belongs to no client`（公钥没写进 `phanes.toml`，或填到了另一个服务的字段）、`signed with the key of client A but sent from an address of client B`（公钥和 IP 不属于同一个客户端）、`noise: decrypt error`（客户端用的 Phanes 公钥不对） |
| `denied <gnosis_id>: gnosis not allowed for this client` | 该 id 不在这个客户端的 `allowed_gnosis_ids` 里 |
| `Denied: timestamp outside allowed clock skew` | 两台机器时钟差超过 60s，检查 NTP |

### Celestia 冒烟测试

`examples/celestia.rs` 不用部署任何服务，就能验证 Osaka 经 Cloudflare 到 Phanes Celestia 的整条链路；协议和现状见 [`architecture/phanes.md`](../architecture/phanes.md)。在 RedmiBook 仓库根目录编译并拷贝：`cargo build --release --example celestia && scp target/release/examples/celestia osaka:~/`。

```bash
./celestia --url https://phanes.wenqing.dev \
  --client-private-key-file celestia_client.key \
  --celestia-public-key "<Phanes 的 Celestia 公钥>"   # 可选：--process-name（默认 celestia_example）、--process-metrics-interval-seconds（默认 10）
```

`celestia_client.key` 存这台机器的 base64 Celestia 私钥（行情服务配置里 `[celestia_client]` 的 `client_celestia_private_key`，或前面生成的），权限必须是 600。RedmiBook 每个间隔打两行日志；握手失败、解密失败等安全事件会断开并推送，详见 `architecture/phanes.md`。

## 2. 行情服务 MarketDataProvider（Osaka）

以策略进程所用的用户执行：

```bash
deploy/market_data_provider/market_data_provider.sh install
```

首次运行会：编译 release 二进制、安装 unit、创建 `/etc/market_data_provider/`（`market_data_provider.toml` 从 `deploy/market_data_provider/market_data_provider.example.toml` 复制，并用 openssl 生成 Gnosis 客户端密钥和**这台机器的** Celestia 客户端密钥：私钥直接写进配置，公钥不存文件、安装时从私钥推出并打印，只给 Phanes 用），然后停下并列出待填的占位符：

- `[gnosis_client]` 的 `url` 和 `phanes_gnosis_public_key`（即 Phanes 主机 `deploy/phanes/phanes.sh install` 打印的 Gnosis 公钥）；
- `[celestia_client]` 的 `url` 和 `phanes_celestia_public_key`（同一处打印的 Celestia 公钥）；进程监控经它上报，进程名是 `market_data_provider`，策略的安装会从这里拷走这张表；
- `[bark]` 凭据（release 构建缺少它会拒绝启动）；
- `demo`：`false` 为实盘行情，`true` 为交易所 demo 行情；
- `process_metrics_interval_seconds`：进程监控间隔，必须大于 0（模板为 10）。

在 Phanes 主机上，把安装打印的两个公钥填进这台机器的 `[clients.<名字>]`，确认 `allowed_gnosis_ids` 含 `binance_sbe_real`（实盘）或 `binance_sbe_demo`（demo）且 `[gnoses]` 里有对应表，然后重启 Phanes。再跑一次 `install`：这次会启动并检查 `/health`。

安装会为该用户开启 linger，否则最后一个 SSH 会话结束时 systemd-logind 会删掉 `/dev/shm` 下的 ring，而策略还在读。

| 场景 | 命令 |
|---|---|
| 代码更新后重新部署 | `git pull && deploy/market_data_provider/market_data_provider.sh install`（保留密钥和配置） |
| 看日志 | `journalctl --namespace=market_data_provider -u market_data_provider -f` |
| 状态 | `systemctl status market_data_provider`、`curl localhost:6666/subscribed` |
| 彻底删除 | `deploy/market_data_provider/market_data_provider.sh uninstall`（先停掉策略：它会删除 ring 文件） |
| 从零重装 | `deploy/market_data_provider/market_data_provider.sh reinstall`（**会生成新的 Gnosis 和 Celestia 客户端密钥**；Phanes 要换两个公钥，每个策略都要再 `install` 一次以拷走新的 Celestia 密钥） |

## 3. 占位策略 dummy_strategy（Osaka）

和行情服务同机、以**同一个用户**部署（它要读行情服务在 `/dev/shm` 里创建的 ring）：

```bash
deploy/dummy_strategy/dummy_strategy.sh install
```

脚本会编译 release 二进制、安装 unit，把 `strategy.example.toml` 复制为 `/etc/dummy_strategy/strategy.toml`（600，已有则保留），**每次**都从行情服务的配置拷来 `[celestia_client]`（所以必须先装好、填好行情服务），然后重启并在几秒后确认仍在运行；配置无效时进程退出，日志里有原因。

`strategy.toml` 是所有策略共用的格式，字段见[模板](dummy_strategy/strategy.example.toml)和 [`architecture/dummy_strategy.md`](../architecture/dummy_strategy.md)：

- `strategy_name`：策略名，也是它在 `/dev/shm/market_data_provider/` 下的 ring 目录名；
- `provider_address`：行情服务的 HTTP 地址，即 `127.0.0.1:<market_data_provider.toml 的 port>`；
- `demo`：必须与行情服务配置的 `demo` 一致，否则每个订阅请求都被拒绝（400）；
- `process_metrics_interval_seconds`：进程监控间隔，必须大于 0（模板为 10）；
- `[celestia_client]`：安装时从行情服务配置拷来，不用手填；上报的进程名就是 `strategy_name`；
- `[topics.<交易所>]`：每行一个品种（`BTC-USDT-SPOT`、`BTC-USDT-PERP`），值为行情类型列表（`["BBO", "TRADE"]`），写法与行情服务 HTTP API 相同。

`--report-interval-seconds 60`（延迟报告间隔）是 dummy_strategy 自己的参数，写在 unit 的 `ExecStart` 里，不在 `strategy.toml`。

| 场景 | 命令 |
|---|---|
| 代码更新后重新部署 | `git pull && deploy/dummy_strategy/dummy_strategy.sh install`（保留配置） |
| 修改订阅 | 编辑 `/etc/dummy_strategy/strategy.toml`，`sudo systemctl restart dummy_strategy` |
| 看延迟报告 | `journalctl --namespace=dummy_strategy -u dummy_strategy -f` |
| 彻底删除 | `deploy/dummy_strategy/dummy_strategy.sh uninstall`（ring 文件属于行情服务，保留） |
| 从零重装 | `deploy/dummy_strategy/dummy_strategy.sh reinstall`（配置恢复为模板） |
