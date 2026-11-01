# Phanes

天理法涅斯, 量化系统总控. 本文中的路径如无特别说明, 均相对于 `crates/phanes/`.

部署在 RedmiBook 上，一个进程同时跑两个服务：

- **Gnosis**（API Key 发放服务，寓意天理发放神之心）：Osaka 上的程序启动时经 Cloudflare Tunnel 来取 key，
  请求和回复都是 Noise `KK` 密文（Cloudflare 只能看到乱码）；每次发放 / 拒绝 / 服务挂掉都会通过 Bark 推送到手机。
- **Celestia**（进程监控，寓意天空岛俯瞰提瓦特众生）：Osaka 每秒上报业务进程的 CPU、内存、网络（进程口径），经 WebSocket + Noise `KK` 加密传输。
  目前 Phanes 收到后只打日志并回 ack，入库和看板见 `plan/`。

两个服务各用一对密钥，任一服务的配置有误，Phanes 都拒绝启动。

release 构建在 `/gnosis` 和 `/celestia` 前加 IP 白名单（`/info` 不过滤）：客户端 IP 取 Cloudflare 写入的 `CF-Connecting-IP` 头，
不在该服务 `allowed_clients` 里（或头缺失）一律回空 404，并推送"白名单外的降临者被拦截"（10 分钟节流）。
因此 `listen_address` 必须是回环地址，否则 release 构建拒绝启动。

```
Osaka ──HTTPS──> Cloudflare ──tunnel──> cloudflared ──> 127.0.0.1:8888 Phanes (RedmiBook)
      └──────────── Noise_KK 端到端加密，Cloudflare 看不到明文 ────────────┘
```

| 接口 | 说明 |
|---|---|
| `POST /gnosis` | body 为 Noise 握手消息 1（含加密的 `{key_id, timestamp, nonce}`），返回消息 2（含加密的 key 或拒绝原因）。握手失败返回空 404。 |
| `GET /celestia` | 升级为 WebSocket。前两帧是 Noise 握手，之后每帧是一个加密的 `Batch`，Phanes 回加密的 `Ack`。握手或解密失败直接断开并推送。 |
| `GET /info` | 返回 JSON `{status, server_time, version, build}`（`server_time` 为 UTC 时间、精确到毫秒，如 `2026-10-15 13:30:05.123 UTC`），供 Osaka 探活。不做 IP 过滤，任何人可访问。 |

## 部署后的文件

| 路径 | 内容 |
|---|---|
| `/usr/local/bin/phanes` | 二进制 |
| `/etc/phanes/gnosis.key` / `gnosis.pub` | Gnosis 私钥（600）/ 公钥 |
| `/etc/phanes/celestia.key` / `celestia.pub` | Celestia 私钥（600）/ 公钥 |
| `/etc/phanes/phanes.toml` | 配置（600；监听地址、客户端 IP 白名单、Osaka 的两个公钥、允许的 key id、Bark 凭据） |
| `/etc/phanes/api_keys/<key_id>.txt` | 待发放的 key（600），从 `crates/phanes/api_keys/` 同步 |
| `/etc/systemd/system/phanes.service` | systemd unit |

## RedmiBook 部署

以下命令都在 **Teyvat 仓库根目录**、以**普通用户**执行（脚本内部按需 sudo）。

### 1. 准备

```bash
sudo apt install -y build-essential pkg-config cmake curl
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh   # 已装 Rust 可跳过
```

把要发放的 key 放进 `crates/phanes/api_keys/<key_id>.txt`（已 gitignore，不会提交）。

### 2. 安装

```bash
crates/phanes/deploy/phanes.sh install
```

首次运行会：编译、安装二进制、生成 Gnosis 和 Celestia 密钥、同步 `api_keys/`（为空则报错退出）、生成配置模板、安装 systemd unit，
然后因为配置还没填而**暂不启动**，并打印 **Gnosis 公钥和 Celestia 公钥**（之后要给 Osaka，也可随时 `cat /etc/phanes/*.pub`）。

### 3. 填配置

`/etc/phanes/phanes.toml`：

- `[clients.Osaka]` 填 Osaka 的公网 IP：`ipv4` 必填，`ipv6` 可选（Osaka 有 IPv6，访问 Cloudflare 时可能走 IPv6，记得填上）。
  各服务的 `allowed_clients` 引用这里的名字；引用不存在的名字、IP 无效或同一 IP 出现在两个名字下，Phanes 拒绝启动。
- `gnosis_service_config.public_key` 填 Osaka 的 Gnosis 公钥（见下文 Osaka 第 1 步）；`allowed_key_ids` 列出允许 Osaka 取的 key。
- `celestia_service_config.public_key` 填 Osaka 的 Celestia 公钥。
- `[bark]` 的 `device_key`，以及 `encryption_key`（32 位，Bark App 里设为 AES-256-CBC）。

再跑一次 `crates/phanes/deploy/phanes.sh install`，这次会启动服务并检查 `/info`，手机应收到"天理降临"。

### 4. Cloudflare Tunnel

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

### 5. 让笔记本保持在线

Osaka 每次重启都要取 key，RedmiBook 必须在线：

- 合盖不休眠：`/etc/systemd/logind.conf` 设 `HandleLidSwitch=ignore`、`HandleLidSwitchExternalPower=ignore`，`sudo systemctl restart systemd-logind`。
- 时钟同步（请求时间戳窗口 ±60s）：`timedatectl status` 应为 `synchronized: yes`，否则 `sudo timedatectl set-ntp true`。

### 日常操作

| 场景 | 命令 |
|---|---|
| 代码更新后重新部署 | `git pull && crates/phanes/deploy/phanes.sh install`（保留密钥和配置） |
| 新增 / 修改 key | 改 `crates/phanes/api_keys/`，必要时改 `allowed_key_ids`，再 `crates/phanes/deploy/phanes.sh install` |
| 看日志 | `journalctl --namespace=phanes -u phanes -f`（Phanes 使用独立的 journal namespace） |
| 彻底删除 | `crates/phanes/deploy/phanes.sh uninstall` |
| 从零重装 | `crates/phanes/deploy/phanes.sh reinstall`（**会生成新 Gnosis 和 Celestia 密钥**，Osaka 要换公钥，`phanes.toml` 要重填） |

`uninstall` 删除上表"部署后的文件"里的所有东西以及 Phanes 的 journal 日志；cloudflared 的 hostname 映射保留。
`uninstall` / `reinstall` 会先确认，加 `--yes` 跳过。

## Osaka 配置

### 1. 生成客户端密钥（只需 openssl）

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

### 2. RedmiBook 公钥

把 RedmiBook 的 `/etc/phanes/gnosis.pub`、`/etc/phanes/celestia.pub` 内容配给 Osaka 上的程序。

### 3. 客户端代码

Gnosis 协议的两端都在 `layline::gnosis`，Osaka 上的 crate（如 `market_data_provider`）依赖 workspace 里的 `layline`，参考 Teyvat 根目录的 [`examples/fetch.rs`](../../examples/fetch.rs)：

```toml
layline.workspace = true
```

```rust
let request = Request::new("binance_demo_trading", get_current_timestamp_nanoseconds());
let (initiator, first_message) =
    Initiator::write_request(&client_private_key, &gnosis_public_key, &request)?;
// POST first_message 到 https://phanes.wenqing.dev/gnosis
let response = initiator.read_response(&second_message)?;   // Response::Granted / Response::Denied
```

## 端到端验证（在 Osaka 上）

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

### 进程监控（简化版）

Teyvat 根目录的 `examples/celestia/` 模拟 Osaka 端（`client.rs` 发送与补发、`sampler.rs` 采样）：每秒采样一个进程，发给 Phanes。
没收到 ack 的数据在内存里最多缓存 1 小时，断线后指数退避重连（1s → 60s）并补发。Osaka 正式版只需 `phanes::celestia::proto` 和 `layline::noise`（正式客户端出现时，`celestia::proto` 将移入 `layline`）。

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

## 开发

在 Teyvat 仓库根目录：

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets && cargo test --workspace
```

debug 构建（`cargo run --bin phanes -- serve --config <phanes.toml>`）不需要 `[bark]`，所有推送改为打 `info` 日志，也不挂 IP 白名单
（`examples/` 直连、没有 `CF-Connecting-IP`），日志里的客户端名字显示为 `unfiltered`，方便本机开发；
release 构建（部署用）缺少 `[bark]` 会报错并拒绝启动。`phanes.toml` 含凭据，必须 `chmod 600`，否则拒绝启动。

测试 Bark 推送：`cargo run --example bark_push -- <phanes.toml> "message"`。
