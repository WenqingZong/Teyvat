# Phanes（总控）

天理法涅斯，量化系统的总控，部署在 **RedmiBook**。代码在 `crates/phanes`（库），二进制是 `src/bin/phanes.rs`。部署步骤见 [`deploy/README.md`](../deploy/README.md)，尚未实现的部分见 [`plan/celestia.md`](../plan/celestia.md) 和 [`plan/database.md`](../plan/database.md)。

一个进程、一个端口上同时运行两个服务：

- **Gnosis**（API Key 发放，寓意天理发放神之心）：Osaka 上的程序启动时经 Cloudflare Tunnel 来取 API Key。请求和回复都是 Noise `KK` 密文，Cloudflare 只能看到乱码。每次发放、拒绝、服务挂掉都通过 Bark 推送到手机。
- **Celestia**（进程监控，寓意天空岛俯瞰提瓦特众生）：Osaka 每秒上报业务进程的 CPU、内存、网络（进程口径），经 WebSocket + Noise `KK` 加密传输。**目前 Phanes 收到后只打日志并回 ack**，入库和看板还没做。

```text
Osaka --HTTPS--> Cloudflare --tunnel--> cloudflared --> 127.0.0.1:8888 Phanes (RedmiBook)
  +-------- Noise_KK end to end: Cloudflare sees only ciphertext --------+
```

| 接口 | 说明 |
|---|---|
| `POST /gnosis` | body 为 Noise 握手消息 1（含加密的 `{key_id, timestamp, nonce}`），返回消息 2（含加密的 key 或拒绝原因）。握手失败返回空 404。 |
| `GET /celestia` | 升级为 WebSocket。前两帧是 Noise 握手，之后每帧是一个加密的 `Batch`，Phanes 回加密的 `Ack`。握手或解密失败直接断开并推送。 |
| `GET /info` | 返回 JSON `{status, server_time, version, build}`（`server_time` 为 UTC 时间、精确到毫秒，如 `2026-10-15 13:30:05.123 UTC`），供 Osaka 探活。不做 IP 过滤，任何人可访问。 |

## 要求

- Osaka ↔ RedmiBook 的通讯**端到端加密**：请求和回复都是密文。Cloudflare 会终止 TLS、能看到 HTTP 明文，因此必须在应用层加密，不能依赖 TLS。
- 双方已各自持有对方的 SSH 公钥（ed25519）。
- 两端都用 Rust（tokio）。
- 暂不考虑 RedmiBook 本机的安全：API Key 以明文 JSON 存放在仓库根目录的 `gnoses/<key_id>.json`。`gnoses/` 提交进 git 的是 dummy 值，用来告诉部署者每个 key 文件有哪些字段；部署前换成真实值，**任何真实密钥都不得进入 git、日志或示例代码**。

## 已定方案

- **Gnosis 协议**：Noise `Noise_KK_25519_ChaChaPoly_BLAKE2s`（`snow` crate），prologue `gnosis`，一次 HTTP POST 完成握手加请求/回复。消息 1 的载荷是 `{key_id, timestamp, nonce}`，消息 2 的载荷是整个 key 文件（一个 JSON 对象）。虽然业务上是"取" key，但用 POST 而不是 GET：握手消息只能放在 body 里（GET 的 body 没有标准语义，Cloudflare 等中间层可能丢弃；放进 URL 又会让密文进入各级日志）；请求有副作用、不幂等（nonce 记入防重放表，每次都触发 Bark 推送）；GET 的响应还可能被 CDN 缓存。
- **Celestia 协议**：Noise 参数相同，prologue `celestia`，`GET /celestia` 升级为 WebSocket。前两帧是握手（空载荷），之后每帧是一个 postcard 编码的密文：Osaka → Phanes 为 `Batch{sequence, samples}`，Phanes → Osaka 为 `Ack{sequence}`。一条连接一次握手，不需要时间窗口和 nonce。虽然数据是客户端发给 Phanes，但必须用 GET：WebSocket 标准（RFC 6455）规定升级握手只能是带 `Upgrade: websocket` 头的 GET；升级之后不再有 HTTP 方法，双方的数据都走同一条连接上的帧。
- **密钥**：每个服务在两端各有一对专用的 X25519 密钥（不复用 SSH 密钥，服务之间也不复用），公钥经现有的 SSH 通道交换。
- **Gnosis 和 Celestia 缺一不可**：任一服务的配置缺失或有误（密钥无效、权限不是 600），Phanes 都拒绝启动。
- **IP 白名单**（只在 release 构建挂载）：每个客户端一张 `[clients.<名字>]` 表，`ipv4` 必填、`ipv6` 可选；各服务的 `allowed_clients` 引用这些名字；`GET /info` 不过滤。白名单只挂在各服务的路由上（`route_layer`）；未知路径回 404 并记完整请求日志，不推送。客户端 IP 只取 `CF-Connecting-IP`（不用 `X-Forwarded-For`）；头缺失、解析失败或不在白名单，一律回空 404 并 Bark 推送（10 分钟节流）。被拦截和未认证的请求，日志记录全部 header 和载荷（body 或出错的 WebSocket 帧）前 4096 字节的 hex；推送放不下这些，只带方法、路径、`User-Agent` / `Upgrade` / `CF-Ray` / `CF-IPCountry` / `CF-Worker` 和载荷长度。`listen_address` 必须是回环地址。名字不存在、IP 无效、同一 IP 出现在两个名字下、`allowed_clients` 为空，都拒绝启动。Cloudflare 面板另有一层 WAF 规则。
- **Celestia 的安全事件**：Noise 握手失败或某帧解密失败（密钥不对或伪造帧），断开连接并 Bark 推送（10 分钟节流）。其他运行时错误（断线、超时、载荷格式错）只断开当前连接、记日志，不推送，由 Osaka 重连补发。
- Osaka 只在启动时取一次 key，之后常驻内存。
- 目前只有一个客户端（Osaka）；需求变了再改，不提前泛化。
- 每次发放或拒绝密钥，都通过 Bark 加密推送（`layline::bark`，AES-256-CBC）通知手机。Bark 凭据放在 `phanes.toml` 的 `[bark]` 表（`device_key` / `encryption_key`，可选 `server`）。所有配置只从 `phanes.toml` 读，该文件必须是 600 权限，不入库。
- 每个服务用自己的密钥对和命名（Gnosis：`gnosis.key` / `gnosis.pub`、`gnosis_private_key`；Celestia：`celestia.key` / `celestia.pub`、`celestia_private_key`），不用泛泛的 `server_*`。

## 代码结构

- 每个服务一个目录。Gnosis 协议要和客户端共用，所以两半（`Request`/`Response`、`Initiator`/`Responder`）都在 `layline::gnosis`，这里只有服务端。Celestia 协议目前在本 crate 之外没有客户端，所以 `celestia/proto.rs` 留在这里；等第二个 crate 需要它时再移进 layline。
- `layline::noise`：所有服务共用的 Noise 层：参数、密钥编解码、`build_handshake(prologue, ..)`、`encrypt`/`decrypt`（握手和传输阶段通用）。服务的协议代码不自己分配缓冲区、不直接调 `snow`，统一走这里。
- `src/gnosis/server.rs`：`GnosisState`、`POST /gnosis`。校验顺序：Noise 握手 → 时间窗口 → nonce 防重放 → ACL → 读文件 `<api_keys_directory>/<key_id>.json`，原样发出。key id 是 `layline::api_keys::ApiKeyId` 枚举：请求里的未知 key id 解析失败，按畸形请求拒绝；`phanes.toml` 的 `allowed_key_ids` 写了未知 key id 则配置解析失败。启动时 `check_api_key_file` 逐个检查 `allowed_key_ids` 的文件：文件存在，且能解析成该 key id 对应的类型（`ApiKeyId::check_key_file`），否则拒绝启动。握手失败回空 404（推送 10 分钟节流）；认证之后的拒绝回加密的 `Denied{reason}`。
- `src/celestia/`：`proto.rs`（`ProcessSample`/`Batch`/`Ack`、`Initiator`/`respond`/`Session`）；`server.rs`（`GET /celestia`：握手 → 解密 → 打日志 → 回 ack；握手或解密失败断开并节流推送；90 秒没有消息就断开）。
- `src/service.rs`：`Service` 枚举（Gnosis / Celestia）和 `Language`；`to_string(Language)` 给出英文日志名或中文推送名。它是叶子模块，服务可以引用自己的名字而不必 import `server`，避免循环依赖。
- `src/server.rs`：`router()` / `run()`，把所有服务合并到一个 axum `Router`（`ClientFilters` 为 `None` 时不挂白名单；`/info` 始终不挂），以及 `GET /info`（JSON：status / server_time（UTC）/ version / build）。
- `src/client_filter.rs`：IP 白名单中间件 `filter_clients`，每个服务一个 `ClientFilter`；放行的请求在扩展里带上 `ClientName`，handler 用它打日志、发推送（没挂中间件时为 `unfiltered`）。
- `src/config.rs`：`PhanesConfig`，含 `[clients]`、`gnosis_service_config` 和 `celestia_service_config`（均必填）；`parse_gnosis_clients_from_toml()` / `parse_celestia_clients_from_toml()` 解析出 `IpAddr` → 名字。密钥文件经 `layline::secrets` 读取。
- `src/request_details.rs`：被拒请求的快照 `RequestDetails`（`RequestHead` 加载荷）；`Display` 输出一行完整日志，`summarize()` 输出推送摘要。
- `src/notify.rs`：推送事件（启动 / 发放 / 拒绝 / 未认证 / 白名单拦截 / 挂了）及其中文文案；后台任务经 `layline::bark::Notifier` 发送：release 构建走 Bark，缺 `[bark]` 就报错退出；debug 构建（本机开发）不需要 Bark，推送改为 `info!` 日志。未认证和白名单拦截的推送按 `Service` 区分，各自有独立的 `UnauthenticatedThrottle`。
- `src/bin/phanes.rs`（仓库根目录）：`phanes keygen`、`phanes serve`。serve 由 supervisor 包裹：任何 panic（panic hook）或致命错误 → 记日志 → 推送"天理陷入沉睡" → 退出码 1。
- 仓库根目录的 `examples/`（只给测试用的代码放这里，不放进 crate）：`fetch.rs` 模拟 Osaka 取 key；`bark_push.rs` 发一条 Bark 推送；`celestia/` 是简化版的 Osaka 进程监控（`client.rs` 内存 `Outbox`、重连补发、30 秒心跳；`sampler.rs` 读 `/proc` 和 `systemctl show`，拿不到网络数据时填 0）。celestia example 的测试经根 `Cargo.toml` 的 `test = true` 随 `cargo test --workspace` 运行。

## Celestia 简化版的现状

`examples/celestia/` 每秒采样一个进程发给 Phanes。没收到 ack 的数据在内存里最多缓存 1 小时，断线后指数退避重连（1 s → 60 s）并补发。

- `--systemd-unit`：CPU、内存、网络都有。网络按进程口径统计，需要该 unit 开 `IPAccounting=yes`，否则为 0。
- `--process-id`：网络为 0。两个参数必须且只能给一个。

RedmiBook 的日志里每秒一行 `Osaka #<sequence>: time .. ms, cpu ..%, memory .. MiB, received .. B/s, sent .. B/s`。

目前的简化：缓存在内存里，Osaka 端进程重启会丢掉未确认的数据；网络数据靠每秒调一次 `systemctl show`；Phanes 端只打日志、不去重。完整设计见 [`plan/celestia.md`](../plan/celestia.md)。

## Osaka 端如何使用 Gnosis

Osaka 上的 crate（如 `market_data_provider`）依赖 workspace 里的 `layline`，参考 `examples/fetch.rs`：

```rust
let request = Request::new("binance_demo_trading", get_current_timestamp_nanoseconds());
let (initiator, first_message) =
    Initiator::write_request(&client_private_key, &gnosis_public_key, &request)?;
// POST first_message 到 https://phanes.wenqing.dev/gnosis
let response = initiator.read_response(&second_message)?;   // Response::Granted / Response::Denied
```

## 本机开发

debug 构建（`cargo run --bin phanes -- serve --config <phanes.toml>`）不需要 `[bark]`，所有推送改为打 `info` 日志，也不挂 IP 白名单（`examples/` 直连、没有 `CF-Connecting-IP`），日志里的客户端名字显示为 `unfiltered`。release 构建（部署用）缺少 `[bark]` 会报错并拒绝启动。`phanes.toml` 含凭据，必须 `chmod 600`，否则拒绝启动。

测试 Bark 推送：`cargo run --example bark_push -- <phanes.toml> "message"`。
