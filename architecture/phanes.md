# Phanes（总控）

天理法涅斯，量化系统的总控，部署在 **RedmiBook**。代码在 `crates/phanes`（库），二进制是 `src/bin/phanes.rs`。部署步骤见 [`deploy/README.md`](../deploy/README.md)，尚未实现的部分见 [`plan/celestia.md`](../plan/celestia.md) 和 [`plan/database.md`](../plan/database.md)。

一个进程、一个端口上同时运行两个服务：

- **Gnosis**（API Key 发放，寓意天理发放神之心）：Osaka 上的程序启动时经 Cloudflare Tunnel 来取 API Key。请求和回复都是 Noise `IK` 密文，Cloudflare 只能看到乱码。每次发放、拒绝、服务挂掉都通过 Bark 推送到手机。
- **Celestia**（进程监控，寓意天空岛俯瞰提瓦特众生）：Osaka 上的进程每隔一个监控间隔上报自己的 CPU、内存、线程、TCP 流量（`layline::process_metrics` 的样本），经 WebSocket + Noise `IK` 加密传输。**目前 Phanes 收到后只打日志并回 ack**，入库和看板还没做。

```text
Osaka --HTTPS--> Cloudflare --tunnel--> cloudflared --> 127.0.0.1:8888 Phanes (RedmiBook)
  +-------- Noise_IK end to end: Cloudflare sees only ciphertext --------+
```

| 接口 | 说明 |
|---|---|
| `POST /gnosis` | body 为 Noise 握手消息 1（含客户端公钥和加密的 `{gnosis_id, timestamp, nonce}`），返回消息 2（含加密的 gnosis 或拒绝原因）。握手失败、公钥不属于任何客户端，返回空 404。 |
| `GET /celestia` | 升级为 WebSocket。前两帧是 Noise 握手（第 1 帧带进程名），之后每帧是一个加密的 `Batch`，Phanes 回加密的 `Ack`。握手或解密失败直接断开并推送。 |
| `GET /info` | 返回 JSON `{status, server_time, version, build, git_commit_hash}`（`server_time` 为 UTC 时间、精确到毫秒，如 `2026-10-15 13:30:05.123 UTC`），供 Osaka 探活。不做 IP 过滤，任何人可访问。 |

## 要求

- Osaka ↔ RedmiBook 的通讯**端到端加密**：请求和回复都是密文。Cloudflare 会终止 TLS、能看到 HTTP 明文，因此必须在应用层加密，不能依赖 TLS。
- 双方已各自持有对方的 SSH 公钥（ed25519）。
- 两端都用 Rust（tokio）。
- 暂不考虑 RedmiBook 本机的安全：gnosis（API Key）以明文写在 `phanes.toml` 的 `[gnoses]` 里（文件权限 600）。仓库里只有 `deploy/phanes/phanes.example.toml` 的占位符，**任何真实密钥都不得进入 git、日志或示例代码**。

## 已定方案

- **Noise `IK`**：`Noise_IK_25519_ChaChaPoly_BLAKE2s`（`snow` crate）。客户端事先知道服务的公钥，并在消息 1 里加密带上自己的公钥；Phanes 读完消息 1 才知道对方是谁，再拿这个公钥去查 `[clients]`。只有持有对应私钥的一方才写得出这条消息，所以公钥就是客户端的身份。查不到的公钥按未认证处理，消息 1 的载荷不会被使用。选 `IK` 而不是 `KK`：`KK` 要求服务事先知道对方是谁，多个客户端时只能先靠 IP 猜公钥（debug 构建没有 IP）或逐个试；`IK` 是"一个服务端、多个客户端"的标准做法。
- **Gnosis 协议**：prologue `gnosis`，一次 HTTP POST 完成握手加请求/回复。消息 1 的载荷是 `{gnosis_id, timestamp, nonce}`，消息 2 的载荷是 `Response<Key>`：`Granted { gnosis }` 直接带 key 本身（如 `{"api_key": "..."}`），客户端按它要的 key 类型解析（`deny_unknown_fields`，字段不符就报错），或 `Denied { reason }`。虽然业务上是"取" key，但用 POST 而不是 GET：握手消息只能放在 body 里（GET 的 body 没有标准语义，Cloudflare 等中间层可能丢弃；放进 URL 又会让密文进入各级日志）；请求有副作用、不幂等（nonce 记入防重放表，每次都触发 Bark 推送）；GET 的响应还可能被 CDN 缓存。
- **Celestia 协议**：Noise 参数相同，prologue `celestia`，`GET /celestia` 升级为 WebSocket。每个上报进程一条连接。前两帧是握手：第 1 帧的载荷是 postcard 编码的 `Hello{process_name}`，第 2 帧载荷为空；Phanes 收到第 1 帧、先用公钥认出客户端，才解码 `Hello`（`Responder::finish`，认不出的客户端的载荷不会被使用）并回第 2 帧，所以连上的那一刻就知道是哪台机器上的哪个进程；已知客户端发来解不开的 `Hello` 只 warn 并断开，不推送。之后每帧是一个 postcard 编码的密文：客户端 → Phanes 为 `Batch{sequence, samples}`（`samples` 是 `layline::process_metrics::ProcessSample`），Phanes → 客户端为 `Ack{sequence}`。一条连接一次握手，不需要时间窗口和 nonce。虽然数据是客户端发给 Phanes，但必须用 GET：WebSocket 标准（RFC 6455）规定升级握手只能是带 `Upgrade: websocket` 头的 GET；升级之后不再有 HTTP 方法，双方的数据都走同一条连接上的帧。
- **以客户端为单位授权**：每个客户端一张 `[clients.<名字>]` 表：`ipv4` 必填、`ipv6` 可选、`client_gnosis_service_public_key`、`client_celestia_service_public_key`、`allowed_gnosis_ids`（可为空，只用 Celestia 的客户端就是空）。任何客户端都可以访问任何服务；Gnosis 认出客户端之后，只发放它 `allowed_gnosis_ids` 里的 gnosis，其余回 `Denied`。以后加客户端（Tokyo、Osaka2）只需加一张表。
- **gnosis 写在 `phanes.toml` 里**：顶层 `[gnoses.<gnosis_id>]`，每张表的字段就是该 id 的 key 类型（`binance_{real,demo}` 为 `api_key` + `secret_key`，`binance_sbe_{real,demo}` 只有 `api_key`）。解析配置时 serde 就会拒绝未知 id、缺字段、多字段和类型不符的 key，不需要手写检查。Phanes 可以持有暂时没有客户端被允许的 gnosis；某个 `allowed_gnosis_ids` 引用了没有表的 id，拒绝启动。
- **密钥**：每个服务在两端各有一对专用的 X25519 密钥（不复用 SSH 密钥，服务之间也不复用），公钥经现有的 SSH 通道交换。公钥泄露本身无害，但不应让白名单之外的人知道，所以代码里公钥和私钥一样放在 `SecretBox<StaticKey>` 里：`Debug` 脱敏、drop 时清零、不打进日志。同一个服务里两个客户端的公钥不能相同（否则分不出是谁），否则拒绝启动。
- **Gnosis 和 Celestia 缺一不可**：任一服务的私钥缺失或有误（密钥无效、权限不是 600），Phanes 都拒绝启动。
- **IP 白名单**（只在 release 构建挂载）：全局一张表，`[clients]` 里任何客户端的地址都能访问所有服务，`GET /info` 不过滤。白名单只挂在服务的路由上（`route_layer`）；未知路径回 404 并记完整请求日志，不推送。客户端 IP 只取 `CF-Connecting-IP`（不用 `X-Forwarded-For`）；头缺失、解析失败或不属于任何客户端，一律回空 404 并 Bark 推送（10 分钟节流）。放行的请求带上 IP 所属的客户端名；服务认出公钥之后，还要求两者是同一个客户端，否则按未认证处理（拿着 Osaka 的私钥从 Tokyo 的 IP 发来，也会被拒）。被拦截和未认证的请求，日志记录全部 header 和载荷（body 或出错的 WebSocket 帧）前 4096 字节的 hex；推送放不下这些，只带方法、路径、`User-Agent` / `Upgrade` / `CF-Ray` / `CF-IPCountry` / `CF-Worker` 和载荷长度；未认证的推送和日志另带拒绝原因，以及 IP 所属的客户端名（只凭 IP，未经密钥验证：Noise 解密失败时根本拿不到客户端公钥）。`listen_address` 必须是回环地址。IP 无效、同一 IP 出现在两个客户端下，都拒绝启动。Cloudflare 面板另有一层 WAF 规则。
- **未认证**：握手失败、公钥不属于任何客户端、公钥和 IP 属于不同客户端，都回空 404（Celestia 为断开连接）并 Bark 推送（10 分钟节流）。公钥也按秘密处理（见"密钥"），日志里不写出未知的公钥。
- **Celestia 的安全事件**：上面的未认证情况，以及某帧解密失败（伪造帧），断开连接并 Bark 推送（10 分钟节流）。其他运行时错误（断线、超时、载荷格式错）只断开当前连接、记日志，不推送，由客户端重连补发。
- 客户端只在启动时取一次 key，之后常驻内存。
- 每次发放或拒绝 gnosis，都通过 Bark 加密推送（`layline::bark`，AES-256-CBC）通知手机。Bark 凭据放在 `phanes.toml` 的 `[bark]` 表（`device_key` / `encryption_key`，可选 `server`）。所有配置只从 `phanes.toml` 读，该文件必须是 600 权限，不入库；解析错误只报 toml 的错误信息、key 路径和行号，不引用原文，所以字段名写错也不会把 gnosis 打进日志。
- 每个服务用自己的密钥对和命名（Gnosis：`phanes.toml` 的 `gnosis_private_key`；Celestia：`celestia_private_key`），不用泛泛的 `server_*`。私钥和其他配置一样直接写在 `phanes.toml` 里（base64），不在配置里放指向其他文件的路径；部署脚本首次安装时用 openssl 生成，直接填进配置。公钥不存成文件：它可以由私钥推出，部署脚本需要打印时现推。

## 代码结构

- 每个服务一个目录。Gnosis 协议要和客户端共用，所以两半（`Request`/`Response`、`Initiator`/`Responder`）以及所有 gnosis 的 id 和 key 类型都在 `layline::gnosis`（`mod.rs` 放协议、`GnosisId` 和 `Gnosis` trait，每个交易所一个文件放它的 key 类型，如 `binance.rs`），这里只有服务端。Celestia 同理：协议和上报端都在 `layline::celestia`（`mod.rs` 是协议，`client.rs` 是各进程用的上报端），这里只有服务端。
- `layline::noise`：所有服务共用的 Noise 层：参数、密钥编解码、`decode_key`（私钥和公钥同一个解码函数，都得到 `SecretBox<StaticKey>`）、`build_initiator(prologue, ..)` / `build_responder(prologue, ..)`、`read_first_message`（服务端读消息 1，得到载荷和客户端公钥；Noise 只在本地用私钥做 DH，消息里只有加密的公钥）、`encrypt`/`decrypt`（握手和传输阶段通用）。服务的协议代码不自己分配缓冲区、不直接调 `snow`，统一走这里。
- `src/clients.rs`：`ClientName`（`String` 的类型别名）、`ClientConfig`（一张 `[clients.<名字>]` 表）和 `Clients`（全部客户端，`Arc` 共享给 IP 白名单和各服务，只有这一份，不派生其他表）。`identify(clients, 服务, 公钥, IP 所属的客户端)` 在其中找出持有该服务公钥的客户端（客户端只有几个，逐个比较），并检查 IP 属于同一个客户端（debug 构建没有 IP，只看公钥）。它是叶子模块。
- `src/gnosis/config.rs`：`Gnoses`（`[gnoses]` 表，每个 `GnosisId` 一个同名字段，类型为该 id 的 key 类型；穷尽的 `match` 保证新增 id 时必须加字段，测试保证字段名和 id 拼写一致）和 `AnyGnosis`（任意一种 key，序列化成 key 本身）。
- `src/gnosis/server.rs`：`GnosisState`、`POST /gnosis`。校验顺序：Noise 握手 → 按公钥认出客户端（并核对 IP）→ 时间窗口 → nonce 防重放 → 该客户端的 `allowed_gnosis_ids` 里有这个 id，再从 `Gnoses` 取出（`load` 已保证有表）。gnosis id 是 `layline::gnosis::GnosisId` 枚举：请求里的未知 id 解析失败，按畸形请求拒绝。握手失败或认不出客户端回空 404（推送 10 分钟节流）；认出之后的拒绝回加密的 `Denied{reason}`。加密回复（Gnosis 的消息 2、Celestia 的消息 2 和 ack）对正确的代码不会失败，所以用 `expect`：真失败了就是 bug，panic 经 panic hook 让 Phanes 推送崩溃并退出。
- `src/celestia/`：`server.rs`（协议的 `Hello`/`Batch`/`Ack`、`Responder`（`read_first_message` → `client_public_key` → `finish` 得到 `Accepted{session, hello, second_message}`）、`Session` 来自 `layline::celestia`；`GET /celestia`：握手认出客户端和进程（`Reporter`，日志里写作 `<客户端>/<进程名>`，连上、断开、出错和每个样本都用它）→ 推送"已连接至天空岛"（不节流）→ 解密 → 打日志 → 回 ack；连接结束时，客户端发来 Close 帧的推送"魔神断绝了与天空岛的连接"，断线、超时、出错的推送"天空岛监控丢失"，安全事件则先推安全事件再推它；同一个 `Reporter` 在旧连接还没结束时连上新连接（旧连接的链路已经悄悄断了），新连接取代旧连接：旧连接立即结束，只推一条"已重新连接至天空岛"，不再推"已连接"和旧连接的"丢失"；认不出或解密失败断开并节流推送；90 秒没有消息就断开）。
- `src/service.rs`：`Service` 枚举（Gnosis / Celestia）和 `Language`；`to_string(Language)` 给出英文日志名或中文推送名。它是叶子模块，服务可以引用自己的名字而不必 import `server`，避免循环依赖。
- `src/server.rs`：`router()` / `run()`，把所有服务合并到一个 axum `Router`，并在服务路由上挂唯一的 `ClientFilter`（为 `None` 时不挂；`/info` 始终不挂），以及 `GET /info`（JSON：status / server_time（UTC）/ version / build / git_commit_hash）。
- `src/client_filter.rs`：IP 白名单中间件 `filter_clients`，全局一个 `ClientFilter`；放行的请求在扩展里带上 IP 所属的 `ClientName`，handler 用 `IpClientName` 取出（没挂中间件时为 `None`）。
- `src/config.rs`：`PhanesConfig`，Rust 字段名和 TOML 完全一致：`[clients.<名字>]`（`ClientConfig`，公钥在解析时就解码，无效即解析失败）和 `[gnoses]`。`load` 解析完立刻检查：IP 不重复、所有公钥（跨客户端、跨服务）不重复、允许的 gnosis 都有表；任何一项不过，Phanes 拒绝启动，之后的服务过程都假设配置合法。配置和私钥文件经 `layline::secrets` 读取。
- `src/request_details.rs`：被拒请求的快照 `RequestDetails`（`RequestHead` 加载荷）；`Display` 输出跟在日志消息后面的多行完整内容（4 空格缩进；header 值用 `Debug` 打印，带引号并转义非 ASCII 字节，伪造不出单独一行；载荷 hex 每行 32 字节），`summarize()` 输出推送摘要。
- `src/notify.rs`：推送事件（启动 / 发放 / 拒绝 / 未认证 / 白名单拦截 / 挂了 / Celestia 连上 / 重连 / 关闭 / 丢失）及其中文文案；后台任务经 `layline::bark::Notifier` 发送：release 构建走 Bark，缺 `[bark]` 就报错退出；debug 构建（本机开发）不需要 Bark，推送改为 `info!` 日志。`enqueue` 把推送放进队列，发送端已关闭时只记一条 error；`ThrottledNotificationSender` 在它前面加一个 `UnauthenticatedThrottle`。未认证的推送按 `Service` 区分，各自有独立的 `ThrottledNotificationSender`；白名单拦截不分服务，一个。
- `src/bin/phanes.rs`（仓库根目录）：`phanes keygen` 向终端打印一对新的 base64 密钥（不写文件，也不进日志，私钥只应填进它主人的配置），用于新客户端或换密钥；`phanes serve --config <phanes.toml>` 加载配置后启动进程监控（`layline::process_metrics`，间隔为 `process_metrics_interval_seconds`，见 [`overview.md`](overview.md)「进程监控」），整个服务由 supervisor 包裹：任何 panic（panic hook）或致命错误 → 记日志 → 推送"天理陷入沉睡" → 退出码 1。
- 仓库根目录的 `examples/`（只给测试用的代码放这里，不放进 crate）：`fetch.rs` 模拟客户端取 gnosis（按任意 id 取，所以把 gnosis 当 JSON 打出来）；`bark_push.rs` 发一条 Bark 推送；`celestia.rs` 是 Celestia 的冒烟测试：一个监控自己、并经 `layline::celestia::client` 上报给 Phanes 的进程，和服务的做法相同，不部署任何服务就能验证到 Phanes 的整条链路。

## Celestia 简化版的现状

market_data_provider 和每个策略都把 `layline::process_metrics` 的样本经 `layline::celestia::client` 上报给 Phanes（Phanes 自己只打本地日志），间隔是各自配置里的 `process_metrics_interval_seconds`：

- **配置**：两者的配置里有同样的 `[celestia_client]` 表（`CelestiaClientConfig`：`url`、`client_celestia_private_key`、`phanes_celestia_public_key`）。私钥是**机器**的：market_data_provider 的安装生成它，并打印公钥给这台机器的 `[clients.<名字>].client_celestia_service_public_key`；策略的安装每次都从 market_data_provider 的配置把这张表的设置拷过来。
- **上报端**（`run_celestia_reporter`）：和监控一样是 async 任务，放在进程不在热路径上的 runtime 上（见 [`overview.md`](overview.md)「线程模型」）。`ReporterConfig::new` 在启动时把 URL 换成 `/celestia` 的 WebSocket 地址，scheme 不对就拒绝启动。监控任务经 `run_process_monitor` 把样本 `try_send` 进一个容量为 1 的 channel；上报端**不论连上与否都立即取走**（连接、握手和退避期间也一样，靠 `take_samples_while` 和它们一起 `select!`），放进唯一的缓冲 `Outbox`，每个样本一个 `Batch`，收到 ack 才删除。`Outbox` 最多存一小时，满了丢最老的。断线后按 `layline::backoff::exponential_backoff` 重连（1 s → 60 s），重连后补发 `Outbox` 里的全部。上报出任何错都只 warn 并重连，不让进程退出。`run_celestia_reporter` 的最后一个参数是关停信号（进程传入 `layline::shutdown::shutdown_signal()`，即 SIGINT 或 SIGTERM）：信号到达时，已连上就等未确认的 ack（最多 5 秒），发出 reason 为信号名的 Close 帧，再等 Phanes 回 Close（最多 5 秒），然后返回；没连上就直接返回。所有样本发送端都已关闭（监控任务结束）时同样排空后关闭，Close 帧的 reason 是 `no more samples to report`，所以上报端发出的 Close 总带 reason。market_data_provider 和 dummy_strategy 在它返回后 flush 日志并以 0 退出，所以 Ctrl-C 和 `systemctl stop` 在手机上显示为"断绝"，和事故区分开。
- **进程身份**：Noise 密钥认证的是机器，同一台机器上的进程共用它；握手第 1 帧 `Hello` 里的 `process_name`（来自 `ReporterConfig`）只用来区分它们：market_data_provider 是 `market_data_provider`，策略是它的 `strategy_name`。
- **Phanes 端**：连上和正常关闭各打一行 `Celestia reporter Osaka/market_data_provider connected` / `disconnected: SIGINT`，被取代时打 `reconnected, replacing its old connection`；推送正文都有 `魔神: <客户端>` 和 `服务: <进程名>` 两行：连上推"已连接至天空岛"；客户端发 Close 推"魔神断绝了与天空岛的连接"，加一行 `原因: <Close 帧的 reason>`（如 `SIGINT`、`SIGTERM`，没给 reason 为"客户端未说明"）；断线、超时、出错推"天空岛监控丢失"，加一行连接出错的原文，如 `原因: no message within 90s`；新连接取代旧连接时只推"已重新连接至天空岛"，加一行 `旧连接最后一条消息: <N> 秒前`，即中断了多久。登记表（`CelestiaState::connections`）和判定都在锁里，所以旧连接恰好自己结束的同时新连接到来，推的也是重连；握手没完成的连接什么都不推；每个样本打两行，和本地的格式相同，前面加上 `<客户端>/<进程名> #<sequence>`；第一行还有样本的 `timestamp_seconds`，区分补发的旧样本和新样本：

```text
Osaka/market_data_provider #1794080694544 taken at 1794080757: process metrics: cpu 0.6%, memory anon 6.8 MiB / ... , sampled in 2155 µs
Osaka/market_data_provider #1794080694544: thread metrics (cpu, voluntary/involuntary context switches per second): market_data_pro#45988 0.2% 1.3/0.0; ...
```

`examples/celestia.rs` 是同一套上报的冒烟测试。目前的简化：缓存在内存里，进程重启会丢掉未确认的数据；Phanes 端只打日志、不去重、不入库。完整设计见 [`plan/celestia.md`](../plan/celestia.md)。

## 客户端如何使用 Gnosis

客户端上的 crate（如 `market_data_provider`）依赖 workspace 里的 `layline`，按 key 类型取 gnosis，类型决定 id（`Gnosis::gnosis_id(demo)`），参考 `market_data_provider/src/gnosis_client.rs` 和 `examples/fetch.rs`：

```rust
let request = Request::new(BinanceSbeApiKey::gnosis_id(demo), get_current_timestamp_nanoseconds());
let (initiator, first_message) =
    Initiator::write_request(&client_private_key, &gnosis_public_key, &request)?;
// POST first_message 到 https://phanes.wenqing.dev/gnosis
let response = initiator.read_response::<BinanceSbeApiKey>(&second_message)?;   // Response::Granted { gnosis } / Response::Denied { reason }
```

## 本机开发

debug 构建（`cargo run --bin phanes -- serve --config <phanes.toml>`）不需要 `[bark]`，所有推送改为打 `info` 日志，也不挂 IP 白名单（`examples/` 直连、没有 `CF-Connecting-IP`），客户端只按公钥识别。release 构建（部署用）缺少 `[bark]` 会报错并拒绝启动。`phanes.toml` 含凭据，必须 `chmod 600`，否则拒绝启动。

测试 Bark 推送：`cargo run --example bark_push -- <phanes.toml> "message"`。
