# Osaka 监控（Celestia）计划

状态：简化版已实现：market_data_provider 和策略用 `layline::process_metrics` 采样、经 `layline::celestia::client` 上报（内存缓存、重连补发），Phanes 收到后只打日志。已实现部分见 [`architecture/phanes.md`](../architecture/phanes.md)「Celestia 简化版的现状」和 [`architecture/overview.md`](../architecture/overview.md)「进程监控」。实施进度见 §8。标 **【待定】** 的条目需要确认后再动手。

## 1. 目标

- 实时监控 Osaka 上**长期运行的业务程序**（不是本仓库的 example）：
  - 进程 CPU 使用量
  - 进程内存使用量
  - 网络接收、发送数据量（进程口径）
- 同时传输业务程序内部的可观测数据：每秒十几个数据点。
- RedmiBook 负责存储和看板：既能实时看，也能长期保留、查询历史。
- 看板经 RedmiBook 上的 cloudflared 发布到公网，手机也能看。
- Osaka 的**日志不传**。

## 2. 约束

- Osaka 很弱（1/8 OCPU、1G 内存），监控对它的额外负载要尽量接近零。CPU 和内存比磁盘紧张得多：50G 磁盘可以用来缓存。
- Osaka → RedmiBook 端到端加密。Cloudflare 会终止 TLS，所以必须在应用层加密。
- RedmiBook 常开（合盖也不休眠），但家用网络会断，Cloudflare Tunnel 也会重连。
- 两端都用 Rust + tokio。编码规范沿用 `CLAUDE.md`。

## 3. 已定决策

| # | 决策 | 说明 |
|---|---|---|
| D1 | **单独生成一对 X25519 密钥**给监控用，不复用 Gnosis 密钥 | API Key 通道和监控通道互不影响，一边泄露不波及另一边。prologue 也单独用 `celestia`。 |
| D2 | **断网时 Osaka 落盘缓存**，RedmiBook **确认收到**后才删除 | 只"发出去"不算数。要等 RedmiBook 回 ack（数据已持久化）才删，见 §5.3。 |
| D3 | 存储和看板**全部部署在 RedmiBook** | Osaka 只采集、缓存、发送。 |
| D4 | 实时 + 长期历史都要 | 存储用 ClickHouse，见 `plan/database.md`。 |
| D5 | 看板经 RedmiBook 的 cloudflared 发布到公网 | 必须加鉴权，见 §6.3。 |
| D6 | 日志不传 | 只传指标。 |
| D7 | 网络流量用**进程口径** | 只统计业务程序自己的收发字节，见 §5.1。 |
| D8 | Celestia 服务端**集成进 Phanes**，不单独做 binary | 和 Gnosis 跑在同一个 `phanes` 进程、同一个 axum 监听端口和 Cloudflare hostname 上，作为 Phanes 的一个模块。部署、systemd unit、Bark 通知都复用现有的。 |
| D9 | Gnosis 和 Celestia **缺一不可** | 任一服务的配置缺失或有误，Phanes 拒绝启动。 |

## 4. 总体架构

```text
Osaka (inside the business process)                              RedmiBook
+---------------------------------------------+                  +-------------------------------------+
| business code --record()--> bounded channel |                  | phanes process, Celestia module     |
| /proc sampler (1 s) -----------^            |                  |   Noise decrypt -> validate         |
|        |                                    |  WebSocket       |   -> write to ClickHouse            |
|        v                                    | (Noise frames)   |   ack only after the write succeeds |
| pack one batch per second                   | --Cloudflare-->  |               |                     |
|   -> append to the disk spool               |    Tunnel        |               v                     |
|        |                                    | <---- Ack -----  | ClickHouse (localhost)              |
| sender: read spool -> send -> await ack     |                  |               ^                     |
|   -> delete fully acked segments            |                  | Grafana (localhost)                 |
+---------------------------------------------+                  |               ^                     |
                                                                 | cloudflared + Cloudflare Access     |
                                                                 +---------------+---------------------+
                                                                                 | HTTPS
                                                                          phone / browser
```

## 5. Osaka 侧：`celestia-client` 库

嵌入业务程序，不单独起 agent 进程（省一个进程的内存）。

### 5.1 采集

- **CPU**：每秒读 `/proc/self/stat`（utime + stime），按差值算使用率。
- **内存**：每秒读 `/proc/self/statm` 的 RSS。
- **网络**（进程口径，D7）：netlink `sock_diag` 读本进程 TCP socket 的 `tcp_info` 字节数（不含两次采样之间开关的连接和 UDP），已实现，见 `architecture/overview.md`「进程监控」。不依赖 systemd，本地运行也有数。
- **内部数据**：业务代码调用 `record(name, value)`，写入有界 channel。channel 满了就丢弃，并累加 `celestia_dropped` 计数，**绝不阻塞业务逻辑**。
- 时间戳由 Osaka 打（毫秒），假设 Osaka 的 NTP 正常。

### 5.2 打包与编码

- 后台任务每秒把这一秒的所有点打成一个 `Batch { sequence, points }`。`sequence` 单调递增，并持久化到磁盘，程序重启后也不回退。
- 编码用 postcard（紧凑二进制）。估算每秒几百字节，一天约 40 MB 量级。

### 5.3 磁盘 spool（断网缓存）

- **统一走磁盘**：每批先追加写进 spool，发送任务再从 spool 读出来发。断网和正常两种情况走同一条代码路径，程序重启也不丢数据。
- spool 按段（segment）切分，比如每段一小时或 1 MB。某段里所有批次都被 ack 之后，整段删除。
- 写盘每秒一次，不逐条 fsync，磁盘 IO 可以忽略。
- **容量上限**：比如 10 GB（按上面的估算够存半年以上）。超限时丢最老的段，并通过一个指标上报。
- 断网恢复后：**实时数据优先**发送，积压数据在后台**限速补发**，避免 Osaka 的 CPU 和网络瞬间打满。
- RedmiBook 按 `sequence` 去重，重复补发也不会出问题。

### 5.4 容错原则

Celestia 出任何错（断线、磁盘满、编码失败）都只能**降级**：记日志、丢数据、上报计数。**不得 panic，不得拖垮业务程序**。这和 Phanes 的"挂了就退出"正好相反。

## 6. RedmiBook 侧

### 6.1 Phanes 的 Celestia 模块（D8）

- 集成在 `phanes` 进程里，和 Gnosis 共用 axum 监听地址（`127.0.0.1:8888`）和 hostname `phanes.wenqing.dev`。
- 配置：`phanes.toml` 的 `celestia_private_key`，每个客户端的 Celestia 公钥在它的 `[clients.<名字>]` 表里（`client_celestia_service_public_key`）；之后加 ClickHouse 地址等。私钥由部署脚本调 `phanes keygen` 生成。
- **故障隔离**：同一进程意味着 Celestia 出问题会波及 Gnosis。约定如下：
  - Noise 握手或解密失败是安全事件，断开并 Bark 推送（节流）。其他运行时错误（ClickHouse 写失败、断线、超时、载荷解码失败）只记日志、断开当前连接或不回 ack，由 Osaka 重发，不推送、不上报 supervisor。
  - panic 仍按现有规则处理：整个 Phanes 退出并推送"Phanes 挂了"，由 systemd 拉起。因此 Celestia 代码同样禁止 `unwrap()`，不能有可能 panic 的路径。
  - 配置缺失或有误时 Phanes 拒绝启动（D9）。
- 路由：`GET /celestia` 升级为 WebSocket，挂在已有的 Cloudflare Tunnel 后面。
- 流程：
  1. WebSocket 建立。
  2. 两条消息完成 `Noise_IK` 握手（D1 的新密钥，prologue `celestia`），Phanes 按第 1 条消息里的公钥认出客户端。握手失败或认不出客户端直接断开，Bark 推送节流沿用 Gnosis 的做法。
  3. 进入 transport 模式，每个 WS 帧是一个密文 `Batch`。
  4. 解密 → 校验 → 写入 ClickHouse → **写成功后**回加密的 `Ack { sequence }`。
- 心跳：双方每 30 秒发一次 ping，防止 Cloudflare 回收空闲连接。断线后 Osaka 指数退避重连。
- 一条连接对应一次握手，重连就重新握手，不需要时间窗口和 nonce 表。

### 6.2 存储与看板

- 存储：ClickHouse，表结构和容量管理见 `plan/database.md`。
- **Grafana** 做看板，用 ClickHouse 插件：实时自动刷新（比如 5 秒），历史任意选时间范围，跨周期对比用 Time shift。只监听 `127.0.0.1`。
- 两者都用 systemd 管理，由 `deploy/` 统一安装和卸载。

### 6.3 公网访问（手机查看）

- cloudflared 把 Grafana 发布到一个子域名。
- **前面必须挂 Cloudflare Access**（Zero Trust，个人免费额度够用），比如用邮箱 OTP，只允许你自己的邮箱访问。Grafana 自身的登录作为第二层。
- 注意：看板这条链路上 Cloudflare **能看到明文**（展示出来的图表数据）。端到端加密保护的是 Osaka → RedmiBook 的传输，不保护公网看板。监控数据不含密钥，这个代价可以接受，在此明确记录。【待定 Q5】
- `/celestia` 这条入口**不能**挂 Access 的交互式登录，否则 Osaka 连不上。它的安全性由 Noise 保证。

### 6.4 告警（Bark）

复用 `layline::bark::Notifier`（release 走 Bark，debug 打日志），推送事件和文案在 `crates/phanes/src/notify.rs`。只推安全事件：

- Noise 握手失败、解密失败：推送"未授权的天空岛监控连接"（节流 10 分钟）。
- 断线、没数据、补发都不推送。
- 阈值告警（CPU、内存）：【待定 Q4】

## 7. 代码组织

Teyvat 已经是 cargo workspace，共用代码在 `layline`（只有被两个及以上 crate 使用的代码才放进去）。

| 位置 | 内容 | 状态 |
|---|---|---|
| `layline::noise` | Noise 参数、握手、加解密，Gnosis 和 Celestia 共用 | 已有 |
| `layline::celestia` | Celestia 协议（`Batch`、`Ack`、握手、编解码），样本类型是 `layline::process_metrics::ProcessSample` | 已有 |
| `crates/phanes`（Celestia 模块） | 接收、写 ClickHouse、去重、告警 | 接收、解密、回 ack、安全事件推送已有；ClickHouse 和去重未做 |
| `layline::process_metrics`、`layline::celestia::client` | 采集、发送 | 已有（内存缓存）；spool 未做 |

Bark 和 `Notifier` 在 `layline::bark`。

## 8. 实施步骤（确认后执行）

1. **【已完成】** 改造为 workspace，把共用协议抽出来：Teyvat workspace，Noise 层和 Gnosis 协议在 `layline`。Gnosis 的行为不变，测试全过。
2. **【已完成】** 定义 Celestia 协议（消息类型、握手、编解码），加单元测试：`crates/layline/src/celestia/mod.rs`。
3. **【部分完成】** 在 `phanes` 里加 Celestia 模块：WS + Noise + ack、每个客户端的 Celestia 公钥、密钥由部署脚本用 openssl 生成并写进 `phanes.toml`，均已完成；写 ClickHouse、去重未做。
4. 写 `celestia-client`：采集、spool、发送、重连、限速补发。**【部分完成】** 采集是 `layline::process_metrics`（进程 runtime 上的 async 任务：CPU、内存分 anon/file/shmem、按线程的 CPU 和上下文切换、fd 数、TCP 收发，网络用 netlink `sock_diag` 进程口径，取代原计划的 systemd `IPAccounting`；间隔来自各服务配置的 `process_metrics_interval_seconds`）；发送是 `layline::celestia::client`（async 任务、单一内存 Outbox、断线期间照常收样本、重连补发；`[celestia_client]` 配置表，整台机器共用一把 Celestia 密钥，`Batch` 带 `process_name` 区分进程），已接入 market_data_provider 和 dummy_strategy。spool（磁盘缓存）和限速补发未做。
5. **【部分完成】** 写 `examples/`：Celestia 冒烟测试（`examples/celestia.rs`：监控自己并经 `layline::celestia::client` 上报）已完成；基于磁盘 spool 的断网补发测试随第 4 步一起做。
6. 部署：ClickHouse、Grafana、systemd unit，扩展部署脚本，配置 cloudflared 路由和 Cloudflare Access。
7. Grafana 看板（CPU、内存、网络、内部指标），配置以 JSON 形式入库。
8. **【已完成】** 告警：安全事件推送。
9. 每完成一步，把实现的部分写进 `architecture/phanes.md`，并更新本文和 `deploy/README.md`。

## 9. 待定问题

| # | 问题 | 推荐 |
|---|---|---|
| Q4 | 内部数据的形态：都是"名字 + 数值"吗？有没有直方图、标签、字符串事件？指标名是固定的一组还是会动态增加？是否需要阈值告警？ | 需要你给一份样例 |
| Q5 | 公网看板让 Cloudflare 能看到图表明文，能否接受？ | 接受（数据不含密钥），加 Cloudflare Access |
| Q6 | 内部数据每个点原样传，还是允许 Osaka 按秒聚合？ | 原样传（量很小） |
| Q7 | Osaka spool 上限多大？ | 10 GB |
