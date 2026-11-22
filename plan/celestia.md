# Osaka 监控（Celestia）计划

状态：简化版已实现——market_data_provider 和策略经 `layline::process_metrics` 采样、`layline::celestia::client` 上报（内存 Outbox、重连补发），Phanes 收到后只打日志并回 ack；`examples/celestia.rs` 是同一套上报的冒烟测试。已实现部分见 [`architecture/phanes.md`](../architecture/phanes.md)「Celestia 简化版的现状」和 [`architecture/overview.md`](../architecture/overview.md)「进程监控」，本文只写未实现的部分：**磁盘 spool 与限速补发（§4.3）、ClickHouse 入库与按 `sequence` 去重（§5.1）、Grafana 看板与部署（§5.2）**。标 **【待定】** 的条目需要确认后再动手。

## 1. 目标与约束

**目标**：实时监控 Osaka 上**长期运行的业务程序**（不是本仓库的 example）的进程 CPU、内存、网络收发（进程口径），同时传输业务程序内部的可观测数据（每秒十几个数据点）；RedmiBook 负责存储和看板，既能实时看也能长期保留、查询历史，看板经 RedmiBook 上的 cloudflared 发布到公网，手机也能看；Osaka 的**日志不传**。

**约束**：Osaka 很弱（1/8 OCPU、1 GB 内存），监控对它的额外负载要尽量接近零；CPU 和内存比磁盘紧张得多，50 GB 磁盘可以用来缓存。Osaka → RedmiBook 端到端加密：Cloudflare 会终止 TLS，必须在应用层加密。RedmiBook 常开（合盖也不休眠），但家用网络会断，Cloudflare Tunnel 也会重连。两端都用 Rust + tokio，编码规范沿用 `AGENTS.md`。

## 2. 已定决策

| # | 决策 | 说明 |
|---|---|---|
| D1 | **单独生成一对 X25519 密钥**给监控用，不复用 Gnosis 密钥 | API Key 通道和监控通道互不影响，一边泄露不波及另一边。prologue 也单独用 `celestia`。 |
| D2 | **断网时 Osaka 落盘缓存**，RedmiBook **确认收到**后才删除 | 只"发出去"不算数，要等 RedmiBook 回 ack（数据已持久化）才删，见 §4.3。 |
| D3 | 存储和看板**全部部署在 RedmiBook** | Osaka 只采集、缓存、发送。 |
| D4 | 实时 + 长期历史都要 | 存储用 ClickHouse，见 [`database.md`](database.md)。 |
| D5 | 看板经 RedmiBook 的 cloudflared 发布到公网 | 必须加鉴权，见 §5.2。 |
| D6 | 日志不传 | 只传指标。 |
| D7 | 网络流量用**进程口径** | 只统计业务程序自己的收发字节，见 §4.1。 |
| D8 | Celestia 服务端**集成进 Phanes**，不单独做 binary | 和 Gnosis 同一个进程、同一个 axum 端口和 Cloudflare hostname，部署、systemd unit、Bark 都复用现有的。 |
| D9 | Gnosis 和 Celestia **缺一不可** | 任一服务的配置缺失或有误，Phanes 拒绝启动。 |

## 3. 总体架构

```text
Osaka（业务进程内）: business code --record()--> channel --+
                    /proc sampler (1 s) -------------------+-> 每秒打一个 Batch -> 磁盘 spool
                    sender: 读 spool -> 发送 -> 等 ack -> 删掉已确认的段
                        |  WebSocket，每帧一个 Noise 密文，经 Cloudflare Tunnel
                        v
RedmiBook: phanes（解密 -> 校验 -> 写 ClickHouse -> 写成功才回 Ack）
           ClickHouse + Grafana（只监听 127.0.0.1），cloudflared + Cloudflare Access -> 手机
```

## 4. Osaka 侧：`celestia-client` 库（嵌入业务程序，不单独起 agent 进程，省一个进程的内存）

### 4.1 采集

- **内部数据**（未实现）：业务代码调用 `record(name, value)`，写入有界 channel。channel 满了就丢弃，并累加 `celestia_dropped` 计数，**绝不阻塞业务逻辑**。
- 时间戳由 Osaka 打（毫秒），假设 Osaka 的 NTP 正常。
- 进程 CPU、内存和 TCP 收发已实现（`layline::process_metrics`，进程口径、不依赖 systemd），见 [`architecture/overview.md`](../architecture/overview.md)「进程监控」。

### 4.2 打包与编码

- 后台任务每秒把这一秒的所有点打成一个 `Batch { sequence, points }`；`sequence` 单调递增并持久化到磁盘，程序重启后也不回退。编码用 postcard（紧凑二进制），估算每秒几百字节，一天约 40 MB 量级。

### 4.3 磁盘 spool（断网缓存）【待定】

- **统一走磁盘**：每批先追加写进 spool，发送任务再从 spool 读出来发。断网和正常两种情况走同一条代码路径，程序重启也不丢数据。现在的实现只有内存里的单一 `Outbox`（最多存一小时），重启即丢。
- spool 按段（segment）切分，比如每段一小时或 1 MB。某段里所有批次都被 ack 之后，整段删除。写盘每秒一次，不逐条 fsync，磁盘 IO 可以忽略。
- **容量上限**：比如 10 GB（按上面的估算够存半年以上）。超限时丢最老的段，并通过一个指标上报。
- 断网恢复后：**实时数据优先**发送，积压数据在后台**限速补发**，避免 Osaka 的 CPU 和网络瞬间打满；RedmiBook 按 `sequence` 去重，重复补发也不会出问题。

### 4.4 容错原则

Celestia 出任何错（断线、磁盘满、编码失败）都只能**降级**：记日志、丢数据、上报计数。**不得 panic，不得拖垮业务程序**。这和 Phanes 的"挂了就退出"正好相反。

## 5. RedmiBook 侧

### 5.1 Phanes 的 Celestia 模块

接收、解密、回 ack、安全事件推送、客户端和进程识别均已实现（见 [`architecture/phanes.md`](../architecture/phanes.md)），剩余：

- **写入即确认**：解密 → 校验 → 写入 ClickHouse → **写成功后**才回加密的 `Ack { sequence }`；写失败不推送、不回 ack。`phanes.toml` 到时候要加 ClickHouse 地址等配置。
- **去重**：按 `sequence` 去重，重复补发不产生重复点。
- **心跳**：双方每 30 秒发一次 ping，防止 Cloudflare 回收空闲连接（现在靠 90 秒无消息断开加客户端重连）。
- **故障隔离**：同一进程意味着 Celestia 出问题会波及 Gnosis。约定：Noise 握手或解密失败是安全事件，断开并 Bark 推送（10 分钟节流）；其他运行时错误（ClickHouse 写失败、断线、超时、载荷解码失败）只记日志、断开当前连接或不回 ack，由 Osaka 重发，不推送、不上报 supervisor。panic 仍让整个 Phanes 退出并推送、由 systemd 拉起，因此 Celestia 代码同样禁止 `unwrap()`。
- **告警**：复用 `layline::bark::Notifier`（release 走 Bark，debug 打日志），只推安全事件（已实现）：Noise 握手失败、解密失败推送"未授权的天空岛监控连接"并节流 10 分钟；断线、没数据、补发都不推送。阈值告警（CPU、内存）：【待定 Q4】。

### 5.2 存储、看板与公网访问

- 存储：ClickHouse，表结构和容量管理见 [`database.md`](database.md)。**Grafana** 做看板，用官方 ClickHouse 插件：实时自动刷新（比如 5 秒），历史任意选时间范围，跨周期对比用 Time shift，只监听 `127.0.0.1`。两者都用 systemd 管理，由 `deploy/` 统一安装和卸载。
- cloudflared 把 Grafana 发布到一个子域名，**前面必须挂 Cloudflare Access**（Zero Trust，个人免费额度够用），比如用邮箱 OTP，只允许你自己的邮箱访问；Grafana 自身的登录作为第二层。
- 注意：看板这条链路上 Cloudflare **能看到明文**（展示出来的图表数据）。端到端加密保护的是 Osaka → RedmiBook 的传输，不保护公网看板；监控数据不含密钥，这个代价可以接受，在此明确记录。【待定 Q5】另外，`/celestia` 这条入口**不能**挂 Access 的交互式登录，否则 Osaka 连不上，它的安全性由 Noise 保证。

## 6. 待定问题

| # | 问题 | 推荐 |
|---|---|---|
| Q4 | 内部数据的形态：都是"名字 + 数值"吗？有没有直方图、标签、字符串事件？指标名是固定的一组还是会动态增加？是否需要阈值告警？ | 需要你给一份样例 |
| Q5 | 公网看板让 Cloudflare 能看到图表明文，能否接受？ | 接受（数据不含密钥），加 Cloudflare Access |
| Q6 | 内部数据每个点原样传，还是允许 Osaka 按秒聚合？ | 原样传（量很小） |
| Q7 | Osaka spool 上限多大？ | 10 GB |
