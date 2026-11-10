# 延迟计划

状态：本文的提速方向均未实现，各项动手前需要先讨论，标 **【待定】** 的需要先确认。依据是 2026-11-10 在本地开发机、Osaka、Tokyo 三处同时运行的实测。延迟的测量方式见 [`architecture/dummy_strategy.md`](../architecture/dummy_strategy.md)，各段的口径见 [`architecture/market_data_provider.md`](../architecture/market_data_provider.md)「时间戳」：

- **provider**：行情服务读到交易所消息，到写入 ring 之前
- **transport**：序列化、写进 ring，直到策略读到
- **parse**：策略读到，到处理完
- **internal** = provider + transport

## 1. 实测对比（2026-11-10）

### 条件

| | 本地开发机 | Osaka | Tokyo |
|---|---|---|---|
| CPU | Intel Core Ultra 5 235U，WSL2（Hyper-V） | AMD EPYC 7551，2 vCPU（SMT），KVM，1/8 OCPU | Neoverse-N1，4 核，无 SMT |
| steal（`vmstat` 的 `st`） | — | 28–31% | 0% |
| 代码 | 当天未提交的版本（改动不在热路径） | d910664 | 797bd87（与 d910664 只差文档和 `scp_to.sh`） |
| 时段（北京时间） | 02:54–07:22 | 01:49–12:19 | 01:48–12:18 |
| 统计窗口 | 26 个 × 600 s | 21 个 × 1800 s | 21 个 × 1800 s |
| 消息速率 | 31 条/s | 30.7 条/s | 30.7 条/s |

三处都是 demo 环境，订阅同样 8 个 topic：Binance BTC、ETH 的现货和永续，各自的 BBO 和 Trade。远端只取和本地重叠的时段，数字几乎不变。本地窗口较短，所以它的 p99.9 和 max 天然偏小，p50 到 p99 不受影响。

### 延迟（µs，各窗口分位数的中位数，p50 / p90 / p99 / p99.9）

| | 本地 | Osaka | Tokyo 预热期（前 4 个窗口） | Tokyo 稳态（05:48 起的 14 个窗口） |
|---|---|---|---|---|
| transport | 6.8 / 12.0 / 73 / 216 | 3.9 / **14 798** / **45 233** / 51 773 | 1.0 / 2.7 / 28 / 36 | 1.1 / 2.5 / 3.8 / 15 |
| provider | 2.9 / 6.4 / 14 / 55 | 2.1 / 6.8 / 22 / 65 | 1.4 / 4.3 / 15 / 36 | 1.5 / 4.3 / 8.7 / 23 |
| internal | 9.7 / 18 / 88 / 259 | 6.2 / 14 804 / 45 256 / 51 887 | 2.5 / 7.0 / 37 / 70 | 2.6 / 6.8 / 12 / 38 |
| parse | 0.64 / 3.05 / 4.96 / 26 | 0.48 / 2.17 / 2.81 / 17 | 0.12 / 0.22 / 0.52 / 0.94 | 0.16 / 0.36 / 0.68 / 1.04 |
| transport 每窗口 max | 375–10 891 | 60 843–78 488 | 138–1 170 | 49–3 276 |

### 资源

| | 本地 | Osaka | Tokyo |
|---|---|---|---|
| 策略被动上下文切换 | 1.5–2.8 次/s | 11.6 次/s | 7 次/s |
| 行情服务 CPU | 0.2–0.3% | 0.4% | 0.2% |
| anon 内存（行情服务 / 策略） | 6.1–6.8 / 5.0–5.6 MiB | 6.3–6.8 / 7.2–8.3 MiB | 19.4 / 14.3 MiB |

三处的策略主线程都在忙轮询，CPU 都是 100%。三处内存整夜都没有增长。Tokyo 的 anon 内存是另外两处的 2–3 倍，原因没查，已经排除页大小（三处都是 4 KiB）。

## 2. 结论

1. **Tokyo 全面最快，数据支持把生产放在 Tokyo。** 稳态下 internal p99 是 12 µs，本地是 88 µs，Osaka 是 45 ms；parse 比两台 x86 机器快 4–8 倍。原因是独占物理核、没有 SMT、没有 steal。
2. **Osaka 只能验证功能是否正确，不能测延迟。** p50 正常，p90 起是 12–20 ms。st 约 30%、us 约 24%，说明忙轮询的策略只拿到大约半个 vCPU，其余时间整段被宿主机收回。被收回期间到达的消息要等十几到几十毫秒。进程指标显示的 99.9% CPU 是虚拟机内看到的数字，看不出 steal。
3. **本地 WSL2 的数字只能用于版本之间对比。** 这与 `architecture/market_data_provider.md`「基准」的说法一致。本地整夜都还在 ring 的预热期（见第 4 条），所以它的 p99 应该和 Tokyo 预热期比（73 对 28 µs），而不是和稳态比。
4. **ring 写满一圈之前，transport p99 高约 7 倍。** Tokyo 刚启动的约 3 小时里，transport p99 是 26–31 µs，之后降到 3.4–4.1 µs。按 topic 拆开看，每个 ring 的 p99 都恰好在它写满一圈之后的下一个窗口下降：

   | ring（BTC、ETH 共用） | 文件最后修改（写满一圈） | p99 下降的窗口 |
   |---|---|---|
   | SPOT/TRADE | 04:08 | 04:18–04:48 |
   | SPOT/BBO | 04:16 | 04:18–04:48 |
   | PERP/BBO | 04:37 | 04:48–05:18 |
   | PERP/TRADE | 05:06 | 05:18–05:48 |

   - **解释：** 4 个 ring 各 8 MiB，页面在第一次写到时才分配。每用到一个新的 4 KiB 页，写端要分配并清零，写端和读端都要处理缺页；在虚拟机里还要叠加宿主机那一层的缺页。
   - **为什么只影响 p99：** 每条消息约 100 B，一页约 40 条，所以约 2.5% 的消息要付这笔代价。这正好落在 p99，影响不到 p90。
   - **旁证：** 同期策略的 shmem 指标从 5.4 MiB 涨到 32.0 MiB。
   - **还没拆开的部分：** 这笔代价主要在写端还是读端。不过两边都算在 transport 里。
   - **影响：** 行情服务或策略每次重启后，都要过 2.5–3.5 小时 p99 才会降下来（按现在的消息量；ring 文件不删，但新进程的映射要重新缺页）。
5. **现货 Trade 的 provider 尾部更长。** Tokyo 稳态下，`BTC-USDT-SPOT/TRADE` 的 provider p99 是 12–44 µs，其他 topic 是 5–6 µs。
   - **代码事实：** SBE 的一个 trades 帧带多笔成交，每笔都用同一个接收时间（`LatencyMetric::received`）。之后它们逐笔进入 `pending`，各自经过一次 worker 的 `select!` 和 `publish`，所以帧里越靠后的成交等得越久。这是真实延迟，不是测量误差。
   - **推断（未核实）：** 尾部变长是逐笔处理的累积造成的。每帧有多少笔成交、每笔的处理成本是多少，都还没测。
6. **Tokyo 偶尔有毫秒级的 max。** 稳态 14 个窗口里有 5 个的 transport max 在 1.1–3.3 ms，每 30 分钟只有几次。策略每秒被动切换 7 次，`/proc/cmdline` 里没有任何隔离参数。推断是策略所在的核偶尔被内核线程或其他任务抢占，未验证。

## 3. 提速方向

按预期收益和代价排序。每一项都先在 Tokyo 上实测，再决定是否保留。

### 3.1 ring 预先映射【待定】

- **做法：** 写端和读端打开 ring 时，用 memmap2 的 `MmapOptions::populate()`（`MAP_POPULATE`）一次性映射全部页面。写端那一侧会同时分配 tmpfs 页。
- **预期：** 消除结论 4 的预热期。重启后立即进入稳态，transport p99 约 4 µs。
- **代价：** 每个 ring 打开时多花一次 8 MiB 的映射（毫秒级，不在热路径上）；每个策略最多 64 MiB 的 `/dev/shm` 从按需分配变成立即占用。Tokyo 有 24 GB 内存，没有问题；Osaka 只有 1 GB，要算一下余量。
- **验证：** 在 Tokyo 重启两个服务后，前 4 个窗口的 transport p99 应与稳态相同。
- **后续可选：** ring 放到大页上（Tokyo 的 `/dev/shm` 现在不用大页，`shmem_enabled` 是 never），8 MiB 只占 4 个 2 MiB 页，进一步减少 TLB miss。需要改系统配置，收益要等 3.1 做完后再测。

### 3.2 Tokyo 上绑核和隔离热路径线程【待定】

- **做法：** 给策略的忙轮询线程和行情服务的交易所 worker 线程各分一个独占核，其他线程和系统任务留在剩下的核上。有两个层次：
  - systemd 的 `CPUAffinity`，或代码里设置线程亲和性；
  - 内核参数 `isolcpus`、`nohz_full`、`rcu_nocbs`，需要重启 Tokyo。
- **预期：** 去掉结论 6 的毫秒级 max，并降低 p99.9。
- **待定：**
  - 4 个核怎么分。例如 0 号给系统和其他线程，1 号给 Binance worker，2 号给策略，3 号留给以后的 OKX worker。
  - 绑核写进 deploy（systemd unit）还是代码。
- **验证：** 策略的被动上下文切换从 7 次/s 降到接近 0，transport max 不再出现毫秒级。

### 3.3 现货 trades 帧整帧发布【待定】

- **第一步只测量：** 先统计 SBE trades 帧每帧有多少笔成交，以及每笔从出 `pending` 到写完 ring 的耗时，确认结论 5 的推断。
- **做法：** 如果推断成立，就降低每笔的成本。例如同一帧的成交在一次循环里连续发布，不再每笔都回到 `select!`。具体形状和 `ExchangeConnector::next_event` 的接口一起讨论（见 [`market_data_provider.md`](market_data_provider.md)「代码里的 TODO」）。
- **预期：** 现货 Trade 的 provider p99 从 12–44 µs 降到其他 topic 的水平，约 6 µs。

### 3.4 自研定长槽 ring（长远）

见 [`market_data_provider.md`](market_data_provider.md)「用自研的定长槽 ring 替换 bcast」。Tokyo 稳态下 internal p50 是 2.6 µs，其中 transport 1.1 µs、provider 1.5 µs，transport 已经不是大头。是否动手，仍以 `poll_fan_out` 和 `codec` 两个 bench 的结果为准，最好在 Tokyo 上重跑这两个 bench。

## 4. 以后做对比时的口径

- **先看 steal。** 跑 `vmstat 1` 看 `st` 列，不为 0 的机器只看 p50。Xian 被定为基准测试机，启用前要先确认它的 steal 为 0；如果和 Osaka 一样，它就不适合测内部延迟。
- **排除 ring 预热期。** 以策略的 shmem 指标达到所有 ring 的总大小为准（8 个 topic 时是 32 MiB）。3.1 实现之后就不需要这一步了。
- **窗口长度一致。** `--report-interval-seconds` 要相同。各窗口的分位数不能合并，只能比较各窗口分位数的中位数和范围。
- **控制其他条件。** 同一个 commit，同样的 topic，同一时段（消息速率会影响尾部）。
- **只比 p50 到 p99.9。** max 只用来发现异常，不用来做对比。

## 附：同期发现的 Celestia 断线

北京时间 03:47:39.351–.355，Osaka 和 Tokyo 一共 4 个进程的 Celestia 连接在 4 ms 内同时被重置（`Connection reset without closing handshake`）。问题只能在 Cloudflare Tunnel 或 RedmiBook 一侧，不在交易机器上。同一时段还有其他几次断线：

- 03:15 只有 Osaka 的策略断了。
- 03:19 两台机器都断了，但相隔 3 秒。
- 12:08 只有 Osaka 的行情服务断了，12:27 只有 Tokyo 的行情服务断了。

这些断线和延迟无关，记在这里供排查 Phanes 一侧时参考。
