<p align="center">
  <img src="assets/ruvoy-logo.png" alt="Ruvoy" width="360">
</p>

<p align="center"><strong>Ruby, carried by Envoy.</strong></p>

<p align="center">
  <img alt="Rust 2024" src="https://img.shields.io/badge/Rust-2024-000000?logo=rust&logoColor=white">
  <img alt="Ruby 4.0.5" src="https://img.shields.io/badge/Ruby-4.0.5-CC342D?logo=ruby&logoColor=white">
  <img alt="Envoy 1.39.1" src="https://img.shields.io/badge/Envoy-1.39.1-AC6199?logo=envoyproxy&logoColor=white">
  <a href="LICENSE"><img alt="Apache License 2.0" src="https://img.shields.io/badge/License-Apache--2.0-blue.svg"></a>
</p>

<p align="center"><a href="README.md">English</a> | 简体中文 | <a href="README.ja.md">日本語</a></p>

Ruvoy 是运行在 Envoy 内部的 Ruby 应用运行时。Envoy 负责 HTTP，Ruby 负责
Rack，两者之间由 Rust owned 消息桥接。

## 安装

```console
$ gem install ruvoy
```

gem 内含动态模块与 Envoy 二进制，不需要再装别的，也不需要手工对齐版本。

| | |
|---|---|
| Ruby | 4.0.x。模块链接到构建时的那个 Ruby，因此 gem 按 ABI 分别发布。 |
| 平台 | Linux `x86_64` 与 `aarch64`，与 Envoy 官方发布二进制的平台一致。 |
| Envoy | 已内置（1.39.1，动态模块 ABI 0.1.0），不单独安装。 |

## 快速开始

```console
$ ruvoy examples/hello/config.ru
$ curl localhost:8080/
hello from ruby 4.0.5
```

`ruvoy` 接收一个 rackup 并把它跑起来：生成 Envoy 配置，然后用 Envoy 替换自身，
因此信号与退出码都归 Envoy，容器里不需要额外的进程管理。

```console
$ ruvoy --help
Usage: ruvoy [options] [config.ru]
    -p, --port PORT                  Listen on PORT (default 8080)
    -a, --address ADDRESS            Bind to ADDRESS (default 127.0.0.1)
        --admin-port PORT            Expose Envoy's admin interface on PORT
        --print-config               Print the generated Envoy configuration and exit
```

要在自己配置的 Envoy 里运行，`--print-config` 会打印 Ruvoy 本来会用的 listener；
模块名是 `ruvoy_fiber`，它的 filter config 就是 rackup 的路径。

## 状态

数据面已实现，并由 CI 中针对真实 Envoy 运行的套件覆盖。配置热更新已在运行中的
Envoy 上驱动验证，1 小时 180 万请求的 soak 之后堆、文件描述符与线程数均持平。
接口可能变更。

## 特性

- **Envoy 原生 HTTP** — HTTP/1.1、HTTP/2、连接、超时和下游生命周期全部由
  Envoy 掌控。
- **Rack 应用** — 加载普通的 `config.ru`，接收由 Envoy 请求构建的 Rack
  环境。
- **Fiber 并发** — scheduler-aware 的 Ruby I/O 在专属 CRuby owner 线程上
  交错执行，Envoy worker 从不进入 Ruby。
- **Owned-data 边界** — 只有 Rust 拥有的请求和响应跨线程传递；Ruby 对象
  从不跨线程。
- **流式响应** — 响应体按应用产出的节奏逐块下发，下游背压直接施加到 Ruby
  生产者，而不是靠缓冲吸收。
- **流式请求** — 请求头一到就调用应用，请求体边到边读；应用跟不上时客户端
  被减速，而不是把请求体缓冲下来。
- **有界准入** — 请求数预算在工作进入 Ruby 之前就拒绝超额部分。
- **无上游 HTTP hop** — Rack 调用不需要经 loopback 连接到独立的 Ruby 应用
  服务器。

## 用与不用 Ruvoy

| | 不用 Ruvoy | 用 Ruvoy |
|---|---|---|
| 请求路径 | Envoy → 上游 HTTP → Ruby 服务器 | Envoy → Rust owned 消息 → Ruby |
| Ruby 宿主 | 独立的服务器进程（一个或多个） | Envoy 内部的专属 CRuby owner 线程 |
| 协议交接 | 请求被序列化到另一条 HTTP 连接上 | 请求以 owned 数据形式跨越 worker 边界 |
| I/O 并发 | 服务器线程或 worker | Ruby owner 线程上的 Async fiber |
| 故障边界 | Envoy 与 Ruby 是独立进程 | Envoy 与 Ruby 共享同一进程 |
| CPU 密集的 Ruby | 多个 Ruby worker 可用多核 | 单个 CRuby VM 仍受 GVL 限制 |

Ruvoy 面向 scheduler-aware I/O 和短小的 CPU-light Rack 工作负载设计，
不会让 CPU 密集的 Ruby 代码并行执行。

## 架构

```text
client
  → Envoy worker
  → owned Rust request
  → CRuby owner thread
  → Async Fiber
  → owned Rust response
  → original Envoy worker
  → client
```

Envoy worker 从不获取 Ruby 句柄、从不进入 VM。Ruby owner 线程运行 Rack
应用，结果被提交回持有下游 stream 的那个 worker。

## 当前范围

Ruvoy 构建 Rack 环境、提供二进制 `rack.input`、保留重复的响应头、消费
可枚举的响应体，并在响应体支持时调用 `close`。

响应体增量流式下发：响应头只发送一次，应用每 yield 一块就转发一块，因此
首字节在响应体完成之前就能到达客户端。慢客户端会对 Ruby 生产者施加背压
而不是在内存中堆积；客户端断开会停止枚举并关闭响应体。

请求体同样是流式的：请求头一到就调用应用，应用通过 `rack.input` 边到边读，
上传与处理因此重叠。应用还没读走的部分留在 Envoy 里，Envoy 会暂停从客户端
读取直到腾出空间。不支持裸 socket 劫持（`rack.hijack`），连接始终属于
Envoy。

响应体在响应头已经发出之后才失败时，只能停止：dynamic module 无法重置一个已经
开始应答的流，因此客户端收到的是一个短响应而不是错误。可能中途失败的应用应当
自己带上长度或校验值。

## 扩展

每个 Ruvoy 进程只运行一个 CRuby VM，因此单进程内的 Ruby 工作受 GVL 限制
只能用一核。横向扩展靠更多进程而不是更多线程：每个副本跑一个 Ruvoy，像
任何单 VM Ruby 服务器一样按副本数扩展。

## 基准测试

### 环境

| 组件 | 配置 |
|---|---|
| 机器 | 4 vCPU、32 GiB 内存、x86_64 |
| 系统 | Linux |
| Envoy | `1.39.0`，concurrency `1` |
| Ruby | `4.0.5` |
| Rust | `1.97.1` |
| Rack | `3.2.6` |
| 压测工具 | oha `1.15.0` |

### 混合压力

该轮测试覆盖准入控制、恢复、客户端取消和资源稳定性，共三个完整周期：

- 稳态流量：每周期 45,000 个 HTTP/2 no-op 请求、6,000 个带 200 ms 等待的
  HTTP/1.1 请求、1,200 个带 256 KiB 请求体的 HTTP/1.1 请求；
- 请求数过载：15 秒内最多准入 256 个请求；
- 请求体总量过载：15 秒内准入体预算 64 MiB；
- 客户端取消：128 个刻意取消的请求；
- 恢复：每周期 20,000 个 no-op 请求和 2,000 个等待请求。

| 结果 | 数值 |
|---|---:|
| 总请求数 | `2,503,763` |
| HTTP 200 | `251,400` |
| 预期过载 HTTP 503 | `2,252,363` |
| 传输错误 | `0` |
| 稳态与恢复阶段失败 | `0` |
| 控制 listener 最大延迟 | `35.615 ms` |
| 每周期结束后的文件描述符数 | `59` |
| Ruby live-heap 变化（恢复周期 1 → 3） | `+2 slots` |
| 请求体过载期间峰值 RSS | `489,332 KiB` |

所有稳态和恢复请求均返回 HTTP 200。全部 HTTP 503 都产生于刻意的过载阶段，
计为被拒绝的工作，不计入成功吞吐。

该轮压测工具与服务器同机运行，服务器未打满主机 CPU，因此这组结果只支撑
韧性与有界过载的主张，不是最大吞吐测量。

### 服务器对比

在固定 commit 上对比 Ruvoy、直连 Falcon `0.55.6`、直连 Puma `7.2.0` 和
Envoy → Puma，四者运行同一个 Rack fixture：

- oha 运行在局域网内一台独立的 2 vCPU 主机上（RTT 约 0.03 ms），压测端
  从不与服务器争抢 CPU。
- 每次测量为 10 秒预热加 30 秒加压；每个单元 7 轮，每轮轮转架构顺序。
- 一轮只有在所有请求都返回 HTTP 200 且传输错误为零时才计入。报告值为
  中位数并附相对标准差。
- 每个架构都只运行一个 Ruby 执行上下文：Puma 单进程模式 100 线程、单个
  Falcon 实例、单个 Ruvoy owner 线程、Envoy `concurrency 1`。
- 判定标准在运行前即已固定：只有当吞吐和 p99 的改善同时超过 5% 与实测
  轮间波动两者中的较大值时，才允许声称优势。

No-op Rack 应用，HTTP/1.1，100 连接，明文：

| 服务器 | RPS 中位数（RSD） | p50 ms | p99 ms | CPU | RSS MiB |
|---|---:|---:|---:|---:|---:|
| Ruvoy | `31,270`（1.9%） | `3.06` | `5.01` | 189% | 94 |
| Falcon | `12,716`（0.5%） | `7.67` | `9.63` | 99% | 83 |
| Puma | `10,410`（5.5%） | `8.84` | `20.96` | 87% | 127 |
| Envoy → Puma | `9,754`（4.0%） | `9.92` | `17.63` | 159% | 185 |

相同负载启用 TLS 1.3——Ruvoy 和 Envoy → Puma 由 Envoy 终结，Falcon 和
Puma 进程内终结：

| 服务器 | RPS 中位数（RSD） | p50 ms | p99 ms | CPU | RSS MiB |
|---|---:|---:|---:|---:|---:|
| Ruvoy | `32,065`（2.9%） | `2.98` | `4.85` | 192% | 95 |
| Falcon | `11,401`（3.0%） | `8.73` | `10.61` | 99% | 91 |
| Envoy → Puma | `9,674`（4.4%） | `9.97` | `17.44` | 162% | 191 |
| Puma | `8,728`（6.7%） | `9.87` | `25.52` | 87% | 148 |

Falcon 是隔离架构变量的对照：两者同为 fiber-per-request，差异只在于请求
是以 owned 数据在同一进程内传递，还是经 loopback HTTP hop 传递。相对
Falcon，Ruvoy 明文下吞吐 `+146%`、p99 `−48%`，TLS 下 `+181%`、`−54%`，
均超过预注册的噪声阈值。相对 Envoy → Puma 的幅度为 `+221%` 和 `+231%`。

四条如实限定：

- No-op 场景测量的是每请求框架开销，因此这些是上限值。真实应用耗时越长，
  相对幅度越小：在 100 连接、模拟 200 ms 等待下，四个服务器全部停在同一
  个约 495 RPS 的并发上限。
- Ruvoy 的 CPU 列包含了同进程内处理 HTTP 的 Envoy worker（约 1.9 核，
  Falcon 约 1.0 核）。按每核计幅度约为 `+29%`，其余来自 Ruby 线程搭配了
  代理级 HTTP 前端。
- 这里的每个数字都来自「等待时会让出」的应用。用 C 扩展写的驱动会释放解释器锁，
  却不会让出 fiber，因此一次这样的调用会卡住同一线程上的其它全部请求。
  `wait50` 与 `block50` 两个场景直接测量这个差异，它们不属于上面这轮 campaign。
- 另跑过一个 1 MiB 响应场景，但不产生排名：在每秒约 1,000 个该响应以上
  时，四个架构全部收敛到网络路径的吞吐上限，波动达 59–96%，该场景测的是
  链路而不是服务器。

未测量：CPU 密集的 Rack、真实的数据库或 HTTP 客户端驱动、多进程部署。

测量由 `scripts/run-benchmark.sh` 产出，脚本强制执行上述协议——远端发压、
轮转、预热、逐轮校验和预注册判定——并为每次运行保留原始 oha 输出、CPU 和
RSS 采样以及生成的汇总。

## 许可证

Apache License 2.0，见 [LICENSE](LICENSE)。
