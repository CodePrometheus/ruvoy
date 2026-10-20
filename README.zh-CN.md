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
$ gem install ruvoy ruvoy-envoy
```

`ruvoy` gem 内含动态模块。Envoy 来自 `ruvoy-envoy` gem 或机器自带：ruvoy 依次查找
`RUVOY_ENVOY`、`ruvoy-envoy`、`PATH` 上的 `envoy`。

| | |
|---|---|
| Ruby | 4.0.x。模块链接到构建时的那个 Ruby，因此 gem 按 ABI 分别发布。 |
| 平台 | Linux `x86_64` 与 `aarch64`，与 Envoy 官方发布二进制的平台一致。 |
| Envoy | 1.39（动态模块 ABI 0.1.0），来自 `ruvoy-envoy` gem 或机器自带。 |

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

要在自己配置的 Envoy 里运行，`--print-config` 会打印 Ruvoy 本来会用的配置：命名
rackup 的 `ruvoy` bootstrap 扩展（见 [生命周期](#生命周期)），以及一个 rack 过滤器
加入它的 listener。过滤器自己的配置是一个可选的 JSON 对象（见
[Envoy context 与上游调用](#envoy-context-与上游调用)）。

## 状态

数据面已实现，并由 CI 中针对真实 Envoy 运行的套件覆盖。配置热更新已在运行中的
Envoy 上驱动验证；3 小时、573 万请求、2.28 万个 WebSocket 会话、359 次监听器更新的
soak 之后，存活内存、文件描述符、线程数与 p99 延迟均持平，常驻内存仍每小时上涨约 3%，
占着它的是分配器与 Ruby 堆留下的空闲内存，而非存活数据。
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
- **Envoy context 与上游调用** — 可选：应用能看到 Envoy 对请求掌握的路由、
  连接、TLS 与 metadata，并能在请求内调用 Envoy 的 cluster。

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
读取直到腾出空间。应用读过的部分会保留下来，128 KiB 以内放内存，超出放临时
文件，所以 `rack.input` 可以倒回重读，照顾那些按早期 Rack 版本编写的框架。不支持
裸 socket 劫持（`rack.hijack`），连接始终属于
Envoy。WebSocket 等协议升级改用 Rack 3 的 `rack.protocol` 加流式响应体，支持
HTTP/1.1 与 HTTP/2，需要 Envoy 放行（`upgrade_configs`，HTTP/2 另需 `allow_connect`）。

响应体在响应头已经发出之后才失败时，Ruvoy 会重置这个流，客户端看到的是错误，
而不是一个看似完整的短响应。

## 阻塞调用

只有当 fiber 等待的调用配合 Ruby 的 fiber 调度器时，它才能不占用运行时线程地
等待：Ruby 自己的 socket 与 IO、`sleep`，以及许多驱动提供的异步 API 都是如此。
而在内核里阻塞的 C 扩展调用会占住所有请求共用的那一个线程，请求只能排队：
这类 50ms 的调用，无论多少客户端在等，每秒大约只能服务 20 个请求。

`Ruvoy.offload` 把这样的调用放到它自己的 Ruby 线程上执行，发起调用的 fiber 在一旁等待：

```ruby
rows = Ruvoy.offload { connection.sync_exec(sql) }

# At most 16 in flight; callers beyond that wait their turn.
DATABASE = Ruvoy::Offload.new(limit: 16)
rows = DATABASE.call { connection.sync_exec(sql) }
```

- 它对阻塞时释放 GVL 的扩展有效，阻塞型 C 调用通常都是这样写的。阻塞时仍持有
  GVL 的扩展会卡住所有线程，Ruby 这一侧无能为力。
- 调用无法被中途切断。调用方被打断时（客户端断开或超时触发），它仍会等调用结束
  才退出，因此调用正在使用的连接不会在使用中被还回去。
- 代码块在另一个线程上运行，所以线程局部状态（例如按线程分配的连接）属于那个线
  程，而不是调用方。
- 驱动有调度器友好的 API 时优先用它。`offload` 每次调用要占一个线程，不给上限时
  不做任何限流。

## Rails

Rails 应用无需改动即可运行，只需加上所有 fiber 服务器都需要的一项设置：

```ruby
config.active_support.isolation_level = :fiber
```

Active Record 默认按线程分配连接，而每个请求的 fiber 都跑在同一个运行时线程上，
所以不加这项设置，并发请求会共用同一个连接；加上之后，每个在途请求各持一个连接。

- 不支持 Action Cable：它需要 `rack.hijack`。
- 请求经 Envoy 到达 Rails，Envoy 会在转交的请求上加上 `X-Forwarded-Proto`。
- 静态文件由前面的 Envoy 提供，把 `config.public_file_server.enabled` 设为 false
  可以省掉 Rails 每个请求的磁盘查找：小响应上实测吞吐高出约五分之一。
- `test/e2e/rails_app_test.rb` 把一个 production 模式的 Rails 8.1 应用同时跑在
  Ruvoy 和参照服务器上，逐条比对响应，并检查并发下与事务中途客户端断开时的连接行为。

## Envoy context 与上游调用

两项可选能力让应用拿到比 Rack 请求更多的 Envoy 信息。两者默认关闭，只有
filter config 要求时才开启，且只在 fiber 运行时提供。此时配置是一个 JSON 对象：

```json
{
  "context": { "metadata_namespaces": ["acme.tenant"] },
  "upstream": { "clusters": ["users"], "timeout_ms": 15000, "max_response_bytes": 1048576 }
}
```

### `ruvoy.context`

Envoy 对这个请求的视图，在请求头到达时由 worker 拷贝出来；Ruby 对象只在应用
读取时才构造。

```ruby
context = env["ruvoy.context"]
context.route_name        # => "api", or nil for an unnamed route
context.connection        # => {"id"=>7, "source_address"=>"10.0.0.7", "source_port"=>51234, ...}
context.tls               # => nil on plaintext, else {"version"=>"TLSv1.3", "server_name"=>..., "peer_certificate"=>...}
context.dynamic_metadata  # => {"acme.tenant"=>{"id"=>"t-42"}}, from filters ahead of Ruvoy
context.route_metadata    # => the same namespaces, from the matched route
context.to_h              # => all of the above
```

Envoy 模块接口能报告的内容决定了几条限制：

- 只有路由配置给 route 起了名字，route 才有名字。terminal filter 也需要一个
  route 条目才能被命名：给它加上 `non_forwarding_action: {}`。
- `server_name` 是客户端发送的 SNI，只有 listener 配置了 TLS inspector
  listener filter，Envoy 才会记录它。
- `peer_certificate` 表示客户端出示了证书。只有在要求客户端证书、且证书须由
  受信任 CA 签发的 listener 上，它才能作为客户端身份。
- 只报告第一个 URI 和第一个 DNS subject alternative name。
- metadata 的值是字符串、数字（一律为 Float）、布尔值及它们组成的列表。嵌套
  结构会被略过。

### `ruvoy.upstream`

调用 Envoy 配置里的某个 cluster，返回状态码、响应头，以及完整响应体（String）：

```ruby
status, headers, body = env["ruvoy.upstream"].call(
  "users", "GET", "/users/42",
  headers: { "accept" => "application/json" }, timeout: 2
)
```

调用由该请求所在的 worker 经 cluster 发出，所以 cluster 的连接池、TLS 设置和
熔断器都会生效，`x-envoy-retry-on` 这类请求头可以让 Envoy 重试。发起调用的
fiber 等待时不占用运行时线程，其他请求照常服务，同一请求里的多个调用可以重叠。
重复的响应头以 Array 返回。

- 只能调用 `upstream.clusters` 里列出的 cluster，其他 cluster 会抛
  `ArgumentError`。
- 调用进行中由 Envoy 自己生成的回复按普通响应返回：超时为 504，上游连不上
  或中途断开为 503。
- 调用发不出去时抛 `Ruvoy::UpstreamError`：cluster 不存在，或者 cluster 当场
  拒绝了调用，例如没有健康主机、熔断器已打开。请求在调用完成前已经结束时也会抛出。
- 响应体超过 `max_response_bytes` 或 Envoy 自身的缓冲上限时，抛它的子类
  `Ruvoy::UpstreamResponseTooLarge`，调用方据此区分「该少要一点」和「稍后重试」。
- `call` 把响应整体缓冲，所以 `max_response_bytes` 限定了单次调用在内存里能占用的大小。

`stream` 参数相同，但收到响应头就返回，响应体流式交出，可以直接作为 Rack 响应，
所以代理只需一行：

```ruby
run ->(env) { env["ruvoy.upstream"].stream("api", env["REQUEST_METHOD"], env["PATH_INFO"], body: env["rack.input"]) }
```

Envoy 对它不做流控，读者落后超过 `upstream.stream_buffer_bytes`（默认 1 MiB）
即重置流。带上 `protocol: "websocket"` 时，响应会把客户端也升级，并双向转发。

调用和流会带上请求的 trace 上下文：`traceparent`、`tracestate`、`x-request-id`
和 B3 系列头，调用自己设置了的除外。

[`examples/extensions`](examples/extensions) 用一个替身服务把两项能力都跑一遍，
包括同一请求里两次调用的重叠。

## 生命周期

`ruvoy` bootstrap 扩展掌管 Ruby 运行时：它命名 rackup，并规定运行时按什么条件服务它；
每个 listener 的 rack 过滤器都加入这个运行时。没有配置它时，rack 过滤器会被拒绝。

```yaml
bootstrap_extensions:
  - name: ruvoy
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.bootstrap.dynamic_modules.v3.DynamicModuleBootstrapExtension
      dynamic_module_config: { name: ruvoy_fiber, do_not_close: true }
      extension_name: ruvoy
      extension_config:
        "@type": type.googleapis.com/google.protobuf.StringValue
        value: '{"rackup": "/srv/app/config.ru"}'
```

| 设置 | 默认值 | |
|---|---|---|
| `rackup` | | 应用。必填。 |
| `max_inflight_requests` | 1024 | 同时在 Ruby 里的请求数；再多一个以 503 拒绝。 |
| `shutdown_timeout_ms` | 10000 | 关停时等待仍在运行的 fiber 多久。 |
| `diagnostics` | false | 给响应加上 `x-ruvoy-*` 诊断头。 |

- 应用加载完成、`Ruvoy::Lifecycle.before_ready { }` 注册的钩子跑完之后，Envoy 才变为 ready。
  应用加载失败会让 Envoy 退出。
- `Ruvoy::Lifecycle.on_drain { }` 在 Envoy 开始 drain 时运行，此时请求照常服务：用来关闭长连接、
  停止后台工作。
- Envoy 的关停会停止运行时：仍在运行的 fiber 被取消，并有 `shutdown_timeout_ms` 的时间收尾。
- admin 接口能到达 Ruby：`/ruvoy`（概览）、`/ruvoy/gc`（`GET` 返回 `GC.stat`，`POST` 触发回收，
  `?compact=1` 同时整理）、`/ruvoy/fibers`（存活 fiber 及回溯）、`/ruvoy/config`。Ruby 在自己的
  线程上作答，两秒内没答的返回 504。
- `Ruvoy::Lifecycle.settings` 保存上面这些设置。
- Envoy 的内存分配器默认不归还已释放的内存，除非 `memory_allocator_manager` 设置了
  `bytes_to_release`；不设的话常驻内存只增不减。生成的配置每秒归还 1 MiB，手写配置也应设置。

## 统计

Ruvoy 通过 Envoy 的统计上报，数字会进入 Envoy 配置的任何 sink，包括 admin 接口的
`/stats/prometheus`。

- `requests_total`、`rejected_total{reason}`、`responses_total{outcome}` 与
  `duration_ms`：请求数，以及进入应用的请求如何结束。
- `inflight_requests`、`reactor_idle_ms` 与 `resident_bytes`：由 bootstrap 扩展每秒在主线程
  读取一次，请求路径上不做任何发布，空闲时显示的就是空闲的运行时。
- `upstream_calls_total{cluster, outcome}`：按应用看到的结局给调用计数，
  `completed`、`refused`、`timed_out`、`overflow`、`too_large`、`reset` 或
  `abandoned`。`upstream_duration_ms{cluster}` 对 `call` 计到完整响应，对 `stream`
  计到响应头。状态码和 Envoy 自己的超时在 cluster 的统计里。

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

测量由 `bench/run.rb` 产出，脚本强制执行上述协议——远端发压、
轮转、预热、逐轮校验和预注册判定——并为每次运行保留原始 oha 输出、CPU 和
RSS 采样以及生成的汇总。

## 许可证

Apache License 2.0，见 [LICENSE](LICENSE)。
