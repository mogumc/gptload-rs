# TODO

## 近期

### 1. 429 Retry-After 解析

**目标**：上游 429 响应中的 `Retry-After` header 告诉我们多久后可以重试，用它替代固定冷却。

**实现**：
- `execute_attempt` 中收到 429 时，提取 `Retry-After` header
- 解析两种格式：
  - 秒数：`Retry-After: 5` → 冷却 5 秒
  - HTTP-date：`Retry-After: Wed, 21 Oct 2025 07:28:00 GMT` → 计算毫秒差
- 在 `on_upstream_status` 中传入解析后的冷却时长，替代固定 `rate_limit_cooldown_ms`
- 优先级：`Retry-After` header > `rate_limit_cooldown_ms` 配置 > 默认 3s
- 上限：最大冷却不超过 `max_rate_limit_cooldown_ms`（新配置，默认 30s），防止上游返回离谱值

**配置变更**：
```toml
[key]
rate_limit_cooldown_ms = 3000          # 默认冷却（无 Retry-After 时）
max_rate_limit_cooldown_ms = 30000     # Retry-After 冷却上限
```

**涉及文件**：`src/proxy.rs`（提取 header）、`src/state.rs`（`on_upstream_status` 接受冷却参数）

---

### 2. 优雅关闭 (Graceful Shutdown)

**目标**：收到 SIGTERM/SIGINT 时，不直接退出，而是等待进行中的请求完成。

**实现**：
- `main.rs` 中用 `tokio::signal::ctrl_c()` 或 `signal::unix::signal(SignalKind::terminate())` 捕获信号
- 信号到达后，停止接受新连接（`hyper::server` 的 graceful shutdown）
- 用 `AtomicBool` 标记 shutting_down 状态，`select_for_model` 在此状态下返回 None
- 等待 `requests_inflight` 降到 0，或超过 `graceful_shutdown_timeout_secs`
- 超时后强制退出
- 日志：`"graceful shutdown: waiting for N inflight requests..."`, `"graceful shutdown: timeout, forcing exit"`

**配置变更**：
```toml
[server]
graceful_shutdown_timeout_secs = 30   # 默认 30 秒
```

**涉及文件**：`src/main.rs`（信号处理）、`src/state.rs`（shutting_down 标志）、`src/proxy.rs`（拒绝新请求）

---

### 3. CORS 支持

**目标**：允许浏览器端前端直接调用 proxy API。

**实现**：
- 新配置项 `cors_origins`：允许的 origin 列表，默认 `["*"]`
- 在 `proxy.rs` 的 `handle` 函数入口处：
  - 如果请求是 `OPTIONS` 且有 `Origin` header → 返回 CORS 预检响应
  - 否则在所有响应上附加 `Access-Control-Allow-Origin` 等 header
- 不匹配的 origin → 不附加 CORS header（浏览器自行拒绝）
- Admin API 同样需要支持（前端 UI 跨域访问）

**配置变更**：
```toml
[server]
cors_origins = ["*"]                    # 默认允许所有
# cors_origins = ["https://example.com"]  # 或指定域名
```

**涉及文件**：`src/config.rs`（新字段）、`src/proxy.rs`（CORS middleware）

---

## 中期

### 4. 请求队列 / 背压

**目标**：当所有 key 都不可用（并发上限、冷却中、无 active key）时，排队等待而非直接 503。

**实现**：
- `select_for_model` 返回 None 时，不立即返回 503
- 用 `tokio::sync::Semaphore` 实现排队：
  - 每个 upstream 一个 semaphore，permits = 所有 key 的 max_concurrent 之和
  - `acquire()` 阻塞直到有 permit 可用或超时
- 配置：
  - `queue_enabled: bool` — 是否启用排队（默认 false）
  - `queue_max_depth: usize` — 最大排队数（默认 100）
  - `queue_timeout_ms: u64` — 排队超时（默认 10000ms）
- 超时或队列满 → 返回 429（带 Retry-After）而非 503
- 公平性：FIFO，不按 token 分组（简单实现）
- Prometheus：`gptload_queue_depth`、`gptload_queue_timeout_total`

**配置变更**：
```toml
[server]
queue_enabled = false
queue_max_depth = 100
queue_timeout_ms = 10000
```

**涉及文件**：`src/proxy.rs`（排队逻辑）、`src/state.rs`（semaphore 管理）

---

### 5. Config 热重载 (SIGHUP)

**目标**：发送 SIGHUP 信号重新读取 config.toml，无需重启进程。

**实现**：
- `main.rs` 注册 SIGHUP handler
- 信号到达 → 重新 `Config::load()` → diff 与当前配置
- **可热更新**（直接替换）：
  - `proxy_tokens`、`admin_tokens`、`export_token`
  - `request_timeout_ms`、`max_retries`、`retry_status_codes`
  - `key.*`（blacklist_threshold, max_concurrent_per_key, rate_limit_cooldown_ms 等）
  - `upstreams[].weight`、`upstreams[].max_concurrent_per_key`
  - `cors_origins`
- **不可热更新**（日志提示需重启）：
  - `listen_addr`（需要重新 bind）
  - `worker_threads`（tokio runtime 不支持动态调整）
  - `data_dir`（数据库路径）
- **Upstream 增删**：新增 upstream 直接加入；删除 upstream 从 snapshot 中移除
- **Key 变更**：通过 admin API 管理，config 中的 keys 不参与热重载（已是数据库）
- 日志：`"config reloaded: request_timeout_ms 30000→60000"`, `"config: listen_addr changed, restart required"`

**涉及文件**：`src/main.rs`（SIGHUP handler）、`src/state.rs`（apply_config_diff）、`src/config.rs`（diff 支持）

---

## 远期

### 6. 多格式适配（Anthropic / Gemini）

**目标**：同一个 proxy 同时代理 OpenAI、Anthropic、Gemini 格式的 API。

**实现**：
- UpstreamConfig 新增 `format` 字段：`"openai"`（默认）、`"anthropic"`、`"gemini"`
- **Anthropic Messages API**：
  - 请求转换：OpenAI `/v1/chat/completions` → Anthropic `/v1/messages`
  - Header 转换：`Authorization: Bearer sk-xxx` → `x-api-key: sk-xxx` + `anthropic-version: 2023-06-01`
  - Body 转换：`messages` 格式、`max_tokens` → `max_tokens`（字段名相同但结构不同）
  - 响应转换：Anthropic SSE → OpenAI SSE 格式
  - 流式：Anthropic `content_block_delta` → OpenAI `choices[0].delta`
- **Gemini GenerateContent**：
  - 请求转换：OpenAI → Gemini `generateContent` / `streamGenerateContent`
  - Header 转换：`Authorization: Bearer` → URL 参数 `?key=xxx`
  - Body 转换：`messages` → `contents[].parts[]`
  - 响应转换：Gemini JSON → OpenAI 格式
- **自动检测**：根据 base_url 猜测（`api.anthropic.com` → anthropic，`generativelanguage.googleapis.com` → gemini）
- Key 格式适配：Anthropic key 以 `sk-ant-` 开头，Gemini key 格式不同

**配置变更**：
```toml
[[upstreams]]
id = "claude"
base_url = "https://api.anthropic.com"
format = "anthropic"
keys = ["sk-ant-xxx"]

[[upstreams]]
id = "gemini"
base_url = "https://generativelanguage.googleapis.com"
format = "gemini"
keys = ["AIzaSyxxx"]
```

**涉及文件**：新文件 `src/format.rs`（格式转换逻辑）、`src/proxy.rs`（路由分发）、`src/state.rs`（UpstreamConfig 扩展）

---

### 7. Web UI 增强

**目标**：Admin UI 从"能用"变成"好用"。

**功能清单**：
- **Dark mode**：CSS 变量 + 切换按钮，跟随系统偏好
- **Key 延迟分布图表**：用 Chart.js 或 lightweight-charts 展示每个 key 的 p50/p90/p99
- **实时请求流**：WebSocket 推送，展示最近 N 条请求（方法、路径、状态码、延迟、key、upstream）
- **多 upstream 对比视图**：表格形式并排展示各 upstream 的 keys/errors/latency
- **一键测试 key**：点击按钮发送测试请求（`GET /v1/models`），验证 key 是否有效
- **Key 搜索/过滤**：按状态（active/invalid/cooldown）过滤，按 failure_count 排序
- **Upstream 权重调整滑块**：实时调整 weight，无需重启
- **配置预览**：展示当前生效的配置（只读）
- **请求日志详情**：点击单条请求查看完整信息（headers、body、timing breakdown）

**技术方案**：
- 前端保持纯 HTML/JS/CSS（无构建工具），通过 `<script>` 引入
- 考虑引入 Alpine.js 或 htmx 简化交互
- 图表用 Chart.js（CDN 引入，约 60KB）
- WebSocket 复用现有 SSE endpoint 或新增 WS endpoint

**涉及文件**：`src/static/index.html`、`src/static/app.js`、`src/admin.rs`（新增 WS endpoint）

---

### 8. Upstream 代理支持（SOCKS/HTTP）

**目标**：gptload-rs 访问上游时，通过代理连接。用于网络受限环境（如国内访问 OpenAI）。

**实现**：
- 依赖：`hyper-proxy` 或 `hyper-socks` crate
- UpstreamConfig 新增 `proxy` 字段：
  - `socks5://user:pass@host:port`
  - `http://user:pass@host:port`
  - 不设置 → 直连
- 构建 hyper Client 时，根据 upstream 的 proxy 配置选择 connector：
  - 无 proxy → `hyper_rustls::HttpsConnector`（现有逻辑）
  - HTTP proxy → `hyper_proxy::ProxyConnector`
  - SOCKS5 → `hyper_socks5::SocksConnector`
- 每个 upstream 独立的代理配置（不同 upstream 可以走不同代理）
- 连接池复用：代理连接池与直连池独立管理
- DNS 解析：SOCKS5 支持远程 DNS 解析（`-D` 模式），避免本地 DNS 污染

**配置变更**：
```toml
[[upstreams]]
id = "openai"
base_url = "https://api.openai.com"
proxy = "socks5://127.0.0.1:1080"       # 通过本地 SOCKS5 代理
keys = ["sk-xxx"]

[[upstreams]]
id = "openai-direct"
base_url = "https://api.openai.com"
# proxy = ""                             # 直连（默认）
keys = ["sk-yyy"]

[[upstreams]]
id = "openai-http"
base_url = "https://api.openai.com"
proxy = "http://proxy.company.com:8080"  # HTTP 代理
keys = ["sk-zzz"]
```

**依赖变更** (`Cargo.toml`)：
```toml
hyper-proxy = "0.9"      # HTTP/HTTPS proxy
tokio-socks = "0.5"      # SOCKS5 support (via hyper-socks 或自行实现 connector)
```

**涉及文件**：`src/config.rs`（proxy 字段）、`src/state.rs`（构建不同 connector）、`Cargo.toml`（新依赖）

---

## 大目标 (Big Targets)

### 9. AI Gateway — 从 Proxy 演进为 Gateway

**愿景**：不只是转发请求，而是成为 AI 基础设施的核心层。

**9.1 Token 预算管理**
- 每个 proxy_token / 团队有 token 配额（每日/每月）
- 基于 usage injection 的 token 计数做精确计量
- 超配额 → 拒绝请求（402 Payment Required）
- 配额管理 API：设置、查询、重置
- 支持 burst（允许短期超配额，但长期受限）

**9.2 智能路由 — 成本优化**
- 不同 provider 定价不同（OpenAI vs Anthropic vs Gemini）
- 根据请求特征选择最优 provider：
  - 延迟敏感 → 选最近的 upstream
  - 成本敏感 → 选最便宜的 upstream
  - 质量敏感 → 选模型最好的 upstream
- 配置每个 upstream 的 cost_per_1k_tokens
- 路由策略：`cheapest`、`fastest`、`quality`、`balanced`

**9.3 响应缓存**
- 对完全相同的请求（相同 model + messages + params）缓存响应
- 缓存键：请求 body 的 hash
- TTL 可配置，默认 5 分钟
- 适用场景：重复查询、测试、批量处理
- 存储：内存 LRU 或 Redis

**9.4 Prompt 前缀去重**
- 很多请求共享相同的 system prompt
- 检测公共前缀，只传一次
- 与 upstream 的 prompt caching 特性配合（如 OpenAI 的 automatic prompt caching）
- 节省 token 费用

**9.5 安全层**
- Prompt 注入检测：用规则或模型检测恶意 prompt
- PII 检测/脱敏：自动识别并遮蔽请求中的个人信息
- 内容审核：检测违规内容（暴力、色情等）
- 可选：用上游模型做 content moderation

**涉及**：整个项目架构级重构。新增 `src/gateway/` 模块组。

---

### 10. 可观测性平台 (Observability)

**10.1 OpenTelemetry 分布式追踪**
- 每个请求生成 trace_id
- Span：proxy 接收 → upstream 请求 → 响应处理
- 导出到 Jaeger / Zipkin / Grafana Tempo
- 可视化请求在多个 upstream 之间的重试路径

**10.2 结构化日志**
- 每条请求日志包含：trace_id、model、upstream、key（脱敏）、token usage、延迟 breakdown
- JSON 格式输出，可接入 ELK / Loki
- 日志级别动态调整（无需重启）

**10.3 成本分析仪表盘**
- 按 team（proxy_token）统计 token 花费
- 按 model 统计 usage 和 cost
- 按 upstream 统计成功率、延迟、成本
- 趋势图、对比图、预算预警

**10.4 告警**
- 基于 Prometheus 指标的告警规则
- 示例：key 活跃率 < 30%、upstream 5xx 率 > 10%、队列深度 > 50
- Webhook / Email / Slack 通知

**涉及**：`src/telemetry/` 模块、Prometheus 指标扩展、Admin UI 仪表盘。

---

### 11. Developer SDK & 生态

**11.1 CLI 管理工具**
- `gptload key add --upstream openai --keys "sk-xxx,sk-yyy"`
- `gptload key list --upstream openai --status invalid`
- `gptload config validate --file config.toml`
- `gptload bench --model gpt-4 --concurrent 50 --requests 1000`
- 独立 binary，通过 Admin API 交互

**11.2 Terraform Provider**
- `resource "gptload_upstream" "openai" { ... }`
- `resource "gptload_key" "k1" { upstream = gptload_upstream.openai.id ... }`
- `resource "gptload_token" "team_a" { balance = 1000000 ... }`
- 适合 IaC 管理大量 upstream 和 key

**11.3 Kubernetes Operator**
- CRD：`GptloadConfig`、`UpstreamPool`、`KeyPool`
- Operator 自动管理 gptload-rs 实例
- 自动滚动更新、HPA 扩缩容
- Key 轮转自动化（CronJob 定期更新 Secret）

**11.4 Client SDK**
- Python：`gptload.Client(upstream="openai").chat(model="gpt-4", ...)`
- Node.js / Go 同理
- 内置重试、负载均衡、failover
- 与直接用 OpenAI SDK 兼容，只需改 base_url

**涉及**：新仓库、新项目。

---

### 12. 高性能数据面 (High-Performance Data Plane)

**12.1 io_uring I/O**
- 用 `io-uring` crate 替代 tokio 的 epoll
- 减少系统调用开销，提升吞吐
- 适合极高并发场景（10k+ RPS）

**12.2 零拷贝转发**
- 当前 proxy 将 body 读入 Bytes 再转发
- 对于非重试请求，直接 pipe 上游 body 到下游
- 减少内存拷贝和分配

**12.3 连接池优化**
- 每个 upstream 独立的连接池
- 连接池大小可配置
- 连接复用策略：keep-alive、max idle per host

**12.4 SIMD 加速**
- JSON 解析/序列化用 SIMD 优化
- Header 解析用 SIMD 加速
- 可引入 `simd-json` crate

**涉及**：`src/proxy.rs`、`src/state.rs`、`Cargo.toml`。底层重构。

---

### 13. 多区域联邦 (Multi-Region Federation)

**愿景**：多个 gptload-rs 实例跨区域部署，共享状态，智能路由。

**13.1 状态同步**
- 方案一：Redis Cluster 共享 key 状态（failure_count、cooldown、active_requests）
- 方案二：CRDT 数据结构 + Gossip 协议（无中心依赖）
- 方案三：etcd/Consul 作为一致性存储

**13.2 全局路由**
- 客户端请求到达最近的 gptload-rs 实例
- 实例根据全局状态选择最优 upstream
- 考虑跨区域延迟、upstream 健康状态、key 可用性

**13.3 故障转移**
- 某个区域的 gptload-rs 实例挂掉
- DNS / 负载均衡器自动切换到其他区域
- 其他区域的实例已同步 key 状态，无缝接管

**涉及**：新增 `src/federation/` 模块、外部依赖（Redis/etcd）。架构级变更。
