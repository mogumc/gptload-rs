# TODO

## 近期

### ~~1. Dark Mode~~ ✅ 已完成

### ~~X-Forwarded-For 反代支持~~ ✅ 已完成
- 解析 `X-Forwarded-For` / `X-Real-IP` 头，正确获取 nginx 反代后的真实客户端 IP

### ~~OpenTelemetry 分布式追踪~~ ✅ 已完成
- 3 层 Span：`proxy.request` → `proxy.forward` → `proxy.upstream_http`
- W3C TraceContext 入站提取 + 出站注入
- OTLP HTTP 导出（Jaeger/Tempo 等）
- 配置：[telemetry].otlp_endpoint

---

## 中长期

### 2. AI Gateway — 从 Proxy 演进为 Gateway

**2.1 Token 预算管理**
- 每个 proxy_token / 团队有 token 配额（每日/每月）
- 基于 usage injection 的 token 计数做精确计量
- 超配额 → 拒绝请求（402 Payment Required）
- 配额管理 API：设置、查询、重置

**2.2 智能路由 — 成本优化**
- 不同 provider 定价不同（OpenAI vs Anthropic vs Gemini）
- 配置每个 upstream 的 cost_per_1k_tokens
- 路由策略：`cheapest`、`fastest`、`quality`、`balanced`

**2.3 响应缓存**
- 对完全相同的请求（相同 model + messages + params）缓存响应
- 缓存键：请求 body 的 hash
- TTL 可配置，默认 5 分钟
- 存储：内存 LRU 或 Redis

**2.4 Prompt 前缀去重**
- 检测公共前缀（如 system prompt），只传一次
- 与 upstream 的 prompt caching 特性配合

**2.5 安全层**
- Prompt 注入检测
- PII 检测/脱敏
- 内容审核

**涉及**：整个项目架构级重构。新增 `src/gateway/` 模块组。

---

### 3. 可观测性平台 (Observability)

**3.1 OpenTelemetry 分布式追踪**
- 每个请求生成 trace_id
- Span：proxy 接收 → upstream 请求 → 响应处理
- 导出到 Jaeger / Zipkin / Grafana Tempo

**3.2 结构化日志**
- JSON 格式输出，接入 ELK / Loki
- 日志级别动态调整

**3.3 成本分析仪表盘**
- 按 team/model/upstream 统计 token 花费和成本
- 趋势图、对比图、预算预警

**3.4 告警**
- 基于 Prometheus 指标的告警规则
- Webhook / Email / Slack 通知

**涉及**：`src/telemetry/` 模块、Prometheus 指标扩展、Admin UI 仪表盘。

---

### 4. Developer SDK & 生态

**4.1 CLI 管理工具**
- `gptload key add --upstream openai --keys "sk-xxx,sk-yyy"`
- `gptload config validate --file config.toml`
- `gptload bench --model gpt-4 --concurrent 50 --requests 1000`
- 独立 binary，通过 Admin API 交互

**4.2 Terraform Provider / K8s Operator / Client SDK**
- 新仓库、新项目。

---

### 5. 高性能数据面 (High-Performance Data Plane)

- io_uring I/O、零拷贝转发、连接池优化、SIMD 加速
- 底层重构，适合极高并发场景（10k+ RPS）

---

### 6. 多区域联邦 (Multi-Region Federation)

- 多 gptload-rs 实例跨区域部署，共享状态，智能路由
- 新增 `src/federation/` 模块、外部依赖（Redis/etcd）
