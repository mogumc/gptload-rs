<p align="center">
  <img src="/icon.png" alt="HeartWith" width="150"/>
</p>

<h1 align="center">Aequi</h1>

<p align="center">
  高性能 OpenAI 格式 API 透明代理，Rust + Tokio 编写。支持多上游多密钥负载均衡、智能故障转移、计费管理、管理后台和实时监控。
</p>

<p align="center">
  <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="MIT License"/>
  <img src="https://img.shields.io/badge/Rust-2021-orange" alt="Rust"/>
</p>

<div align="center">

> ⚡ **1,700+ RPS** 单机吞吐，毫秒级延迟  
> 🔐 **二元 key 状态** active/invalid，无 cooldown 定时器  
> 🛡️ **并发保护** per-key 并发限制，零 429 风暴  
> 📊 **实时监控** SSE 流式推送，零轮询  
> 🎛️ **管理后台** Web UI + REST API，支持批量操作  
> 📦 **Docker 部署** ghcr.io/mogumc/aequi，开箱即用

---

## 快速开始

### Docker（推荐）

```bash
docker pull ghcr.io/mogumc/aequi:latest

docker run -d --name aequi \
  -p 3000:3000 \
  -v ./config.toml:/app/config.toml:ro \
  -v ./data:/app/data \
  ghcr.io/mogumc/aequi:latest
```

### 源码构建

```bash
# 安装依赖（Ubuntu/Debian）
sudo apt-get install -y build-essential pkg-config libssl-dev

# 构建发布版（含 LTO + strip）
cargo build --release

# 可选：启用 mimalloc 分配器（高并发下更低延迟）
cargo build --release --features mimalloc
```

### 配置

```bash
cp config.example.toml config.toml
# 填写上游地址和密钥
```

### 启动

```bash
RUST_LOG=info ./target/release/aequi --config config.toml
```

启动后：
- **管理后台**: http://127.0.0.1:3000/web/
- **健康检查**: http://127.0.0.1:3000/health
- **SIGHUP 热重载**: `kill -HUP $(pidof aequi)`（Unix）

---

## 核心特性

### Key 管理

采用 **active/invalid 二元状态** 模型，不依赖 cooldown 定时器：

| 上游响应 | 行为 |
|---------|------|
| 401/403 | 失败计数 +1，达阈值标记 invalid |
| 429 | 不惩罚 key，重试其他 key |
| 5xx | 不惩罚 key，重试其他 upstream |
| 超时/网络错误 | 重试其他 upstream |

**自动恢复**：后台定时验证 invalid key，通过则恢复为 active。纯原子操作，无 Mutex。

### 并发保护

每个 key 有并发上限，防止上游限流：

```toml
[key]
max_concurrent_per_key = 5  # 全局默认

[[upstreams]]
id = "strict-provider"
max_concurrent_per_key = 3   # 覆盖全局

[[upstreams]]
id = "generous-provider"
max_concurrent_per_key = 20
```

超限请求立即返回 503，不浪费上游资源。

### 计费系统

**预留-结算** 模式，防止余额超扣：

1. `reserve_request()` — 预扣除最低扣费额度
2. `settle_reserved_usage()` — 按实际 token 结算
3. `release_reservation()` — 失败时归还

支持模型级费率配置，按 input/output token 分别计价。

### 上游代理

支持 HTTP/SOCKS5 代理连接上游：

```toml
[[upstreams]]
id = "openai"
base_url = "https://api.openai.com"
proxy = "socks5://127.0.0.1:1080"

[[upstreams]]
id = "custom"
base_url = "https://api.example.com"
proxy = "http://proxy:8080"
```

### 热重载

Unix 系统支持 SIGHUP 信号触发热重载，运行时更新配置无需重启。

---

## 配置参考

```toml
listen_addr = "0.0.0.0:3000"
# worker_threads = 4                  # 可选，默认 CPU 核心数
request_timeout_ms = 60000
max_retries = 5
retry_status_codes = [429, 500, 502, 503, 504]
admin_tokens = ["your-admin-token"]   # 必需
data_dir = "./data"

[server]
graceful_shutdown_timeout_secs = 30
cors_origins = ["*"]
queue_enabled = false
queue_max_depth = 100
queue_timeout_ms = 10000

[key]
blacklist_threshold = 3               # 401/403 失败阈值，0=禁用
max_concurrent_per_key = 5            # 全局默认并发限制，0=不限制
rate_limit_cooldown_ms = 3000         # 429 默认冷却（无 Retry-After 时）
max_rate_limit_cooldown_ms = 30000    # Retry-After 冷却上限
revalidation_interval_secs = 300      # invalid key 验证间隔
revalidation_timeout_secs = 20        # 验证请求超时
```

---

## 管理 API

所有 API 需携带 `X-Admin-Token` 请求头。

### Upstream 管理

```bash
# 列出
curl http://localhost:3000/admin/api/v1/upstreams -H "X-Admin-Token: xxx"

# 添加
curl -X POST http://localhost:3000/admin/api/v1/upstreams \
  -H "X-Admin-Token: xxx" -H "Content-Type: application/json" \
  -d '{"id":"new","base_url":"https://api.example.com","weight":1}'

# 删除
curl -X DELETE http://localhost:3000/admin/api/v1/upstreams/new \
  -H "X-Admin-Token: xxx"
```

### Key 管理

```bash
# 添加（JSON）
curl -X POST http://localhost:3000/admin/api/v1/upstreams/openai/keys \
  -H "X-Admin-Token: xxx" -H "Content-Type: application/json" \
  -d '{"keys":["sk-xxx1","sk-xxx2"]}'

# 添加（纯文本，每行一个）
curl -X POST http://localhost:3000/admin/api/v1/upstreams/openai/keys \
  -H "X-Admin-Token: xxx" -H "Content-Type: text/plain" \
  -d @keys.txt

# 替换所有 key
curl -X PUT http://localhost:3000/admin/api/v1/upstreams/openai/keys \
  -H "X-Admin-Token: xxx" -H "Content-Type: application/json" \
  -d '{"keys":["sk-xxx1","sk-xxx2"]}'

# 删除指定 key
curl -X DELETE http://localhost:3000/admin/api/v1/upstreams/openai/keys \
  -H "X-Admin-Token: xxx" -H "Content-Type: application/json" \
  -d '{"keys":["sk-xxx1"]}'

# 查看 key 状态（分页）
curl "http://localhost:3000/admin/api/v1/upstreams/openai/keys?offset=0&limit=100" \
  -H "X-Admin-Token: xxx"
```

### Key 状态操作

```bash
# 恢复指定 key（invalid → active）
curl -X POST http://localhost:3000/admin/api/v1/upstreams/openai/keys/release \
  -H "X-Admin-Token: xxx" -H "Content-Type: application/json" \
  -d '{"keys":["sk-xxx1"]}'

# 恢复所有 invalid key
curl -X POST http://localhost:3000/admin/api/v1/upstreams/openai/keys/release \
  -H "X-Admin-Token: xxx" -H "Content-Type: application/json" \
  -d '{"all":true}'

# 标记为 invalid
curl -X POST http://localhost:3000/admin/api/v1/upstreams/openai/keys/invalidate \
  -H "X-Admin-Token: xxx" -H "Content-Type: application/json" \
  -d '{"keys":["sk-xxx1"]}'
```

### 计费管理

```bash
# 创建计费 key
curl -X POST http://localhost:3000/admin/api/v1/billing/keys \
  -H "X-Admin-Token: xxx" -H "Content-Type: application/json" \
  -d '{"key":"my-api-key","balance":100000}'

# 查询余额
curl http://localhost:3000/admin/api/v1/billing/keys/my-api-key \
  -H "X-Admin-Token: xxx"

# 调整余额
curl -X POST http://localhost:3000/admin/api/v1/billing/keys/my-api-key/adjust \
  -H "X-Admin-Token: xxx" -H "Content-Type: application/json" \
  -d '{"delta":50000}'

# 删除计费 key
curl -X DELETE http://localhost:3000/admin/api/v1/billing/keys/my-api-key \
  -H "X-Admin-Token: xxx"
```

### 模型路由

```bash
# 查看路由
curl http://localhost:3000/admin/api/v1/models/routes -H "X-Admin-Token: xxx"

# 设置路由
curl -X PUT http://localhost:3000/admin/api/v1/models/routes \
  -H "X-Admin-Token: xxx" -H "Content-Type: application/json" \
  -d '{"upstreams":{"openai":["gpt-4o","gpt-4o-mini"]}}'

# 从上游刷新模型列表
curl -X POST http://localhost:3000/admin/api/v1/upstreams/openai/models/refresh \
  -H "X-Admin-Token: xxx"
```

### 其他

```bash
# 热重载内存索引
curl -X POST http://localhost:3000/admin/api/v1/reload \
  -H "X-Admin-Token: xxx"

# 实时统计（SSE）
curl "http://localhost:3000/admin/api/v1/stats/stream?token=xxx"
```

---

## 客户端调用

```python
from openai import OpenAI

client = OpenAI(
    api_key="your-billing-key",
    base_url="http://localhost:3000"
)

response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Hello!"}]
)
```

健康检查：

```bash
curl http://localhost:3000/health
```

```json
{
    "status": "ok",
    "upstreams": 2,
    "keys_total": 100,
    "keys_active": 85,
    "requests_inflight": 12
}
```

---

## 请求处理流程

```
客户端请求
    │
    ▼
[proxy] 路由匹配
  ├─ /health     → 系统状态
  ├─ /admin      → 管理 API/UI
  └─ /v1/*       → 代理流程
       │
       ▼
[proxy] 认证检查（API Key）
       │
       ▼
[proxy] 提取 billing key，检查余额
       │
       ▼
[state] select_for_model()
  ├─ 加权轮询 upstream
  ├─ select_key() 选 active key（跳过并发达上限的）
  └─ 无可用 → 503
       │
       ▼
[proxy] 计费预留（reserve_request）
       │
       ▼
[proxy] 替换 Authorization，请求上游
       │
       ▼
[proxy] 处理响应
  ├─ 200           → 计费结算，转发
  ├─ 401/403       → 失败计数，重试其他 key
  ├─ 429           → 重试其他 key（不惩罚当前）
  ├─ 5xx           → 重试其他 upstream
  └─ 超时/网络错误  → 重试其他 upstream
       │
       ▼
[proxy] 失败时释放计费预留
       │
       ▼
返回客户端
```

---

## 架构

```
src/
├── main.rs            # 入口，Tokio 运行时、优雅关闭、SIGHUP 热重载
├── config.rs          # TOML 配置加载与验证
├── state.rs           # 路由状态、key 选择、加权轮询
├── proxy.rs           # HTTP 代理、重试逻辑、流式处理
├── admin.rs           # 管理 API、Prometheus 指标、SSE 推送
├── billing.rs         # 计费预留/结算/余额管理
├── storage.rs         # sled 嵌入式数据库封装
├── route.rs           # 模型路由匹配
├── format.rs          # OpenAI / Anthropic / Gemini 格式兼容
├── upstream_client.rs # 上游 HTTP 客户端（支持 SOCKS5/HTTP 代理）
├── util.rs            # 工具函数
└── static/
    └── dist/          # 管理后台前端（独立项目编译输出）
```

---

## 性能

实测数据（53 key，100 并发）：

| 指标 | 数值 |
|------|------|
| 代理吞吐 | 1,700+ RPS |
| 平均延迟 | 58ms |
| P99 延迟 | 554ms |
| 内存占用 | ~18MB |
| CPU 占用 | ~2% /

瓶颈在上游限流，不在代理本身。启用 `mimalloc` 特性可在高并发下进一步降低延迟。

---

## 许可证

MIT License
