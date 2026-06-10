# gptload-rs

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
![Rust](https://img.shields.io/badge/Rust-2021-orange)

高性能 OpenAI 格式 API 透明代理，Rust 编写。支持多上游多密钥负载均衡、智能故障转移、实时监控和管理后台。

> ⚡ **极高吞吐** - 单机 1,700+ RPS，毫秒级延迟  
> 🔐 **智能故障转移** - 二元 key 状态（active/invalid），自动恢复  
> 🛡️ **并发保护** - Per-key 并发限制，防止上游 429  
> 📊 **实时监控** - SSE 流式推送，零轮询  
> 🎛️ **管理后台** - Web UI + REST API，支持批量操作

---

## 核心特性

### Key 管理（gpt-load 风格）

采用 **active/invalid 二元状态** 模型，取代传统的 cooldown 定时器：

- **401/403** → 失败计数 +1，达到阈值标记 invalid
- **429** → 不惩罚 key，重试其他 key 即可
- **5xx** → 不惩罚 key，重试其他 upstream
- **自动恢复** → 后台定时验证 invalid key，通过则恢复
- **无竞态** → 纯原子操作，无 Mutex、无 cooldown 定时器

### 并发保护

每个 key 有并发上限（默认 5），防止上游限流：

```toml
[key]
max_concurrent_per_key = 5  # 全局默认

[[upstreams]]
id = "strict-provider"
max_concurrent_per_key = 3  # 该渠道限流严格，用更小的值

[[upstreams]]
id = "generous-provider"
max_concurrent_per_key = 20  # 该渠道限流宽松
```

超限请求立即返回 503，不浪费上游资源。

### 计费预留

采用**预留-结算**模式，防止余额超扣：

1. 请求前：`reserve_request()` 原子扣减 1
2. 成功后：`settle_reserved_usage()` 按实际 token 结算
3. 失败时：`release_reservation()` 归还预留

---

## 快速开始

### 构建

```bash
# 安装依赖（Ubuntu/Debian）
sudo apt-get install -y build-essential pkg-config libssl-dev

# 构建发布版本
cargo build --release
```

### 配置

```bash
cp config.example.toml config.toml
```

**config.toml 示例：**

```toml
listen_addr = "0.0.0.0:8080"
request_timeout_ms = 60000
max_retries = 5
retry_status_codes = [429, 500, 502, 503, 504]
admin_tokens = ["your-admin-token"]
data_dir = "./data"

[server]
graceful_shutdown_timeout_secs = 30
cors_origins = ["*"]
queue_enabled = false
queue_max_depth = 100
queue_timeout_ms = 10000

[key]
blacklist_threshold = 3           # 401/403 失败多少次后标记 invalid
max_concurrent_per_key = 5        # 每个 key 最大并发
rate_limit_cooldown_ms = 3000     # 无 Retry-After 时的 429 冷却
max_rate_limit_cooldown_ms = 30000 # Retry-After 冷却上限
revalidation_interval_secs = 300  # 多久验证一次 invalid key
revalidation_timeout_secs = 20    # 验证请求超时

[[upstreams]]
id = "openai"
base_url = "https://api.openai.com"
weight = 1
format = "openai"                 # openai / anthropic / gemini，可自动检测
# proxy = "socks5://127.0.0.1:1080"

[[upstreams]]
id = "xiaomimimo"
base_url = "https://token-plan-sgp.xiaomimimo.com"
weight = 1
max_concurrent_per_key = 3  # 该渠道限流严格
```

### 启动

```bash
RUST_LOG=info ./target/release/gptload-rs --config config.toml
```

启动后：
- **管理后台**: http://127.0.0.1:8080/web/
- **健康检查**: http://127.0.0.1:8080/health

---

## 使用

### 客户端调用

```python
from openai import OpenAI

client = OpenAI(
    api_key="your-billing-key",  # 需先通过 admin API 创建
    base_url="http://localhost:8080"
)

response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Hello!"}]
)
```

### 健康检查

```bash
curl http://localhost:8080/health
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

## 管理 API

所有 API 需要 `X-Admin-Token` 请求头。

### Upstream 管理

```bash
# 列出 upstream
curl http://localhost:8080/admin/api/v1/upstreams -H "X-Admin-Token: xxx"

# 添加 upstream
curl -X POST http://localhost:8080/admin/api/v1/upstreams \
  -H "X-Admin-Token: xxx" \
  -H "Content-Type: application/json" \
  -d '{"id":"new","base_url":"https://api.example.com","weight":1}'

# 删除 upstream
curl -X DELETE http://localhost:8080/admin/api/v1/upstreams/new \
  -H "X-Admin-Token: xxx"
```

### Key 管理

```bash
# 添加 key（JSON）
curl -X POST http://localhost:8080/admin/api/v1/upstreams/openai/keys \
  -H "X-Admin-Token: xxx" \
  -H "Content-Type: application/json" \
  -d '{"keys":["sk-xxx1","sk-xxx2"]}'

# 添加 key（纯文本，每行一个）
curl -X POST http://localhost:8080/admin/api/v1/upstreams/openai/keys \
  -H "X-Admin-Token: xxx" \
  -H "Content-Type: text/plain" \
  -d @keys.txt

# 替换所有 key
curl -X PUT http://localhost:8080/admin/api/v1/upstreams/openai/keys \
  -H "X-Admin-Token: xxx" \
  -H "Content-Type: application/json" \
  -d '{"keys":["sk-xxx1","sk-xxx2"]}'

# 删除指定 key
curl -X DELETE http://localhost:8080/admin/api/v1/upstreams/openai/keys \
  -H "X-Admin-Token: xxx" \
  -H "Content-Type: application/json" \
  -d '{"keys":["sk-xxx1"]}'

# 查看 key 状态（分页）
curl "http://localhost:8080/admin/api/v1/upstreams/openai/keys?offset=0&limit=100" \
  -H "X-Admin-Token: xxx"
```

### Key 状态操作

```bash
# 恢复指定 key（从 invalid → active）
curl -X POST http://localhost:8080/admin/api/v1/upstreams/openai/keys/release \
  -H "X-Admin-Token: xxx" \
  -H "Content-Type: application/json" \
  -d '{"keys":["sk-xxx1","sk-xxx2"]}'

# 恢复所有 invalid key
curl -X POST http://localhost:8080/admin/api/v1/upstreams/openai/keys/release \
  -H "X-Admin-Token: xxx" \
  -H "Content-Type: application/json" \
  -d '{"all":true}'

# 标记 key 为 invalid
curl -X POST http://localhost:8080/admin/api/v1/upstreams/openai/keys/invalidate \
  -H "X-Admin-Token: xxx" \
  -H "Content-Type: application/json" \
  -d '{"keys":["sk-xxx1"]}'
```

### 计费管理

```bash
# 创建计费 key
curl -X POST http://localhost:8080/admin/api/v1/billing/keys \
  -H "X-Admin-Token: xxx" \
  -H "Content-Type: application/json" \
  -d '{"key":"my-api-key","balance":100000}'

# 查询余额
curl http://localhost:8080/admin/api/v1/billing/keys/my-api-key \
  -H "X-Admin-Token: xxx"

# 调整余额
curl -X POST http://localhost:8080/admin/api/v1/billing/keys/my-api-key/adjust \
  -H "X-Admin-Token: xxx" \
  -H "Content-Type: application/json" \
  -d '{"delta":50000}'

# 删除计费 key
curl -X DELETE http://localhost:8080/admin/api/v1/billing/keys/my-api-key \
  -H "X-Admin-Token: xxx"
```

### 模型路由

```bash
# 查看模型路由
curl http://localhost:8080/admin/api/v1/models/routes -H "X-Admin-Token: xxx"

# 设置模型路由
curl -X PUT http://localhost:8080/admin/api/v1/models/routes \
  -H "X-Admin-Token: xxx" \
  -H "Content-Type: application/json" \
  -d '{"upstreams":{"openai":["gpt-4o","gpt-4o-mini"]}}'

# 刷新模型（从上游获取）
curl -X POST http://localhost:8080/admin/api/v1/upstreams/openai/models/refresh \
  -H "X-Admin-Token: xxx"
```

### 热加载

```bash
# 从数据库重建内存索引
curl -X POST http://localhost:8080/admin/api/v1/reload \
  -H "X-Admin-Token: xxx"
```

### 实时统计（SSE）

```bash
curl "http://localhost:8080/admin/api/v1/stats/stream?token=your-admin-token"
```

---

## 请求处理流程

```
客户端请求
    ↓
[proxy.rs] 路由
  ├─ /health → 返回系统状态
  ├─ /admin → 管理 API/UI
  └─ /v1/* → 代理流程
    ↓
[proxy.rs] 认证检查（X-Proxy-Token）
    ↓
[proxy.rs] 提取客户端 billing key，检查余额
    ↓
[state.rs] select_for_model()
  ├─ 加权轮询选择 upstream
  ├─ 跳过无模型的 upstream
  ├─ select_key() 选择 active key（跳过并发达上限的）
  └─ 无可用 key → 503
    ↓
[proxy.rs] 计费预留（reserve_request）
    ↓
[proxy.rs] 替换 Authorization，发送到上游
    ↓
[proxy.rs] 处理响应
  ├─ 200 → 计费结算，转发响应
  ├─ 401/403 → 失败计数+1，重试其他 key
  ├─ 429 → 重试其他 key（不惩罚当前 key）
  ├─ 5xx → 重试其他 upstream
  └─ 超时/网络错误 → 重试其他 upstream
    ↓
[proxy.rs] 释放计费预留（如果失败）
    ↓
返回客户端
```

---

## 配置参考

### 完整配置

```toml
listen_addr = "0.0.0.0:8080"
worker_threads = 4                    # 可选，默认 CPU 核心数
request_timeout_ms = 60000
max_retries = 5
retry_status_codes = [429, 500, 502, 503, 504]
proxy_tokens = ["proxy-token-1"]      # 可选，不设则允许所有请求
admin_tokens = ["admin-token-1"]      # 必需
data_dir = "./data"
usage_inject_upstreams = ["openai"]   # 可选，注入 stream_options.include_usage

[key]
blacklist_threshold = 3               # 401/403 失败次数阈值，0=禁用
max_concurrent_per_key = 5            # 全局默认并发限制，0=不限制
revalidation_interval_secs = 300      # invalid key 验证间隔（秒）
revalidation_timeout_secs = 20        # 验证请求超时（秒）

[[upstreams]]
id = "openai"
base_url = "https://api.openai.com"
weight = 1
max_concurrent_per_key = 10           # 可选，覆盖全局默认

[[upstreams]]
id = "backup"
base_url = "https://backup-api.example.com"
weight = 2
```

---

## 架构

```
src/
├── main.rs       # 入口，Tokio 运行时
├── config.rs     # 配置加载与验证
├── state.rs      # 路由状态、key 选择、故障转移
├── proxy.rs      # HTTP 代理、重试逻辑
├── admin.rs      # 管理 API 和 Web UI
├── billing.rs    # 计费预留/结算
├── storage.rs    # sled 数据库封装
└── static/       # 前端资源
    ├── index.html
    └── app.js
```

---

## 性能

实测数据（53 个 key，100 并发）：

| 指标 | 数值 |
|------|------|
| 代理吞吐 | 1,700+ RPS |
| 平均延迟 | 58ms |
| P99 延迟 | 554ms |
| 内存占用 | ~18MB |
| CPU 占用 | ~2% |

瓶颈在上游限流（每个 key ~5 并发），不在代理本身。

---

## 许可证

MIT License
