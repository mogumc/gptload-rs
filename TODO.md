# TODO

## 支持更多 upstream 格式
- Anthropic 原生格式（Messages API）
- Gemini 原生格式（GenerateContent）
- 自动检测 upstream 类型，适配不同的请求/响应格式

## Prometheus metrics 导出
- /metrics 端点
- 请求总数、成功数、失败数（按状态码分）
- 延迟直方图（p50/p90/p99）
- 每个 upstream/key 的指标
- 并发请求数、key 状态分布
