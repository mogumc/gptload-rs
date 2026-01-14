
use crate::admin;
use crate::state::{sanitize_hop_headers, RequestLogEntry, RouterState, HDR_AUTHORIZATION};
use crate::util::now_ms;
use flate2::{Decompress, FlushDecompress, Status};
use hyper::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use std::io;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio_stream::wrappers::ReceiverStream;

pub async fn serve_http(addr: SocketAddr, state: Arc<RouterState>) -> anyhow::Result<()> {
    let make_svc = make_service_fn(move |conn: &AddrStream| {
        let state = state.clone();
        let remote_addr = conn.remote_addr();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let state = state.clone();
                async move { Ok::<_, Infallible>(handle(req, state, remote_addr).await) }
            }))
        }
    });

    let server = Server::bind(&addr)
        .tcp_nodelay(true)
        .serve(make_svc);

    server.await?;
    Ok(())
}

async fn handle(
    req: Request<Body>,
    state: Arc<RouterState>,
    client_addr: SocketAddr,
) -> Response<Body> {
    let path = req.uri().path().to_string();

    // Health check.
    if req.method() == hyper::Method::GET && path == "/health" {
        return Response::new(Body::from("ok"));
    }

    // Admin UI/API.
    if path.starts_with("/admin") {
        return admin::handle_admin(req, state).await;
    }

    let start = Instant::now();
    let client_ip = client_addr.ip().to_string();
    let method = req.method().clone();

    // Proxy traffic auth (optional).
    if !state.authorize_proxy(&req) {
        let resp = RouterState::json_error(
            http::StatusCode::UNAUTHORIZED,
            "missing or invalid X-Proxy-Token",
            "proxy_unauthorized",
        );
        let ctx = RequestLogContext {
            start,
            client_ip,
            method: method.to_string(),
            path,
            model: None,
            upstream_id: None,
            req_bytes: 0,
        };
        record_request(&state, &ctx, resp.status().as_u16(), 0, None);
        return resp;
    }

    let billing_key = match extract_api_key(req.headers()) {
        Some(key) => key,
        None => {
            let resp = RouterState::json_error(
                http::StatusCode::UNAUTHORIZED,
                "missing api key",
                "api_key_required",
            );
            let ctx = RequestLogContext {
                start,
                client_ip,
                method: method.to_string(),
                path,
                model: None,
                upstream_id: None,
                req_bytes: 0,
            };
            record_request(&state, &ctx, resp.status().as_u16(), 0, None);
            return resp;
        }
    };

    let balance = match state.billing.get_balance(&billing_key) {
        Some(b) => b,
        None => {
            let resp = RouterState::json_error(
                http::StatusCode::UNAUTHORIZED,
                "invalid api key",
                "api_key_invalid",
            );
            let ctx = RequestLogContext {
                start,
                client_ip,
                method: method.to_string(),
                path,
                model: None,
                upstream_id: None,
                req_bytes: 0,
            };
            record_request(&state, &ctx, resp.status().as_u16(), 0, None);
            return resp;
        }
    };

    if balance < 0 {
        let resp = RouterState::json_error(
            http::StatusCode::UNAUTHORIZED,
            "insufficient balance",
            "balance_insufficient",
        );
        let ctx = RequestLogContext {
            start,
            client_ip,
            method: method.to_string(),
            path,
            model: None,
            upstream_id: None,
            req_bytes: 0,
        };
        record_request(&state, &ctx, resp.status().as_u16(), 0, None);
        return resp;
    }

    // Stats: request start.
    state.stats.requests_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    state.stats.requests_inflight.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let t0 = Instant::now();

    let now = now_ms();
    let resp = if req.method() == hyper::Method::GET
        && (path == "/v1/models" || path == "/v1/models/")
    {
        let (resp, resp_bytes) = models_list(&state);
        let ctx = RequestLogContext {
            start,
            client_ip,
            method: method.to_string(),
            path,
            model: None,
            upstream_id: None,
            req_bytes: 0,
        };
        record_request(&state, &ctx, resp.status().as_u16(), resp_bytes, None);
        resp
    } else {
        forward(
            req,
            state.clone(),
            now,
            start,
            client_ip,
            method,
            path,
            billing_key,
        )
        .await
    };

    // Stats: latency + inflight.
    let dur = t0.elapsed();
    state.record_latency(dur.as_nanos() as u64);
    state.stats.requests_inflight.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);

    resp
}

async fn forward(
    req: Request<Body>,
    state: Arc<RouterState>,
    now_ms: u64,
    start: Instant,
    client_ip: String,
    method: hyper::Method,
    path: String,
    billing_key: String,
) -> Response<Body> {
    const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

    let (parts, body) = req.into_parts();

    // Extract URI and method early (before moving parts)
    let original_pq = parts
        .uri
        .path_and_query()
        .cloned()
        .unwrap_or_else(|| http::uri::PathAndQuery::from_static("/"));
    let path_model = original_pq
        .path()
        .strip_prefix("/v1/models/")
        .and_then(|s| if s.is_empty() { None } else { Some(s.to_string()) });
    let out_method = parts.method.clone();
    let version = parts.version;
    let headers = parts.headers.clone();

    // Read body into bytes for potential retries (necessary for 429 retry)
    use hyper::body::HttpBody;
    let mut body_bytes = Vec::new();
    let mut body_reader = body;
    while let Some(chunk_result) = body_reader.data().await {
        match chunk_result {
            Ok(chunk) => {
                if body_bytes.len().saturating_add(chunk.len()) > MAX_REQUEST_BODY_BYTES {
                    return RouterState::json_error(
                        http::StatusCode::PAYLOAD_TOO_LARGE,
                        "request body too large",
                        "body_too_large",
                    );
                }
                body_bytes.extend_from_slice(&chunk);
            }
            Err(_) => {
                return RouterState::json_error(
                    http::StatusCode::BAD_GATEWAY,
                    "failed to read request body",
                    "body_read_error",
                );
            }
        }
    }
    let body_bytes = bytes::Bytes::from(body_bytes);
    let req_bytes = body_bytes.len();

    let mut req_json = parse_request_json(&headers, &body_bytes);
    let mut model = req_json
        .as_ref()
        .and_then(|v| v.get("model"))
        .and_then(|m| m.as_str())
        .map(|s| s.to_string());
    if model.is_none() {
        model = path_model;
    }

    let stream_request = req_json
        .as_ref()
        .and_then(|v| v.get("stream"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let is_chat_completions = path == "/v1/chat/completions" || path == "/v1/chat/completions/";

    let mut log_ctx = RequestLogContext {
        start,
        client_ip,
        method: method.to_string(),
        path,
        model: model.clone(),
        upstream_id: None,
        req_bytes,
    };

    let Some(model) = model else {
        let resp = RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "missing model",
            "model_required",
        );
        record_request(&state, &log_ctx, resp.status().as_u16(), 0, None);
        return resp;
    };

    let mut sel = if !state.model_exists(&model) {
        let resp = RouterState::json_error(
            http::StatusCode::NOT_FOUND,
            "model not found",
            "model_not_found",
        );
        record_request(&state, &log_ctx, resp.status().as_u16(), 0, None);
        return resp;
    } else if let Some(sel) = state.select_for_model(&model, now_ms) {
        sel
    } else {
        let resp = RouterState::json_error(
            http::StatusCode::SERVICE_UNAVAILABLE,
            "no available upstream keys for model",
            "model_unavailable",
        );
        record_request(&state, &log_ctx, resp.status().as_u16(), 0, None);
        return resp;
    };

    let mut body_bytes = body_bytes;
    let mut injected = false;
    if stream_request && is_chat_completions && state.should_inject_usage(sel.upstream.id.as_ref())
    {
        if let Some(ref mut json) = req_json {
            if ensure_stream_usage(json) {
                if let Ok(encoded) = serde_json::to_vec(json) {
                    body_bytes = bytes::Bytes::from(encoded);
                    injected = true;
                }
            }
        }
    }

    // Maximum retries on 429 (rate limit)
    const MAX_RETRIES: usize = 5;
    let mut retry_count = 0;

    loop {
        log_ctx.upstream_id = Some(sel.upstream.id.to_string());
        let upstream = &sel.upstream;

        let uri = match upstream.build_uri(&original_pq) {
            Ok(u) => u,
            Err(_) => {
                return RouterState::json_error(
                    http::StatusCode::BAD_GATEWAY,
                    "invalid upstream URI",
                    "invalid_upstream_uri",
                );
            }
        };

        // Build a new request for this attempt using the builder pattern
        let mut builder = hyper::Request::builder()
            .method(out_method.clone())
            .uri(uri)
            .version(version);

        // Copy headers and sanitize
        for (name, value) in headers.iter() {
            builder = builder.header(name.clone(), value.clone());
        }

        let mut out_req = match builder.body(Body::from(body_bytes.clone())) {
            Ok(req) => req,
            Err(_) => {
                return RouterState::json_error(
                    http::StatusCode::BAD_GATEWAY,
                    "failed to build request",
                    "request_build_error",
                );
            }
        };

        // Strip hop-by-hop headers & proxy/admin auth, and replace Authorization.
        sanitize_hop_headers(out_req.headers_mut());
        out_req.headers_mut().remove(HDR_AUTHORIZATION);
        out_req.headers_mut().insert(HDR_AUTHORIZATION, sel.key.auth_header.clone());
        if injected {
            out_req.headers_mut().remove(CONTENT_LENGTH);
            if let Ok(v) = http::HeaderValue::from_str(&body_bytes.len().to_string()) {
                out_req.headers_mut().insert(CONTENT_LENGTH, v);
            }
        }

        // Enforce timeout.
        let res = tokio::time::timeout(state.request_timeout, state.client.request(out_req)).await;

        match res {
            Ok(Ok(up_resp)) => {
                let status = up_resp.status();
                state.on_upstream_status(&sel, status, now_ms);

                // Check if we should retry on 429 with another key
                if status == http::StatusCode::TOO_MANY_REQUESTS && retry_count < MAX_RETRIES {
                    // Try to select an alternative key
                    let next = state.select_for_model(&model, now_ms);
                    if let Some(new_sel) = next {
                        retry_count += 1;
                        sel = new_sel;
                        // Continue loop to retry with new key
                        continue;
                    }
                }

                return proxy_upstream_response(
                    up_resp,
                    state.clone(),
                    log_ctx,
                    stream_request,
                    Some(billing_key.clone()),
                );
            }
            Ok(Err(_e)) => {
                state.on_network_error(&sel, now_ms);
                let resp = RouterState::json_error(
                    http::StatusCode::BAD_GATEWAY,
                    "upstream request failed",
                    "upstream_error",
                );
                record_request(&state, &log_ctx, resp.status().as_u16(), 0, None);
                return resp;
            }
            Err(_) => {
                state.on_timeout(&sel, now_ms);
                let resp = RouterState::json_error(
                    http::StatusCode::GATEWAY_TIMEOUT,
                    "upstream request timeout",
                    "upstream_timeout",
                );
                record_request(&state, &log_ctx, resp.status().as_u16(), 0, None);
                return resp;
            }
        }
    }
}

#[derive(Clone)]
struct RequestLogContext {
    start: Instant,
    client_ip: String,
    method: String,
    path: String,
    model: Option<String>,
    upstream_id: Option<String>,
    req_bytes: usize,
}

#[derive(Clone, Copy)]
struct UsageTokens {
    prompt: u64,
    completion: u64,
    total: u64,
}

fn record_request(
    state: &RouterState,
    ctx: &RequestLogContext,
    status: u16,
    resp_bytes: usize,
    usage: Option<UsageTokens>,
) {
    let entry = RequestLogEntry {
        ts_ms: now_ms(),
        client_ip: ctx.client_ip.clone(),
        method: ctx.method.clone(),
        path: ctx.path.clone(),
        model: ctx.model.clone(),
        upstream_id: ctx.upstream_id.clone(),
        status,
        latency_ms: ctx.start.elapsed().as_millis() as u64,
        req_bytes: ctx.req_bytes,
        resp_bytes,
        prompt_tokens: usage.map(|u| u.prompt),
        completion_tokens: usage.map(|u| u.completion),
        total_tokens: usage.map(|u| u.total),
    };
    state.record_request(entry);
}

fn proxy_upstream_response(
    up_resp: Response<Body>,
    state: Arc<RouterState>,
    log_ctx: RequestLogContext,
    stream_request: bool,
    billing_key: Option<String>,
) -> Response<Body> {
    let (mut parts, body) = up_resp.into_parts();
    sanitize_hop_headers(&mut parts.headers);

    let status = parts.status;
    let content_type = parts
        .headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let content_encoding = parts
        .headers
        .get(CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let is_event_stream = content_type.starts_with("text/event-stream");
    let want_sse_usage = stream_request && is_event_stream;
    let want_json_usage = !stream_request
        || (content_type.starts_with("application/json") && !want_sse_usage);
    let want_usage = want_sse_usage || want_json_usage;

    let mut decoder = if want_usage && content_encoding.contains("gzip") {
        Some(GzipDecoder::new())
    } else {
        None
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, io::Error>>(32);
    tokio::spawn(async move {
        use hyper::body::HttpBody;
        const MAX_PARSE_BYTES: usize = 32 * 1024 * 1024;

        let mut resp_bytes = 0usize;
        let mut usage: Option<UsageTokens> = None;
        let mut parse_enabled = want_usage;
        let mut sse_buf = String::new();
        let mut json_buf: Vec<u8> = Vec::new();
        let mut json_overflow = false;

        let mut body = body;
        while let Some(chunk) = body.data().await {
            match chunk {
                Ok(chunk) => {
                    resp_bytes = resp_bytes.saturating_add(chunk.len());
                    if tx.send(Ok(chunk.clone())).await.is_err() {
                        break;
                    }

                    if !parse_enabled || usage.is_some() {
                        continue;
                    }

                    let parse_bytes = if let Some(dec) = decoder.as_mut() {
                        match dec.decompress_chunk(&chunk) {
                            Ok(out) => out,
                            Err(_) => {
                                parse_enabled = false;
                                Vec::new()
                            }
                        }
                    } else {
                        chunk.to_vec()
                    };

                    if parse_bytes.is_empty() {
                        continue;
                    }

                    if want_sse_usage {
                        if let Some(found) = parse_sse_usage(&mut sse_buf, &parse_bytes) {
                            usage = Some(found);
                        }
                    } else if want_json_usage && !json_overflow {
                        if json_buf.len().saturating_add(parse_bytes.len()) > MAX_PARSE_BYTES {
                            json_overflow = true;
                            continue;
                        }
                        json_buf.extend_from_slice(&parse_bytes);
                    }
                }
                Err(_) => break,
            }
        }

        if usage.is_none() && want_json_usage && !json_overflow {
            usage = usage_from_json_bytes(&json_buf);
        }

        if let (Some(key), Some(found)) = (billing_key.as_deref(), usage) {
            let _ = state.billing.apply_usage(key, found.total);
        }
        record_request(&state, &log_ctx, status.as_u16(), resp_bytes, usage);
    });

    Response::from_parts(parts, Body::wrap_stream(ReceiverStream::new(rx)))
}

fn extract_api_key(headers: &hyper::HeaderMap) -> Option<String> {
    if let Some(h) = headers.get("x-api-key") {
        if let Ok(s) = h.to_str() {
            let key = s.trim();
            if !key.is_empty() {
                return Some(key.to_string());
            }
        }
    }
    if let Some(h) = headers.get(HDR_AUTHORIZATION) {
        if let Ok(s) = h.to_str() {
            let raw = s.trim();
            if raw.is_empty() {
                return None;
            }
            let key = raw
                .strip_prefix("Bearer ")
                .or_else(|| raw.strip_prefix("bearer "))
                .unwrap_or(raw)
                .trim();
            if !key.is_empty() {
                return Some(key.to_string());
            }
        }
    }
    None
}

fn models_list(state: &RouterState) -> (Response<Body>, usize) {
    let routes = state.get_model_routes();
    let mut models: Vec<String> = routes.models.keys().cloned().collect();
    models.sort();

    let data: Vec<serde_json::Value> = models
        .iter()
        .map(|id| serde_json::json!({ "id": id, "object": "model" }))
        .collect();

    let body = serde_json::json!({
        "object": "list",
        "data": data
    });

    let body_str = body.to_string();
    let resp = Response::builder()
        .status(http::StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body_str.clone()))
        .unwrap_or_else(|_| RouterState::json_error(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "failed to build response",
            "response_build_error",
        ));
    (resp, body_str.len())
}

fn parse_request_json(
    headers: &hyper::HeaderMap,
    body: &bytes::Bytes,
) -> Option<serde_json::Value> {
    if body.is_empty() {
        return None;
    }
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !content_type.is_empty() && !content_type.starts_with("application/json") {
        return None;
    }
    serde_json::from_slice(body).ok()
}

fn ensure_stream_usage(v: &mut serde_json::Value) -> bool {
    let obj = match v.as_object_mut() {
        Some(obj) => obj,
        None => return false,
    };
    let stream = obj
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !stream {
        return false;
    }
    let opts = obj
        .entry("stream_options")
        .or_insert_with(|| serde_json::json!({}));
    let opts_obj = match opts.as_object_mut() {
        Some(obj) => obj,
        None => return false,
    };
    let entry = opts_obj
        .entry("include_usage")
        .or_insert(serde_json::Value::Bool(true));
    match entry.as_bool() {
        Some(true) => false,
        _ => {
            *entry = serde_json::Value::Bool(true);
            true
        }
    }
}

fn usage_from_json_bytes(body: &[u8]) -> Option<UsageTokens> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    extract_usage_from_value(&v)
}

fn extract_usage_from_value(v: &serde_json::Value) -> Option<UsageTokens> {
    let usage = v.get("usage")?;
    let prompt = usage.get("prompt_tokens").and_then(|v| v.as_u64());
    let completion = usage.get("completion_tokens").and_then(|v| v.as_u64());
    let total = usage.get("total_tokens").and_then(|v| v.as_u64()).or_else(|| {
        match (prompt, completion) {
            (Some(p), Some(c)) => Some(p + c),
            _ => None,
        }
    });

    if prompt.is_none() && completion.is_none() && total.is_none() {
        return None;
    }

    Some(UsageTokens {
        prompt: prompt.unwrap_or(0),
        completion: completion.unwrap_or(0),
        total: total.unwrap_or(0),
    })
}

fn parse_sse_usage(buf: &mut String, chunk: &[u8]) -> Option<UsageTokens> {
    let mut found = None;
    let text = String::from_utf8_lossy(chunk);
    buf.push_str(&text);

    while let Some(pos) = buf.find('\n') {
        let line = buf[..pos].trim_end_matches('\r').to_string();
        buf.drain(..=pos);
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data == "[DONE]" {
            continue;
        }
        if !data.contains("\"usage\"") {
            continue;
        }
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
            if let Some(u) = extract_usage_from_value(&v) {
                found = Some(u);
            }
        }
    }
    found
}

struct GzipDecoder {
    decompressor: Decompress,
}

impl GzipDecoder {
    fn new() -> Self {
        Self {
            decompressor: Decompress::new(true),
        }
    }

    fn decompress_chunk(&mut self, input: &[u8]) -> Result<Vec<u8>, io::Error> {
        let mut out = Vec::new();
        let mut offset = 0usize;
        while offset < input.len() {
            let mut buf = [0u8; 8192];
            let in_before = self.decompressor.total_in();
            let out_before = self.decompressor.total_out();
            let status = self
                .decompressor
                .decompress(&input[offset..], &mut buf, FlushDecompress::None)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let in_after = self.decompressor.total_in();
            let out_after = self.decompressor.total_out();
            let used_in = (in_after - in_before) as usize;
            let produced = (out_after - out_before) as usize;
            offset = offset.saturating_add(used_in);
            if produced > 0 {
                out.extend_from_slice(&buf[..produced]);
            }
            if status == Status::StreamEnd {
                break;
            }
            if used_in == 0 && produced == 0 {
                break;
            }
        }
        Ok(out)
    }
}
