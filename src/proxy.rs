use crate::admin;
use crate::billing::ReserveResult;
use crate::config::UpstreamFormat;
use crate::format::{self, AuthStyle};
use crate::state::{
    sanitize_hop_headers, KeyGuard, RequestLogEntry, RouterState, Selected, HDR_AUTHORIZATION,
};
use crate::util::now_ms;
use flate2::{Decompress, FlushDecompress, Status};
use hyper::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, ORIGIN};
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tracing::Instrument;

static REQUEST_LOG_ID: AtomicU64 = AtomicU64::new(1);

/// RAII guard: tracks inflight count and request latency.
/// For streaming responses, moved into the spawned body-consumption task
/// so the inflight counter stays accurate until the stream finishes.
struct RequestLifecycle {
    state: Arc<RouterState>,
    start: Instant,
    active: bool,
}

impl RequestLifecycle {
    fn start(state: Arc<RouterState>) -> Self {
        state.stats.requests_total.fetch_add(1, Ordering::Relaxed);
        state.stats.requests_inflight.fetch_add(1, Ordering::Relaxed);
        Self {
            state,
            start: Instant::now(),
            active: true,
        }
    }

    fn finish(&mut self) {
        if !self.active {
            return;
        }
        let dur = self.start.elapsed();
        self.state.record_latency(dur.as_nanos() as u64);
        self.state.stats.requests_inflight.fetch_sub(1, Ordering::Relaxed);
        self.active = false;
    }
}

impl Drop for RequestLifecycle {
    fn drop(&mut self) {
        self.finish();
    }
}

/// Resolve the real client IP from headers set by reverse proxies (nginx, etc.).
///
/// Checks `X-Forwarded-For` (takes the leftmost non-trusted IP),
/// then `X-Real-IP`, falling back to the TCP peer address.
fn resolve_client_ip(req: &hyper::HeaderMap, peer: SocketAddr) -> String {
    // X-Forwarded-For: client, proxy1, proxy2, ...
    if let Some(xff) = req
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
    {
        // Take the leftmost address (original client).
        let first = xff.split(',').next().unwrap_or("").trim();
        if !first.is_empty() {
            return first.to_string();
        }
    }

    // X-Real-IP (nginx single-IP alternative).
    if let Some(xri) = req
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
    {
        return xri.trim().to_string();
    }

    // Fall back to TCP peer address.
    peer.ip().to_string()
}

// ── Route helpers ─────────────────────────────────────────────────────

/// Health-check endpoint: returns upstream/key counts and inflight requests.
fn check_health(state: &Arc<RouterState>) -> Response<Body> {
    let snap = state.snapshot.load_full();
    let mut total_keys = 0usize;
    let mut active_keys = 0usize;
    for u in snap.upstreams.iter() {
        let keys = u.keys.load_full();
        total_keys += keys.len();
        active_keys += keys.iter().filter(|k| k.is_active()).count();
    }
    let inflight = state
        .stats
        .requests_inflight
        .load(std::sync::atomic::Ordering::Relaxed);
    let body = serde_json::json!({
        "status": "ok",
        "upstreams": snap.upstreams.len(),
        "keys_total": total_keys,
        "keys_active": active_keys,
        "requests_inflight": inflight,
    });
    Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// Authenticate a proxy request: proxy token → API key → balance check.
/// Returns the billing key on success, or an error response.
fn authenticate_request(
    state: &Arc<RouterState>,
    req: &Request<Body>,
    ctx: &RequestLogContext,
) -> Result<String, Response<Body>> {
    if !state.authorize_proxy(req) {
        return Err(logged_json_error(
            state,
            ctx,
            http::StatusCode::UNAUTHORIZED,
            "missing or invalid X-Proxy-Token",
            "proxy_unauthorized",
        ));
    }

    let billing_key = match extract_api_key(req.headers()) {
        Some(key) => key,
        None => {
            return Err(logged_json_error(
                state,
                ctx,
                http::StatusCode::UNAUTHORIZED,
                "missing api key",
                "api_key_required",
            ));
        }
    };

    let balance = match state.billing.get_balance(&billing_key) {
        Some(b) => b,
        None => {
            return Err(logged_json_error(
                state,
                ctx,
                http::StatusCode::UNAUTHORIZED,
                "invalid api key",
                "api_key_invalid",
            ));
        }
    };

    if balance <= 0 && balance != -1 {
        return Err(logged_json_error(
            state,
            ctx,
            http::StatusCode::UNAUTHORIZED,
            "insufficient balance",
            "balance_insufficient",
        ));
    }

    Ok(billing_key)
}

// ── HTTP server ───────────────────────────────────────────────────────

pub async fn serve_http<F>(
    addr: SocketAddr,
    state: Arc<RouterState>,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
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
        .serve(make_svc)
        .with_graceful_shutdown(shutdown);

    server.await?;
    Ok(())
}

async fn handle(
    req: Request<Body>,
    state: Arc<RouterState>,
    client_addr: SocketAddr,
) -> Response<Body> {
    let origin = req.headers().get(ORIGIN).cloned();
    if req.method() == hyper::Method::OPTIONS && origin.is_some() {
        return cors_preflight(&state, origin.as_ref());
    }

    let resp = handle_inner(req, state.clone(), client_addr).await;
    add_cors_headers(resp, &state, origin.as_ref())
}

async fn handle_inner(
    req: Request<Body>,
    state: Arc<RouterState>,
    client_addr: SocketAddr,
) -> Response<Body> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();
    let client_ip = resolve_client_ip(req.headers(), client_addr);

    let span = tracing::info_span!(
        "proxy.request",
        http.method = %method,
        http.url = %path,
        net.peer.ip = %client_ip,
    );

    async move {
    // Health check.
    if method == hyper::Method::GET && path == "/health" {
        return check_health(&state);
    }

    // Root redirect to admin web UI.
    if (method == hyper::Method::GET || method == hyper::Method::HEAD) && (path == "/" || path == "") {
        return Response::builder()
            .status(http::StatusCode::FOUND)
            .header(http::header::LOCATION, "/web")
            .body(Body::empty())
            .unwrap();
    }

    // Prometheus metrics.
    if method == hyper::Method::GET && path == "/metrics" {
        return admin::prometheus_metrics(state).await;
    }

    // Admin UI/API.
    if path.starts_with("/admin") || path.starts_with("/web") {
        return crate::route::handle_admin(req, state).await;
    }

    if state.is_shutting_down() {
        return RouterState::json_error(
            http::StatusCode::SERVICE_UNAVAILABLE,
            "server is shutting down",
            "shutting_down",
        );
    }

    let start = Instant::now();
    let mut base_log_ctx = RequestLogContext::new(
        start,
        client_ip.clone(),
        method.to_string(),
        path.clone(),
        None,
        None,
        0,
        None,
        None,
        0,
    );

    // Authenticate (proxy token → API key → balance).
    let billing_key = match authenticate_request(&state, &req, &base_log_ctx) {
        Ok(key) => key,
        Err(resp) => return resp,
    };
    base_log_ctx.billing_key = Some(billing_key.clone());

    // Stats: request start. The guard is moved into proxied response streams
    // so streaming requests stay inflight until the body finishes or is dropped.
    let lifecycle = RequestLifecycle::start(state.clone());

    let resp =
        if req.method() == hyper::Method::GET && (path == "/v1/models" || path == "/v1/models/") {
            let (resp, resp_bytes) = models_list(&state, &billing_key);
            record_request(
                &state,
                &base_log_ctx,
                resp.status().as_u16(),
                resp_bytes,
                None,
            );
            resp
        } else {
            forward(
                req,
                state.clone(),
                lifecycle,
                start,
                client_ip,
                method,
                path,
                billing_key,
            )
            .await
        };

    resp
    }
    .instrument(span)
    .await
}

enum AttemptResult {
    /// Request completed (success or final failure) — return response to client.
    Success(Response<Body>),
    /// Retry with a different key/upstream.
    Retry(Selected),
}

async fn execute_attempt(
    state: &Arc<RouterState>,
    sel: &Selected,
    upstream: &Arc<crate::state::Upstream>,
    original_pq: &http::uri::PathAndQuery,
    out_method: &hyper::Method,
    version: http::Version,
    headers: &hyper::HeaderMap,
    body_bytes: &bytes::Bytes,
    injected: bool,
    billing_reserved: &mut bool,
    billing_key: &str,
    log_ctx: &RequestLogContext,
    stream_request: bool,
    lifecycle: &mut Option<RequestLifecycle>,
) -> AttemptResult {
    let now = now_ms();
    let attempt_start = Instant::now();

    let model = log_ctx.model.as_deref().unwrap_or_default();
    let adapted = match format::adapt_request(
        upstream.format,
        original_pq,
        out_method,
        body_bytes,
        model,
        sel.key.key.as_ref(),
    ) {
        Ok(adapted) => adapted,
        Err(resp) => {
            if *billing_reserved {
                let _ = state.billing.release_reservation(billing_key);
            }
            return AttemptResult::Success(logged_response(state, log_ctx, resp));
        }
    };

    let uri = match upstream.build_uri(&adapted.path_and_query) {
        Ok(u) => u,
        Err(_) => {
            if *billing_reserved {
                let _ = state.billing.release_reservation(billing_key);
            }
            return AttemptResult::Success(RouterState::json_error(
                http::StatusCode::BAD_GATEWAY,
                "invalid upstream URI",
                "invalid_upstream_uri",
            ));
        }
    };

    let out_req = match build_upstream_request(
        adapted.method,
        uri,
        version,
        headers,
        adapted.body,
        sel,
        injected,
        adapted.auth_style,
    ) {
        Ok(req) => req,
        Err(resp) => {
            if *billing_reserved {
                let _ = state.billing.release_reservation(billing_key);
            }
            return AttemptResult::Success(logged_response(state, log_ctx, resp));
        }
    };

    if !*billing_reserved {
        match state.billing.reserve_request(billing_key) {
            ReserveResult::Reserved => {
                *billing_reserved = true;
            }
            ReserveResult::Insufficient => {
                return AttemptResult::Success(logged_json_error(
                    state,
                    log_ctx,
                    http::StatusCode::UNAUTHORIZED,
                    "insufficient balance",
                    "balance_insufficient",
                ));
            }
            ReserveResult::Missing => {
                return AttemptResult::Success(logged_json_error(
                    state,
                    log_ctx,
                    http::StatusCode::UNAUTHORIZED,
                    "invalid api key",
                    "api_key_invalid",
                ));
            }
        }
    }

    let http_span = tracing::info_span!(
        "proxy.upstream_http",
        upstream.id = %sel.upstream.id,
        upstream.base_url = %upstream.base_url,
        model = %log_ctx.model.as_deref().unwrap_or_default(),
    );

    let res = tokio::time::timeout(state.request_timeout(), upstream.client.request(out_req))
        .instrument(http_span)
        .await;

    match res {
        Ok(Ok(up_resp)) => {
            let upstream_ms = attempt_start.elapsed().as_millis() as u64;
            sel.key.record_latency_ms(upstream_ms);
            let status = up_resp.status();
            let retry_after_ms = if status == http::StatusCode::TOO_MANY_REQUESTS {
                parse_retry_after_ms(up_resp.headers().get(http::header::RETRY_AFTER))
            } else {
                None
            };
            state.on_upstream_status(sel, status, retry_after_ms);

            let should_retry = should_retry_status(state, status);

            if should_retry {
                let billing_key_level = state.store.get_key_level(billing_key);
                if let Some(new_sel) =
                    state.select_for_model_excluding(&log_ctx.model.clone().unwrap_or_default(), billing_key_level, Some((&sel.upstream.id, &sel.key.key)))
                {
                    drop(up_resp);
                    tracing::debug!(
                        status = %status,
                        old_upstream = %sel.upstream.id,
                        new_upstream = %new_sel.upstream.id,
                        "retrying with different key/upstream"
                    );
                    return AttemptResult::Retry(new_sel);
                }
            }

            let up_resp = format::adapt_response(
                upstream.format,
                up_resp,
                stream_request,
                log_ctx.model.clone(),
            )
            .await;

            AttemptResult::Success(proxy_upstream_response(
                up_resp,
                state.clone(),
                log_ctx.clone(),
                stream_request,
                Some(billing_key.to_string()),
                lifecycle.take(),
            )
            .await)
        }
        Ok(Err(_e)) => {
            sel.key
                .record_latency_ms(attempt_start.elapsed().as_millis() as u64);
            state.on_network_error(sel, now);

            let billing_key_level = state.store.get_key_level(billing_key);
            if let Some(new_sel) =
                state.select_for_model_excluding(&log_ctx.model.clone().unwrap_or_default(), billing_key_level, Some((&sel.upstream.id, &sel.key.key)))
            {
                tracing::debug!(
                    old_upstream = %sel.upstream.id,
                    new_upstream = %new_sel.upstream.id,
                    "retrying after network error"
                );
                return AttemptResult::Retry(new_sel);
            }

            if *billing_reserved {
                let _ = state.billing.release_reservation(billing_key);
            }
            AttemptResult::Success(logged_response(
                state,
                log_ctx,
                RouterState::json_error(
                    http::StatusCode::BAD_GATEWAY,
                    "upstream request failed",
                    "upstream_error",
                ),
            ))
        }
        Err(_) => {
            sel.key
                .record_latency_ms(attempt_start.elapsed().as_millis() as u64);
            state.on_timeout(sel, now);

            let billing_key_level = state.store.get_key_level(billing_key);
            if let Some(new_sel) =
                state.select_for_model_excluding(&log_ctx.model.clone().unwrap_or_default(), billing_key_level, Some((&sel.upstream.id, &sel.key.key)))
            {
                tracing::debug!(
                    old_upstream = %sel.upstream.id,
                    new_upstream = %new_sel.upstream.id,
                    "retrying after timeout"
                );
                return AttemptResult::Retry(new_sel);
            }

            if *billing_reserved {
                let _ = state.billing.release_reservation(billing_key);
            }
            AttemptResult::Success(logged_response(
                state,
                log_ctx,
                RouterState::json_error(
                    http::StatusCode::GATEWAY_TIMEOUT,
                    "upstream request timeout",
                    "upstream_timeout",
                ),
            ))
        }
    }
}

async fn read_request_body(body: Body) -> Result<bytes::Bytes, Response<Body>> {
    const MAX_BYTES: usize = 16 * 1024 * 1024;
    use hyper::body::HttpBody;
    let mut body_bytes = Vec::new();
    let mut body_reader = body;
    while let Some(chunk_result) = body_reader.data().await {
        match chunk_result {
            Ok(chunk) => {
                if body_bytes.len().saturating_add(chunk.len()) > MAX_BYTES {
                    return Err(RouterState::json_error(
                        http::StatusCode::PAYLOAD_TOO_LARGE,
                        "request body too large",
                        "body_too_large",
                    ));
                }
                body_bytes.extend_from_slice(&chunk);
            }
            Err(_) => {
                return Err(RouterState::json_error(
                    http::StatusCode::BAD_GATEWAY,
                    "failed to read request body",
                    "body_read_error",
                ));
            }
        }
    }
    Ok(bytes::Bytes::from(body_bytes))
}

/// OpenAI-compatible API paths that are allowed to be proxied to upstream.
/// Any /v1/* path NOT in this list is rejected to avoid unbilled upstream charges.
const ALLOWED_API_PATHS: &[&str] = &["/v1/chat/completions"];

fn is_allowed_api_path(path: &str) -> bool {
    if !path.starts_with("/v1/") {
        return true; // non-API paths pass through (admin, web, health, etc.)
    }
    let normalized = path.trim_end_matches('/');
    ALLOWED_API_PATHS.iter().any(|p| *p == normalized)
}

async fn forward(
    req: Request<Body>,
    state: Arc<RouterState>,
    lifecycle: RequestLifecycle,
    start: Instant,
    client_ip: String,
    method: hyper::Method,
    path: String,
    billing_key: String,
) -> Response<Body> {
    // Reject unknown /v1/* paths — they would be proxied but NOT billed.
    if !is_allowed_api_path(&path) {
        return RouterState::json_error(
            http::StatusCode::NOT_FOUND,
            "endpoint not supported",
            "unsupported_endpoint",
        );
    }

    let mut lifecycle = Some(lifecycle);

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
        .and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        });
    let out_method = parts.method.clone();
    let version = parts.version;
    let headers = parts.headers.clone();
    let request_headers = sanitize_log_headers(&headers);

    // Read body into bytes for potential retries.
    let body_bytes = match read_request_body(body).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let req_bytes = body_bytes.len();
    let request_body = String::from_utf8(body_bytes.to_vec())
        .ok()
        .filter(|s| s.len() <= 16 * 1024);

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

    let mut log_ctx = RequestLogContext::new(
        start,
        client_ip,
        method.to_string(),
        path,
        model.clone(),
        None,
        req_bytes,
        request_headers,
        request_body,
        0,
    );
    log_ctx.is_stream = Some(stream_request);
    log_ctx.billing_key = Some(billing_key.clone());

    let Some(mut model) = model else {
        return logged_json_error(
            &state,
            &log_ctx,
            http::StatusCode::BAD_REQUEST,
            "missing model",
            "model_required",
        );
    };

    let mut queue_wait_ms = 0u64;
    let now = now_ms();
    let mut sel = if !state.model_exists(&model) {
        return logged_json_error(
            &state,
            &log_ctx,
            http::StatusCode::NOT_FOUND,
            "model not found",
            "model_not_found",
        );
    } else if let Some(sel) = state.select_for_model(&model, state.store.get_key_level(&billing_key), now) {
        sel
    } else {
        match wait_for_selection(&state, &model, state.store.get_key_level(&billing_key)).await {
            Ok((sel, waited)) => {
                queue_wait_ms = waited;
                sel
            }
            Err(resp) => return logged_response(&state, &log_ctx, resp),
        }
    };
    log_ctx.queue_ms = queue_wait_ms;

    // Apply model mapping: incoming name → upstream internal name.
    // Save original name for cost calculation (map key is user-facing model).
    let billing_model = model.clone();
    if let Some(mapped) = sel.upstream.model_map.get(&model) {
        let mapped = mapped.clone();
        if let Some(ref mut json) = req_json {
            json["model"] = serde_json::Value::String(mapped.clone());
        }
        log_ctx.model = Some(mapped.clone());
        model = mapped;
    }
    log_ctx.billing_model = Some(billing_model);

    let mut body_bytes = body_bytes;
    let mut injected = false;
    if stream_request
        && is_chat_completions
        && sel.upstream.format == UpstreamFormat::Openai
        && state.should_inject_usage(sel.upstream.id.as_ref())
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

    // Retry policy from config.
    let max_retries = state.max_retries();

    let forward_span = tracing::info_span!(
        "proxy.forward",
        model = %model,
        stream = stream_request,
    );

    let state_c = state.clone();
    let mut log_ctx_c = log_ctx.clone();

    async move {
    let mut retry_count = 0;
    let mut billing_reserved = false;

    loop {
        log_ctx_c.upstream_id = Some(sel.upstream.id.to_string());
        let upstream = &sel.upstream;

        // Track per-key concurrency for this attempt. Guard decrements on drop.
        let _key_guard = KeyGuard::acquire(sel.key.clone());

        let result = execute_attempt(
            &state_c,
            &sel,
            upstream,
            &original_pq,
            &out_method,
            version,
            &headers,
            &body_bytes,
            injected,
            &mut billing_reserved,
            &billing_key,
            &log_ctx_c,
            stream_request,
            &mut lifecycle,
        )
        .await;

        // _key_guard dropped here → fetch_sub, then notify
        drop(_key_guard);
        state_c.notify_capacity();

        match result {
            AttemptResult::Success(resp) => return resp,
            AttemptResult::Retry(new_sel) => {
                retry_count += 1;
                if retry_count > max_retries {
                    if billing_reserved {
                        let _ = state.billing.release_reservation(&billing_key);
                    }
                    return logged_json_error(
                        &state,
                        &log_ctx,
                        http::StatusCode::BAD_GATEWAY,
                        "max retries exceeded",
                        "max_retries",
                    );
                }
                sel = new_sel;
                continue;
            }
        }
    }
    }
    .instrument(forward_span)
    .await
}

async fn wait_for_selection(
    state: &Arc<RouterState>,
    model: &str,
    billing_key_level: i32,
) -> Result<(Selected, u64), Response<Body>> {
    let server = state.server_config();
    if !server.queue_enabled {
        return Err(RouterState::json_error(
            http::StatusCode::SERVICE_UNAVAILABLE,
            "no available upstream keys for model",
            "model_unavailable",
        ));
    }

    let Some(_guard) = state.queue_enter() else {
        return Err(queue_rejected_response(server.queue_timeout_ms));
    };

    let start = Instant::now();
    let timeout = tokio::time::sleep(std::time::Duration::from_millis(server.queue_timeout_ms));
    tokio::pin!(timeout);

    loop {
        if state.is_shutting_down() {
            return Err(RouterState::json_error(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "server is shutting down",
                "shutting_down",
            ));
        }
        if let Some(sel) = state.select_for_model(model, billing_key_level, now_ms()) {
            return Ok((sel, start.elapsed().as_millis() as u64));
        }

        if let Some(delay) = next_cooldown_delay(state, model) {
            let cooldown = tokio::time::sleep(delay);
            tokio::pin!(cooldown);
            tokio::select! {
                _ = &mut timeout => {
                    state.stats.queue_timeout_total.fetch_add(1, Ordering::Relaxed);
                    return Err(queue_rejected_response(server.queue_timeout_ms));
                }
                _ = state.queue_notify.notified() => {}
                _ = &mut cooldown => {}
            }
        } else {
            tokio::select! {
                _ = &mut timeout => {
                    state.stats.queue_timeout_total.fetch_add(1, Ordering::Relaxed);
                    return Err(queue_rejected_response(server.queue_timeout_ms));
                }
                _ = state.queue_notify.notified() => {}
            }
        }
    }
}

fn next_cooldown_delay(state: &RouterState, model: &str) -> Option<std::time::Duration> {
    let now = now_ms();
    let snap = state.snapshot.load_full();
    let mut next_until: Option<u64> = None;
    for upstream in snap.upstreams.iter() {
        if !upstream.models.load().contains(model) {
            continue;
        }
        let keys = upstream.active_keys.load_full();
        for key in keys.iter() {
            let until = key.cooldown_until_ms.load(Ordering::Relaxed);
            if until > now {
                next_until = Some(next_until.map_or(until, |cur| cur.min(until)));
            }
        }
    }
    next_until.map(|until| {
        std::time::Duration::from_millis(until.saturating_sub(now).max(1))
    })
}

fn queue_rejected_response(queue_timeout_ms: u64) -> Response<Body> {
    let retry_after_secs = ((queue_timeout_ms.max(1000) + 999) / 1000).max(1);
    let mut resp = RouterState::json_error(
        http::StatusCode::TOO_MANY_REQUESTS,
        "request queue full or timed out",
        "queue_unavailable",
    );
    if let Ok(value) = http::HeaderValue::from_str(&retry_after_secs.to_string()) {
        resp.headers_mut().insert(http::header::RETRY_AFTER, value);
    }
    resp
}

fn cors_preflight(state: &RouterState, origin: Option<&http::HeaderValue>) -> Response<Body> {
    let resp = Response::builder()
        .status(http::StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap_or_else(|_| Response::new(Body::empty()));
    add_cors_headers(resp, state, origin)
}

fn add_cors_headers(
    mut resp: Response<Body>,
    state: &RouterState,
    origin: Option<&http::HeaderValue>,
) -> Response<Body> {
    if let Some(value) = allowed_cors_origin(state, origin) {
        let headers = resp.headers_mut();
        headers.insert("access-control-allow-origin", value);
        headers.insert(
            "access-control-allow-methods",
            http::HeaderValue::from_static("GET,POST,PUT,DELETE,OPTIONS"),
        );
        headers.insert(
            "access-control-allow-headers",
            http::HeaderValue::from_static(
                "authorization,content-type,x-api-key,x-proxy-token,x-admin-token",
            ),
        );
        headers.insert(
            "access-control-expose-headers",
            http::HeaderValue::from_static("retry-after,content-type"),
        );
        headers.insert(
            "access-control-max-age",
            http::HeaderValue::from_static("600"),
        );
    }
    resp
}

fn allowed_cors_origin(
    state: &RouterState,
    origin: Option<&http::HeaderValue>,
) -> Option<http::HeaderValue> {
    let origin = origin?;
    let cfg = state.server_config();
    if cfg.cors_origins.iter().any(|o| o == "*") {
        return Some(http::HeaderValue::from_static("*"));
    }
    let origin_str = origin.to_str().ok()?;
    if cfg.cors_origins.iter().any(|allowed| allowed == origin_str) {
        Some(origin.clone())
    } else {
        None
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
    billing_model: Option<String>,
    billing_key: Option<String>,
    req_bytes: usize,
    request_headers: Option<std::collections::BTreeMap<String, String>>,
    request_body: Option<String>,
    queue_ms: u64,
    is_stream: Option<bool>,
}

impl RequestLogContext {
    fn new(
        start: Instant,
        client_ip: String,
        method: String,
        path: String,
        model: Option<String>,
        upstream_id: Option<String>,
        req_bytes: usize,
        request_headers: Option<std::collections::BTreeMap<String, String>>,
        request_body: Option<String>,
        queue_ms: u64,
    ) -> Self {
        Self {
            start,
            client_ip,
            method,
            path,
            model,
            upstream_id,
            billing_model: None,
            billing_key: None,
            req_bytes,
            request_headers,
            request_body,
            queue_ms,
            is_stream: None, // set later when parsing req body
        }
    }
}

#[derive(Clone, Copy)]
struct UsageTokens {
    prompt: u64,
    completion: u64,
    thought: u64,
    total: u64,
}

impl UsageTokens {
    /// Total output tokens for billing: visible output + thinking tokens.
    /// Billing charges thinking at the output rate, not a separate rate.
    fn billing_completion(&self) -> u64 {
        self.completion.saturating_add(self.thought)
    }
}

fn record_request(
    state: &RouterState,
    ctx: &RequestLogContext,
    status: u16,
    resp_bytes: usize,
    usage: Option<UsageTokens>,
) {
    if !ctx.path.starts_with("/v1") {
        return;
    }
    let total_ms = ctx.start.elapsed().as_millis() as u64;
    let entry = RequestLogEntry {
        id: REQUEST_LOG_ID.fetch_add(1, Ordering::Relaxed),
        ts_ms: now_ms(),
        client_ip: ctx.client_ip.clone(),
        method: ctx.method.clone(),
        path: ctx.path.clone(),
        model: ctx.model.clone(),
        upstream_id: ctx.upstream_id.clone(),
        billing_key: ctx.billing_key.clone(),
        status,
        latency_ms: total_ms,
        req_bytes: ctx.req_bytes,
        resp_bytes,
        prompt_tokens: usage.as_ref().map(|u| u.prompt),
        completion_tokens: usage.as_ref().map(|u| u.billing_completion()),
        thought_tokens: usage.as_ref().map(|u| u.thought),
        total_tokens: usage.map(|u| u.total),
        request_headers: ctx.request_headers.clone(),
        request_body: ctx.request_body.clone(),
        timing: crate::state::RequestTiming {
            queue_ms: ctx.queue_ms,
            upstream_ms: total_ms.saturating_sub(ctx.queue_ms),
            total_ms,
            attempts: 0,
        },
        is_stream: ctx.is_stream,
    };
    state.record_request(entry);
}

fn logged_json_error(
    state: &RouterState,
    ctx: &RequestLogContext,
    status: http::StatusCode,
    message: &str,
    code: &str,
) -> Response<Body> {
    let resp = RouterState::json_error(status, message, code);
    logged_response(state, ctx, resp)
}

fn logged_response(
    state: &RouterState,
    ctx: &RequestLogContext,
    resp: Response<Body>,
) -> Response<Body> {
    record_request(state, ctx, resp.status().as_u16(), 0, None);
    resp
}

fn build_upstream_request(
    method: hyper::Method,
    uri: http::Uri,
    version: http::Version,
    headers: &hyper::HeaderMap,
    body_bytes: bytes::Bytes,
    sel: &crate::state::Selected,
    injected: bool,
    auth_style: AuthStyle,
) -> Result<Request<Body>, Response<Body>> {
    let mut builder = hyper::Request::builder()
        .method(method)
        .uri(uri)
        .version(version);

    for (name, value) in headers.iter() {
        builder = builder.header(name.clone(), value.clone());
    }

    let mut out_req = builder.body(Body::from(body_bytes.clone())).map_err(|_| {
        RouterState::json_error(
            http::StatusCode::BAD_GATEWAY,
            "failed to build request",
            "request_build_error",
        )
    })?;

    sanitize_hop_headers(out_req.headers_mut());
    // Strip compression headers so upstream (especially Cloudflare) does not
    // compress the response. gptload-rs reads the full body to extract token
    // usage; decompressing gzip/brotli adds complexity and is unnecessary for
    // API proxy traffic where response bodies are small.
    out_req.headers_mut().remove(ACCEPT_ENCODING);
    out_req.headers_mut().remove(HDR_AUTHORIZATION);
    out_req.headers_mut().remove("x-api-key");
    out_req.headers_mut().remove("anthropic-version");
    match auth_style {
        AuthStyle::OpenAiBearer => {
            out_req
                .headers_mut()
                .insert(HDR_AUTHORIZATION, sel.key.auth_header.clone());
        }
        AuthStyle::AnthropicKey => {
            let key = http::HeaderValue::from_str(sel.key.key.as_ref()).map_err(|_| {
                RouterState::json_error(
                    http::StatusCode::BAD_GATEWAY,
                    "invalid upstream key header",
                    "request_build_error",
                )
            })?;
            out_req.headers_mut().insert("x-api-key", key);
            out_req.headers_mut().insert(
                "anthropic-version",
                http::HeaderValue::from_static("2023-06-01"),
            );
        }
        AuthStyle::None => {}
    }
    if injected || !body_bytes.is_empty() {
        out_req.headers_mut().remove(CONTENT_LENGTH);
        if let Ok(v) = http::HeaderValue::from_str(&body_bytes.len().to_string()) {
            out_req.headers_mut().insert(CONTENT_LENGTH, v);
        }
    }

    Ok(out_req)
}

fn should_retry_status(state: &RouterState, status: http::StatusCode) -> bool {
    status == http::StatusCode::UNAUTHORIZED
        || status == http::StatusCode::FORBIDDEN
        || state.should_retry_status(status)
}

async fn proxy_upstream_response(
    up_resp: Response<Body>,
    state: Arc<RouterState>,
    log_ctx: RequestLogContext,
    stream_request: bool,
    billing_key: Option<String>,
    lifecycle: Option<RequestLifecycle>,
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

    // ── Non-streaming: read body synchronously, extract usage, bill, return ──
    if !stream_request {
        let (resp_bytes, was_decompressed) = read_and_bill_body(
            body,
            content_type,
            content_encoding,
            status,
            &state,
            &log_ctx,
            billing_key.as_deref(),
        )
        .await;

        let body_len = resp_bytes.len();
        let body = Body::from(resp_bytes);
        if let Ok(v) = http::HeaderValue::from_str(&body_len.to_string()) {
            parts.headers.insert(CONTENT_LENGTH, v);
        }
        if was_decompressed {
            parts.headers.remove(CONTENT_ENCODING);
        }
        return Response::from_parts(parts, body);
    }

    // ── Streaming: spawn task to forward chunks and extract SSE usage ──
    let is_event_stream = content_type.starts_with("text/event-stream");
    let want_sse_usage = is_event_stream;

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, io::Error>>(32);

    // Clone before the move — watchdog needs copies.
    let state_w = state.clone();
    let log_ctx_w = log_ctx.clone();
    let billing_key_w = billing_key.clone();

    let bill_handle = tokio::spawn(async move {
        let _lifecycle = lifecycle;
        use hyper::body::HttpBody;
        const MAX_SSE_BUF_BYTES: usize = 2 * 1024 * 1024;

        let mut resp_bytes = 0usize;
        let mut usage: Option<UsageTokens> = None;
        let mut sse_buf = String::new();

        let mut body = body;
        while let Some(chunk) = body.data().await {
            match chunk {
                Ok(chunk) => {
                    resp_bytes = resp_bytes.saturating_add(chunk.len());
                    if tx.send(Ok(chunk.clone())).await.is_err() {
                        break;
                    }
                    if !want_sse_usage {
                        continue;
                    }
                    if sse_buf.len().saturating_add(chunk.len()) > MAX_SSE_BUF_BYTES {
                        continue;
                    }
                    if let Some(found) = parse_sse_usage(&mut sse_buf, &chunk) {
                        // Merge across chunks: some formats (Anthropic) split
                        // prompt_tokens (message_start) and completion_tokens (message_delta)
                        // into separate SSE events. Non-zero values from later chunks
                        // override earlier ones.
                        usage = Some(match usage {
                            Some(prev) => UsageTokens {
                                prompt: if found.prompt > 0 { found.prompt } else { prev.prompt },
                                completion: if found.completion > 0 { found.completion } else { prev.completion },
                                thought: if found.thought > 0 { found.thought } else { prev.thought },
                                total: (found.prompt.max(prev.prompt) + found.completion.max(prev.completion) + found.thought.max(prev.thought))
                                    .max(found.total)
                                    .max(prev.total),
                            },
                            None => found,
                        });
                    }
                }
                Err(_) => break,
            }
        }

        if let Some(key) = billing_key.as_deref() {
            let is_billable = log_ctx.path.starts_with("/v1/chat/completions");
            let is_2xx = status.is_success();
            if let Some(found) = usage {
                let model_costs = &state.runtime.load_full().model_costs;
                let model = log_ctx.billing_model.as_deref().unwrap_or("");
                let bill_out = found.billing_completion();
                let cost = crate::billing::compute_credit_cost(found.prompt, bill_out, model, model_costs);
                let _ = state.billing.settle_reserved_usage(
                    key, found.prompt, bill_out, model, model_costs,
                );
                state.stats.prompt_tokens_total.fetch_add(found.prompt, std::sync::atomic::Ordering::Relaxed);
                state.stats.completion_tokens_total.fetch_add(bill_out, std::sync::atomic::Ordering::Relaxed);
                state.stats.thought_tokens_total.fetch_add(found.thought, std::sync::atomic::Ordering::Relaxed);
                state.stats.tokens_total.fetch_add(found.total, std::sync::atomic::Ordering::Relaxed);
                let _ = state.store.add_key_usage(key, found.total, cost);
            } else if !is_billable || !is_2xx {
                let _ = state.billing.release_reservation(key);
            }
        }
        record_request(&state, &log_ctx, status.as_u16(), resp_bytes, usage);
    });

    // Watchdog: if the billing task panics, release the reservation so the
    // pre-deducted 1 µcredit is not permanently lost.
    tokio::spawn(async move {
        if let Err(e) = bill_handle.await {
            if e.is_panic() {
                if let Some(key) = billing_key_w.as_deref() {
                    let _ = state_w.billing.release_reservation(key);
                }
                record_request(&state_w, &log_ctx_w, status.as_u16(), 0, None);
            }
        }
    });

    Response::from_parts(parts, Body::wrap_stream(ReceiverStream::new(rx)))
}

/// Read non-streaming body, extract usage, bill.
/// Returns (body_bytes, was_decompressed). If gzip, body is decompressed.
async fn read_and_bill_body(
    body: Body,
    content_type: &str,
    content_encoding: &str,
    status: http::StatusCode,
    state: &Arc<RouterState>,
    log_ctx: &RequestLogContext,
    billing_key: Option<&str>,
) -> (Vec<u8>, bool) {
    use hyper::body::HttpBody;
    const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

    let mut raw = Vec::new();
    let mut body = body;
    let mut overflow = false;
    while let Some(chunk) = body.data().await {
        match chunk {
            Ok(chunk) => {
                if raw.len().saturating_add(chunk.len()) > MAX_BODY_BYTES {
                    overflow = true;
                    continue;
                }
                raw.extend_from_slice(&chunk);
            }
            Err(_) => break,
        }
    }

    let is_gzip = content_encoding.contains("gzip");

    // If gzip, decompress the response body for the client AND for usage parsing.
    let (out_bytes, was_decompressed) = if is_gzip {
        match decompress_gzip(&raw) {
            Ok(decompressed) => (decompressed, true),
            Err(_) => (raw, false), // decompression failed, return raw as-is
        }
    } else {
        (raw, false)
    };

    let resp_bytes = out_bytes.len();

    let is_billable = log_ctx.path.starts_with("/v1/chat/completions");
    let is_2xx = status.is_success();

    let usage = if is_billable && is_2xx && !overflow && content_type.starts_with("application/json") {
        if !out_bytes.is_empty() {
            match usage_from_json_bytes(&out_bytes) {
                Some(u) => {
                    tracing::debug!(
                        prompt = u.prompt,
                        completion = u.completion,
                        thought = u.thought,
                        total = u.total,
                        "non-streaming usage extracted"
                    );
                    Some(u)
                }
                None => {
                    tracing::warn!(
                        path = %log_ctx.path,
                        model = %log_ctx.model.as_deref().unwrap_or(""),
                        content_type = %content_type,
                        body_len = out_bytes.len(),
                        body_preview = %String::from_utf8_lossy(&out_bytes[..out_bytes.len().min(256)]),
                        "non-streaming usage NOT found in JSON body — upstream may not return usage in non-streaming mode"
                    );
                    None
                }
            }
        } else {
            None
        }
    } else {
        if is_billable && is_2xx && !overflow {
            tracing::debug!(
                path = %log_ctx.path,
                content_type = %content_type,
                content_encoding = %content_encoding,
                "skipping usage parse: content_type not application/json"
            );
        }
        None
    };

    if let Some(key) = billing_key {
        if let Some(found) = usage {
            let model_costs = &state.runtime.load_full().model_costs;
            let model = log_ctx.billing_model.as_deref().unwrap_or("");
            let bill_out = found.billing_completion();
            let cost = crate::billing::compute_credit_cost(found.prompt, bill_out, model, model_costs);
            let _ = state.billing.settle_reserved_usage(
                key, found.prompt, bill_out, model, model_costs,
            );
            state.stats.prompt_tokens_total.fetch_add(found.prompt, std::sync::atomic::Ordering::Relaxed);
            state.stats.completion_tokens_total.fetch_add(bill_out, std::sync::atomic::Ordering::Relaxed);
            state.stats.thought_tokens_total.fetch_add(found.thought, std::sync::atomic::Ordering::Relaxed);
            state.stats.tokens_total.fetch_add(found.total, std::sync::atomic::Ordering::Relaxed);
            let _ = state.store.add_key_usage(key, found.total, cost);
        } else if !is_billable || !is_2xx {
            let _ = state.billing.release_reservation(key);
        }
        // else: is_billable && is_2xx && usage=None → pre-deducted 1 µcredit stays → min charge
    }
    record_request(state, log_ctx, status.as_u16(), resp_bytes, usage);

    (out_bytes, was_decompressed)
}

fn decompress_gzip(input: &[u8]) -> Result<Vec<u8>, io::Error> {
    let mut decoder = GzipDecoder::new();
    decoder.decompress_chunk(input)
}

fn parse_retry_after_ms(value: Option<&http::HeaderValue>) -> Option<u64> {
    let raw = value?.to_str().ok()?.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(secs.saturating_mul(1000));
    }
    let dt = httpdate::parse_http_date(raw).ok()?;
    let now = std::time::SystemTime::now();
    let dur = dt.duration_since(now).unwrap_or_default();
    Some(dur.as_millis() as u64)
}

fn sanitize_log_headers(
    headers: &hyper::HeaderMap,
) -> Option<std::collections::BTreeMap<String, String>> {
    if headers.is_empty() {
        return None;
    }
    let mut out = std::collections::BTreeMap::new();
    for (name, value) in headers.iter() {
        let key = name.as_str().to_ascii_lowercase();
        if key == "authorization" || key == "x-api-key" || key.contains("token") {
            out.insert(key, "<redacted>".to_string());
        } else if let Ok(v) = value.to_str() {
            out.insert(key, v.chars().take(512).collect());
        }
    }
    Some(out)
}

fn extract_api_key(headers: &hyper::HeaderMap) -> Option<String> {
    // Validate key chars inline (hot path, no allocation for invalid keys).
    fn key_ok(k: &str) -> bool {
        !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    }
    if let Some(h) = headers.get("x-api-key") {
        if let Ok(s) = h.to_str() {
            let key = s.trim();
            if key_ok(key) {
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
            if key_ok(key) {
                return Some(key.to_string());
            }
        }
    }
    None
}

fn models_list(state: &RouterState, billing_key: &str) -> (Response<Body>, usize) {
    let routes = state.get_model_routes();
    let snap = state.snapshot.load_full();
    let user_level = state.store.get_key_level(billing_key);

    // Collect upstream IDs accessible at the user's key level.
    let accessible: Vec<&str> = snap
        .upstreams
        .iter()
        .filter(|u| user_level == -1 || u.min_key_level <= user_level)
        .map(|u| u.id.as_ref())
        .collect();

    // Filter models to only those with at least one accessible upstream.
    let mut models: Vec<String> = routes
        .models
        .iter()
        .filter(|(_model, upstreams)| upstreams.iter().any(|uid| accessible.contains(&uid.as_str())))
        .map(|(model, _)| model.clone())
        .collect();
    models.sort();

    // Apply reverse model mapping for each model across accessible upstreams only.
    let mut models: Vec<String> = models
        .into_iter()
        .map(|m| {
            for u in snap.upstreams.iter() {
                if !accessible.contains(&u.id.as_ref()) {
                    continue;
                }
                if let Some(user_name) = u.model_rmap.get(&m) {
                    return user_name.clone();
                }
            }
            m
        })
        .collect();
    models.sort();
    models.dedup();

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
        .unwrap_or_else(|_| {
            RouterState::json_error(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                "failed to build response",
                "response_build_error",
            )
        });
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
    let stream = obj.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
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
    let thought = usage.get("thought_tokens").and_then(|v| v.as_u64());
    let total = usage
        .get("total_tokens")
        .and_then(|v| v.as_u64())
        .or_else(|| match (prompt, completion) {
            (Some(p), Some(c)) => Some(p + c),
            _ => None,
        });

    if prompt.is_none() && completion.is_none() && total.is_none() {
        return None;
    }

    Some(UsageTokens {
        prompt: prompt.unwrap_or(0),
        completion: completion.unwrap_or(0),
        thought: thought.unwrap_or(0),
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
                tracing::debug!(
                    prompt = u.prompt,
                    completion = u.completion,
                    thought = u.thought,
                    total = u.total,
                    raw_usage = %v.get("usage").map(|u| u.to_string()).unwrap_or_default(),
                    "sse usage found"
                );
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

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn retry_after_seconds_parses_to_millis() {
        let value = HeaderValue::from_static("5");
        assert_eq!(parse_retry_after_ms(Some(&value)), Some(5000));
    }

    #[test]
    fn retry_after_past_date_is_zero() {
        let value = HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT");
        assert_eq!(parse_retry_after_ms(Some(&value)), Some(0));
    }

    /// Test: extract usage from a standard OpenAI chat completion response (non-streaming).
    #[test]
    fn extract_usage_from_openai_response() {
        let body = br#"{
            "id": "chatcmpl-xxx",
            "object": "chat.completion",
            "created": 1234567890,
            "model": "gpt-4o",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "Hello"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}
        }"#;
        let usage = usage_from_json_bytes(body).expect("should extract usage from standard OpenAI response");
        assert_eq!(usage.prompt, 10);
        assert_eq!(usage.completion, 20);
        assert_eq!(usage.total, 30);
    }

    /// Edge case: OpenAI response where usage fields are 0
    #[test]
    fn openai_zero_token_response() {
        let body = br#"{
            "id": "chatcmpl-xxx",
            "object": "chat.completion",
            "created": 1234567890,
            "model": "gpt-4o",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": ""}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        }"#;
        let usage = usage_from_json_bytes(body).expect("should extract usage even with zero tokens");
        assert_eq!(usage.prompt, 0);
        assert_eq!(usage.completion, 0);
        assert_eq!(usage.total, 0);
    }

    /// Edge case: response without usage field at all.
    #[test]
    fn response_without_usage_field_returns_none() {
        let body = br#"{
            "id": "chatcmpl-xxx",
            "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi"}, "finish_reason": "stop"}]
        }"#;
        assert!(usage_from_json_bytes(body).is_none(), "should return None when no usage field");
    }

    /// Test: extract usage from image generation response format (no total_tokens).
    #[test]
    fn extract_usage_from_image_generation_response() {
        let body = br#"{
            "data": [{"b64_json": "base64...", "revised_prompt": "A cat"}],
            "usage": {"prompt_tokens": 50, "completion_tokens": 300}
        }"#;
        let usage = usage_from_json_bytes(body)
            .expect("should extract usage from image generation response");
        assert_eq!(usage.prompt, 50);
        assert_eq!(usage.completion, 300);
        assert_eq!(usage.total, 350, "total should fallback to prompt+completion");
    }
}
