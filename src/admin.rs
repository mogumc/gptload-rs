use crate::config::UpstreamConfig;
use crate::state::{build_key_states, validate_keys, MetricsWindow, RouterState};
use crate::util::{now_ms, query_get};
use bytes::Bytes;
use hyper::{Body, Method, Request, Response};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;

const INDEX_HTML: &str = include_str!("static/index.html");
const APP_JS: &str = include_str!("static/app.js");

pub async fn handle_admin(req: Request<Body>, state: Arc<RouterState>) -> Response<Body> {
    let path = req.uri().path();

    // Redirect /admin -> /admin/
    if path == "/admin" {
        return Response::builder()
            .status(301)
            .header("location", "/admin/")
            .body(Body::empty())
            .unwrap();
    }

    // Static UI
    if req.method() == Method::GET && (path == "/admin/" || path == "/admin/index.html") {
        return Response::builder()
            .status(200)
            .header("content-type", "text/html; charset=utf-8")
            .header("cache-control", "no-store")
            .body(Body::from(INDEX_HTML))
            .unwrap();
    }
    if req.method() == Method::GET && path == "/admin/app.js" {
        return Response::builder()
            .status(200)
            .header("content-type", "application/javascript; charset=utf-8")
            .header("cache-control", "no-store")
            .body(Body::from(APP_JS))
            .unwrap();
    }

    // API
    if path.starts_with("/admin/api/") {
        return handle_api(req, state).await;
    }

    Response::builder()
        .status(404)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Body::from("not found"))
        .unwrap()
}

async fn handle_api(req: Request<Body>, state: Arc<RouterState>) -> Response<Body> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    // All admin API endpoints require admin token.
    let admin_ok = state.authorize_admin_header(&req);
    if !admin_ok {
        return RouterState::json_error(
            http::StatusCode::UNAUTHORIZED,
            "missing or invalid admin token",
            "admin_unauthorized",
        );
    }

    match (&method, path.as_str()) {
        (&Method::GET, "/admin/api/v1/stats/stream") => stats_stream(state).await,
        (&Method::GET, "/admin/api/v1/upstreams") => api_list_upstreams(state).await,
        (&Method::POST, "/admin/api/v1/upstreams") => api_add_upstream(req, state).await,
        (&Method::GET, "/admin/api/v1/stats") => api_stats_snapshot(state).await,
        (&Method::POST, "/admin/api/v1/reload") => api_reload_all(state).await,
        (&Method::GET, "/admin/api/v1/models/routes") => api_get_model_routes(state).await,
        (&Method::PUT, "/admin/api/v1/models/routes") => api_put_model_routes(req, state).await,
        (&Method::GET, "/admin/api/v1/requests") => api_requests(state, req.uri()).await,
        (&Method::GET, "/admin/api/v1/metrics") => api_metrics(state, req.uri()).await,
        (&Method::POST, "/admin/api/v1/billing/keys") => api_billing_create_key(req, state).await,
        _ => {
            // Dynamic routes:
            if let Some(rest) = path.strip_prefix("/admin/api/v1/billing/keys/") {
                return handle_billing_key_subroutes(req, state, rest).await;
            }
            if let Some(rest) = path.strip_prefix("/admin/api/v1/upstreams/") {
                return handle_upstream_subroutes(req, state, rest).await;
            }
            Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"error":"not_found"}"#))
                .unwrap()
        }
    }
}

async fn handle_billing_key_subroutes(
    req: Request<Body>,
    state: Arc<RouterState>,
    rest: &str,
) -> Response<Body> {
    let mut parts = rest.split('/');
    let key = match parts.next() {
        Some(s) if !s.is_empty() => s,
        _ => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                "missing key",
                "bad_request",
            )
        }
    };
    let action = parts.next().unwrap_or("");

    if action.is_empty() {
        return match *req.method() {
            Method::GET => api_billing_get_balance(state, key).await,
            _ => Response::builder()
                .status(405)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"error":"method_not_allowed"}"#))
                .unwrap(),
        };
    }

    if action == "adjust" {
        return match *req.method() {
            Method::POST => api_billing_adjust_balance(req, state, key).await,
            _ => Response::builder()
                .status(405)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"error":"method_not_allowed"}"#))
                .unwrap(),
        };
    }

    Response::builder()
        .status(404)
        .header("content-type", "application/json")
        .body(Body::from(r#"{"error":"not_found"}"#))
        .unwrap()
}

#[derive(Deserialize)]
struct BillingCreateBody {
    key: String,
    balance: Option<i64>,
}

#[derive(Deserialize)]
struct BillingAdjustBody {
    delta: i64,
}

async fn api_billing_create_key(req: Request<Body>, state: Arc<RouterState>) -> Response<Body> {
    let body = match read_body_limit(req, 256 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                &format!("read body: {e}"),
                "bad_request",
            )
        }
    };
    let payload: BillingCreateBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                &format!("invalid json: {e}"),
                "bad_request",
            )
        }
    };
    let key = payload.key.trim();
    if key.is_empty() {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "key must not be empty",
            "bad_request",
        );
    }
    let balance = payload.balance.unwrap_or(0);
    let created = match state.billing.create_key(key.to_string(), balance) {
        Ok(v) => v,
        Err(e) => {
            return RouterState::json_error(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                &format!("create key failed: {e}"),
                "billing_error",
            )
        }
    };
    if !created {
        return RouterState::json_error(
            http::StatusCode::CONFLICT,
            "key already exists",
            "key_exists",
        );
    }
    json_ok(&serde_json::json!({
        "key": key,
        "balance": balance,
        "created": true
    }))
}

async fn api_billing_get_balance(state: Arc<RouterState>, key: &str) -> Response<Body> {
    match state.billing.get_balance(key) {
        Some(balance) => json_ok(&serde_json::json!({
            "key": key,
            "balance": balance
        })),
        None => RouterState::json_error(
            http::StatusCode::NOT_FOUND,
            "key not found",
            "key_not_found",
        ),
    }
}

async fn api_billing_adjust_balance(
    req: Request<Body>,
    state: Arc<RouterState>,
    key: &str,
) -> Response<Body> {
    let body = match read_body_limit(req, 256 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                &format!("read body: {e}"),
                "bad_request",
            )
        }
    };
    let payload: BillingAdjustBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                &format!("invalid json: {e}"),
                "bad_request",
            )
        }
    };

    match state.billing.adjust_balance(key, payload.delta) {
        Some(balance) => json_ok(&serde_json::json!({
            "key": key,
            "delta": payload.delta,
            "balance": balance
        })),
        None => RouterState::json_error(
            http::StatusCode::NOT_FOUND,
            "key not found",
            "key_not_found",
        ),
    }
}

async fn handle_upstream_subroutes(
    req: Request<Body>,
    state: Arc<RouterState>,
    rest: &str,
) -> Response<Body> {
    // rest like "{id}" / "{id}/keys" / "{id}/models/refresh"
    let mut parts = rest.split('/');
    let upstream_id = match parts.next() {
        Some(s) if !s.is_empty() => s,
        _ => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                "missing upstream id",
                "bad_request",
            )
        }
    };
    let sub = parts.next().unwrap_or("");

    if sub.is_empty() {
        match *req.method() {
            Method::PUT => return api_update_upstream(req, state, upstream_id).await,
            Method::DELETE => return api_delete_upstream(req, state, upstream_id).await,
            _ => {
                return Response::builder()
                    .status(405)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"error":"method_not_allowed"}"#))
                    .unwrap();
            }
        }
    }

    if sub == "models" {
        let action = parts.next().unwrap_or("");
        if action == "refresh" {
            if *req.method() == Method::POST {
                return api_refresh_models(state, upstream_id).await;
            }
            return Response::builder()
                .status(405)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"error":"method_not_allowed"}"#))
                .unwrap();
        }
        return Response::builder()
            .status(404)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"error":"not_found"}"#))
            .unwrap();
    }

    if sub != "keys" {
        return Response::builder()
            .status(404)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"error":"not_found"}"#))
            .unwrap();
    }

    match *req.method() {
        Method::POST => api_add_keys(req, state, upstream_id).await,
        Method::PUT => api_replace_keys(req, state, upstream_id).await,
        Method::DELETE => api_delete_keys(req, state, upstream_id).await,
        Method::GET => api_list_keys(state, upstream_id, req.uri()).await,
        _ => Response::builder()
            .status(405)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"error":"method_not_allowed"}"#))
            .unwrap(),
    }
}

async fn api_get_model_routes(state: Arc<RouterState>) -> Response<Body> {
    let routes = state.get_model_routes();
    json_ok(&routes)
}

#[derive(Deserialize)]
struct ModelRoutesBody {
    upstreams: BTreeMap<String, Vec<String>>,
}

async fn api_put_model_routes(req: Request<Body>, state: Arc<RouterState>) -> Response<Body> {
    let body = match read_body_limit(req, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                &e.to_string(),
                "bad_request",
            )
        }
    };

    let routes_body: ModelRoutesBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                &format!("invalid json: {e}"),
                "bad_request",
            )
        }
    };

    match state.save_model_routes(routes_body.upstreams) {
        Ok(routes) => json_ok(&routes),
        Err(e) => RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request"),
    }
}

async fn api_refresh_models(state: Arc<RouterState>, upstream_id: &str) -> Response<Body> {
    match state.fetch_models_preview(upstream_id).await {
        Ok(models) => json_ok(&serde_json::json!({
            "upstream": upstream_id,
            "count": models.len(),
            "models": models
        })),
        Err(e) => RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request"),
    }
}

#[derive(Deserialize)]
struct UpstreamBody {
    id: String,
    base_url: String,
    weight: Option<usize>,
}

#[derive(Deserialize)]
struct UpstreamUpdateBody {
    base_url: String,
    weight: Option<usize>,
}

async fn api_add_upstream(req: Request<Body>, state: Arc<RouterState>) -> Response<Body> {
    let body = match read_body_limit(req, 256 * 1024).await {
        Ok(b) => b,
        Err(e) => return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request"),
    };
    let input: UpstreamBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                &format!("invalid json: {e}"),
                "bad_request",
            )
        }
    };
    if input.id.trim().is_empty() {
        return RouterState::json_error(http::StatusCode::BAD_REQUEST, "missing id", "bad_request");
    }
    if input.base_url.trim().is_empty() {
        return RouterState::json_error(http::StatusCode::BAD_REQUEST, "missing base_url", "bad_request");
    }
    let cfg = UpstreamConfig {
        id: input.id.trim().to_string(),
        base_url: input.base_url.trim().to_string(),
        weight: input.weight,
    };
    let state2 = state.clone();
    let res = tokio::task::spawn_blocking(move || state2.add_upstream(cfg)).await;
    match res {
        Ok(Ok(_)) => {
            let state3 = state.clone();
            let id = input.id;
            tokio::spawn(async move {
                state3.refresh_missing_models_for_upstream(&id).await;
            });
            json_ok(&serde_json::json!({"ok": true}))
        }
        Ok(Err(e)) => RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request"),
        Err(e) => RouterState::json_error(http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "internal_error"),
    }
}

async fn api_update_upstream(req: Request<Body>, state: Arc<RouterState>, upstream_id: &str) -> Response<Body> {
    let body = match read_body_limit(req, 256 * 1024).await {
        Ok(b) => b,
        Err(e) => return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request"),
    };
    let input: UpstreamUpdateBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                &format!("invalid json: {e}"),
                "bad_request",
            )
        }
    };
    if input.base_url.trim().is_empty() {
        return RouterState::json_error(http::StatusCode::BAD_REQUEST, "missing base_url", "bad_request");
    }
    let state2 = state.clone();
    let id = upstream_id.to_string();
    let base_url = input.base_url.trim().to_string();
    let weight = input.weight;
    let res = tokio::task::spawn_blocking(move || state2.update_upstream(&id, base_url, weight)).await;
    match res {
        Ok(Ok(_)) => json_ok(&serde_json::json!({"ok": true})),
        Ok(Err(e)) => RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request"),
        Err(e) => RouterState::json_error(http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "internal_error"),
    }
}

async fn api_delete_upstream(req: Request<Body>, state: Arc<RouterState>, upstream_id: &str) -> Response<Body> {
    let delete_keys = query_get(req.uri(), "delete_keys")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let state2 = state.clone();
    let id = upstream_id.to_string();
    let res = tokio::task::spawn_blocking(move || state2.delete_upstream(&id, delete_keys)).await;
    match res {
        Ok(Ok(_)) => json_ok(&serde_json::json!({"ok": true, "delete_keys": delete_keys})),
        Ok(Err(e)) => RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request"),
        Err(e) => RouterState::json_error(http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "internal_error"),
    }
}

#[derive(Serialize)]
struct UpstreamInfo {
    id: String,
    base_url: String,
    weight: usize,
    keys_total: usize,
    upstream_cooldown_until_ms: u64,
    upstream_fail_streak: u32,

    selected_total: u64,

    responses_2xx: u64,
    responses_3xx: u64,
    responses_4xx: u64,
    responses_5xx: u64,
    errors_timeout: u64,
    errors_network: u64,
}

async fn api_list_upstreams(state: Arc<RouterState>) -> Response<Body> {
    let snap = state.snapshot.load_full();
    let mut ups: Vec<UpstreamInfo> = Vec::with_capacity(snap.upstreams.len());
    for u in snap.upstreams.iter() {
        ups.push(UpstreamInfo {
            id: u.id.to_string(),
            base_url: u.base_url.to_string(),
            weight: u.weight,
            keys_total: u.keys_len(),
            upstream_cooldown_until_ms: u.cooldown_until_ms.load(std::sync::atomic::Ordering::Relaxed),
            upstream_fail_streak: u.fail_streak.load(std::sync::atomic::Ordering::Relaxed),
            selected_total: u.stats.selected_total.load(std::sync::atomic::Ordering::Relaxed),
            responses_2xx: u.stats.responses_2xx.load(std::sync::atomic::Ordering::Relaxed),
            responses_3xx: u.stats.responses_3xx.load(std::sync::atomic::Ordering::Relaxed),
            responses_4xx: u.stats.responses_4xx.load(std::sync::atomic::Ordering::Relaxed),
            responses_5xx: u.stats.responses_5xx.load(std::sync::atomic::Ordering::Relaxed),
            errors_timeout: u.stats.errors_timeout.load(std::sync::atomic::Ordering::Relaxed),
            errors_network: u.stats.errors_network.load(std::sync::atomic::Ordering::Relaxed),
        });
    }

    json_ok(&ups)
}

#[derive(Serialize)]
struct StatsSnapshot {
    ts_ms: u64,
    uptime_s: u64,

    requests_total: u64,
    requests_inflight: u64,
    upstream_selected_total: u64,

    responses_2xx: u64,
    responses_3xx: u64,
    responses_4xx: u64,
    responses_5xx: u64,

    errors_timeout: u64,
    errors_network: u64,

    latency_avg_ms: f64,
    latency_max_ms: f64,
    latency_count: u64,

    upstreams: Vec<UpstreamInfo>,
}

fn build_snapshot(state: &RouterState) -> StatsSnapshot {
    let ts = now_ms();
    let uptime_s = (ts.saturating_sub(state.stats.started_at_ms)) / 1000;

    let latency_count = state.stats.latency_count.load(std::sync::atomic::Ordering::Relaxed);
    let latency_total = state.stats.latency_ns_total.load(std::sync::atomic::Ordering::Relaxed);
    let latency_max = state.stats.latency_ns_max.load(std::sync::atomic::Ordering::Relaxed);

    let latency_avg_ms = if latency_count == 0 {
        0.0
    } else {
        (latency_total as f64) / (latency_count as f64) / 1_000_000.0
    };
    let latency_max_ms = (latency_max as f64) / 1_000_000.0;

    let snap = state.snapshot.load_full();
    let mut ups: Vec<UpstreamInfo> = Vec::with_capacity(snap.upstreams.len());
    for u in snap.upstreams.iter() {
        ups.push(UpstreamInfo {
            id: u.id.to_string(),
            base_url: u.base_url.to_string(),
            weight: u.weight,
            keys_total: u.keys_len(),
            upstream_cooldown_until_ms: u.cooldown_until_ms.load(std::sync::atomic::Ordering::Relaxed),
            upstream_fail_streak: u.fail_streak.load(std::sync::atomic::Ordering::Relaxed),
            selected_total: u.stats.selected_total.load(std::sync::atomic::Ordering::Relaxed),
            responses_2xx: u.stats.responses_2xx.load(std::sync::atomic::Ordering::Relaxed),
            responses_3xx: u.stats.responses_3xx.load(std::sync::atomic::Ordering::Relaxed),
            responses_4xx: u.stats.responses_4xx.load(std::sync::atomic::Ordering::Relaxed),
            responses_5xx: u.stats.responses_5xx.load(std::sync::atomic::Ordering::Relaxed),
            errors_timeout: u.stats.errors_timeout.load(std::sync::atomic::Ordering::Relaxed),
            errors_network: u.stats.errors_network.load(std::sync::atomic::Ordering::Relaxed),
        });
    }

    StatsSnapshot {
        ts_ms: ts,
        uptime_s,
        requests_total: state.stats.requests_total.load(std::sync::atomic::Ordering::Relaxed),
        requests_inflight: state.stats.requests_inflight.load(std::sync::atomic::Ordering::Relaxed),
        upstream_selected_total: state.stats.upstream_selected_total.load(std::sync::atomic::Ordering::Relaxed),
        responses_2xx: state.stats.responses_2xx.load(std::sync::atomic::Ordering::Relaxed),
        responses_3xx: state.stats.responses_3xx.load(std::sync::atomic::Ordering::Relaxed),
        responses_4xx: state.stats.responses_4xx.load(std::sync::atomic::Ordering::Relaxed),
        responses_5xx: state.stats.responses_5xx.load(std::sync::atomic::Ordering::Relaxed),
        errors_timeout: state.stats.errors_timeout.load(std::sync::atomic::Ordering::Relaxed),
        errors_network: state.stats.errors_network.load(std::sync::atomic::Ordering::Relaxed),
        latency_avg_ms,
        latency_max_ms,
        latency_count,
        upstreams: ups,
    }
}

async fn api_stats_snapshot(state: Arc<RouterState>) -> Response<Body> {
    let snap = build_snapshot(&state);
    json_ok(&snap)
}

async fn api_requests(state: Arc<RouterState>, uri: &http::Uri) -> Response<Body> {
    let limit: usize = query_get(uri, "limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(200)
        .clamp(1, 5000);
    let list = state.recent_requests(limit);
    json_ok(&serde_json::json!({
        "now_ms": now_ms(),
        "count": list.len(),
        "requests": list
    }))
}

async fn api_metrics(state: Arc<RouterState>, uri: &http::Uri) -> Response<Body> {
    let window = query_get(uri, "window").unwrap_or("minute");
    let win = MetricsWindow::from_str(window);
    let buckets = state.metrics_snapshot(win);
    json_ok(&serde_json::json!({
        "window": win.as_str(),
        "now_ms": now_ms(),
        "buckets": buckets
    }))
}

async fn stats_stream(state: Arc<RouterState>) -> Response<Body> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(32);
    let state2 = state.clone();

    tokio::spawn(async move {
        let mut last_total = state2.stats.requests_total.load(std::sync::atomic::Ordering::Relaxed);
        loop {
            let snap = build_snapshot(&state2);
            let total = snap.requests_total;
            let rps = total.saturating_sub(last_total);
            last_total = total;

            let mut v = serde_json::to_value(&snap).unwrap_or(serde_json::json!({"error":"snapshot_failed"}));
            if let serde_json::Value::Object(ref mut m) = v {
                m.insert("rps".into(), serde_json::json!(rps));
            }
            let s = match serde_json::to_string(&v) {
                Ok(s) => s,
                Err(_) => String::from(r#"{"error":"json"}"#),
            };
            let msg = format!("data: {}\n\n", s);

            if tx.send(Ok(Bytes::from(msg))).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    Response::builder()
        .status(200)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("connection", "keep-alive")
        .body(Body::wrap_stream(ReceiverStream::new(rx)))
        .unwrap()
}

async fn api_reload_all(state: Arc<RouterState>) -> Response<Body> {
    let mut results = Vec::new();
    let snap = state.snapshot.load_full();
    for u in snap.upstreams.iter() {
        let id = u.id.to_string();
        let id_clone = id.clone();
        let store = state.store.clone();
        let u2 = u.clone();

        // Reload in blocking thread.
        let res = tokio::task::spawn_blocking(move || -> anyhow::Result<usize> {
            let keys = store.load_all_keys(&id_clone)?;
            let ks = build_key_states(keys)?;
            let n = ks.len();
            u2.keys.store(ks);
            Ok(n)
        })
        .await;

        match res {
            Ok(Ok(n)) => results.push(serde_json::json!({"id": id, "keys_total": n, "ok": true})),
            Ok(Err(e)) => results.push(serde_json::json!({"id": id, "ok": false, "error": e.to_string()})),
            Err(e) => results.push(serde_json::json!({"id": id, "ok": false, "error": e.to_string()})),
        }
    }

    let state2 = state.clone();
    tokio::spawn(async move {
        state2.refresh_missing_models_routes().await;
    });

    json_ok(&serde_json::json!({ "reloaded": results }))
}

#[derive(Deserialize)]
struct JsonKeysBody {
    keys: Vec<String>,
    dedupe: Option<bool>,
}

async fn api_add_keys(req: Request<Body>, state: Arc<RouterState>, upstream_id: &str) -> Response<Body> {
    let Some((_idx, upstream)) = state.upstream_by_id(upstream_id) else {
        return RouterState::json_error(http::StatusCode::NOT_FOUND, "unknown upstream id", "not_found");
    };

    let (keys, dedupe) = match parse_keys_body(req).await {
        Ok(v) => v,
        Err(e) => return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e, "bad_request"),
    };

    let keys = if dedupe { dedupe_keys(keys) } else { keys };
    if keys.is_empty() {
        return RouterState::json_error(http::StatusCode::BAD_REQUEST, "no keys provided", "bad_request");
    }
    if let Err(e) = validate_keys(&keys) {
        return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request");
    }

    let store = state.store.clone();
    let id = upstream_id.to_string();
    let upstream2 = upstream.clone();

    let res = tokio::task::spawn_blocking(move || -> anyhow::Result<serde_json::Value> {
        let add_res = store.add_keys(&id, &keys)?;
        let inserted = add_res.inserted;
        let existed = add_res.existed;

        // Build new KeyState arcs only for inserted keys and append to in-memory list.
        let inserted_states = build_key_states(add_res.inserted_keys)?;
        let old = upstream2.keys.load_full();
        let mut merged: Vec<Arc<crate::state::KeyState>> = Vec::with_capacity(old.len() + inserted_states.len());
        merged.extend(old.iter().cloned());
        merged.extend(inserted_states.iter().cloned());
        upstream2.keys.store(Arc::new(merged));

        Ok(serde_json::json!({
            "ok": true,
            "upstream": id,
            "inserted": inserted,
            "existed": existed,
            "keys_total": upstream2.keys_len()
        }))
    })
    .await;

    match res {
        Ok(Ok(v)) => {
            let state2 = state.clone();
            let id2 = upstream_id.to_string();
            tokio::spawn(async move {
                state2.refresh_missing_models_for_upstream(&id2).await;
            });
            json_ok(&v)
        }
        Ok(Err(e)) => RouterState::json_error(http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "internal_error"),
        Err(e) => RouterState::json_error(http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "internal_error"),
    }
}

async fn api_replace_keys(req: Request<Body>, state: Arc<RouterState>, upstream_id: &str) -> Response<Body> {
    let Some((_idx, upstream)) = state.upstream_by_id(upstream_id) else {
        return RouterState::json_error(http::StatusCode::NOT_FOUND, "unknown upstream id", "not_found");
    };

    let (keys, dedupe) = match parse_keys_body(req).await {
        Ok(v) => v,
        Err(e) => return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e, "bad_request"),
    };

    let keys = if dedupe { dedupe_keys(keys) } else { keys };
    if keys.is_empty() {
        return RouterState::json_error(http::StatusCode::BAD_REQUEST, "no keys provided", "bad_request");
    }
    if let Err(e) = validate_keys(&keys) {
        return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request");
    }

    let store = state.store.clone();
    let id = upstream_id.to_string();
    let upstream2 = upstream.clone();

    let res = tokio::task::spawn_blocking(move || -> anyhow::Result<serde_json::Value> {
        store.replace_keys(&id, &keys)?;
        let ks = build_key_states(keys)?;
        let n = ks.len();
        upstream2.keys.store(ks);
        Ok(serde_json::json!({
            "ok": true,
            "upstream": id,
            "keys_total": n
        }))
    })
    .await;

    match res {
        Ok(Ok(v)) => {
            let state2 = state.clone();
            let id2 = upstream_id.to_string();
            tokio::spawn(async move {
                state2.refresh_missing_models_for_upstream(&id2).await;
            });
            json_ok(&v)
        }
        Ok(Err(e)) => RouterState::json_error(http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "internal_error"),
        Err(e) => RouterState::json_error(http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "internal_error"),
    }
}

async fn api_delete_keys(req: Request<Body>, state: Arc<RouterState>, upstream_id: &str) -> Response<Body> {
    let Some((_idx, upstream)) = state.upstream_by_id(upstream_id) else {
        return RouterState::json_error(http::StatusCode::NOT_FOUND, "unknown upstream id", "not_found");
    };

    let (keys, dedupe) = match parse_keys_body(req).await {
        Ok(v) => v,
        Err(e) => return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e, "bad_request"),
    };
    if keys.is_empty() {
        return RouterState::json_error(http::StatusCode::BAD_REQUEST, "no keys provided", "bad_request");
    }

    let store = state.store.clone();
    let id = upstream_id.to_string();
    let upstream2 = upstream.clone();

    let res = tokio::task::spawn_blocking(move || -> anyhow::Result<serde_json::Value> {
        let keys = if dedupe { dedupe_keys(keys) } else { keys };
        let removed = store.delete_keys(&id, &keys)?;

        // Update in-memory: filter out removed keys.
        let remove_set: ahash::AHashSet<&str> = keys.iter().map(|s| s.as_str()).collect();
        let old = upstream2.keys.load_full();
        let mut kept: Vec<Arc<crate::state::KeyState>> = Vec::with_capacity(old.len().saturating_sub(removed));
        for k in old.iter() {
            if !remove_set.contains(k.key.as_ref()) {
                kept.push(k.clone());
            }
        }
        upstream2.keys.store(Arc::new(kept));

        Ok(serde_json::json!({
            "ok": true,
            "upstream": id,
            "removed": removed,
            "keys_total": upstream2.keys_len()
        }))
    })
    .await;

    match res {
        Ok(Ok(v)) => json_ok(&v),
        Ok(Err(e)) => RouterState::json_error(http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "internal_error"),
        Err(e) => RouterState::json_error(http::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "internal_error"),
    }
}

#[derive(Serialize)]
struct KeyInfo {
    key: String,
    cooldown_until_ms: u64,
    fail_streak: u32,
}

async fn api_list_keys(state: Arc<RouterState>, upstream_id: &str, uri: &http::Uri) -> Response<Body> {
    let Some((_idx, upstream)) = state.upstream_by_id(upstream_id) else {
        return RouterState::json_error(http::StatusCode::NOT_FOUND, "unknown upstream id", "not_found");
    };

    let limit: usize = query_get(uri, "limit")
        .and_then(|s: &str| s.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 5000);
    let offset: usize = query_get(uri, "offset")
        .and_then(|s: &str| s.parse::<usize>().ok())
        .unwrap_or(0);

    let now = now_ms();

    let keys_arc = upstream.keys.load_full();
    let keys = keys_arc.as_ref();
    let total = keys.len();
    let end = (offset + limit).min(total);

    let mut out: Vec<KeyInfo> = Vec::with_capacity(end.saturating_sub(offset));
    for k in keys.iter().skip(offset).take(end - offset) {
        out.push(KeyInfo {
            key: k.key.to_string(),
            cooldown_until_ms: k.cooldown_until_ms.load(std::sync::atomic::Ordering::Relaxed),
            fail_streak: k.fail_streak.load(std::sync::atomic::Ordering::Relaxed),
        });
    }

    json_ok(&serde_json::json!({
        "upstream": upstream_id,
        "total": total,
        "offset": offset,
        "limit": limit,
        "now_ms": now,
        "keys": out
    }))
}

async fn parse_keys_body(req: Request<Body>) -> Result<(Vec<String>, bool), String> {
    // Accept:
    // - text/plain: newline-separated keys
    // - application/json: {"keys": ["k1", "k2"], "dedupe": true}
    let content_type = req
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let body_bytes = read_body_limit(req, 50 * 1024 * 1024).await.map_err(|e| e.to_string())?; // 50MB

    if content_type.starts_with("application/json") {
        let v: JsonKeysBody = serde_json::from_slice(&body_bytes).map_err(|e| format!("invalid json: {e}"))?;
        Ok((v.keys, v.dedupe.unwrap_or(true)))
    } else {
        // Treat as plain text.
        let s = std::str::from_utf8(&body_bytes).map_err(|_| "body is not utf-8".to_string())?;
        let mut keys: Vec<String> = Vec::new();
        for line in s.lines() {
            let k = line.trim();
            if !k.is_empty() {
                keys.push(k.to_string());
            }
        }
        Ok((keys, true))
    }
}

async fn read_body_limit(mut req: Request<Body>, limit: usize) -> anyhow::Result<Bytes> {
    use hyper::body::HttpBody;
    let mut buf = Vec::new();
    while let Some(chunk) = req.body_mut().data().await {
        let chunk = chunk?;
        if buf.len() + chunk.len() > limit {
            anyhow::bail!("body too large (limit {} bytes)", limit);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(buf))
}

fn dedupe_keys(keys: Vec<String>) -> Vec<String> {
    let mut set: ahash::AHashSet<String> = ahash::AHashSet::with_capacity(keys.len().max(1));
    let mut out = Vec::with_capacity(keys.len());
    for k in keys {
        let k = k.trim().to_string();
        if k.is_empty() {
            continue;
        }
        if set.insert(k.clone()) {
            out.push(k);
        }
    }
    out
}

fn json_ok<T: ?Sized + Serialize>(v: &T) -> Response<Body> {
    let body = match serde_json::to_vec(v) {
        Ok(b) => b,
        Err(_) => br#"{"error":"json"}"#.to_vec(),
    };
    Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(Body::from(body))
        .unwrap()
}
