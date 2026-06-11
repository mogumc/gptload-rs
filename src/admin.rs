use crate::config::{UpstreamConfig, UpstreamFormat};
use crate::state::{build_key_states, validate_keys, MetricsWindow, RouterState};
use crate::util::{now_ms, query_get};
use bytes::Bytes;
use hyper::{Body, Method, Request, Response};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;

// Embedded static files from dist/
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "src/static/dist"]
struct Asset;

/// Get content-type and cache-control for a file path.
fn mime_and_cache(path: &str) -> (&'static str, &'static str) {
    match path.rsplit('.').next() {
        Some("html") => ("text/html; charset=utf-8", "no-store"),
        Some("js") => ("application/javascript; charset=utf-8", "max-age=31536000, immutable"),
        Some("css") => ("text/css; charset=utf-8", "max-age=31536000, immutable"),
        Some("woff") => ("font/woff", "max-age=31536000, immutable"),
        Some("woff2") => ("font/woff2", "max-age=31536000, immutable"),
        Some("ttf") => ("font/ttf", "max-age=31536000, immutable"),
        Some("otf") => ("font/otf", "max-age=31536000, immutable"),
        Some("json") => ("application/json", "no-store"),
        Some("png") => ("image/png", "max-age=31536000, immutable"),
        Some("jpg") | Some("jpeg") => ("image/jpeg", "max-age=31536000, immutable"),
        Some("gif") => ("image/gif", "max-age=31536000, immutable"),
        Some("svg") => ("image/svg+xml", "max-age=31536000, immutable"),
        Some("ico") => ("image/x-icon", "max-age=31536000, immutable"),
        Some("webp") => ("image/webp", "max-age=31536000, immutable"),
        Some("avif") => ("image/avif", "max-age=31536000, immutable"),
        Some("wasm") => ("application/wasm", "max-age=31536000, immutable"),
        Some("map") => ("application/json", "max-age=31536000, immutable"),
        _ => ("application/octet-stream", "max-age=31536000, immutable"),
    }
}

/// Read request body and parse as JSON. Returns error response on failure.
async fn parse_json_body<T: serde::de::DeserializeOwned>(
    req: Request<Body>,
) -> Result<T, Response<Body>> {
    let body = read_body_limit(req, 10 * 1024 * 1024).await.map_err(|e| {
        RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request")
    })?;
    serde_json::from_slice(&body).map_err(|e| {
        RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            &format!("invalid json: {e}"),
            "bad_request",
        )
    })
}

/// Look up upstream by id. Returns error response if not found.
fn get_upstream(
    state: &RouterState,
    id: &str,
) -> Result<(usize, Arc<crate::state::Upstream>), Response<Body>> {
    state.upstream_by_id(id).ok_or_else(|| {
        RouterState::json_error(
            http::StatusCode::NOT_FOUND,
            "unknown upstream id",
            "not_found",
        )
    })
}

pub async fn handle_admin(req: Request<Body>, state: Arc<RouterState>) -> Response<Body> {
    let path = req.uri().path();

    // Serve static files from /web/ (no auth — SPA handles token input).
    let is_web = req.method() == Method::GET
        && (path == "/web" || path == "/web/" || path.starts_with("/web/"));
    if is_web {
        let file = path.strip_prefix("/web").unwrap_or("");
        let file = file.strip_prefix('/').unwrap_or("");
        let file = if file.is_empty() { "index.html" } else { file };

        if let Some(asset) = Asset::get(file) {
            let (content_type, cache_control) = mime_and_cache(file);
            return Response::builder()
                .status(200)
                .header("content-type", content_type)
                .header("cache-control", cache_control)
                .body(Body::from(asset.data.as_ref().to_vec()))
                .unwrap();
        }

        // File not found in dist
        return Response::builder()
            .status(404)
            .header("content-type", "text/plain; charset=utf-8")
            .body(Body::from("404 Not Found"))
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

    // All admin API endpoints require admin token via X-Admin-Token header.
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
        (&Method::GET, "/admin/api/v1/model-costs") => api_get_model_costs(state).await,
        (&Method::POST, "/admin/api/v1/model-costs") => api_set_model_costs(req, state).await,
        (&Method::POST, "/admin/api/v1/reload") => api_reload_all(state).await,
        (&Method::GET, "/admin/api/v1/config") => api_config_preview(state).await,
        (&Method::GET, "/admin/api/v1/models/routes") => api_get_model_routes(state).await,
        (&Method::PUT, "/admin/api/v1/models/routes") => api_put_model_routes(req, state).await,
        (&Method::GET, "/admin/api/v1/requests/stream") => requests_stream(state).await,
        (&Method::GET, "/admin/api/v1/requests") => api_requests(state, req.uri()).await,
        (&Method::GET, "/admin/api/v1/requests/history") => api_requests_history(state, req.uri()).await,
        (&Method::GET, "/admin/api/v1/metrics") => api_metrics(state, req.uri()).await,
        (&Method::GET, "/admin/api/v1/billing/keys") => api_billing_list_keys(state).await,
        (&Method::GET, "/admin/api/v1/billing/overview") => api_billing_overview(state).await,
        (&Method::POST, "/admin/api/v1/billing/keys") => api_billing_create_key(req, state).await,
        _ => {
            // Dynamic routes:
            if let Some(rest) = path.strip_prefix("/admin/api/v1/billing/") {
                if rest != "keys" && rest != "overview" {
                    return handle_billing_key_subroutes(req, state, rest).await;
                }
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
            Method::DELETE => api_billing_delete_key(state, key).await,
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
            _ => method_not_allowed(),
        };
    }

    if action == "level" {
        return match *req.method() {
            Method::GET => api_billing_get_level(state, key).await,
            Method::POST => api_billing_set_level(req, state, key).await,
            _ => method_not_allowed(),
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

async fn api_billing_list_keys(state: Arc<RouterState>) -> Response<Body> {
    let keys = state.billing.list_keys();
    let items: Vec<serde_json::Value> = keys
        .into_iter()
        .map(|(key, balance)| {
            let level = state.store.get_key_level(&key);
            serde_json::json!({
                "key": key,
                "balance": if balance == -1 { -1.0_f64 } else { balance as f64 / crate::billing::MICRO_PER_CREDIT as f64 },
                "level": level
            })
        })
        .collect();
    json_ok(&serde_json::json!({ "keys": items }))
}

async fn api_billing_overview(state: Arc<RouterState>) -> Response<Body> {
    let keys = state.billing.list_keys();
    let total_keys = keys.len();
    let mut total_balance: i64 = 0;
    let mut unlimited_count = 0;
    let mut zero_or_less = 0;
    let mut key_details: Vec<serde_json::Value> = Vec::with_capacity(keys.len());
    for (key, balance) in &keys {
        if *balance == -1 {
            unlimited_count += 1;
        } else {
            total_balance = total_balance.saturating_add(*balance);
            if *balance <= 0 {
                zero_or_less += 1;
            }
        }
        key_details.push(serde_json::json!({
            "key": key,
            "balance": balance,
            "label": if *balance == -1 { "unlimited" } else if *balance <= 0 { "exhausted" } else { "active" }
        }));
    }

    let rt = state.runtime.load_full();
    let model_costs: serde_json::Value = rt
        .model_costs
        .iter()
        .map(|(m, c)| serde_json::json!({ "model": m, "input": c.input, "output": c.output }))
        .collect();

    let snap = state.snapshot.load_full();
    let upstream_summary: Vec<serde_json::Value> = snap.upstreams.iter().map(|u| {
        let keys = u.keys.load_full();
        let active = keys.iter().filter(|k| k.is_active()).count();
        serde_json::json!({
            "id": u.id.as_ref(),
            "total_keys": keys.len(),
            "active_keys": active,
            "format": u.format.as_str(),
            "min_key_level": u.min_key_level,
            "model_map": u.model_map.iter().map(|(k, v)| serde_json::json!({k: v})).collect::<Vec<_>>(),
        })
    }).collect();

    let stats = &state.stats;
        let mut platform_tokens: u64 = 0;
        let mut platform_credits: i64 = 0;
        let mut key_usages: Vec<serde_json::Value> = Vec::with_capacity(keys.len());
        for (key, _balance) in &keys {
            let (tokens, credits) = state.store.get_key_usage(key);
            platform_tokens += tokens;
            platform_credits += credits;
            key_usages.push(serde_json::json!({
                "key": key,
                "tokens": tokens,
                "credits": credits as f64 / crate::billing::MICRO_PER_CREDIT as f64
            }));
        }

        json_ok(&serde_json::json!({
        "billing": {
            "total_keys": total_keys,
            "unlimited_keys": unlimited_count,
            "active_keys": total_keys - unlimited_count - zero_or_less,
            "exhausted_keys": zero_or_less,
            "total_balance": total_balance as f64 / crate::billing::MICRO_PER_CREDIT as f64,
        },
        "model_costs": model_costs,
        "usage": {
            "tokens": platform_tokens,
            "credits": platform_credits as f64 / crate::billing::MICRO_PER_CREDIT as f64,
        },
        "key_usage": key_usages,
        "upstreams": upstream_summary,
        "requests_total": stats.requests_total.load(std::sync::atomic::Ordering::Relaxed),
        "requests_inflight": stats.requests_inflight.load(std::sync::atomic::Ordering::Relaxed),
    }))
}

async fn api_billing_create_key(req: Request<Body>, state: Arc<RouterState>) -> Response<Body> {
    let payload: BillingCreateBody = match parse_json_body(req).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let key = payload.key.trim();
    if key.is_empty() {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "key must not be empty",
            "bad_request",
        );
    }
    if let Err(e) = crate::util::validate_key_chars(key) {
        return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e, "bad_request");
    }
    let balance_credits = payload.balance.unwrap_or(0).max(-1);
    // Reject balances that would overflow i64 when converted to micro-credits.
    if balance_credits > 0 && balance_credits > crate::billing::MAX_BALANCE_CREDITS {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            &format!(
                "balance exceeds maximum allowed ({} credits)",
                crate::billing::MAX_BALANCE_CREDITS
            ),
            "bad_request",
        );
    }
    let balance_micro = if balance_credits == -1 { -1 } else { balance_credits.saturating_mul(crate::billing::MICRO_PER_CREDIT) };
    let created = match state.billing.create_key(key.to_string(), balance_micro) {
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
        "balance": balance_credits,
        "created": true
    }))
}

async fn api_billing_get_balance(state: Arc<RouterState>, key: &str) -> Response<Body> {
    match state.billing.get_balance(key) {
        Some(balance) => {
            let credits = if balance == -1 { -1.0 } else { balance as f64 / crate::billing::MICRO_PER_CREDIT as f64 };
            json_ok(&serde_json::json!({
                "key": key,
                "balance": credits,
                "balance_micro": balance,
            }))
        }
        None => RouterState::json_error(
            http::StatusCode::NOT_FOUND,
            "key not found",
            "key_not_found",
        ),
    }
}

async fn api_billing_get_level(state: Arc<RouterState>, key: &str) -> Response<Body> {
    let level = state.store.get_key_level(key);
    json_ok(&serde_json::json!({"key": key, "level": level}))
}

async fn api_billing_set_level(
    req: Request<Body>,
    state: Arc<RouterState>,
    key: &str,
) -> Response<Body> {
    let body: serde_json::Value = match parse_json_body(req).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let level = match body.get("level").and_then(|v| v.as_i64()) {
        Some(n) if n >= 0 || n == -1 => n as i32,
        Some(_) => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                "level must be >= 0 or -1",
                "bad_request",
            );
        }
        None => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                "missing 'level' field",
                "bad_request",
            );
        }
    };
    let key_str = key.to_string();
    let store = state.store.clone();
    let res = tokio::task::spawn_blocking(move || store.set_key_level(&key_str, level)).await;
    match res {
        Ok(Ok(())) => json_ok(&serde_json::json!({"ok": true, "key": key, "level": level})),
        Ok(Err(e)) => RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            &e.to_string(),
            "bad_request",
        ),
        Err(_) => RouterState::json_error(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "spawn_blocking failed",
            "internal_error",
        ),
    }
}

async fn api_billing_delete_key(state: Arc<RouterState>, key: &str) -> Response<Body> {
    match state.billing.delete_key(key) {
        Ok(true) => json_ok(&serde_json::json!({
            "deleted": true,
            "key": key
        })),
        Ok(false) => RouterState::json_error(
            http::StatusCode::NOT_FOUND,
            "key not found",
            "key_not_found",
        ),
        Err(e) => RouterState::json_error(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            &format!("delete failed: {e}"),
            "billing_error",
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

    let max_delta: i64 = 1_000_000;
    if payload.delta > max_delta || payload.delta < -max_delta {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            &format!("delta must be between -{} and {}", max_delta, max_delta),
            "bad_request",
        );
    }

    match state.billing.adjust_balance(key, payload.delta.saturating_mul(crate::billing::MICRO_PER_CREDIT)) {
        Some(balance) => {
            let credits = if balance == -1 { -1.0 } else { balance as f64 / crate::billing::MICRO_PER_CREDIT as f64 };
            json_ok(&serde_json::json!({ "key": key, "delta": payload.delta, "balance": credits }))
        }
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

    // Third segment selects a key sub-action: "" (CRUD), "release", "test", or "ban".
    let action = parts.next().unwrap_or("");
    match action {
        "" => match *req.method() {
            Method::POST => api_add_keys(req, state, upstream_id).await,
            Method::PUT => api_replace_keys(req, state, upstream_id).await,
            Method::DELETE => api_delete_keys(req, state, upstream_id).await,
            Method::GET => api_list_keys(state, upstream_id, req.uri()).await,
            _ => method_not_allowed(),
        },
        "release" => match *req.method() {
            Method::POST => api_release_keys(req, state, upstream_id).await,
            _ => method_not_allowed(),
        },
        "test" => match *req.method() {
            Method::POST => api_test_key(req, state, upstream_id).await,
            _ => method_not_allowed(),
        },
        "invalidate" | "ban" => match *req.method() {
            Method::POST => api_invalidate_keys(req, state, upstream_id).await,
            _ => method_not_allowed(),
        },
        "export" => match *req.method() {
            Method::GET => api_export_keys(state, upstream_id).await,
            _ => method_not_allowed(),
        },
        _ => Response::builder()
            .status(404)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"error":"not_found"}"#))
            .unwrap(),
    }
}

fn method_not_allowed() -> Response<Body> {
    Response::builder()
        .status(405)
        .header("content-type", "application/json")
        .body(Body::from(r#"{"error":"method_not_allowed"}"#))
        .unwrap()
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
    let routes_body: ModelRoutesBody = match parse_json_body(req).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match state.save_model_routes(routes_body.upstreams) {
        Ok(routes) => json_ok(&routes),
        Err(e) => {
            RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request")
        }
    }
}

async fn api_refresh_models(state: Arc<RouterState>, upstream_id: &str) -> Response<Body> {
    match state.fetch_models_preview(upstream_id).await {
        Ok(models) => json_ok(&serde_json::json!({
            "upstream": upstream_id,
            "count": models.len(),
            "models": models
        })),
        Err(e) => {
            RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request")
        }
    }
}

#[derive(Deserialize)]
struct UpstreamBody {
    id: String,
    base_url: String,
    weight: Option<usize>,
    max_concurrent_per_key: Option<u32>,
    format: Option<UpstreamFormat>,
    proxy: Option<String>,
    #[serde(default)]
    model_map: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    min_key_level: Option<i32>,
}

#[derive(Deserialize)]
struct UpstreamUpdateBody {
    base_url: String,
    weight: Option<usize>,
    max_concurrent_per_key: Option<u32>,
    format: Option<UpstreamFormat>,
    proxy: Option<String>,
    #[serde(default)]
    model_map: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    min_key_level: Option<i32>,
}

async fn api_add_upstream(req: Request<Body>, state: Arc<RouterState>) -> Response<Body> {
    let input: UpstreamBody = match parse_json_body(req).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if input.id.trim().is_empty() {
        return RouterState::json_error(http::StatusCode::BAD_REQUEST, "missing id", "bad_request");
    }
    if input.base_url.trim().is_empty() {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "missing base_url",
            "bad_request",
        );
    }
    if input.weight.unwrap_or(1) > 10_000 {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "weight must be ≤ 10000",
            "bad_request",
        );
    }
    if input.max_concurrent_per_key.unwrap_or(0) > 256 {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "max_concurrent_per_key must be ≤ 256",
            "bad_request",
        );
    }
    let min_level = input.min_key_level.unwrap_or(0);
    if min_level < 0 && min_level != -1 {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "min_key_level must be >= 0 or -1",
            "bad_request",
        );
    }
    let cfg = UpstreamConfig {
        id: input.id.trim().to_string(),
        base_url: input.base_url.trim().to_string(),
        weight: input.weight,
        max_concurrent_per_key: input.max_concurrent_per_key,
        format: input.format,
        proxy: input.proxy.filter(|p| !p.trim().is_empty()),
        model_map: input.model_map.unwrap_or_default(),
        min_key_level: input.min_key_level.unwrap_or(0),
    };
    let state2 = state.clone();
    let res = tokio::task::spawn_blocking(move || state2.add_upstream(cfg)).await;
    match res {
        Ok(Ok(_)) => json_ok(&serde_json::json!({
            "ok": true,
            "upstreams": state.snapshot.load_full().upstreams.len(),
        })),
        Ok(Err(e)) => {
            RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request")
        }
        Err(e) => RouterState::json_error(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
            "internal_error",
        ),
    }
}

async fn api_update_upstream(
    req: Request<Body>,
    state: Arc<RouterState>,
    upstream_id: &str,
) -> Response<Body> {
    let input: UpstreamUpdateBody = match parse_json_body(req).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if input.base_url.trim().is_empty() {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "missing base_url",
            "bad_request",
        );
    }
    if input.weight.unwrap_or(1) > 10_000 {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "weight must be ≤ 10000",
            "bad_request",
        );
    }
    if input.max_concurrent_per_key.unwrap_or(0) > 256 {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "max_concurrent_per_key must be ≤ 256",
            "bad_request",
        );
    }
    let min_level = input.min_key_level.unwrap_or(0);
    if min_level < 0 && min_level != -1 {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "min_key_level must be >= 0 or -1",
            "bad_request",
        );
    }
    let state2 = state.clone();
    let id = upstream_id.to_string();
    let cfg = UpstreamConfig {
        id: id.clone(),
        base_url: input.base_url.trim().to_string(),
        weight: input.weight,
        max_concurrent_per_key: input.max_concurrent_per_key,
        format: input.format,
        proxy: input.proxy.filter(|p| !p.trim().is_empty()),
        model_map: input.model_map.unwrap_or_default(),
        min_key_level: input.min_key_level.unwrap_or(0),
    };
    let res = tokio::task::spawn_blocking(move || state2.update_upstream(&id, cfg)).await;
    match res {
        Ok(Ok(_)) => json_ok(&serde_json::json!({"ok": true})),
        Ok(Err(e)) => {
            RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request")
        }
        Err(e) => RouterState::json_error(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
            "internal_error",
        ),
    }
}

async fn api_delete_upstream(
    req: Request<Body>,
    state: Arc<RouterState>,
    upstream_id: &str,
) -> Response<Body> {
    let delete_keys = query_get(req.uri(), "delete_keys")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let state2 = state.clone();
    let id = upstream_id.to_string();
    let res = tokio::task::spawn_blocking(move || state2.delete_upstream(&id, delete_keys)).await;
    match res {
        Ok(Ok(_)) => json_ok(&serde_json::json!({"ok": true, "delete_keys": delete_keys})),
        Ok(Err(e)) => {
            RouterState::json_error(http::StatusCode::BAD_REQUEST, &e.to_string(), "bad_request")
        }
        Err(e) => RouterState::json_error(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
            "internal_error",
        ),
    }
}

#[derive(Serialize)]
struct UpstreamInfo {
    id: String,
    base_url: String,
    format: String,
    proxy: Option<String>,
    weight: usize,
    max_concurrent_per_key: u32,
    keys_total: usize,
    keys_active: usize,
    keys_invalid: usize,
    keys_cooldown: usize,

    selected_total: u64,

    responses_2xx: u64,
    responses_3xx: u64,
    responses_4xx: u64,
    responses_5xx: u64,
    errors_timeout: u64,
    errors_network: u64,
}

fn build_upstream_info(u: &crate::state::Upstream, global_max: u32) -> UpstreamInfo {
    let keys_arc = u.keys.load_full();
    let total = keys_arc.len();
    let invalid = keys_arc.iter().filter(|k| !k.is_active()).count();
    let now = now_ms();
    let cooldown = keys_arc
        .iter()
        .filter(|k| {
            let until = k.cooldown_until_ms.load(std::sync::atomic::Ordering::Relaxed);
            k.is_active() && until > 0 && now < until
        })
        .count();
    let effective_max = if u.max_concurrent_per_key > 0 {
        u.max_concurrent_per_key
    } else {
        global_max
    };
    UpstreamInfo {
        id: u.id.to_string(),
        base_url: u.base_url.to_string(),
        format: u.format.as_str().to_string(),
        proxy: u.proxy.clone(),
        weight: u.weight,
        max_concurrent_per_key: effective_max,
        keys_total: total,
        keys_active: total.saturating_sub(invalid),
        keys_invalid: invalid,
        keys_cooldown: cooldown,
        selected_total: u
            .stats
            .selected_total
            .load(std::sync::atomic::Ordering::Relaxed),
        responses_2xx: u
            .stats
            .responses_2xx
            .load(std::sync::atomic::Ordering::Relaxed),
        responses_3xx: u
            .stats
            .responses_3xx
            .load(std::sync::atomic::Ordering::Relaxed),
        responses_4xx: u
            .stats
            .responses_4xx
            .load(std::sync::atomic::Ordering::Relaxed),
        responses_5xx: u
            .stats
            .responses_5xx
            .load(std::sync::atomic::Ordering::Relaxed),
        errors_timeout: u
            .stats
            .errors_timeout
            .load(std::sync::atomic::Ordering::Relaxed),
        errors_network: u
            .stats
            .errors_network
            .load(std::sync::atomic::Ordering::Relaxed),
    }
}

async fn api_get_model_costs(state: Arc<RouterState>) -> Response<Body> {
    let rt = state.runtime.load_full();
    let costs: std::collections::BTreeMap<&str, &crate::config::ModelCost> = rt
        .model_costs
        .iter()
        .map(|(k, v)| (k.as_str(), v))
        .collect();
    json_ok(&serde_json::json!(costs))
}

async fn api_set_model_costs(req: Request<Body>, state: Arc<RouterState>) -> Response<Body> {
    let input: serde_json::Value = match parse_json_body(req).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let obj = match input.as_object() {
        Some(o) => o,
        None => {
            return RouterState::json_error(
                http::StatusCode::BAD_REQUEST,
                "expected JSON object of model costs",
                "bad_request",
            );
        }
    };
    let mut costs = ahash::AHashMap::new();
    for (model, v) in obj {
        let cost: crate::config::ModelCost = match serde_json::from_value(v.clone()) {
            Ok(c) => c,
            Err(e) => {
                return RouterState::json_error(
                    http::StatusCode::BAD_REQUEST,
                    &format!("invalid cost for model {}: {}", model, e),
                    "bad_request",
                );
            }
        };
        costs.insert(model.clone(), cost);
    }
    state.set_model_costs(costs);
    json_ok(&serde_json::json!({"ok": true}))
}

async fn api_list_upstreams(state: Arc<RouterState>) -> Response<Body> {
    let snap = state.snapshot.load_full();
    let global_max = state.key_config().max_concurrent_per_key;

    let ups: Vec<UpstreamInfo> = snap
        .upstreams
        .iter()
        .map(|u| build_upstream_info(u, global_max))
        .collect();
    json_ok(&ups)
}

#[derive(Serialize)]
struct StatsSnapshot {
    ts_ms: u64,
    uptime_s: u64,

    max_retries: usize,
    retry_status_codes: Vec<u16>,

    requests_total: u64,
    requests_inflight: u64,
    upstream_selected_total: u64,

    responses_2xx: u64,
    responses_3xx: u64,
    responses_4xx: u64,
    responses_5xx: u64,

    errors_timeout: u64,
    errors_network: u64,
    queue_depth: u64,
    queue_timeout_total: u64,
    queue_enabled: bool,

    latency_avg_ms: f64,
    latency_max_ms: f64,
    latency_count: u64,

    prompt_tokens_total: u64,
    completion_tokens_total: u64,
    thought_tokens_total: u64,
    tokens_total: u64,

    upstreams: Vec<UpstreamInfo>,
}

fn build_snapshot(state: &RouterState) -> StatsSnapshot {
    let ts = now_ms();
    let uptime_s = (ts.saturating_sub(state.stats.started_at_ms)) / 1000;

    let latency_count = state
        .stats
        .latency_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let latency_total = state
        .stats
        .latency_ns_total
        .load(std::sync::atomic::Ordering::Relaxed);
    let latency_max = state
        .stats
        .latency_ns_max
        .load(std::sync::atomic::Ordering::Relaxed);

    let latency_avg_ms = if latency_count == 0 {
        0.0
    } else {
        (latency_total as f64) / (latency_count as f64) / 1_000_000.0
    };
    let latency_max_ms = (latency_max as f64) / 1_000_000.0;

    let snap = state.snapshot.load_full();
    let global_max = state.key_config().max_concurrent_per_key;
    let _now = ts;
    let ups: Vec<UpstreamInfo> = snap
        .upstreams
        .iter()
        .map(|u| build_upstream_info(u, global_max))
        .collect();
    let server = state.server_config();

    StatsSnapshot {
        ts_ms: ts,
        uptime_s,
        max_retries: state.max_retries(),
        retry_status_codes: state.retry_status_codes_sorted(),
        requests_total: state
            .stats
            .requests_total
            .load(std::sync::atomic::Ordering::Relaxed),
        requests_inflight: state
            .stats
            .requests_inflight
            .load(std::sync::atomic::Ordering::Relaxed),
        upstream_selected_total: state
            .stats
            .upstream_selected_total
            .load(std::sync::atomic::Ordering::Relaxed),
        responses_2xx: state
            .stats
            .responses_2xx
            .load(std::sync::atomic::Ordering::Relaxed),
        responses_3xx: state
            .stats
            .responses_3xx
            .load(std::sync::atomic::Ordering::Relaxed),
        responses_4xx: state
            .stats
            .responses_4xx
            .load(std::sync::atomic::Ordering::Relaxed),
        responses_5xx: state
            .stats
            .responses_5xx
            .load(std::sync::atomic::Ordering::Relaxed),
        errors_timeout: state
            .stats
            .errors_timeout
            .load(std::sync::atomic::Ordering::Relaxed),
        errors_network: state
            .stats
            .errors_network
            .load(std::sync::atomic::Ordering::Relaxed),
        queue_depth: state
            .stats
            .queue_depth
            .load(std::sync::atomic::Ordering::Relaxed),
        queue_timeout_total: state
            .stats
            .queue_timeout_total
            .load(std::sync::atomic::Ordering::Relaxed),
        queue_enabled: server.queue_enabled,
        latency_avg_ms,
        latency_max_ms,
        latency_count,
        prompt_tokens_total: state
            .stats
            .prompt_tokens_total
            .load(std::sync::atomic::Ordering::Relaxed),
        completion_tokens_total: state
            .stats
            .completion_tokens_total
            .load(std::sync::atomic::Ordering::Relaxed),
        thought_tokens_total: state
            .stats
            .thought_tokens_total
            .load(std::sync::atomic::Ordering::Relaxed),
        tokens_total: state
            .stats
            .tokens_total
            .load(std::sync::atomic::Ordering::Relaxed),
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

/// Read historical requests from the JSONL file, newest first.
/// ?limit=N  (default 100, max 5000)
/// ?before=ts_ms  (optional, only return entries before this timestamp)
async fn api_requests_history(state: Arc<RouterState>, uri: &http::Uri) -> Response<Body> {
    let limit: usize = query_get(uri, "limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 5000);
    let before: Option<u64> = query_get(uri, "before").and_then(|s| s.parse().ok());

    let path = state.requests_log_path.clone();
    let items = tokio::task::spawn_blocking(move || {
        read_request_log_reverse(&path, limit, before)
    }).await.unwrap_or_default();

    json_ok(&serde_json::json!({
        "now_ms": now_ms(),
        "count": items.len(),
        "source": "file",
        "requests": items
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

async fn api_config_preview(state: Arc<RouterState>) -> Response<Body> {
    json_ok(&state.config_preview())
}

/// Prometheus metrics endpoint.
/// Returns metrics in Prometheus text exposition format (OpenMetrics compatible).
pub async fn prometheus_metrics(state: Arc<RouterState>) -> Response<Body> {
    let snap = state.snapshot.load_full();
    let now = now_ms();
    let uptime_s = (now.saturating_sub(state.stats.started_at_ms)) / 1000;
    let mut buf = String::with_capacity(4096);

    write_prometheus_global(&mut buf, &state, uptime_s);
    write_prometheus_upstreams(&mut buf, snap.upstreams.as_slice(), now);
    write_prometheus_keys(&mut buf, snap.upstreams.as_slice(), now);

    Response::builder()
        .status(200)
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(Body::from(buf))
        .unwrap()
}

/// Global metrics: uptime, requests, inflight, queue, responses, errors, latency, selection.
fn write_prometheus_global(buf: &mut String, state: &RouterState, uptime_s: u64) {
    // Uptime
    let _ = writeln!(buf, "# HELP gptload_uptime_seconds Uptime in seconds");
    let _ = writeln!(buf, "# TYPE gptload_uptime_seconds gauge");
    let _ = writeln!(buf, "gptload_uptime_seconds {}", uptime_s);

    // Requests total
    let _ = writeln!(buf, "# HELP gptload_requests_total Total number of requests");
    let _ = writeln!(buf, "# TYPE gptload_requests_total counter");
    let _ = writeln!(
        buf,
        "gptload_requests_total {}",
        state.stats.requests_total.load(std::sync::atomic::Ordering::Relaxed)
    );

    // Requests inflight
    let _ = writeln!(buf, "# HELP gptload_requests_inflight Currently inflight requests");
    let _ = writeln!(buf, "# TYPE gptload_requests_inflight gauge");
    let _ = writeln!(
        buf,
        "gptload_requests_inflight {}",
        state.stats.requests_inflight.load(std::sync::atomic::Ordering::Relaxed)
    );

    // Queue
    let _ = writeln!(buf, "# HELP gptload_queue_depth Currently queued requests");
    let _ = writeln!(buf, "# TYPE gptload_queue_depth gauge");
    let _ = writeln!(
        buf,
        "gptload_queue_depth {}",
        state.stats.queue_depth.load(std::sync::atomic::Ordering::Relaxed)
    );
    let _ = writeln!(buf, "# HELP gptload_queue_timeout_total Queue timeout/full rejections");
    let _ = writeln!(buf, "# TYPE gptload_queue_timeout_total counter");
    let _ = writeln!(
        buf,
        "gptload_queue_timeout_total {}",
        state.stats.queue_timeout_total.load(std::sync::atomic::Ordering::Relaxed)
    );

    // Response status codes (global)
    let _ = writeln!(buf, "# HELP gptload_responses_total Total responses by status class");
    let _ = writeln!(buf, "# TYPE gptload_responses_total counter");
    let _ = writeln!(
        buf,
        "gptload_responses_total{{status_class=\"2xx\"}} {}",
        state.stats.responses_2xx.load(std::sync::atomic::Ordering::Relaxed)
    );
    let _ = writeln!(
        buf,
        "gptload_responses_total{{status_class=\"3xx\"}} {}",
        state.stats.responses_3xx.load(std::sync::atomic::Ordering::Relaxed)
    );
    let _ = writeln!(
        buf,
        "gptload_responses_total{{status_class=\"4xx\"}} {}",
        state.stats.responses_4xx.load(std::sync::atomic::Ordering::Relaxed)
    );
    let _ = writeln!(
        buf,
        "gptload_responses_total{{status_class=\"5xx\"}} {}",
        state.stats.responses_5xx.load(std::sync::atomic::Ordering::Relaxed)
    );

    // Errors
    let _ = writeln!(buf, "# HELP gptload_errors_total Total errors by type");
    let _ = writeln!(buf, "# TYPE gptload_errors_total counter");
    let _ = writeln!(
        buf,
        "gptload_errors_total{{type=\"timeout\"}} {}",
        state.stats.errors_timeout.load(std::sync::atomic::Ordering::Relaxed)
    );
    let _ = writeln!(
        buf,
        "gptload_errors_total{{type=\"network\"}} {}",
        state.stats.errors_network.load(std::sync::atomic::Ordering::Relaxed)
    );

    // Latency
    let latency_count = state.stats.latency_count.load(std::sync::atomic::Ordering::Relaxed);
    let latency_total_ns = state.stats.latency_ns_total.load(std::sync::atomic::Ordering::Relaxed);
    let latency_max_ns = state.stats.latency_ns_max.load(std::sync::atomic::Ordering::Relaxed);

    let _ = writeln!(buf, "# HELP gptload_request_duration_seconds Request latency");
    let _ = writeln!(buf, "# TYPE gptload_request_duration_seconds summary");
    if latency_count > 0 {
        let avg_s = (latency_total_ns as f64) / (latency_count as f64) / 1_000_000_000.0;
        let _ = writeln!(
            buf,
            "gptload_request_duration_seconds{{quantile=\"avg\"}} {:.6}",
            avg_s
        );
    }
    let _ = writeln!(
        buf,
        "gptload_request_duration_seconds{{quantile=\"max\"}} {:.6}",
        (latency_max_ns as f64) / 1_000_000_000.0
    );
    let _ = writeln!(buf, "gptload_request_duration_count {}", latency_count);
    let _ = writeln!(
        buf,
        "gptload_request_duration_sum_seconds {:.6}",
        (latency_total_ns as f64) / 1_000_000_000.0
    );

    // Upstream selection
    let _ = writeln!(buf, "# HELP gptload_upstream_selected_total Total upstream selections");
    let _ = writeln!(buf, "# TYPE gptload_upstream_selected_total counter");
    let _ = writeln!(
        buf,
        "gptload_upstream_selected_total {}",
        state.stats.upstream_selected_total.load(std::sync::atomic::Ordering::Relaxed)
    );
}

/// Per-upstream metrics: keys, responses, errors, selection.
fn write_prometheus_upstreams(buf: &mut String, upstreams: &[Arc<crate::state::Upstream>], now: u64) {
    let _ = writeln!(buf, "# HELP gptload_upstream_responses_total Per-upstream responses by status class");
    let _ = writeln!(buf, "# TYPE gptload_upstream_responses_total counter");
    let _ = writeln!(buf, "# HELP gptload_upstream_errors_total Per-upstream errors by type");
    let _ = writeln!(buf, "# TYPE gptload_upstream_errors_total counter");
    let _ = writeln!(buf, "# HELP gptload_upstream_selected_total Per-upstream selection count");
    let _ = writeln!(buf, "# TYPE gptload_upstream_selected_total counter");
    let _ = writeln!(buf, "# HELP gptload_upstream_keys Total keys per upstream");
    let _ = writeln!(buf, "# TYPE gptload_upstream_keys gauge");
    let _ = writeln!(buf, "# HELP gptload_upstream_active_keys Active keys per upstream");
    let _ = writeln!(buf, "# TYPE gptload_upstream_active_keys gauge");
    let _ = writeln!(buf, "# HELP gptload_upstream_invalid_keys Invalid keys per upstream");
    let _ = writeln!(buf, "# TYPE gptload_upstream_invalid_keys gauge");
    let _ = writeln!(buf, "# HELP gptload_upstream_cooldown_keys Keys in 429 cooldown per upstream");
    let _ = writeln!(buf, "# TYPE gptload_upstream_cooldown_keys gauge");

    for u in upstreams {
        let id = u.id.as_ref();
        let keys_arc = u.keys.load_full();
        let total = keys_arc.len();
        let mut invalid = 0usize;
        let mut active = 0usize;
        let mut cooldown = 0usize;
        for k in keys_arc.iter() {
            if k.is_active() {
                active += 1;
                let until = k.cooldown_until_ms.load(std::sync::atomic::Ordering::Relaxed);
                if until > 0 && now < until {
                    cooldown += 1;
                }
            } else {
                invalid += 1;
            }
        }

        let _ = writeln!(buf, "gptload_upstream_keys{{upstream=\"{}\"}} {}", id, total);
        let _ = writeln!(buf, "gptload_upstream_active_keys{{upstream=\"{}\"}} {}", id, active);
        let _ = writeln!(buf, "gptload_upstream_invalid_keys{{upstream=\"{}\"}} {}", id, invalid);
        let _ = writeln!(buf, "gptload_upstream_cooldown_keys{{upstream=\"{}\"}} {}", id, cooldown);

        let sel = u.stats.selected_total.load(std::sync::atomic::Ordering::Relaxed);
        let _ = writeln!(buf, "gptload_upstream_selected_total{{upstream=\"{}\"}} {}", id, sel);

        let r2 = u.stats.responses_2xx.load(std::sync::atomic::Ordering::Relaxed);
        let r3 = u.stats.responses_3xx.load(std::sync::atomic::Ordering::Relaxed);
        let r4 = u.stats.responses_4xx.load(std::sync::atomic::Ordering::Relaxed);
        let r5 = u.stats.responses_5xx.load(std::sync::atomic::Ordering::Relaxed);
        let _ = writeln!(buf, "gptload_upstream_responses_total{{upstream=\"{}\",status_class=\"2xx\"}} {}", id, r2);
        let _ = writeln!(buf, "gptload_upstream_responses_total{{upstream=\"{}\",status_class=\"3xx\"}} {}", id, r3);
        let _ = writeln!(buf, "gptload_upstream_responses_total{{upstream=\"{}\",status_class=\"4xx\"}} {}", id, r4);
        let _ = writeln!(buf, "gptload_upstream_responses_total{{upstream=\"{}\",status_class=\"5xx\"}} {}", id, r5);

        let et = u.stats.errors_timeout.load(std::sync::atomic::Ordering::Relaxed);
        let en = u.stats.errors_network.load(std::sync::atomic::Ordering::Relaxed);
        let _ = writeln!(buf, "gptload_upstream_errors_total{{upstream=\"{}\",type=\"timeout\"}} {}", id, et);
        let _ = writeln!(buf, "gptload_upstream_errors_total{{upstream=\"{}\",type=\"network\"}} {}", id, en);
    }
}

/// Global key distribution: total, active, invalid, cooldown counts.
fn write_prometheus_keys(buf: &mut String, upstreams: &[Arc<crate::state::Upstream>], now: u64) {
    let mut total_keys = 0usize;
    let mut active_keys = 0usize;
    let mut cooldown_keys = 0usize;
    for u in upstreams {
        let keys = u.keys.load_full();
        total_keys += keys.len();
        for k in keys.iter() {
            if k.is_active() {
                active_keys += 1;
                let until = k.cooldown_until_ms.load(std::sync::atomic::Ordering::Relaxed);
                if until > 0 && now < until {
                    cooldown_keys += 1;
                }
            }
        }
    }
    let _ = writeln!(buf, "# HELP gptload_keys_total Total keys across all upstreams");
    let _ = writeln!(buf, "# TYPE gptload_keys_total gauge");
    let _ = writeln!(buf, "gptload_keys_total {}", total_keys);
    let _ = writeln!(buf, "# HELP gptload_keys_active Active keys across all upstreams");
    let _ = writeln!(buf, "# TYPE gptload_keys_active gauge");
    let _ = writeln!(buf, "gptload_keys_active {}", active_keys);
    let _ = writeln!(buf, "# HELP gptload_keys_invalid Invalid keys across all upstreams");
    let _ = writeln!(buf, "# TYPE gptload_keys_invalid gauge");
    let _ = writeln!(buf, "gptload_keys_invalid {}", total_keys.saturating_sub(active_keys));
    let _ = writeln!(buf, "# HELP gptload_keys_cooldown Keys in 429 cooldown");
    let _ = writeln!(buf, "# TYPE gptload_keys_cooldown gauge");
    let _ = writeln!(buf, "gptload_keys_cooldown {}", cooldown_keys);
}

async fn stats_stream(state: Arc<RouterState>) -> Response<Body> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(32);
    let state2 = state.clone();

    tokio::spawn(async move {
        let mut last_total = state2
            .stats
            .requests_total
            .load(std::sync::atomic::Ordering::Relaxed);
        loop {
            let snap = build_snapshot(&state2);
            let total = snap.requests_total;
            let raw = total.saturating_sub(last_total); // requests since last tick (2s)
            last_total = total;
            let rpm = raw.saturating_mul(30); // extrapolate to per-minute

            let mut v = serde_json::to_value(&snap)
                .unwrap_or(serde_json::json!({"error":"snapshot_failed"}));
            if let serde_json::Value::Object(ref mut m) = v {
                m.insert("rpm".into(), serde_json::json!(rpm));
            }
            let s = match serde_json::to_string(&v) {
                Ok(s) => s,
                Err(_) => String::from(r#"{"error":"json"}"#),
            };
            let msg = format!("data: {}\n\n", s);

            if tx.send(Ok(Bytes::from(msg))).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
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

async fn requests_stream(state: Arc<RouterState>) -> Response<Body> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(128);
    let mut sub = state.subscribe_requests();

    tokio::spawn(async move {
        loop {
            match sub.recv().await {
                Ok(entry) => {
                    let payload = serde_json::to_string(&entry)
                        .unwrap_or_else(|_| String::from(r#"{"error":"json"}"#));
                    let msg = format!("data: {}\n\n", payload);
                    if tx.send(Ok(Bytes::from(msg))).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
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
        let admin_write_lock = state.admin_write_lock.clone();

        // Reload in blocking thread.
        let res = tokio::task::spawn_blocking(move || -> anyhow::Result<usize> {
            let _admin_guard = admin_write_lock
                .lock()
                .map_err(|_| anyhow::anyhow!("admin write lock poisoned"))?;
            let _guard = u2
                .keys_update_lock
                .lock()
                .map_err(|_| anyhow::anyhow!("key update lock poisoned"))?;
            let keys = store.load_all_keys(&id_clone)?;
            let ks = build_key_states(keys, None)?;
            let n = ks.len();
            u2.keys.store(ks);
            u2.rebuild_active_keys();
            Ok(n)
        })
        .await;

        match res {
            Ok(Ok(n)) => results.push(serde_json::json!({"id": id, "keys_total": n, "ok": true})),
            Ok(Err(e)) => {
                results.push(serde_json::json!({"id": id, "ok": false, "error": e.to_string()}))
            }
            Err(e) => {
                results.push(serde_json::json!({"id": id, "ok": false, "error": e.to_string()}))
            }
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

async fn api_add_keys(
    req: Request<Body>,
    state: Arc<RouterState>,
    upstream_id: &str,
) -> Response<Body> {
    let Some((_idx, upstream)) = state.upstream_by_id(upstream_id) else {
        return RouterState::json_error(
            http::StatusCode::NOT_FOUND,
            "unknown upstream id",
            "not_found",
        );
    };

    let (keys, dedupe) = match parse_keys_body(req).await {
        Ok(v) => v,
        Err(e) => return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e, "bad_request"),
    };

    let keys = if dedupe { dedupe_keys(keys) } else { keys };
    if keys.is_empty() {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "no keys provided",
            "bad_request",
        );
    }
    if let Err(e) = validate_keys(&keys) {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            &e.to_string(),
            "bad_request",
        );
    }

    let store = state.store.clone();
    let id = upstream_id.to_string();
    let upstream2 = upstream.clone();
    let admin_write_lock = state.admin_write_lock.clone();

    let res = tokio::task::spawn_blocking(move || -> anyhow::Result<serde_json::Value> {
        let _admin_guard = admin_write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("admin write lock poisoned"))?;
        let _guard = upstream2
            .keys_update_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("key update lock poisoned"))?;
        let add_res = store.add_keys(&id, &keys)?;
        let inserted = add_res.inserted;
        let existed = add_res.existed;

        // Build new KeyState arcs only for inserted keys and append to in-memory list.
        let inserted_states = build_key_states(add_res.inserted_keys, Some(&store))?;
        let old = upstream2.keys.load_full();
        let mut merged: Vec<Arc<crate::state::KeyState>> =
            Vec::with_capacity(old.len() + inserted_states.len());
        merged.extend(old.iter().cloned());
        merged.extend(inserted_states.iter().cloned());
        upstream2.keys.store(Arc::new(merged));
        upstream2.rebuild_active_keys();

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
        Ok(Err(e)) => RouterState::json_error(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
            "internal_error",
        ),
        Err(e) => RouterState::json_error(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
            "internal_error",
        ),
    }
}

async fn api_replace_keys(
    req: Request<Body>,
    state: Arc<RouterState>,
    upstream_id: &str,
) -> Response<Body> {
    let Some((_idx, upstream)) = state.upstream_by_id(upstream_id) else {
        return RouterState::json_error(
            http::StatusCode::NOT_FOUND,
            "unknown upstream id",
            "not_found",
        );
    };

    let (keys, dedupe) = match parse_keys_body(req).await {
        Ok(v) => v,
        Err(e) => return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e, "bad_request"),
    };

    let keys = if dedupe { dedupe_keys(keys) } else { keys };
    if keys.is_empty() {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "no keys provided",
            "bad_request",
        );
    }
    if let Err(e) = validate_keys(&keys) {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            &e.to_string(),
            "bad_request",
        );
    }

    let store = state.store.clone();
    let id = upstream_id.to_string();
    let upstream2 = upstream.clone();
    let admin_write_lock = state.admin_write_lock.clone();

    let res = tokio::task::spawn_blocking(move || -> anyhow::Result<serde_json::Value> {
        let _admin_guard = admin_write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("admin write lock poisoned"))?;
        let _guard = upstream2
            .keys_update_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("key update lock poisoned"))?;
        store.replace_keys(&id, &keys)?;
        let ks = build_key_states(keys, Some(&store))?;
        let n = ks.len();
        upstream2.keys.store(ks);
        upstream2.rebuild_active_keys();
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
        Ok(Err(e)) => RouterState::json_error(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
            "internal_error",
        ),
        Err(e) => RouterState::json_error(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
            "internal_error",
        ),
    }
}

async fn api_delete_keys(
    req: Request<Body>,
    state: Arc<RouterState>,
    upstream_id: &str,
) -> Response<Body> {
    let Some((_idx, upstream)) = state.upstream_by_id(upstream_id) else {
        return RouterState::json_error(
            http::StatusCode::NOT_FOUND,
            "unknown upstream id",
            "not_found",
        );
    };

    let (keys, dedupe) = match parse_keys_body(req).await {
        Ok(v) => v,
        Err(e) => return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e, "bad_request"),
    };
    if keys.is_empty() {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            "no keys provided",
            "bad_request",
        );
    }

    let store = state.store.clone();
    let id = upstream_id.to_string();
    let upstream2 = upstream.clone();
    let admin_write_lock = state.admin_write_lock.clone();

    let res = tokio::task::spawn_blocking(move || -> anyhow::Result<serde_json::Value> {
        let _admin_guard = admin_write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("admin write lock poisoned"))?;
        let _guard = upstream2
            .keys_update_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("key update lock poisoned"))?;
        let keys = if dedupe { dedupe_keys(keys) } else { keys };
        let removed = store.delete_keys(&id, &keys)?;

        // Update in-memory: filter out removed keys.
        let remove_set: ahash::AHashSet<&str> = keys.iter().map(|s| s.as_str()).collect();
        let old = upstream2.keys.load_full();
        let mut kept: Vec<Arc<crate::state::KeyState>> =
            Vec::with_capacity(old.len().saturating_sub(removed));
        for k in old.iter() {
            if !remove_set.contains(k.key.as_ref()) {
                kept.push(k.clone());
            }
        }
        upstream2.keys.store(Arc::new(kept));
        upstream2.rebuild_active_keys();

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
        Ok(Err(e)) => RouterState::json_error(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
            "internal_error",
        ),
        Err(e) => RouterState::json_error(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
            "internal_error",
        ),
    }
}

#[derive(Deserialize, Default)]
struct KeyStatusBody {
    #[serde(default)]
    keys: Option<Vec<String>>,
    #[serde(default)]
    all: Option<bool>,
}

enum KeyStatusScope {
    All,
    Keys(ahash::AHashSet<String>),
}

/// `all:true` selects all keys. Otherwise `keys` must contain at least one non-empty key.
fn scoped_key_set(body: &KeyStatusBody) -> Result<KeyStatusScope, String> {
    if matches!(body.all, Some(true)) {
        return Ok(KeyStatusScope::All);
    }
    let keys = body
        .keys
        .as_ref()
        .ok_or_else(|| "keys must be provided unless all is true".to_string())?;
    let mut set = ahash::AHashSet::with_capacity(keys.len().max(1));
    for k in keys {
        let k = k.trim();
        if !k.is_empty() {
            if let Err(e) = crate::util::validate_key_chars(k) {
                return Err(e);
            }
            set.insert(k.to_string());
        }
    }
    if set.is_empty() {
        Err("keys must contain at least one non-empty key unless all is true".to_string())
    } else {
        Ok(KeyStatusScope::Keys(set))
    }
}

async fn api_release_keys(
    req: Request<Body>,
    state: Arc<RouterState>,
    upstream_id: &str,
) -> Response<Body> {
    let (_idx, upstream) = match get_upstream(&state, upstream_id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let body: KeyStatusBody = match parse_json_body(req).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let restored = match scoped_key_set(&body) {
        Ok(KeyStatusScope::Keys(set)) => upstream.restore_keys(&set),
        Ok(KeyStatusScope::All) => upstream.restore_all_keys(),
        Err(e) => return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e, "bad_request"),
    };
    json_ok(&serde_json::json!({
        "ok": true,
        "upstream": upstream_id,
        "restored": restored,
        "keys_total": upstream.keys_len()
    }))
}

async fn api_invalidate_keys(
    req: Request<Body>,
    state: Arc<RouterState>,
    upstream_id: &str,
) -> Response<Body> {
    let (_idx, upstream) = match get_upstream(&state, upstream_id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let body: KeyStatusBody = match parse_json_body(req).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let invalidated = match scoped_key_set(&body) {
        Ok(KeyStatusScope::Keys(set)) => upstream.invalidate_keys(&set),
        Ok(KeyStatusScope::All) => {
            let all: ahash::AHashSet<String> = upstream
                .keys
                .load_full()
                .iter()
                .map(|k| k.key.to_string())
                .collect();
            upstream.invalidate_keys(&all)
        }
        Err(e) => return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e, "bad_request"),
    };
    json_ok(&serde_json::json!({
        "ok": true,
        "upstream": upstream_id,
        "invalidated": invalidated,
        "keys_total": upstream.keys_len()
    }))
}

#[derive(Deserialize)]
struct KeyTestBody {
    key: String,
}

async fn api_test_key(
    req: Request<Body>,
    state: Arc<RouterState>,
    upstream_id: &str,
) -> Response<Body> {
    let body: KeyTestBody = match parse_json_body(req).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let started = std::time::Instant::now();
    match state.test_key_by_value(upstream_id, &body.key).await {
        Ok(valid) => json_ok(&serde_json::json!({
            "ok": true,
            "valid": valid,
            "latency_ms": started.elapsed().as_millis() as u64,
            "upstream": upstream_id,
        })),
        Err(e) => RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            &e.to_string(),
            "key_test_failed",
        ),
    }
}

#[derive(Serialize)]
struct KeyInfo {
    key: String,
    status: &'static str,
    failure_count: u32,
    active_requests: u32,
    /// Unix ms timestamp when 429 cooldown expires. 0 = not in cooldown.
    cooldown_until_ms: u64,
    latency_p50_ms: Option<u64>,
    latency_p90_ms: Option<u64>,
    latency_p99_ms: Option<u64>,
}

async fn api_list_keys(
    state: Arc<RouterState>,
    upstream_id: &str,
    uri: &http::Uri,
) -> Response<Body> {
    let Some((_idx, upstream)) = state.upstream_by_id(upstream_id) else {
        return RouterState::json_error(
            http::StatusCode::NOT_FOUND,
            "unknown upstream id",
            "not_found",
        );
    };

    let limit: usize = query_get(uri, "limit")
        .and_then(|s: &str| s.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 5000);
    let offset: usize = query_get(uri, "offset")
        .and_then(|s: &str| s.parse::<usize>().ok())
        .unwrap_or(0);

    let keys_arc = upstream.keys.load_full();
    let keys = keys_arc.as_ref();
    let total = keys.len();
    let offset = offset.min(total);
    let end = offset.saturating_add(limit).min(total);

    let mut out: Vec<KeyInfo> = Vec::with_capacity(end.saturating_sub(offset));
    for k in keys.iter().skip(offset).take(end - offset) {
        let status = if k.is_active() { "active" } else { "invalid" };
        let failure_count = k.failure_count.load(std::sync::atomic::Ordering::Relaxed);
        let active_requests = k.active_requests.load(std::sync::atomic::Ordering::Relaxed);
        let cooldown_until_ms = k
            .cooldown_until_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        let (latency_p50_ms, latency_p90_ms, latency_p99_ms) = k.latency_percentiles();
        out.push(KeyInfo {
            key: k.key.to_string(),
            status,
            failure_count,
            active_requests,
            cooldown_until_ms,
            latency_p50_ms,
            latency_p90_ms,
            latency_p99_ms,
        });
    }

    json_ok(&serde_json::json!({
        "upstream": upstream_id,
        "total": total,
        "offset": offset,
        "limit": limit,
        "keys": out
    }))
}

async fn api_export_keys(state: Arc<RouterState>, upstream_id: &str) -> Response<Body> {
    let (_idx, upstream) = match get_upstream(&state, upstream_id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let keys = upstream.keys.load_full();
    let mut body = String::with_capacity(keys.len() * 60);
    for k in keys.iter() {
        body.push_str(k.key.as_ref());
        body.push('\n');
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let filename = format!("{}_keys_{}.txt", upstream_id, ts);
    Response::builder()
        .status(200)
        .header("content-type", "text/plain; charset=utf-8")
        .header("content-disposition", format!("attachment; filename=\"{}\"", filename))
        .body(Body::from(body))
        .unwrap()
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

    let body_bytes = read_body_limit(req, 50 * 1024 * 1024)
        .await
        .map_err(|e| e.to_string())?; // 50MB

    if content_type.starts_with("application/json") {
        let v: JsonKeysBody =
            serde_json::from_slice(&body_bytes).map_err(|e| format!("invalid json: {e}"))?;
        let mut keys: Vec<String> = Vec::with_capacity(v.keys.len());
        for k in v.keys {
            let k = k.trim().to_string();
            if k.is_empty() {
                continue;
            }
            if let Err(e) = crate::util::validate_key_chars(&k) {
                return Err(e);
            }
            keys.push(k);
        }
        Ok((keys, v.dedupe.unwrap_or(true)))
    } else {
        // Treat as plain text.
        let s = std::str::from_utf8(&body_bytes).map_err(|_| "body is not utf-8".to_string())?;
        let mut keys: Vec<String> = Vec::new();
        for line in s.lines() {
            let k = line.trim();
            if k.is_empty() {
                continue;
            }
            if let Err(e) = crate::util::validate_key_chars(k) {
                return Err(e);
            }
            keys.push(k.to_string());
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

/// Read JSONL log file from end to start using reverse chunk reading.
/// Avoids loading the entire file into memory — only accumulates matching entries.
fn read_request_log_reverse(
    path: &std::path::Path,
    limit: usize,
    before: Option<u64>,
) -> Vec<serde_json::Value> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    let file_size = match file.seek(SeekFrom::End(0)) {
        Ok(n) => n,
        Err(_) => return Vec::new(),
    };
    if file_size == 0 {
        return Vec::new();
    }

    let mut out: Vec<serde_json::Value> = Vec::with_capacity(limit);
    let mut leftover: Vec<u8> = Vec::new(); // bytes of a line that crosses chunk boundary
    let mut chunk = vec![0u8; 4096];
    let mut pos = file_size;

    while pos > 0 && out.len() < limit {
        let read_size = (pos as usize).min(chunk.len());
        pos -= read_size as u64;
        if file.seek(SeekFrom::Start(pos)).is_err() {
            break;
        }
        if file.read_exact(&mut chunk[..read_size]).is_err() {
            break;
        }

        // Prepend the current chunk to leftover to form complete lines at the boundary.
        let mut combined = chunk[..read_size].to_vec();
        combined.extend_from_slice(&leftover);
        leftover.clear();

        // Split into lines (keep empty slices to detect trailing newline).
        let mut lines: Vec<&[u8]> = combined.split(|&b| b == b'\n').collect();

        // If the file doesn't end with a newline, the first segment of the
        // first chunk is not really a line — it's the last incomplete line.
        if pos == 0 && lines.len() == 1 && !lines[0].is_empty() {
            // Single line at the very beginning, no trailing newline.
            if let Ok(text) = std::str::from_utf8(lines[0]) {
                let text = text.trim();
                if !text.is_empty() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                        if passes_before(&v, before) && out.len() < limit {
                            out.push(v);
                        }
                    }
                }
            }
            break;
        }

        // The first entry (after split) is the start of a line that was cut;
        // save it for the previous chunk. Process the rest in reverse.
        let carry = lines.remove(0);
        leftover = carry.to_vec();

        for raw in lines.iter().rev() {
            if out.len() >= limit {
                break;
            }
            if let Ok(text) = std::str::from_utf8(raw) {
                let text = text.trim();
                if text.is_empty() {
                    continue;
                }
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                    if passes_before(&v, before) {
                        out.push(v);
                    }
                }
            }
        }
    }

    // Process the final leftover from the beginning of the file.
    if !leftover.is_empty() && out.len() < limit {
        if let Ok(text) = std::str::from_utf8(&leftover) {
            let text = text.trim();
            if !text.is_empty() {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                    if passes_before(&v, before) {
                        out.push(v);
                    }
                }
            }
        }
    }

    out
}

fn passes_before(v: &serde_json::Value, before: Option<u64>) -> bool {
    match before {
        Some(b) => v.get("ts_ms").and_then(|t| t.as_u64()).map(|ts| ts < b).unwrap_or(true),
        None => true,
    }
}
