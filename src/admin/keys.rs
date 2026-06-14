use crate::state::{build_key_states, validate_keys, RouterState};
use hyper::{Body, Request, Response};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize)]
struct JsonKeysBody {
    keys: Vec<String>,
    dedupe: Option<bool>,
}

pub(super) async fn api_add_keys(
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

    let res = state.with_key_write_lock(&upstream, move |u2| {
        let add_res = store.add_keys(&id, &keys)?;
        let inserted = add_res.inserted;
        let existed = add_res.existed;

        // Build new KeyState arcs only for inserted keys and append to in-memory list.
        let inserted_states = build_key_states(add_res.inserted_keys, Some(&store))?;
        let old = u2.keys.load_full();
        let mut merged: Vec<Arc<crate::state::KeyState>> =
            Vec::with_capacity(old.len() + inserted_states.len());
        merged.extend(old.iter().cloned());
        merged.extend(inserted_states.iter().cloned());
        u2.keys.store(Arc::new(merged));
        u2.rebuild_active_keys();

        Ok(serde_json::json!({
            "ok": true,
            "upstream": id,
            "inserted": inserted,
            "existed": existed,
            "keys_total": u2.keys_len()
        }))
    })
    .await;

    let v = match crate::util::spawn_result(res, http::StatusCode::INTERNAL_SERVER_ERROR) {
        Ok(v) => v,
        Err((status, msg)) => return RouterState::json_error(status, &msg, "internal_error"),
    };
    let state2 = state.clone();
    let id2 = upstream_id.to_string();
    tokio::spawn(async move {
        state2.refresh_missing_models_for_upstream(&id2).await;
    });
    super::json_ok(&v)
}

pub(super) async fn api_replace_keys(
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

    let res = state.with_key_write_lock(&upstream, move |u2| {
        store.replace_keys(&id, &keys)?;
        let ks = build_key_states(keys, Some(&store))?;
        let n = ks.len();
        u2.keys.store(ks);
        u2.rebuild_active_keys();
        Ok(serde_json::json!({
            "ok": true,
            "upstream": id,
            "keys_total": n
        }))
    })
    .await;

    let v = match crate::util::spawn_result(res, http::StatusCode::INTERNAL_SERVER_ERROR) {
        Ok(v) => v,
        Err((status, msg)) => return RouterState::json_error(status, &msg, "internal_error"),
    };
    let state2 = state.clone();
    let id2 = upstream_id.to_string();
    tokio::spawn(async move {
        state2.refresh_missing_models_for_upstream(&id2).await;
    });
    super::json_ok(&v)
}

pub(super) async fn api_delete_keys(
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

    let res = state.with_key_write_lock(&upstream, move |u2| {
        let keys = if dedupe { dedupe_keys(keys) } else { keys };
        let removed = store.delete_keys(&id, &keys)?;

        // Update in-memory: filter out removed keys.
        let remove_set: ahash::AHashSet<&str> = keys.iter().map(|s| s.as_str()).collect();
        let old = u2.keys.load_full();
        let mut kept: Vec<Arc<crate::state::KeyState>> =
            Vec::with_capacity(old.len().saturating_sub(removed));
        for k in old.iter() {
            if !remove_set.contains(k.key.as_ref()) {
                kept.push(k.clone());
            }
        }
        u2.keys.store(Arc::new(kept));
        u2.rebuild_active_keys();

        Ok(serde_json::json!({
            "ok": true,
            "upstream": id,
            "removed": removed,
            "keys_total": u2.keys_len()
        }))
    })
    .await;

    match crate::util::spawn_result(res, http::StatusCode::INTERNAL_SERVER_ERROR) {
        Ok(v) => super::json_ok(&v),
        Err((status, msg)) => RouterState::json_error(status, &msg, "internal_error"),
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

pub(super) async fn api_release_keys(
    req: Request<Body>,
    state: Arc<RouterState>,
    upstream_id: &str,
) -> Response<Body> {
    let (_idx, upstream) = match super::upstreams::get_upstream(&state, upstream_id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let body: KeyStatusBody = match super::parse_json_body(req).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let restored = match scoped_key_set(&body) {
        Ok(KeyStatusScope::Keys(set)) => upstream.restore_keys(&set),
        Ok(KeyStatusScope::All) => upstream.restore_all_keys(),
        Err(e) => return RouterState::json_error(http::StatusCode::BAD_REQUEST, &e, "bad_request"),
    };
    super::json_ok(&serde_json::json!({
        "ok": true,
        "upstream": upstream_id,
        "restored": restored,
        "keys_total": upstream.keys_len()
    }))
}

pub(super) async fn api_invalidate_keys(
    req: Request<Body>,
    state: Arc<RouterState>,
    upstream_id: &str,
) -> Response<Body> {
    let (_idx, upstream) = match super::upstreams::get_upstream(&state, upstream_id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let body: KeyStatusBody = match super::parse_json_body(req).await {
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
    super::json_ok(&serde_json::json!({
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

pub(super) async fn api_test_key(
    req: Request<Body>,
    state: Arc<RouterState>,
    upstream_id: &str,
) -> Response<Body> {
    let body: KeyTestBody = match super::parse_json_body(req).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let started = std::time::Instant::now();
    match state.test_key_by_value(upstream_id, &body.key).await {
        Ok(valid) => super::json_ok(&serde_json::json!({
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

pub(super) async fn api_list_keys(
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

    let limit: usize = crate::util::query_get(uri, "limit")
        .and_then(|s: &str| s.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 5000);
    let offset: usize = crate::util::query_get(uri, "offset")
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

    super::json_ok(&serde_json::json!({
        "upstream": upstream_id,
        "total": total,
        "offset": offset,
        "limit": limit,
        "keys": out
    }))
}

pub(super) async fn api_export_keys(state: Arc<RouterState>, upstream_id: &str) -> Response<Body> {
    let (_idx, upstream) = match super::upstreams::get_upstream(&state, upstream_id) {
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

    let body_bytes = crate::util::read_body_limit(req, 50 * 1024 * 1024)
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
