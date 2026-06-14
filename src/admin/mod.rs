mod upstreams;
mod keys;
mod stats;

pub(crate) use upstreams::{handle_upstream_subroutes, api_add_upstream, api_list_upstreams};
pub(crate) use stats::{build_snapshot, api_stats_snapshot, api_requests, api_requests_history, api_metrics, prometheus_metrics};

// Re-export shared response builders from util for use within admin submodules.
pub(crate) use crate::util::json_ok;

use crate::state::{build_key_states, RouterState};
use hyper::{Body, Request, Response};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Read request body and parse as JSON. Returns error response on failure.
pub(crate) async fn parse_json_body<T: serde::de::DeserializeOwned>(
    req: Request<Body>,
) -> Result<T, Response<Body>> {
    let body = crate::util::read_body_limit(req, 10 * 1024 * 1024).await.map_err(|e| {
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

/// Unwrap a `Result<T, Response<Body>>`, returning the error response early on failure.
/// Replaces the 4-line `match parse_json_body(req).await { Ok(v) => v, Err(resp) => return resp }` pattern.
macro_rules! try_parse {
    ($expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };
}
pub(crate) use try_parse;

pub(crate) fn method_not_allowed() -> Response<Body> {
    RouterState::json_error(
        http::StatusCode::METHOD_NOT_ALLOWED,
        "method not allowed",
        "method_not_allowed",
    )
}

pub(crate) async fn api_get_model_routes(state: Arc<RouterState>) -> Response<Body> {
    let routes = state.get_model_routes();
    json_ok(&routes)
}

#[derive(Deserialize)]
struct ModelRoutesBody {
    upstreams: BTreeMap<String, Vec<String>>,
}

pub(crate) async fn api_put_model_routes(req: Request<Body>, state: Arc<RouterState>) -> Response<Body> {
    let routes_body: ModelRoutesBody = try_parse!(parse_json_body(req).await);
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

pub(crate) async fn api_get_model_costs(state: Arc<RouterState>) -> Response<Body> {
    let rt = state.runtime.load_full();
    let costs: std::collections::BTreeMap<&str, &crate::config::ModelCost> = rt
        .model_costs
        .iter()
        .map(|(k, v)| (k.as_str(), v))
        .collect();
    json_ok(&serde_json::json!(costs))
}

pub(crate) async fn api_set_model_costs(req: Request<Body>, state: Arc<RouterState>) -> Response<Body> {
    let input: serde_json::Value = try_parse!(parse_json_body(req).await);
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

pub(crate) async fn api_config_preview(state: Arc<RouterState>) -> Response<Body> {
    json_ok(&state.config_preview())
}

pub(crate) async fn api_reload_all(state: Arc<RouterState>) -> Response<Body> {
    let mut results = Vec::new();
    let snap = state.snapshot.load_full();
    for u in snap.upstreams.iter() {
        let id = u.id.to_string();
        let id_clone = id.clone();
        let store = state.store.clone();

        let res = state.with_key_write_lock(u, move |u2| {
            let keys = store.load_all_keys(&id_clone)?;
            let ks = build_key_states(keys)?;
            let n = ks.len();
            u2.keys.store(ks);
            u2.rebuild_active_keys();
            Ok(n)
        }).await;

        match crate::util::spawn_result(res, http::StatusCode::INTERNAL_SERVER_ERROR) {
            Ok(n) => results.push(serde_json::json!({"id": id, "keys_total": n, "ok": true})),
            Err((_, msg)) => results.push(serde_json::json!({"id": id, "ok": false, "error": msg})),
        }
    }

    let state2 = state.clone();
    tokio::spawn(async move {
        state2.refresh_missing_models_routes().await;
    });

    json_ok(&serde_json::json!({ "reloaded": results }))
}
