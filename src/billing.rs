use crate::storage::KeyStore;
use crate::state::RouterState;
use ahash::AHashMap;
use hyper::{Body, Method, Request, Response};
use serde::Deserialize;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

/// Balance unit: 1 credit = 1,000,000 micro-credits (6 decimal places).
pub const MICRO_PER_CREDIT: i64 = 1_000_000;
/// Maximum balance credits that fit in i64 when converted to micro-credits.
pub const MAX_BALANCE_CREDITS: i64 = i64::MAX / MICRO_PER_CREDIT;
/// Minimum charge: 1 micro-credit.
const MIN_COST_MICRO: i64 = 1;
/// Reservation amount in micro-credits: 1 µcredit as a light gate.
const RESERVE_MICRO: i64 = 1;

pub struct BillingStore {
    balances: Arc<RwLock<AHashMap<String, Arc<AtomicI64>>>>,
    persist_tx: Sender<PersistUpdate>,
}

pub enum ReserveResult {
    Reserved,
    Insufficient,
    Missing,
}

enum PersistUpdate {
    Set { key: String, balance: i64 },
    Delete { key: String },
}

impl BillingStore {
    pub fn new(store: &KeyStore) -> anyhow::Result<Self> {
        let tree = store.open_billing_tree()?;
        let balances = Arc::new(RwLock::new(AHashMap::new()));

        {
            let mut map = balances
                .write()
                .map_err(|_| anyhow::anyhow!("billing balances lock poisoned"))?;
            for item in tree.iter() {
                let (k, v) = item?;
                let key = String::from_utf8_lossy(&k).to_string();
                if let Some(balance) = decode_balance(&v) {
                    map.insert(key, Arc::new(AtomicI64::new(balance)));
                }
            }
        }

        let (tx, rx) = mpsc::channel::<PersistUpdate>();
        let persist_tree = tree.clone();
        thread::spawn(move || {
            let mut pending: AHashMap<String, i64> = AHashMap::new();
            let mut last_flush = Instant::now();
            loop {
                match rx.recv_timeout(Duration::from_millis(500)) {
                    Ok(msg) => match msg {
                        PersistUpdate::Set { key, balance } => {
                            pending.insert(key, balance);
                        }
                        PersistUpdate::Delete { key } => {
                            pending.remove(&key);
                            let _ = persist_tree.remove(key.as_bytes());
                        }
                    },
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }

                if pending.len() >= 1024 || last_flush.elapsed() >= Duration::from_secs(1) {
                    flush_pending(&persist_tree, &mut pending);
                    last_flush = Instant::now();
                }
            }

            if !pending.is_empty() {
                flush_pending(&persist_tree, &mut pending);
            }
        });

        Ok(Self {
            balances,
            persist_tx: tx,
        })
    }

    pub fn create_key(&self, key: String, balance: i64) -> anyhow::Result<bool> {
        let mut map = self
            .balances
            .write()
            .map_err(|_| anyhow::anyhow!("billing balances lock poisoned"))?;
        if map.contains_key(&key) {
            return Ok(false);
        }
        map.insert(key.clone(), Arc::new(AtomicI64::new(balance)));
        drop(map);
        let _ = self.persist_tx.send(PersistUpdate::Set { key, balance });
        Ok(true)
    }

    pub fn delete_key(&self, key: &str) -> anyhow::Result<bool> {
        let mut map = self
            .balances
            .write()
            .map_err(|_| anyhow::anyhow!("billing balances lock poisoned"))?;
        if map.remove(key).is_some() {
            drop(map);
            let _ = self.persist_tx.send(PersistUpdate::Delete {
                key: key.to_string(),
            });
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn get_balance(&self, key: &str) -> Option<i64> {
        let map = self.balances.read().ok()?;
        map.get(key).map(|v| v.load(Ordering::Relaxed))
    }

    pub fn adjust_balance(&self, key: &str, delta: i64) -> Option<i64> {
        let map = self.balances.read().ok()?;
        let balance = map.get(key)?.clone();
        drop(map);
        let mut cur = balance.load(Ordering::Relaxed);
        // -1 means unlimited, reject any adjustment to protect the sentinel
        if cur == -1 {
            return Some(-1);
        }
        loop {
            let new_balance = cur.saturating_add(delta);
            match balance.compare_exchange(cur, new_balance, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    let _ = self.persist_tx.send(PersistUpdate::Set {
                        key: key.to_string(),
                        balance: new_balance,
                    });
                    return Some(new_balance);
                }
                Err(v) => cur = v,
            }
        }
    }

    pub fn reserve_request(&self, key: &str) -> ReserveResult {
        let map = match self.balances.read() {
            Ok(map) => map,
            Err(_) => return ReserveResult::Missing,
        };
        let Some(balance) = map.get(key).cloned() else {
            return ReserveResult::Missing;
        };
        drop(map);

        let mut cur = balance.load(Ordering::Relaxed);
        // -1 means unlimited, always succeed without decrementing
        if cur == -1 {
            return ReserveResult::Reserved;
        }
        loop {
            if cur < RESERVE_MICRO {
                return ReserveResult::Insufficient;
            }
            let new_balance = cur.saturating_sub(RESERVE_MICRO);
            match balance.compare_exchange(cur, new_balance, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    tracing::debug!(
                        key, cur, new_balance,
                        "billing: reserve key={key} {cur} → {new_balance}"
                    );
                    let _ = self.persist_tx.send(PersistUpdate::Set {
                        key: key.to_string(),
                        balance: new_balance,
                    });
                    return ReserveResult::Reserved;
                }
                Err(v) => cur = v,
            }
        }
    }

    pub fn list_keys(&self) -> Vec<(String, i64)> {
        let map = match self.balances.read() {
            Ok(m) => m,
            Err(_) => return Vec::new(),
        };
        let mut keys: Vec<(String, i64)> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect();
        keys.sort_by(|a, b| a.0.cmp(&b.0));
        keys
    }

    pub fn release_reservation(&self, key: &str) -> Option<i64> {
        // -1 means unlimited, nothing to release
        if self.get_balance(key) == Some(-1) {
            return Some(-1);
        }
        let result = self.adjust_balance(key, RESERVE_MICRO);
        tracing::debug!(key, ?result, "billing: release key={key}");
        result
    }

    /// Settle reserved usage. 1 µcredit is already pre-deducted.
    /// If cost > 1: deduct (cost - 1) extra, clamped to 0 (→ 401 on next request).
    /// If cost <= 1: no-op (the pre-deducted 1 already covers it).
    pub fn settle_reserved_usage(
        &self,
        key: &str,
        prompt_tokens: u64,
        completion_tokens: u64,
        model: &str,
        model_costs: &ahash::AHashMap<String, crate::config::ModelCost>,
    ) -> Option<i64> {
        let map = self.balances.read().ok()?;
        let balance = map.get(key)?.clone();
        drop(map);

        let cost = compute_credit_cost(prompt_tokens, completion_tokens, model, model_costs);

        // Cost already covered by pre-deducted 1 µcredit.
        if cost <= RESERVE_MICRO {
            return Some(balance.load(Ordering::Relaxed));
        }

        let extra = cost - RESERVE_MICRO;
        let mut cur = balance.load(Ordering::Relaxed);
        // -1 means unlimited
        if cur == -1 {
            return Some(-1);
        }
        loop {
            let new_balance = cur.saturating_sub(extra);
            // Clamp to 0 — next request will get 401 insufficient balance.
            let clamped = if new_balance < 0 { 0 } else { new_balance };
            match balance.compare_exchange(cur, clamped, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    tracing::debug!(
                        key, cur, clamped, cost, extra,
                        "billing: settle key={key} cost={cost} extra={extra} {cur} → {clamped}"
                    );
                    let _ = self.persist_tx.send(PersistUpdate::Set {
                        key: key.to_string(),
                        balance: clamped,
                    });
                    return Some(clamped);
                }
                Err(v) => cur = v,
            }
        }
    }
}

fn decode_balance(bytes: &[u8]) -> Option<i64> {
    if bytes.len() == 8 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(bytes);
        Some(i64::from_le_bytes(arr))
    } else {
        None
    }
}

fn flush_pending(tree: &sled::Tree, pending: &mut AHashMap<String, i64>) {
    if pending.is_empty() {
        return;
    }
    for (key, balance) in pending.drain() {
        let encoded = balance.to_le_bytes();
        let _ = tree.insert(key.as_bytes(), &encoded);
    }
    let _ = tree.flush();
}

/// Cost in micro-credits: ceil((prompt×input + completion×output)/1000 × 1_000_000).
/// Unknown models fall back to input=0, output=1.0. Minimum 1 micro-credit.
pub fn compute_credit_cost(
    prompt_tokens: u64,
    completion_tokens: u64,
    model: &str,
    model_costs: &ahash::AHashMap<String, crate::config::ModelCost>,
) -> i64 {
    let rate = model_costs.get(model);
    let input_rate = rate.map(|r| r.input).unwrap_or(0.0);
    let output_rate = rate.map(|r| r.output).unwrap_or(1.0);

    let raw = (prompt_tokens as f64 * input_rate + completion_tokens as f64 * output_rate) / 1000.0;
    let cost = (raw * MICRO_PER_CREDIT as f64).ceil() as i64;
    let final_cost = cost.max(MIN_COST_MICRO);
    tracing::debug!(
        model, prompt_tokens, completion_tokens, input_rate, output_rate,
        raw, cost, final_cost,
        "billing: micro_credits={final_cost}"
    );
    final_cost
}

// ── Billing Admin API handlers ──

#[derive(Deserialize)]
struct BillingCreateBody {
    key: String,
    balance: Option<i64>,
}

#[derive(Deserialize)]
struct BillingAdjustBody {
    delta: i64,
}

pub(crate) async fn handle_billing_key_subroutes(
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
            _ => crate::admin::method_not_allowed(),
        };
    }

    if action == "adjust" {
        return match *req.method() {
            Method::POST => api_billing_adjust_balance(req, state, key).await,
            _ => crate::admin::method_not_allowed(),
        };
    }

    if action == "level" {
        return match *req.method() {
            Method::GET => api_billing_get_level(state, key).await,
            Method::POST => api_billing_set_level(req, state, key).await,
            _ => crate::admin::method_not_allowed(),
        };
    }

    Response::builder()
        .status(404)
        .header("content-type", "application/json")
        .body(Body::from(r#"{"error":"not_found"}"#))
        .unwrap()
}

pub(crate) async fn api_billing_list_keys(state: Arc<RouterState>) -> Response<Body> {
    let keys = state.billing.list_keys();
    let items: Vec<serde_json::Value> = keys
        .into_iter()
        .map(|(key, balance)| {
            let level = state.store.get_key_level(&key);
            serde_json::json!({
                "key": key,
                "balance": if balance == -1 { -1.0_f64 } else { balance as f64 / MICRO_PER_CREDIT as f64 },
                "level": level
            })
        })
        .collect();
    crate::admin::json_ok(&serde_json::json!({ "keys": items }))
}

pub(crate) async fn api_billing_overview(state: Arc<RouterState>) -> Response<Body> {
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
                "credits": credits as f64 / MICRO_PER_CREDIT as f64
            }));
        }

        crate::admin::json_ok(&serde_json::json!({
        "billing": {
            "total_keys": total_keys,
            "unlimited_keys": unlimited_count,
            "active_keys": total_keys - unlimited_count - zero_or_less,
            "exhausted_keys": zero_or_less,
            "total_balance": total_balance as f64 / MICRO_PER_CREDIT as f64,
        },
        "model_costs": model_costs,
        "usage": {
            "tokens": platform_tokens,
            "credits": platform_credits as f64 / MICRO_PER_CREDIT as f64,
        },
        "key_usage": key_usages,
        "upstreams": upstream_summary,
        "requests_total": stats.requests_total.load(std::sync::atomic::Ordering::Relaxed),
        "requests_inflight": stats.requests_inflight.load(std::sync::atomic::Ordering::Relaxed),
    }))
}

pub(crate) async fn api_billing_create_key(req: Request<Body>, state: Arc<RouterState>) -> Response<Body> {
    let payload: BillingCreateBody = match crate::admin::parse_json_body(req).await {
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
    if balance_credits > 0 && balance_credits > MAX_BALANCE_CREDITS {
        return RouterState::json_error(
            http::StatusCode::BAD_REQUEST,
            &format!(
                "balance exceeds maximum allowed ({} credits)",
                MAX_BALANCE_CREDITS
            ),
            "bad_request",
        );
    }
    let balance_micro = if balance_credits == -1 { -1 } else { balance_credits.saturating_mul(MICRO_PER_CREDIT) };
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
    crate::admin::json_ok(&serde_json::json!({
        "key": key,
        "balance": balance_credits,
        "created": true
    }))
}

async fn api_billing_get_balance(state: Arc<RouterState>, key: &str) -> Response<Body> {
    match state.billing.get_balance(key) {
        Some(balance) => {
            let credits = if balance == -1 { -1.0 } else { balance as f64 / MICRO_PER_CREDIT as f64 };
            crate::admin::json_ok(&serde_json::json!({
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
    crate::admin::json_ok(&serde_json::json!({"key": key, "level": level}))
}

async fn api_billing_set_level(
    req: Request<Body>,
    state: Arc<RouterState>,
    key: &str,
) -> Response<Body> {
    let body: serde_json::Value = match crate::admin::parse_json_body(req).await {
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
        Ok(Ok(())) => crate::admin::json_ok(&serde_json::json!({"ok": true, "key": key, "level": level})),
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
        Ok(true) => crate::admin::json_ok(&serde_json::json!({
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
    let body = match crate::admin::read_body_limit(req, 256 * 1024).await {
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

    match state.billing.adjust_balance(key, payload.delta.saturating_mul(MICRO_PER_CREDIT)) {
        Some(balance) => {
            let credits = if balance == -1 { -1.0 } else { balance as f64 / MICRO_PER_CREDIT as f64 };
            crate::admin::json_ok(&serde_json::json!({ "key": key, "delta": payload.delta, "balance": credits }))
        }
        None => RouterState::json_error(
            http::StatusCode::NOT_FOUND,
            "key not found",
            "key_not_found",
        ),
    }
}
