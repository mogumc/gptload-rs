use crate::billing::BillingStore;
use crate::config::{BanConfig, Config, UpstreamConfig};
use crate::storage::KeyStore;
use crate::util::now_ms;
use ahash::{AHashMap, AHashSet};
use arc_swap::ArcSwap;
use http::uri::{Authority, PathAndQuery, Scheme};
use hyper::client::HttpConnector;
use hyper::header::{
    HeaderName, CONNECTION, HOST, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, TE, TRAILER,
    TRANSFER_ENCODING, UPGRADE,
};
use hyper::{Body, Client, Method, Request, Response, Uri};
use hyper_rustls::HttpsConnectorBuilder;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

pub const HDR_AUTHORIZATION: HeaderName = hyper::header::AUTHORIZATION;

pub struct RouterState {
    pub request_timeout: Duration,
    pub ban: BanConfig,

    pub proxy_tokens: Option<Arc<AHashSet<String>>>,
    pub admin_tokens: Arc<AHashSet<String>>,
    pub usage_inject_upstreams: Option<Arc<AHashSet<String>>>,

    pub store: Arc<KeyStore>,
    pub billing: Arc<BillingStore>,
    pub model_routes_path: PathBuf,
    pub upstreams_path: PathBuf,

    pub snapshot: ArcSwap<RouterSnapshot>,
    pub sched_rr: Arc<AtomicUsize>,

    pub client: Client<hyper_rustls::HttpsConnector<HttpConnector>, Body>,

    pub stats: Arc<Stats>,
    pub requests: Arc<RequestsLog>,
}

pub struct RouterSnapshot {
    pub upstreams: Vec<Arc<Upstream>>,
    pub upstream_index: AHashMap<String, usize>,
    /// Weighted RR schedule. Contains indices into `upstreams`.
    pub schedule: Vec<usize>,
}

impl Clone for RouterState {
    fn clone(&self) -> Self {
        RouterState {
            request_timeout: self.request_timeout,
            ban: self.ban.clone(),
            proxy_tokens: self.proxy_tokens.clone(),
            admin_tokens: self.admin_tokens.clone(),
            usage_inject_upstreams: self.usage_inject_upstreams.clone(),
            store: self.store.clone(),
            billing: self.billing.clone(),
            model_routes_path: self.model_routes_path.clone(),
            upstreams_path: self.upstreams_path.clone(),
            snapshot: ArcSwap::from(self.snapshot.load_full()),
            sched_rr: Arc::new(AtomicUsize::new(self.sched_rr.load(std::sync::atomic::Ordering::Relaxed))),
            client: self.client.clone(),
            stats: self.stats.clone(),
            requests: self.requests.clone(),
        }
    }
}

pub struct Upstream {
    pub id: Arc<str>,

    pub base_url: Arc<str>,
    pub base_scheme: Scheme,
    pub base_authority: Authority,
    pub base_path: Arc<str>,

    pub weight: usize,

    pub keys: ArcSwap<Vec<Arc<KeyState>>>,
    pub key_rr: AtomicUsize,
    pub models: ArcSwap<AHashSet<String>>,

    // Upstream-level circuit breaker (network/5xx).
    pub cooldown_until_ms: AtomicU64,
    pub fail_streak: AtomicU32,

    pub stats: UpstreamStats,
}

pub struct KeyState {
    pub key: Arc<str>,
    pub auth_header: hyper::header::HeaderValue,
    pub cooldown_until_ms: AtomicU64,
    pub fail_streak: AtomicU32,
}

#[derive(Clone)]
pub struct Selected {
    pub upstream: Arc<Upstream>,
    pub key: Arc<KeyState>,
}

/// Global stats (cheap atomics only).
pub struct Stats {
    pub started_at_ms: u64,

    pub requests_total: AtomicU64,
    pub requests_inflight: AtomicU64,

    pub upstream_selected_total: AtomicU64,

    pub responses_2xx: AtomicU64,
    pub responses_3xx: AtomicU64,
    pub responses_4xx: AtomicU64,
    pub responses_5xx: AtomicU64,

    pub errors_timeout: AtomicU64,
    pub errors_network: AtomicU64,

    pub latency_ns_total: AtomicU64,
    pub latency_count: AtomicU64,
    pub latency_ns_max: AtomicU64,
}

pub struct UpstreamStats {
    pub selected_total: AtomicU64,
    pub responses_2xx: AtomicU64,
    pub responses_3xx: AtomicU64,
    pub responses_4xx: AtomicU64,
    pub responses_5xx: AtomicU64,
    pub errors_timeout: AtomicU64,
    pub errors_network: AtomicU64,
}

impl Default for UpstreamStats {
    fn default() -> Self {
        Self {
            selected_total: AtomicU64::new(0),
            responses_2xx: AtomicU64::new(0),
            responses_3xx: AtomicU64::new(0),
            responses_4xx: AtomicU64::new(0),
            responses_5xx: AtomicU64::new(0),
            errors_timeout: AtomicU64::new(0),
            errors_network: AtomicU64::new(0),
        }
    }
}

impl Stats {
    pub fn new() -> Self {
        let now = now_ms();
        Self {
            started_at_ms: now,
            requests_total: AtomicU64::new(0),
            requests_inflight: AtomicU64::new(0),
            upstream_selected_total: AtomicU64::new(0),
            responses_2xx: AtomicU64::new(0),
            responses_3xx: AtomicU64::new(0),
            responses_4xx: AtomicU64::new(0),
            responses_5xx: AtomicU64::new(0),
            errors_timeout: AtomicU64::new(0),
            errors_network: AtomicU64::new(0),
            latency_ns_total: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            latency_ns_max: AtomicU64::new(0),
        }
    }
}

#[derive(Clone, serde::Serialize)]
pub struct RequestLogEntry {
    pub ts_ms: u64,
    pub client_ip: String,
    pub method: String,
    pub path: String,
    pub model: Option<String>,
    pub upstream_id: Option<String>,
    pub status: u16,
    pub latency_ms: u64,
    pub req_bytes: usize,
    pub resp_bytes: usize,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

#[derive(Clone, serde::Serialize)]
pub struct MetricsBucket {
    pub ts_ms: u64,
    pub total: u64,
    pub success: u64,
    pub failure: u64,
    pub ignored: u64,
}

#[derive(Clone, Copy)]
pub enum MetricsWindow {
    Minute,
    Hour,
    Day,
}

impl MetricsWindow {
    pub fn from_str(s: &str) -> Self {
        match s {
            "hour" => MetricsWindow::Hour,
            "day" => MetricsWindow::Day,
            _ => MetricsWindow::Minute,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            MetricsWindow::Minute => "minute",
            MetricsWindow::Hour => "hour",
            MetricsWindow::Day => "day",
        }
    }
}

pub struct RequestsLog {
    entries: Mutex<VecDeque<RequestLogEntry>>,
    metrics: Mutex<RequestMetrics>,
    cap: usize,
    tx: Option<mpsc::Sender<RequestLogEntry>>,
}

impl RequestsLog {
    pub fn new(cap: usize, tx: Option<mpsc::Sender<RequestLogEntry>>) -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(cap)),
            metrics: Mutex::new(RequestMetrics::new()),
            cap,
            tx,
        }
    }

    pub fn record(&self, entry: RequestLogEntry) {
        if let Some(tx) = &self.tx {
            let _ = tx.try_send(entry.clone());
        }

        {
            let mut entries = self.entries.lock().unwrap();
            entries.push_back(entry.clone());
            while entries.len() > self.cap {
                entries.pop_front();
            }
        }

        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.update(&entry);
        }
    }

    pub fn recent(&self, limit: usize) -> Vec<RequestLogEntry> {
        let entries = self.entries.lock().unwrap();
        entries.iter().rev().take(limit).cloned().collect()
    }

    pub fn metrics_snapshot(&self, window: MetricsWindow) -> Vec<MetricsBucket> {
        let metrics = self.metrics.lock().unwrap();
        metrics.snapshot(window)
    }
}

pub struct RequestMetrics {
    minute: VecDeque<MetricsBucket>,
    hour: VecDeque<MetricsBucket>,
    day: VecDeque<MetricsBucket>,
}

impl RequestMetrics {
    pub fn new() -> Self {
        Self {
            minute: VecDeque::new(),
            hour: VecDeque::new(),
            day: VecDeque::new(),
        }
    }

    pub fn update(&mut self, entry: &RequestLogEntry) {
        let (success, failure, ignored) = classify_status(entry.status);
        let ts_ms = entry.ts_ms;

        update_bucket(&mut self.minute, ts_ms, 60_000, 60, success, failure, ignored);
        update_bucket(&mut self.hour, ts_ms, 3_600_000, 48, success, failure, ignored);
        update_bucket(&mut self.day, ts_ms, 86_400_000, 30, success, failure, ignored);
    }

    pub fn snapshot(&self, window: MetricsWindow) -> Vec<MetricsBucket> {
        match window {
            MetricsWindow::Minute => self.minute.iter().cloned().collect(),
            MetricsWindow::Hour => self.hour.iter().cloned().collect(),
            MetricsWindow::Day => self.day.iter().cloned().collect(),
        }
    }
}

impl RouterState {
    pub fn new(cfg: Config) -> anyhow::Result<Self> {
        let request_timeout = Duration::from_millis(cfg.request_timeout_ms);

        let proxy_tokens = cfg.proxy_tokens.and_then(|v| {
            let mut set = AHashSet::with_capacity(v.len().max(1));
            for t in v {
                let t = t.trim();
                if !t.is_empty() {
                    set.insert(t.to_string());
                }
            }
            if set.is_empty() {
                None
            } else {
                Some(Arc::new(set))
            }
        });

        let mut admin_set = AHashSet::with_capacity(cfg.admin_tokens.len().max(1));
        for t in cfg.admin_tokens {
            if !t.is_empty() {
                admin_set.insert(t);
            }
        }
        let admin_tokens = Arc::new(admin_set);

        let usage_inject_upstreams = cfg.usage_inject_upstreams.and_then(|v| {
            let mut set = AHashSet::with_capacity(v.len().max(1));
            for id in v {
                let id = id.trim();
                if !id.is_empty() {
                    set.insert(id.to_string());
                }
            }
            if set.is_empty() {
                None
            } else {
                Some(Arc::new(set))
            }
        });

        // Storage
        let data_dir: PathBuf = cfg.data_dir;
        let store = Arc::new(KeyStore::open(&data_dir)?);
        let billing = Arc::new(BillingStore::new(&store)?);
        let model_routes_path = data_dir.join("models_routes.json");
        let upstreams_path = data_dir.join("upstreams.json");
        let requests_log_path = data_dir.join("requests.jsonl");
        let log_tx = start_request_log_writer(requests_log_path);
        let requests = Arc::new(RequestsLog::new(5000, log_tx));

        let mut upstream_configs = cfg.upstreams;
        if let Ok(list) = load_upstreams_override(&upstreams_path) {
            upstream_configs = list;
        } else if upstreams_path.exists() {
            tracing::warn!(
                path = %upstreams_path.display(),
                "failed to load upstreams file"
            );
        }

        let snapshot = build_snapshot_from_configs(&upstream_configs, &store)?;

        // HTTPS (and HTTP) connector.
        let https = HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http1()
            .build();

        let client = Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(64)
            .build::<_, Body>(https);

        if let Ok(routes) = load_model_routes(&model_routes_path) {
            apply_loaded_routes(&routes, &snapshot.upstreams, &snapshot.upstream_index);
        } else if model_routes_path.exists() {
            tracing::warn!(
                path = %model_routes_path.display(),
                "failed to load model routes file"
            );
        }

        Ok(Self {
            request_timeout,
            ban: cfg.ban,
            proxy_tokens,
            admin_tokens,
            usage_inject_upstreams,
            store,
            billing,
            model_routes_path,
            upstreams_path,
            snapshot: ArcSwap::from(Arc::new(snapshot)),
            sched_rr: Arc::new(AtomicUsize::new(0)),
            client,
            stats: Arc::new(Stats::new()),
            requests,
        })
    }

    #[inline]
    pub fn authorize_proxy(&self, req: &Request<Body>) -> bool {
        let Some(tokens) = &self.proxy_tokens else {
            return true;
        };
        let Some(h) = req.headers().get("x-proxy-token") else {
            return false;
        };
        match h.to_str() {
            Ok(s) => tokens.contains(s),
            Err(_) => false,
        }
    }

    #[inline]
    pub fn authorize_admin_header(&self, req: &Request<Body>) -> bool {
        let Some(h) = req.headers().get("x-admin-token") else {
            return false;
        };
        match h.to_str() {
            Ok(s) => self.admin_tokens.contains(s),
            Err(_) => false,
        }
    }

    #[inline]
    pub fn authorize_admin_token_str(&self, token: &str) -> bool {
        self.admin_tokens.contains(token)
    }

    #[inline]
    pub fn should_inject_usage(&self, upstream_id: &str) -> bool {
        self.usage_inject_upstreams
            .as_ref()
            .map(|set| set.contains(upstream_id))
            .unwrap_or(false)
    }

    #[inline]
    pub fn record_request(&self, entry: RequestLogEntry) {
        self.requests.record(entry);
    }

    pub fn recent_requests(&self, limit: usize) -> Vec<RequestLogEntry> {
        self.requests.recent(limit)
    }

    pub fn metrics_snapshot(&self, window: MetricsWindow) -> Vec<MetricsBucket> {
        self.requests.metrics_snapshot(window)
    }

    pub fn upstream_by_id(&self, id: &str) -> Option<(usize, Arc<Upstream>)> {
        let snap = self.snapshot.load_full();
        let idx = *snap.upstream_index.get(id)?;
        Some((idx, snap.upstreams[idx].clone()))
    }

    /// Select an upstream + key. Returns None if **all** keys are in cooldown or no keys loaded.
    pub fn select(&self, now_ms: u64) -> Option<Selected> {
        let snap = self.snapshot.load_full();
        let sched_len = snap.schedule.len();
        if sched_len == 0 {
            return None;
        }

        // Try up to schedule length to find an upstream with any available key.
        for _ in 0..sched_len {
            let rr = self.sched_rr.as_ref().fetch_add(1, Ordering::Relaxed);
            let u_idx = snap.schedule[rr % sched_len];

            let u = &snap.upstreams[u_idx];

            let u_until = u.cooldown_until_ms.load(Ordering::Relaxed);
            if u_until > now_ms {
                continue;
            }
            if let Some(k) = u.select_key(now_ms) {
                self.stats.upstream_selected_total.fetch_add(1, Ordering::Relaxed);
                u.stats.selected_total.fetch_add(1, Ordering::Relaxed);
                return Some(Selected {
                    upstream: u.clone(),
                    key: k,
                });
            }
        }

        None
    }

    /// Select an upstream + key that supports the given model.
    pub fn select_for_model(&self, model: &str, now_ms: u64) -> Option<Selected> {
        let snap = self.snapshot.load_full();
        let sched_len = snap.schedule.len();
        if sched_len == 0 {
            return None;
        }

        for _ in 0..sched_len {
            let rr = self.sched_rr.as_ref().fetch_add(1, Ordering::Relaxed);
            let u_idx = snap.schedule[rr % sched_len];
            let u = &snap.upstreams[u_idx];

            if !u.models.load().contains(model) {
                continue;
            }

            let u_until = u.cooldown_until_ms.load(Ordering::Relaxed);
            if u_until > now_ms {
                continue;
            }
            if let Some(k) = u.select_key(now_ms) {
                self.stats.upstream_selected_total.fetch_add(1, Ordering::Relaxed);
                u.stats.selected_total.fetch_add(1, Ordering::Relaxed);
                return Some(Selected {
                    upstream: u.clone(),
                    key: k,
                });
            }
        }

        None
    }

    pub fn model_exists(&self, model: &str) -> bool {
        let snap = self.snapshot.load_full();
        snap.upstreams.iter().any(|u| u.models.load().contains(model))
    }

    pub fn any_models_loaded(&self) -> bool {
        let snap = self.snapshot.load_full();
        snap.upstreams.iter().any(|u| !u.models.load().is_empty())
    }

    pub async fn refresh_missing_models_routes(&self) {
        let routes = load_model_routes(&self.model_routes_path).ok();
        let mut refreshed = 0usize;
        let snap = self.snapshot.load_full();
        for u in snap.upstreams.iter() {
            if routes
                .as_ref()
                .map(|r| routes_has_upstream(r, u.id.as_ref()))
                .unwrap_or(false)
            {
                continue;
            }
            if let Err(e) = self.refresh_models_for_upstream(u.clone()).await {
                tracing::warn!(upstream = %u.id, error = %e, "model refresh failed");
            } else {
                refreshed += 1;
            }
        }
        if refreshed > 0 {
            if let Err(e) = self.persist_model_routes() {
                tracing::warn!(error = %e, "model routes persist failed");
            }
        }
    }

    pub async fn refresh_missing_models_for_upstream(&self, upstream_id: &str) {
        let routes = load_model_routes(&self.model_routes_path).ok();
        if routes
            .as_ref()
            .map(|r| routes_has_upstream(r, upstream_id))
            .unwrap_or(false)
        {
            return;
        }
        if let Err(e) = self.refresh_models_by_id(upstream_id).await {
            tracing::warn!(upstream = %upstream_id, error = %e, "model refresh failed");
        }
    }

    pub async fn refresh_models_by_id(&self, upstream_id: &str) -> anyhow::Result<usize> {
        let Some((_idx, upstream)) = self.upstream_by_id(upstream_id) else {
            anyhow::bail!("unknown upstream id");
        };
        let count = self.refresh_models_for_upstream(upstream).await?;
        if let Err(e) = self.persist_model_routes() {
            tracing::warn!(error = %e, "model routes persist failed");
        }
        Ok(count)
    }

    pub async fn fetch_models_preview(&self, upstream_id: &str) -> anyhow::Result<Vec<String>> {
        let Some((_idx, upstream)) = self.upstream_by_id(upstream_id) else {
            anyhow::bail!("unknown upstream id");
        };
        let models = self.fetch_models_for_upstream(upstream).await?;
        let mut list: Vec<String> = models.into_iter().collect();
        list.sort();
        Ok(list)
    }

    async fn refresh_models_for_upstream(&self, upstream: Arc<Upstream>) -> anyhow::Result<usize> {
        let models = self.fetch_models_for_upstream(upstream.clone()).await?;
        let count = models.len();
        upstream.models.store(Arc::new(models));
        Ok(count)
    }
    #[inline]
    pub fn on_upstream_status(&self, sel: &Selected, status: http::StatusCode, now_ms: u64) {
        let u = &sel.upstream;

        // HTTP response means upstream is reachable; clear upstream cooldown and streak.
        u.fail_streak.store(0, Ordering::Relaxed);
        u.cooldown_until_ms.store(0, Ordering::Relaxed);

        // Upstream per-status stats
        inc_status(&u.stats, status);

        // Global per-status stats
        self.inc_global_status(status);

        if status == http::StatusCode::TOO_MANY_REQUESTS {
            // Key-level rate limit.
            self.ban_key(&sel.key, self.ban.rate_limit_ms, now_ms);
        } else if status == http::StatusCode::UNAUTHORIZED || status == http::StatusCode::FORBIDDEN {
            // Key invalid / forbidden.
            self.ban_key(&sel.key, self.ban.auth_error_ms, now_ms);
        } else if status.is_server_error() {
            // Upstream 5xx: prefer upstream cooldown, not key cooldown.
            self.ban_upstream(u, self.ban.server_error_ms, now_ms);
        } else {
            // Success or other 4xx: reset key streak.
            sel.key.fail_streak.store(0, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn on_timeout(&self, sel: &Selected, now_ms: u64) {
        let u = &sel.upstream;
        self.stats.errors_timeout.fetch_add(1, Ordering::Relaxed);
        u.stats.errors_timeout.fetch_add(1, Ordering::Relaxed);
        self.ban_upstream(u, self.ban.network_error_ms, now_ms);
    }

    #[inline]
    pub fn on_network_error(&self, sel: &Selected, now_ms: u64) {
        let u = &sel.upstream;
        self.stats.errors_network.fetch_add(1, Ordering::Relaxed);
        u.stats.errors_network.fetch_add(1, Ordering::Relaxed);
        self.ban_upstream(u, self.ban.network_error_ms, now_ms);
    }

    #[inline]
    fn inc_global_status(&self, status: http::StatusCode) {
        if status.is_success() {
            self.stats.responses_2xx.fetch_add(1, Ordering::Relaxed);
        } else if status.is_redirection() {
            self.stats.responses_3xx.fetch_add(1, Ordering::Relaxed);
        } else if status.is_client_error() {
            self.stats.responses_4xx.fetch_add(1, Ordering::Relaxed);
        } else if status.is_server_error() {
            self.stats.responses_5xx.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn ban_key(&self, key: &KeyState, base_ms: u64, now_ms: u64) {
        let streak = key.fail_streak.fetch_add(1, Ordering::Relaxed) + 1;
        let max_pow = self.ban.max_backoff_pow.min(30);
        let pow = (streak - 1).min(max_pow);
        let mult = 1u64 << pow;

        let ban_ms = base_ms.saturating_mul(mult);
        let until = now_ms.saturating_add(ban_ms);

        key.cooldown_until_ms.store(until, Ordering::Relaxed);
    }

    fn ban_upstream(&self, u: &Upstream, base_ms: u64, now_ms: u64) {
        let streak = u.fail_streak.fetch_add(1, Ordering::Relaxed) + 1;
        let max_pow = self.ban.max_backoff_pow.min(30);
        let pow = (streak - 1).min(max_pow);
        let mult = 1u64 << pow;

        let ban_ms = base_ms.saturating_mul(mult);
        let until = now_ms.saturating_add(ban_ms);

        u.cooldown_until_ms.store(until, Ordering::Relaxed);
    }

    pub fn record_latency(&self, latency_ns: u64) {
        self.stats.latency_ns_total.fetch_add(latency_ns, Ordering::Relaxed);
        self.stats.latency_count.fetch_add(1, Ordering::Relaxed);

        // Update max with CAS loop.
        let mut cur = self.stats.latency_ns_max.load(Ordering::Relaxed);
        while latency_ns > cur {
            match self.stats.latency_ns_max.compare_exchange_weak(
                cur,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    /// Helper to produce standardized JSON error responses.
    pub fn json_error(status: http::StatusCode, message: &str, code: &str) -> Response<Body> {
        let body = format!(
            r#"{{"error":{{"message":"{}","type":"proxy_error","param":null,"code":"{}"}}}}"#,
            escape_json(message),
            escape_json(code)
        );
        Response::builder()
            .status(status)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap_or_else(|_| Response::new(Body::from("proxy_error")))
    }
}

impl Upstream {
    fn select_key(&self, now_ms: u64) -> Option<Arc<KeyState>> {
        let keys_arc = self.keys.load_full();
        let keys = keys_arc.as_ref();
        let n = keys.len();
        if n == 0 {
            return None;
        }

        let start = self.key_rr.fetch_add(1, Ordering::Relaxed);
        for i in 0..n {
            let idx = (start + i) % n;
            let k = &keys[idx];
            let until = k.cooldown_until_ms.load(Ordering::Relaxed);
            if until <= now_ms {
                return Some(k.clone());
            }
        }
        None
    }

    /// Builds an absolute URI to upstream by combining base scheme+authority and request path/query.
    pub fn build_uri(&self, path_and_query: &http::uri::PathAndQuery) -> anyhow::Result<Uri> {
        if self.base_path.is_empty() || self.base_path.as_ref() == "/" {
            let mut parts = http::uri::Parts::default();
            parts.scheme = Some(self.base_scheme.clone());
            parts.authority = Some(self.base_authority.clone());
            parts.path_and_query = Some(path_and_query.clone());
            Ok(Uri::from_parts(parts)?)
        } else {
            let pq = path_and_query.as_str();
            let (path, query) = match pq.split_once('?') {
                Some((p, q)) => (p, Some(q)),
                None => (pq, None),
            };

            let mut joined = String::with_capacity(self.base_path.len() + path.len() + 8);
            joined.push_str(self.base_path.as_ref());
            if !joined.ends_with('/') {
                joined.push('/');
            }
            joined.push_str(path.trim_start_matches('/'));
            if let Some(q) = query {
                joined.push('?');
                joined.push_str(q);
            }

            let joined_pq: http::uri::PathAndQuery = joined.parse()?;
            let mut parts = http::uri::Parts::default();
            parts.scheme = Some(self.base_scheme.clone());
            parts.authority = Some(self.base_authority.clone());
            parts.path_and_query = Some(joined_pq);
            Ok(Uri::from_parts(parts)?)
        }
    }

    pub fn keys_len(&self) -> usize {
        self.keys.load().len()
    }
}

/// Remove hop-by-hop headers that should not be forwarded.
#[inline]
pub fn sanitize_hop_headers(headers: &mut hyper::HeaderMap) {
    headers.remove(CONNECTION);
    headers.remove(HOST);
    headers.remove("proxy-connection");
    headers.remove(PROXY_AUTHENTICATE);
    headers.remove(PROXY_AUTHORIZATION);
    headers.remove(TE);
    headers.remove(TRAILER);
    headers.remove(TRANSFER_ENCODING);
    headers.remove(UPGRADE);
    headers.remove("x-proxy-token");
    headers.remove("x-admin-token");
}

#[inline]
fn inc_status(stats: &UpstreamStats, status: http::StatusCode) {
    if status.is_success() {
        stats.responses_2xx.fetch_add(1, Ordering::Relaxed);
    } else if status.is_redirection() {
        stats.responses_3xx.fetch_add(1, Ordering::Relaxed);
    } else if status.is_client_error() {
        stats.responses_4xx.fetch_add(1, Ordering::Relaxed);
    } else if status.is_server_error() {
        stats.responses_5xx.fetch_add(1, Ordering::Relaxed);
    }
}

fn parse_upstream(u: UpstreamConfig, weight: usize) -> anyhow::Result<Arc<Upstream>> {
    let name_for_err = u.id.clone();

    let base: Uri = u.base_url.parse()?;

    let scheme = base
        .scheme()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("upstream {}: base_url missing scheme", name_for_err))?;
    let authority = base.authority().cloned().ok_or_else(|| {
        anyhow::anyhow!("upstream {}: base_url missing authority", name_for_err)
    })?;

    let base_path = base.path().trim_end_matches('/').to_string();
    let base_path = if base_path == "/" { String::new() } else { base_path };

    let upstream = Upstream {
        id: Arc::<str>::from(u.id),
        base_url: Arc::<str>::from(u.base_url.clone()),
        base_scheme: scheme,
        base_authority: authority,
        base_path: Arc::<str>::from(base_path),
        weight,
        keys: ArcSwap::from_pointee(Vec::new()),
        key_rr: AtomicUsize::new(0),
        models: ArcSwap::from_pointee(AHashSet::new()),
        cooldown_until_ms: AtomicU64::new(0),
        fail_streak: AtomicU32::new(0),
        stats: UpstreamStats::default(),
    };

    Ok(Arc::new(upstream))
}

pub fn build_key_states(keys: Vec<String>) -> anyhow::Result<Arc<Vec<Arc<KeyState>>>> {
    let mut out: Vec<Arc<KeyState>> = Vec::with_capacity(keys.len());
    for k in keys {
        let k = k.trim();
        if k.is_empty() {
            continue;
        }
        let key_arc: Arc<str> = Arc::<str>::from(k.to_string());
        let auth_header =
            hyper::header::HeaderValue::from_str(&format!("Bearer {}", key_arc)).map_err(|_| {
                anyhow::anyhow!("invalid key (cannot be used in HTTP header)")
            })?;
        out.push(Arc::new(KeyState {
            key: key_arc,
            auth_header,
            cooldown_until_ms: AtomicU64::new(0),
            fail_streak: AtomicU32::new(0),
        }));
    }
    Ok(Arc::new(out))
}

fn parse_models_response(body: &[u8]) -> anyhow::Result<AHashSet<String>> {
    let v: serde_json::Value = serde_json::from_slice(body)?;
    let data = v
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| anyhow::anyhow!("missing data array in models response"))?;

    let mut out: AHashSet<String> = AHashSet::with_capacity(data.len().max(1));
    for item in data {
        if let Some(id) = item.get("id").and_then(|s| s.as_str()) {
            out.insert(id.to_string());
        }
    }
    Ok(out)
}

fn load_upstreams_override(path: &Path) -> anyhow::Result<Vec<UpstreamConfig>> {
    let s = std::fs::read_to_string(path)?;
    let list: Vec<UpstreamConfig> = serde_json::from_str(&s)?;
    Ok(list)
}

fn write_upstreams_override(path: &Path, upstreams: &[UpstreamConfig]) -> anyhow::Result<()> {
    let s = serde_json::to_string_pretty(upstreams)?;
    std::fs::write(path, s)?;
    Ok(())
}

fn write_model_routes(path: &Path, routes: &ModelRoutesFile) -> anyhow::Result<()> {
    let s = serde_json::to_string_pretty(routes)?;
    std::fs::write(path, s)?;
    Ok(())
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct ModelRoutesFile {
    pub updated_at_ms: u64,
    pub models: BTreeMap<String, Vec<String>>,
    pub upstreams: BTreeMap<String, Vec<String>>,
}

fn load_model_routes(path: &Path) -> anyhow::Result<ModelRoutesFile> {
    let s = std::fs::read_to_string(path)?;
    let routes: ModelRoutesFile = serde_json::from_str(&s)?;
    Ok(routes)
}

fn apply_loaded_routes(
    routes: &ModelRoutesFile,
    upstreams: &[Arc<Upstream>],
    upstream_index: &AHashMap<String, usize>,
) {
    apply_routes_to_upstreams(routes, upstreams, upstream_index);
}

fn routes_has_upstream(routes: &ModelRoutesFile, upstream_id: &str) -> bool {
    routes.upstreams.contains_key(upstream_id)
}

impl RouterState {
    pub fn get_model_routes(&self) -> ModelRoutesFile {
        match load_model_routes(&self.model_routes_path) {
            Ok(routes) => routes,
            Err(_) => self.build_model_routes(),
        }
    }

    pub fn save_model_routes(
        &self,
        upstreams: BTreeMap<String, Vec<String>>,
    ) -> anyhow::Result<ModelRoutesFile> {
        let snap = self.snapshot.load_full();
        for id in upstreams.keys() {
            if !snap.upstream_index.contains_key(id) {
                anyhow::bail!("unknown upstream id: {}", id);
            }
        }

        let mut upstreams_clean: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (id, models) in upstreams {
            let mut set = AHashSet::with_capacity(models.len().max(1));
            for m in models {
                let m = m.trim();
                if !m.is_empty() {
                    set.insert(m.to_string());
                }
            }
            let mut list: Vec<String> = set.into_iter().collect();
            list.sort();
            upstreams_clean.insert(id, list);
        }

        let mut models: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (id, list) in &upstreams_clean {
            for model in list {
                models.entry(model.clone()).or_default().push(id.clone());
            }
        }
        for ids in models.values_mut() {
            ids.sort();
            ids.dedup();
        }

        let routes = ModelRoutesFile {
            updated_at_ms: now_ms(),
            models,
            upstreams: upstreams_clean,
        };

        write_model_routes(&self.model_routes_path, &routes)?;
        apply_routes_to_upstreams(&routes, &snap.upstreams, &snap.upstream_index);
        Ok(routes)
    }

    pub fn add_upstream(&self, cfg: UpstreamConfig) -> anyhow::Result<()> {
        let mut list = self.current_upstream_configs();
        if list.iter().any(|u| u.id == cfg.id) {
            anyhow::bail!("upstream id already exists");
        }
        list.push(cfg);
        self.replace_upstreams(list)?;
        Ok(())
    }

    pub fn update_upstream(&self, id: &str, base_url: String, weight: Option<usize>) -> anyhow::Result<()> {
        let mut list = self.current_upstream_configs();
        let mut found = false;
        for u in list.iter_mut() {
            if u.id == id {
                u.base_url = base_url.clone();
                u.weight = weight;
                found = true;
                break;
            }
        }
        if !found {
            anyhow::bail!("unknown upstream id");
        }
        self.replace_upstreams(list)?;
        Ok(())
    }

    pub fn delete_upstream(&self, id: &str, delete_keys: bool) -> anyhow::Result<()> {
        let mut list = self.current_upstream_configs();
        let before = list.len();
        list.retain(|u| u.id != id);
        if list.len() == before {
            anyhow::bail!("unknown upstream id");
        }
        self.replace_upstreams(list)?;
        if delete_keys {
            let empty: Vec<String> = Vec::new();
            self.store.replace_keys(id, &empty)?;
        }
        Ok(())
    }

    fn build_model_routes(&self) -> ModelRoutesFile {
        let mut models: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut upstreams: BTreeMap<String, Vec<String>> = BTreeMap::new();

        let snap = self.snapshot.load_full();
        for u in snap.upstreams.iter() {
            let set = u.models.load_full();
            if set.is_empty() {
                continue;
            }
            let mut model_list: Vec<String> = set.iter().cloned().collect();
            model_list.sort();
            model_list.dedup();

            upstreams.insert(u.id.to_string(), model_list.clone());
            for model in model_list {
                models.entry(model).or_default().push(u.id.to_string());
            }
        }

        for ids in models.values_mut() {
            ids.sort();
            ids.dedup();
        }

        ModelRoutesFile {
            updated_at_ms: now_ms(),
            models,
            upstreams,
        }
    }

    fn persist_model_routes(&self) -> anyhow::Result<()> {
        if !self.any_models_loaded() {
            return Ok(());
        }
        let routes = self.build_model_routes();
        write_model_routes(&self.model_routes_path, &routes)?;
        Ok(())
    }

    fn replace_upstreams(&self, configs: Vec<UpstreamConfig>) -> anyhow::Result<()> {
        let snapshot = build_snapshot_from_configs(&configs, &self.store)?;
        if let Ok(routes) = load_model_routes(&self.model_routes_path) {
            apply_routes_to_upstreams(&routes, &snapshot.upstreams, &snapshot.upstream_index);
        }
        self.snapshot.store(Arc::new(snapshot));
        write_upstreams_override(&self.upstreams_path, &configs)?;
        self.cleanup_model_routes()?;
        Ok(())
    }

    fn current_upstream_configs(&self) -> Vec<UpstreamConfig> {
        let snap = self.snapshot.load_full();
        snap.upstreams
            .iter()
            .map(|u| UpstreamConfig {
                id: u.id.to_string(),
                base_url: u.base_url.to_string(),
                weight: Some(u.weight),
            })
            .collect()
    }

    fn cleanup_model_routes(&self) -> anyhow::Result<()> {
        let Ok(mut routes) = load_model_routes(&self.model_routes_path) else {
            return Ok(());
        };
        let snap = self.snapshot.load_full();
        let mut changed = false;
        routes.upstreams.retain(|id, _| {
            let keep = snap.upstream_index.contains_key(id);
            if !keep {
                changed = true;
            }
            keep
        });
        if !changed {
            return Ok(());
        }
        let mut models: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (id, list) in &routes.upstreams {
            for model in list {
                models.entry(model.clone()).or_default().push(id.clone());
            }
        }
        for ids in models.values_mut() {
            ids.sort();
            ids.dedup();
        }
        routes.models = models;
        routes.updated_at_ms = now_ms();
        write_model_routes(&self.model_routes_path, &routes)?;
        apply_routes_to_upstreams(&routes, &snap.upstreams, &snap.upstream_index);
        Ok(())
    }

    async fn fetch_models_for_upstream(
        &self,
        upstream: Arc<Upstream>,
    ) -> anyhow::Result<AHashSet<String>> {
        let keys = upstream.keys.load_full();
        let now = now_ms();
        let key = keys
            .iter()
            .find(|k| k.cooldown_until_ms.load(Ordering::Relaxed) <= now)
            .cloned()
            .or_else(|| keys.first().cloned())
            .ok_or_else(|| anyhow::anyhow!("no keys loaded"))?;

        let uri = upstream.build_uri(&PathAndQuery::from_static("/v1/models"))?;
        let mut req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Body::empty())?;
        req.headers_mut().insert(HDR_AUTHORIZATION, key.auth_header.clone());

        let resp = match tokio::time::timeout(self.request_timeout, self.client.request(req)).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => anyhow::bail!("upstream request timeout"),
        };
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("upstream returned {}", status);
        }

        let body = hyper::body::to_bytes(resp.into_body()).await?;
        parse_models_response(&body)
    }
}

fn build_snapshot_from_configs(
    configs: &[UpstreamConfig],
    store: &KeyStore,
) -> anyhow::Result<RouterSnapshot> {
    const MAX_WEIGHT: usize = 100;
    if configs.is_empty() {
        anyhow::bail!("no upstreams configured");
    }

    let mut upstreams: Vec<Arc<Upstream>> = Vec::new();
    let mut upstream_index: AHashMap<String, usize> = AHashMap::new();
    let mut schedule: Vec<usize> = Vec::new();

    for u_cfg in configs.iter().cloned() {
        if upstream_index.contains_key(&u_cfg.id) {
            anyhow::bail!("duplicate upstream id: {}", u_cfg.id);
        }
        let weight = u_cfg.weight.unwrap_or(1).clamp(1, MAX_WEIGHT);
        let u = parse_upstream(u_cfg, weight)?;
        let idx = upstreams.len();
        upstream_index.insert(u.id.to_string(), idx);

        let keys = store.load_all_keys(&u.id)?;
        let key_states = build_key_states(keys)?;
        u.keys.store(key_states);

        for _ in 0..weight {
            schedule.push(idx);
        }
        upstreams.push(u);
    }

    Ok(RouterSnapshot {
        upstreams,
        upstream_index,
        schedule,
    })
}

fn apply_routes_to_upstreams(
    routes: &ModelRoutesFile,
    upstreams: &[Arc<Upstream>],
    upstream_index: &AHashMap<String, usize>,
) {
    let mut per_upstream: Vec<AHashSet<String>> = vec![AHashSet::new(); upstreams.len()];
    for (id, models) in &routes.upstreams {
        if let Some(idx) = upstream_index.get(id) {
            for model in models {
                let m = model.trim();
                if !m.is_empty() {
                    per_upstream[*idx].insert(m.to_string());
                }
            }
        }
    }
    for (idx, set) in per_upstream.into_iter().enumerate() {
        upstreams[idx].models.store(Arc::new(set));
    }
}

pub fn validate_keys(keys: &[String]) -> anyhow::Result<()> {
    let mut valid_count = 0usize;
    for k in keys {
        let k = k.trim();
        if k.is_empty() {
            continue;
        }
        valid_count += 1;
        hyper::header::HeaderValue::from_str(&format!("Bearer {}", k))
            .map_err(|_| anyhow::anyhow!("invalid key (cannot be used in HTTP header)"))?;
    }
    if valid_count == 0 {
        anyhow::bail!("no keys provided");
    }
    Ok(())
}

fn classify_status(status: u16) -> (u64, u64, u64) {
    if (200..300).contains(&status) {
        (1, 0, 0)
    } else if status == 404 {
        (0, 0, 1)
    } else {
        (0, 1, 0)
    }
}

fn update_bucket(
    buckets: &mut VecDeque<MetricsBucket>,
    ts_ms: u64,
    step_ms: u64,
    cap: usize,
    success: u64,
    failure: u64,
    ignored: u64,
) {
    let bucket_start = ts_ms - (ts_ms % step_ms);

    if buckets.is_empty() {
        buckets.push_back(MetricsBucket {
            ts_ms: bucket_start,
            total: 0,
            success: 0,
            failure: 0,
            ignored: 0,
        });
    } else {
        let last_start = buckets.back().unwrap().ts_ms;
        if bucket_start > last_start {
            let mut next_start = last_start.saturating_add(step_ms);
            while next_start <= bucket_start {
                buckets.push_back(MetricsBucket {
                    ts_ms: next_start,
                    total: 0,
                    success: 0,
                    failure: 0,
                    ignored: 0,
                });
                next_start = next_start.saturating_add(step_ms);
            }
        }
    }

    if let Some(last) = buckets.back_mut() {
        last.total += 1;
        last.success += success;
        last.failure += failure;
        last.ignored += ignored;
    }

    while buckets.len() > cap {
        buckets.pop_front();
    }
}

fn start_request_log_writer(path: PathBuf) -> Option<mpsc::Sender<RequestLogEntry>> {
    let (tx, mut rx) = mpsc::channel::<RequestLogEntry>(2048);

    tokio::spawn(async move {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await;
        let mut file = match file {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "request log open failed");
                return;
            }
        };

        let mut pending = 0usize;
        let mut tick = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                entry = rx.recv() => {
                    let Some(entry) = entry else { break; };
                    if let Ok(line) = serde_json::to_string(&entry) {
                        if file.write_all(line.as_bytes()).await.is_ok() {
                            let _ = file.write_all(b"\n").await;
                            pending += 1;
                        }
                    }
                    if pending >= 256 {
                        let _ = file.flush().await;
                        pending = 0;
                    }
                }
                _ = tick.tick() => {
                    if pending > 0 {
                        let _ = file.flush().await;
                        pending = 0;
                    }
                }
            }
        }

        let _ = file.flush().await;
    });

    Some(tx)
}

#[inline]
fn escape_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out
}
