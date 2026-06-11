use crate::billing::BillingStore;
use crate::config::{Config, KeyConfig, ServerConfig, UpstreamConfig, UpstreamFormat};
use crate::storage::KeyStore;
use crate::upstream_client::UpstreamClient;
use crate::util::now_ms;
use ahash::{AHashMap, AHashSet};
use arc_swap::ArcSwap;
use http::uri::{Authority, Scheme};
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
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::{broadcast, mpsc, Notify, Semaphore, SemaphorePermit};

pub const HDR_AUTHORIZATION: HeaderName = hyper::header::AUTHORIZATION;

pub struct RouterState {
    pub runtime: ArcSwap<RuntimeConfig>,

    pub request_timeout: Duration,
    pub max_retries: usize,
    pub retry_status_codes: Arc<AHashSet<u16>>,
    pub key_config: KeyConfig,

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
    pub admin_write_lock: Arc<Mutex<()>>,
    pub shutting_down: AtomicBool,
    pub queue_notify: Notify,
    pub queue_slots: Semaphore,
    pub config_path: Option<PathBuf>,
    pub listen_addr: String,
    pub worker_threads: Option<usize>,
    pub data_dir: PathBuf,
}

#[derive(Clone)]
pub struct RuntimeConfig {
    pub request_timeout: Duration,
    pub max_retries: usize,
    pub retry_status_codes: Arc<AHashSet<u16>>,
    pub key_config: KeyConfig,
    pub proxy_tokens: Option<Arc<AHashSet<String>>>,
    pub admin_tokens: Arc<AHashSet<String>>,
    pub usage_inject_upstreams: Option<Arc<AHashSet<String>>>,
    pub model_costs: ahash::AHashMap<String, crate::config::ModelCost>,
    pub server: ServerConfig,
    pub preview: serde_json::Value,
}

pub struct QueueGuard<'a> {
    state: &'a RouterState,
    _permit: SemaphorePermit<'a>,
}

impl Drop for QueueGuard<'_> {
    fn drop(&mut self) {
        self.state.stats.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }
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
            runtime: ArcSwap::from(self.runtime.load_full()),
            request_timeout: self.request_timeout,
            max_retries: self.max_retries,
            retry_status_codes: self.retry_status_codes.clone(),
            key_config: self.key_config.clone(),
            proxy_tokens: self.proxy_tokens.clone(),
            admin_tokens: self.admin_tokens.clone(),
            usage_inject_upstreams: self.usage_inject_upstreams.clone(),
            store: self.store.clone(),
            billing: self.billing.clone(),
            model_routes_path: self.model_routes_path.clone(),
            upstreams_path: self.upstreams_path.clone(),
            snapshot: ArcSwap::from(self.snapshot.load_full()),
            sched_rr: Arc::new(AtomicUsize::new(
                self.sched_rr.load(std::sync::atomic::Ordering::Relaxed),
            )),
            client: self.client.clone(),
            stats: self.stats.clone(),
            requests: self.requests.clone(),
            admin_write_lock: self.admin_write_lock.clone(),
            shutting_down: AtomicBool::new(self.shutting_down.load(Ordering::Relaxed)),
            queue_notify: Notify::new(),
            queue_slots: Semaphore::new(self.server_config().queue_max_depth),
            config_path: self.config_path.clone(),
            listen_addr: self.listen_addr.clone(),
            worker_threads: self.worker_threads,
            data_dir: self.data_dir.clone(),
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
    pub max_concurrent_per_key: u32,
    pub min_key_level: i32,
    pub format: UpstreamFormat,
    pub proxy: Option<String>,
    pub client: UpstreamClient,

    pub keys: ArcSwap<Vec<Arc<KeyState>>>,
    pub active_keys: ArcSwap<Vec<Arc<KeyState>>>,
    pub keys_update_lock: Mutex<()>,
    pub key_rr: AtomicUsize,
    pub models: ArcSwap<AHashSet<String>>,
    /// Incoming model name → upstream model name.
    pub model_map: ahash::AHashMap<String, String>,
    /// Reverse: upstream model name → incoming model name (for /v1/models listing).
    pub model_rmap: ahash::AHashMap<String, String>,

    pub stats: UpstreamStats,
}

/// Key status: active (in the rotation pool) or invalid (blacklisted).
pub const KEY_STATUS_ACTIVE: u8 = 0;
pub const KEY_STATUS_INVALID: u8 = 1;

pub struct KeyState {
    pub key: Arc<str>,
    pub auth_header: hyper::header::HeaderValue,
    pub failure_count: AtomicU32,
    pub status: AtomicU8,
    pub active_requests: AtomicU32,
    /// Permission level: higher = more access. -1 = admin (no restriction).
    pub level: AtomicI32,
    /// Unix ms timestamp when the 429 cooldown expires. 0 = not in cooldown.
    pub cooldown_until_ms: AtomicU64,
    pub latencies_ms: Mutex<VecDeque<u64>>,
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

    pub prompt_tokens_total: AtomicU64,
    pub completion_tokens_total: AtomicU64,
    pub tokens_total: AtomicU64,

    pub queue_depth: AtomicU64,
    pub queue_timeout_total: AtomicU64,

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
            prompt_tokens_total: AtomicU64::new(0),
            completion_tokens_total: AtomicU64::new(0),
            tokens_total: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            queue_timeout_total: AtomicU64::new(0),
            latency_ns_total: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            latency_ns_max: AtomicU64::new(0),
        }
    }
}

#[derive(Clone, serde::Serialize)]
pub struct RequestLogEntry {
    pub id: u64,
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
    pub request_headers: Option<BTreeMap<String, String>>,
    pub request_body: Option<String>,
    pub timing: RequestTiming,
}

#[derive(Clone, Default, serde::Serialize)]
pub struct RequestTiming {
    pub queue_ms: u64,
    pub upstream_ms: u64,
    pub total_ms: u64,
    pub attempts: u32,
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
    broadcast_tx: broadcast::Sender<RequestLogEntry>,
}

impl RequestsLog {
    pub fn new(cap: usize, tx: Option<mpsc::Sender<RequestLogEntry>>) -> Self {
        let (broadcast_tx, _rx) = broadcast::channel(1024);
        Self {
            entries: Mutex::new(VecDeque::with_capacity(cap)),
            metrics: Mutex::new(RequestMetrics::new()),
            cap,
            tx,
            broadcast_tx,
        }
    }

    pub fn record(&self, entry: RequestLogEntry) {
        let _ = self.broadcast_tx.send(entry.clone());
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

    pub fn subscribe(&self) -> broadcast::Receiver<RequestLogEntry> {
        self.broadcast_tx.subscribe()
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

        update_bucket(
            &mut self.minute,
            ts_ms,
            60_000,
            60,
            success,
            failure,
            ignored,
        );
        update_bucket(
            &mut self.hour,
            ts_ms,
            3_600_000,
            48,
            success,
            failure,
            ignored,
        );
        update_bucket(
            &mut self.day,
            ts_ms,
            86_400_000,
            30,
            success,
            failure,
            ignored,
        );
    }

    pub fn snapshot(&self, window: MetricsWindow) -> Vec<MetricsBucket> {
        match window {
            MetricsWindow::Minute => self.minute.iter().cloned().collect(),
            MetricsWindow::Hour => self.hour.iter().cloned().collect(),
            MetricsWindow::Day => self.day.iter().cloned().collect(),
        }
    }
}

impl RuntimeConfig {
    pub fn from_config(cfg: &Config) -> Self {
        let retry_status_codes = cfg
            .retry_status_codes
            .clone()
            .unwrap_or_else(|| vec![429])
            .into_iter()
            .collect::<AHashSet<u16>>();

        let proxy_tokens = cfg.proxy_tokens.clone().and_then(|v| {
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
        for t in cfg.admin_tokens.iter() {
            if !t.is_empty() {
                admin_set.insert(t.clone());
            }
        }

        let usage_inject_upstreams = cfg.usage_inject_upstreams.clone().and_then(|v| {
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

        let model_costs = ahash::AHashMap::new();

        Self {
            request_timeout: Duration::from_millis(cfg.request_timeout_ms),
            max_retries: cfg.max_retries.unwrap_or(5),
            retry_status_codes: Arc::new(retry_status_codes),
            key_config: cfg.key.clone(),
            proxy_tokens,
            admin_tokens: Arc::new(admin_set),
            usage_inject_upstreams,
            model_costs,
            server: cfg.server.clone(),
            preview: config_preview(cfg),
        }
    }
}

impl RouterState {
    pub fn new(cfg: Config, config_path: Option<PathBuf>) -> anyhow::Result<Self> {
        let runtime = Arc::new(RuntimeConfig::from_config(&cfg));
        let queue_max_depth = runtime.server.queue_max_depth;
        let request_timeout = runtime.request_timeout;
        let max_retries = runtime.max_retries;
        let retry_status_codes = runtime.retry_status_codes.clone();
        let proxy_tokens = runtime.proxy_tokens.clone();
        let admin_tokens = runtime.admin_tokens.clone();
        let usage_inject_upstreams = runtime.usage_inject_upstreams.clone();

        // Storage
        let data_dir: PathBuf = cfg.data_dir.clone();
        let store = Arc::new(KeyStore::open(&data_dir)?);
        let billing = Arc::new(BillingStore::new(&store)?);
        let model_routes_path = data_dir.join("models_routes.json");
        let upstreams_path = data_dir.join("upstreams.json");
        let requests_log_path = data_dir.join("requests.jsonl");
        let log_tx = start_request_log_writer(requests_log_path.clone());
        let requests = Arc::new(RequestsLog::new(5000, log_tx));

        let retention_days = runtime.server.request_log_retention_days;
        spawn_request_log_cleanup(requests_log_path, retention_days);

        let mut upstream_configs: Vec<UpstreamConfig> = Vec::new();
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
            runtime: ArcSwap::from(runtime),
            request_timeout,
            max_retries,
            retry_status_codes,
            key_config: cfg.key.clone(),
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
            admin_write_lock: Arc::new(Mutex::new(())),
            shutting_down: AtomicBool::new(false),
            queue_notify: Notify::new(),
            queue_slots: Semaphore::new(queue_max_depth),
            config_path,
            listen_addr: cfg.listen_addr.clone(),
            worker_threads: cfg.worker_threads,
            data_dir,
        })
    }

    #[inline]
    pub fn authorize_proxy(&self, req: &Request<Body>) -> bool {
        let rt = self.runtime.load();
        let Some(tokens) = &rt.proxy_tokens else {
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
            Ok(s) => self.runtime.load().admin_tokens.contains(s),
            Err(_) => false,
        }
    }

    #[inline]
    pub fn should_inject_usage(&self, upstream_id: &str) -> bool {
        self.runtime
            .load()
            .usage_inject_upstreams
            .as_ref()
            .map(|set| set.contains(upstream_id))
            .unwrap_or(false)
    }

    #[inline]
    pub fn should_retry_status(&self, status: http::StatusCode) -> bool {
        self.runtime
            .load()
            .retry_status_codes
            .contains(&status.as_u16())
    }

    pub fn retry_status_codes_sorted(&self) -> Vec<u16> {
        let rt = self.runtime.load();
        let mut v: Vec<u16> = rt.retry_status_codes.iter().copied().collect();
        v.sort_unstable();
        v
    }

    #[inline]
    pub fn record_request(&self, entry: RequestLogEntry) {
        self.requests.record(entry);
    }

    pub fn recent_requests(&self, limit: usize) -> Vec<RequestLogEntry> {
        self.requests.recent(limit)
    }

    pub fn subscribe_requests(&self) -> broadcast::Receiver<RequestLogEntry> {
        self.requests.subscribe()
    }

    pub fn metrics_snapshot(&self, window: MetricsWindow) -> Vec<MetricsBucket> {
        self.requests.metrics_snapshot(window)
    }

    pub fn server_config(&self) -> ServerConfig {
        self.runtime.load().server.clone()
    }

    pub fn request_timeout(&self) -> Duration {
        self.runtime.load().request_timeout
    }

    pub fn max_retries(&self) -> usize {
        self.runtime.load().max_retries
    }

    pub fn key_config(&self) -> KeyConfig {
        self.runtime.load().key_config.clone()
    }

    pub fn config_preview(&self) -> serde_json::Value {
        self.runtime.load().preview.clone()
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Relaxed)
    }

    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Relaxed);
        self.queue_notify.notify_waiters();
    }

    pub fn notify_capacity(&self) {
        self.queue_notify.notify_one();
    }

    pub fn queue_enter(&self) -> Option<QueueGuard<'_>> {
        let permit = self.queue_slots.try_acquire().ok()?;
        self.stats.queue_depth.fetch_add(1, Ordering::Relaxed);
        Some(QueueGuard {
            state: self,
            _permit: permit,
        })
    }

    pub fn upstream_by_id(&self, id: &str) -> Option<(usize, Arc<Upstream>)> {
        let snap = self.snapshot.load_full();
        let idx = *snap.upstream_index.get(id)?;
        Some((idx, snap.upstreams[idx].clone()))
    }

    /// Select an upstream + key that supports the given model.
    /// Returns None only if no upstream has active keys for the model.
    pub fn select_for_model(&self, model: &str, _now_ms: u64) -> Option<Selected> {
        if self.is_shutting_down() {
            return None;
        }
        let snap = self.snapshot.load_full();
        let sched_len = snap.schedule.len();
        if sched_len == 0 {
            return None;
        }

        let global_max = self.key_config().max_concurrent_per_key;

        for _ in 0..sched_len {
            let rr = self.sched_rr.as_ref().fetch_add(1, Ordering::Relaxed);
            let u_idx = snap.schedule[rr % sched_len];
            let u = &snap.upstreams[u_idx];

            if !u.models.load().contains(model) && !u.model_map.contains_key(model) {
                continue;
            }

            // Per-upstream override, fallback to global default.
            let max = if u.max_concurrent_per_key > 0 {
                u.max_concurrent_per_key
            } else {
                global_max
            };

            if let Some(k) = u.select_key(max) {
                // Key level check: -1 = admin (pass always); otherwise need level >= upstream min.
                let key_level = k.level.load(Ordering::Relaxed);
                if key_level != -1 && key_level < u.min_key_level {
                    continue;
                }
                self.stats
                    .upstream_selected_total
                    .fetch_add(1, Ordering::Relaxed);
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
        snap.upstreams
            .iter()
            .any(|u| u.models.load().contains(model) || u.model_map.contains_key(model))
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
    /// Handle an upstream response status. Only auth errors (401/403) count as
    /// key failures. 429 (rate limit) sets a short cooldown on the key.
    #[inline]
    pub fn on_upstream_status(
        &self,
        sel: &Selected,
        status: http::StatusCode,
        retry_after_ms: Option<u64>,
    ) {
        let u = &sel.upstream;

        inc_status(&u.stats, status);
        self.inc_global_status(status);

        // Only count auth errors as key failures (matches gpt-load behaviour).
        if status == http::StatusCode::UNAUTHORIZED || status == http::StatusCode::FORBIDDEN {
            self.handle_auth_failure(sel);
        } else if status == http::StatusCode::TOO_MANY_REQUESTS {
            let key_cfg = self.key_config();
            let configured = key_cfg.rate_limit_cooldown_ms;
            let cooldown_ms = retry_after_ms
                .or(if configured > 0 {
                    Some(configured)
                } else {
                    None
                })
                .unwrap_or(3000)
                .min(key_cfg.max_rate_limit_cooldown_ms.max(1));
            if cooldown_ms > 0 {
                let until = now_ms() + cooldown_ms;
                sel.key.cooldown_until_ms.store(until, Ordering::Relaxed);
            }
        }
    }

    /// Increment failure count; if threshold reached, mark key invalid and
    /// remove it from the upstream's active pool.
    fn handle_auth_failure(&self, sel: &Selected) {
        let threshold = self.key_config().blacklist_threshold;
        if threshold == 0 {
            return; // auto-blacklist disabled
        }

        let key = &sel.key;
        let new_count = key.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        if new_count >= threshold {
            key.status.store(KEY_STATUS_INVALID, Ordering::Relaxed);
            sel.upstream.rebuild_active_keys();
            tracing::info!(
                key = %key.key,
                upstream = %sel.upstream.id,
                failures = new_count,
                "key blacklisted after auth failures"
            );
        }
    }

    #[inline]
    pub fn on_timeout(&self, sel: &Selected, _now_ms: u64) {
        self.stats.errors_timeout.fetch_add(1, Ordering::Relaxed);
        sel.upstream
            .stats
            .errors_timeout
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn on_network_error(&self, sel: &Selected, _now_ms: u64) {
        self.stats.errors_network.fetch_add(1, Ordering::Relaxed);
        sel.upstream
            .stats
            .errors_network
            .fetch_add(1, Ordering::Relaxed);
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

    pub fn record_latency(&self, latency_ns: u64) {
        self.stats
            .latency_ns_total
            .fetch_add(latency_ns, Ordering::Relaxed);
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

    /// Spawn a background task that periodically re-validates invalid keys.
    /// Invalid keys are tested against their upstream; if the upstream responds
    /// with anything other than 401/403, the key is restored to active.
    pub fn start_revalidation(self: &Arc<RouterState>) {
        let state = Arc::clone(self);

        tokio::spawn(async move {
            // Wait a bit before first run to let the system stabilise.
            tokio::time::sleep(Duration::from_secs(10)).await;

            loop {
                let key_cfg = state.key_config();
                let interval_secs = key_cfg.revalidation_interval_secs.max(60);
                let timeout_secs = key_cfg.revalidation_timeout_secs.max(5);
                tracing::debug!("revalidation: checking invalid keys...");
                let start = now_ms();

                let snap = state.snapshot.load_full();
                let mut total_checked = 0u32;
                let mut total_restored = 0u32;

                for upstream in snap.upstreams.iter() {
                    let invalid_keys: Vec<Arc<KeyState>> = upstream
                        .keys
                        .load_full()
                        .iter()
                        .filter(|k| !k.is_active())
                        .cloned()
                        .collect();

                    if invalid_keys.is_empty() {
                        continue;
                    }

                    for key in invalid_keys {
                        total_checked += 1;
                        match validate_key(upstream, &key, Duration::from_secs(timeout_secs)).await
                        {
                            Ok(true) => {
                                key.failure_count.store(0, Ordering::Relaxed);
                                key.status.store(KEY_STATUS_ACTIVE, Ordering::Relaxed);
                                upstream.rebuild_active_keys();
                                total_restored += 1;
                                tracing::info!(
                                    key = %key.key,
                                    upstream = %upstream.id,
                                    "revalidation: key restored"
                                );
                            }
                            Ok(false) => {
                                tracing::debug!(
                                    key = %key.key,
                                    upstream = %upstream.id,
                                    "revalidation: key still invalid"
                                );
                            }
                            Err(e) => {
                                tracing::debug!(
                                    key = %key.key,
                                    upstream = %upstream.id,
                                    error = %e,
                                    "revalidation: request failed"
                                );
                            }
                        }
                    }
                }

                let elapsed_ms = now_ms().saturating_sub(start);
                if total_checked > 0 {
                    tracing::info!(
                        checked = total_checked,
                        restored = total_restored,
                        elapsed_ms,
                        "revalidation cycle complete"
                    );
                }

                tokio::time::sleep(Duration::from_secs(interval_secs)).await;
            }
        });
    }
}

/// Test a single key by calling the upstream's /v1/models endpoint.
/// Returns Ok(true) if the key is valid (not 401/403), Ok(false) if invalid.
pub async fn validate_key(
    upstream: &Upstream,
    key: &KeyState,
    timeout: Duration,
) -> anyhow::Result<bool> {
    use hyper::body::HttpBody;

    let pq: http::uri::PathAndQuery = match upstream.format {
        UpstreamFormat::Openai | UpstreamFormat::Anthropic => {
            http::uri::PathAndQuery::from_static("/v1/models")
        }
        UpstreamFormat::Gemini => {
            let path = format!("/v1beta/models?key={}", url_encode(key.key.as_ref()));
            path.parse()?
        }
    };
    let uri = upstream.build_uri(&pq)?;
    let mut req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())?;
    insert_key_headers(req.headers_mut(), upstream.format, key)?;

    let mut resp = match tokio::time::timeout(timeout, upstream.client.request(req)).await {
        Ok(Ok(resp)) => resp,
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => anyhow::bail!("validation request timeout"),
    };

    let status = resp.status();
    // Drain the body to allow connection reuse.
    while let Some(Ok(_)) = resp.data().await {}

    // 401/403 = key is still invalid. Anything else = key is probably fine.
    Ok(status != http::StatusCode::UNAUTHORIZED && status != http::StatusCode::FORBIDDEN)
}

impl KeyState {
    pub fn is_active(&self) -> bool {
        self.status.load(Ordering::Relaxed) == KEY_STATUS_ACTIVE
    }

    pub fn record_latency_ms(&self, latency_ms: u64) {
        let Ok(mut latencies) = self.latencies_ms.lock() else {
            return;
        };
        latencies.push_back(latency_ms);
        while latencies.len() > 256 {
            latencies.pop_front();
        }
    }

    pub fn latency_percentiles(&self) -> (Option<u64>, Option<u64>, Option<u64>) {
        let Ok(latencies) = self.latencies_ms.lock() else {
            return (None, None, None);
        };
        if latencies.is_empty() {
            return (None, None, None);
        }
        let mut values: Vec<u64> = latencies.iter().copied().collect();
        values.sort_unstable();
        (
            Some(percentile(&values, 50)),
            Some(percentile(&values, 90)),
            Some(percentile(&values, 99)),
        )
    }
}

impl Upstream {
    /// Select an active key via atomic round-robin, skipping keys at their
    /// concurrency limit and keys in 429 cooldown. Returns None if no active
    /// keys available.
    fn select_key(&self, max_concurrent: u32) -> Option<Arc<KeyState>> {
        let keys = self.active_keys.load_full();
        let n = keys.len();
        if n == 0 {
            return None;
        }
        let now = now_ms();
        let start = self.key_rr.fetch_add(1, Ordering::Relaxed);
        for i in 0..n {
            let idx = (start + i) % n;
            let k = &keys[idx];
            // Skip keys in 429 cooldown.
            let until = k.cooldown_until_ms.load(Ordering::Relaxed);
            if until > 0 && now < until {
                continue;
            }
            // Clear expired cooldown (no-op if already 0).
            if until > 0 && now >= until {
                k.cooldown_until_ms.store(0, Ordering::Relaxed);
            }
            // Skip keys at concurrency limit.
            if max_concurrent > 0 && k.active_requests.load(Ordering::Relaxed) >= max_concurrent {
                continue;
            }
            return Some(k.clone());
        }
        // All keys at limit or in cooldown — return None to signal backpressure.
        None
    }

    /// Rebuild the active_keys list from the full keys list based on current status.
    pub fn rebuild_active_keys(&self) {
        let all = self.keys.load_full();
        let active: Vec<Arc<KeyState>> = all.iter().filter(|k| k.is_active()).cloned().collect();
        self.active_keys.store(Arc::new(active));
    }

    /// Restore specified keys to active. Returns count restored.
    pub fn restore_keys(&self, set: &AHashSet<String>) -> usize {
        let all = self.keys.load_full();
        let mut n = 0;
        for k in all.iter() {
            if set.contains(k.key.as_ref()) {
                k.failure_count.store(0, Ordering::Relaxed);
                k.status.store(KEY_STATUS_ACTIVE, Ordering::Relaxed);
                n += 1;
            }
        }
        if n > 0 {
            self.rebuild_active_keys();
        }
        n
    }

    /// Restore all invalid keys. Returns count restored.
    pub fn restore_all_keys(&self) -> usize {
        let all = self.keys.load_full();
        let mut n = 0;
        for k in all.iter() {
            if !k.is_active() {
                k.failure_count.store(0, Ordering::Relaxed);
                k.status.store(KEY_STATUS_ACTIVE, Ordering::Relaxed);
                n += 1;
            }
        }
        if n > 0 {
            self.rebuild_active_keys();
        }
        n
    }

    /// Invalidate specified keys. Returns count invalidated.
    pub fn invalidate_keys(&self, set: &AHashSet<String>) -> usize {
        let all = self.keys.load_full();
        let mut n = 0;
        for k in all.iter() {
            if set.contains(k.key.as_ref()) {
                k.status.store(KEY_STATUS_INVALID, Ordering::Relaxed);
                n += 1;
            }
        }
        if n > 0 {
            self.rebuild_active_keys();
        }
        n
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
    let format = u
        .format
        .unwrap_or_else(|| UpstreamFormat::detect(&u.base_url));
    let proxy = u.proxy.clone().filter(|p| !p.trim().is_empty());
    let client = UpstreamClient::new(proxy.as_deref())?;

    let scheme = base
        .scheme()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("upstream {}: base_url missing scheme", name_for_err))?;
    let authority = base
        .authority()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("upstream {}: base_url missing authority", name_for_err))?;

    let base_path = base.path().trim_end_matches('/').to_string();
    let base_path = if base_path == "/" {
        String::new()
    } else {
        base_path
    };

    let mut model_map = ahash::AHashMap::new();
    let mut model_rmap = ahash::AHashMap::new();
    for (k, v) in u.model_map.iter() {
        model_map.insert(k.clone(), v.clone());
        model_rmap.insert(v.clone(), k.clone());
    }

    let upstream = Upstream {
        id: Arc::<str>::from(u.id),
        base_url: Arc::<str>::from(u.base_url.clone()),
        base_scheme: scheme,
        base_authority: authority,
        base_path: Arc::<str>::from(base_path),
        weight,
        max_concurrent_per_key: u.max_concurrent_per_key.unwrap_or(0),
        min_key_level: u.min_key_level,
        format,
        proxy,
        client,
        keys: ArcSwap::from_pointee(Vec::new()),
        active_keys: ArcSwap::from_pointee(Vec::new()),
        keys_update_lock: Mutex::new(()),
        key_rr: AtomicUsize::new(0),
        models: ArcSwap::from_pointee(AHashSet::new()),
        model_map,
        model_rmap,
        stats: UpstreamStats::default(),
    };

    Ok(Arc::new(upstream))
}

pub fn build_key_states(keys: Vec<String>, store: Option<&crate::storage::KeyStore>) -> anyhow::Result<Arc<Vec<Arc<KeyState>>>> {
    let mut out: Vec<Arc<KeyState>> = Vec::with_capacity(keys.len());
    for k in keys {
        let k = k.trim();
        if k.is_empty() {
            continue;
        }
        if let Err(e) = crate::util::validate_key_chars(k) {
            tracing::warn!(key = %k, error = %e, "key rejected");
            continue;
        }
        let key_arc: Arc<str> = Arc::<str>::from(k.to_string());
        let auth_header = hyper::header::HeaderValue::from_str(&format!("Bearer {}", key_arc))
            .map_err(|_| anyhow::anyhow!("invalid key (cannot be used in HTTP header)"))?;
        let level = store.map(|s| s.get_key_level(k)).unwrap_or(0);
        out.push(Arc::new(KeyState {
            key: key_arc,
            auth_header,
            failure_count: AtomicU32::new(0),
            status: AtomicU8::new(KEY_STATUS_ACTIVE),
            active_requests: AtomicU32::new(0),
            level: AtomicI32::new(level),
            cooldown_until_ms: AtomicU64::new(0),
            latencies_ms: Mutex::new(VecDeque::with_capacity(256)),
        }));
    }
    Ok(Arc::new(out))
}

fn parse_models_response(format: UpstreamFormat, body: &[u8]) -> anyhow::Result<AHashSet<String>> {
    let v: serde_json::Value = serde_json::from_slice(body)?;
    let mut out: AHashSet<String> = AHashSet::new();
    match format {
        UpstreamFormat::Openai | UpstreamFormat::Anthropic => {
            let data = v
                .get("data")
                .and_then(|d| d.as_array())
                .ok_or_else(|| anyhow::anyhow!("missing data array in models response"))?;
            out.reserve(data.len());
            for item in data {
                if let Some(id) = item.get("id").and_then(|s| s.as_str()) {
                    out.insert(id.to_string());
                }
            }
        }
        UpstreamFormat::Gemini => {
            let data = v
                .get("models")
                .and_then(|d| d.as_array())
                .ok_or_else(|| anyhow::anyhow!("missing models array in gemini response"))?;
            out.reserve(data.len());
            for item in data {
                if let Some(name) = item.get("name").and_then(|s| s.as_str()) {
                    out.insert(name.trim_start_matches("models/").to_string());
                }
            }
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
        let _guard = self
            .admin_write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("admin write lock poisoned"))?;
        let mut list = self.current_upstream_configs();
        if list.iter().any(|u| u.id == cfg.id) {
            anyhow::bail!("upstream id already exists");
        }
        list.push(cfg);
        self.replace_upstreams(list)?;
        Ok(())
    }

    pub fn update_upstream(&self, id: &str, mut cfg: UpstreamConfig) -> anyhow::Result<()> {
        let _guard = self
            .admin_write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("admin write lock poisoned"))?;
        let mut list = self.current_upstream_configs();
        let mut found = false;
        cfg.id = id.to_string();
        if cfg.format.is_none() {
            cfg.format = Some(UpstreamFormat::detect(&cfg.base_url));
        }
        for u in list.iter_mut() {
            if u.id == id {
                *u = cfg.clone();
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
        let _guard = self
            .admin_write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("admin write lock poisoned"))?;
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

    pub async fn test_key_by_value(
        &self,
        upstream_id: &str,
        key_value: &str,
    ) -> anyhow::Result<bool> {
        let Some((_idx, upstream)) = self.upstream_by_id(upstream_id) else {
            anyhow::bail!("unknown upstream id");
        };
        let key_value = key_value.trim();
        if key_value.is_empty() {
            anyhow::bail!("key must not be empty");
        }
        let key = build_key_states(vec![key_value.to_string()], None)?
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("invalid key"))?;
        validate_key(
            &upstream,
            &key,
            Duration::from_secs(self.key_config().revalidation_timeout_secs.max(5)),
        )
        .await
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
                max_concurrent_per_key: if u.max_concurrent_per_key > 0 {
                    Some(u.max_concurrent_per_key)
                } else {
                    None
                },
                format: Some(u.format),
                proxy: u.proxy.clone(),
                model_map: u.model_map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                min_key_level: u.min_key_level,
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
        let keys = upstream.active_keys.load_full();
        let key = keys
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no active keys for upstream"))?;

        let pq: http::uri::PathAndQuery = match upstream.format {
            UpstreamFormat::Openai | UpstreamFormat::Anthropic => {
                http::uri::PathAndQuery::from_static("/v1/models")
            }
            UpstreamFormat::Gemini => {
                let path = format!("/v1beta/models?key={}", url_encode(key.key.as_ref()));
                path.parse()?
            }
        };
        let uri = upstream.build_uri(&pq)?;
        let mut req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Body::empty())?;
        insert_key_headers(req.headers_mut(), upstream.format, &key)?;

        let resp = match tokio::time::timeout(self.request_timeout(), upstream.client.request(req))
            .await
        {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => anyhow::bail!("upstream request timeout"),
        };
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("upstream returned {}", status);
        }

        let body = hyper::body::to_bytes(resp.into_body()).await?;
        parse_models_response(upstream.format, &body)
    }
}

fn build_snapshot_from_configs(
    configs: &[UpstreamConfig],
    store: &KeyStore,
) -> anyhow::Result<RouterSnapshot> {
    const MAX_WEIGHT: usize = 100;

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
        let key_states = build_key_states(keys, Some(store))?;
        // active_keys starts as a copy of all keys (all active by default).
        let active = key_states.iter().cloned().collect::<Vec<_>>();
        u.keys.store(key_states);
        u.active_keys.store(Arc::new(active));

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

impl RouterState {
    /// Update model costs at runtime (via admin API).
    pub fn set_model_costs(&self, costs: ahash::AHashMap<String, crate::config::ModelCost>) {
        let old = self.runtime.load_full();
        let mut rt: RuntimeConfig = (*old).clone();
        rt.model_costs = costs;
        self.runtime.store(Arc::new(rt));
    }
}

#[cfg(unix)]
impl RouterState {
    pub fn apply_config_reload(&self, cfg: Config) -> anyhow::Result<()> {
        if cfg.listen_addr != self.listen_addr {
            tracing::warn!(
                old = %self.listen_addr,
                new = %cfg.listen_addr,
                "config: listen_addr changed, restart required"
            );
        }
        if cfg.worker_threads != self.worker_threads {
            tracing::warn!(
                old = ?self.worker_threads,
                new = ?cfg.worker_threads,
                "config: worker_threads changed, restart required"
            );
        }
        if cfg.data_dir != self.data_dir {
            tracing::warn!(
                old = %self.data_dir.display(),
                new = %cfg.data_dir.display(),
                "config: data_dir changed, restart required"
            );
        }

        let old = self.runtime.load_full();
        let mut runtime = RuntimeConfig::from_config(&cfg);
        runtime.model_costs = old.model_costs.clone(); // preserve admin-set model costs
        let runtime = Arc::new(runtime);
        if runtime.server.queue_max_depth > old.server.queue_max_depth {
            self.queue_slots
                .add_permits(runtime.server.queue_max_depth - old.server.queue_max_depth);
        } else if runtime.server.queue_max_depth < old.server.queue_max_depth {
            tracing::warn!(
                old = old.server.queue_max_depth,
                new = runtime.server.queue_max_depth,
                "config: queue_max_depth decrease applies after restart"
            );
        }
        log_runtime_config_changes(&old, &runtime);
        self.runtime.store(runtime);

        let _guard = self
            .admin_write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("admin write lock poisoned"))?;
        self.queue_notify.notify_waiters();
        Ok(())
    }
}

#[cfg(unix)]
fn log_runtime_config_changes(old: &RuntimeConfig, new: &RuntimeConfig) {
    if old.request_timeout != new.request_timeout {
        tracing::info!(
            old_ms = old.request_timeout.as_millis() as u64,
            new_ms = new.request_timeout.as_millis() as u64,
            "config reloaded: request_timeout_ms"
        );
    }
    if old.max_retries != new.max_retries {
        tracing::info!(
            old = old.max_retries,
            new = new.max_retries,
            "config reloaded: max_retries"
        );
    }
    if old.server.cors_origins != new.server.cors_origins {
        tracing::info!("config reloaded: cors_origins");
    }
    if old.server.queue_enabled != new.server.queue_enabled
        || old.server.queue_max_depth != new.server.queue_max_depth
        || old.server.queue_timeout_ms != new.server.queue_timeout_ms
    {
        tracing::info!("config reloaded: queue settings");
    }
}

fn config_preview(cfg: &Config) -> serde_json::Value {
    let mut v = serde_json::to_value(cfg).unwrap_or_else(|_| serde_json::json!({}));
    redact_config_value(&mut v);
    v
}

fn redact_config_value(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            for (k, value) in map.iter_mut() {
                if k.contains("token") || k == "keys" {
                    *value = serde_json::json!("<redacted>");
                } else {
                    redact_config_value(value);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_config_value(item);
            }
        }
        _ => {}
    }
}

fn insert_key_headers(
    headers: &mut hyper::HeaderMap,
    format: UpstreamFormat,
    key: &KeyState,
) -> anyhow::Result<()> {
    match format {
        UpstreamFormat::Openai => {
            headers.insert(HDR_AUTHORIZATION, key.auth_header.clone());
        }
        UpstreamFormat::Anthropic => {
            let value = hyper::header::HeaderValue::from_str(key.key.as_ref())?;
            headers.insert("x-api-key", value);
            headers.insert(
                "anthropic-version",
                hyper::header::HeaderValue::from_static("2023-06-01"),
            );
        }
        UpstreamFormat::Gemini => {}
    }
    Ok(())
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len().saturating_sub(1)) * pct) / 100;
    sorted[idx]
}

fn url_encode(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
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

/// Clean old request log entries from the JSONL file.
/// Returns (entries_kept, entries_removed).
async fn cleanup_request_log(path: &Path, retention_days: u64) -> (usize, usize) {
    if retention_days == 0 {
        return (0, 0);
    }
    let cutoff_ms = now_ms().saturating_sub(retention_days * 86_400_000);

    let content = match tokio::fs::read_to_string(path).await {
        Ok(c) => c,
        Err(_) => return (0, 0), // file doesn't exist or can't be read
    };

    let mut kept = 0usize;
    let mut removed = 0usize;
    let mut new_content = String::with_capacity(content.len());

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Parse timestamp to decide keep/remove, then clone only if kept.
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(ts) = v.get("ts_ms").and_then(|t| t.as_u64()) {
                if ts < cutoff_ms {
                    removed += 1;
                    continue;
                }
            }
        }
        new_content.push_str(line);
        new_content.push('\n');
        kept += 1;
    }

    if removed > 0 {
        if let Err(e) = tokio::fs::write(path, &new_content).await {
            tracing::warn!(path = %path.display(), error = %e, "request log cleanup write failed");
            return (0, 0);
        }
    }

    (kept, removed)
}

/// Spawn a task that periodically cleans old request log entries.
/// Runs immediately at startup, then every 24 hours.
pub fn spawn_request_log_cleanup(path: PathBuf, retention_days: u64) {
    if retention_days == 0 {
        return;
    }
    tokio::spawn(async move {
        // Run immediately.
        let (kept, removed) = cleanup_request_log(&path, retention_days).await;
        tracing::info!(
            path = %path.display(),
            kept,
            removed,
            retention_days,
            "request log cleanup: {kept} kept, {removed} removed (>{retention_days}d)"
        );

        // Then every 24 hours.
        let mut interval = tokio::time::interval(Duration::from_secs(86_400));
        loop {
            interval.tick().await;
            let (kept, removed) = cleanup_request_log(&path, retention_days).await;
            tracing::info!(
                path = %path.display(),
                kept,
                removed,
                retention_days,
                "request log cleanup: {kept} kept, {removed} removed (>{retention_days}d)"
            );
        }
    });
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
