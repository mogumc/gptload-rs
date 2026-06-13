use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Model cost rates (credits per 1K tokens, or flat per-request fee).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelCost {
    /// Credits per 1K prompt tokens.
    #[serde(default)]
    pub input: f64,
    /// Credits per 1K completion tokens.
    #[serde(default = "default_output_rate")]
    pub output: f64,
    /// Flat credits per request. When set, input/output rates are ignored —
    /// used for image generation and other non-text APIs where token
    /// counting is meaningless.
    #[serde(default)]
    pub per_request: Option<u64>,
}

fn default_output_rate() -> f64 {
    1.0
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Proxy listen address, HTTP only.
    pub listen_addr: String,

    /// Tokio runtime worker threads.
    pub worker_threads: Option<usize>,

    /// Upstream request timeout (ms).
    pub request_timeout_ms: u64,

    /// Maximum retry attempts for retryable upstream responses.
    pub max_retries: Option<usize>,

    /// Upstream HTTP status codes that should trigger retry.
    pub retry_status_codes: Option<Vec<u16>>,

    /// List of tokens required in `X-Admin-Token` for admin API requests.
    pub admin_tokens: Vec<String>,

    /// Directory for persistent data (keys DB).
    pub data_dir: PathBuf,

    /// Upstream ids eligible for stream usage injection.
    pub usage_inject_upstreams: Option<Vec<String>>,

    /// Server-level runtime behavior.
    #[serde(default)]
    pub server: ServerConfig,

    pub key: KeyConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    /// Wait this long for in-flight requests after SIGINT/SIGTERM.
    #[serde(default = "default_graceful_shutdown_timeout_secs")]
    pub graceful_shutdown_timeout_secs: u64,

    /// Allowed CORS origins. `["*"]` allows every origin.
    #[serde(default = "default_cors_origins")]
    pub cors_origins: Vec<String>,

    /// Queue requests while all eligible keys are busy/cooling down.
    #[serde(default)]
    pub queue_enabled: bool,

    /// Maximum requests waiting in the queue.
    #[serde(default = "default_queue_max_depth")]
    pub queue_max_depth: usize,

    /// Maximum time a queued request waits for capacity.
    #[serde(default = "default_queue_timeout_ms")]
    pub queue_timeout_ms: u64,

    /// Auto-delete request logs older than this many days. 0 = disabled, default 7.
    #[serde(default = "default_request_log_retention_days")]
    pub request_log_retention_days: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            graceful_shutdown_timeout_secs: default_graceful_shutdown_timeout_secs(),
            cors_origins: default_cors_origins(),
            queue_enabled: false,
            queue_max_depth: default_queue_max_depth(),
            queue_timeout_ms: default_queue_timeout_ms(),
            request_log_retention_days: default_request_log_retention_days(),
        }
    }
}

fn default_graceful_shutdown_timeout_secs() -> u64 {
    30
}

fn default_cors_origins() -> Vec<String> {
    vec!["*".to_string()]
}

fn default_queue_max_depth() -> usize {
    100
}

fn default_queue_timeout_ms() -> u64 {
    10_000
}

fn default_request_log_retention_days() -> u64 {
    7
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KeyConfig {
    /// Number of auth failures (401/403) before a key is marked invalid.
    /// 0 means disabled (keys are never auto-invalidated).
    pub blacklist_threshold: u32,

    /// Max concurrent requests per key. Keys at this limit are skipped
    /// during selection. 0 means no limit.
    #[serde(default)]
    pub max_concurrent_per_key: u32,

    /// Cooldown (ms) after a 429 rate-limit response before the key can be
    /// selected again. 0 = disabled.
    #[serde(default = "default_rate_limit_cooldown_ms")]
    pub rate_limit_cooldown_ms: u64,

    /// Upper bound for Retry-After-driven rate-limit cooldowns.
    #[serde(default = "default_max_rate_limit_cooldown_ms")]
    pub max_rate_limit_cooldown_ms: u64,

    /// How often (seconds) to re-validate invalid keys.
    pub revalidation_interval_secs: u64,

    /// Timeout (seconds) for each re-validation request.
    pub revalidation_timeout_secs: u64,
}

fn default_rate_limit_cooldown_ms() -> u64 {
    3000
}

fn default_max_rate_limit_cooldown_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UpstreamFormat {
    Openai,
    Anthropic,
    Gemini,
}

impl UpstreamFormat {
    pub fn detect(base_url: &str) -> Self {
        if base_url.contains("api.anthropic.com") {
            Self::Anthropic
        } else if base_url.contains("generativelanguage.googleapis.com") {
            Self::Gemini
        } else {
            Self::Openai
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::Anthropic => "anthropic",
            Self::Gemini => "gemini",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UpstreamConfig {
    /// Stable upstream id (used by admin API and key DB).
    pub id: String,
    /// Example: https://api.openai.com
    pub base_url: String,
    /// Weighted round-robin (default 1).
    pub weight: Option<usize>,
    /// Per-key concurrency limit for this upstream. Overrides global default.
    /// 0 = use global default.
    #[serde(default)]
    pub max_concurrent_per_key: Option<u32>,
    /// API wire format. If omitted, it is guessed from base_url.
    #[serde(default)]
    pub format: Option<UpstreamFormat>,
    /// Optional outbound proxy URL: http://..., https://..., socks5://...
    #[serde(default)]
    pub proxy: Option<String>,
    /// Model name mapping: incoming model → upstream model.
    /// Example: `model_map = { "gpt-4o" = "deepseek-chat" }`.
    #[serde(default)]
    pub model_map: HashMap<String, String>,
    /// Minimum key level required to use this upstream. Default 0. -1 = no restriction.
    #[serde(default)]
    pub min_key_level: i32,
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let s = fs::read_to_string(path)?;
        let mut cfg: Config = toml::from_str(&s)?;
        cfg.normalize()?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn normalize(&mut self) -> anyhow::Result<()> {
        // Trim tokens.
        for t in self.admin_tokens.iter_mut() {
            *t = t.trim().to_string();
        }
        self.admin_tokens.retain(|t| !t.is_empty());
        if let Some(v) = &mut self.usage_inject_upstreams {
            for id in v.iter_mut() {
                *id = id.trim().to_string();
            }
            v.retain(|id| !id.is_empty());
            if v.is_empty() {
                self.usage_inject_upstreams = None;
            }
        }
        if let Some(v) = &mut self.retry_status_codes {
            v.retain(|code| *code >= 100 && *code <= 599);
            v.sort_unstable();
            v.dedup();
            if v.is_empty() {
                self.retry_status_codes = None;
            }
        }
        for origin in self.server.cors_origins.iter_mut() {
            *origin = origin.trim().to_string();
        }
        self.server.cors_origins.retain(|o| !o.is_empty());
        if self.server.cors_origins.is_empty() {
            self.server.cors_origins = default_cors_origins();
        }
        Ok(())
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.admin_tokens.is_empty() {
            anyhow::bail!("config: admin_tokens must not be empty");
        }
        if let Some(codes) = &self.retry_status_codes {
            for code in codes {
                if *code < 100 || *code > 599 {
                    anyhow::bail!(
                        "config: retry_status_codes contains invalid status code: {code}"
                    );
                }
            }
        }
        Ok(())
    }
}
