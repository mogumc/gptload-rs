
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
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

    /// Optional list of tokens required in `X-Proxy-Token` for non-admin requests.
    pub proxy_tokens: Option<Vec<String>>,

    /// List of tokens required in `X-Admin-Token` for admin API requests.
    pub admin_tokens: Vec<String>,

    /// Directory for persistent data (keys DB).
    pub data_dir: PathBuf,

    /// Upstream ids eligible for stream usage injection.
    pub usage_inject_upstreams: Option<Vec<String>>,

    pub ban: BanConfig,

    pub upstreams: Vec<UpstreamConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BanConfig {
    pub rate_limit_ms: u64,
    pub server_error_ms: u64,
    pub network_error_ms: u64,
    pub auth_error_ms: u64,
    pub max_backoff_pow: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UpstreamConfig {
    /// Stable upstream id (used by admin API and key DB).
    pub id: String,
    /// Example: https://api.openai.com
    pub base_url: String,
    /// Weighted round-robin (default 1).
    pub weight: Option<usize>,
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
        if let Some(v) = &mut self.proxy_tokens {
            for t in v.iter_mut() {
                *t = t.trim().to_string();
            }
            v.retain(|t| !t.is_empty());
            if v.is_empty() {
                self.proxy_tokens = None;
            }
        }
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
        Ok(())
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.admin_tokens.is_empty() {
            anyhow::bail!("config: admin_tokens must not be empty");
        }
        if self.upstreams.is_empty() {
            anyhow::bail!("config: upstreams must not be empty");
        }
        for (i, u) in self.upstreams.iter().enumerate() {
            if u.id.trim().is_empty() {
                anyhow::bail!("config: upstreams[{i}].id must not be empty");
            }
            if !(u.base_url.starts_with("http://") || u.base_url.starts_with("https://")) {
                anyhow::bail!(
                    "config: upstreams[{i}].base_url must start with http:// or https://"
                );
            }
        }
        if let Some(codes) = &self.retry_status_codes {
            for code in codes {
                if *code < 100 || *code > 599 {
                    anyhow::bail!("config: retry_status_codes contains invalid status code: {code}");
                }
            }
        }
        Ok(())
    }
}
