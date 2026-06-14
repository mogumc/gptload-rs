mod server;
mod forward;
mod response;
mod usage;

pub use server::serve_http;

use crate::state::RouterState;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

/// RAII guard: tracks inflight count and request latency.
/// For streaming responses, moved into the spawned body-consumption task
/// so the inflight counter stays accurate until the stream finishes.
pub(crate) struct RequestLifecycle {
    state: Arc<RouterState>,
    start: Instant,
    active: bool,
}

impl RequestLifecycle {
    pub(crate) fn start(state: Arc<RouterState>) -> Self {
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

#[derive(Clone)]
pub(crate) struct RequestLogContext {
    pub(crate) start: Instant,
    pub(crate) client_ip: String,
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) model: Option<String>,
    pub(crate) upstream_id: Option<String>,
    pub(crate) billing_model: Option<String>,
    pub(crate) billing_key: Option<String>,
    pub(crate) req_bytes: usize,
    pub(crate) request_headers: Option<std::collections::BTreeMap<String, String>>,
    pub(crate) request_body: Option<String>,
    pub(crate) queue_ms: u64,
    pub(crate) is_stream: Option<bool>,
    pub(crate) token_source: Option<String>,
}

impl RequestLogContext {
    pub(crate) fn new(
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
            token_source: None,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct UsageTokens {
    pub(crate) prompt: u64,
    pub(crate) completion: u64,
    pub(crate) thought: u64,
    pub(crate) total: u64,
}

impl UsageTokens {
    /// Total output tokens for billing: visible output + thinking tokens.
    /// Billing charges thinking at the output rate, not a separate rate.
    pub(crate) fn billing_completion(&self) -> u64 {
        self.completion.saturating_add(self.thought)
    }
}
