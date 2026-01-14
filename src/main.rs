
#![forbid(unsafe_code)]

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod admin;
mod billing;
mod config;
mod proxy;
mod state;
mod storage;
mod util;

use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "gptload-rs", version, about = "High-performance OpenAI-format proxy with admin UI/API, hot key reload, realtime stats")]
struct Cli {
    /// Path to TOML config
    #[arg(long, default_value = "config.toml")]
    config: String,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .with_level(true)
        .init();

    let cfg = config::Config::load(&cli.config)?;

    let worker_threads = cfg.worker_threads.unwrap_or_else(num_cpus::get);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .thread_name("gptload-worker")
        .build()?;

    rt.block_on(async move {
        let addr: SocketAddr = cfg.listen_addr.parse()?;
        let state = Arc::new(state::RouterState::new(cfg)?);
        state.refresh_missing_models_routes().await;
        tracing::info!(%addr, "listening (admin at /admin/)");
        proxy::serve_http(addr, state).await
    })
}
