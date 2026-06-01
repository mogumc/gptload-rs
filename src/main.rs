#![forbid(unsafe_code)]

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod admin;
mod billing;
mod config;
mod format;
mod proxy;
mod state;
mod storage;
mod upstream_client;
mod util;

use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "gptload-rs",
    version,
    about = "High-performance OpenAI-format proxy with admin UI/API, hot key reload, realtime stats"
)]
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

    let config_path = cli.config.clone();
    let cfg = config::Config::load(&config_path)?;

    let worker_threads = cfg.worker_threads.unwrap_or_else(num_cpus::get);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .thread_name("gptload-worker")
        .build()?;

    rt.block_on(async move {
        let addr: SocketAddr = cfg.listen_addr.parse()?;
        let state = Arc::new(state::RouterState::new(
            cfg,
            Some(PathBuf::from(config_path.clone())),
        )?);
        state.refresh_missing_models_routes().await;
        state.start_revalidation();
        spawn_config_reload(state.clone(), config_path);
        tracing::info!(%addr, "listening (admin at /admin/)");
        let shutdown = graceful_shutdown_signal(state.clone());
        proxy::serve_http(addr, state, shutdown).await
    })
}

async fn graceful_shutdown_signal(state: Arc<state::RouterState>) {
    wait_shutdown_signal().await;
    state.begin_shutdown();
    tokio::spawn(wait_inflight_or_exit(state));
}

async fn wait_inflight_or_exit(state: Arc<state::RouterState>) {
    let timeout_secs = state.server_config().graceful_shutdown_timeout_secs;
    let deadline = tokio::time::sleep(Duration::from_secs(timeout_secs));
    tokio::pin!(deadline);
    let mut tick = tokio::time::interval(Duration::from_secs(1));

    loop {
        let inflight = state.stats.requests_inflight.load(Ordering::Relaxed);
        if inflight == 0 {
            tracing::info!("graceful shutdown: no inflight requests, exiting");
            break;
        }
        tokio::select! {
            _ = &mut deadline => {
                tracing::warn!(
                    inflight,
                    "graceful shutdown: timeout, forcing exit"
                );
                std::process::exit(0);
            }
            _ = tick.tick() => {
                tracing::info!(
                    inflight,
                    "graceful shutdown: waiting for inflight requests..."
                );
            }
        }
    }
}

async fn wait_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async {
                if let Some(ref mut term) = term {
                    term.recv().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn spawn_config_reload(state: Arc<state::RouterState>, config_path: String) {
    #[cfg(unix)]
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let mut hup = match signal(SignalKind::hangup()) {
            Ok(signal) => signal,
            Err(e) => {
                tracing::warn!(error = %e, "failed to install SIGHUP handler");
                return;
            }
        };
        while hup.recv().await.is_some() {
            match config::Config::load(&config_path) {
                Ok(cfg) => match state.apply_config_reload(cfg) {
                    Ok(()) => tracing::info!("config reloaded"),
                    Err(e) => tracing::warn!(error = %e, "config reload failed"),
                },
                Err(e) => tracing::warn!(error = %e, "config reload failed"),
            }
        }
    });

    #[cfg(not(unix))]
    {
        let _ = state;
        let _ = config_path;
    }
}
