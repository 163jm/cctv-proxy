mod browser;
mod cache;
mod cctv;
mod db;
mod gateway;
mod m3u8;
mod native;
mod provincial;
mod state;

use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "tv-proxy", about = "Chinese TV live-stream proxy (Rust rewrite)")]
struct Args {
    /// Directory containing app.db and chrome/
    #[arg(long, env = "APP_DIR", default_value = ".")]
    app_dir: PathBuf,

    /// Public-facing port
    #[arg(long, env = "PORT", default_value = "3000")]
    port: u16,

    /// Bind address
    #[arg(long, env = "BIND", default_value = "0.0.0.0")]
    bind: String,

    /// Path to headless-shell binary (overrides auto-detect)
    #[arg(long, env = "CHROME_PATH")]
    chrome_path: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| args.log_level.as_str().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let db_path = args.app_dir.join("app.db");
    info!("Loading database from {:?}", db_path);
    let db = db::AppDb::load(&db_path)?;
    info!(
        "Loaded {} CCTV channels, {} provincial channels, {} site rules",
        db.cctv_channels.len(),
        db.channels.len(),
        db.site_rules.len()
    );

    let jstv_auth_enabled = db.jstv_auth_enabled;

    let chrome_path = args.chrome_path.unwrap_or_else(|| {
        let local = args.app_dir.join("chrome/headless-shell");
        if local.exists() {
            local.to_string_lossy().into_owned()
        } else {
            "headless-shell".to_string()
        }
    });
    info!("Chrome path: {}", chrome_path);

    let chrome_dir = args.app_dir.join("chrome");
    let native = native::NativeLibs::load(&chrome_dir, jstv_auth_enabled);
    if native.decryptor.is_some() {
        info!("TS decryptor (delib.so) loaded");
    } else {
        warn!("TS decryptor not available - CCTV segments will not be decrypted");
    }
    if native.signer.is_some() {
        info!("JSTV signer (media_utils.so) loaded");
    } else if jstv_auth_enabled {
        warn!("JSTV auth enabled but media_utils.so not loaded");
    }

    let state = state::AppState::new(db, native, chrome_path.clone(), args.app_dir.clone());
    let browser = browser::BrowserPool::new(chrome_path);

    {
        let state_clone = state.clone();
        let browser_clone = Arc::clone(&browser);
        tokio::spawn(async move {
            provincial::run_poller(state_clone, browser_clone).await;
        });
    }

    let app = gateway::router(state, browser)
        .layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any),
        );

    let addr: SocketAddr = format!("{}:{}", args.bind, args.port).parse()?;
    info!(
        "\n==================================================\n \
         Unified Gateway listening on http://{}\n \
         Combined M3U: http://{}/live.m3u\n\
         ==================================================",
        addr, addr
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Server shut down cleanly.");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("Shutdown signal received");
}
