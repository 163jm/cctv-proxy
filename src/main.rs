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

/// Locate the headless Chrome binary for the current platform.
///
/// The original project bundles a Linux (often ARM64) `chrome/headless-shell`;
/// on Windows that binary cannot run — this is the #1 reason browser-dependent
/// provincial channels fail there. We look for platform-appropriate names first
/// and warn loudly if only the Linux binary is present.
fn resolve_chrome_path(app_dir: &std::path::Path) -> String {
    let chrome_dir = app_dir.join("chrome");

    #[cfg(target_os = "windows")]
    {
        for candidate in [
            chrome_dir.join("headless-shell.exe"),
            chrome_dir.join("chrome.exe"),
            app_dir.join("headless-shell.exe"),
            app_dir.join("chrome.exe"),
        ] {
            if candidate.exists() {
                return candidate.to_string_lossy().into_owned();
            }
        }
        // Only the Linux binary present → warn, then return it anyway so the
        // spawn error surfaces in the logs instead of silently doing nothing.
        let linux_bin = chrome_dir.join("headless-shell");
        if linux_bin.exists() {
            native::warn_if_elf_on_windows(&linux_bin, "Headless browser");
            return linux_bin.to_string_lossy().into_owned();
        }
        "headless-shell".to_string()
    }

    #[cfg(not(target_os = "windows"))]
    {
        for candidate in [chrome_dir.join("headless-shell"), app_dir.join("headless-shell")] {
            if candidate.exists() {
                return candidate.to_string_lossy().into_owned();
            }
        }
        "headless-shell".to_string()
    }
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

    let chrome_path = args.chrome_path.unwrap_or_else(|| resolve_chrome_path(&args.app_dir));
    info!("Chrome path: {}", chrome_path);

    let chrome_dir = args.app_dir.join("chrome");
    let native = native::NativeLibs::load(&chrome_dir, jstv_auth_enabled);
    if native.decryptor.is_some() {
        info!("TS decryptor (delib) loaded");
    } else {
        warn!("TS decryptor not available - CCTV segments will not be decrypted");
    }
    if native.signer.is_some() {
        info!("JSTV signer (media_utils) loaded");
    } else if jstv_auth_enabled {
        warn!("JSTV auth enabled but media_utils not loaded");
    }

    // Startup diagnostics: make it obvious why whole groups of channels fail.
    let signed_count = db
        .channels
        .iter()
        .filter(|c| {
            c.id.starts_with("js_")
                || c.id.starts_with("zj_")
                || c.id.starts_with("sd_")
                || c.id.starts_with("sh_")
        })
        .count();
    if signed_count > 0 && native.signer.is_none() {
        warn!(
            "{} signed channel(s) (js_/zj_/sd_/sh_) need media_utils but it is NOT loaded — \
             these channels will fail until the correct native library is provided.",
            signed_count
        );
    }
    let browser_channels = db
        .channels
        .iter()
        .filter(|c| {
            !(c.id.starts_with("js_")
                || c.id.starts_with("zj_")
                || c.id.starts_with("sd_")
                || c.id.starts_with("sh_"))
        })
        .count();
    if browser_channels > 0 {
        info!(
            "{} channel(s) rely on the headless browser for stream discovery.",
            browser_channels
        );
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
