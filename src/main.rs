mod browser;
mod cache;
mod cctv;
mod db;
mod gateway;
mod m3u8;
mod native;
mod provincial;
mod state;

use anyhow::{Context, Result};
use clap::Parser;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::signal;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
/// Search order:
///   1. same directory as the tv-proxy executable (new default layout)
///   2. `chrome/` inside APP_DIR (backward compatibility with the old layout)
///
/// On Windows, platform-appropriate names (headless-shell.exe / chrome.exe)
/// are preferred; if only a Linux `headless-shell` is present we warn loudly —
/// it cannot run on Windows and is the #1 reason browser-dependent provincial
/// channels fail there.
fn resolve_chrome_path(exe_dir: Option<&std::path::Path>, app_dir: &std::path::Path) -> String {
    // 1) Same directory as the tv-proxy binary
    if let Some(dir) = exe_dir {
        #[cfg(target_os = "windows")]
        {
            for candidate in [dir.join("headless-shell.exe"), dir.join("chrome.exe")] {
                if candidate.exists() {
                    return candidate.to_string_lossy().into_owned();
                }
            }
            // Linux binary copied next to the exe → warn, return anyway so the
            // spawn error surfaces in the logs.
            let linux_bin = dir.join("headless-shell");
            if linux_bin.exists() {
                native::warn_if_elf_on_windows(&linux_bin, "Headless browser");
                return linux_bin.to_string_lossy().into_owned();
            }
        }
        #[cfg(not(target_os = "windows"))]
        {
            let candidate = dir.join("headless-shell");
            if candidate.exists() {
                return candidate.to_string_lossy().into_owned();
            }
        }
    }

    // 2) chrome/ dir inside APP_DIR (backward compatibility)
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

    // Executable's own directory. Everything (app.db + delib + media_utils +
    // headless-shell) can live next to the binary; APP_DIR is only a fallback
    // for the old layout / explicit overrides.
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()));

    // app.db: prefer the binary's directory, fall back to APP_DIR (default ".").
    let db_path = exe_dir
        .as_ref()
        .map(|d| d.join("app.db"))
        .filter(|p| p.exists())
        .unwrap_or_else(|| args.app_dir.join("app.db"));
    info!("Loading database from {:?}", db_path);
    let db = db::AppDb::load(&db_path)
        .with_context(|| {
            format!(
                "Failed to open app.db at {:?} (looked next to the binary first, then in APP_DIR)",
                db_path
            )
        })?;
    info!(
        "Loaded {} CCTV channels, {} provincial channels, {} site rules",
        db.cctv_channels.len(),
        db.channels.len(),
        db.site_rules.len()
    );

    let jstv_auth_enabled = db.jstv_auth_enabled;

    // Native libs: delib / media_utils / headless-shell also prefer the binary's
    // directory; `chrome/` inside APP_DIR is kept as a fallback for old layouts.
    let chrome_dir = args.app_dir.join("chrome");

    let mut lib_dirs: Vec<&Path> = Vec::new();
    if let Some(d) = &exe_dir {
        lib_dirs.push(d.as_path());
    }
    lib_dirs.push(chrome_dir.as_path());

    let native = native::NativeLibs::load(&lib_dirs, jstv_auth_enabled);
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

    let chrome_path = args
        .chrome_path
        .unwrap_or_else(|| resolve_chrome_path(exe_dir.as_deref(), &args.app_dir));
    info!("Chrome path: {}", chrome_path);

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
