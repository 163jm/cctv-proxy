/// Headless browser stream fetcher.
///
/// Uses Chrome DevTools Protocol via a subprocess + CDP JSON over HTTP/WebSocket.
/// This replaces the Puppeteer usage in the original Node.js code.
///
/// Strategy:
///   1. Spawn headless-shell with `--remote-debugging-port=0` (auto port).
///   2. Read the actual port from stderr.
///   3. Use the CDP `/json/new` endpoint to open a tab, navigate, intercept
///      network events, and wait for a matching `.m3u8` request URL.
///   4. Return the URL and close the tab (leaving the browser process alive
///      for the next call via a shared handle).
///
/// This approach avoids external crate dependencies beyond `reqwest` and `tokio`.

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use serde::Deserialize;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

#[derive(Debug, Deserialize)]
struct CdpTarget {
    id: String,
    #[serde(rename = "webSocketDebuggerUrl")]
    ws_url: Option<String>,
}

pub struct BrowserHandle {
    process: Child,
    port: u16,
    http: reqwest::Client,
}

impl BrowserHandle {
    pub async fn spawn(chrome_path: &str) -> Result<Self> {
        let mut child = Command::new(chrome_path)
            .args([
                "--headless",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--mute-audio",
                "--autoplay-policy=no-user-gesture-required",
                "--disable-blink-features=AutomationControlled",
                "--remote-debugging-port=0", // OS picks a free port
                "--remote-debugging-address=127.0.0.1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to spawn headless-shell")?;

        // Read stderr to find "DevTools listening on ws://127.0.0.1:{port}/..."
        let stderr = child.stderr.take().unwrap();
        let port = read_devtools_port(stderr).await?;
        info!("Chrome DevTools listening on port {}", port);

        Ok(Self {
            process: child,
            port,
            http: reqwest::Client::new(),
        })
    }

    /// Open a new tab, navigate to `url`, intercept network requests,
    /// return the first URL matching `m3u8_match`, then close the tab.
    pub async fn fetch_stream_url(
        &self,
        page_url: &str,
        m3u8_match: &str,
        user_agent: &str,
        fetch_timeout: Duration,
    ) -> Result<Option<String>> {
        // Create a new tab via CDP REST API
        let new_tab: CdpTarget = self
            .http
            .put(format!("http://127.0.0.1:{}/json/new?{}", self.port, page_url))
            .send()
            .await?
            .json()
            .await?;

        let ws_url = new_tab
            .ws_url
            .ok_or_else(|| anyhow!("No WebSocket URL for new tab"))?;

        let result = timeout(
            fetch_timeout,
            drive_tab(&ws_url, page_url, m3u8_match, user_agent),
        )
        .await;

        // Close the tab regardless of outcome
        let _ = self
            .http
            .get(format!(
                "http://127.0.0.1:{}/json/close/{}",
                self.port, new_tab.id
            ))
            .send()
            .await;

        match result {
            Ok(inner) => inner,
            Err(_) => Ok(None), // timeout
        }
    }
}

impl Drop for BrowserHandle {
    fn drop(&mut self) {
        let _ = self.process.start_kill();
    }
}

/// Read the DevTools port from the child's stderr stream.
async fn read_devtools_port(
    stderr: tokio::process::ChildStderr,
) -> Result<u16> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let mut reader = BufReader::new(stderr).lines();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, reader.next_line()).await {
            Ok(Ok(Some(line))) => {
                // e.g. "DevTools listening on ws://127.0.0.1:34521/..."
                if line.contains("DevTools listening on ws://") {
                    if let Some(port_str) = line.split(':').nth(3) {
                        let port: u16 = port_str
                            .split('/')
                            .next()
                            .unwrap_or("")
                            .parse()
                            .context("Invalid DevTools port")?;
                        return Ok(port);
                    }
                }
            }
            _ => break,
        }
    }
    Err(anyhow!("Timed out waiting for Chrome DevTools port"))
}

/// Drive a single tab via CDP WebSocket: navigate and intercept network requests.
async fn drive_tab(
    ws_url: &str,
    page_url: &str,
    m3u8_match: &str,
    user_agent: &str,
) -> Result<Option<String>> {
    let (ws, _) = connect_async(ws_url).await?;
    let (mut write, mut read) = ws.split();

    let mut id_counter: u64 = 1;
    let mut found_url: Option<String> = None;

    // Helper to send CDP commands
    macro_rules! cdp {
        ($method:expr, $params:tt) => {{
            let cmd = serde_json::json!({
                "id": id_counter,
                "method": $method,
                "params": $params
            });
            id_counter += 1;
            use futures::SinkExt;
            write
                .send(Message::Text(cmd.to_string()))
                .await
                .ok();
        }};
    }

    // Enable Network domain
    cdp!("Network.enable", {});
    // Set UA
    cdp!("Network.setUserAgentOverride", { "userAgent": user_agent });
    // Block images/fonts/media to speed things up
    cdp!("Network.setBlockedURLs", {
        "urls": ["*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp", "*.woff", "*.woff2", "*.ttf", "*.mp4", "*.mp3"]
    });
    // Navigate
    cdp!("Page.navigate", { "url": page_url });
    // Trigger autoplay
    sleep(Duration::from_millis(500)).await;
    cdp!("Runtime.evaluate", {
        "expression": "document.querySelectorAll('video').forEach(v => v.play().catch(()=>{}))"
    });

    // Listen for network request events
    let match_str = m3u8_match.to_string();
    for _ in 0..200 {
        // up to ~40s in 200ms steps
        match tokio::time::timeout(Duration::from_millis(200), read.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                    let method = val["method"].as_str().unwrap_or("");
                    if method == "Network.requestWillBeSent" {
                        let url = val["params"]["request"]["url"]
                            .as_str()
                            .unwrap_or("");
                        if url.contains(&match_str)
                            && !url.contains("ts")
                            && !url.contains("log.")
                            && !url.contains("beacon")
                            && !url.contains("collect")
                        {
                            found_url = Some(url.to_string());
                            break;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    Ok(found_url)
}

/// A long-lived browser pool that keeps one Chrome instance alive.
pub struct BrowserPool {
    handle: Mutex<Option<BrowserHandle>>,
    chrome_path: String,
}

impl BrowserPool {
    pub fn new(chrome_path: String) -> Arc<Self> {
        Arc::new(Self {
            handle: Mutex::new(None),
            chrome_path,
        })
    }

    /// Fetch a stream URL, starting the browser if needed.
    pub async fn fetch(
        &self,
        page_url: &str,
        m3u8_match: &str,
        user_agent: &str,
        fetch_timeout_ms: u64,
    ) -> Option<String> {
        let mut guard = self.handle.lock().await;

        // Lazily start Chrome
        if guard.is_none() {
            match BrowserHandle::spawn(&self.chrome_path).await {
                Ok(h) => {
                    info!("Chrome started");
                    *guard = Some(h);
                }
                Err(e) => {
                    warn!("Failed to start Chrome: {}", e);
                    return None;
                }
            }
        }

        let handle = guard.as_ref().unwrap();
        let timeout_dur = Duration::from_millis(fetch_timeout_ms);

        match handle
            .fetch_stream_url(page_url, m3u8_match, user_agent, timeout_dur)
            .await
        {
            Ok(url) => url,
            Err(e) => {
                warn!("Browser fetch error: {}", e);
                // Kill stale browser; will restart on next call
                *guard = None;
                None
            }
        }
    }
}
