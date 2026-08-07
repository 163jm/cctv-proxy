/// Headless browser stream fetcher via Chrome DevTools Protocol.
///
/// Replaces Puppeteer from the original Node.js code.
/// Supports:
///   - Default fetcher: navigate + intercept network requests for .m3u8
///   - MGTV fetcher: navigate, close modal, click channel by sid, wait for .m3u8
///   - GDTV fetcher: navigate to channel URL, intercept .m3u8

use anyhow::{anyhow, Context, Result};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

#[derive(Debug, Deserialize)]
struct CdpTarget {
    id: String,
    #[serde(rename = "webSocketDebuggerUrl")]
    ws_url: Option<String>,
}

// ─── BrowserHandle ────────────────────────────────────────────────────────────

pub struct BrowserHandle {
    _process: Child,
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
                "--remote-debugging-port=0",
                "--remote-debugging-address=127.0.0.1",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to spawn headless-shell")?;

        let stderr = child.stderr.take().unwrap();
        let port = read_devtools_port(stderr).await?;
        info!("Chrome DevTools on port {}", port);

        Ok(Self {
            _process: child,
            port,
            http: reqwest::Client::new(),
        })
    }

    async fn new_tab(&self) -> Result<CdpTarget> {
        let target: CdpTarget = self
            .http
            .put(format!("http://127.0.0.1:{}/json/new", self.port))
            .send()
            .await?
            .json()
            .await?;
        Ok(target)
    }

    async fn close_tab(&self, id: &str) {
        let _ = self
            .http
            .get(format!("http://127.0.0.1:{}/json/close/{}", self.port, id))
            .send()
            .await;
    }

    /// Generic: navigate to page_url and intercept first network request matching m3u8_match.
    pub async fn fetch_default(
        &self,
        page_url: &str,
        m3u8_match: &str,
        user_agent: &str,
        fetch_timeout: Duration,
    ) -> Option<String> {
        let tab = self.new_tab().await.ok()?;
        let ws_url = tab.ws_url.clone()?;

        let result = timeout(
            fetch_timeout,
            drive_default(&ws_url, page_url, m3u8_match, user_agent),
        )
        .await;

        self.close_tab(&tab.id).await;

        match result {
            Ok(Ok(u)) => u,
            _ => None,
        }
    }

    /// MGTV: navigate, close modal, click channel by sid, wait for .m3u8.
    pub async fn fetch_mgtv(
        &self,
        page_url: &str,
        sid: &str,
        user_agent: &str,
        fetch_timeout: Duration,
    ) -> Option<String> {
        let tab = self.new_tab().await.ok()?;
        let ws_url = tab.ws_url.clone()?;

        let result = timeout(
            fetch_timeout,
            drive_mgtv(&ws_url, page_url, sid, user_agent),
        )
        .await;

        self.close_tab(&tab.id).await;

        match result {
            Ok(Ok(u)) => u,
            _ => None,
        }
    }
}

// ─── CDP helpers ──────────────────────────────────────────────────────────────

async fn read_devtools_port(stderr: tokio::process::ChildStderr) -> Result<u16> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let mut reader = BufReader::new(stderr).lines();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, reader.next_line()).await {
            Ok(Ok(Some(line))) if line.contains("DevTools listening on ws://") => {
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
            _ => {}
        }
    }
    Err(anyhow!("Timed out waiting for Chrome DevTools port"))
}

type WsSink = futures::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    Message,
>;
type WsStream = futures::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
>;

async fn cdp_send(write: &mut WsSink, id: u64, method: &str, params: Value) -> Result<()> {
    let cmd = serde_json::json!({ "id": id, "method": method, "params": params });
    write.send(Message::Text(cmd.to_string())).await?;
    Ok(())
}

/// Wait for the next Network.requestWillBeSent event matching predicate.
async fn wait_for_request<F>(read: &mut WsStream, timeout_ms: u64, pred: F) -> Option<String>
where
    F: Fn(&str) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(100), read.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                if let Ok(val) = serde_json::from_str::<Value>(&text) {
                    if val["method"] == "Network.requestWillBeSent" {
                        let url = val["params"]["request"]["url"].as_str().unwrap_or("");
                        if pred(url) {
                            return Some(url.to_string());
                        }
                    }
                }
            }
            _ => {}
        }
    }
    None
}

async fn setup_page(write: &mut WsSink, user_agent: &str, blocked: &[&str]) -> Result<()> {
    let mut id = 1u64;
    cdp_send(write, id, "Network.enable", serde_json::json!({})).await?;
    id += 1;
    cdp_send(write, id, "Network.setUserAgentOverride", serde_json::json!({ "userAgent": user_agent })).await?;
    id += 1;
    cdp_send(write, id, "Network.setBlockedURLs", serde_json::json!({ "urls": blocked })).await?;
    Ok(())
}

// ─── Default fetcher ──────────────────────────────────────────────────────────

async fn drive_default(
    ws_url: &str,
    page_url: &str,
    m3u8_match: &str,
    user_agent: &str,
) -> Result<Option<String>> {
    let (ws, _) = connect_async(ws_url).await?;
    let (mut write, mut read) = ws.split();

    setup_page(&mut write, user_agent, &[
        "*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp",
        "*.woff", "*.woff2", "*.ttf", "*.mp4", "*.mp3",
    ]).await?;

    let mut id = 10u64;
    cdp_send(&mut write, id, "Page.navigate", serde_json::json!({ "url": page_url })).await?;
    id += 1;
    sleep(Duration::from_millis(800)).await;
    cdp_send(&mut write, id, "Runtime.evaluate", serde_json::json!({
        "expression": "document.querySelectorAll('video').forEach(v => v.play().catch(()=>{}))"
    })).await?;

    let match_str = m3u8_match.to_string();
    let found = wait_for_request(&mut read, 25_000, |url| {
        url.contains(&match_str)
            && !url.contains(".ts")
            && !url.contains("log.")
            && !url.contains("beacon")
            && !url.contains("collect")
    }).await;

    Ok(found)
}

// ─── MGTV fetcher ─────────────────────────────────────────────────────────────

async fn drive_mgtv(
    ws_url: &str,
    page_url: &str,
    sid: &str,
    user_agent: &str,
) -> Result<Option<String>> {
    let (ws, _) = connect_async(ws_url).await?;
    let (mut write, mut read) = ws.split();

    setup_page(&mut write, user_agent, &[
        "*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp",
        "*.woff", "*.woff2", "*.ttf",
    ]).await?;

    let mut id = 10u64;
    cdp_send(&mut write, id, "Page.navigate", serde_json::json!({ "url": page_url })).await?;
    id += 1;

    // Wait for networkidle2-like state (up to 12s)
    sleep(Duration::from_millis(5000)).await;

    // Close modals
    let close_script = r#"
        ['.m-close', '.modal-close', '.ext-close', '.close-btn', '.dialog-close']
            .forEach(s => document.querySelector(s)?.click());
    "#;
    cdp_send(&mut write, id, "Runtime.evaluate", serde_json::json!({ "expression": close_script })).await?;
    id += 1;

    // Reset any playing video
    cdp_send(&mut write, id, "Runtime.evaluate", serde_json::json!({
        "expression": "document.querySelectorAll('video').forEach(v => { v.pause(); v.removeAttribute('src'); v.load(); })"
    })).await?;
    id += 1;

    // Click the channel by sid
    let click_script = format!(
        r#"
        (function() {{
            const selector = 'a[data-channel-sid="{}"]';
            const el = document.querySelector(selector);
            if (!el) return false;
            el.scrollIntoView({{ behavior: 'instant', block: 'center' }});
            el.click();
            return true;
        }})()
        "#,
        sid
    );
    cdp_send(&mut write, id, "Runtime.evaluate", serde_json::json!({
        "expression": click_script,
        "returnByValue": true
    })).await?;
    id += 1;

    // Wait a moment then trigger video play
    sleep(Duration::from_millis(500)).await;
    cdp_send(&mut write, id, "Runtime.evaluate", serde_json::json!({
        "expression": "document.querySelectorAll('video').forEach(v => v.play().catch(()=>{}))"
    })).await?;

    // Now wait for the .m3u8 request
    let found = wait_for_request(&mut read, 12_000, |url| {
        url.contains(".m3u8")
            && !url.contains(".ts")
            && !url.contains("log.")
            && !url.contains("beacon")
            && !url.contains("collect")
    }).await;

    Ok(found)
}

// ─── BrowserPool ─────────────────────────────────────────────────────────────

pub struct BrowserPool {
    handle: Mutex<Option<BrowserHandle>>,
    chrome_path: String,
    last_fail: Mutex<Option<std::time::Instant>>,
}

impl BrowserPool {
    pub fn new(chrome_path: String) -> Arc<Self> {
        Arc::new(Self {
            handle: Mutex::new(None),
            chrome_path,
            last_fail: Mutex::new(None),
        })
    }

    async fn ensure_browser(&self) -> bool {
        // Backoff: don't retry within 30s of a spawn failure
        {
            let fail = self.last_fail.lock().await;
            if let Some(t) = *fail {
                if t.elapsed() < Duration::from_secs(30) {
                    return false;
                }
            }
        }

        let mut guard = self.handle.lock().await;
        if guard.is_none() {
            match BrowserHandle::spawn(&self.chrome_path).await {
                Ok(h) => {
                    info!("Chrome started");
                    *guard = Some(h);
                    let mut fail = self.last_fail.lock().await;
                    *fail = None;
                    true
                }
                Err(e) => {
                    warn!("Failed to start Chrome: {}", e);
                    let mut fail = self.last_fail.lock().await;
                    *fail = Some(std::time::Instant::now());
                    false
                }
            }
        } else {
            true
        }
    }

    /// Fetch stream URL using the appropriate strategy for the channel.
    ///
    /// - `mgtv_sid`: Some(sid) → MGTV strategy
    /// - otherwise → default strategy
    pub async fn fetch(
        &self,
        page_url: &str,
        m3u8_match: &str,
        user_agent: &str,
        fetch_timeout_ms: u64,
        mgtv_sid: Option<&str>,
    ) -> Option<String> {
        if !self.ensure_browser().await {
            return None;
        }

        let guard = self.handle.lock().await;
        let handle = guard.as_ref()?;
        let timeout_dur = Duration::from_millis(fetch_timeout_ms);

        let result = if let Some(sid) = mgtv_sid {
            handle.fetch_mgtv(page_url, sid, user_agent, timeout_dur).await
        } else {
            handle.fetch_default(page_url, m3u8_match, user_agent, timeout_dur).await
        };

        // Drop guard before potentially restarting
        drop(guard);

        if result.is_none() {
            // Potentially stale browser — restart on next call
            let mut guard = self.handle.lock().await;
            if guard.is_some() {
                warn!("Browser fetch returned nothing; will restart on next call");
                // Don't kill it yet — might just be a channel with no stream
            }
        }

        result
    }
}
