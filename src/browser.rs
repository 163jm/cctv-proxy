/// Headless browser stream fetcher via Chrome DevTools Protocol.
///
/// Replaces Puppeteer from the original Node.js code.
/// Supports:
///   - Default fetcher: navigate + intercept network requests for .m3u8
///   - MGTV fetcher: navigate, close modal, click channel by sid, wait for .m3u8
///   - GDTV fetcher: navigate to channel URL, intercept .m3u8
///
/// Differences vs the original Puppeteer logic that were ported:
///   - Applies the site / global DOM filter script via
///     `Page.addScriptToEvaluateOnNewDocument` (original: setupPageFilters +
///     page.evaluateOnNewDocument). Without this, many sites show overlays or
///     never autoplay, so the .m3u8 request never fires.
///   - Blocks images/fonts/media plus every domain in `blocked_domains` from the
///     DB via `Network.setBlockedURLs` (original: request interception).
///   - Repeatedly nudges `<video>` elements to play while waiting for the
///     stream request (sites create the player lazily).

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

/// Static URL patterns always blocked (mirrors original request interception of
/// image / font / media resource types).
const BASE_BLOCKED_PATTERNS: &[&str] = &[
    "*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp",
    "*.woff", "*.woff2", "*.ttf", "*.eot",
    "*.mp4", "*.mp3", "*.aac", "*.ogg",
];

/// Autoplay nudge: mute + play every <video> element. Polled repeatedly because
/// players are often created lazily after the page settles.
const AUTOPLAY_SCRIPT: &str = r#"
    (function() {
        var n = 0;
        document.querySelectorAll('video').forEach(function(v) {
            try {
                v.muted = true;
                v.autoplay = true;
                var p = v.play();
                if (p && p.catch) p.catch(function(){});
                n++;
            } catch(e) {}
        });
        return n;
    })()
"#;

// ─── BrowserHandle ────────────────────────────────────────────────────────────

/// MGTV-specific fetch options, sourced from site_rules.selectors in the DB
/// (mirrors the original action_script which reads rule.selectors).
pub struct MgtvOpts<'a> {
    pub sid: &'a str,
    /// CSS selectors tried in order to close modal dialogs.
    pub close_selectors: &'a [String],
    /// Channel item selector template; "{sid}" is replaced with the channel sid.
    pub item_template: &'a str,
}

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
        dom_filter: &str,
        blocked: &[String],
    ) -> Option<String> {
        let tab = self.new_tab().await.ok()?;
        let ws_url = tab.ws_url.clone()?;

        let result = timeout(
            fetch_timeout,
            drive_default(&ws_url, page_url, m3u8_match, user_agent, dom_filter, blocked),
        )
        .await;

        self.close_tab(&tab.id).await;

        match result {
            Ok(Ok(u)) => u,
            _ => None,
        }
    }

    /// MGTV: navigate, close modal, click channel by sid, wait for .m3u8.
    /// `close_selectors` / `item_template` come from site_rules.selectors (DB),
    /// falling back to defaults in the caller.
    pub async fn fetch_mgtv(
        &self,
        page_url: &str,
        user_agent: &str,
        fetch_timeout: Duration,
        dom_filter: &str,
        blocked: &[String],
        opts: &MgtvOpts<'_>,
    ) -> Option<String> {
        let tab = self.new_tab().await.ok()?;
        let ws_url = tab.ws_url.clone()?;

        let result = timeout(
            fetch_timeout,
            drive_mgtv(
                &ws_url,
                page_url,
                user_agent,
                dom_filter,
                blocked,
                opts,
            ),
        )
        .await;

        self.close_tab(&tab.id).await;

        match result {
            Ok(Ok(u)) => u,
            _ => None,
        }
    }

    /// GDTV (广东台): navigate to the channel detail page and capture the first
    /// .m3u8 the player requests. Mirrors the original gd_ action_script
    /// (waitUntil networkidle2 + short wait loop), but with autoplay nudging to
    /// survive lazy players.
    pub async fn fetch_gdtv(
        &self,
        page_url: &str,
        m3u8_match: &str,
        user_agent: &str,
        fetch_timeout: Duration,
        dom_filter: &str,
        blocked: &[String],
    ) -> Option<String> {
        let tab = self.new_tab().await.ok()?;
        let ws_url = tab.ws_url.clone()?;

        let result = timeout(
            fetch_timeout,
            drive_gdtv(&ws_url, page_url, m3u8_match, user_agent, dom_filter, blocked),
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

fn is_wanted_stream_url(url: &str, match_str: &str) -> bool {
    url.contains(match_str)
        && !url.contains(".ts")
        && !url.contains("log.")
        && !url.contains("beacon")
        && !url.contains("collect")
}

/// Enable network, override UA, inject the DOM filter (site-specific or global)
/// and block images/fonts/media + DB blocked_domains.
async fn setup_page(
    write: &mut WsSink,
    user_agent: &str,
    dom_filter: &str,
    blocked: &[String],
) -> Result<()> {
    let mut id = 1u64;
    cdp_send(write, id, "Network.enable", serde_json::json!({})).await?;
    id += 1;
    cdp_send(
        write,
        id,
        "Network.setUserAgentOverride",
        serde_json::json!({ "userAgent": user_agent }),
    )
    .await?;
    id += 1;

    if !dom_filter.is_empty() {
        // Mirrors original page.evaluateOnNewDocument(dom_filter): runs the script
        // before any page JS, removing ads/overlays and disabling animations.
        cdp_send(
            write,
            id,
            "Page.addScriptToEvaluateOnNewDocument",
            serde_json::json!({ "source": dom_filter }),
        )
        .await?;
        id += 1;
    }

    let mut urls: Vec<String> = BASE_BLOCKED_PATTERNS
        .iter()
        .map(|s| s.to_string())
        .collect();
    for domain in blocked {
        if domain.trim().is_empty() {
            continue;
        }
        urls.push(format!("*{}*", domain));
        urls.push(format!("*://{}/*", domain));
        urls.push(format!("*://*.{}/*", domain));
    }
    cdp_send(write, id, "Network.setBlockedURLs", serde_json::json!({ "urls": urls })).await?;
    Ok(())
}

// ─── Default fetcher ──────────────────────────────────────────────────────────

async fn drive_default(
    ws_url: &str,
    page_url: &str,
    m3u8_match: &str,
    user_agent: &str,
    dom_filter: &str,
    blocked: &[String],
) -> Result<Option<String>> {
    drive_wait(ws_url, page_url, m3u8_match, user_agent, dom_filter, blocked, 25, true).await
}

/// GDTV (广东台): mirrors the original gd_ action_script — navigate to the
/// channel detail page, wait ~12s max for a .m3u8 request, nudging autoplay.
async fn drive_gdtv(
    ws_url: &str,
    page_url: &str,
    m3u8_match: &str,
    user_agent: &str,
    dom_filter: &str,
    blocked: &[String],
) -> Result<Option<String>> {
    drive_wait(ws_url, page_url, m3u8_match, user_agent, dom_filter, blocked, 12, true).await
}

/// Shared generic fetch: navigate, then loop reading network events until a
/// wanted .m3u8 request is seen or the deadline passes. Optionally nudges
/// `<video>` elements to play periodically (sites create players lazily).
async fn drive_wait(
    ws_url: &str,
    page_url: &str,
    m3u8_match: &str,
    user_agent: &str,
    dom_filter: &str,
    blocked: &[String],
    wait_secs: u64,
    autoplay: bool,
) -> Result<Option<String>> {
    let (ws, _) = connect_async(ws_url).await?;
    let (mut write, mut read) = ws.split();

    setup_page(&mut write, user_agent, dom_filter, blocked).await?;

    let mut id = 10u64;
    cdp_send(&mut write, id, "Page.navigate", serde_json::json!({ "url": page_url })).await?;
    id += 1;

    let match_str = m3u8_match.to_string();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_secs);
    let mut last_play = tokio::time::Instant::now() - Duration::from_secs(1);

    loop {
        // 1) Read any pending network events, return as soon as the stream URL appears.
        match tokio::time::timeout(Duration::from_millis(150), read.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                if let Ok(val) = serde_json::from_str::<Value>(&text) {
                    if val["method"] == "Network.requestWillBeSent" {
                        let url = val["params"]["request"]["url"].as_str().unwrap_or("");
                        if is_wanted_stream_url(url, &match_str) {
                            return Ok(Some(url.to_string()));
                        }
                    }
                }
            }
            _ => {}
        }

        if tokio::time::Instant::now() >= deadline {
            break;
        }

        // 2) Periodically nudge <video> elements to play (mirrors the original
        //    `v.play()` after networkidle0, but keeps retrying since players are
        //    often created after load).
        if autoplay && last_play.elapsed() >= Duration::from_millis(400) {
            last_play = tokio::time::Instant::now();
            let _ = cdp_send(
                &mut write,
                id,
                "Runtime.evaluate",
                serde_json::json!({ "expression": AUTOPLAY_SCRIPT }),
            )
            .await;
            id += 1;
        }
    }

    Ok(None)
}

// ─── MGTV fetcher ─────────────────────────────────────────────────────────────

async fn drive_mgtv(
    ws_url: &str,
    page_url: &str,
    user_agent: &str,
    dom_filter: &str,
    blocked: &[String],
    opts: &MgtvOpts<'_>,
) -> Result<Option<String>> {
    let (ws, _) = connect_async(ws_url).await?;
    let (mut write, mut read) = ws.split();

    setup_page(&mut write, user_agent, dom_filter, blocked).await?;

    let mut id = 10u64;
    cdp_send(&mut write, id, "Page.navigate", serde_json::json!({ "url": page_url })).await?;
    id += 1;

    // Wait for networkidle2-like state (up to 12s)
    sleep(Duration::from_millis(5000)).await;

    // Close modals (selectors come from site_rules.selectors.close_modal)
    let close_json = serde_json::to_string(opts.close_selectors).unwrap_or_else(|_| "[]".to_string());
    let close_script = format!(
        r#"
        (function() {{
            var sels = {};
            for (var i = 0; i < sels.length; i++) {{
                var el = document.querySelector(sels[i]);
                if (el) el.click();
            }}
        }})()
        "#,
        close_json
    );
    cdp_send(&mut write, id, "Runtime.evaluate", serde_json::json!({ "expression": close_script })).await?;
    id += 1;

    // Reset any playing video
    cdp_send(&mut write, id, "Runtime.evaluate", serde_json::json!({
        "expression": "document.querySelectorAll('video').forEach(v => { v.pause(); v.removeAttribute('src'); v.load(); })"
    })).await?;
    id += 1;

    // Click the channel by sid (selector template from site_rules.selectors.channel_item)
    let item_selector = opts.item_template.replace("{sid}", opts.sid);
    let click_script = format!(
        r#"
        (function() {{
            const selector = '{}';
            const el = document.querySelector(selector);
            if (!el) return false;
            el.scrollIntoView({{ behavior: 'instant', block: 'center' }});
            el.click();
            return true;
        }})()
        "#,
        item_selector.replace('\'', "\\'")
    );
    cdp_send(&mut write, id, "Runtime.evaluate", serde_json::json!({
        "expression": click_script,
        "returnByValue": true
    })).await?;
    id += 1;

    // Wait a moment then trigger video play
    sleep(Duration::from_millis(500)).await;
    cdp_send(&mut write, id, "Runtime.evaluate", serde_json::json!({
        "expression": AUTOPLAY_SCRIPT
    })).await?;

    // Now wait for the .m3u8 request
    let found = wait_for_request(&mut read, 12_000, |url| {
        is_wanted_stream_url(url, ".m3u8")
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
    /// - `mgtv`: Some(opts) → MGTV strategy (close modals + click channel by sid)
    /// - `gdtv`: true → GDTV strategy (navigate channel detail page, 12s cap)
    /// - otherwise → default strategy (25s cap, autoplay nudge)
    /// - `dom_filter`: site-specific DOM filter script, or the global one
    /// - `blocked`: extra domains to block from the DB (blocked_domains)
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch(
        &self,
        page_url: &str,
        m3u8_match: &str,
        user_agent: &str,
        fetch_timeout_ms: u64,
        dom_filter: &str,
        blocked: &[String],
        mgtv: Option<MgtvOpts<'_>>,
        gdtv: bool,
    ) -> Option<String> {
        if !self.ensure_browser().await {
            return None;
        }

        let guard = self.handle.lock().await;
        let handle = guard.as_ref()?;
        let timeout_dur = Duration::from_millis(fetch_timeout_ms);

        let result = if let Some(opts) = mgtv {
            handle
                .fetch_mgtv(page_url, user_agent, timeout_dur, dom_filter, blocked, &opts)
                .await
        } else if gdtv {
            handle
                .fetch_gdtv(page_url, m3u8_match, user_agent, timeout_dur, dom_filter, blocked)
                .await
        } else {
            handle
                .fetch_default(page_url, m3u8_match, user_agent, timeout_dur, dom_filter, blocked)
                .await
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
