/// Provincial TV proxy handlers.
use crate::{
    browser::{BrowserPool, MgtvOpts},
    db::Channel,
    m3u8::{self, is_auth_error},
    state::AppState,
};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
    Extension,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{info, warn};

/// GET /live.m3u
pub async fn live_m3u(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let host = host_from_headers(&headers, "localhost:3000");
    let mut out = "#EXTM3U\n".to_string();

    let mut channels: Vec<_> = state.channels.values().collect();
    channels.sort_by(|a, b| {
        a.group_name.cmp(&b.group_name).then(a.name.cmp(&b.name))
    });

    for ch in channels {
        let group = ch.group_name.as_deref().unwrap_or("其他");
        out.push_str(&m3u8::m3u_entry(&ch.id, &ch.name, group, &host));
    }

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "audio/x-mpegurl; charset=utf-8"),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        out,
    )
}

/// GET /{id}/playlist.m3u8
pub async fn proxy_playlist(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
    Extension(browser): Extension<Arc<BrowserPool>>,
) -> impl IntoResponse {
    let host = host_from_headers(&headers, "localhost:3000");

    let channel = match state.channels.get(&channel_id) {
        Some(c) => c.clone(),
        None => return (StatusCode::NOT_FOUND, "Channel not found").into_response(),
    };

    for attempt in 0..=1 {
        let stream_url = match ensure_stream_url(&state, &browser, &channel_id, &channel).await {
            Some(u) => u,
            None => break,
        };

        match fetch_and_rewrite_m3u8(&state, &stream_url, &host, &channel_id).await {
            Ok(content) => {
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")
                    .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
                    .body(Body::from(content))
                    .unwrap();
            }
            Err(e) => {
                warn!("{} M3U8 fetch failed (attempt {}): {}", channel_id, attempt, e);
                invalidate_channel(&state, &channel_id, &channel.url);
            }
        }
    }

    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")],
        "Temporary unavailable",
    ).into_response()
}

/// GET /{id}/segment/{encoded}
pub async fn proxy_segment(
    State(state): State<AppState>,
    Path((channel_id, encoded)): Path<(String, String)>,
) -> impl IntoResponse {
    proxy_resource(&state, &channel_id, &encoded, "video/mp2t", "public, max-age=8").await
}

/// GET /{id}/key/{encoded}
pub async fn proxy_key(
    State(state): State<AppState>,
    Path((channel_id, encoded)): Path<(String, String)>,
) -> impl IntoResponse {
    proxy_resource(&state, &channel_id, &encoded, "application/octet-stream", "no-cache").await
}

async fn proxy_resource(
    state: &AppState,
    channel_id: &str,
    encoded: &str,
    content_type: &str,
    cache_control: &str,
) -> Response {
    let url = match decode_url(encoded) {
        Ok(u) => u,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid URL").into_response(),
    };

    // Check segment cache
    if let Some(cached) = state.segment_cache.get(&url) {
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(header::CACHE_CONTROL, cache_control)
            .body(Body::from(cached))
            .unwrap();
    }

    let extra_headers = state.referer_headers(channel_id, &url);
    let mut req = state.http.get(&url).header("Accept-Encoding", "identity");
    for (k, v) in &extra_headers {
        req = req.header(k.as_str(), v.as_str());
    }

    let resp = match timeout(Duration::from_millis(state.proxy_timeout_ms), req.send()).await {
        Ok(Ok(r)) => r,
        _ => return (StatusCode::BAD_GATEWAY, "Upstream timeout").into_response(),
    };

    let status = resp.status().as_u16();
    if is_auth_error(status, "") {
        invalidate_channel(state, channel_id, "");
        return (StatusCode::FORBIDDEN, "Token expired").into_response();
    }

    let data = match resp.bytes().await {
        Ok(b) => b,
        Err(_) => return (StatusCode::BAD_GATEWAY, "Read error").into_response(),
    };

    state.segment_cache.set(url, data.clone());

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .header(header::CACHE_CONTROL, cache_control)
        .body(Body::from(data))
        .unwrap()
}

/// GET /admin/cache
pub async fn admin_cache(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.stream_cache.stats())
}

/// POST /admin/poller/refresh
pub async fn admin_refresh(State(state): State<AppState>) -> impl IntoResponse {
    // Wake the background poller immediately (previously this was a no-op stub).
    state.refresh.notify_one();
    (StatusCode::OK, "Polling triggered")
}

// ─── Core: ensure valid stream URL ───────────────────────────────────────────

pub async fn ensure_stream_url(
    state: &AppState,
    browser: &BrowserPool,
    channel_id: &str,
    channel: &Channel,
) -> Option<String> {
    // 1. JSTV native signing (js_, zj_, sd_, sh_)
    // Original JS: generateSignedUrl(channelId) — passes the full channelId, NOT jstv_id
    if state.is_signed_channel(channel_id) {
        if let Some(ref signer) = state.native.signer {
            if let Some(url) = signer.get_signed_url(channel_id) {
                state.stream_cache.set(channel_id, &channel.url, &url, "native_signer");
                let ch_state = state.get_or_create_channel_state(channel_id);
                let mut s = ch_state.lock().await;
                s.stream_url = Some(url.clone());
                s.stream_url_fetched_at = Some(Instant::now());
                return Some(url);
            }
        }
        return None;
    }

    // 2. Check in-memory channel state
    {
        let ch_state = state.get_or_create_channel_state(channel_id);
        let s = ch_state.lock().await;
        if let Some(ref url) = s.stream_url {
            return Some(url.clone());
        }
        if s.fetching {
            drop(s);
            sleep_ms(300).await;
            let s2 = ch_state.lock().await;
            return s2.stream_url.clone();
        }
    }

    // 3. Check persistent stream cache (with liveness probe)
    if let Some(cached_url) = state.stream_cache.get(channel_id, &channel.url) {
        if probe_url(state, channel_id, &cached_url).await {
            let ch_state = state.get_or_create_channel_state(channel_id);
            let mut s = ch_state.lock().await;
            s.stream_url = Some(cached_url.clone());
            s.stream_url_fetched_at = Some(Instant::now());
            return Some(cached_url);
        }
        state.stream_cache.invalidate(channel_id, &channel.url);
    }

    // 4. Browser fetch (serialised by semaphore — only 1 at a time globally)
    let _permit = state.browser_sem.acquire().await.ok()?;

    // Triple-check after acquiring semaphore (another task may have finished)
    {
        let ch_state = state.get_or_create_channel_state(channel_id);
        let s = ch_state.lock().await;
        if let Some(ref url) = s.stream_url {
            return Some(url.clone());
        }
        if s.fetching {
            // Still fetching even with semaphore held — wait it out
            drop(s);
            sleep_ms(500).await;
            let s2 = ch_state.lock().await;
            return s2.stream_url.clone();
        }
    }
    // Also re-check stream cache after acquiring semaphore
    if let Some(cached_url) = state.stream_cache.get(channel_id, &channel.url) {
        if probe_url(state, channel_id, &cached_url).await {
            let ch_state = state.get_or_create_channel_state(channel_id);
            let mut s = ch_state.lock().await;
            s.stream_url = Some(cached_url.clone());
            return Some(cached_url);
        }
        state.stream_cache.invalidate(channel_id, &channel.url);
    }

    {
        let ch_state = state.get_or_create_channel_state(channel_id);
        let mut s = ch_state.lock().await;
        s.fetching = true;
    }

    let stream_url = do_browser_fetch(state, browser, channel_id, channel).await;

    {
        let ch_state = state.get_or_create_channel_state(channel_id);
        let mut s = ch_state.lock().await;
        s.fetching = false;
        if let Some(ref url) = stream_url {
            s.stream_url = Some(url.clone());
            s.stream_url_fetched_at = Some(Instant::now());
        }
    }

    if let Some(ref url) = stream_url {
        state.stream_cache.set(channel_id, &channel.url, url, "browser");
        info!("[{}] Browser got stream: {}", channel_id, url);
    } else {
        warn!("[{}] Browser failed to get stream", channel_id);
    }

    stream_url
}

async fn do_browser_fetch(
    state: &AppState,
    browser: &BrowserPool,
    channel_id: &str,
    channel: &Channel,
) -> Option<String> {
    let rule = state.site_rule_for(channel_id);

    // DOM filter: site-specific rule overrides the global one (mirrors original
    // setupPageFilters: rule?.dom_filter || GLOBAL_DOM_FILTER).
    let dom_filter: String = rule
        .and_then(|r| r.dom_filter.clone())
        .unwrap_or_else(|| state.global_dom_filter.to_string());
    let blocked: Vec<String> = state.blocked_domains.iter().cloned().collect();

    let page_url = rule
        .and_then(|r| r.target_url.clone())
        .unwrap_or_else(|| channel.url.clone());
    let m3u8_match = rule
        .and_then(|r| r.m3u8_match.clone())
        .unwrap_or_else(|| ".m3u8".to_string());

    // MGTV: navigate live page, close modals, click channel by sid. Selectors
    // come from site_rules.selectors (JSON), falling back to original defaults.
    if channel_id.starts_with("mg_") {
        let sid = channel.sid.as_deref()?;
        let (close_selectors, item_template) = mgtv_selectors(rule);
        let opts = MgtvOpts {
            sid,
            close_selectors: &close_selectors,
            item_template: &item_template,
        };
        info!("[{}] MGTV browser fetch sid={} (selectors from DB)", channel_id, sid);
        return browser
            .fetch(
                &page_url,
                ".m3u8",
                &state.user_agent,
                state.fetch_timeout_ms,
                &dom_filter,
                &blocked,
                Some(opts),
                false,
            )
            .await;
    }

    // GDTV (广东台): dedicated channel-detail-page fetch (mirrors gd_ action_script).
    if channel_id.starts_with("gd_") {
        info!("[{}] GDTV browser fetch: {}", channel_id, page_url);
        return browser
            .fetch(
                &page_url,
                &m3u8_match,
                &state.user_agent,
                state.fetch_timeout_ms,
                &dom_filter,
                &blocked,
                None,
                true,
            )
            .await;
    }

    // All other channels (hb_ 河北 etc.): default fetch (navigate + intercept .m3u8)
    info!("[{}] Default browser fetch: {}", channel_id, page_url);
    browser
        .fetch(
            &page_url,
            &m3u8_match,
            &state.user_agent,
            state.fetch_timeout_ms,
            &dom_filter,
            &blocked,
            None,
            false,
        )
        .await
}

/// Parse site_rules.selectors (JSON) for MGTV: `close_modal` selector list and
/// `channel_item` template. Falls back to the original defaults if unset/invalid.
fn mgtv_selectors(rule: Option<&crate::db::SiteRule>) -> (Vec<String>, String) {
    let default_close = [
        ".m-close", ".modal-close", ".ext-close", ".close-btn", ".dialog-close",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect::<Vec<_>>();
    let default_item = "a[data-channel-sid=\"{sid}\"]".to_string();

    let Some(selectors) = rule.and_then(|r| r.selectors.as_deref()) else {
        return (default_close, default_item);
    };
    let Ok(val) = serde_json::from_str::<serde_json::Value>(selectors) else {
        return (default_close, default_item);
    };

    let close = val
        .get("close_modal")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|s| s.as_str().map(|s| s.to_string()))
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default_close.clone());

    let item = val
        .get("channel_item")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or(default_item);

    (close, item)
}

fn invalidate_channel(state: &AppState, channel_id: &str, url: &str) {
    state.stream_cache.invalidate(channel_id, url);
    let ch_state = state.get_or_create_channel_state(channel_id);
    if let Ok(mut s) = ch_state.try_lock() {
        s.stream_url = None;
    }
    state.m3u8_cache.remove(&format!("{}:", channel_id));
}

async fn probe_url(state: &AppState, channel_id: &str, url: &str) -> bool {
    let extra = state.referer_headers(channel_id, url);
    let mut req = state.http.get(url);
    for (k, v) in &extra {
        req = req.header(k.as_str(), v.as_str());
    }
    match timeout(Duration::from_secs(8), req.send()).await {
        Ok(Ok(r)) if r.status().is_success() => {
            // Mirror the original JS: a cached stream URL is only valid if the
            // body actually looks like an M3U8 playlist (a 2xx HTML error/auth
            // page must not be accepted).
            match timeout(Duration::from_secs(8), r.text()).await {
                Ok(Ok(text)) => {
                    let t = text.trim_start();
                    t.starts_with("#EXTM3U") || t.starts_with("#EXT-X-")
                }
                _ => false,
            }
        }
        _ => false,
    }
}

async fn fetch_and_rewrite_m3u8(
    state: &AppState,
    stream_url: &str,
    proxy_host: &str,
    channel_id: &str,
) -> anyhow::Result<String> {
    let cache_key = format!("{}:{}", channel_id, stream_url);
    if let Some(cached) = state.m3u8_cache.get(&cache_key) {
        return Ok(cached);
    }

    let extra_headers = state.referer_headers(channel_id, stream_url);
    let mut req = state
        .http
        .get(stream_url)
        .header("Cache-Control", "no-cache")
        .header("Pragma", "no-cache");
    for (k, v) in &extra_headers {
        req = req.header(k.as_str(), v.as_str());
    }

    let resp = req.send().await?;
    let status = resp.status().as_u16();
    let text = resp.text().await?;

    if is_auth_error(status, &text) {
        return Err(anyhow::anyhow!("Auth error: HTTP {}", status));
    }
    if status < 200 || status >= 300 {
        return Err(anyhow::anyhow!("HTTP {}", status));
    }

    let result = m3u8::rewrite_playlist(&text, stream_url, proxy_host, channel_id, "segment");
    state.m3u8_cache.set(cache_key, result.clone());
    Ok(result)
}

// ─── Background poller ────────────────────────────────────────────────────────

pub async fn run_poller(state: AppState, browser: Arc<BrowserPool>) {
    // Initial delay — let the server warm up first
    tokio::time::sleep(Duration::from_secs(30)).await;

    loop {
        info!("[Poller] Starting provincial channel re-check");

        let channel_ids: Vec<String> = state.channels.keys().cloned().collect();
        for cid in &channel_ids {
            let ch = match state.channels.get(cid) {
                Some(c) => c.clone(),
                None => continue,
            };

            // Skip unpolled channels (is_polled=0)
            if let Some(rule) = state.site_rule_for(cid) {
                if !rule.is_polled {
                    continue;
                }
            }

            // Skip signed channels (generate on demand)
            if state.is_signed_channel(cid) {
                continue;
            }

            // Check if existing cache entry is still alive
            if let Some(cached_url) = state.stream_cache.get(cid, &ch.url) {
                if probe_url(&state, cid, &cached_url).await {
                    // Extend TTL
                    state.stream_cache.set(cid, &ch.url, &cached_url, "poller_refresh");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                state.stream_cache.invalidate(cid, &ch.url);
                let ch_state = state.get_or_create_channel_state(cid);
                let mut s = ch_state.lock().await;
                s.stream_url = None;
                drop(s);
            }

            // Re-fetch via browser
            let _ = ensure_stream_url(&state, &browser, cid, &ch).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        state.stream_cache.cleanup_expired();
        info!("[Poller] Done; waiting {}ms or manual refresh", state.poll_interval_ms);
        tokio::select! {
            _ = state.refresh.notified() => {
                info!("[Poller] Manual refresh triggered via /admin/poller/refresh");
            }
            _ = tokio::time::sleep(Duration::from_millis(state.poll_interval_ms)) => {}
        }
    }
}

// ─── helpers ─────────────────────────────────────────────────────────────────

fn host_from_headers(headers: &HeaderMap, fallback: &str) -> String {
    headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(fallback)
        .to_string()
}

fn decode_url(encoded: &str) -> anyhow::Result<String> {
    let bytes = URL_SAFE_NO_PAD.decode(encoded.as_bytes())?;
    Ok(String::from_utf8(bytes)?)
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}
