/// Provincial TV proxy handlers.
///
/// Routes:
///   GET /live.m3u                        → M3U playlist of all provincial channels
///   GET /{id}/playlist.m3u8              → Serve (and auto-fetch) the real stream
///   GET /{id}/segment/{encoded}          → Proxy a media segment
///   GET /{id}/key/{encoded}              → Proxy an HLS encryption key
///   GET /admin/cache                     → Cache stats (JSON)
///   POST /admin/poller/refresh           → Trigger manual re-poll
use crate::{
    browser::BrowserPool,
    m3u8::{self, is_auth_error},
    state::AppState,
};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{debug, info, warn};

/// GET /live.m3u
pub async fn live_m3u(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let host = host_from_headers(&headers, "localhost:3000");
    let mut out = "#EXTM3U\n".to_string();

    for ch in state.channels.values() {
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
    axum::Extension(browser): axum::Extension<Arc<BrowserPool>>,
) -> impl IntoResponse {
    let host = host_from_headers(&headers, "localhost:3000");

    let channel = match state.channels.get(&channel_id) {
        Some(c) => c.clone(),
        None => return (StatusCode::NOT_FOUND, "Channel not found").into_response(),
    };

    // Attempt up to 2 times (to handle expired tokens)
    for attempt in 0..=1 {
        let stream_url = match ensure_stream_url(
            &state,
            &browser,
            &channel_id,
            &channel.url,
        )
        .await
        {
            Some(u) => u,
            None => break,
        };

        match fetch_and_rewrite_m3u8(&state, &stream_url, &host, &channel_id).await {
            Ok(content) => {
                let etag = format!("{:x}", md5::compute(&content));
                if headers
                    .get(header::IF_NONE_MATCH)
                    .and_then(|v| v.to_str().ok())
                    == Some(&etag)
                {
                    return StatusCode::NOT_MODIFIED.into_response();
                }
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")
                    .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
                    .header("ETag", etag)
                    .body(Body::from(content))
                    .unwrap();
            }
            Err(e) => {
                warn!("{} M3U8 fetch failed (attempt {}): {}", channel_id, attempt, e);
                // Invalidate cache and retry
                let ch_state = state.get_or_create_channel_state(&channel_id);
                let mut s = ch_state.lock().await;
                s.stream_url = None;
                drop(s);
                state.stream_cache.invalidate(&channel_id, &channel.url);
                state.m3u8_cache.remove(&format!("{}:{}", channel_id, stream_url));
            }
        }
    }

    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")],
        "Temporary unavailable",
    )
        .into_response()
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

    let resp = match timeout(
        Duration::from_millis(state.proxy_timeout_ms),
        req.send(),
    )
    .await
    {
        Ok(Ok(r)) => r,
        _ => return (StatusCode::BAD_GATEWAY, "Upstream timeout").into_response(),
    };

    let status = resp.status().as_u16();

    if is_auth_error(status, "") {
        // Token expired — invalidate channel stream URL
        let ch_state = state.get_or_create_channel_state(channel_id);
        let mut s = ch_state.lock().await;
        s.stream_url = None;
        drop(s);
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

/// POST /admin/poller/refresh  — no-op placeholder; poller runs autonomously
pub async fn admin_refresh() -> impl IntoResponse {
    (StatusCode::OK, "Polling triggered")
}

// ─── Core: ensure we have a valid stream URL ──────────────────────────────────

pub async fn ensure_stream_url(
    state: &AppState,
    browser: &BrowserPool,
    channel_id: &str,
    original_url: &str,
) -> Option<String> {
    // 1. Native signing (js_, zj_, sd_, sh_)
    if state.is_signed_channel(channel_id) {
        if let Some(signer) = &state.native.signer {
            if let Some(url) = signer.get_signed_url(channel_id) {
                state.stream_cache.set(channel_id, original_url, &url, "native_signer");
                let ch_state = state.get_or_create_channel_state(channel_id);
                let mut s = ch_state.lock().await;
                s.stream_url = Some(url.clone());
                s.stream_url_fetched_at = Some(Instant::now());
                return Some(url);
            }
        }
        return None;
    }

    // 2. Check per-channel state (in-memory)
    {
        let ch_state = state.get_or_create_channel_state(channel_id);
        let s = ch_state.lock().await;
        if let Some(ref url) = s.stream_url {
            return Some(url.clone());
        }
        if s.fetching {
            // Another task is fetching; wait briefly
            drop(s);
            tokio::time::sleep(Duration::from_millis(300)).await;
            let s2 = ch_state.lock().await;
            if let Some(ref url) = s2.stream_url {
                return Some(url.clone());
            }
            return None;
        }
    }

    // 3. Check persistent stream cache
    if let Some(cached_url) = state.stream_cache.get(channel_id, original_url) {
        // Quick liveness check
        let extra = state.referer_headers(channel_id, &cached_url);
        let mut req = state.http.get(&cached_url);
        for (k, v) in &extra {
            req = req.header(k.as_str(), v.as_str());
        }
        if let Ok(Ok(r)) =
            timeout(Duration::from_secs(8), req.send()).await
        {
            if r.status().is_success() {
                let ch_state = state.get_or_create_channel_state(channel_id);
                let mut s = ch_state.lock().await;
                s.stream_url = Some(cached_url.clone());
                s.stream_url_fetched_at = Some(Instant::now());
                return Some(cached_url);
            }
        }
        state.stream_cache.invalidate(channel_id, original_url);
    }

    // 4. Browser fetch (holds semaphore to limit concurrency)
    let _permit = state.browser_sem.acquire().await.ok()?;
    {
        let ch_state = state.get_or_create_channel_state(channel_id);
        let mut s = ch_state.lock().await;
        s.fetching = true;
    }

    let rule = state.site_rule_for(channel_id);
    let page_url = rule
        .and_then(|r| r.target_url.as_deref())
        .unwrap_or(original_url);
    let m3u8_match = rule
        .and_then(|r| r.m3u8_match.as_deref())
        .unwrap_or(".m3u8");

    info!("Browser fetching stream for {}", channel_id);
    let stream_url = browser
        .fetch(page_url, m3u8_match, &state.user_agent, state.fetch_timeout_ms)
        .await;

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
        state.stream_cache.set(channel_id, original_url, url, "browser");
        info!("Browser got stream for {}: {}", channel_id, url);
    } else {
        warn!("Browser failed to get stream for {}", channel_id);
    }

    stream_url
}

/// Fetch the real M3U8 and rewrite all segment URLs to go through our proxy.
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
    let interval = Duration::from_millis(state.poll_interval_ms);
    loop {
        tokio::time::sleep(interval).await;
        info!("[Poller] Starting provincial channel re-check");

        // Refresh all channels that have been cached (or need initial fetch)
        let channel_ids: Vec<String> = state.channels.keys().cloned().collect();
        for cid in &channel_ids {
            let ch = match state.channels.get(cid) {
                Some(c) => c.clone(),
                None => continue,
            };

            // Skip unpolled channels
            if let Some(rule) = state.site_rule_for(cid) {
                if !rule.is_polled {
                    continue;
                }
            }

            // Skip signed channels (they generate on demand)
            if state.is_signed_channel(cid) {
                continue;
            }

            // Check if existing cache entry is still valid
            if let Some(cached_url) = state.stream_cache.get(cid, &ch.url) {
                let extra = state.referer_headers(cid, &cached_url);
                let mut req = state.http.get(&cached_url);
                for (k, v) in &extra {
                    req = req.header(k.as_str(), v.as_str());
                }
                if let Ok(Ok(r)) = timeout(Duration::from_secs(5), req.send()).await {
                    if r.status().is_success() {
                        // Still alive; extend TTL
                        state.stream_cache.set(cid, &ch.url, &cached_url, "poller_refresh");
                        continue;
                    }
                }
                // Stale — invalidate
                state.stream_cache.invalidate(cid, &ch.url);
                let ch_state = state.get_or_create_channel_state(cid);
                let mut s = ch_state.lock().await;
                s.stream_url = None;
                drop(s);
            }

            // Re-fetch via browser
            let _url = ensure_stream_url(&state, &browser, cid, &ch.url).await;
            // Small delay between channels to avoid hammering Chrome
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        state.stream_cache.cleanup_expired();
        info!("[Poller] Done");
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
