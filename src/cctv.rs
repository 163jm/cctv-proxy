/// CCTV proxy handlers.
///
/// Routes:
///   GET /live.m3u          → M3U playlist of all CCTV channels
///   GET /{id}/playlist.m3u8 → Fetch & rewrite upstream M3U8
///   GET /{id}/ts/{encoded}  → Proxy (and optionally decrypt) a .ts segment
use crate::{m3u8, state::AppState};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use bytes::Bytes;
use tracing::{debug, warn};

const CCTV_REFERER: &str = "https://tv.cctv.com/";
const CCTV_ORIGIN: &str = "https://tv.cctv.com";
const TIMEOUT_SECS: u64 = 6;

/// GET /cctv/live.m3u  — full channel list
pub async fn live_m3u(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let host = host_from_headers(&headers, "localhost:3000");
    let mut out = "#EXTM3U\n".to_string();

    let mut channels: Vec<_> = state.cctv_channels.values().collect();
    channels.sort_by_key(|c| c.sort_order);

    for ch in channels {
        out.push_str(&m3u8::m3u_entry(&ch.id, &ch.name, "CCTV", &host));
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "audio/x-mpegurl; charset=utf-8"),
         (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")],
        out,
    )
}

/// GET /{id}/playlist.m3u8
pub async fn proxy_playlist(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let host = host_from_headers(&headers, "localhost:3000");

    let ch = match state.cctv_channels.get(&channel_id) {
        Some(c) => c.clone(),
        None => return (StatusCode::NOT_FOUND, "Not Found").into_response(),
    };

    // Try primary URL, then fallback
    let candidates: Vec<String> = [
        Some(m3u8::fix_cctv_url(&ch.m3u8_url)),
        ch.fallback_url.as_deref().map(m3u8::fix_cctv_url),
    ]
    .into_iter()
    .flatten()
    .collect::<std::collections::LinkedList<_>>()
    .into_iter()
    .collect();

    for url in &candidates {
        let text = match fetch_text(&state, url).await {
            Some(t) => t,
            None => continue,
        };

        // Resolve master → media playlist if needed
        let (final_url, final_text) = if text.contains("#EXT-X-STREAM-INF")
            && !text.contains("#EXTINF")
        {
            if let Some(sub) = m3u8::select_highest_bandwidth(&text, url) {
                match fetch_text(&state, &sub).await {
                    Some(t) => (sub, t),
                    None => continue,
                }
            } else {
                continue;
            }
        } else {
            (url.clone(), text)
        };

        if !final_text.contains("#EXTINF") && !final_text.contains(".ts") {
            continue;
        }

        let rewritten = m3u8::rewrite_playlist(&final_text, &final_url, &host, &channel_id);

        return (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/x-mpegURL"),
                (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            rewritten,
        )
            .into_response();
    }

    (StatusCode::BAD_GATEWAY, "Upstream error").into_response()
}

/// GET /{id}/ts/{encoded_url}
pub async fn proxy_ts(
    State(state): State<AppState>,
    Path((channel_id, encoded)): Path<(String, String)>,
) -> impl IntoResponse {
    let ts_url = match URL_SAFE_NO_PAD.decode(encoded.as_bytes()) {
        Ok(b) => match String::from_utf8(b) {
            Ok(s) => s,
            Err(_) => return (StatusCode::BAD_REQUEST, "Bad URL encoding").into_response(),
        },
        Err(_) => return (StatusCode::BAD_REQUEST, "Bad base64").into_response(),
    };

    let data = match fetch_bytes(&state, &ts_url).await {
        Some(d) => d,
        None => return (StatusCode::BAD_GATEWAY, "TS upstream error").into_response(),
    };

    // Attempt native decryption via delib.so
    let payload = if data.len() >= 188 {
        if let Some(ref dec) = state.native.decryptor {
            dec.decrypt(&data).map(Bytes::from).unwrap_or(data)
        } else {
            data
        }
    } else {
        data
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "video/mp2t")
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .header(header::CACHE_CONTROL, "public, max-age=5")
        .body(Body::from(payload))
        .unwrap()
}

// ─── helpers ─────────────────────────────────────────────────────────────────

fn host_from_headers(headers: &HeaderMap, fallback: &str) -> String {
    headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(fallback)
        .to_string()
}

async fn fetch_text(state: &AppState, url: &str) -> Option<String> {
    let resp = state
        .http
        .get(url)
        .timeout(std::time::Duration::from_secs(TIMEOUT_SECS))
        .header("Referer", CCTV_REFERER)
        .header("Origin", CCTV_ORIGIN)
        .header("Accept", "*/*")
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        return None;
    }
    resp.text().await.ok()
}

async fn fetch_bytes(state: &AppState, url: &str) -> Option<Bytes> {
    let resp = state
        .http
        .get(url)
        .timeout(std::time::Duration::from_secs(TIMEOUT_SECS))
        .header("Referer", CCTV_REFERER)
        .header("Origin", CCTV_ORIGIN)
        .header("Accept", "*/*")
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        return None;
    }
    resp.bytes().await.ok()
}
