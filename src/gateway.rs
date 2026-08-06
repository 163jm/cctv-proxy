/// Gateway: the unified public-facing router.
use crate::{cctv, m3u8, provincial, state::AppState, browser::BrowserPool};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Router,
};
use std::sync::Arc;

pub fn router(state: AppState, browser: Arc<BrowserPool>) -> Router {
    Router::new()
        .route("/", get(merged_m3u))
        .route("/live.m3u", get(merged_m3u))
        .route("/cctv/live.m3u", get(cctv::live_m3u))
        .route("/:id/playlist.m3u8", get(smart_playlist))
        // /ts/ → CCTV (with CCTV Referer/Origin headers)
        .route("/:id/ts/:encoded", get(cctv_ts_or_provincial_segment))
        // /key/ → smart: CCTV keys need CCTV Referer too
        .route("/:id/key/:encoded", get(smart_key))
        // /segment/ → provincial
        .route("/:id/segment/:encoded", get(provincial::proxy_segment))
        .route("/admin/cache", get(provincial::admin_cache))
        .route("/admin/poller/refresh", post(provincial::admin_refresh))
        .with_state(state)
        .layer(Extension(browser))
}

/// Merged M3U: CCTV + provincial channels.
async fn merged_m3u(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let host = host_from_headers(&headers, "localhost:3000");
    let mut out = "#EXTM3U\n".to_string();

    let mut cctv_channels: Vec<_> = state.cctv_channels.values().collect();
    cctv_channels.sort_by_key(|c| c.sort_order);
    for ch in cctv_channels {
        out.push_str(&m3u8::m3u_entry(&ch.id, &ch.name, "CCTV", &host));
    }
    for ch in state.channels.values() {
        let group = ch.group_name.as_deref().unwrap_or("其他");
        out.push_str(&m3u8::m3u_entry(&ch.id, &ch.name, group, &host));
    }

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "audio/x-mpegurl; charset=utf-8"),
            (header::CONTENT_DISPOSITION, r#"inline; filename="live.m3u""#),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        out,
    )
}

/// Route playlist requests to CCTV or provincial handler.
async fn smart_playlist(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
    browser: Extension<Arc<BrowserPool>>,
) -> Response {
    if state.is_cctv(&channel_id) {
        cctv::proxy_playlist(State(state), Path(channel_id), headers).await.into_response()
    } else {
        provincial::proxy_playlist(State(state), Path(channel_id), headers, browser)
            .await
            .into_response()
    }
}

/// Route /{id}/ts/{encoded}: CCTV uses /ts/, but also fall back for provincial if needed.
async fn cctv_ts_or_provincial_segment(
    State(state): State<AppState>,
    Path((channel_id, encoded)): Path<(String, String)>,
) -> Response {
    if state.is_cctv(&channel_id) {
        cctv::proxy_ts(State(state), Path((channel_id, encoded)))
            .await
            .into_response()
    } else {
        provincial::proxy_segment(State(state), Path((channel_id, encoded)))
            .await
            .into_response()
    }
}

/// Route /{id}/key/{encoded}: CCTV keys need CCTV Referer; provincial keys use their own.
async fn smart_key(
    State(state): State<AppState>,
    Path((channel_id, encoded)): Path<(String, String)>,
) -> Response {
    if state.is_cctv(&channel_id) {
        // Re-use proxy_ts which already sends CCTV Referer/Origin; key bytes are
        // identical to TS in terms of proxying (raw bytes, no decryption).
        cctv::proxy_ts(State(state), Path((channel_id, encoded)))
            .await
            .into_response()
    } else {
        provincial::proxy_key(State(state), Path((channel_id, encoded)))
            .await
            .into_response()
    }
}

fn host_from_headers(headers: &HeaderMap, fallback: &str) -> String {
    headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(fallback)
        .to_string()
}
