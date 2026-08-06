/// Gateway: the unified public-facing router.
///
/// Routes everything through a single axum Router:
///   GET /live.m3u             → Merged M3U of all channels (CCTV + provincial)
///   GET /                     → same as /live.m3u
///   GET /cctv/{id}/...        → CCTV sub-handlers
///   GET /admin/...            → Admin endpoints
///   GET /{id}/...             → Provincial sub-handlers (default)
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
        // ── Unified M3U listing ────────────────────────────────────────────
        .route("/", get(merged_m3u))
        .route("/live.m3u", get(merged_m3u))
        // ── CCTV ──────────────────────────────────────────────────────────
        .route("/cctv/live.m3u", get(cctv::live_m3u))
        .route("/:id/playlist.m3u8", get(smart_playlist))
        .route("/:id/ts/:encoded", get(cctv_ts_or_provincial_segment))
        // ── Provincial-specific routes ─────────────────────────────────────
        .route("/:id/segment/:encoded", get(provincial::proxy_segment))
        .route("/:id/key/:encoded", get(provincial::proxy_key))
        // ── Admin ──────────────────────────────────────────────────────────
        .route("/admin/cache", get(provincial::admin_cache))
        .route("/admin/poller/refresh", post(provincial::admin_refresh))
        // ── State + browser extension ──────────────────────────────────────
        .with_state(state)
        .layer(Extension(browser))
}

/// Merged M3U: combine CCTV + provincial channels into a single playlist.
async fn merged_m3u(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let host = host_from_headers(&headers, "localhost:3000");
    let mut out = "#EXTM3U\n".to_string();

    // CCTV first, sorted by sort_order
    let mut cctv_channels: Vec<_> = state.cctv_channels.values().collect();
    cctv_channels.sort_by_key(|c| c.sort_order);
    for ch in cctv_channels {
        out.push_str(&m3u8::m3u_entry(&ch.id, &ch.name, "CCTV", &host));
    }

    // Provincial channels
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

/// Route playlist requests to either CCTV or provincial handler.
async fn smart_playlist(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
    browser: Extension<Arc<BrowserPool>>,
) -> Response {
    if state.is_cctv(&channel_id) {
        cctv::proxy_playlist(State(state), Path(channel_id), headers).await.into_response()
    } else {
        provincial::proxy_playlist(
            State(state),
            Path(channel_id),
            headers,
            browser,
        )
        .await
        .into_response()
    }
}

/// Route /{id}/ts/{encoded}: only CCTV uses /ts/; provincial uses /segment/.
/// This handler exists so CCTV m3u8 rewritten URLs (/{id}/ts/...) still work
/// when served from the gateway port.
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

fn host_from_headers(headers: &HeaderMap, fallback: &str) -> String {
    headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(fallback)
        .to_string()
}
