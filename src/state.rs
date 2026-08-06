use crate::{
    cache::{M3u8Cache, SegmentCache, StreamCache},
    db::{AppDb, CctvChannel, Channel, SiteRule},
    native::NativeLibs,
};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

/// Per-channel live state for the provincial proxy
#[derive(Default)]
pub struct ChannelState {
    /// Resolved stream URL (e.g. the real .m3u8 from the TV station)
    pub stream_url: Option<String>,
    /// When the stream_url was fetched (used for freshness checks)
    pub stream_url_fetched_at: Option<std::time::Instant>,
    /// True while a browser fetch is in progress
    pub fetching: bool,
    /// Number of active "viewers" (connections using this channel right now)
    pub viewer_count: u32,
}

pub type SharedChannelState = Arc<Mutex<ChannelState>>;

#[derive(Clone)]
pub struct AppState {
    // ── Config from DB ──────────────────────────────────────────────────────
    pub cctv_channels: Arc<HashMap<String, CctvChannel>>,
    pub channels: Arc<HashMap<String, Channel>>,
    pub site_rules: Arc<Vec<SiteRule>>,
    pub user_agent: Arc<String>,
    pub blocked_domains: Arc<Vec<String>>,

    // ── Timeouts & TTLs ─────────────────────────────────────────────────────
    pub fetch_timeout_ms: u64,
    pub proxy_timeout_ms: u64,
    pub decrypt_cache_ttl_ms: u64,
    pub poll_interval_ms: u64,

    // ── Native libs ─────────────────────────────────────────────────────────
    pub native: Arc<NativeLibs>,

    // ── Caches ──────────────────────────────────────────────────────────────
    pub stream_cache: StreamCache,
    pub segment_cache: Arc<SegmentCache>,
    pub m3u8_cache: Arc<M3u8Cache>,

    // ── Per-channel live state ───────────────────────────────────────────────
    pub channel_states: Arc<DashMap<String, SharedChannelState>>,

    // ── Limit concurrent browser fetches (1 at a time to avoid Chrome OOM) ──
    pub browser_sem: Arc<Semaphore>,

    // ── HTTP client (shared, connection-pooled) ──────────────────────────────
    pub http: reqwest::Client,

    // ── Chrome path for headless browser ────────────────────────────────────
    pub chrome_path: Arc<String>,

    // ── App directory (where app.db lives) ──────────────────────────────────
    pub app_dir: Arc<std::path::PathBuf>,
}

impl AppState {
    pub fn new(db: AppDb, native: NativeLibs, chrome_path: String, app_dir: std::path::PathBuf) -> Self {
        let cctv_map: HashMap<String, CctvChannel> = db
            .cctv_channels
            .into_iter()
            .map(|c| (c.id.clone(), c))
            .collect();

        let channel_map: HashMap<String, Channel> = db
            .channels
            .into_iter()
            .map(|c| (c.id.clone(), c))
            .collect();

        let http = reqwest::Client::builder()
            .user_agent(&db.user_agent)
            .timeout(std::time::Duration::from_millis(db.proxy_timeout_ms))
            .redirect(reqwest::redirect::Policy::limited(5))
            .gzip(true)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            cctv_channels: Arc::new(cctv_map),
            channels: Arc::new(channel_map),
            site_rules: Arc::new(db.site_rules),
            user_agent: Arc::new(db.user_agent),
            blocked_domains: Arc::new(db.blocked_domains),
            fetch_timeout_ms: db.fetch_timeout_ms,
            proxy_timeout_ms: db.proxy_timeout_ms,
            decrypt_cache_ttl_ms: db.decrypt_cache_ttl_ms,
            poll_interval_ms: db.poll_interval_ms,
            native: Arc::new(native),
            stream_cache: StreamCache::new(db.decrypt_cache_ttl_ms),
            segment_cache: Arc::new(SegmentCache::new(10_000, 80)),
            m3u8_cache: Arc::new(M3u8Cache::new(1_500, 200)),
            channel_states: Arc::new(DashMap::new()),
            browser_sem: Arc::new(Semaphore::new(1)),
            http,
            chrome_path: Arc::new(chrome_path),
            app_dir: Arc::new(app_dir),
        }
    }

    /// True if this channel is handled by the CCTV sub-proxy
    pub fn is_cctv(&self, channel_id: &str) -> bool {
        self.cctv_channels.contains_key(channel_id)
            || channel_id.to_lowercase().starts_with("cctv")
    }

    /// True if this channel uses native signing (js_, zj_, sd_, sh_ prefixes)
    pub fn is_signed_channel(&self, channel_id: &str) -> bool {
        channel_id.starts_with("js_")
            || channel_id.starts_with("zj_")
            || channel_id.starts_with("sd_")
            || channel_id.starts_with("sh_")
    }

    pub fn get_or_create_channel_state(&self, channel_id: &str) -> SharedChannelState {
        self.channel_states
            .entry(channel_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(ChannelState::default())))
            .clone()
    }

    pub fn site_rule_for(&self, channel_id: &str) -> Option<&SiteRule> {
        self.site_rules
            .iter()
            .find(|r| channel_id.starts_with(&r.prefix))
    }

    /// Build Referer/Origin headers for a given channel
    pub fn referer_headers(&self, channel_id: &str, stream_url: &str) -> Vec<(String, String)> {
        let mut headers = Vec::new();
        if let Some(rule) = self.site_rule_for(channel_id) {
            if let Some(ref r) = rule.referer {
                headers.push(("Referer".to_string(), r.clone()));
            }
            if let Some(ref o) = rule.origin {
                headers.push(("Origin".to_string(), o.clone()));
            }
        } else if let Ok(parsed) = url::Url::parse(stream_url) {
            let origin = format!("{}://{}", parsed.scheme(), parsed.host_str().unwrap_or(""));
            headers.push(("Referer".to_string(), format!("{}/", origin)));
            headers.push(("Origin".to_string(), origin));
        }
        // Shanghai channels need a specific UA
        if channel_id.starts_with("sh_") {
            headers.push((
                "User-Agent".to_string(),
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
                    .to_string(),
            ));
        }
        headers
    }
}
