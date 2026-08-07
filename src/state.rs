use crate::{
    cache::{M3u8Cache, SegmentCache, StreamCache},
    db::{AppDb, CctvChannel, Channel, SiteRule},
    native::NativeLibs,
};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, Semaphore};

#[derive(Default)]
pub struct ChannelState {
    pub stream_url: Option<String>,
    pub stream_url_fetched_at: Option<std::time::Instant>,
    pub fetching: bool,
    pub viewer_count: u32,
}

pub type SharedChannelState = Arc<Mutex<ChannelState>>;

#[derive(Clone)]
pub struct AppState {
    pub cctv_channels: Arc<HashMap<String, CctvChannel>>,
    pub channels: Arc<HashMap<String, Channel>>,
    pub site_rules: Arc<Vec<SiteRule>>,
    pub global_dom_filter: Arc<String>,
    pub user_agent: Arc<String>,
    pub blocked_domains: Arc<Vec<String>>,
    pub fetch_timeout_ms: u64,
    pub proxy_timeout_ms: u64,
    pub decrypt_cache_ttl_ms: u64,
    pub poll_interval_ms: u64,
    pub native: Arc<NativeLibs>,
    pub stream_cache: StreamCache,
    pub segment_cache: Arc<SegmentCache>,
    pub m3u8_cache: Arc<M3u8Cache>,
    pub channel_states: Arc<DashMap<String, SharedChannelState>>,
    pub browser_sem: Arc<Semaphore>,
    /// Signalled by POST /admin/poller/refresh to trigger an immediate poll pass.
    pub refresh: Arc<Notify>,
    pub http: reqwest::Client,
    pub chrome_path: Arc<String>,
    pub app_dir: Arc<std::path::PathBuf>,
}

impl AppState {
    pub fn new(db: AppDb, native: NativeLibs, chrome_path: String, app_dir: std::path::PathBuf) -> Self {
        let cctv_map: HashMap<String, CctvChannel> =
            db.cctv_channels.into_iter().map(|c| (c.id.clone(), c)).collect();
        let channel_map: HashMap<String, Channel> =
            db.channels.into_iter().map(|c| (c.id.clone(), c)).collect();

        let http = reqwest::Client::builder()
            .user_agent(db.user_agent.as_str())
            .timeout(std::time::Duration::from_millis(db.proxy_timeout_ms))
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            cctv_channels: Arc::new(cctv_map),
            channels: Arc::new(channel_map),
            site_rules: Arc::new(db.site_rules),
            global_dom_filter: Arc::new(db.global_dom_filter),
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
            // Allow 1 concurrent browser fetch (one tab at a time, reuse process)
            browser_sem: Arc::new(Semaphore::new(1)),
            refresh: Arc::new(Notify::new()),
            http,
            chrome_path: Arc::new(chrome_path),
            app_dir: Arc::new(app_dir),
        }
    }

    pub fn is_cctv(&self, channel_id: &str) -> bool {
        self.cctv_channels.contains_key(channel_id)
            || channel_id.to_lowercase().starts_with("cctv")
    }

    /// Channels that use native signing via media_utils.so
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

    /// Build Referer/Origin/UA headers for proxying segments of a given channel.
    pub fn referer_headers(&self, channel_id: &str, stream_url: &str) -> Vec<(String, String)> {
        let mut headers = vec![
            ("Accept".to_string(), "*/*".to_string()),
            ("Accept-Language".to_string(), "zh-CN,zh;q=0.9".to_string()),
            ("Accept-Encoding".to_string(), "identity".to_string()),
            ("Connection".to_string(), "keep-alive".to_string()),
        ];

        // Use rule's Referer/Origin if defined
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

        // Shanghai channels need a specific newer UA
        if channel_id.starts_with("sh_") {
            headers.push((
                "User-Agent".to_string(),
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
                    .to_string(),
            ));
        } else {
            headers.push(("User-Agent".to_string(), (*self.user_agent).clone()));
        }

        headers
    }
}
