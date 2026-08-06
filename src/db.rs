use anyhow::Result;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CctvChannel {
    pub id: String,
    pub name: String,
    pub m3u8_url: String,
    pub fallback_url: Option<String>,
    pub sort_order: i64,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Channel {
    pub id: String,
    pub name: String,
    pub url: String,
    pub group_name: Option<String>,
    pub channel_type: String,
    pub sid: Option<String>,
    pub jstv_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteRule {
    pub prefix: String,
    pub name: String,
    pub target_url: Option<String>,
    pub m3u8_match: Option<String>,
    pub referer: Option<String>,
    pub origin: Option<String>,
    pub dom_filter: Option<String>,
    pub selectors: Option<String>,
    pub action_script: Option<String>,
    pub is_polled: bool,
}

#[derive(Debug, Clone)]
pub struct AppDb {
    pub cctv_channels: Vec<CctvChannel>,
    pub channels: Vec<Channel>,
    pub site_rules: Vec<SiteRule>,
    pub user_agent: String,
    pub blocked_domains: Vec<String>,
    pub poll_interval_ms: u64,
    pub decrypt_cache_ttl_ms: u64,
    pub fetch_timeout_ms: u64,
    pub proxy_timeout_ms: u64,
    pub jstv_auth_enabled: bool,
}

impl AppDb {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;

        // Load sys_config as a helper closure
        let get_config = |key: &str, default: &str| -> String {
            conn.query_row(
                "SELECT value FROM sys_config WHERE key = ?1",
                [key],
                |row| row.get::<_, String>(0),
            )
            .unwrap_or_else(|_| default.to_string())
        };

        let user_agent = get_config("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
        let blocked_domains: Vec<String> =
            serde_json::from_str(&get_config("blocked_domains", "[]")).unwrap_or_default();
        let poll_interval_ms: u64 = get_config("poll_interval_ms", "1500000")
            .parse()
            .unwrap_or(1_500_000);
        let decrypt_cache_ttl_ms: u64 = get_config("decrypt_cache_ttl_ms", "1800000")
            .parse()
            .unwrap_or(1_800_000);
        let fetch_timeout_ms: u64 = get_config("fetch_timeout_ms", "20000")
            .parse()
            .unwrap_or(20_000);
        let proxy_timeout_ms: u64 = get_config("proxy_timeout_ms", "8000")
            .parse()
            .unwrap_or(8_000);
        let jstv_auth_enabled = get_config("jstv_auth_enabled", "false") == "true";

        // Load CCTV channels
        let mut stmt = conn.prepare(
            "SELECT id, name, m3u8_url, fallback_url, sort_order, enabled \
             FROM cctv_channels WHERE enabled = 1 ORDER BY sort_order ASC",
        )?;
        let cctv_channels = stmt
            .query_map([], |row| {
                Ok(CctvChannel {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    m3u8_url: row.get(2)?,
                    fallback_url: row.get(3)?,
                    sort_order: row.get(4)?,
                    enabled: row.get::<_, i64>(5)? != 0,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        // Load provincial channels
        let mut stmt = conn.prepare(
            "SELECT id, name, url, group_name, type, sid, jstv_id FROM channels",
        )?;
        let channels = stmt
            .query_map([], |row| {
                Ok(Channel {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    url: row.get(2)?,
                    group_name: row.get(3)?,
                    channel_type: row.get::<_, Option<String>>(4)?.unwrap_or_else(|| "m3u8".to_string()),
                    sid: row.get(5)?,
                    jstv_id: row.get(6)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        // Load site rules (exclude global)
        let mut stmt = conn.prepare(
            "SELECT prefix, name, target_url, m3u8_match, referer, origin, \
             dom_filter, selectors, action_script, is_polled \
             FROM site_rules WHERE prefix != 'global'",
        )?;
        let site_rules = stmt
            .query_map([], |row| {
                Ok(SiteRule {
                    prefix: row.get(0)?,
                    name: row.get(1)?,
                    target_url: row.get(2)?,
                    m3u8_match: row.get(3)?,
                    referer: row.get(4)?,
                    origin: row.get(5)?,
                    dom_filter: row.get(6)?,
                    selectors: row.get(7)?,
                    action_script: row.get(8)?,
                    is_polled: row.get::<_, Option<i64>>(9)?.unwrap_or(1) != 0,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(AppDb {
            cctv_channels,
            channels,
            site_rules,
            user_agent,
            blocked_domains,
            poll_interval_ms,
            decrypt_cache_ttl_ms,
            fetch_timeout_ms,
            proxy_timeout_ms,
            jstv_auth_enabled,
        })
    }
}
