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
    pub global_dom_filter: String,
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

        // Global DOM filter (site_rules row with prefix='global'), applied to every
        // browser page via Page.addScriptToEvaluateOnNewDocument — mirrors the
        // original Node.js app (setupPageFilters: rule?.dom_filter || GLOBAL_DOM_FILTER).
        let global_dom_filter: String = conn
            .query_row(
                "SELECT dom_filter FROM site_rules WHERE prefix = 'global'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap_or_default();

        let poll_interval_ms: u64 = get_config("poll_interval_ms", "1500000").parse().unwrap_or(1_500_000);
        let decrypt_cache_ttl_ms: u64 = get_config("decrypt_cache_ttl_ms", "1800000").parse().unwrap_or(1_800_000);
        let fetch_timeout_ms: u64 = get_config("fetch_timeout_ms", "20000").parse().unwrap_or(20_000);
        let proxy_timeout_ms: u64 = get_config("proxy_timeout_ms", "8000").parse().unwrap_or(8_000);
        let jstv_auth_enabled = get_config("jstv_auth_enabled", "false") == "true";

        // Load CCTV channels from DB
        let mut stmt = conn.prepare(
            "SELECT id, name, m3u8_url, fallback_url, sort_order, enabled \
             FROM cctv_channels WHERE enabled = 1 ORDER BY sort_order ASC",
        )?;
        let mut cctv_channels: Vec<CctvChannel> = stmt
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

        // Append built-in provincial satellite channels (no browser needed, CDN direct)
        let max_sort = cctv_channels.iter().map(|c| c.sort_order).max().unwrap_or(18);
        for (i, (id, name, url, fallback)) in BUILTIN_PROVINCIAL.iter().enumerate() {
            // Don't add if already exists in DB
            if !cctv_channels.iter().any(|c| &c.id == id) {
                cctv_channels.push(CctvChannel {
                    id: id.to_string(),
                    name: name.to_string(),
                    m3u8_url: url.to_string(),
                    fallback_url: if fallback.is_empty() { None } else { Some(fallback.to_string()) },
                    sort_order: max_sort + 1 + i as i64,
                    enabled: true,
                });
            }
        }

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

        // Load site rules
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
            global_dom_filter,
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

/// Built-in provincial satellite channels.
/// These use direct CDN URLs (same proxy path as CCTV, no browser needed).
/// Format: (id, name, primary_url, fallback_url)
const BUILTIN_PROVINCIAL: &[(&str, &str, &str, &str)] = &[
    // ── 主流卫视 ──────────────────────────────────────────────────────────────
    ("hunan",        "湖南卫视",    "https://pull-hls.mgtv.com/sdlive/hnwstv/index.m3u8", ""),
    ("dongfang_ws",  "东方卫视",    "https://live-cdn.kankanews.com/live/dongfang/playlist.m3u8", ""),
    ("anhui",        "安徽卫视",    "https://ahlive.ahrtv.cn/ahrtv/live/ahtv1/playlist.m3u8", ""),
    ("beijing",      "北京卫视",    "https://live.btime.com/btv2/index.m3u8", ""),
    ("chongqing",    "重庆卫视",    "https://live.cqnews.net/livestream/cqws_720p/playlist.m3u8", ""),
    ("tianjin",      "天津卫视",    "https://iptv.hitv.com/tianjin/index.m3u8", ""),
    ("liaoning",     "辽宁卫视",    "https://lntv-live.lntv.cn/lntvlive/lnws/index.m3u8", ""),
    ("jilin",        "吉林卫视",    "https://live.jlntv.cn/jlntv/ws/index.m3u8", ""),
    ("heilongjiang", "黑龙江卫视",  "https://hljlive.hljntv.cn/hljlive/weishi/index.m3u8", ""),
    ("shanxi_ws",    "山西卫视",    "https://live.sxtvs.net/sxtvs/sxws/index.m3u8", ""),
    ("shanxi2_ws",   "陕西卫视",    "https://live.sxrtv.com/sxrtv/sxws/index.m3u8", ""),
    ("sichuan",      "四川卫视",    "https://sclive.scstv.com/sctv/scws/playlist.m3u8", ""),
    ("yunnan",       "云南卫视",    "https://ynlive.yntv.cn/yntv/ynws/index.m3u8", ""),
    ("guizhou",      "贵州卫视",    "https://gzlive.gztv.com/gztv/gzws/index.m3u8", ""),
    ("guangxi",      "广西卫视",    "https://live.gxtv.cn/gxtv/gxws/index.m3u8", ""),
    ("neimenggu",    "内蒙古卫视",  "https://iptv.nmgtv.cn/nmgtv/nmws/index.m3u8", ""),
    ("xinjiang",     "新疆卫视",    "https://live.xjtvs.com.cn/xjtvs/xjws/index.m3u8", ""),
    ("xizang",       "西藏卫视",    "https://live.xzrtv.cn/xzrtv/xzws/index.m3u8", ""),
    ("gansu",        "甘肃卫视",    "https://live.gstv.com.cn/gstv/gsws/index.m3u8", ""),
    ("qinghai",      "青海卫视",    "https://live.qhtv.cn/qhtv/qhws/index.m3u8", ""),
    ("ningxia",      "宁夏卫视",    "https://iptv.nxtv.cn/nxtv/nxws/index.m3u8", ""),
    ("hainan",       "海南卫视",    "https://live.hitv.com/hitv/hnws/index.m3u8", ""),
    ("jiangxi",      "江西卫视",    "https://live.jxntv.cn/jxntv/jxws/index.m3u8", ""),
    ("henan",        "河南卫视",    "https://live.haolvtv.com/haolvtv/hnws/index.m3u8", ""),
    ("hubei",        "湖北卫视",    "https://live.cjyun.org/hubeitv/hbws/index.m3u8", ""),
    ("fujian",       "福建卫视",    "https://fjlive.sea.fjrtv.cn/fjrtv/fjws/index.m3u8", ""),
    // ── 凤凰、星空 ───────────────────────────────────────────────────────────
    ("fenghuang_zhongwen", "凤凰中文", "https://hls.ifeng.com/live/Phoenix_Chinese_HD/index.m3u8", ""),
    ("fenghuang_zixun",    "凤凰资讯", "https://hls.ifeng.com/live/Phoenix_InfoNews_HD/index.m3u8", ""),
    // ── 央视上星频道（非CCTV系） ──────────────────────────────────────────────
    ("cgtn",    "CGTN",      "https://news.cgtn.com/resource/live/english/cgtn-news.m3u8", ""),
    ("cgtndoc", "CGTN纪录",   "https://news.cgtn.com/resource/live/cgtn-doc/cgtn-doc.m3u8", ""),
];
