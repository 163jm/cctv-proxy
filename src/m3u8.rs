/// M3U8 playlist parsing and URL-rewriting.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use url::Url;

/// Select the variant with highest BANDWIDTH from a master playlist.
pub fn select_highest_bandwidth(text: &str, base_url: &str) -> Option<String> {
    let base = Url::parse(base_url).ok()?;
    let mut best_bw: u64 = 0;
    let mut best_url: Option<String> = None;
    let mut cur_bw: u64 = 0;

    for line in text.lines() {
        let t = line.trim();
        if t.starts_with("#EXT-X-STREAM-INF") {
            if let Some(bw_str) = t.split("BANDWIDTH=").nth(1) {
                cur_bw = bw_str.split(',').next().unwrap_or("0").parse().unwrap_or(0);
            }
        } else if !t.is_empty() && !t.starts_with('#') && cur_bw > 0 {
            if cur_bw > best_bw {
                best_bw = cur_bw;
                best_url = Some(resolve_url(t, &base));
            }
            cur_bw = 0;
        }
    }
    best_url
}

fn resolve_url(href: &str, base: &Url) -> String {
    if href.starts_with("http") {
        href.to_string()
    } else {
        base.join(href).map(|u| u.to_string()).unwrap_or_else(|_| href.to_string())
    }
}

/// Rewrite a media playlist so all segment URLs are routed through our proxy.
///
/// `seg_action`: the URL segment between channel_id and encoded URL.
///   - CCTV uses "ts"       → `/{id}/ts/{encoded}`      (gateway → cctv::proxy_ts, adds CCTV Referer)
///   - Provincial uses "segment" → `/{id}/segment/{enc}` (gateway → provincial::proxy_segment)
pub fn rewrite_playlist(
    text: &str,
    final_url: &str,
    proxy_host: &str,
    channel_id: &str,
    seg_action: &str,
) -> String {
    let base = match Url::parse(final_url) {
        Ok(u) => u,
        Err(_) => return text.to_string(),
    };
    let scheme_host = format!("{}://{}", base.scheme(), base.host_str().unwrap_or(""));

    // sub_base: directory of the playlist URL
    let sub_base: String = {
        let s = final_url;
        s[..s.rfind('/').map(|i| i + 1).unwrap_or(s.len())].to_string()
    };

    let lines: Vec<String> = text
        .lines()
        .map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return line.to_string();
            }

            // CCTV: all directive lines (including #EXT-X-KEY) pass through unchanged.
            // The decryption is handled natively by delib.so, not via HLS AES-128.
            // Provincial uses seg_action="segment" and rewrites KEY separately.
            if trimmed.starts_with('#') {
                if seg_action == "ts" {
                    // CCTV: pass all directives through unchanged (mirrors original JS)
                    return line.to_string();
                }
                // Provincial: rewrite KEY and MAP URIs
                if trimmed.starts_with("#EXT-X-KEY") && trimmed.contains("URI=") {
                    return rewrite_attr_uri(trimmed, "key", &base, proxy_host, channel_id);
                }
                if trimmed.starts_with("#EXT-X-MAP") && trimmed.contains("URI=") {
                    return rewrite_attr_uri(trimmed, seg_action, &base, proxy_host, channel_id);
                }
                return line.to_string();
            }

            // Skip tracking/logging URLs
            if trimmed.contains("log.")
                || trimmed.contains("report")
                || trimmed.contains("beacon")
                || trimmed.contains("collect")
            {
                return line.to_string();
            }

            // Match original JS: bare names with no '.', '/' or '?' pass through
            // (e.g. pure-number CCTV segments resolved by the player from base URL).
            if !trimmed.contains('.') && !trimmed.contains('/') && !trimmed.contains('?') {
                return line.to_string();
            }

            // Resolve to absolute URL
            let abs_url = if trimmed.starts_with("http") {
                trimmed.to_string()
            } else if trimmed.starts_with('/') {
                format!("{}{}", scheme_host, trimmed)
            } else {
                format!("{}{}", sub_base, trimmed)
            };

            let encoded = URL_SAFE_NO_PAD.encode(abs_url.as_bytes());
            format!("http://{}/{}/{}/{}", proxy_host, channel_id, seg_action, encoded)
        })
        .collect();

    lines.join("\n")
}

fn rewrite_attr_uri(
    line: &str,
    action: &str,
    base: &Url,
    proxy_host: &str,
    channel_id: &str,
) -> String {
    if let Some(start) = line.find("URI=\"") {
        let after = &line[start + 5..];
        if let Some(end) = after.find('"') {
            let original_uri = &after[..end];
            let abs = base
                .join(original_uri)
                .map(|u| u.to_string())
                .unwrap_or_else(|_| original_uri.to_string());
            let encoded = URL_SAFE_NO_PAD.encode(abs.as_bytes());
            let new_uri = format!("http://{}/{}/{}/{}", proxy_host, channel_id, action, encoded);
            return line.replace(
                &format!("URI=\"{}\"", original_uri),
                &format!("URI=\"{}\"", new_uri),
            );
        }
    }
    line.to_string()
}

/// Fix CCTV m3u8 URLs to prefer 720P playlist format.
pub fn fix_cctv_url(url: &str) -> String {
    if url.contains("/index.m3u8") && !url.contains("b=200-2100") && !url.contains("BR=") {
        if let Some(pos) = url.find("/index.m3u8") {
            return format!("{}_720P/playlist.m3u8?wsApp=HLS", &url[..pos]);
        }
    }
    url.to_string()
}

/// Check if an HTTP response looks like an auth error.
pub fn is_auth_error(status: u16, body: &str) -> bool {
    if status == 403 || status == 401 {
        return true;
    }
    let lower = body.to_lowercase();
    ["txsecret", "txtime", "auth_key", "auth failed", "auth expired", "signature", "unauthorized"]
        .iter()
        .any(|kw| lower.contains(kw))
}

/// Generate an M3U playlist entry for a channel.
pub fn m3u_entry(channel_id: &str, name: &str, group: &str, proxy_host: &str) -> String {
    format!(
        "#EXTINF:-1 tvg-id=\"{id}\" tvg-name=\"{name}\" group-title=\"{group}\",{name}\nhttp://{host}/{id}/playlist.m3u8\n",
        id = channel_id,
        name = name,
        group = group,
        host = proxy_host,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_cctv_uses_ts_path() {
        let playlist = "#EXTM3U\n#EXTINF:6.0,\nhttps://cdn.example.com/live/seg001.ts";
        let result = rewrite_playlist(
            playlist,
            "https://cdn.example.com/live/playlist.m3u8",
            "localhost:3000",
            "cctv1",
            "ts",
        );
        assert!(result.contains("/cctv1/ts/"), "CCTV should use /ts/ path");
    }

    #[test]
    fn test_rewrite_provincial_uses_segment_path() {
        let playlist = "#EXTM3U\n#EXTINF:6.0,\nhttps://cdn.example.com/live/seg001.ts";
        let result = rewrite_playlist(
            playlist,
            "https://cdn.example.com/live/playlist.m3u8",
            "localhost:3000",
            "gd_1",
            "segment",
        );
        assert!(result.contains("/gd_1/segment/"), "Provincial should use /segment/ path");
    }

    #[test]
    fn test_select_highest_bandwidth() {
        let master = "#EXTM3U\n\
            #EXT-X-STREAM-INF:BANDWIDTH=500000\nlow.m3u8\n\
            #EXT-X-STREAM-INF:BANDWIDTH=2000000\nhigh.m3u8";
        let result = select_highest_bandwidth(master, "https://cdn.example.com/live/index.m3u8");
        assert!(result.unwrap().contains("high.m3u8"));
    }

    #[test]
    fn test_fix_cctv_url() {
        let url = "https://example.com/channel/index.m3u8";
        let fixed = fix_cctv_url(url);
        assert!(fixed.contains("_720P/playlist.m3u8"));

        let url2 = "https://example.com/channel/index.m3u8?BR=td";
        assert_eq!(fix_cctv_url(url2), url2); // has BR=, don't rewrite
    }
}
