/// In-memory stream URL cache with TTL, mirroring the original JSON cache logic.
/// Keyed by `{channel_id}:{hostname}`.
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEntry {
    pub channel_id: String,
    pub original_url: String,
    pub stream_url: String,
    pub source: String,
    pub expires_at: std::time::SystemTime,
    pub hit_count: u64,
}

impl StreamEntry {
    pub fn is_valid(&self) -> bool {
        self.expires_at > std::time::SystemTime::now()
    }
}

#[derive(Clone)]
pub struct StreamCache {
    inner: Arc<DashMap<String, StreamEntry>>,
    ttl: Duration,
}

impl StreamCache {
    pub fn new(ttl_ms: u64) -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            ttl: Duration::from_millis(ttl_ms),
        }
    }

    fn make_key(channel_id: &str, url: &str) -> Option<String> {
        let parsed = url::Url::parse(url).ok()?;
        Some(format!("{}:{}", channel_id, parsed.host_str().unwrap_or("")))
    }

    pub fn get(&self, channel_id: &str, original_url: &str) -> Option<String> {
        let key = Self::make_key(channel_id, original_url)?;
        let mut entry = self.inner.get_mut(&key)?;
        if !entry.is_valid() {
            drop(entry);
            self.inner.remove(&key);
            return None;
        }
        entry.hit_count += 1;
        Some(entry.stream_url.clone())
    }

    pub fn set(&self, channel_id: &str, original_url: &str, stream_url: &str, source: &str) {
        let Some(key) = Self::make_key(channel_id, original_url) else {
            return;
        };
        let expires_at = std::time::SystemTime::now() + self.ttl;
        self.inner.insert(
            key,
            StreamEntry {
                channel_id: channel_id.to_string(),
                original_url: original_url.to_string(),
                stream_url: stream_url.to_string(),
                source: source.to_string(),
                expires_at,
                hit_count: 0,
            },
        );
    }

    pub fn invalidate(&self, channel_id: &str, original_url: &str) {
        if let Some(key) = Self::make_key(channel_id, original_url) {
            self.inner.remove(&key);
        }
    }

    pub fn cleanup_expired(&self) {
        self.inner.retain(|_, v| v.is_valid());
    }

    pub fn all_entries(&self) -> Vec<StreamEntry> {
        self.inner.iter().map(|r| r.value().clone()).collect()
    }

    pub fn stats(&self) -> serde_json::Value {
        let now = std::time::SystemTime::now();
        let all: Vec<_> = self.inner.iter().collect();
        let valid = all.iter().filter(|e| e.is_valid()).count();
        let total_hits: u64 = all.iter().map(|e| e.hit_count).sum();
        serde_json::json!({
            "total_entries": all.len(),
            "valid_entries": valid,
            "total_hits": total_hits,
        })
    }
}

// ─── Segment / M3U8 short-lived caches ──────────────────────────────────────

struct TimedEntry<T> {
    data: T,
    inserted: Instant,
}

pub struct SegmentCache {
    inner: DashMap<String, TimedEntry<bytes::Bytes>>,
    ttl: Duration,
    max_size: usize,
}

impl SegmentCache {
    pub fn new(ttl_ms: u64, max_size: usize) -> Self {
        Self {
            inner: DashMap::new(),
            ttl: Duration::from_millis(ttl_ms),
            max_size,
        }
    }

    pub fn get(&self, url: &str) -> Option<bytes::Bytes> {
        let entry = self.inner.get(url)?;
        if entry.inserted.elapsed() > self.ttl {
            drop(entry);
            self.inner.remove(url);
            return None;
        }
        Some(entry.data.clone())
    }

    pub fn set(&self, url: String, data: bytes::Bytes) {
        if self.inner.len() >= self.max_size {
            // Evict one expired entry if possible, else skip
            let expired: Vec<_> = self
                .inner
                .iter()
                .filter(|e| e.inserted.elapsed() > self.ttl)
                .map(|e| e.key().clone())
                .take(1)
                .collect();
            for k in expired {
                self.inner.remove(&k);
            }
        }
        self.inner.insert(url, TimedEntry { data, inserted: Instant::now() });
    }
}

pub struct M3u8Cache {
    inner: DashMap<String, TimedEntry<String>>,
    ttl: Duration,
    max_size: usize,
}

impl M3u8Cache {
    pub fn new(ttl_ms: u64, max_size: usize) -> Self {
        Self {
            inner: DashMap::new(),
            ttl: Duration::from_millis(ttl_ms),
            max_size,
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let entry = self.inner.get(key)?;
        if entry.inserted.elapsed() > self.ttl {
            drop(entry);
            self.inner.remove(key);
            return None;
        }
        Some(entry.data.clone())
    }

    pub fn set(&self, key: String, content: String) {
        if self.inner.len() >= self.max_size {
            let expired: Vec<_> = self
                .inner
                .iter()
                .filter(|e| e.inserted.elapsed() > self.ttl)
                .map(|e| e.key().clone())
                .take(10)
                .collect();
            for k in expired {
                self.inner.remove(&k);
            }
        }
        self.inner.insert(key, TimedEntry { data: content, inserted: Instant::now() });
    }

    pub fn remove(&self, key: &str) {
        self.inner.remove(key);
    }
}
