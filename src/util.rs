
use std::time::{SystemTime, UNIX_EPOCH};

#[inline]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Very small query parser for `?a=b&c=d`.
/// Returns value for `key` if present. No percent-decoding (tokens are expected to be simple).
#[inline]
pub fn query_get<'a>(uri: &'a http::Uri, key: &'a str) -> Option<&'a str> {
    let q = uri.query()?;
    for part in q.split('&') {
        let mut it = part.splitn(2, '=');
        let k = it.next()?;
        if k == key {
            return it.next();
        }
    }
    None
}
