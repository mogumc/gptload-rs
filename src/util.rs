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

// ── Token estimation: character-type weight heuristic ──
// Based on new-api's token_estimator.go. No external deps, no tokenizer file.
// Accuracy: ±15-30%. Suitable for fallback billing when upstream returns no usage.

struct TokenWeights {
    word: f64,
    number: f64,
    cjk: f64,
    symbol: f64,
    math_symbol: f64,
    url_delim: f64,
    at_sign: f64,
    emoji: f64,
    newline: f64,
    space: f64,
}

const OPENAI_WEIGHTS: TokenWeights = TokenWeights {
    word: 1.02,
    number: 1.55,
    cjk: 0.85,
    symbol: 0.4,
    math_symbol: 2.68,
    url_delim: 1.0,
    at_sign: 2.0,
    emoji: 2.12,
    newline: 0.5,
    space: 0.42,
};

#[derive(Clone, Copy, PartialEq)]
enum WordType {
    None,
    Latin,
    Number,
}

/// Estimate token count using OpenAI-compatible character-type weights.
/// Stateful grouping: consecutive letters/digits count as one token;
/// CJK/emoji/symbols count per character.
pub fn estimate_tokens(text: &str) -> u64 {
    if text.is_empty() {
        return 0;
    }
    let mut count: f64 = 0.0;
    let mut current = WordType::None;

    for ch in text.chars() {
        if ch.is_whitespace() {
            current = WordType::None;
            count += if ch == '\n' || ch == '\t' {
                OPENAI_WEIGHTS.newline
            } else {
                OPENAI_WEIGHTS.space
            };
            continue;
        }
        if is_cjk(ch) {
            current = WordType::None;
            count += OPENAI_WEIGHTS.cjk;
            continue;
        }
        if is_emoji(ch) {
            current = WordType::None;
            count += OPENAI_WEIGHTS.emoji;
            continue;
        }
        if ch.is_alphabetic() || ch.is_numeric() {
            let new_type = if ch.is_numeric() {
                WordType::Number
            } else {
                WordType::Latin
            };
            if current == WordType::None || current != new_type {
                count += if new_type == WordType::Number {
                    OPENAI_WEIGHTS.number
                } else {
                    OPENAI_WEIGHTS.word
                };
                current = new_type;
            }
            continue;
        }
        // Symbol class
        current = WordType::None;
        if is_math_symbol(ch) {
            count += OPENAI_WEIGHTS.math_symbol;
        } else if ch == '@' {
            count += OPENAI_WEIGHTS.at_sign;
        } else if is_url_delim(ch) {
            count += OPENAI_WEIGHTS.url_delim;
        } else {
            count += OPENAI_WEIGHTS.symbol;
        }
    }

    (count.ceil() as u64).max(1)
}

/// Accumulate visible + reasoning content from an SSE data chunk.
/// Models with CoT/thinking emit `reasoning_content` alongside `content`;
/// both are output tokens and must be counted for billing estimation.
pub fn extract_sse_content(chunk: &[u8], buf: &mut String) {
    // Zero-alloc fast path for valid UTF-8 (the normal case for SSE chunks).
    let text: std::borrow::Cow<str> = match std::str::from_utf8(chunk) {
        Ok(s) => std::borrow::Cow::Borrowed(s),
        Err(_) => String::from_utf8_lossy(chunk),
    };
    let mut extracted = 0usize;
    for line in text.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(data) else {
            continue;
        };
        let Some(delta) = v
            .get("choices")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("delta"))
        else {
            continue;
        };
        if let Some(s) = delta.get("reasoning_content").and_then(|v| v.as_str()) {
            buf.push_str(s);
            extracted += s.len();
        }
        if let Some(s) = delta.get("content").and_then(|v| v.as_str()) {
            buf.push_str(s);
            extracted += s.len();
        }
    }
    if extracted > 0 {
        tracing::debug!(extracted, "sse content extracted for fallback estimation");
    }
}

fn is_cjk(ch: char) -> bool {
    matches!(
        ch as u32,
        0x4E00..=0x9FFF
            | 0x3400..=0x4DBF
            | 0x20000..=0x2A6DF
            | 0x3040..=0x30FF
            | 0xAC00..=0xD7A3
            | 0xF900..=0xFAFF
    )
}

fn is_emoji(ch: char) -> bool {
    matches!(
        ch as u32,
        0x1F300..=0x1F9FF
            | 0x2600..=0x26FF
            | 0x2700..=0x27BF
            | 0x1FA00..=0x1FAFF
    )
}

fn is_math_symbol(ch: char) -> bool {
    matches!(
        ch as u32,
        0x2200..=0x22FF | 0x2A00..=0x2AFF | 0x1D400..=0x1D7FF
    ) || matches!(
        ch,
        '\u{2211}' | '\u{222B}' | '\u{2202}' | '\u{221A}' | '\u{221E}' | '\u{2248}' | '\u{2260}' | '\u{2264}' | '\u{2265}' | '\u{00B1}' | '\u{00D7}' | '\u{00F7}'
    )
}

fn is_url_delim(ch: char) -> bool {
    matches!(ch, '/' | ':' | '?' | '&' | '=' | '#' | ';' | '%')
}

// ── Key validation ──

/// Validate key string: only [A-Za-z0-9_-] allowed.
pub fn validate_key_chars(key: &str) -> Result<(), String> {
    if key.is_empty() {
        return Err("key must not be empty".to_string());
    }
    if !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        return Err(format!(
            "key contains invalid character '{}' (only A-Za-z0-9 _ - allowed)",
            key.chars().find(|c| !c.is_ascii_alphanumeric() && *c != '_' && *c != '-').unwrap_or(' ')
        ));
    }
    Ok(())
}
