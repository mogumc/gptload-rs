use crate::config::UpstreamFormat;
use bytes::Bytes;
use hyper::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Body, Method, Response};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_stream::wrappers::ReceiverStream;

pub enum AuthStyle {
    OpenAiBearer,
    AnthropicKey,
    None,
}

pub struct AdaptedRequest {
    pub method: Method,
    pub path_and_query: http::uri::PathAndQuery,
    pub body: Bytes,
    pub auth_style: AuthStyle,
}

pub fn adapt_request(
    format: UpstreamFormat,
    original_pq: &http::uri::PathAndQuery,
    method: &Method,
    body: &Bytes,
    model: &str,
    key: &str,
) -> Result<AdaptedRequest, Response<Body>> {
    match format {
        UpstreamFormat::Openai => Ok(AdaptedRequest {
            method: method.clone(),
            path_and_query: original_pq.clone(),
            body: body.clone(),
            auth_style: AuthStyle::OpenAiBearer,
        }),
        UpstreamFormat::Anthropic => adapt_anthropic_request(original_pq, body),
        UpstreamFormat::Gemini => adapt_gemini_request(original_pq, body, model, key),
    }
}

pub async fn adapt_response(
    format: UpstreamFormat,
    up_resp: Response<Body>,
    stream_request: bool,
    model: Option<String>,
) -> Response<Body> {
    match format {
        UpstreamFormat::Openai => up_resp,
        UpstreamFormat::Anthropic => {
            if stream_request {
                transform_sse_response(up_resp, model, anthropic_sse_to_openai)
            } else {
                transform_json_response(up_resp, model, anthropic_json_to_openai).await
            }
        }
        UpstreamFormat::Gemini => {
            if stream_request {
                transform_sse_response(up_resp, model, gemini_sse_to_openai)
            } else {
                transform_json_response(up_resp, model, gemini_json_to_openai).await
            }
        }
    }
}

fn adapt_anthropic_request(
    original_pq: &http::uri::PathAndQuery,
    body: &Bytes,
) -> Result<AdaptedRequest, Response<Body>> {
    if !is_chat_completions_path(original_pq.path()) {
        return Err(format_error(
            http::StatusCode::BAD_REQUEST,
            "anthropic format only supports /v1/chat/completions",
            "unsupported_format_path",
        ));
    }
    let v: serde_json::Value = serde_json::from_slice(body).map_err(|_| {
        format_error(
            http::StatusCode::BAD_REQUEST,
            "request body must be valid json",
            "bad_request",
        )
    })?;
    let model = v.get("model").and_then(|m| m.as_str()).unwrap_or_default();
    let max_tokens = v
        .get("max_tokens")
        .or_else(|| v.get("max_completion_tokens"))
        .and_then(|n| n.as_u64())
        .unwrap_or(1024);
    let stream = v.get("stream").and_then(|s| s.as_bool()).unwrap_or(false);

    let mut system_parts = Vec::new();
    let mut messages = Vec::new();
    if let Some(input_messages) = v.get("messages").and_then(|m| m.as_array()) {
        for msg in input_messages {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("user");
            let content = msg
                .get("content")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            if role == "system" {
                let text = content_to_text(&content);
                if !text.is_empty() {
                    system_parts.push(text);
                }
                continue;
            }
            let out_role = if role == "assistant" {
                "assistant"
            } else {
                "user"
            };
            messages.push(serde_json::json!({
                "role": out_role,
                "content": content_to_anthropic_blocks(&content),
            }));
        }
    }

    let mut out = serde_json::json!({
        "model": model,
        "max_tokens": max_tokens,
        "messages": messages,
        "stream": stream,
    });
    if !system_parts.is_empty() {
        out["system"] = serde_json::Value::String(system_parts.join("\n\n"));
    }
    // Pass through tools and tool_choice.
    if let Some(tools) = v.get("tools") {
        let anthropic_tools: Vec<serde_json::Value> = tools
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| t.get("function"))
                    .map(|f| {
                        serde_json::json!({
                            "name": f.get("name"),
                            "description": f.get("description"),
                            "input_schema": f.get("parameters").cloned().unwrap_or(serde_json::json!({})),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        if !anthropic_tools.is_empty() {
            out["tools"] = serde_json::Value::Array(anthropic_tools);
        }
    }
    if let Some(tc) = v.get("tool_choice") {
        if tc.as_str() == Some("auto") || tc.as_str() == Some("any") {
            out["tool_choice"] = serde_json::json!({"type": "auto"});
        } else if tc.as_str() == Some("required") {
            out["tool_choice"] = serde_json::json!({"type": "any"});
        } else if let Some(name) = tc.get("function").and_then(|f| f.get("name")).and_then(|n| n.as_str()) {
            out["tool_choice"] = serde_json::json!({"type": "tool", "name": name});
        }
    }
    copy_number(&v, &mut out, "temperature", "temperature");
    copy_number(&v, &mut out, "top_p", "top_p");
    if let Some(stop) = v.get("stop") {
        out["stop_sequences"] = match stop {
            serde_json::Value::Array(_) => stop.clone(),
            serde_json::Value::String(_) => serde_json::json!([stop.clone()]),
            _ => serde_json::Value::Null,
        };
    }

    Ok(AdaptedRequest {
        method: Method::POST,
        path_and_query: http::uri::PathAndQuery::from_static("/v1/messages"),
        body: Bytes::from(out.to_string()),
        auth_style: AuthStyle::AnthropicKey,
    })
}

fn adapt_gemini_request(
    original_pq: &http::uri::PathAndQuery,
    body: &Bytes,
    model: &str,
    key: &str,
) -> Result<AdaptedRequest, Response<Body>> {
    if !is_chat_completions_path(original_pq.path()) {
        return Err(format_error(
            http::StatusCode::BAD_REQUEST,
            "gemini format only supports /v1/chat/completions",
            "unsupported_format_path",
        ));
    }
    let v: serde_json::Value = serde_json::from_slice(body).map_err(|_| {
        format_error(
            http::StatusCode::BAD_REQUEST,
            "request body must be valid json",
            "bad_request",
        )
    })?;
    let stream = v.get("stream").and_then(|s| s.as_bool()).unwrap_or(false);

    let mut system_parts = Vec::new();
    let mut contents = Vec::new();
    if let Some(input_messages) = v.get("messages").and_then(|m| m.as_array()) {
        for msg in input_messages {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("user");
            let content = msg
                .get("content")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            if role == "system" {
                let text = content_to_text(&content);
                if !text.is_empty() {
                    system_parts.push(text);
                }
                continue;
            }
            let parts = content_to_gemini_parts(&content);
            if parts.is_empty() {
                continue;
            }
            let out_role = if role == "assistant" { "model" } else { "user" };
            contents.push(serde_json::json!({
                "role": out_role,
                "parts": parts,
            }));
        }
    }

    let mut out = serde_json::json!({ "contents": contents });
    if !system_parts.is_empty() {
        out["systemInstruction"] = serde_json::json!({
            "parts": [{"text": system_parts.join("\n\n")}]
        });
    }

    let mut generation = serde_json::Map::new();
    if let Some(n) = v
        .get("max_tokens")
        .or_else(|| v.get("max_completion_tokens"))
        .and_then(|n| n.as_u64())
    {
        generation.insert("maxOutputTokens".to_string(), serde_json::json!(n));
    }
    if let Some(n) = v.get("temperature").and_then(|n| n.as_f64()) {
        generation.insert("temperature".to_string(), serde_json::json!(n));
    }
    if let Some(n) = v.get("top_p").and_then(|n| n.as_f64()) {
        generation.insert("topP".to_string(), serde_json::json!(n));
    }
    if let Some(stop) = v.get("stop") {
        let stops = match stop {
            serde_json::Value::Array(a) => a.clone(),
            serde_json::Value::String(_) => vec![stop.clone()],
            _ => Vec::new(),
        };
        if !stops.is_empty() {
            generation.insert("stopSequences".to_string(), serde_json::Value::Array(stops));
        }
    }
    if !generation.is_empty() {
        out["generationConfig"] = serde_json::Value::Object(generation);
    }

    let model_path = if model.starts_with("models/") {
        model.to_string()
    } else {
        format!("models/{model}")
    };
    let action = if stream {
        "streamGenerateContent"
    } else {
        "generateContent"
    };
    let encoded_key = utf8_percent_encode(key, NON_ALPHANUMERIC).to_string();
    let path = if stream {
        format!("/v1beta/{model_path}:{action}?alt=sse&key={encoded_key}")
    } else {
        format!("/v1beta/{model_path}:{action}?key={encoded_key}")
    };

    let path_and_query = path.parse().map_err(|_| {
        format_error(
            http::StatusCode::BAD_GATEWAY,
            "invalid gemini upstream path",
            "invalid_upstream_uri",
        )
    })?;
    Ok(AdaptedRequest {
        method: Method::POST,
        path_and_query,
        body: Bytes::from(out.to_string()),
        auth_style: AuthStyle::None,
    })
}

fn is_chat_completions_path(path: &str) -> bool {
    path == "/v1/chat/completions" || path == "/v1/chat/completions/"
}

fn copy_number(src: &serde_json::Value, dst: &mut serde_json::Value, src_key: &str, dst_key: &str) {
    if let Some(v) = src.get(src_key).and_then(|n| n.as_f64()) {
        dst[dst_key] = serde_json::json!(v);
    }
}

fn content_to_text(content: &serde_json::Value) -> String {
    match content {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(parts) => parts
            .iter()
            .filter_map(|part| {
                part.get("text")
                    .and_then(|t| t.as_str())
                    .or_else(|| part.get("content").and_then(|t| t.as_str()))
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// Max decoded file size: 20 MB (matching OpenAI / Anthropic limits).
const MAX_DECODED_BYTES: usize = 20 * 1024 * 1024;

fn content_to_anthropic_blocks(content: &serde_json::Value) -> serde_json::Value {
    match content {
        serde_json::Value::Array(parts) => {
            let out: Vec<serde_json::Value> = parts
                .iter()
                .filter_map(|part| openai_part_to_anthropic(part))
                .collect();
            serde_json::Value::Array(out)
        }
        _ => {
            let text = content_to_text(content);
            if text.len() > MAX_DECODED_BYTES {
                tracing::warn!(text_len = text.len(), "anthropic: fallback text content dropped (exceeds {}MB)", MAX_DECODED_BYTES / 1024 / 1024);
                return serde_json::Value::Array(vec![]);
            }
            serde_json::json!([{"type": "text", "text": text}])
        },
    }
}

/// Binary data extracted from an OpenAI multimodal content part.
struct BinaryAttachment {
    mime: String,
    data: String,
}

/// Extract binary data from an OpenAI content part (image_url / input_audio / file).
/// Handles data URI parsing, size checks, and MIME inference.
/// `provider` is used for warn logs ("anthropic" or "gemini").
fn extract_binary_attachment(part: &serde_json::Value, provider: &str) -> Option<BinaryAttachment> {
    let part_type = part.get("type").and_then(|t| t.as_str()).unwrap_or("");

    match part_type {
        "image_url" => {
            let url = part.get("image_url").and_then(|u| u.get("url")).and_then(|u| u.as_str());
            let url = match url {
                Some(u) => u,
                None => return None,
            };
            match parse_data_uri(url, MAX_DECODED_BYTES) {
                Some((mime, data)) => Some(BinaryAttachment { mime, data }),
                None => {
                    tracing::warn!(%url, "{provider}: image_url dropped (data URI parse failed or exceeds {}MB)", MAX_DECODED_BYTES / 1024 / 1024);
                    None
                }
            }
        }
        "input_audio" => {
            let audio = part.get("input_audio")?;
            let data = audio.get("data").and_then(|d| d.as_str()).unwrap_or("");
            let format = audio.get("format").and_then(|f| f.as_str()).unwrap_or("wav");
            if data.len() > MAX_DECODED_BYTES {
                tracing::warn!(data_len = data.len(), format, "{provider}: input_audio dropped (exceeds {}MB)", MAX_DECODED_BYTES / 1024 / 1024);
                return None;
            }
            Some(BinaryAttachment { mime: format!("audio/{}", format), data: data.to_string() })
        }
        "file" => {
            let file = part.get("file")?;
            let file_data = file.get("file_data").and_then(|d| d.as_str()).unwrap_or("");
            let filename = file.get("filename").and_then(|f| f.as_str()).unwrap_or("");
            if file_data.len() > MAX_DECODED_BYTES {
                tracing::warn!(data_len = file_data.len(), %filename, "{provider}: file dropped (exceeds {}MB)", MAX_DECODED_BYTES / 1024 / 1024);
                return None;
            }
            Some(BinaryAttachment { mime: mime_from_filename(filename), data: file_data.to_string() })
        }
        _ => None,
    }
}

/// Convert a single OpenAI content part to an Anthropic block.
fn openai_part_to_anthropic(part: &serde_json::Value) -> Option<serde_json::Value> {
    let part_type = part.get("type").and_then(|t| t.as_str()).unwrap_or("");

    let text = part
        .get("text")
        .and_then(|t| t.as_str())
        .or_else(|| part.get("content").and_then(|t| t.as_str()));
    if let Some(s) = text {
        if part_type.is_empty() || part_type == "text" {
            return Some(serde_json::json!({"type": "text", "text": s}));
        }
    }

    if let Some(att) = extract_binary_attachment(part, "anthropic") {
        let block_type = if part_type == "image_url" { "image" } else { "document" };
        return Some(serde_json::json!({
            "type": block_type,
            "source": {"type": "base64", "media_type": att.mime, "data": att.data}
        }));
    }
    // If we got here and part_type was recognized (image/audio/file), the warn was logged by extract_binary_attachment.
    if matches!(part_type, "image_url" | "input_audio" | "file") {
        return None;
    }

    // Fallback: plain text
    if let Some(s) = text {
        if s.len() > MAX_DECODED_BYTES {
            tracing::warn!(text_len = s.len(), "anthropic: fallback text block dropped (exceeds {}MB)", MAX_DECODED_BYTES / 1024 / 1024);
            return None;
        }
        return Some(serde_json::json!({"type": "text", "text": s}));
    }

    None
}

/// Convert OpenAI-format content (string or array) to Gemini parts.
fn content_to_gemini_parts(content: &serde_json::Value) -> Vec<serde_json::Value> {
    match content {
        serde_json::Value::Array(parts) => parts
            .iter()
            .filter_map(|part| openai_part_to_gemini(part))
            .collect(),
        serde_json::Value::String(s) => {
            if s.len() > MAX_DECODED_BYTES {
                tracing::warn!(text_len = s.len(), "gemini: text content dropped (exceeds {}MB)", MAX_DECODED_BYTES / 1024 / 1024);
                return vec![];
            }
            vec![serde_json::json!({"text": s})]
        }
        _ => {
            let text = content_to_text(content);
            if text.is_empty() { return vec![]; }
            if text.len() > MAX_DECODED_BYTES {
                tracing::warn!(text_len = text.len(), "gemini: fallback text content dropped (exceeds {}MB)", MAX_DECODED_BYTES / 1024 / 1024);
                return vec![];
            }
            vec![serde_json::json!({"text": text})]
        }
    }
}

/// Convert a single OpenAI content part to a Gemini part.
fn openai_part_to_gemini(part: &serde_json::Value) -> Option<serde_json::Value> {
    let part_type = part.get("type").and_then(|t| t.as_str()).unwrap_or("");

    let text = part
        .get("text")
        .and_then(|t| t.as_str())
        .or_else(|| part.get("content").and_then(|t| t.as_str()));
    if let Some(s) = text {
        if part_type.is_empty() || part_type == "text" {
            return Some(serde_json::json!({"text": s}));
        }
    }

    if let Some(att) = extract_binary_attachment(part, "gemini") {
        return Some(serde_json::json!({"inlineData": {"mimeType": att.mime, "data": att.data}}));
    }
    if matches!(part_type, "image_url" | "input_audio" | "file") {
        return None;
    }

    None
}

/// Parse a data URI "data:<mime>;base64,<data>" → (mime, data).
/// Returns None if the decoded data exceeds `max_bytes`.
fn parse_data_uri(uri: &str, max_bytes: usize) -> Option<(String, String)> {
    let stripped = uri.strip_prefix("data:")?;
    let (mime_and_encoding, data) = stripped.split_once(',')?;
    let mime = mime_and_encoding.trim_end_matches(";base64").trim_end_matches(";BASE64");
    if mime.is_empty() || data.is_empty() {
        return None;
    }
    if data.len() > max_bytes {
        return None;
    }
    Some((mime.to_string(), data.to_string()))
}

/// Map a filename extension to MIME type. Falls back to application/octet-stream.
fn mime_from_filename(filename: &str) -> String {
    let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "pdf" => "application/pdf".into(),
        "doc" => "application/msword".into(),
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document".into(),
        "xls" => "application/vnd.ms-excel".into(),
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".into(),
        "ppt" => "application/vnd.ms-powerpoint".into(),
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation".into(),
        "txt" => "text/plain".into(),
        "csv" => "text/csv".into(),
        "html" | "htm" => "text/html".into(),
        "json" => "application/json".into(),
        "xml" => "application/xml".into(),
        "zip" => "application/zip".into(),
        "mp3" => "audio/mpeg".into(),
        "mp4" => "video/mp4".into(),
        "wav" => "audio/wav".into(),
        "ogg" => "audio/ogg".into(),
        "webm" => "video/webm".into(),
        "png" => "image/png".into(),
        "jpg" | "jpeg" => "image/jpeg".into(),
        "gif" => "image/gif".into(),
        "webp" => "image/webp".into(),
        "svg" => "image/svg+xml".into(),
        _ => "application/octet-stream".into(),
    }
}

async fn transform_json_response(
    up_resp: Response<Body>,
    model: Option<String>,
    f: fn(&serde_json::Value, Option<String>) -> serde_json::Value,
) -> Response<Body> {
    let (mut parts, body) = up_resp.into_parts();
    parts.headers.remove(CONTENT_LENGTH);
    parts.headers.remove(CONTENT_ENCODING);
    parts.headers.insert(
        CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    let body = match hyper::body::to_bytes(body).await {
        Ok(body) => body,
        Err(_) => {
            parts.status = http::StatusCode::BAD_GATEWAY;
            return Response::from_parts(
                parts,
                Body::from(r#"{"error":{"message":"failed to read upstream response"}}"#),
            );
        }
    };
    if !parts.status.is_success() {
        let error_msg = extract_upstream_error(&body);
        let error_body = serde_json::json!({
            "error": {
                "message": error_msg,
                "type": "upstream_error",
                "code": parts.status.as_u16()
            }
        });
        return Response::from_parts(parts, Body::from(error_body.to_string()));
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "upstream JSON parse failed, returning raw body");
            return Response::from_parts(parts, Body::from(body));
        }
    };
    let out = f(&value, model);
    Response::from_parts(parts, Body::from(out.to_string()))
}

fn transform_sse_response(
    up_resp: Response<Body>,
    model: Option<String>,
    f: fn(&serde_json::Value, Option<&str>) -> Vec<serde_json::Value>,
) -> Response<Body> {
    let (mut parts, body) = up_resp.into_parts();
    parts.headers.remove(CONTENT_LENGTH);
    parts.headers.remove(CONTENT_ENCODING);
    parts.headers.insert(
        CONTENT_TYPE,
        http::HeaderValue::from_static("text/event-stream"),
    );
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, io::Error>>(32);
    tokio::spawn(async move {
        use hyper::body::HttpBody;
        let mut body = body;
        let mut buf = String::new();
        while let Some(chunk) = body.data().await {
            let Ok(chunk) = chunk else {
                break;
            };
            buf.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(pos) = buf.find('\n') {
                let line = buf[..pos].trim_end_matches('\r').to_string();
                buf.drain(..=pos);
                let Some(data) = line.strip_prefix("data:") else {
                    continue;
                };
                let data = data.trim();
                if data.is_empty() || data == "[DONE]" {
                    continue;
                }
                let Ok(value) = serde_json::from_str::<serde_json::Value>(data) else {
                    continue;
                };
                for chunk in f(&value, model.as_deref()) {
                    let msg = format!("data: {}\n\n", chunk);
                    if tx.send(Ok(Bytes::from(msg))).await.is_err() {
                        return;
                    }
                }
            }
            if buf.len() > 1024 * 1024 {
                buf.clear();
            }
        }
        let _ = tx.send(Ok(Bytes::from_static(b"data: [DONE]\n\n"))).await;
    });
    Response::from_parts(parts, Body::wrap_stream(ReceiverStream::new(rx)))
}

fn anthropic_json_to_openai(v: &serde_json::Value, model: Option<String>) -> serde_json::Value {
    let id = v
        .get("id")
        .and_then(|s| s.as_str())
        .unwrap_or("chatcmpl-anthropic");
    let model = v
        .get("model")
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
        .or(model)
        .unwrap_or_default();
    let content = v
        .get("content")
        .and_then(|c| c.as_array())
        .map(|parts| {
            parts
                .iter()
                .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default();
    let input = v
        .get("usage")
        .and_then(|u| u.get("input_tokens"))
        .and_then(|n| n.as_u64())
        .unwrap_or(0);
    let output = v
        .get("usage")
        .and_then(|u| u.get("output_tokens"))
        .and_then(|n| n.as_u64())
        .unwrap_or(0);
    chat_completion_json(id, &model, content, input, output)
}

fn gemini_json_to_openai(v: &serde_json::Value, model: Option<String>) -> serde_json::Value {
    let model = model.unwrap_or_default();
    let candidate = v
        .get("candidates")
        .and_then(|c| c.as_array())
        .and_then(|a| a.first())
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let content = candidate
        .get("content")
        .and_then(|c| c.get("parts"))
        .and_then(|p| p.as_array())
        .map(|parts| {
            parts
                .iter()
                .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default();
    let prompt = v
        .get("usageMetadata")
        .and_then(|u| u.get("promptTokenCount"))
        .and_then(|n| n.as_u64())
        .unwrap_or(0);
    let candidates = v
        .get("usageMetadata")
        .and_then(|u| u.get("candidatesTokenCount"))
        .and_then(|n| n.as_u64())
        .unwrap_or(0);
    let thought = v
        .get("usageMetadata")
        .and_then(|u| u.get("thoughtsTokenCount"))
        .and_then(|n| n.as_u64())
        .unwrap_or(0);
    // completion_tokens = candidates + thoughts, treating all output as "completion".
    let completion = candidates.saturating_add(thought);
    let total = v
        .get("usageMetadata")
        .and_then(|u| u.get("totalTokenCount"))
        .and_then(|n| n.as_u64())
        .unwrap_or(prompt + completion);
    let mut resp = chat_completion_json("chatcmpl-gemini", &model, content, prompt, completion);
    resp["usage"]["thought_tokens"] = serde_json::json!(thought);
    resp["usage"]["total_tokens"] = serde_json::json!(total);
    resp
}

fn chat_completion_json(
    id: &str,
    model: &str,
    content: String,
    prompt_tokens: u64,
    completion_tokens: u64,
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "object": "chat.completion",
        "created": unix_secs(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens
        }
    })
}

fn anthropic_sse_to_openai(v: &serde_json::Value, model: Option<&str>) -> Vec<serde_json::Value> {
    let ty = v.get("type").and_then(|s| s.as_str()).unwrap_or("");
    match ty {
        "message_start" => {
            let id = v
                .get("message")
                .and_then(|m| m.get("id"))
                .and_then(|s| s.as_str())
                .unwrap_or("chatcmpl-anthropic");
            let model_str = v
                .get("message")
                .and_then(|m| m.get("model"))
                .and_then(|s| s.as_str())
                .unwrap_or(model.unwrap_or(""));
            vec![chat_chunk_json(
                id,
                model_str,
                serde_json::json!({"role": "assistant", "content": ""}),
                None,
                None,
            )]
        }
        "content_block_delta" => {
            let text = v
                .get("delta")
                .and_then(|d| d.get("text"))
                .and_then(|s| s.as_str())
                .unwrap_or("");
            if text.is_empty() {
                Vec::new()
            } else {
                vec![chat_chunk_json(
                    "chatcmpl-anthropic",
                    model.unwrap_or(""),
                    serde_json::json!({"content": text}),
                    None,
                    None,
                )]
            }
        }
        "message_delta" => {
            let usage = v.get("usage").map(|u| {
                let output = u.get("output_tokens").and_then(|n| n.as_u64()).unwrap_or(0);
                serde_json::json!({
                    "prompt_tokens": 0,
                    "completion_tokens": output,
                    "total_tokens": output
                })
            });
            usage
                .map(|usage| {
                    vec![chat_chunk_json(
                        "chatcmpl-anthropic",
                        model.unwrap_or(""),
                        serde_json::json!({}),
                        None,
                        Some(usage),
                    )]
                })
                .unwrap_or_default()
        }
        "message_stop" => vec![chat_chunk_json(
            "chatcmpl-anthropic",
            model.unwrap_or(""),
            serde_json::json!({}),
            Some("stop"),
            None,
        )],
        _ => Vec::new(),
    }
}

fn gemini_sse_to_openai(v: &serde_json::Value, model: Option<&str>) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    if let Some(candidate) = v
        .get("candidates")
        .and_then(|c| c.as_array())
        .and_then(|a| a.first())
    {
        let text = candidate
            .get("content")
            .and_then(|c| c.get("parts"))
            .and_then(|p| p.as_array())
            .map(|parts| {
                parts
                    .iter()
                    .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default();
        if !text.is_empty() {
            out.push(chat_chunk_json(
                "chatcmpl-gemini",
                model.unwrap_or(""),
                serde_json::json!({"content": text}),
                None,
                None,
            ));
        }
        if candidate.get("finishReason").is_some() {
            out.push(chat_chunk_json(
                "chatcmpl-gemini",
                model.unwrap_or(""),
                serde_json::json!({}),
                Some("stop"),
                None,
            ));
        }
    }
    if let Some(usage) = v.get("usageMetadata") {
        let prompt = usage
            .get("promptTokenCount")
            .and_then(|n| n.as_u64())
            .unwrap_or(0);
        let completion = usage
            .get("candidatesTokenCount")
            .and_then(|n| n.as_u64())
            .unwrap_or(0);
        let thought = usage
            .get("thoughtsTokenCount")
            .and_then(|n| n.as_u64())
            .unwrap_or(0);
        // Use Gemini's native totalTokenCount which includes thoughts.
        // Fallback to sum if missing.
        let total = usage
            .get("totalTokenCount")
            .and_then(|n| n.as_u64())
            .unwrap_or(prompt + completion + thought);
        out.push(chat_chunk_json(
            "chatcmpl-gemini",
            model.unwrap_or(""),
            serde_json::json!({}),
            None,
            Some(serde_json::json!({
                "prompt_tokens": prompt,
                "completion_tokens": completion,
                "thought_tokens": thought,
                "total_tokens": total
            })),
        ));
    }
    out
}

/// Extract a human-readable error message from an upstream error response.
fn extract_upstream_error(body: &[u8]) -> String {
    let v: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return String::from_utf8_lossy(body).into_owned(),
    };
    // Anthropic / Gemini / OpenAI all use error.message
    if let Some(msg) = v.get("error").and_then(|e| e.get("message")).and_then(|m| m.as_str()) {
        return msg.to_string();
    }
    // Generic fallback: error object as string
    v.get("error")
        .and_then(|e| e.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown upstream error".to_string())
}

fn chat_chunk_json(
    id: &str,
    model: &str,
    delta: serde_json::Value,
    finish_reason: Option<&str>,
    usage: Option<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": unix_secs(),
        "model": model,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish_reason
        }],
        "usage": usage
    })
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn format_error(status: http::StatusCode, message: &str, code: &str) -> Response<Body> {
    let body = serde_json::json!({
        "error": {
            "message": message,
            "type": "proxy_error",
            "param": null,
            "code": code
        }
    });
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap_or_else(|_| Response::new(Body::from("proxy_error")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anthropic_request_moves_system_and_messages() {
        let body = Bytes::from_static(
            br#"{"model":"claude-3","messages":[{"role":"system","content":"sys"},{"role":"user","content":"hi"}],"max_tokens":7,"stream":true}"#,
        );
        let adapted = adapt_request(
            UpstreamFormat::Anthropic,
            &"/v1/chat/completions".parse().unwrap(),
            &Method::POST,
            &body,
            "claude-3",
            "sk-ant-test",
        )
        .unwrap();
        assert_eq!(adapted.path_and_query.as_str(), "/v1/messages");
        let v: serde_json::Value = serde_json::from_slice(&adapted.body).unwrap();
        assert_eq!(v["system"], "sys");
        assert_eq!(v["messages"][0]["role"], "user");
        assert_eq!(v["max_tokens"], 7);
    }

    #[test]
    fn gemini_request_uses_generate_content_path() {
        let body = Bytes::from_static(
            br#"{"model":"gemini-1.5-pro","messages":[{"role":"user","content":"hi"}],"stream":false}"#,
        );
        let adapted = adapt_request(
            UpstreamFormat::Gemini,
            &"/v1/chat/completions".parse().unwrap(),
            &Method::POST,
            &body,
            "gemini-1.5-pro",
            "AIza test",
        )
        .unwrap();
        assert!(adapted
            .path_and_query
            .as_str()
            .starts_with("/v1beta/models/gemini-1.5-pro:generateContent?key="));
        let v: serde_json::Value = serde_json::from_slice(&adapted.body).unwrap();
        assert_eq!(v["contents"][0]["role"], "user");
    }

    /// Round-trip: Gemini JSON → OpenAI format → serialize → parse → verify usage fields.
    #[test]
    fn gemini_json_to_openai_produces_correct_usage() {
        let gemini_resp = serde_json::json!({
            "candidates": [{
                "content": {
                    "parts": [{"text": "Hello from Gemini"}],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "usageMetadata": {
                "promptTokenCount": 15,
                "candidatesTokenCount": 25,
                "thoughtsTokenCount": 5,
                "totalTokenCount": 45
            },
            "modelVersion": "gemini-2.0-flash"
        });

        let converted = gemini_json_to_openai(&gemini_resp, Some("gemini-2.0-flash".to_string()));

        assert_eq!(converted["usage"]["prompt_tokens"], 15);
        assert_eq!(converted["usage"]["completion_tokens"], 30); // 25 + 5 thoughts
        assert_eq!(converted["usage"]["thought_tokens"], 5);
        assert_eq!(converted["usage"]["total_tokens"], 45);

        // Serialize → parse back (simulates HTTP body round-trip)
        let serialized = converted.to_string();
        let parsed: serde_json::Value =
            serde_json::from_str(&serialized).expect("should parse serialized JSON");

        assert_eq!(parsed["usage"]["prompt_tokens"], 15);
        assert_eq!(parsed["usage"]["completion_tokens"], 30);
        assert_eq!(parsed["usage"]["thought_tokens"], 5);
        assert_eq!(parsed["usage"]["total_tokens"], 45);
    }

    /// Round-trip: Anthropic JSON → OpenAI format → serialize → parse → verify usage fields.
    #[test]
    fn anthropic_json_to_openai_produces_correct_usage() {
        let anthropic_resp = serde_json::json!({
            "id": "msg_xxx",
            "type": "message",
            "role": "assistant",
            "model": "claude-sonnet-4-20250514",
            "content": [{"type": "text", "text": "Hello from Claude"}],
            "usage": {
                "input_tokens": 12,
                "output_tokens": 34
            }
        });

        let converted =
            anthropic_json_to_openai(&anthropic_resp, Some("claude-sonnet-4-20250514".to_string()));

        assert_eq!(converted["usage"]["prompt_tokens"], 12);
        assert_eq!(converted["usage"]["completion_tokens"], 34);
        assert_eq!(converted["usage"]["total_tokens"], 46);

        let serialized = converted.to_string();
        let parsed: serde_json::Value =
            serde_json::from_str(&serialized).expect("should parse serialized JSON");

        assert_eq!(parsed["usage"]["prompt_tokens"], 12);
        assert_eq!(parsed["usage"]["completion_tokens"], 34);
        assert_eq!(parsed["usage"]["total_tokens"], 46);
    }

    /// Edge case: Gemini safety-blocked response still has usage metadata.
    #[test]
    fn gemini_safety_blocked_still_produces_usage() {
        let gemini_resp = serde_json::json!({
            "candidates": [{
                "finishReason": "SAFETY",
                "safetyRatings": [{"category": "HARM_CATEGORY_HARASSMENT", "probability": "HIGH"}]
            }],
            "usageMetadata": {
                "promptTokenCount": 8,
                "totalTokenCount": 8
            },
            "modelVersion": "gemini-2.0-flash"
        });

        let converted = gemini_json_to_openai(&gemini_resp, Some("gemini-2.0-flash".to_string()));

        let serialized = converted.to_string();
        let parsed: serde_json::Value =
            serde_json::from_str(&serialized).expect("should parse");

        assert_eq!(parsed["usage"]["prompt_tokens"], 8);
        assert_eq!(parsed["usage"]["completion_tokens"], 0); // no candidatesTokenCount
        assert_eq!(parsed["usage"]["total_tokens"], 8);
    }

    /// Edge case: Anthropic response with 0 tokens.
    #[test]
    fn anthropic_zero_tokens_produces_correct_usage() {
        let anthropic_resp = serde_json::json!({
            "id": "msg_xxx",
            "type": "message",
            "role": "assistant",
            "model": "claude-sonnet-4-20250514",
            "content": [{"type": "text", "text": ""}],
            "usage": {
                "input_tokens": 0,
                "output_tokens": 0
            }
        });

        let converted =
            anthropic_json_to_openai(&anthropic_resp, Some("claude-sonnet-4-20250514".to_string()));

        let serialized = converted.to_string();
        let parsed: serde_json::Value =
            serde_json::from_str(&serialized).expect("should parse");

        assert_eq!(parsed["usage"]["prompt_tokens"], 0);
        assert_eq!(parsed["usage"]["completion_tokens"], 0);
        assert_eq!(parsed["usage"]["total_tokens"], 0);
    }

}
