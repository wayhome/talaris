//! 最小 HTTP/1.1 client codec
//!
//! 只覆盖 v1 需要的两件事：
//! 1. 编码 `GET / HTTP/1.1` 请求（用于 WS Upgrade）
//! 2. 解析响应的 status line + headers（用于 WS Upgrade 校验 + 未来 REST）
//!
//! 不做：chunked transfer encoding、HTTP/2、redirect、cookie。Body 处理留给
//! caller（WS upgrade 的 101 没 body；REST 用 Content-Length 切）。

#![allow(clippy::cast_possible_truncation)]

use std::fmt::Write as _;
use thiserror::Error;

/// 单个 response 头部总字节数上限。超过即拒，防止恶意 server 永远不发
/// `\r\n\r\n` 让 `find_double_crlf` 每次 feed 都 O(N) 重新扫一遍，整体退化为
/// O(N²)，同时 `Vec<(&str,&str)>` 无界增长。16 KiB 是 nginx / Apache 默认值。
pub const MAX_RESPONSE_HEADER_BYTES: usize = 16 * 1024;

/// 单个 response 头数量上限。RFC 没强制，常见实现 ≤ 100；这里取 64。
pub const MAX_RESPONSE_HEADER_COUNT: usize = 64;

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("malformed status line")]
    BadStatusLine,
    #[error("unsupported HTTP response version")]
    UnsupportedVersion,
    #[error("malformed header line")]
    BadHeader,
    #[error("response not utf-8")]
    NotUtf8,
    #[error("response headers exceeded {limit} bytes")]
    HeadersTooLarge { limit: usize },
    #[error("response had more than {limit} headers")]
    TooManyHeaders { limit: usize },
    #[error("response uses Transfer-Encoding; not supported")]
    UnsupportedTransferEncoding,
}

/// Request builder（caller 拼好传给 [`encode_request`]）
#[derive(Debug)]
pub struct Http1Builder<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub host: &'a str,
    pub headers: Vec<(&'a str, &'a str)>,
}

/// 解析后的 response 视图（借用输入 buffer）
#[derive(Debug)]
pub struct Http1Response<'a> {
    pub version: &'a str,
    pub status: u16,
    pub reason: &'a str,
    pub headers: Vec<(&'a str, &'a str)>,
    /// `header_end` 是 \r\n\r\n 之后第一个 body 字节的 offset；caller 据此切 body
    pub header_end: usize,
}

impl<'a> Http1Response<'a> {
    /// 大小写不敏感查 header
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&'a str> {
        self.header_values(name).next()
    }

    /// 大小写不敏感遍历所有同名 header。
    pub fn header_values<'b>(&'b self, name: &'b str) -> impl Iterator<Item = &'a str> + 'b {
        self.headers
            .iter()
            .filter(move |(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| *v)
    }
}

/// 编码 request 到 `buf`（append）
///
/// 输出形如：
/// ```text
/// GET /ws/api/v2 HTTP/1.1\r\n
/// Host: www.deribit.com\r\n
/// Upgrade: websocket\r\n
/// ...
/// \r\n
/// ```
pub fn encode_request(buf: &mut Vec<u8>, b: &Http1Builder<'_>) {
    // safety: writing to Vec<u8> via fmt::Write through a wrapper; ignore fmt errors
    // (impossible for Vec).
    let _ = write!(VecFmt(buf), "{} {} HTTP/1.1\r\n", b.method, b.path);
    let _ = write!(VecFmt(buf), "Host: {}\r\n", b.host);
    for (n, v) in &b.headers {
        let _ = write!(VecFmt(buf), "{n}: {v}\r\n");
    }
    buf.extend_from_slice(b"\r\n");
}

/// 把 fmt::Write 适配到 Vec<u8>
struct VecFmt<'a>(&'a mut Vec<u8>);
impl std::fmt::Write for VecFmt<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

/// 解析 response headers。
///
/// 返回值：
/// - `Ok(None)`：还没收到完整的 \r\n\r\n，需要再 feed
/// - `Ok(Some((response, header_end)))`：成功；header_end 是 body 起始位置
/// - `Err(_)`：协议错误
pub fn parse_response(input: &[u8]) -> Result<Option<(Http1Response<'_>, usize)>, HttpError> {
    // 在找 \r\n\r\n 之前先卡 size：恶意 server 故意不发 terminator，每次 feed
    // 都让 find_double_crlf 重新扫整个 buffer。
    let search_window = input.len().min(MAX_RESPONSE_HEADER_BYTES);
    let Some(header_end) = find_double_crlf(&input[..search_window]) else {
        if input.len() > MAX_RESPONSE_HEADER_BYTES {
            return Err(HttpError::HeadersTooLarge {
                limit: MAX_RESPONSE_HEADER_BYTES,
            });
        }
        return Ok(None);
    };

    let header_bytes = &input[..header_end];
    let s = std::str::from_utf8(header_bytes).map_err(|_| HttpError::NotUtf8)?;
    let mut lines = s.split("\r\n");

    let status_line = lines.next().ok_or(HttpError::BadStatusLine)?;
    let (version, status, reason) = parse_status_line(status_line)?;

    let mut headers: Vec<(&str, &str)> = Vec::with_capacity(8);
    for line in lines {
        if line.is_empty() {
            break;
        }
        if headers.len() >= MAX_RESPONSE_HEADER_COUNT {
            return Err(HttpError::TooManyHeaders {
                limit: MAX_RESPONSE_HEADER_COUNT,
            });
        }
        let (name, value) = line.split_once(':').ok_or(HttpError::BadHeader)?;
        // RFC 7230 §3.2.4: 不允许 name 与 ':' 之间有 whitespace；name 自身也不能含。
        // 拒掉 "Foo : bar" 这种 smuggling 形态。
        if name.is_empty() || name.bytes().any(|b| matches!(b, b' ' | b'\t')) {
            return Err(HttpError::BadHeader);
        }
        let trimmed_name = name; // already validated no surrounding ws
        let trimmed_value = value.trim();
        // 显式拒 Transfer-Encoding（doc 早就声明不支持 chunked；这里把"声明"变成
        // 强校验，免得 caller 把 chunked body 当 body bytes 处理）
        if trimmed_name.eq_ignore_ascii_case("Transfer-Encoding")
            && !trimmed_value.eq_ignore_ascii_case("identity")
        {
            return Err(HttpError::UnsupportedTransferEncoding);
        }
        headers.push((trimmed_name, trimmed_value));
    }

    let body_start = header_end + 4; // past \r\n\r\n
    Ok(Some((
        Http1Response {
            version,
            status,
            reason,
            headers,
            header_end: body_start,
        },
        body_start,
    )))
}

fn find_double_crlf(input: &[u8]) -> Option<usize> {
    input.windows(4).position(|w| w == b"\r\n\r\n")
}

fn parse_status_line(line: &str) -> Result<(&str, u16, &str), HttpError> {
    // HTTP/1.1 NNN Reason Phrase
    let mut parts = line.splitn(3, ' ');
    let version = parts.next().ok_or(HttpError::BadStatusLine)?;
    if version != "HTTP/1.1" {
        return Err(HttpError::UnsupportedVersion);
    }
    let code_str = parts.next().ok_or(HttpError::BadStatusLine)?;
    let reason = parts.next().unwrap_or("");
    let code: u16 = code_str.parse().map_err(|_| HttpError::BadStatusLine)?;
    Ok((version, code, reason))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn encode_get_upgrade() {
        let mut buf = Vec::new();
        let b = Http1Builder {
            method: "GET",
            path: "/ws/api/v2",
            host: "www.deribit.com",
            headers: vec![
                ("Upgrade", "websocket"),
                ("Connection", "Upgrade"),
                ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
                ("Sec-WebSocket-Version", "13"),
            ],
        };
        encode_request(&mut buf, &b);
        let s = std::str::from_utf8(&buf).unwrap();
        assert!(s.starts_with("GET /ws/api/v2 HTTP/1.1\r\n"));
        assert!(s.contains("Host: www.deribit.com\r\n"));
        assert!(s.contains("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"));
        assert!(s.ends_with("\r\n\r\n"));
    }

    #[test]
    fn parse_101_response() {
        let resp = b"HTTP/1.1 101 Switching Protocols\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\
            \r\n";
        let (r, end) = parse_response(resp).unwrap().unwrap();
        assert_eq!(r.version, "HTTP/1.1");
        assert_eq!(r.status, 101);
        assert_eq!(r.reason, "Switching Protocols");
        assert_eq!(r.header("upgrade"), Some("websocket"));
        assert_eq!(r.header("Connection"), Some("Upgrade"));
        assert_eq!(
            r.header("Sec-WebSocket-Accept"),
            Some("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
        );
        assert_eq!(end, resp.len());
    }

    #[test]
    fn partial_response_returns_none() {
        // Missing final \r\n\r\n
        let resp = b"HTTP/1.1 101 OK\r\nUpgrade: websocket\r\n";
        assert!(parse_response(resp).unwrap().is_none());
    }

    #[test]
    fn malformed_status_rejected() {
        let resp = b"BOGUS\r\n\r\n";
        assert!(parse_response(resp).is_err());
    }

    #[test]
    fn unsupported_status_version_rejected() {
        let resp = b"HTTP/1.0 101 Switching Protocols\r\n\r\n";
        let err = parse_response(resp).unwrap_err();
        assert!(matches!(err, HttpError::UnsupportedVersion));

        let resp = b"BOGUS 101 Switching Protocols\r\n\r\n";
        let err = parse_response(resp).unwrap_err();
        assert!(matches!(err, HttpError::UnsupportedVersion));
    }

    #[test]
    fn header_values_iterates_duplicates() {
        let resp = b"HTTP/1.1 101 Switching Protocols\r\n\
            Connection: keep-alive\r\n\
            Connection: Upgrade\r\n\
            \r\n";
        let (r, _) = parse_response(resp).unwrap().unwrap();
        let values: Vec<&str> = r.header_values("connection").collect();
        assert_eq!(values, vec!["keep-alive", "Upgrade"]);
        assert_eq!(r.header("Connection"), Some("keep-alive"));
    }

    #[test]
    fn oversize_headers_rejected() {
        // 一坨没有 \r\n\r\n 终止的大 buffer
        let mut buf = Vec::with_capacity(MAX_RESPONSE_HEADER_BYTES + 100);
        buf.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
        while buf.len() < MAX_RESPONSE_HEADER_BYTES + 50 {
            buf.extend_from_slice(b"X-Filler: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n");
        }
        let err = parse_response(&buf).unwrap_err();
        assert!(matches!(err, HttpError::HeadersTooLarge { .. }));
    }

    #[test]
    fn too_many_headers_rejected() {
        let mut buf = String::from("HTTP/1.1 200 OK\r\n");
        for i in 0..=MAX_RESPONSE_HEADER_COUNT {
            buf.push_str(&format!("X-{i}: v\r\n"));
        }
        buf.push_str("\r\n");
        let err = parse_response(buf.as_bytes()).unwrap_err();
        assert!(matches!(err, HttpError::TooManyHeaders { .. }));
    }

    #[test]
    fn transfer_encoding_chunked_rejected() {
        let resp =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: text/plain\r\n\r\n";
        let err = parse_response(resp).unwrap_err();
        assert!(matches!(err, HttpError::UnsupportedTransferEncoding));
    }

    #[test]
    fn transfer_encoding_identity_accepted() {
        let resp = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: identity\r\n\r\n";
        assert!(parse_response(resp).is_ok());
    }

    #[test]
    fn header_with_ws_before_colon_rejected() {
        // RFC 7230 §3.2.4 禁止 name 与 ':' 之间有空白
        let resp = b"HTTP/1.1 200 OK\r\nFoo : bar\r\n\r\n";
        let err = parse_response(resp).unwrap_err();
        assert!(matches!(err, HttpError::BadHeader));
    }

    #[test]
    fn partial_below_cap_still_returns_none() {
        // 短 buffer 没 \r\n\r\n 应该返 None（需要更多数据），不是 error
        let resp = b"HTTP/1.1 101";
        assert!(parse_response(resp).unwrap().is_none());
    }
}
