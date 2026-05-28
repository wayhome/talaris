//! RFC 6455 §4 Client-side opening handshake
//!
//! 三件事：
//! 1. 生成 16 字节随机 Sec-WebSocket-Key（base64 编码，24 字符）
//! 2. 编码 GET Upgrade 请求字节
//! 3. 校验 server 返回的 101 响应（status / Upgrade / Connection / Sec-WebSocket-Accept）
//!
//! Sec-WebSocket-Accept = `base64(sha1(client_key + GUID))`，
//! GUID = `258EAFA5-E914-47DA-95CA-C5AB0DC85B11`（RFC §4.2.2）。

use crate::http::{Http1Builder, Http1Response, encode_request};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use sha1::{Digest, Sha1};
use thiserror::Error;

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[derive(Debug, Error)]
pub enum HandshakeError {
    #[error("RNG failed to produce client key")]
    RngFailure,
    #[error("server returned status {0}, expected 101")]
    BadStatus(u16),
    #[error("missing required header: {0}")]
    MissingHeader(&'static str),
    #[error("Upgrade header is not 'websocket'")]
    BadUpgrade,
    #[error("Connection header does not contain 'Upgrade'")]
    BadConnection,
    #[error("Sec-WebSocket-Accept verification failed")]
    BadAccept,
    /// 客户端 offer 了 subprotocols，但 server 回了一个不在 list 里的（或多个）。
    /// RFC §4.1 step 6 要求 client 必须 fail。
    #[error("server returned unexpected Sec-WebSocket-Protocol: {0}")]
    BadSubprotocol(String),
}

/// 生成 16 字节随机数并 base64 编码（24 字符 + "=="）
pub fn generate_key() -> Result<String, HandshakeError> {
    use ring::rand::SecureRandom;
    let mut raw = [0_u8; 16];
    let rng = ring::rand::SystemRandom::new();
    rng.fill(&mut raw).map_err(|_| HandshakeError::RngFailure)?;
    Ok(B64.encode(raw))
}

/// 算 `base64(sha1(client_key + GUID))`
#[must_use]
pub fn compute_accept(client_key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(client_key.as_bytes());
    hasher.update(WS_GUID.as_bytes());
    let digest = hasher.finalize();
    B64.encode(digest)
}

#[must_use]
pub fn verify_accept(client_key: &str, server_accept: &str) -> bool {
    compute_accept(client_key) == server_accept
}

/// Upgrade 请求参数
#[derive(Debug)]
pub struct UpgradeRequest<'a> {
    pub host: &'a str,
    pub path: &'a str,
    pub key: &'a str,
    pub subprotocols: &'a [&'a str],
    pub origin: Option<&'a str>,
}

/// 编码 GET Upgrade 请求字节到 `buf`
pub fn encode_upgrade_request(buf: &mut Vec<u8>, req: &UpgradeRequest<'_>) {
    let subproto_joined: String = req.subprotocols.join(", ");

    let mut headers: Vec<(&str, &str)> = vec![
        ("Upgrade", "websocket"),
        ("Connection", "Upgrade"),
        ("Sec-WebSocket-Key", req.key),
        ("Sec-WebSocket-Version", "13"),
    ];
    if !req.subprotocols.is_empty() {
        headers.push(("Sec-WebSocket-Protocol", subproto_joined.as_str()));
    }
    if let Some(o) = req.origin {
        headers.push(("Origin", o));
    }

    let b = Http1Builder {
        method: "GET",
        path: req.path,
        host: req.host,
        headers,
    };
    encode_request(buf, &b);
}

/// 校验 server 的 101 响应
///
/// `offered_subprotocols` 是 client `Sec-WebSocket-Protocol` 请求里列的候选；
/// 若为空表示 client 不在意 subprotocol。
///
/// 通过 = `Ok(())`；任一项失败返回对应 `HandshakeError`。
pub fn verify_upgrade_response(
    response: &Http1Response<'_>,
    client_key: &str,
    offered_subprotocols: &[&str],
) -> Result<(), HandshakeError> {
    if response.status != 101 {
        return Err(HandshakeError::BadStatus(response.status));
    }
    let upgrade = response
        .header("Upgrade")
        .ok_or(HandshakeError::MissingHeader("Upgrade"))?;
    if !upgrade.eq_ignore_ascii_case("websocket") {
        return Err(HandshakeError::BadUpgrade);
    }
    let connection = response
        .header("Connection")
        .ok_or(HandshakeError::MissingHeader("Connection"))?;
    let has_upgrade = connection
        .split(',')
        .any(|s| s.trim().eq_ignore_ascii_case("Upgrade"));
    if !has_upgrade {
        return Err(HandshakeError::BadConnection);
    }
    let accept = response
        .header("Sec-WebSocket-Accept")
        .ok_or(HandshakeError::MissingHeader("Sec-WebSocket-Accept"))?;
    if !verify_accept(client_key, accept) {
        return Err(HandshakeError::BadAccept);
    }
    // RFC §4.1 step 6：server 在 Sec-WebSocket-Protocol 里回的值必须是 client
    // offered 的某一项。client 没 offer 时，server 不应该回这个 header；
    // 如果它回了，按 RFC 也是 protocol violation。
    if let Some(server_proto) = response.header("Sec-WebSocket-Protocol") {
        let server_proto = server_proto.trim();
        if offered_subprotocols.is_empty() {
            return Err(HandshakeError::BadSubprotocol(server_proto.to_owned()));
        }
        // server 可以返回 single value（合法）或 0 value（header 缺）。返回多值
        // 是 protocol violation；这里宽松一点，只要 trim 后能在 offered 中找到
        // 即可（部分 server 实现会 echo 带空格的多值）。
        if !offered_subprotocols
            .iter()
            .any(|o| o.eq_ignore_ascii_case(server_proto))
        {
            return Err(HandshakeError::BadSubprotocol(server_proto.to_owned()));
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::http::parse_response;

    #[test]
    fn rfc_sample_accept() {
        // RFC 6455 §1.3 example
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let expected = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        assert_eq!(compute_accept(key), expected);
        assert!(verify_accept(key, expected));
    }

    #[test]
    fn generate_key_is_24_chars() {
        let k = generate_key().unwrap();
        assert_eq!(k.len(), 24);
        // Decodes to 16 bytes
        let raw = B64.decode(&k).unwrap();
        assert_eq!(raw.len(), 16);
    }

    #[test]
    fn upgrade_request_well_formed() {
        let mut buf = Vec::new();
        let req = UpgradeRequest {
            host: "www.deribit.com",
            path: "/ws/api/v2",
            key: "dGhlIHNhbXBsZSBub25jZQ==",
            subprotocols: &[],
            origin: None,
        };
        encode_upgrade_request(&mut buf, &req);
        let s = std::str::from_utf8(&buf).unwrap();
        assert!(s.starts_with("GET /ws/api/v2 HTTP/1.1\r\n"));
        assert!(s.contains("Host: www.deribit.com\r\n"));
        assert!(s.contains("Upgrade: websocket\r\n"));
        assert!(s.contains("Connection: Upgrade\r\n"));
        assert!(s.contains("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"));
        assert!(s.contains("Sec-WebSocket-Version: 13\r\n"));
    }

    #[test]
    fn verify_101_ok() {
        let resp = b"HTTP/1.1 101 Switching Protocols\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\
            \r\n";
        let (r, _) = parse_response(resp).unwrap().unwrap();
        verify_upgrade_response(&r, "dGhlIHNhbXBsZSBub25jZQ==", &[]).unwrap();
    }

    #[test]
    fn verify_rejects_bad_accept() {
        let resp = b"HTTP/1.1 101 OK\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Accept: AAAAAAAAAAAAAAAAAAAAAAAAAAA=\r\n\
            \r\n";
        let (r, _) = parse_response(resp).unwrap().unwrap();
        let err = verify_upgrade_response(&r, "dGhlIHNhbXBsZSBub25jZQ==", &[]).unwrap_err();
        assert!(matches!(err, HandshakeError::BadAccept));
    }

    #[test]
    fn verify_rejects_non_101() {
        let resp = b"HTTP/1.1 400 Bad Request\r\n\r\n";
        let (r, _) = parse_response(resp).unwrap().unwrap();
        let err = verify_upgrade_response(&r, "any", &[]).unwrap_err();
        assert!(matches!(err, HandshakeError::BadStatus(400)));
    }

    #[test]
    fn verify_rejects_unsolicited_subprotocol() {
        // client 没 offer 任何 subprotocol，但 server 回了一个
        let resp = b"HTTP/1.1 101 OK\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\
            Sec-WebSocket-Protocol: chat\r\n\
            \r\n";
        let (r, _) = parse_response(resp).unwrap().unwrap();
        let err = verify_upgrade_response(&r, "dGhlIHNhbXBsZSBub25jZQ==", &[]).unwrap_err();
        assert!(matches!(err, HandshakeError::BadSubprotocol(_)));
    }

    #[test]
    fn verify_rejects_subprotocol_not_in_offered() {
        let resp = b"HTTP/1.1 101 OK\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\
            Sec-WebSocket-Protocol: notice\r\n\
            \r\n";
        let (r, _) = parse_response(resp).unwrap().unwrap();
        let err = verify_upgrade_response(&r, "dGhlIHNhbXBsZSBub25jZQ==", &["chat", "echo"])
            .unwrap_err();
        assert!(matches!(err, HandshakeError::BadSubprotocol(_)));
    }

    #[test]
    fn verify_accepts_subprotocol_in_offered() {
        let resp = b"HTTP/1.1 101 OK\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\
            Sec-WebSocket-Protocol: chat\r\n\
            \r\n";
        let (r, _) = parse_response(resp).unwrap().unwrap();
        verify_upgrade_response(&r, "dGhlIHNhbXBsZSBub25jZQ==", &["chat", "echo"]).unwrap();
    }

    #[test]
    fn verify_connection_multi_value() {
        let resp = b"HTTP/1.1 101 OK\r\n\
            Upgrade: websocket\r\n\
            Connection: keep-alive, Upgrade\r\n\
            Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\
            \r\n";
        let (r, _) = parse_response(resp).unwrap().unwrap();
        verify_upgrade_response(&r, "dGhlIHNhbXBsZSBub25jZQ==", &[]).unwrap();
    }
}
