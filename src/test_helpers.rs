//! 单 crate 内测试辅助：单 client、单 frame 的极简 plain WS echo server。
//!
//! `connection.rs` / `pool.rs` 的测试都用这同一份 helper，避免重复实现。
//! 协议覆盖：HTTP upgrade → 收一帧 Text → echo Text → 收 Close → echo Close。
//! 不处理 fragmented、不处理 multi-message。

// 整个文件都是 `#[cfg(test)]` 下的 helper —— 大量 unwrap/expect/panic 是测试常态。
// dead_code 是因为 helper 只在 `#[cfg(all(test, target_os = "linux"))]` 的
// pool tests 用，macOS 端 test 跑不到（io_uring 走 stub.rs 的 unimplemented!）。
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    dead_code
)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use crate::ws::OpCode;
use crate::ws::frame::{MAX_HEADER_LEN, encode_header};
use crate::ws::handshake::compute_accept;
use crate::ws::mask::mask_inplace;

/// Accept 1 个 client、跑一遍 echo 流程后退出。`shutdown` 是 mpsc rx 的所有权
/// 占位（caller 持 tx，drop 时通知线程结束——但本 helper 不读这个 rx，单纯
/// 用生命周期标记）。
pub(crate) fn run_echo_server(listener: TcpListener, shutdown: std::sync::mpsc::Receiver<()>) {
    let (mut stream, _) = listener.accept().expect("accept");
    stream.set_nodelay(true).unwrap();
    let _ = shutdown;

    // ── HTTP upgrade ────────────────────────────────────────────────
    let mut buf = [0_u8; 4096];
    let mut req = Vec::new();
    loop {
        let n = stream.read(&mut buf).unwrap();
        assert!(n > 0, "client closed before sending request");
        req.extend_from_slice(&buf[..n]);
        if req.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let req_str = std::str::from_utf8(&req).unwrap();
    let key = req_str
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("sec-websocket-key:"))
        .and_then(|l| l.split(':').nth(1))
        .expect("Sec-WebSocket-Key header")
        .trim();
    let accept = compute_accept(key);
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream.write_all(resp.as_bytes()).unwrap();

    // ── 接收 1 个 masked text frame ────────────────────────────────
    let (opcode, payload) = read_one_frame(&mut stream);
    assert_eq!(opcode, OpCode::Text);

    // ── echo 同 payload，server-side 不 mask ────────────────────────
    let mut frame = vec![0_u8; MAX_HEADER_LEN];
    let hn = encode_header(&mut frame, true, OpCode::Text, None, payload.len() as u64);
    frame.truncate(hn);
    frame.extend_from_slice(&payload);
    stream.write_all(&frame).unwrap();

    // ── 等 client 主动 close（或直接退）────────────────────────────
    let (close_opcode, _) = read_one_frame(&mut stream);
    assert_eq!(close_opcode, OpCode::Close);
    let mut close_frame = vec![0_u8; MAX_HEADER_LEN + 2];
    let hn = encode_header(&mut close_frame, true, OpCode::Close, None, 2);
    close_frame.truncate(hn);
    close_frame.extend_from_slice(&1000_u16.to_be_bytes());
    let _ = stream.write_all(&close_frame);
}

/// 极简同步 frame 读取（masked 自动 unmask）。仅测试用——只够 echo 场景，
/// 不处理 fragmented、不处理 extended payload length。
pub(crate) fn read_one_frame(stream: &mut TcpStream) -> (OpCode, Vec<u8>) {
    let mut hdr = [0_u8; 2];
    stream.read_exact(&mut hdr).unwrap();
    let fin = (hdr[0] & 0x80) != 0;
    assert!(fin, "fragmented frames not supported in test server");
    let opcode = match hdr[0] & 0x0F {
        0x1 => OpCode::Text,
        0x2 => OpCode::Binary,
        0x8 => OpCode::Close,
        0x9 => OpCode::Ping,
        0xA => OpCode::Pong,
        other => panic!("unsupported opcode 0x{other:x}"),
    };
    let masked = (hdr[1] & 0x80) != 0;
    let len_field = hdr[1] & 0x7F;
    let len: usize = if len_field < 126 {
        usize::from(len_field)
    } else if len_field == 126 {
        let mut b = [0_u8; 2];
        stream.read_exact(&mut b).unwrap();
        usize::from(u16::from_be_bytes(b))
    } else {
        let mut b = [0_u8; 8];
        stream.read_exact(&mut b).unwrap();
        usize::try_from(u64::from_be_bytes(b)).unwrap()
    };
    let mut mask = [0_u8; 4];
    if masked {
        stream.read_exact(&mut mask).unwrap();
    }
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).unwrap();
    if masked {
        mask_inplace(&mut payload, mask);
    }
    (opcode, payload)
}
