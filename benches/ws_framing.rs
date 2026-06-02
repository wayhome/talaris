// WS framing micro-bench：mask / encode_header / parse_header / stream decode /
// compute_accept 每 op 几 ns 的代价。
//
// ## 这层 bench 在测什么
//
// 协议层每帧固定开销 —— **跟 IO 完全无关**，纯 CPU 路径。pool_ws_echo 测出
// 的延迟 = tcp_echo + 2×（这里测的 framing 总和）+ Pool state machine。这层
// 跑出来的数字是上层 budget。
//
// ## variants
//
// - `mask_inplace`：AVX2 fast path + 8-byte scalar fallback。三档 payload：
//     - 8 B  (control / 心跳)
//     - 1 KiB (Deribit 订单 / 单个 quote update)
//     - 64 KiB (orderbook full snapshot)
// - `encode_header`：短头（payload ≤ 125）+ 中头（≤ 65535，2-byte len 路径）
// - `parse_header`：同上但反向
// - `stream_decode`：预编码 server→client WS Binary byte stream，在内存里 decode
//   成完整 message；对比 talaris FrameParser / talaris WsClient / tungstenite。
// - `compute_accept`：握手专用 SHA1+base64，每条 conn 只一次，加这条是确认
//                    它在 cold start 的 budget 里
//
// ## 输出
//
// 每个 variant 的 ns/op（tight-loop 平均）。这个量级（ns ~ 几十 ns）记录到
// histogram 反而被 record 本身的开销淹没，所以这里直接 (elapsed / N)。
//
// ## 运行
//
// ```bash
// taskset -c 0-7 cargo bench --bench ws_framing -- --iters 10000000
// ```
//
// 默认 iters = 10M —— 让 elapsed 在百毫秒量级，时钟分辨率不再是误差源。

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::unwrap_used,
    clippy::doc_markdown,
    clippy::similar_names
)]

#[path = "common/mod.rs"]
mod common;

use std::hint::black_box;
use std::io::{Read, Write};
use std::time::Instant;

use talaris::ws::OpCode;
use talaris::ws::frame::{MAX_HEADER_LEN, encode_header, parse_header};
use talaris::ws::handshake::compute_accept;
use talaris::ws::mask::mask_inplace;
use talaris::ws::parser::{FeedOutcome, FrameEvent, FrameParser};
use talaris::ws::{Event as WsEvent, WsClient, WsConfig};
use tokio_tungstenite::tungstenite::protocol::{Message, Role, WebSocket, WebSocketConfig};

fn main() {
    let iters: u64 = common::arg_or("--iters", 10_000_000);
    let stream_frames: u64 = common::arg_or("--stream-frames", 200_000);
    let stream_payloads_csv: String = common::arg_or("--stream-payloads", "64,256,1024".to_owned());
    let stream_payloads = parse_payload_list(&stream_payloads_csv);
    let tungstenite_read_buffer: usize = common::arg_or("--tungstenite-read-buffer", 128 * 1024);
    // 这一层全是 user-space CPU，pin 一下避免 scheduler migrate；不需要 SQ_POLL。
    let pin_cpu: Option<usize> = common::arg_opt("--user-cpu");

    #[cfg(target_os = "linux")]
    let _pin_guard = pin_cpu.map(|cpu| common::PinGuard::pin("ws_framing", cpu));
    let _ = pin_cpu; // suppress unused on non-linux

    eprintln!("[ws_framing] iters={iters}");
    eprintln!("[ws_framing] stream_frames={stream_frames}");
    eprintln!("[ws_framing] stream_payloads={stream_payloads:?}");
    println!();
    println!("=== ws::mask::mask_inplace (AVX2 fast path + 8-byte scalar fallback) ===");
    bench_mask("mask  8B    ", 8, iters);
    bench_mask("mask  1 KiB ", 1024, iters / 100);
    bench_mask("mask  64 KiB", 64 * 1024, iters / 10_000);

    println!();
    println!("=== ws::frame::encode_header ===");
    bench_encode_header("encode  short (≤125)", 100, iters);
    bench_encode_header("encode  medium (2B len)", 8000, iters);

    println!();
    println!("=== ws::frame::parse_header ===");
    bench_parse_header("parse   short", 100, iters);
    bench_parse_header("parse   medium", 8000, iters);

    println!();
    println!("=== websocket stream decode (server->client Binary frames, in-memory) ===");
    println!(
        "{:<24} │ {:>8} │ {:>14} │ {:>10} │ {:>12} │ {:>14} │ {:>14}",
        "variant", "payload", "frames", "elapsed", "ns/frame", "frames/s", "checksum"
    );
    println!("{}", "─".repeat(116));
    for &payload in &stream_payloads {
        let wire = common::pre_encode_ws_binary_chunk(payload, stream_frames as usize);
        let talaris_frame = bench_stream_talaris_frame_parser(&wire, stream_frames, payload);
        let talaris_ws = bench_stream_talaris_ws_client(&wire, stream_frames, payload);
        let tungstenite =
            bench_stream_tungstenite(&wire, stream_frames, payload, tungstenite_read_buffer);
        print_stream_row("talaris FrameParser", payload, &talaris_frame);
        print_stream_row("talaris WsClient", payload, &talaris_ws);
        print_stream_row("tungstenite", payload, &tungstenite);
    }

    println!();
    println!("=== ws::handshake::compute_accept (per-connection, cold start only) ===");
    bench_compute_accept(iters / 1000);
}

struct StreamOutcome {
    frames: u64,
    elapsed: std::time::Duration,
    checksum: u64,
}

impl StreamOutcome {
    fn ns_per_frame(&self) -> u64 {
        if self.frames == 0 {
            return 0;
        }
        (self.elapsed.as_nanos() / u128::from(self.frames)) as u64
    }

    fn frames_per_sec(&self) -> f64 {
        self.frames as f64 / self.elapsed.as_secs_f64()
    }
}

struct SliceStream<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SliceStream<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
}

impl Read for SliceStream<'_> {
    fn read(&mut self, dst: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.bytes.len() {
            return Ok(0);
        }
        let n = dst.len().min(self.bytes.len() - self.pos);
        dst[..n].copy_from_slice(&self.bytes[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

impl Write for SliceStream<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn bench_mask(label: &str, size: usize, iters: u64) {
    // 用 0 初始化也行 —— mask_inplace 不读做模式相关分支，只是 byte XOR
    let mut buf = vec![0xAA_u8; size];
    let key = [0x11_u8, 0x22, 0x33, 0x44];
    // warmup
    for _ in 0..(iters / 100).max(1) {
        mask_inplace(black_box(&mut buf), black_box(key));
    }

    let start = Instant::now();
    for _ in 0..iters {
        mask_inplace(black_box(&mut buf), black_box(key));
    }
    let elapsed = start.elapsed();
    let ns_per_op = elapsed.as_nanos() as f64 / iters as f64;
    let bytes_per_op = size as f64;
    let throughput_gbps = (bytes_per_op * iters as f64) * 8.0 / elapsed.as_secs_f64() / 1e9;
    println!(
        "  {label}: {:>10.2} ns/op  ({:>6.2} GB/s, {:>5.2} GiB/s)",
        ns_per_op,
        bytes_per_op * iters as f64 / elapsed.as_secs_f64() / 1e9,
        throughput_gbps / 8.0
    );
}

fn bench_encode_header(label: &str, payload_len: u64, iters: u64) {
    let mut dst = vec![0_u8; MAX_HEADER_LEN];
    let mask = Some([0x11_u8, 0x22, 0x33, 0x44]);
    // warmup
    for _ in 0..(iters / 100).max(1) {
        let _ = encode_header(black_box(&mut dst), true, OpCode::Text, mask, payload_len);
    }

    let start = Instant::now();
    let mut acc = 0_usize;
    for _ in 0..iters {
        acc = acc.wrapping_add(encode_header(
            black_box(&mut dst),
            true,
            OpCode::Text,
            mask,
            black_box(payload_len),
        ));
    }
    let elapsed = start.elapsed();
    black_box(acc);
    let ns_per_op = elapsed.as_nanos() as f64 / iters as f64;
    println!("  {label}: {ns_per_op:>10.2} ns/op");
}

fn bench_parse_header(label: &str, payload_len: u64, iters: u64) {
    // 先用 encode_header 准备一帧 header
    let mut frame = vec![0_u8; MAX_HEADER_LEN];
    let hn = encode_header(
        &mut frame,
        true,
        OpCode::Text,
        Some([0x11, 0x22, 0x33, 0x44]),
        payload_len,
    );
    let frame = &frame[..hn];

    // warmup
    for _ in 0..(iters / 100).max(1) {
        let _ = parse_header(black_box(frame));
    }

    let start = Instant::now();
    let mut acc = 0_u64;
    for _ in 0..iters {
        let r = parse_header(black_box(frame)).expect("parse ok");
        let (h, _consumed) = r.expect("frame complete");
        acc = acc.wrapping_add(h.payload_len);
    }
    let elapsed = start.elapsed();
    black_box(acc);
    let ns_per_op = elapsed.as_nanos() as f64 / iters as f64;
    println!("  {label}: {ns_per_op:>10.2} ns/op");
}

fn bench_stream_talaris_frame_parser(
    wire: &[u8],
    expected_frames: u64,
    expected_payload: usize,
) -> StreamOutcome {
    let mut parser = FrameParser::new();
    let start = Instant::now();
    let mut pos = 0_usize;
    let mut frames = 0_u64;
    let mut checksum = 0_u64;
    while pos < wire.len() || !parser.is_idle() {
        match parser
            .feed_one(black_box(&wire[pos..]))
            .expect("frame parse")
        {
            FeedOutcome::NeedMore { consumed } => {
                pos += consumed;
                assert!(
                    pos == wire.len() && parser.is_idle(),
                    "in-memory wire should be complete"
                );
            }
            FeedOutcome::Event { consumed, event } => {
                pos += consumed;
                match event {
                    FrameEvent::FrameStart(header) => {
                        debug_assert_eq!(header.payload_len as usize, expected_payload);
                    }
                    FrameEvent::PayloadChunk(chunk) => {
                        checksum = checksum.rotate_left(5)
                            ^ chunk.len() as u64
                            ^ u64::from(chunk.first().copied().unwrap_or_default());
                    }
                    FrameEvent::FrameEnd => frames += 1,
                }
            }
        }
    }
    let elapsed = start.elapsed();
    assert_eq!(frames, expected_frames);
    StreamOutcome {
        frames,
        elapsed,
        checksum,
    }
}

fn bench_stream_talaris_ws_client(
    wire: &[u8],
    expected_frames: u64,
    expected_payload: usize,
) -> StreamOutcome {
    let mut ws = open_talaris_ws_client();
    let start = Instant::now();
    ws.feed_recv(black_box(wire));
    let mut frames = 0_u64;
    let mut checksum = 0_u64;
    while frames < expected_frames {
        match ws.poll_event().expect("event").expect("ws event") {
            WsEvent::Binary(payload) => {
                debug_assert_eq!(payload.len(), expected_payload);
                frames += 1;
                checksum = checksum.rotate_left(5)
                    ^ payload.len() as u64
                    ^ u64::from(payload.first().copied().unwrap_or_default());
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
    let elapsed = start.elapsed();
    StreamOutcome {
        frames,
        elapsed,
        checksum,
    }
}

fn bench_stream_tungstenite(
    wire: &[u8],
    expected_frames: u64,
    expected_payload: usize,
    read_buffer_size: usize,
) -> StreamOutcome {
    let stream = SliceStream::new(wire);
    let config = WebSocketConfig::default()
        .read_buffer_size(read_buffer_size)
        .max_message_size(Some(expected_payload.max(1) * 2))
        .max_frame_size(Some(expected_payload.max(1) * 2));
    let mut ws = WebSocket::from_raw_socket(stream, Role::Client, Some(config));
    let start = Instant::now();
    let mut frames = 0_u64;
    let mut checksum = 0_u64;
    while frames < expected_frames {
        match ws.read().expect("tungstenite read") {
            Message::Binary(payload) => {
                debug_assert_eq!(payload.len(), expected_payload);
                frames += 1;
                checksum = checksum.rotate_left(5)
                    ^ payload.len() as u64
                    ^ u64::from(payload.first().copied().unwrap_or_default());
            }
            other => panic!("unexpected tungstenite message: {other:?}"),
        }
    }
    let elapsed = start.elapsed();
    StreamOutcome {
        frames,
        elapsed,
        checksum,
    }
}

fn open_talaris_ws_client() -> WsClient {
    let mut ws = WsClient::new_client(WsConfig::new("localhost", "/")).expect("ws client");
    ws.begin_handshake().expect("begin handshake");
    let req = std::str::from_utf8(ws.pending_tx()).expect("handshake request utf8");
    let key = req
        .lines()
        .find_map(|line| line.strip_prefix("Sec-WebSocket-Key: "))
        .expect("Sec-WebSocket-Key")
        .trim();
    let accept = compute_accept(key);
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    ws.ack_tx(ws.pending_tx().len());
    ws.feed_recv(response.as_bytes());
    match ws
        .poll_event()
        .expect("handshake event")
        .expect("handshake ok")
    {
        WsEvent::HandshakeComplete => ws,
        other => panic!("expected handshake complete, got {other:?}"),
    }
}

fn print_stream_row(label: &str, payload: usize, outcome: &StreamOutcome) {
    println!(
        "{:<24} │ {:>8} │ {:>14} │ {:>9.3}s │ {:>12} │ {:>14} │ {:>14}",
        label,
        payload,
        common::fmt_int(outcome.frames),
        outcome.elapsed.as_secs_f64(),
        common::fmt_int(outcome.ns_per_frame()),
        common::fmt_int(outcome.frames_per_sec() as u64),
        outcome.checksum,
    );
}

fn parse_payload_list(csv: &str) -> Vec<usize> {
    let payloads: Vec<_> = csv
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().expect("payload size"))
        .collect();
    assert!(!payloads.is_empty(), "--stream-payloads must not be empty");
    payloads
}

fn bench_compute_accept(iters: u64) {
    // RFC §1.3 example
    let key = "dGhlIHNhbXBsZSBub25jZQ==";

    for _ in 0..(iters / 100).max(1) {
        let _ = compute_accept(black_box(key));
    }

    let start = Instant::now();
    let mut acc = 0_usize;
    for _ in 0..iters {
        acc = acc.wrapping_add(compute_accept(black_box(key)).len());
    }
    let elapsed = start.elapsed();
    black_box(acc);
    let ns_per_op = elapsed.as_nanos() as f64 / iters as f64;
    println!("  compute_accept (RFC example key, {iters} iters): {ns_per_op:>10.2} ns/op");
}
