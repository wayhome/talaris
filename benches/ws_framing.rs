// WS framing micro-bench：mask / encode_header / parse_header / compute_accept
// 每 op 几 ns 的代价。
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
use std::time::Instant;

use talaris::ws::OpCode;
use talaris::ws::frame::{MAX_HEADER_LEN, encode_header, parse_header};
use talaris::ws::handshake::compute_accept;
use talaris::ws::mask::mask_inplace;

fn main() {
    let iters: u64 = common::arg_or("--iters", 10_000_000);
    // 这一层全是 user-space CPU，pin 一下避免 scheduler migrate；不需要 SQ_POLL。
    let pin_cpu: Option<usize> = common::arg_opt("--user-cpu");

    #[cfg(target_os = "linux")]
    let _pin_guard = pin_cpu.map(|cpu| common::PinGuard::pin("ws_framing", cpu));
    let _ = pin_cpu; // suppress unused on non-linux

    eprintln!("[ws_framing] iters={iters}");
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
    println!("=== ws::handshake::compute_accept (per-connection, cold start only) ===");
    bench_compute_accept(iters / 1000);
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
    println!(
        "  compute_accept (RFC example key, {iters} iters): {ns_per_op:>10.2} ns/op"
    );
}
