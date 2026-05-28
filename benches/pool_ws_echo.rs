// Pool 全栈 WS echo bench：talaris::Pool + in-process WS echo server。
//
// ## 这层 bench 在测什么
//
// 走 Pool 完整对外 API 路径：`send_text` → io_uring（SQ_POLL + pinned）→
// kernel TCP → loopback → server unmask + echo → kernel → multishot recv →
// WsClient 解帧 → `pump()` 回调。这是 talaris 用户真正会跑的延迟。
//
// 分解（理论上）：
//
//     pool_ws_echo_RTT  ≈  tcp_echo_RTT
//                       + 2 × (mask 帧 + encode/parse header)   ← 见 ws_framing
//                       + Pool state machine + multishot rearm 摊销
//
// 如果三者加起来不能解释这层的延迟，证明 Pool 自己 hot path 上有可挖的余量。
//
// ## variants
//
// - `default`     —— `PoolConfig::default()`：不开 SQ_POLL，不 pin。看上限。
// - `recommended` —— SQ_POLL on + user pin + sq_poll kthread pin。HFT 生产用。
//
// ## 运行
//
// ```bash
// taskset -c 0-7 cargo bench --bench pool_ws_echo -- \
//     --iters 100000 --warmup 10000 --payload 64
// ```

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
    clippy::similar_names,
    clippy::too_many_lines
)]

#[path = "common/mod.rs"]
mod common;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("pool_ws_echo: skipped — io_uring 只在 Linux 上可用");
}

#[cfg(target_os = "linux")]
fn main() {
    linux_impl::run();
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
    use std::thread;
    use std::time::Instant;

    use hdrhistogram::Histogram;
    use talaris::connection::{ConnectionConfig, State};
    use talaris::proactor::unpin_current_thread;
    use talaris::ws::Event as WsEvent;
    use talaris::{Pool, PoolConfig};

    use super::common;

    pub fn run() {
        let iters: u64 = common::arg_or("--iters", 100_000);
        let warmup: u64 = common::arg_or("--warmup", 10_000);
        let payload: usize = common::arg_or("--payload", 64);
        let server_cpu: usize = common::arg_or("--server-cpu", 4);
        let user_cpu: usize = common::arg_or("--user-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);

        eprintln!(
            "[pool_ws_echo] iters={iters} warmup={warmup} payload={payload}B"
        );
        eprintln!(
            "[pool_ws_echo] server-cpu={server_cpu} user-cpu={user_cpu} sq-poll-cpu={sq_poll_cpu}"
        );

        let h_default = run_variant(
            "default (no SQ_POLL, no pin)",
            iters,
            warmup,
            payload,
            server_cpu,
            None,
            None,
        );
        let h_recommended = run_variant(
            "recommended (SQ_POLL + pin)",
            iters,
            warmup,
            payload,
            server_cpu,
            Some(user_cpu),
            Some(sq_poll_cpu),
        );

        println!();
        println!("=== Pool WS echo RTT (payload={payload}B, iters={iters}) ===");
        common::print_comparison(&[
            ("default", &h_default),
            ("recommended", &h_recommended),
        ]);
        println!();
        println!("--- per-variant detail ---");
        common::print_hist("default     ", &h_default);
        common::print_hist("recommended ", &h_recommended);
    }

    /// 跑一个 variant：起 echo server thread → client thread → 收 histogram。
    /// 每个 variant 用全新的 server / client，互不污染 cache / scheduler 状态。
    fn run_variant(
        label: &'static str,
        iters: u64,
        warmup: u64,
        payload: usize,
        server_cpu: usize,
        user_cpu: Option<usize>,
        sq_poll_cpu: Option<u32>,
    ) -> Histogram<u64> {
        let listener =
            TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let server =
            common::spawn_ws_echo_server(listener, Some(server_cpu), /* sessions */ 1);

        let client = thread::Builder::new()
            .name(format!("pool-bench-{label}"))
            .spawn(move || {
                if let Some(cpu) = user_cpu {
                    common::pin_or_warn(label, cpu);
                }
                eprintln!("[{label}] starting (user={user_cpu:?} sq={sq_poll_cpu:?})");

                let mut cfg = ConnectionConfig::new("localhost", addr.port(), "/echo")
                    .with_tls(false);
                if let Some(cpu) = sq_poll_cpu {
                    cfg = cfg.with_sq_poll(10_000, Some(cpu));
                }
                let mut pool = Pool::new(PoolConfig::new(cfg.proactor)).expect("pool");
                let h = pool.connect_blocking_to(cfg, addr).expect("connect");
                assert_eq!(pool.state(h), Some(State::Open));

                let mut payload_buf = vec![0_u8; payload];
                for (i, b) in payload_buf.iter_mut().enumerate() {
                    *b = b'a' + ((i % 26) as u8);
                }

                let mut hist = common::new_hist();
                let total = iters + warmup;
                for iter in 0..total {
                    // 在 payload 头 8 字节塞 seq 号，方便 sanity check
                    payload_buf[..8].copy_from_slice(&iter.to_le_bytes());

                    let t0 = Instant::now();
                    pool.send_binary(h, &payload_buf).expect("send");

                    // pump 到本轮 echo 回来；每轮只有一个 echo，pump 完就退
                    let mut got = false;
                    while !got {
                        pool.pump(|_h, ev| {
                            if let WsEvent::Binary(data) = ev {
                                // sanity check：seq 必须匹配（早期 multishot bug
                                // 错位很容易这里炸）
                                assert_eq!(
                                    &data[..8],
                                    &iter.to_le_bytes(),
                                    "echo seq mismatch at iter {iter}"
                                );
                                got = true;
                            }
                        })
                        .expect("pump");
                    }
                    let dt = t0.elapsed();
                    if iter >= warmup {
                        common::record_ns(&mut hist, dt);
                    }
                }

                pool.initiate_close(h, 1000, "bye").ok();
                for _ in 0..50 {
                    let _ = pool.pump_nowait(|_, _| {});
                    if matches!(pool.state(h), Some(State::Closed)) {
                        break;
                    }
                }
                let _ = unpin_current_thread();
                hist
            })
            .expect("spawn client");

        let hist = client.join().expect("client panic");
        server.join().expect("server panic");
        hist
    }
}
