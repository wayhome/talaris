// Pool 全栈 WS echo bench：talaris::Pool + in-process WS echo server。
//
// ## 这层 bench 在测什么
//
// 走 Pool 完整对外 API：`send_text` → io_uring → kernel TCP → loopback → server
// unmask + echo → kernel → multishot recv → WsClient 解帧 → `pump()` 回调。
//
// 分解（理论上）：
//   pool_ws_echo_RTT  ≈  tcp_echo_RTT
//                     + 2 × (mask 帧 + encode/parse header)   ← ws_framing 量级
//                     + Pool state machine
//
// 任何 pool 这层延迟超出三者之和的部分，是 Pool / WsClient hot path 可挖的余量。
//
// ## 严格控制变量
//
// - **串行执行**：default → unpin → recommended，两个 variant 依次 inline 跑。
// - **数据量对齐**：`--iters N`，可选 `--seconds T`。
// - **fresh server per variant**：每个 variant 重新 bind listener + spawn server。
// - **server 单线程**：单 OS 线程 sync echo，pin 在自己 isolated CPU 上。
// - **payload 对称**：两 variant 同 payload buffer，每 iter 头 8 字节 seq 号。
//
// ## variants
//
// - `default`     —— `PoolConfig::default()`：不开 SQ_POLL，不 pin。看上限。
// - `recommended` —— SQ_POLL + user pin + sq_poll kthread pin。HFT 生产推荐配置。
//
// ⚠️ 实测注意：单 in-flight RTT workload（典型同步往返）下 SQ_POLL 由于无法
// 摊销 kthread 跨 CPU coherency 开销，反而可能慢于 default。SQ_POLL 在
// pool_fanout 或 burst workload 才回本。
//
// ## 运行
//
// ```bash
// taskset -c 0-7 cargo bench --bench pool_ws_echo -- \
//     --iters 100000 --warmup 10000 --payload 64
//
// # wall-clock 对齐：
// taskset -c 0-7 cargo bench --bench pool_ws_echo -- --seconds 5 --payload 64
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
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
    use std::thread;
    use std::time::Instant;

    use hdrhistogram::Histogram;
    use talaris::connection::{ConnectionConfig, State};
    use talaris::ws::Event as WsEvent;
    use talaris::{Pool, PoolConfig};

    use super::common;
    use super::common::{PinGuard, StopMode};

    pub fn run() {
        let stop = StopMode::from_args(100_000);
        let warmup: u64 = common::arg_or("--warmup", 10_000);
        let payload: usize = common::arg_or("--payload", 64);
        let server_cpu: usize = common::arg_or("--server-cpu", 4);
        let user_cpu: usize = common::arg_or("--user-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);

        eprintln!("=========================================================");
        eprintln!(" pool_ws_echo — Pool 全栈 WS echo RTT");
        eprintln!("=========================================================");
        eprintln!(" stop      : {}", stop.describe());
        eprintln!(" warmup    : {warmup}");
        eprintln!(" payload   : {payload}B");
        eprintln!(" server-cpu: {server_cpu}  (fresh listener per variant)");
        eprintln!(" user-cpu  : {user_cpu}");
        eprintln!(" sq-poll-cpu: {sq_poll_cpu}");
        eprintln!(" execution : 串行，inline on main thread，每 variant 之间 unpin");
        eprintln!();

        // ── variant 1/2 ──────────────────────────────────────────────────
        eprintln!("─── variant 1/2: default (no SQ_POLL, no pin) ───");
        let h_default = with_fresh_ws_server(server_cpu, |addr| {
            run_pool_variant("default", addr, stop, warmup, payload, None, None)
        });

        // ── variant 2/2 ──────────────────────────────────────────────────
        eprintln!();
        eprintln!("─── variant 2/2: recommended (SQ_POLL + pin) ───");
        let h_recommended = with_fresh_ws_server(server_cpu, |addr| {
            run_pool_variant(
                "recommended",
                addr,
                stop,
                warmup,
                payload,
                Some(user_cpu),
                Some(sq_poll_cpu),
            )
        });

        println!();
        println!("=== Pool WS echo RTT (payload={payload}B) ===");
        common::print_comparison(&[
            ("default", &h_default),
            ("recommended", &h_recommended),
        ]);
        println!();
        println!("--- per-variant detail ---");
        common::print_hist("default    ", &h_default);
        common::print_hist("recommended", &h_recommended);
    }

    /// Fresh WS echo server per variant.
    fn with_fresh_ws_server<R>(server_cpu: usize, body: impl FnOnce(SocketAddr) -> R) -> R {
        let listener =
            TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let server = thread::Builder::new()
            .name("ws-echo-srv".into())
            .spawn(move || {
                let _g = PinGuard::pin("ws-echo-srv", server_cpu);
                let (stream, _) = listener.accept().expect("accept");
                common::run_ws_echo_session(stream);
            })
            .expect("spawn server");
        eprintln!("[bench] fresh ws-echo server on {addr}, cpu={server_cpu}");
        let result = body(addr);
        server.join().expect("server thread panic");
        result
    }

    fn run_pool_variant(
        label: &'static str,
        addr: SocketAddr,
        stop: StopMode,
        warmup: u64,
        payload: usize,
        user_cpu: Option<usize>,
        sq_poll_cpu: Option<u32>,
    ) -> Histogram<u64> {
        let _guard = user_cpu.map(|cpu| PinGuard::pin(label, cpu));
        eprintln!("[{label}] user={user_cpu:?} sq_poll_cpu={sq_poll_cpu:?}");

        let mut cfg = ConnectionConfig::new("localhost", addr.port(), "/echo").with_tls(false);
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

        // ── warmup ──
        let mut seq = 0_u64;
        for _ in 0..warmup {
            one_pool_rtt(&mut pool, h, &mut payload_buf, seq);
            seq += 1;
        }

        // ── measure ──
        let mut hist = common::new_hist();
        let bench_start = Instant::now();
        let mut iter = 0_u64;
        while stop.keep_going(iter, bench_start) {
            let t0 = Instant::now();
            one_pool_rtt(&mut pool, h, &mut payload_buf, seq);
            common::record_ns(&mut hist, t0.elapsed());
            seq += 1;
            iter += 1;
        }
        let wall = bench_start.elapsed();
        eprintln!(
            "[{label}] {iter} iter in {:.3}s ({:.0} iter/s)",
            wall.as_secs_f64(),
            iter as f64 / wall.as_secs_f64()
        );

        // 干净关
        pool.initiate_close(h, 1000, "bye").ok();
        for _ in 0..50 {
            let _ = pool.pump_nowait(|_, _| {});
            if matches!(pool.state(h), Some(State::Closed)) {
                break;
            }
        }
        hist
    }

    #[inline(always)]
    fn one_pool_rtt(
        pool: &mut Pool,
        h: talaris::ConnHandle,
        payload_buf: &mut [u8],
        seq: u64,
    ) {
        payload_buf[..8].copy_from_slice(&seq.to_le_bytes());
        pool.send_binary(h, payload_buf).expect("send");
        let mut got = false;
        while !got {
            pool.pump(|_h, ev| {
                if let WsEvent::Binary(data) = ev {
                    assert_eq!(&data[..8], &seq.to_le_bytes(), "echo seq mismatch");
                    got = true;
                }
            })
            .expect("pump");
        }
    }
}
