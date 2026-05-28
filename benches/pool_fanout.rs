// Pool fanout bench：一个 Pool 同时驱动 N 条 WS 的 RTT + 吞吐。
//
// ## 这层 bench 在测什么
//
// 给定 N ∈ {1, 4, 16, 64} 条 conn 共享一个 [`Pool`]：每轮 N 个 send 一起塞
// 进 Pool（背靠背），pump 直到 N 个 echo 全回来；记录 round 内 per-echo RTT
// （从"全员发出"到"本条 echo 拿到"）和整体吞吐 (msg/s)。
//
// 重点不是单 RTT 绝对值（那是 pool_ws_echo 的事），而是：
//
// 1. **slot-table 路由** 的 N→1 衰减：CQE token → conn_id 是 O(1)，但实际
//    cache miss、bgid 切换、send_buf hand-over 的代价随 N 怎么走。
// 2. **multishot rearm 摊销** 多 conn 下能不能跟上：每条 conn 独占一个 bgid。
// 3. **send_binary 拥塞**：N 个 send 同帧 batch 进 SQ，SQ 满时 Pool 内部
//    backpressure 处理是否平滑。
//
// ## 严格控制变量
//
// - **串行执行**：N=1 → unpin → server tear down → fresh server → N=4 → ...
// - **数据量对齐**：每个 N 跑 `--iters` 个 round（默认 50_000）。可选 `--seconds`。
// - **fresh listener + fresh server per N**：每个 N 起一个新 listener bind +
//   新 server 线程，彻底隔离 N 之间的 socket / kernel state。
// - **server 单 OS 线程**：使用 tokio current_thread runtime（不是 N 个线程）。
//   server 端只占 1 个 CPU，client 测量不被 server-side 调度抖动污染。
//   早期版本给每条 conn spawn 一条 OS 线程，N=64 时 64 个 server worker 抢 8
//   个 CPU，把 client 测的尾延迟全淹了。
//
// ## 注意
//
// Pool::pump() 不调 `sync_ws_open_state`（那是 `pub(crate)`，只在
// drive_conn_until_open 内调），所以并发 `submit_connect` + 用户 pump 直到 Open
// 的非阻塞 handshake 用法目前 hang。fanout 这里走顺序 `connect_blocking_to`，
// loopback 单 conn handshake ~1 ms，N=64 一次性 60ms 不影响 steady state。
// 等 lib 层 fix。
//
// ## 运行
//
// ```bash
// taskset -c 0-7 cargo bench --bench pool_fanout -- \
//     --iters 50000 --warmup 5000 --payload 64 --n-list 1,4,16,64
//
// # wall-clock 对齐 throughput：
// taskset -c 0-7 cargo bench --bench pool_fanout -- \
//     --seconds 5 --payload 64 --n-list 1,4,16,64
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
    eprintln!("pool_fanout: skipped — io_uring 只在 Linux 上可用");
}

#[cfg(target_os = "linux")]
fn main() {
    linux_impl::run();
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
    use std::time::Instant;

    use hdrhistogram::Histogram;
    use talaris::connection::{ConnectionConfig, State};
    use talaris::ws::Event as WsEvent;
    use talaris::{ConnHandle, Pool, PoolConfig};

    use super::common;
    use super::common::{PinGuard, StopMode};

    struct Row {
        n: u32,
        hist: Histogram<u64>,
        msgs_per_sec: f64,
    }

    pub fn run() {
        let stop = StopMode::from_args(50_000);
        let warmup: u64 = common::arg_or("--warmup", 5_000);
        let payload: usize = common::arg_or("--payload", 64);
        let server_cpu: usize = common::arg_or("--server-cpu", 4);
        let user_cpu: usize = common::arg_or("--user-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);
        let n_list: String = common::arg_or("--n-list", "1,4,16,64".to_string());
        let ns: Vec<u32> = n_list
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        eprintln!("=========================================================");
        eprintln!(" pool_fanout — Pool N-conn 路由 + 吞吐 scaling");
        eprintln!("=========================================================");
        eprintln!(" stop      : {}", stop.describe());
        eprintln!(" warmup    : {warmup} (per N)");
        eprintln!(" payload   : {payload}B");
        eprintln!(" n-list    : {ns:?}");
        eprintln!(" server-cpu: {server_cpu} (单 OS 线程 async, fresh per N)");
        eprintln!(" user-cpu  : {user_cpu}");
        eprintln!(" sq-poll-cpu: {sq_poll_cpu}");
        eprintln!(" execution : 串行，inline on main thread，每 N 之间 unpin");
        eprintln!();

        let mut rows: Vec<Row> = Vec::new();
        let n_total = ns.len();
        for (idx, &n) in ns.iter().enumerate() {
            eprintln!(
                "─── variant {}/{}: N={n} ───",
                idx + 1,
                n_total
            );
            let (hist, msgs_per_sec) = with_fresh_async_server(server_cpu, n, |addr| {
                run_fanout_n(n, addr, stop, warmup, payload, user_cpu, sq_poll_cpu)
            });
            rows.push(Row {
                n,
                hist,
                msgs_per_sec,
            });
            eprintln!();
        }

        println!();
        println!("=== Pool fanout (payload={payload}B) ===");
        println!(
            "{:<8} {:>14} {:>14} {:>14} {:>14} {:>14} {:>16}",
            "conns", "mean(ns)", "p50(ns)", "p99(ns)", "p99.9(ns)", "max(ns)", "msgs/sec"
        );
        for r in &rows {
            println!(
                "N={:<6} {:>14} {:>14} {:>14} {:>14} {:>14} {:>16}",
                r.n,
                fmt_int(r.hist.mean() as u64),
                fmt_int(r.hist.value_at_quantile(0.50)),
                fmt_int(r.hist.value_at_quantile(0.99)),
                fmt_int(r.hist.value_at_quantile(0.999)),
                fmt_int(r.hist.max()),
                fmt_int(r.msgs_per_sec as u64),
            );
        }
    }

    /// Fresh single-threaded async WS echo server per N.
    fn with_fresh_async_server<R>(
        server_cpu: usize,
        n_conns: u32,
        body: impl FnOnce(SocketAddr) -> R,
    ) -> R {
        let listener =
            TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let server =
            common::spawn_ws_echo_server_multiplexed(listener, n_conns, Some(server_cpu));
        eprintln!(
            "[bench] fresh single-thread async WS server on {addr}, cpu={server_cpu}, n_conns={n_conns}"
        );
        let result = body(addr);
        server.join().expect("server thread panic");
        result
    }

    fn run_fanout_n(
        n_conns: u32,
        addr: SocketAddr,
        stop: StopMode,
        warmup: u64,
        payload: usize,
        user_cpu: usize,
        sq_poll_cpu: u32,
    ) -> (Histogram<u64>, f64) {
        let _guard = PinGuard::pin("fanout-client", user_cpu);
        eprintln!("[fanout N={n_conns}] user→CPU {user_cpu}, SQ_POLL→CPU {sq_poll_cpu}");

        let cfg_template = ConnectionConfig::new("localhost", addr.port(), "/echo")
            .with_tls(false)
            .with_sq_poll(10_000, Some(sq_poll_cpu));
        let mut pool = Pool::new(PoolConfig::new(cfg_template.proactor)).expect("pool");

        // 顺序 connect N 条（Pool::pump 不 sync open-state，非阻塞并发 handshake
        // 走不通）。loopback 上每条 ~1 ms。
        let mut handles: Vec<ConnHandle> = Vec::with_capacity(n_conns as usize);
        let connect_start = Instant::now();
        for _ in 0..n_conns {
            let h = pool
                .connect_blocking_to(cfg_template.clone(), addr)
                .expect("connect");
            assert_eq!(pool.state(h), Some(State::Open));
            handles.push(h);
        }
        eprintln!(
            "[fanout N={n_conns}] {n_conns} handshakes done in {:?}",
            connect_start.elapsed()
        );

        let mut payload_buf = vec![0_u8; payload];
        for (i, b) in payload_buf.iter_mut().enumerate() {
            *b = b'a' + ((i % 26) as u8);
        }

        // ── warmup ──
        let mut seq = 0_u64;
        for _ in 0..warmup {
            one_fanout_round(&mut pool, &handles, &mut payload_buf, seq, None);
            seq += 1;
        }

        // ── measure ──
        let mut hist = common::new_hist();
        let bench_start = Instant::now();
        let mut iter = 0_u64;
        while stop.keep_going(iter, bench_start) {
            one_fanout_round(&mut pool, &handles, &mut payload_buf, seq, Some(&mut hist));
            seq += 1;
            iter += 1;
        }
        let wall = bench_start.elapsed();
        let total_msgs = iter as f64 * f64::from(n_conns);
        let msgs_per_sec = total_msgs / wall.as_secs_f64();
        eprintln!(
            "[fanout N={n_conns}] {iter} rounds × {n_conns} msgs in {:.3}s = {:.0} msg/s",
            wall.as_secs_f64(),
            msgs_per_sec
        );

        // close
        for &h in &handles {
            pool.initiate_close(h, 1000, "bye").ok();
        }
        let close_start = Instant::now();
        while close_start.elapsed() < std::time::Duration::from_secs(2) {
            let _ = pool.pump_nowait(|_, _| {});
            let all_closed = handles
                .iter()
                .all(|h| matches!(pool.state(*h), Some(State::Closed)));
            if all_closed {
                break;
            }
        }
        (hist, msgs_per_sec)
    }

    /// 一轮 fanout：N 个 send 批量入 SQ → pump 收 N 个 echo。
    /// `hist = Some` 时每条 echo 完成都 record 一条 sample；`None` 是 warmup phase。
    #[inline(always)]
    fn one_fanout_round(
        pool: &mut Pool,
        handles: &[ConnHandle],
        payload_buf: &mut [u8],
        seq: u64,
        mut hist: Option<&mut Histogram<u64>>,
    ) {
        payload_buf[..8].copy_from_slice(&seq.to_le_bytes());
        let round_t0 = Instant::now();
        for &h in handles {
            pool.send_binary(h, payload_buf).expect("send");
        }
        let mut got = 0_u32;
        let n = handles.len() as u32;
        while got < n {
            pool.pump(|_h, ev| {
                if let WsEvent::Binary(data) = ev {
                    assert_eq!(&data[..8], &seq.to_le_bytes(), "fanout seq mismatch");
                    got += 1;
                    if let Some(ref mut h) = hist {
                        common::record_ns(h, round_t0.elapsed());
                    }
                }
            })
            .expect("pump");
        }
    }

    fn fmt_int(n: u64) -> String {
        let s = n.to_string();
        let bytes = s.as_bytes();
        let mut out = String::with_capacity(s.len() + s.len() / 3);
        for (i, &b) in bytes.iter().enumerate() {
            if i > 0 && (bytes.len() - i) % 3 == 0 {
                out.push(',');
            }
            out.push(b as char);
        }
        out
    }
}
