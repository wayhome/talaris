// Pool fanout bench：一个 Pool 同时驱动 N 条 WS 的 RTT + 吞吐。
//
// ## 这层 bench 在测什么
//
// 给定 N ∈ {1, 4, 16, 64} 条 conn 共享一个 [`Pool`]：每轮 N 个 send 一起塞
// 进 Pool（背靠背），pump 直到 N 个 echo 全回来；记录 round 内 per-conn 的
// RTT（从"全员发出"到"本条 echo 拿到"）和整体吞吐 (msg/s)。
//
// 这层重点验证的不是单 RTT 的绝对低延迟（那是 pool_ws_echo 的事），而是：
//
// 1. **slot-table 路由** 的 N→1 衰减：CQE 拿到 token 解出 conn_id 是 O(1)，但
//    实际 cache miss、bgid 切换、send_buf hand-over 的代价随 N 怎么走。
// 2. **multishot rearm 摊销** 在多 conn 下能不能跟上：每条 conn 独占一个 bgid，
//    N 大了 buffer ring 总占用变大，rearm 频率也变化。
// 3. **send_binary 拥塞** 的处理：N 个 send 同帧 batch 进 SQ 时 SQ 容易满，看 Pool
//    内部 backpressure 处理是否平滑。
//
// ## 默认 N 序列
//
// `--n-list 1,4,16,64`（按需覆盖），每个 N 跑独立 server + 独立 Pool。
//
// ## 运行
//
// ```bash
// taskset -c 0-7 cargo bench --bench pool_fanout -- \
//     --iters 50000 --warmup 5000 --payload 64 --n-list 1,4,16,64
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
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
    use std::thread;
    use std::time::Instant;

    use hdrhistogram::Histogram;
    use talaris::connection::{ConnectionConfig, State};
    use talaris::proactor::unpin_current_thread;
    use talaris::ws::Event as WsEvent;
    use talaris::{ConnHandle, Pool, PoolConfig};

    use super::common;

    pub fn run() {
        let iters: u64 = common::arg_or("--iters", 50_000);
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

        eprintln!(
            "[pool_fanout] iters={iters} warmup={warmup} payload={payload}B \
             n_list={ns:?} server-cpu={server_cpu} user-cpu={user_cpu} \
             sq-poll-cpu={sq_poll_cpu}"
        );

        let mut rows: Vec<(String, u64, u64, u64, u64, f64)> = Vec::new();
        for &n in &ns {
            let (hist, msgs_per_sec) =
                run_for_n(n, iters, warmup, payload, server_cpu, user_cpu, sq_poll_cpu);
            rows.push((
                format!("N={n}"),
                hist.mean() as u64,
                hist.value_at_quantile(0.50),
                hist.value_at_quantile(0.99),
                hist.value_at_quantile(0.999),
                msgs_per_sec,
            ));
        }

        println!();
        println!("=== Pool fanout (payload={payload}B, iters/round={iters}) ===");
        println!(
            "{:<8} {:>12} {:>12} {:>12} {:>14} {:>16}",
            "conns", "mean(ns)", "p50(ns)", "p99(ns)", "p99.9(ns)", "msgs/sec"
        );
        for (label, mean, p50, p99, p999, msgs) in &rows {
            println!(
                "{:<8} {:>12} {:>12} {:>12} {:>14} {:>16}",
                label,
                fmt_int(*mean),
                fmt_int(*p50),
                fmt_int(*p99),
                fmt_int(*p999),
                fmt_int(*msgs as u64),
            );
        }
    }

    fn run_for_n(
        n_conns: u32,
        iters: u64,
        warmup: u64,
        payload: usize,
        server_cpu: usize,
        user_cpu: usize,
        sq_poll_cpu: u32,
    ) -> (Histogram<u64>, f64) {
        // ── server：一条 acceptor thread，accept N 个 conn 后给每条 conn 起
        //    一个 echo worker thread（loop echo 直到 client 关）。
        let listener =
            TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let server_thread = thread::Builder::new()
            .name(format!("fanout-srv-{n_conns}"))
            .spawn(move || {
                common::pin_or_warn("fanout-srv", server_cpu);
                let mut workers = Vec::new();
                for i in 0..n_conns {
                    let (s, _) = listener.accept().expect("accept");
                    let w = thread::Builder::new()
                        .name(format!("ws-echo-{i}"))
                        .spawn(move || common::run_ws_echo_session(s))
                        .expect("spawn worker");
                    workers.push(w);
                }
                for w in workers {
                    let _ = w.join();
                }
            })
            .expect("spawn srv");

        // ── client：pin + 起 Pool + 并发 submit_connect N 条 → pump 到全 Open。
        let client = thread::Builder::new()
            .name(format!("fanout-client-{n_conns}"))
            .spawn(move || {
                common::pin_or_warn("fanout-client", user_cpu);
                eprintln!("[pool_fanout] N={n_conns} starting");

                let mut cfg_template = ConnectionConfig::new("localhost", addr.port(), "/echo")
                    .with_tls(false)
                    .with_sq_poll(10_000, Some(sq_poll_cpu));
                let mut pool = Pool::new(PoolConfig::new(cfg_template.proactor)).expect("pool");

                // 并发 submit N 条 connect
                let mut handles: Vec<ConnHandle> = Vec::with_capacity(n_conns as usize);
                for _ in 0..n_conns {
                    let cfg = cfg_template.clone();
                    let h = pool.submit_connect_to(cfg, addr).expect("submit_connect");
                    handles.push(h);
                }
                cfg_template.proactor.entries = 0; // 防意外复用

                // pump 直到全员 Open
                let connect_start = Instant::now();
                loop {
                    pool.pump(|_, _| {}).expect("pump connect");
                    let all_open = handles
                        .iter()
                        .all(|h| pool.state(*h) == Some(State::Open));
                    if all_open {
                        break;
                    }
                    if connect_start.elapsed() > std::time::Duration::from_secs(30) {
                        panic!("connect timeout, fanout N={n_conns}");
                    }
                }
                eprintln!(
                    "[pool_fanout] N={n_conns} all handshakes done in {:?}",
                    connect_start.elapsed()
                );

                let mut payload_buf = vec![0_u8; payload];
                for (i, b) in payload_buf.iter_mut().enumerate() {
                    *b = b'a' + ((i % 26) as u8);
                }

                let mut hist = common::new_hist();
                let total = iters + warmup;
                let bench_start = Instant::now();

                for iter in 0..total {
                    payload_buf[..8].copy_from_slice(&iter.to_le_bytes());

                    let round_t0 = Instant::now();
                    for &h in &handles {
                        pool.send_binary(h, &payload_buf).expect("send");
                    }

                    let mut got = 0_u32;
                    while got < n_conns {
                        pool.pump(|_h, ev| {
                            if let WsEvent::Binary(data) = ev {
                                assert_eq!(
                                    &data[..8],
                                    &iter.to_le_bytes(),
                                    "seq mismatch in fanout iter {iter}"
                                );
                                got += 1;
                                if iter >= warmup {
                                    let dt = round_t0.elapsed();
                                    common::record_ns(&mut hist, dt);
                                }
                            }
                        })
                        .expect("pump");
                    }
                }
                let bench_elapsed = bench_start.elapsed();
                let measured_iters = iters; // warmup 段不算在吞吐里
                let total_msgs = measured_iters as f64 * f64::from(n_conns);
                // 减掉 warmup 占用的时间近似：用 bench 总时间 × (measured / total) 反推
                // 不必精确——只为给量级感
                let measured_secs = bench_elapsed.as_secs_f64()
                    * (measured_iters as f64 / total as f64);
                let msgs_per_sec = total_msgs / measured_secs;

                // 关闭所有 conn
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

                let _ = unpin_current_thread();
                (hist, msgs_per_sec)
            })
            .expect("spawn client");

        let (hist, msgs_per_sec) = client.join().expect("client panic");
        server_thread.join().expect("server panic");
        (hist, msgs_per_sec)
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
