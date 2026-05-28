// TCP echo round-trip bench：talaris (io_uring + SQ_POLL + pinned) vs
// tokio (epoll + current_thread + pinned)。
//
// ## 这层 bench 在测什么
//
// 比 proactor_overhead 多了：完整的 TCP 协议栈 + loopback 设备 + 一次 server
// echo。两侧 client 走的是 raw TCP，**协议层完全对称**，差异只来自 IO model。
//
// ## 严格控制变量
//
// - **串行执行**：talaris → unpin → tokio，两个 variant 依次在 main thread 上
//   inline 跑。中间用 `PinGuard` drop 自动 unpin。
// - **数据量对齐**：默认 `--iters N`；可选 `--seconds T` 走 wall-clock 对齐。
// - **fresh server per variant**：每个 variant 起一个全新 listener bind + 全新
//   server thread。TCP TIME_WAIT / kernel socket buffer / 文件描述符号都从 0
//   起，前一 variant 残留不会带进来。
// - **server 单线程**：echo 只用 1 个 OS 线程，pin 在自己专属的 isolated CPU
//   上，client 端测量不被 server 调度污染。
// - **payload 对称**：两侧 send/recv 同样的 byte buffer，每 iter 头 8 字节带
//   seq 号做 sanity check。
//
// ## 拓扑（默认匹配 ripple-testnet-tokyo `isolcpus=1-5`，SMT pairs (0,4) (1,5)
// (2,6) (3,7)）：
//
// ```text
//   CPU 0  6  7  ← OS noise (非 isolated)
//   CPU 1  ← talaris user thread
//   CPU 5  ← talaris SQ_POLL kthread (sibling of 1)
//   CPU 2  ← tokio worker thread
//   CPU 4  ← echo server
//   CPU 3  ← spare
// ```
//
// ## 运行
//
// ```bash
// taskset -c 0-7 cargo bench --bench tcp_echo -- \
//     --iters 200000 --warmup 20000 --payload 64
//
// # wall-clock 对齐 throughput 比较：
// taskset -c 0-7 cargo bench --bench tcp_echo -- \
//     --seconds 5 --warmup 20000 --payload 64
// ```
//
// talaris 这条 client 是 bench 内临时拼的 raw-TCP wheel（直接拿 `Proactor` +
// `TcpSocket`）。**不进 lib**。

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
    eprintln!("tcp_echo: skipped — io_uring 只在 Linux 上可用");
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
    use talaris::proactor::{
        Completion, Domain, OpKind, Proactor, ProactorConfig, SockAddr, SqeFlags, TcpSocket,
        UserData,
    };

    use super::common;
    use super::common::{PinGuard, StopMode};

    pub fn run() {
        let stop = StopMode::from_args(200_000);
        let warmup: u64 = common::arg_or("--warmup", 20_000);
        let payload: usize = common::arg_or("--payload", 64);
        let server_cpu: usize = common::arg_or("--server-cpu", 4);
        let talaris_cpu: usize = common::arg_or("--talaris-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);
        let tokio_cpu: usize = common::arg_or("--tokio-cpu", 2);

        eprintln!("=========================================================");
        eprintln!(" tcp_echo — TCP RTT, talaris (io_uring) vs tokio (epoll)");
        eprintln!("=========================================================");
        eprintln!(" stop      : {}", stop.describe());
        eprintln!(" warmup    : {warmup}");
        eprintln!(" payload   : {payload}B");
        eprintln!(" server-cpu: {server_cpu}  (fresh listener per variant)");
        eprintln!(" talaris   : user→CPU {talaris_cpu}, SQ_POLL→CPU {sq_poll_cpu}");
        eprintln!(" tokio     : worker→CPU {tokio_cpu}");
        eprintln!(" execution : 串行，inline on main thread，每 variant 之间 unpin");
        eprintln!();

        // ── variant 1/2: talaris ────────────────────────────────────────
        eprintln!("─── variant 1/2: talaris (io_uring + SQ_POLL + pinned) ───");
        let h_talaris = with_fresh_tcp_server(server_cpu, |addr| {
            run_talaris(addr, stop, warmup, payload, talaris_cpu, sq_poll_cpu)
        });

        // ── variant 2/2: tokio ──────────────────────────────────────────
        eprintln!();
        eprintln!("─── variant 2/2: tokio (epoll + current_thread + pinned) ───");
        let h_tokio = with_fresh_tcp_server(server_cpu, |addr| {
            run_tokio(addr, stop, warmup, payload, tokio_cpu)
        });

        println!();
        println!("=== TCP echo RTT (payload={payload}B) ===");
        common::print_comparison(&[
            ("talaris (io_uring)", &h_talaris),
            ("tokio (epoll)", &h_tokio),
        ]);
    }

    /// 给 variant 起一个全新 listener + server thread；body 跑完后等 server 退出。
    /// 保证 variant 间 server 端 TCP state / kernel buffer / OS thread 全是 fresh 的。
    fn with_fresh_tcp_server<R>(server_cpu: usize, body: impl FnOnce(SocketAddr) -> R) -> R {
        let listener =
            TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let server = thread::Builder::new()
            .name("tcp-echo-srv".into())
            .spawn(move || common::run_tcp_echo_once(listener, Some(server_cpu)))
            .expect("spawn server");
        eprintln!("[bench] fresh tcp-echo server on {addr}, cpu={server_cpu}");
        let result = body(addr);
        // 客户端 close → server read 0 → server return；这里只是等线程实际退出
        server.join().expect("server thread panic");
        result
    }

    fn run_talaris(
        addr: SocketAddr,
        stop: StopMode,
        warmup: u64,
        payload_size: usize,
        user_cpu: usize,
        sq_poll_cpu: u32,
    ) -> Histogram<u64> {
        let _guard = PinGuard::pin("talaris", user_cpu);
        eprintln!(
            "[talaris] user→CPU {user_cpu}, SQ_POLL kthread→CPU {sq_poll_cpu}"
        );

        let proactor_cfg = ProactorConfig {
            entries: 64,
            sq_poll_idle_ms: Some(10_000),
            sq_poll_cpu: Some(sq_poll_cpu),
        };
        let mut proactor = Proactor::new(proactor_cfg).expect("proactor");

        let sock = TcpSocket::new(Domain::V4).expect("socket");
        sock.set_nodelay(true).expect("nodelay");
        let fd = sock.as_raw_fd();
        let sock_addr = SockAddr::from_std(addr);

        // connect
        let connect_ud = UserData::new(OpKind::Connect, 0);
        // SAFETY: sock_addr / sock 都活到 connect CQE 之后
        unsafe {
            proactor
                .submit_connect(fd, &sock_addr, connect_ud, SqeFlags::NONE)
                .expect("submit_connect");
        }
        proactor.submit_and_wait(1).expect("connect");
        let mut got: Option<Completion> = None;
        proactor.drain_completions(|c| got = Some(c));
        got.expect("connect CQE").to_result().expect("connect ok");

        let mut send_buf = vec![0_u8; payload_size];
        let mut recv_buf = vec![0_u8; payload_size];
        for (i, b) in send_buf.iter_mut().enumerate() {
            *b = i as u8;
        }

        // ── warmup ──
        let mut seq = 0_u64;
        for _ in 0..warmup {
            run_one_rtt(&mut proactor, fd, &mut send_buf, &mut recv_buf, seq);
            seq += 1;
        }

        // ── measure ──
        let mut hist = common::new_hist();
        let bench_start = Instant::now();
        let mut iter = 0_u64;
        while stop.keep_going(iter, bench_start) {
            let t0 = Instant::now();
            run_one_rtt(&mut proactor, fd, &mut send_buf, &mut recv_buf, seq);
            common::record_ns(&mut hist, t0.elapsed());
            seq += 1;
            iter += 1;
        }
        let wall = bench_start.elapsed();
        eprintln!(
            "[talaris] {iter} iter in {:.3}s ({:.0} iter/s)",
            wall.as_secs_f64(),
            iter as f64 / wall.as_secs_f64()
        );

        // close
        let raw_fd = sock.as_raw_fd();
        std::mem::forget(sock);
        let close_ud = UserData::new(OpKind::Close, 0);
        // SAFETY: TcpSocket forget 后没 RAII 跟踪 fd
        unsafe { proactor.submit_close_raw(raw_fd, close_ud).expect("close") };
        proactor.submit_and_wait(1).expect("close wait");
        proactor.drain_completions(|_| {});

        hist
        // guard drops → unpin
    }

    #[inline(always)]
    fn run_one_rtt(
        proactor: &mut Proactor,
        fd: std::os::fd::RawFd,
        send_buf: &mut [u8],
        recv_buf: &mut [u8],
        seq: u64,
    ) {
        send_buf[..8].copy_from_slice(&seq.to_le_bytes());
        let send_ud = UserData::new(OpKind::Send, seq);
        let recv_ud = UserData::new(OpKind::Recv, seq);
        let len = send_buf.len() as u32;
        // SAFETY: send_buf / recv_buf 整轮存活；fd 已 connected。
        unsafe {
            proactor
                .submit_send(fd, send_buf.as_ptr(), len, send_ud, SqeFlags::NONE)
                .expect("send");
            proactor
                .submit_recv(fd, recv_buf.as_mut_ptr(), len, recv_ud, SqeFlags::NONE)
                .expect("recv");
        }
        proactor.submit().expect("submit");
        let mut got = 0_u32;
        while got < 2 {
            proactor.wait_for_cqe(1).expect("wait cqe");
            proactor.drain_completions(|c| {
                got += 1;
                let res = c.to_result().expect("op ok");
                match c.user_data.kind() {
                    Some(OpKind::Send) | Some(OpKind::Recv) => {
                        assert_eq!(res, send_buf.len(), "short op");
                    }
                    other => panic!("unexpected CQE kind {other:?}"),
                }
            });
        }
        debug_assert_eq!(&recv_buf[..8], &seq.to_le_bytes());
    }

    fn run_tokio(
        addr: SocketAddr,
        stop: StopMode,
        warmup: u64,
        payload_size: usize,
        user_cpu: usize,
    ) -> Histogram<u64> {
        let _guard = PinGuard::pin("tokio", user_cpu);
        eprintln!("[tokio] worker→CPU {user_cpu}");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("rt");

        rt.block_on(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            use tokio::net::TcpStream;

            let mut stream = TcpStream::connect(addr).await.expect("connect");
            stream.set_nodelay(true).expect("nodelay");

            let mut send_buf = vec![0_u8; payload_size];
            for (i, b) in send_buf.iter_mut().enumerate() {
                *b = i as u8;
            }
            let mut recv_buf = vec![0_u8; payload_size];

            // warmup
            let mut seq = 0_u64;
            for _ in 0..warmup {
                send_buf[..8].copy_from_slice(&seq.to_le_bytes());
                stream.write_all(&send_buf).await.expect("write");
                stream.read_exact(&mut recv_buf).await.expect("read");
                debug_assert_eq!(&recv_buf[..8], &seq.to_le_bytes());
                seq += 1;
            }

            // measure
            let mut hist = common::new_hist();
            let bench_start = Instant::now();
            let mut iter = 0_u64;
            while stop.keep_going(iter, bench_start) {
                send_buf[..8].copy_from_slice(&seq.to_le_bytes());
                let t0 = Instant::now();
                stream.write_all(&send_buf).await.expect("write");
                stream.read_exact(&mut recv_buf).await.expect("read");
                common::record_ns(&mut hist, t0.elapsed());
                debug_assert_eq!(&recv_buf[..8], &seq.to_le_bytes());
                seq += 1;
                iter += 1;
            }
            let wall = bench_start.elapsed();
            eprintln!(
                "[tokio] {iter} iter in {:.3}s ({:.0} iter/s)",
                wall.as_secs_f64(),
                iter as f64 / wall.as_secs_f64()
            );

            stream.shutdown().await.ok();
            hist
        })
        // guard drops → unpin
    }
}
