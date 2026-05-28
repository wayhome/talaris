#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown
)]
//! Quickstart —— talaris Pool 从 0 到 1 走通的最小例子。
//!
//! 展示 3 件事：
//!   1. 怎样把 talaris 加进 Cargo.toml 并 `use` 起来；
//!   2. 怎样组装 [`Pool`] / [`ConnectionConfig`]，把 SQ_POLL 开起来；
//!   3. 怎样把 user 线程钉到指定 CPU，让 io_uring kthread 跑在 sibling 上。
//!
//! 运行：
//!   ```bash
//!   # 假设 CPU 1 / 5 是同一物理核的 SMT pair，且都被 isolcpus 隔离
//!   cargo run --release --example quickstart -- --user-cpu 1 --sq-poll-cpu 5
//!   ```
//!
//! 例子用 [`echo.websocket.events`](https://echo.websocket.events) 公网 WS echo
//! 服务做 round-trip，所以机器需要能访问互联网（wss / TLS）。要离线跑就把
//! `host` 改成自己的本地 ws server。
//!
//! ⚠️ talaris 是 Linux-only 的（io_uring）。macOS / Windows 上 stub 模块只让
//! crate 能 type-check，example 的 `main()` 会直接打印 "skipped" 退出。

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("talaris quickstart: skipped — io_uring 只在 Linux 上可用");
}

// ─── Linux 实现 ────────────────────────────────────────────────────────
#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::time::{Duration, Instant};
    use talaris::connection::{ConnectionConfig, State};
    use talaris::proactor::pin_current_thread_to;
    use talaris::ws::Event as WsEvent;
    use talaris::{Pool, PoolConfig};

    // ── 0. 解析两个 CPU 编号 ─────────────────────────────────────────
    // 极简 CLI：`--user-cpu N --sq-poll-cpu M`。失败时回退到 (1, 5)。
    let (user_cpu, sq_poll_cpu) = parse_cpu_args();
    eprintln!("[quickstart] user thread → CPU {user_cpu}, SQ_POLL kthread → CPU {sq_poll_cpu}");

    // ── 1. 把 user 线程钉死在 user_cpu ───────────────────────────────
    // io_uring 的 user 端代码（Pool::pump、CQE drain）跑在这条线程上。在 hot
    // loop 启动前 pin 一次，避开 scheduler migration 抖动；SQ_POLL kthread 由
    // ConnectionConfig::with_sq_poll(.., Some(cpu)) 在 io_uring init 时钉。
    pin_current_thread_to(user_cpu)?;

    // ── 2. 组装 ConnectionConfig ─────────────────────────────────────
    // 这里同时开了 TLS（443 端口 + wss）。SQ_POLL idle 10 秒 —— kthread 在 hot
    // path 上 spin，超过 10 秒没新 SQE 才会进 sleep；下次 submit 自动 wakeup。
    let cfg = ConnectionConfig::new("echo.websocket.events", 443, "/")
        .with_tls(true)
        .with_sq_poll(10_000, Some(sq_poll_cpu as u32));

    // ── 3. 起 Pool 并阻塞 connect ───────────────────────────────────
    // PoolConfig 透传 proactor 配置（entries / SQ_POLL）—— 这里直接复用 cfg 里的。
    let mut pool = Pool::new(PoolConfig::new(cfg.proactor))?;
    let handle = pool.connect_blocking(cfg)?;
    assert_eq!(pool.state(handle), Some(State::Open));
    eprintln!("[quickstart] WS upgrade + TLS handshake OK");

    // ── 4. 发一条 text frame，pump 直到收到 echo ─────────────────────
    let payload = br#"{"ping":"talaris"}"#;
    let sent_at = Instant::now();
    pool.send_text(handle, payload)?;

    // pump() 阻塞等至少 1 个 CQE；echo 服务先吐 welcome banner 再回声，所以这
    // 里循环到自己的 payload 回来为止。给一个 5s 安全网。
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got_echo = false;
    while Instant::now() < deadline && !got_echo {
        pool.pump(|_h, ev| {
            if let WsEvent::Text(s) = ev {
                eprintln!("[quickstart] ← {s}");
                if s.contains("talaris") {
                    got_echo = true;
                }
            }
        })?;
    }
    let rtt = sent_at.elapsed();
    if got_echo {
        eprintln!("[quickstart] round-trip RTT (incl. WAN) = {rtt:?}");
    } else {
        eprintln!("[quickstart] timed out waiting for echo");
    }

    // ── 5. 干净退出 —— 主动 Close handshake ─────────────────────────
    pool.initiate_close(handle, 1000, "bye")?;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && !matches!(pool.state(handle), Some(State::Closed)) {
        let _ = pool.pump_nowait(|_, _| {});
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn parse_cpu_args() -> (usize, usize) {
    // 极简 --user-cpu / --sq-poll-cpu 解析。缺省 (1, 5) —— ripple-testnet-tokyo
    // 上 CPU 1/5 是同一物理核的两条 SMT，且都在 isolcpus 隔离列表里。
    let mut user_cpu = 1_usize;
    let mut sq_poll_cpu = 5_usize;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--user-cpu" => {
                if let Some(v) = args.next().and_then(|s| s.parse().ok()) {
                    user_cpu = v;
                }
            }
            "--sq-poll-cpu" => {
                if let Some(v) = args.next().and_then(|s| s.parse().ok()) {
                    sq_poll_cpu = v;
                }
            }
            _ => {}
        }
    }
    (user_cpu, sq_poll_cpu)
}
