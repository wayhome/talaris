// Proactor overhead bench：io_uring Nop SQE→CQE 的纯地板延迟。
//
// ## 这层 bench 在测什么
//
// 提交一个 Nop op（kernel 内部不做任何 IO 工作）→ 拿到 CQE → 取出。这是
// io_uring 整条 SQE→CQE 通路本身的延迟下限。任何上层 bench (tcp_echo /
// pool_ws_echo) 测出的延迟都必须 ≥ 这层。
//
// ## 4 个 variant
//
// 1. `vanilla`     —— 不 SQ_POLL，不 pin。每次 submit 都进 io_uring_enter
//                     syscall，scheduler 自由 migrate。
// 2. `pinned`      —— user 钉 CPU，submit 仍走 syscall。看 pin 单独贡献。
// 3. `sqpoll`      —— SQ_POLL on（kthread 钉到 sibling），user 不 pin。看
//                     SQ_POLL 单独贡献。
// 4. `sqpoll+pin`  —— 两者都开。
//
// ## 严格控制变量
//
// - **串行执行**：4 个 variant 依次跑在 main thread 上（不开 sub-thread），
//   每个 variant 之间 `PinGuard` drop 自动 unpin，affinity 不污染下一个。
// - **数据量对齐**：默认 `--iters N`，所有 variant 跑 N 次（默认 100_000）。
// - **wall-clock 对齐（可选）**：`--seconds T`，所有 variant 跑 T 秒。
// - **warmup 隔离**：每 variant 自带 warmup（默认 10_000 iter），数据不进 hist。
// - **Proactor 实例隔离**：每 variant 用全新 `Proactor` —— SQE/CQE 计数器 + SQ
//   ring 都从 0 起，前一 variant 残留不会带过来。
//
// ## 运行
//
// ```bash
// taskset -c 0-7 cargo bench --bench proactor_overhead -- \
//     --iters 100000 --warmup 10000 --user-cpu 1 --sq-poll-cpu 5
//
// # 或者 wall-clock 对齐：
// taskset -c 0-7 cargo bench --bench proactor_overhead -- \
//     --seconds 5 --warmup 10000 --user-cpu 1 --sq-poll-cpu 5
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
    eprintln!("proactor_overhead: skipped — io_uring 只在 Linux 上可用");
}

#[cfg(target_os = "linux")]
fn main() {
    linux_impl::run();
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::time::Instant;

    use hdrhistogram::Histogram;
    use talaris::proactor::{OpKind, Proactor, ProactorConfig, UserData};

    use super::common;
    use super::common::{PinGuard, StopMode};

    pub fn run() {
        let stop = StopMode::from_args(100_000);
        let warmup: u64 = common::arg_or("--warmup", 10_000);
        let user_cpu: usize = common::arg_or("--user-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);

        eprintln!("=========================================================");
        eprintln!(" proactor_overhead — Nop SQE→CQE 地板延迟");
        eprintln!("=========================================================");
        eprintln!(" stop      : {}", stop.describe());
        eprintln!(" warmup    : {warmup} iter (excluded from hist)");
        eprintln!(" user-cpu  : {user_cpu}");
        eprintln!(" sq-poll-cpu: {sq_poll_cpu}");
        eprintln!(" execution : 串行，inline on main thread，每 variant 之间 unpin");
        eprintln!();

        // ──── 4 个 variant 串行跑 ────────────────────────────────────────
        eprintln!("─── variant 1/4: vanilla (no SQ_POLL, no pin) ───");
        let h_vanilla = run_variant(
            "vanilla",
            ProactorConfig {
                entries: 64,
                sq_poll_idle_ms: None,
                sq_poll_cpu: None,
            },
            None,
            stop,
            warmup,
        );

        eprintln!("─── variant 2/4: pinned only (no SQ_POLL) ───");
        let h_pinned = run_variant(
            "pinned",
            ProactorConfig {
                entries: 64,
                sq_poll_idle_ms: None,
                sq_poll_cpu: None,
            },
            Some(user_cpu),
            stop,
            warmup,
        );

        eprintln!("─── variant 3/4: sqpoll only (no user pin) ───");
        let h_sqpoll = run_variant(
            "sqpoll",
            ProactorConfig {
                entries: 64,
                sq_poll_idle_ms: Some(10_000),
                sq_poll_cpu: Some(sq_poll_cpu),
            },
            None,
            stop,
            warmup,
        );

        eprintln!("─── variant 4/4: sqpoll + user pin ───");
        let h_sqpoll_pin = run_variant(
            "sqpoll+pin",
            ProactorConfig {
                entries: 64,
                sq_poll_idle_ms: Some(10_000),
                sq_poll_cpu: Some(sq_poll_cpu),
            },
            Some(user_cpu),
            stop,
            warmup,
        );

        println!();
        println!("=== Nop SQE→CQE RTT ===");
        common::print_comparison(&[
            ("vanilla", &h_vanilla),
            ("pinned", &h_pinned),
            ("sqpoll", &h_sqpoll),
            ("sqpoll+pin", &h_sqpoll_pin),
        ]);
        println!();
        println!("--- per-variant detail ---");
        common::print_hist("vanilla   ", &h_vanilla);
        common::print_hist("pinned    ", &h_pinned);
        common::print_hist("sqpoll    ", &h_sqpoll);
        common::print_hist("sqpoll+pin", &h_sqpoll_pin);
    }

    /// **Inline on main thread**：不开 sub-thread。`PinGuard` 在 fn 结束时
    /// drop，自动 unpin。每 variant 用全新 Proactor 实例，前后无残留。
    fn run_variant(
        label: &'static str,
        cfg: ProactorConfig,
        pin_cpu: Option<usize>,
        stop: StopMode,
        warmup: u64,
    ) -> Histogram<u64> {
        let _guard = pin_cpu.map(|cpu| PinGuard::pin(label, cpu));
        eprintln!(
            "[{label}] SQ_POLL={:?} sq_cpu={:?} pinned={pin_cpu:?}",
            cfg.sq_poll_idle_ms, cfg.sq_poll_cpu
        );

        let mut proactor = Proactor::new(cfg).expect("proactor");

        // warmup
        let mut seq = 0_u64;
        for _ in 0..warmup {
            run_one_nop(&mut proactor, seq);
            seq += 1;
        }

        // measure phase
        let mut hist = common::new_hist();
        let bench_start = Instant::now();
        let mut iter = 0_u64;
        while stop.keep_going(iter, bench_start) {
            let t0 = Instant::now();
            run_one_nop(&mut proactor, seq);
            let dt = t0.elapsed();
            common::record_ns(&mut hist, dt);
            seq += 1;
            iter += 1;
        }
        let wall = bench_start.elapsed();
        eprintln!(
            "[{label}] done {iter} iter in {:.3}s ({:.0} iter/s)",
            wall.as_secs_f64(),
            iter as f64 / wall.as_secs_f64()
        );

        hist
        // _guard drops here → unpin
    }

    #[inline(always)]
    fn run_one_nop(proactor: &mut Proactor, seq: u64) {
        let ud = UserData::new(OpKind::Nop, seq);
        proactor.submit_nop(ud).expect("submit_nop");
        proactor.submit().expect("submit");
        proactor.wait_for_cqe(1).expect("wait");
        let mut got = false;
        proactor.drain_completions(|c| {
            let _ = c.to_result().expect("nop ok");
            got = true;
        });
        debug_assert!(got);
    }
}
