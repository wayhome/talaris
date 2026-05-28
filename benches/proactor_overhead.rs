// Proactor overhead bench：io_uring Nop SQE→CQE 的纯地板延迟。
//
// ## 这层 bench 在测什么
//
// 提交一个 Nop op（kernel 内部不做任何 IO 工作）→ 拿到 CQE → 取出。这是
// io_uring 整条 SQE→CQE 通路本身的延迟下限：
//
// - 用户态写 SQE
// - kernel 看到 SQE（取决于有没有 SQ_POLL）
// - kernel 立刻填 CQE（Nop 没真活）
// - 用户态读 CQE（取决于有没有 wait_for_cqe / busy poll）
//
// 任何上层 bench (tcp_echo / pool_ws_echo / ws_framing) 测出的延迟都必须 ≥
// 这层。如果上层只比这层多几百 ns，那是 IO 几乎免费；如果多很多，得看协议
// 栈或 application logic 哪里耗。
//
// ## 4 个 variant
//
// 1. `vanilla`     —— 既不 SQ_POLL，user 也不 pin。每次 submit 都进
//                     `io_uring_enter` syscall，scheduler 自由 migrate。
// 2. `pinned`      —— user 钉 CPU，但 submit 仍走 syscall。看 pin 单独能省多少。
// 3. `sqpoll`      —— SQ_POLL on（kthread 钉到 sibling），user 不 pin。看
//                     SQ_POLL 单独能省多少 submit-syscall。
// 4. `sqpoll+pin`  —— 两者都开（推荐的 hot path 配置）。最优地板。
//
// ## 运行
//
// ```bash
// taskset -c 0-7 cargo bench --bench proactor_overhead -- \
//     --iters 200000 --warmup 20000 --user-cpu 1 --sq-poll-cpu 5
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
    use std::thread;
    use std::time::Instant;

    use hdrhistogram::Histogram;
    use talaris::proactor::{
        OpKind, Proactor, ProactorConfig, UserData, unpin_current_thread,
    };

    use super::common;

    pub fn run() {
        let iters: u64 = common::arg_or("--iters", 200_000);
        let warmup: u64 = common::arg_or("--warmup", 20_000);
        let user_cpu: usize = common::arg_or("--user-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);

        eprintln!(
            "[proactor_overhead] iters={iters} warmup={warmup} user-cpu={user_cpu} \
             sq-poll-cpu={sq_poll_cpu}"
        );

        // 每个 variant 起独立线程 —— pin / unpin / SQ_POLL kthread CPU 都按 thread
        // 计，跨 variant 不串。
        let h_vanilla = run_variant(
            "vanilla",
            ProactorConfig {
                entries: 64,
                sq_poll_idle_ms: None,
                sq_poll_cpu: None,
            },
            None,
            iters,
            warmup,
        );
        let h_pinned = run_variant(
            "pinned",
            ProactorConfig {
                entries: 64,
                sq_poll_idle_ms: None,
                sq_poll_cpu: None,
            },
            Some(user_cpu),
            iters,
            warmup,
        );
        let h_sqpoll = run_variant(
            "sqpoll",
            ProactorConfig {
                entries: 64,
                sq_poll_idle_ms: Some(10_000),
                sq_poll_cpu: Some(sq_poll_cpu),
            },
            None,
            iters,
            warmup,
        );
        let h_sqpoll_pin = run_variant(
            "sqpoll+pin",
            ProactorConfig {
                entries: 64,
                sq_poll_idle_ms: Some(10_000),
                sq_poll_cpu: Some(sq_poll_cpu),
            },
            Some(user_cpu),
            iters,
            warmup,
        );

        println!();
        println!("=== Nop SQE→CQE RTT (iters={iters}) ===");
        common::print_comparison(&[
            ("vanilla", &h_vanilla),
            ("pinned", &h_pinned),
            ("sqpoll", &h_sqpoll),
            ("sqpoll+pin", &h_sqpoll_pin),
        ]);
        // 在 4 列对比之外再打四行单独的，方便直接复制（多 variant 时上面 ratio
        // 只对最后一列有意义）
        println!();
        println!("--- per-variant detail ---");
        common::print_hist("vanilla", &h_vanilla);
        common::print_hist("pinned", &h_pinned);
        common::print_hist("sqpoll", &h_sqpoll);
        common::print_hist("sqpoll+pin", &h_sqpoll_pin);
    }

    fn run_variant(
        label: &'static str,
        cfg: ProactorConfig,
        pin_cpu: Option<usize>,
        iters: u64,
        warmup: u64,
    ) -> Histogram<u64> {
        thread::Builder::new()
            .name(format!("nop-{label}"))
            .spawn(move || {
                if let Some(cpu) = pin_cpu {
                    common::pin_or_warn(label, cpu);
                }
                eprintln!(
                    "[{label}] SQ_POLL={:?} sq_cpu={:?} pinned={:?}",
                    cfg.sq_poll_idle_ms, cfg.sq_poll_cpu, pin_cpu
                );

                let mut proactor = Proactor::new(cfg).expect("proactor");
                let mut hist = common::new_hist();
                let total = iters + warmup;
                for iter in 0..total {
                    let t0 = Instant::now();
                    let ud = UserData::new(OpKind::Nop, iter);
                    proactor.submit_nop(ud).expect("submit_nop");
                    // SQ_POLL: submit() 多数不进 syscall；wait_for_cqe(1) 才进
                    proactor.submit().expect("submit");
                    proactor.wait_for_cqe(1).expect("wait");
                    let mut seen = false;
                    proactor.drain_completions(|c| {
                        let _ = c.to_result().expect("nop ok");
                        seen = true;
                    });
                    let dt = t0.elapsed();
                    assert!(seen, "no CQE drained");
                    if iter >= warmup {
                        common::record_ns(&mut hist, dt);
                    }
                }

                let _ = unpin_current_thread();
                hist
            })
            .expect("spawn variant")
            .join()
            .expect("variant panic")
    }
}
