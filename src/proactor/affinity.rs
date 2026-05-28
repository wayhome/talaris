//! CPU affinity helper —— 把当前线程钉到指定 CPU
//!
//! 在 hot path 启动 [`super::Proactor`] 之前调一次，让 kernel scheduler 不再
//! 把这条线程迁出。配合 SQ_POLL 用最佳：
//! - User 线程钉一个 CPU（cache 稳定、TLB warm、no migration cost）
//! - SQ_POLL kernel 线程钉另一个 CPU（不和 user 线程抢同一个 core）
//!
//! 生产部署还要做的（运维层，本 crate 不管）：
//! - kernel cmdline `isolcpus=N` 把 CPU 隔出来不给普通 task
//! - `IRQ` affinity 把网卡中断也固定到非 isolated 的 CPU
//! - `nohz_full=N` 关 tick 中断
//!
//! F3.2 只做最基本的：当前线程 `sched_setaffinity`。其余运维层留到 deploy。

use std::io;

#[derive(Debug, thiserror::Error)]
pub enum AffinityError {
    #[error("core_affinity::get_core_ids returned None (running in restricted env?)")]
    QueryFailed,
    #[error("cpu {requested} out of range (system has {available} logical CPUs)")]
    OutOfRange { requested: usize, available: usize },
    #[error("set_for_current failed for cpu {0}")]
    SetFailed(usize),
}

impl From<AffinityError> for io::Error {
    fn from(e: AffinityError) -> Self {
        Self::other(e.to_string())
    }
}

/// 把当前线程钉到 logical CPU `cpu`（0-indexed，按 `core_affinity::get_core_ids`
/// 返回顺序——通常等于 `/proc/cpuinfo` 的 processor 顺序）。
///
/// 调用必须在线程进入 hot loop 前完成。pinning 是 best-effort——失败会返回
/// 错误，caller 决定 hard-fail 还是 warn-and-continue（ripple 的经验：基建层
/// 用 warn）。
pub fn pin_current_thread_to(cpu: usize) -> Result<(), AffinityError> {
    let cores = core_affinity::get_core_ids().ok_or(AffinityError::QueryFailed)?;
    let id = cores.get(cpu).copied().ok_or(AffinityError::OutOfRange {
        requested: cpu,
        available: cores.len(),
    })?;
    if core_affinity::set_for_current(id) {
        Ok(())
    } else {
        Err(AffinityError::SetFailed(cpu))
    }
}

/// 把当前线程的 affinity 重置到全部 CPU。
///
/// `core_affinity` 自身不提供 reset API（它的设计是 set 单一 core）；我们直接
/// 走 `libc::sched_setaffinity` 把 cpu_set 填满。
///
/// 用途：benchmark 在同进程内跑多个 variant 时，pinned variant 之后要 unpin
/// 才能让后续 unpinned variant 拿到真实测量。
pub fn unpin_current_thread() -> Result<(), AffinityError> {
    // 用 nproc 决定 mask 大小；过设置满 cpu_set_t 也无害
    let n = num_cpus_safe();
    // SAFETY: cpu_set_t 是 POD，0-init 合法
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    for i in 0..n {
        // SAFETY: i < n，CPU_SET 内部做边界检查
        unsafe { libc::CPU_SET(i, &mut set) };
    }
    // pid=0 表示当前线程
    // SAFETY: set 仍 alive 且指向有效 cpu_set_t
    let rc = unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) };
    if rc == 0 {
        Ok(())
    } else {
        Err(AffinityError::SetFailed(usize::MAX))
    }
}

fn num_cpus_safe() -> usize {
    core_affinity::get_core_ids().map(|v| v.len()).unwrap_or(1)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn pin_to_cpu_0_succeeds_on_any_machine() {
        // 任意 Linux 机器至少有 1 个 logical CPU
        pin_current_thread_to(0).unwrap();
    }

    #[test]
    fn out_of_range_cpu_returns_error() {
        let r = pin_current_thread_to(usize::MAX);
        assert!(matches!(r, Err(AffinityError::OutOfRange { .. })));
    }
}
