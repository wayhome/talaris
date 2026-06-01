//! io_uring proactor —— Stage F 骨架
//!
//! 单 OS thread 驱动 [`super::ws::WsClient`] / [`super::tls::TlsAdapter`] /
//! [`super::http`] 的字节口。详见仓库 `docs/plan.md` "Hot Path Anatomy"。
//!
//! ## 为什么叫 Proactor（不是 Reactor）
//!
//! - **Reactor**（epoll / mio / Netty）：kernel 通知 "fd 现在可读/可写"，应用
//!   自己调 `read()` / `write()` syscall，user-space copy 由应用执行。
//! - **Proactor**（IOCP / io_uring）：应用提交 "请把 N 字节读进我提供的 buffer"
//!   异步指令，**kernel 完成 IO 后直接把字节写进 user buffer**，给应用一条
//!   completion event。
//!
//! 我们走的是教科书 Proactor：`submit_recv_multishot(fd, bgid, ud)` → kernel 用
//! [`BufferRing`] 里的某块 buffer 接数据 → CQE 带 `bid` + `len` 回来 → 应用拿到
//! `&[u8]` 已在 user space 准备好，**整条 recv 路径没有一次 `read(2)` syscall**。
//!
//! Tokio 生态长期把 io_uring 抽象层也叫 `Reactor`（tokio-uring / glommio 等），
//! 是 mio-based reactor 命名的历史延续，并非概念对的；我们这里用 `Proactor` 表
//! 态——既反映 io_uring + multishot recv + provided buffers + SQ_POLL 的完整
//! Proactor 形态，也避免读 hot path 代码的人误以为是 readiness 模型。
//!
//! ## 分阶段
//!
//! - **F1（当前）**：最小可用骨架。不开 `SQ_POLL` / multishot / fixed buffer /
//!   `IOSQE_IO_LINK` / pin core。专注验证 SQE/CQE 语义跑通——能 connect、recv、
//!   send、close 一条 TCP socket，user_data 编解码正确。
//! - F2：把 socket + `TlsAdapter` + `WsClient` 三件套用 proactor 驱动起来。
//! - F3：加 `SQ_POLL`、multishot recv、`IORING_REGISTER_BUFFERS`、
//!   `IOSQE_IO_LINK` 链 send+recv、pin core。每条都要 benchmark 背书。
//! - F4：`benchmarks/proactor/` loopback echo bench，目标 SQE→CQE P99 < 1µs。
//!
//! ## 平台
//!
//! 真正的实现只在 `cfg(target_os = "linux")` 下编译。macOS 走 `stub`，
//! API 形态一致但调用即 `unimplemented!()`，让本地 `cargo check` 仍过。

#[cfg(target_os = "linux")]
mod affinity;
#[cfg(target_os = "linux")]
mod buf_ring;
#[cfg(target_os = "linux")]
mod op;
#[cfg(target_os = "linux")]
mod socket;
#[cfg(target_os = "linux")]
mod uring;

#[cfg(not(target_os = "linux"))]
mod stub;

#[cfg(target_os = "linux")]
pub use affinity::{AffinityError, pin_current_thread_to, unpin_current_thread};
#[cfg(target_os = "linux")]
pub use buf_ring::{BufferRing, BufferRingError};
#[cfg(target_os = "linux")]
pub use op::{Completion, OpKind, SqeFlags, UserData};
#[cfg(target_os = "linux")]
pub use socket::{Domain, SockAddr, TcpSocket};
#[cfg(target_os = "linux")]
pub use uring::{Proactor, ProactorConfig, ProactorError};

#[cfg(not(target_os = "linux"))]
pub use stub::{
    AffinityError, BufferRing, BufferRingError, Completion, Domain, OpKind, Proactor,
    ProactorConfig, ProactorError, SockAddr, SqeFlags, TcpSocket, UserData,
};

#[cfg(not(target_os = "linux"))]
pub use stub::{pin_current_thread_to, unpin_current_thread};
