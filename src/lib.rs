//! network —— hot-path 通信层
//!
//! io_uring proactor + 自实现 WS (RFC 6455) + TLS + HTTP/1.1 codec。所有上层
//! adapter 只走这一份 IO 实现。
//!
//! ## 当前 v1 read-only ready
//!
//! - [`ws`]   RFC 6455 client 全栈（frame / mask / parser / handshake / close / client）
//! - [`tls`]  rustls 字节驱动 adapter（ALPN http/1.1）
//! - [`http`] 最小 HTTP/1.1 codec（WS upgrade response 解析；REST client 缺）
//! - [`proactor`] io_uring 完整原语：F1 connect/recv/send/close + F2 driver
//!   + F3 SQ_POLL/pin_core/BufferRing(multishot)/IO_LINK
//! - [`pool`]   multi-conn driver：1 [`proactor`] 服务 N 条 WS，CQE 按 conn_id
//!   路由；recv 路径零拷贝（raw ptr split borrow），单条 conn 也走 Pool。
//! - [`connection`] 公共类型 [`State`] / [`ConnectionConfig`] / [`ConnectionError`]
//!   及 buf_ring 常量；driver 实现见 [`pool`] / `connection_state`。
//!
//! ## 待加（按需 just-in-time）
//!
//! - **HMAC** —— Deribit / Binance auth signature。Phase 4.5 authenticated
//!   subscribe / Phase 5 下单时加
//! - **JWT (ES256/EdDSA)** —— Deribit fork token。同上
//! - **rate-limit (token bucket)** —— order rate 守门。Phase 5 下单时加
//! - **HTTP/1.1 真客户端**（keep-alive + REST）—— Deribit REST API（如
//!   get_instruments）。仅 cold start 用，Phase 4+ 需要时加
//! - **重连 supervisor** —— cold side tokio，不在 hot IO 路径。属 adapter / bot 层

#![forbid(unsafe_op_in_unsafe_fn)]
// 以下 allow 是真正的 style / 不关 HFT 正确性的 lint —— 留 crate-level OK
#![allow(clippy::borrow_as_ptr)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_ptr_alignment)]
#![allow(clippy::comparison_chain)]
#![allow(clippy::decimal_bitwise_operands)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::explicit_iter_loop)]
#![allow(clippy::format_push_string)]
#![allow(clippy::indexing_slicing)]
#![allow(clippy::ip_constant)]
#![allow(clippy::iter_with_drain)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_fields_in_debug)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::needless_continue)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::redundant_pub_crate)]
#![cfg_attr(test, allow(clippy::float_cmp))]
// 注意：早期版本在这里 crate-wide allow 了 `unwrap_used / expect_used / panic`，
// 等于把 Cargo.toml [lints.clippy] 配的 HFT 守门 lint 全废了。现在改回 warn，
// 个别真需要 panic-on-invariant 的位置改用 module-level / item-level allow。

pub(crate) mod cursor_buf;
pub mod http;
pub mod proactor;
pub mod tls;
pub mod ws;

// TEMP: gate 打开做 cross-platform type-check（macOS 路径走 proactor/stub.rs）
pub mod connection;
pub(crate) mod connection_state;
pub mod pool;

pub use pool::{ConnHandle, Pool, PoolConfig};

#[cfg(test)]
pub(crate) mod test_helpers;
