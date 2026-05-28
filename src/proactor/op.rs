//! SQE 操作类型 + user_data 编码
//!
//! 每个 SQE 携带 `user_data: u64`，CQE 完成时原样回来——是 caller 区分
//! "哪个操作完成了" 的唯一线索。F1 编码方案：
//!
//! ```text
//! ┌─────────┬──────────────────────────────────────────────┐
//! │ bits 63..56 │           bits 55..0                     │
//! │  OpKind     │    caller-defined token (conn_id + seq)  │
//! └─────────┴──────────────────────────────────────────────┘
//! ```
//!
//! 这样 proactor 不用维护 SQE→op 的 side table，CQE 拿到就能直接分发。
//! caller 应通过 [`UserData::new`] 构造，CQE 端用 [`UserData::kind`] /
//! [`UserData::token`] 拆开。

/// SQE 操作类型。F1 起 5 种：connect、recv、send、close、nop。
/// `Nop` 主要用于 microbench（测 SQE→CQE 纯 proactor 开销）和未来 heartbeat。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum OpKind {
    Connect = 1,
    Recv = 2,
    Send = 3,
    Close = 4,
    Nop = 5,
}

impl OpKind {
    /// 从 u8 还原。未知值返回 None（caller 应当成 protocol error）。
    #[inline]
    #[must_use]
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Connect),
            2 => Some(Self::Recv),
            3 => Some(Self::Send),
            4 => Some(Self::Close),
            5 => Some(Self::Nop),
            _ => None,
        }
    }
}

/// 64-bit user_data 编码：`OpKind << 56 | token & 0x00FF_FFFF_FFFF_FFFF`。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct UserData(u64);

impl UserData {
    const TOKEN_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

    /// 构造一个 user_data。`token` 的高 8 位会被截断（让位给 kind）。
    #[inline]
    #[must_use]
    pub const fn new(kind: OpKind, token: u64) -> Self {
        Self(((kind as u64) << 56) | (token & Self::TOKEN_MASK))
    }

    /// 从 CQE 的 raw `user_data: u64` 还原。
    #[inline]
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// 拿到 u64 形式，交给 io_uring SQE。
    #[inline]
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// 解出 OpKind。CQE 端用。
    #[inline]
    #[must_use]
    pub fn kind(self) -> Option<OpKind> {
        OpKind::from_u8((self.0 >> 56) as u8)
    }

    /// 解出 caller token（低 56 位）。CQE 端用。
    #[inline]
    #[must_use]
    pub const fn token(self) -> u64 {
        self.0 & Self::TOKEN_MASK
    }
}

/// CQE 解码后的事件。
#[derive(Debug, Clone, Copy)]
pub struct Completion {
    /// 还原的 user_data
    pub user_data: UserData,
    /// CQE.result —— `>= 0` 是 op 自定义语义（recv/send 返回字节数；
    /// connect/close 是 0），`< 0` 是 `-errno`。
    pub result: i32,
    /// CQE.flags（multishot / buffer pool 用，F1 暂不解析）
    pub flags: u32,
}

impl Completion {
    /// 把 result 转成 `io::Result<usize>`：负值当 `-errno`，正值当字节数。
    pub fn to_result(self) -> std::io::Result<usize> {
        if self.result >= 0 {
            #[allow(clippy::cast_sign_loss)]
            Ok(self.result as usize)
        } else {
            Err(std::io::Error::from_raw_os_error(-self.result))
        }
    }

    /// multishot recv 用：解出 kernel 给的 buffer_id（CQE flags 高位 +
    /// `IORING_CQE_F_BUFFER`）。非 multishot CQE 返 None。
    #[inline]
    #[must_use]
    pub fn buffer_id(self) -> Option<u16> {
        io_uring::cqueue::buffer_select(self.flags)
    }

    /// multishot 用：`IORING_CQE_F_MORE` 是否设。true 表示这条 multishot SQE
    /// 还活着，后续还会有 CQE；false 表示结束（caller 必须重新 submit）。
    #[inline]
    #[must_use]
    pub fn has_more(self) -> bool {
        io_uring::cqueue::more(self.flags)
    }
}

/// SQE flags —— `io_uring::squeue::Flags` 的精选子集。对外稳定 API。
///
/// 目前只暴露 [`IO_LINK`](Self::IO_LINK)（F3.4）。后续按需开放
/// `CQE_SKIP_SUCCESS` / `BUFFER_SELECT` 等。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub struct SqeFlags(u8);

impl SqeFlags {
    /// 无 flag。
    pub const NONE: Self = Self(0);

    /// `IOSQE_IO_LINK` —— 下一个 SQE 依赖本 SQE 成功完成。chain 中任一失败，
    /// 后续 SQE 自动取消（CQE result = -ECANCELED）。chain 在不带 `IO_LINK`
    /// 的 SQE 处自然结束。
    pub const IO_LINK: Self = Self(io_uring::squeue::Flags::IO_LINK.bits());

    #[inline]
    #[must_use]
    pub const fn empty() -> Self {
        Self::NONE
    }

    #[inline]
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// crate 内部用——转回 io_uring crate 的 Flags。
    #[inline]
    pub(crate) fn into_io_uring(self) -> io_uring::squeue::Flags {
        io_uring::squeue::Flags::from_bits_truncate(self.0)
    }
}

impl std::ops::BitOr for SqeFlags {
    type Output = Self;
    #[inline]
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for SqeFlags {
    #[inline]
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn user_data_roundtrip() {
        let ud = UserData::new(OpKind::Recv, 0x1234_5678_9ABC);
        assert_eq!(ud.kind(), Some(OpKind::Recv));
        assert_eq!(ud.token(), 0x1234_5678_9ABC);

        let raw = ud.raw();
        let restored = UserData::from_raw(raw);
        assert_eq!(restored, ud);
    }

    #[test]
    fn token_high_bits_truncated() {
        // 高 8 位 token 会被 kind 占掉
        let ud = UserData::new(OpKind::Send, 0xFFFF_FFFF_FFFF_FFFF);
        assert_eq!(ud.kind(), Some(OpKind::Send));
        assert_eq!(ud.token(), 0x00FF_FFFF_FFFF_FFFF);
    }

    #[test]
    fn all_kinds_distinct() {
        let a = UserData::new(OpKind::Connect, 1);
        let b = UserData::new(OpKind::Recv, 1);
        let c = UserData::new(OpKind::Send, 1);
        let d = UserData::new(OpKind::Close, 1);
        assert_ne!(a.raw(), b.raw());
        assert_ne!(b.raw(), c.raw());
        assert_ne!(c.raw(), d.raw());
        assert_eq!(a.token(), 1);
        assert_eq!(d.token(), 1);
    }

    #[test]
    fn unknown_kind_returns_none() {
        let bogus = UserData::from_raw(0xFF00_0000_0000_0000);
        assert!(bogus.kind().is_none());
    }

    #[test]
    fn completion_to_result_positive() {
        let c = Completion {
            user_data: UserData::new(OpKind::Recv, 0),
            result: 42,
            flags: 0,
        };
        assert_eq!(c.to_result().unwrap(), 42);
    }

    #[test]
    fn completion_to_result_errno() {
        let c = Completion {
            user_data: UserData::new(OpKind::Connect, 0),
            result: -libc::ECONNREFUSED,
            flags: 0,
        };
        let err = c.to_result().unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ECONNREFUSED));
    }
}
