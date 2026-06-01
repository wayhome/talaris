//! `CursorBuf` —— head-cursor buffer
//!
//! 这里把 `(data: Vec<u8>, head: usize)` 包装一下：
//!
//! - `consume(n)` 仅自增 head；head 追上 len 时整体 `clear()`（O(1) 复原）
//! - `extend_from_slice` 在 capacity 真要重分配前才做 compact memmove，
//!   多数情况下 head==0（consume 全部后），完全不 memmove
//! - `as_slice / as_ptr` 始终返回 `&data[head..]` 视图
//!
//! 不变式：caller 用 `len()` 拿到的就是 "可读字节数"；`as_slice / as_ptr` 与
//! `consume` 严格配对（kernel 持有 `as_ptr()` 直到 CQE 回来前，不要再调
//! `extend_from_slice` 触发 realloc / 内部 memmove）。

#![allow(clippy::module_name_repetitions)]
// `is_empty / as_ptr / clear` 是 CursorBuf 完整 API 的一部分；目前 ws.recv_buf
// 只用到 extend/consume/as_slice，其余方法留作给未来 send_buf 切到 CursorBuf
// 或外部使用。dead-code 警告在 `-D warnings` 下会 fail build。
#![allow(dead_code)]

/// head-cursor backed by a `Vec<u8>`. 不实现 `Deref<Vec<u8>>` 来禁止 caller
/// 误用 `drain` / `truncate` 等会 invalidate cursor 语义的操作。
#[derive(Debug)]
pub(crate) struct CursorBuf {
    data: Vec<u8>,
    head: usize,
}

impl CursorBuf {
    #[must_use]
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self {
            data: Vec::with_capacity(cap),
            head: 0,
        }
    }

    /// 可读字节数（`data.len() - head`）。
    #[inline]
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.data.len() - self.head
    }

    #[inline]
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.head == self.data.len()
    }

    #[inline]
    #[must_use]
    pub(crate) fn as_slice(&self) -> &[u8] {
        // `head <= data.len()` 由所有 mutator 维护，slice index 永不越界
        &self.data[self.head..]
    }

    /// 数据区起始指针。Hot path 给 io_uring SQE 用。CQE 回来前 caller 必须
    /// 保证不调任何 mutator（`extend_from_slice` / `consume` / `clear`）。
    #[inline]
    #[must_use]
    pub(crate) fn as_ptr(&self) -> *const u8 {
        // SAFETY: head <= data.len()；如果 data 为空 (cap=0)，as_ptr 是 dangling
        // 但 add(0) 仍合法。
        unsafe { self.data.as_ptr().add(self.head) }
    }

    /// 把 `bytes` 追加到 buffer。如果当前 underlying Vec 没空间且 head > 0，
    /// 先 compact（memmove + reset head）再追加 —— 避免无谓的 realloc 同时
    /// 把 memmove 摊到第一次 capacity-exceed 而不是 every consume。
    pub(crate) fn extend_from_slice(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let need = self.data.len() + bytes.len();
        if need > self.data.capacity() && self.head > 0 {
            self.compact();
        }
        self.data.extend_from_slice(bytes);
    }

    /// 标记前 `n` 字节已消费。`n == len()` 时整体 reset 到空（O(1)），否则
    /// 仅自增 head（O(1)）。`n > len()` panic。
    #[inline]
    pub(crate) fn consume(&mut self, n: usize) {
        debug_assert!(n <= self.len(), "consume({n}) > len({})", self.len());
        self.head += n;
        if self.head == self.data.len() {
            self.data.clear();
            self.head = 0;
        }
    }

    /// 显式整体清空（等价 `consume(len())`）。
    #[inline]
    pub(crate) fn clear(&mut self) {
        self.data.clear();
        self.head = 0;
    }

    fn compact(&mut self) {
        if self.head == 0 {
            return;
        }
        // 把 data[head..] 移到 front。Vec::drain(..head) 是经典实现 —— 这里
        // 等价 memmove + len 调整，无 realloc。
        self.data.drain(..self.head);
        self.head = 0;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn extend_then_consume_all_resets() {
        let mut b = CursorBuf::with_capacity(16);
        b.extend_from_slice(b"hello");
        assert_eq!(b.len(), 5);
        assert_eq!(b.as_slice(), b"hello");
        b.consume(5);
        assert!(b.is_empty());
        // 内部 reset：head 重新 0
        assert_eq!(b.as_slice(), b"");
    }

    #[test]
    fn partial_consume_advances_head() {
        let mut b = CursorBuf::with_capacity(16);
        b.extend_from_slice(b"hello world");
        b.consume(6);
        assert_eq!(b.as_slice(), b"world");
        assert_eq!(b.len(), 5);
    }

    #[test]
    fn extend_after_partial_compacts_only_when_full() {
        let mut b = CursorBuf::with_capacity(8);
        b.extend_from_slice(b"abcdef"); // len=6, cap=8
        b.consume(3); // head=3, data.len()=6
        // 还有空间，append 不 compact
        b.extend_from_slice(b"X"); // data.len()=7, head=3
        assert_eq!(b.as_slice(), b"defX");
        // 再加触发 compact
        b.extend_from_slice(b"YZ"); // need=10 > cap=8 → compact 到 head=0 再 extend
        assert_eq!(b.as_slice(), b"defXYZ");
        assert_eq!(b.len(), 6);
    }

    #[test]
    fn consume_exact_length_zeroes_head() {
        let mut b = CursorBuf::with_capacity(4);
        b.extend_from_slice(b"ab");
        b.consume(1);
        b.consume(1);
        assert_eq!(b.len(), 0);
        // 再 extend，应该从 head=0 重新装
        b.extend_from_slice(b"cd");
        assert_eq!(b.as_slice(), b"cd");
    }

    #[test]
    fn empty_extend_is_noop() {
        let mut b = CursorBuf::with_capacity(4);
        b.extend_from_slice(b"");
        assert!(b.is_empty());
        b.extend_from_slice(b"x");
        b.extend_from_slice(b"");
        assert_eq!(b.as_slice(), b"x");
    }

    #[test]
    fn as_ptr_aligns_with_head() {
        let mut b = CursorBuf::with_capacity(8);
        b.extend_from_slice(b"hello");
        let ptr0 = b.as_ptr();
        b.consume(2);
        // SAFETY: head advanced 2 bytes
        unsafe {
            assert_eq!(*ptr0.add(2), *b.as_ptr());
        }
    }
}
