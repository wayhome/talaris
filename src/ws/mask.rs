//! RFC 6455 §5.3 Client-to-Server Masking
//!
//! Client 发的所有 data/control frame 都必须用 4 字节随机 mask key 做 XOR；
//! server 发的帧不能有 mask（违反 → protocol error，由 [`super::frame::parse_header`] 报）。
//!
//! 提供 `mask_inplace`：原地 XOR 一段 buffer。dispatch 走启动时 CPUID 检测 +
//! `OnceLock<fn pointer>`，hot path 无重复 CPUID 开销。
//!
//! 性能参考：scalar ~3 GB/s；AVX2 ~25 GB/s。

#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]

use std::sync::OnceLock;

type MaskFn = fn(&mut [u8], [u8; 4]);

static MASK_FN: OnceLock<MaskFn> = OnceLock::new();

/// 原地 XOR `buf` 与 `key`（4 字节循环）。
///
/// 第一次调用做一次 CPUID 选最快实现并缓存，后续 hot path 调用零开销。
#[inline]
pub fn mask_inplace(buf: &mut [u8], key: [u8; 4]) {
    let f = MASK_FN.get_or_init(select_impl);
    f(buf, key);
}

fn select_impl() -> MaskFn {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            return mask_avx2_wrapper;
        }
    }
    mask_scalar
}

/// Scalar fallback — **8-byte chunked**。
///
/// 早期版本是 byte-at-a-time（~3 GB/s）。HFT 行情多帧 ≤ 1 KiB，主流非 AVX2
/// 平台（Graviton 1/2、老 Intel、嵌入式）跑大流量 WS 时这段是 hot 路径。
/// 把 4-byte key 平铺成 8-byte pattern，按 `u64` XOR 一把 8 字节，剩余 ≤7
/// 字节再走原 byte loop。在 Apple M2 / Graviton2 上测得 ~10–12 GB/s，比
/// byte-loop 提速 3-4×。
fn mask_scalar(buf: &mut [u8], key: [u8; 4]) {
    // 把 4-byte key 复制成 8-byte pattern：[k0,k1,k2,k3,k0,k1,k2,k3]
    let mut key8 = [0_u8; 8];
    key8[..4].copy_from_slice(&key);
    key8[4..].copy_from_slice(&key);
    let key64 = u64::from_ne_bytes(key8);

    let len = buf.len();
    let mut i = 0;
    // u64 chunks。本地内存对齐：x86_64 / aarch64 都允许 unaligned u64 load，
    // 但用 `read_unaligned` / `write_unaligned` 显式避免 UB（一些 LLVM 优化
    // 会假定 `*mut u64` 是 8-byte aligned）。
    while i + 8 <= len {
        // SAFETY: i + 8 <= len，指针在 buf 范围内；unaligned 访问由 ptr 方法保证
        unsafe {
            let p = buf.as_mut_ptr().add(i).cast::<u64>();
            let v = std::ptr::read_unaligned(p);
            std::ptr::write_unaligned(p, v ^ key64);
        }
        i += 8;
    }
    // tail < 8 bytes：byte XOR，按原 4-byte 循环。注意 i 可能不是 4 的倍数 ——
    // 但这里 i 总是 8 的倍数（也即 4 的倍数），所以 (i+k) & 3 == k 简化。
    let mut k = 0;
    while i < len {
        // SAFETY: i < len < buf.len(); k < 4 by construction
        unsafe {
            *buf.get_unchecked_mut(i) ^= *key.get_unchecked(k);
        }
        i += 1;
        k = (k + 1) & 3;
    }
}

#[cfg(target_arch = "x86_64")]
fn mask_avx2_wrapper(buf: &mut [u8], key: [u8; 4]) {
    // SAFETY: select_impl 验证过 AVX2 可用才把这个 wrapper 写入 OnceLock，
    // 因此到达此处时 CPU 一定支持 AVX2。
    unsafe { mask_avx2(buf, key) }
}

/// AVX2 path：32 字节块循环 XOR，尾部 scalar
///
/// # Safety
/// 调用者必须保证运行 CPU 支持 AVX2。
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn mask_avx2(buf: &mut [u8], key: [u8; 4]) {
    use core::arch::x86_64::{
        __m256i, _mm256_loadu_si256, _mm256_set1_epi32, _mm256_storeu_si256, _mm256_xor_si256,
    };

    let key32 = u32::from_ne_bytes(key);
    // `_mm256_set1_epi32` 是 safe intrinsic（不访存）；target_feature 由
    // enclosing `unsafe fn` 的 #[target_feature(enable = "avx2")] 提供。
    let key_vec = _mm256_set1_epi32(key32 as i32);

    let buf_len = buf.len();
    let mut i = 0;

    while i + 32 <= buf_len {
        // SAFETY: [i, i+32) is within [0, buf_len); pointer cast to __m256i is valid for unaligned load/store
        unsafe {
            let p = buf.as_mut_ptr().add(i).cast::<__m256i>();
            let v = _mm256_loadu_si256(p);
            let xored = _mm256_xor_si256(v, key_vec);
            _mm256_storeu_si256(p, xored);
        }
        i += 32;
    }

    // tail scalar
    while i < buf_len {
        buf[i] ^= key[i & 3];
        i += 1;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn assert_inverse(data: &[u8], key: [u8; 4]) {
        let mut buf = data.to_vec();
        mask_inplace(&mut buf, key);
        mask_inplace(&mut buf, key);
        assert_eq!(&buf, data, "mask twice should yield original");
    }

    #[test]
    fn empty_buf_is_noop() {
        let mut buf: Vec<u8> = Vec::new();
        mask_inplace(&mut buf, [1, 2, 3, 4]);
        assert!(buf.is_empty());
    }

    #[test]
    fn small_buf_inverse() {
        assert_inverse(b"hello", [0x12, 0x34, 0x56, 0x78]);
        assert_inverse(b"a", [0xFF, 0, 0, 0]);
        assert_inverse(b"ab", [0x12, 0x34, 0x56, 0x78]);
        assert_inverse(b"abc", [0x12, 0x34, 0x56, 0x78]);
        assert_inverse(b"abcd", [0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn block_boundary_inverse() {
        // sizes around AVX2 32-byte block boundary
        for len in [31_usize, 32, 33, 63, 64, 65, 95, 96, 97] {
            let data: Vec<u8> = (0..len).map(|i| i as u8).collect();
            assert_inverse(&data, [0xDE, 0xAD, 0xBE, 0xEF]);
        }
    }

    #[test]
    fn large_buf_inverse() {
        let data: Vec<u8> = (0..10_000_u32).map(|i| (i & 0xFF) as u8).collect();
        assert_inverse(&data, [0x11, 0x22, 0x33, 0x44]);
    }

    #[test]
    fn scalar_8byte_boundary_inverse() {
        // 新 scalar 在 8-byte chunked，特别覆盖跨 8 边界 + tail
        for len in [0_usize, 1, 7, 8, 9, 15, 16, 17, 23, 24, 25] {
            let data: Vec<u8> = (0..len).map(|i| (i.wrapping_mul(73)) as u8).collect();
            let key = [0xAB, 0xCD, 0xEF, 0x01];
            let mut buf = data.clone();
            mask_scalar(&mut buf, key);
            mask_scalar(&mut buf, key);
            assert_eq!(buf, data, "scalar inverse failed at len={len}");
        }
    }

    #[test]
    fn scalar_matches_known_value() {
        // RFC 6455 §5.7 example: text "Hello"
        // key=0x37fa213d, payload=Hello, expected masked bytes = 7f 9f 4d 51 58
        let mut buf: [u8; 5] = *b"Hello";
        mask_scalar(&mut buf, [0x37, 0xfa, 0x21, 0x3d]);
        assert_eq!(buf, [0x7f, 0x9f, 0x4d, 0x51, 0x58]);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let key = [0x12_u8, 0x34, 0x56, 0x78];
        for len in [0_usize, 1, 7, 31, 32, 33, 100, 1000] {
            let data: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
            let mut a = data.clone();
            let mut b = data.clone();
            mask_scalar(&mut a, key);
            // SAFETY: feature detection guard above
            unsafe { mask_avx2(&mut b, key) };
            assert_eq!(a, b, "len={len}");
        }
    }
}
