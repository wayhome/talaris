//! RFC 6455 §5.2 Base Framing Protocol —— header codec
//!
//! 纯函数：`parse_header` 解析最多 14 字节得到 [`FrameHeader`]，
//! `encode_header` 把 header 写进 caller 提供的 buffer。无状态、无 IO、不分配。
//!
//! 帧头长度：
//! - 7-bit len, unmasked:   2 字节（server→client 最常见）
//! - 7-bit len, masked:     6 字节
//! - 16-bit ext len, unmasked: 4 字节
//! - 16-bit ext len, masked:   8 字节
//! - 64-bit ext len, unmasked: 10 字节
//! - 64-bit ext len, masked:   14 字节（client→server 上限）

#![allow(clippy::cast_possible_truncation, clippy::cast_lossless)]

use core::convert::TryFrom;
use thiserror::Error;

/// 帧头最大字节数（client→server 64-bit len 带 mask）
pub const MAX_HEADER_LEN: usize = 14;

/// Control frame payload 上限（RFC §5.5）
pub const MAX_CONTROL_PAYLOAD: u64 = 125;

/// RFC §5.2 opcode（低 4 bit）
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpCode {
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
}

impl OpCode {
    #[inline]
    #[must_use]
    pub const fn is_control(self) -> bool {
        matches!(self, Self::Close | Self::Ping | Self::Pong)
    }

    #[inline]
    #[must_use]
    pub const fn is_data(self) -> bool {
        matches!(self, Self::Continuation | Self::Text | Self::Binary)
    }
}

impl TryFrom<u8> for OpCode {
    type Error = FrameError;

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x0 => Ok(Self::Continuation),
            0x1 => Ok(Self::Text),
            0x2 => Ok(Self::Binary),
            0x8 => Ok(Self::Close),
            0x9 => Ok(Self::Ping),
            0xA => Ok(Self::Pong),
            // 0x3-0x7 reserved data, 0xB-0xF reserved control
            other => Err(FrameError::InvalidOpCode(other)),
        }
    }
}

/// 解析得到的帧头
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    pub fin: bool,
    pub opcode: OpCode,
    /// 收到的帧不应该带 mask（server→client 不 mask）；
    /// `Some` 仅在 encode_header 给 client→server 用。
    pub mask: Option<[u8; 4]>,
    pub payload_len: u64,
}

/// 帧层错误。所有 RFC §5 违规都映射到这里，
/// 上层 client 据此决定是否发 close 1002 protocol error。
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum FrameError {
    #[error("RSV1/2/3 bits set but no extension negotiated (RFC §5.2)")]
    RsvBitsSet,
    #[error("invalid or reserved opcode: 0x{0:X}")]
    InvalidOpCode(u8),
    #[error("control frame must have FIN=1 (RFC §5.4)")]
    ControlFrameFragmented,
    #[error("control frame payload > 125 bytes (RFC §5.5)")]
    ControlFrameTooLarge,
    #[error("payload length 64-bit MSB set (RFC §5.2)")]
    PayloadTooLarge,
    #[error("server sent masked frame (RFC §5.1 violation)")]
    ServerSentMaskedFrame,
}

/// 解析 frame header。
///
/// 返回值：
/// - `Ok(None)`：bytes 不够解析完整 header，再 feed
/// - `Ok(Some((header, consumed)))`：成功，consumed 是 header 占用字节数
/// - `Err(_)`：协议违规
///
/// 不消费 payload，不分配。
pub fn parse_header(buf: &[u8]) -> Result<Option<(FrameHeader, usize)>, FrameError> {
    if buf.len() < 2 {
        return Ok(None);
    }

    let b0 = buf[0];
    let b1 = buf[1];

    let fin = (b0 & 0x80) != 0;
    if (b0 & 0x70) != 0 {
        return Err(FrameError::RsvBitsSet);
    }
    let opcode = OpCode::try_from(b0 & 0x0F)?;

    let masked = (b1 & 0x80) != 0;
    let len7 = b1 & 0x7F;

    let (payload_len, len_field_size) = match len7 {
        0..=125 => (u64::from(len7), 0_usize),
        126 => {
            if buf.len() < 4 {
                return Ok(None);
            }
            (u64::from(u16::from_be_bytes([buf[2], buf[3]])), 2)
        }
        127 => {
            if buf.len() < 10 {
                return Ok(None);
            }
            let bytes = [buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9]];
            let val = u64::from_be_bytes(bytes);
            // RFC §5.2: 64-bit length MSB must be 0
            if val & 0x8000_0000_0000_0000 != 0 {
                return Err(FrameError::PayloadTooLarge);
            }
            (val, 8)
        }
        _ => unreachable!("len7 is masked to 7 bits"),
    };

    if opcode.is_control() {
        if !fin {
            return Err(FrameError::ControlFrameFragmented);
        }
        if payload_len > MAX_CONTROL_PAYLOAD {
            return Err(FrameError::ControlFrameTooLarge);
        }
    }

    let mask_field_size = if masked { 4 } else { 0 };
    let header_size = 2 + len_field_size + mask_field_size;

    if buf.len() < header_size {
        return Ok(None);
    }

    let mask = if masked {
        let off = 2 + len_field_size;
        Some([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
    } else {
        None
    };

    Ok(Some((
        FrameHeader {
            fin,
            opcode,
            mask,
            payload_len,
        },
        header_size,
    )))
}

/// 编码 frame header 到 `dst`，返回写入字节数（最多 [`MAX_HEADER_LEN`]）。
///
/// `dst` 必须 ≥ 14 字节。client 模式下 `mask` 必须为 `Some`（RFC §5.3）。
pub fn encode_header(
    dst: &mut [u8],
    fin: bool,
    opcode: OpCode,
    mask: Option<[u8; 4]>,
    payload_len: u64,
) -> usize {
    debug_assert!(dst.len() >= MAX_HEADER_LEN);

    let mut b0 = opcode as u8;
    if fin {
        b0 |= 0x80;
    }
    dst[0] = b0;

    let masked_bit: u8 = if mask.is_some() { 0x80 } else { 0 };

    let mut idx = if payload_len <= 125 {
        dst[1] = masked_bit | (payload_len as u8);
        2
    } else if payload_len <= 0xFFFF {
        dst[1] = masked_bit | 126;
        dst[2..4].copy_from_slice(&(payload_len as u16).to_be_bytes());
        4
    } else {
        dst[1] = masked_bit | 127;
        dst[2..10].copy_from_slice(&payload_len.to_be_bytes());
        10
    };

    if let Some(m) = mask {
        dst[idx..idx + 4].copy_from_slice(&m);
        idx += 4;
    }

    idx
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn small_text_unmasked_roundtrip() {
        let mut buf = [0u8; 14];
        let n = encode_header(&mut buf, true, OpCode::Text, None, 5);
        assert_eq!(n, 2);
        let (h, c) = parse_header(&buf[..n]).unwrap().unwrap();
        assert_eq!(c, 2);
        assert!(h.fin);
        assert_eq!(h.opcode, OpCode::Text);
        assert_eq!(h.payload_len, 5);
        assert!(h.mask.is_none());
    }

    #[test]
    fn binary_masked_extended_roundtrip() {
        let mut buf = [0u8; 14];
        let mask = [0x12, 0x34, 0x56, 0x78];
        let n = encode_header(&mut buf, true, OpCode::Binary, Some(mask), 12_345);
        assert_eq!(n, 8); // 2 base + 2 ext + 4 mask
        let (h, c) = parse_header(&buf[..n]).unwrap().unwrap();
        assert_eq!(c, 8);
        assert_eq!(h.payload_len, 12_345);
        assert_eq!(h.mask, Some(mask));
    }

    #[test]
    fn binary_unmasked_64bit_len() {
        let mut buf = [0u8; 14];
        let n = encode_header(&mut buf, false, OpCode::Binary, None, 1_000_000);
        assert_eq!(n, 10);
        let (h, c) = parse_header(&buf[..n]).unwrap().unwrap();
        assert_eq!(c, 10);
        assert_eq!(h.payload_len, 1_000_000);
        assert!(!h.fin);
    }

    #[test]
    fn ping_with_mask_full_header() {
        let mut buf = [0u8; 14];
        let mask = [0xDE, 0xAD, 0xBE, 0xEF];
        let n = encode_header(&mut buf, true, OpCode::Ping, Some(mask), 4);
        assert_eq!(n, 6); // 2 base + 0 ext + 4 mask
        let (h, _) = parse_header(&buf[..n]).unwrap().unwrap();
        assert!(h.opcode.is_control());
        assert_eq!(h.mask, Some(mask));
    }

    #[test]
    fn fin_zero_control_rejected() {
        // FIN=0, opcode=Ping(0x9), len=0
        let buf = [0x09_u8, 0x00];
        let err = parse_header(&buf).unwrap_err();
        assert_eq!(err, FrameError::ControlFrameFragmented);
    }

    #[test]
    fn rsv_bits_rejected() {
        // RSV1=1, FIN=1, opcode=Text
        let buf = [0xC1_u8, 0x00];
        let err = parse_header(&buf).unwrap_err();
        assert_eq!(err, FrameError::RsvBitsSet);
    }

    #[test]
    fn reserved_opcode_rejected() {
        // FIN=1, opcode=0x3 reserved data
        let buf = [0x83_u8, 0x00];
        let err = parse_header(&buf).unwrap_err();
        assert_eq!(err, FrameError::InvalidOpCode(0x3));
        // FIN=1, opcode=0xB reserved control
        let buf2 = [0x8B_u8, 0x00];
        let err2 = parse_header(&buf2).unwrap_err();
        assert_eq!(err2, FrameError::InvalidOpCode(0xB));
    }

    #[test]
    fn control_oversize_rejected() {
        // Ping with extended len=126 (would be 256+ bytes)
        let buf = [0x89_u8, 0x7E, 0x01, 0x00];
        let err = parse_header(&buf).unwrap_err();
        assert_eq!(err, FrameError::ControlFrameTooLarge);
    }

    #[test]
    fn payload_64bit_msb_rejected() {
        // 64-bit len with MSB set
        let buf = [0x82_u8, 0x7F, 0x80, 0, 0, 0, 0, 0, 0, 0];
        let err = parse_header(&buf).unwrap_err();
        assert_eq!(err, FrameError::PayloadTooLarge);
    }

    #[test]
    fn partial_header_returns_none() {
        // Just byte 0
        assert!(parse_header(&[0x82_u8]).unwrap().is_none());
        // 2-byte 16-bit len header missing extended bytes
        assert!(parse_header(&[0x82_u8, 0x7E]).unwrap().is_none());
        // Missing mask key bytes
        assert!(parse_header(&[0x82_u8, 0x85]).unwrap().is_none());
    }
}
