# talaris

> Predictable&Low-latency, zero-jitter HFT data-pipeline transport for Linux.

[![crates.io](https://img.shields.io/crates/v/talaris.svg)](https://crates.io/crates/talaris)
[![docs.rs](https://docs.rs/talaris/badge.svg)](https://docs.rs/talaris)
[![license](https://img.shields.io/badge/license-GPL--3.0--or--later-blue.svg)](LICENSE)

> The name `talaria` (Hermes' winged sandals) was already taken on crates.io,
> so this crate ships as `talaris` — the Latin singular form of the same word.
> `cargo add talaris`, `use talaris::*;`.

`talaris` is the io_uring-based data-plane behind a high-frequency-trading
order/quote pipeline. It owns the byte pump end-to-end so that p99/p99.9
latency is bounded by hardware, not by a stack of generic async runtimes.

## What's in the box

- **io_uring proactor** — F1/F2/F3 features: SQ_POLL, pin-to-core, multishot
  `recv` over a registered `BufferRing`, `IO_LINK` chains, owned-fd `close`.
- **WebSocket client** (RFC 6455) — frame codec, masking (AVX2 + 8-byte
  chunked scalar fallback), streaming parser, fragment reassembly, close
  handshake, auto-pong, CSPRNG mask keys (RFC §10.3 compliant).
- **TLS** — `rustls` 0.23 driven by raw bytes (no `tokio` / no `async-std`),
  ALPN `http/1.1` requested **and verified**, `close_notify` surfaced to the
  caller.
- **HTTP/1.1 codec** — minimal, sized for WS Upgrade. Header size cap (16 KiB)
  / count cap (64) / explicit `Transfer-Encoding` reject for DoS hardening.
- **Pool** — single io_uring drives N WebSocket connections. CQE routing is
  O(1) slot-table lookup. `submit_connect` returns a handle immediately so
  N connections can hand-shake concurrently.

## Platform

Linux only at runtime. The crate compiles cleanly on macOS / Windows (a stub
`proactor` keeps types in scope so non-Linux IDEs can type-check the full
codebase), but every io_uring entry point panics with
`unimplemented!()` outside Linux. CI / production builds must target Linux.

Tested on Linux 6.x with io_uring features: `SETUP_SQPOLL`,
`REGISTER_PBUF_RING`, `OP_RECV_MULTISHOT`, `IOSQE_IO_LINK`.

## Quick start

```toml
[dependencies]
talaris = "0.1"
```

```rust,no_run
# #[cfg(target_os = "linux")]
# fn run() -> Result<(), Box<dyn std::error::Error>> {
use talaris::connection::ConnectionConfig;
use talaris::ws::Event as WsEvent;
use talaris::{Pool, PoolConfig};

let cfg = ConnectionConfig::new("www.deribit.com", 443, "/ws/api/v2")
    .with_sq_poll(100, None);
let mut pool = Pool::new(PoolConfig::new(cfg.proactor))?;
let h = pool.connect_blocking(cfg)?;

pool.send_text(h, br#"{"jsonrpc":"2.0","id":1,"method":"public/test","params":{}}"#)?;

loop {
    pool.pump(|handle, ev| {
        if let WsEvent::Text(s) = ev {
            println!("{handle:?}: {s}");
        }
    })?;
}
# Ok(())
# }
```

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
