# Talaris HFT WebSocket 调优控制面

日期：2026-06-02

这版只暴露底层数据结构变量，不引入交易所/频道 profile。默认值保持旧行为；bench CLI 中除 `--buf-size` / `--buf-entries` 外，`0` 表示使用库默认或原推导值。

## Library API

| 层级 | 变量 | 影响的数据结构 / 路径 | 默认 |
| --- | --- | --- | --- |
| `ProactorConfig` | `entries` | io_uring SQ/CQ 容量 | `256` |
| `ProactorConfig` | `sq_poll_idle_ms`, `sq_poll_cpu` | SQ_POLL kernel thread 行为 | off |
| `PoolConfig` | `initial_conn_capacity` | `Pool.conns: Vec<Option<ConnectionState>>` 初始容量 | `0` |
| `PoolConfig` | `completion_batch_capacity` | `Pool.completions_buf: Vec<Completion>` 初始容量 | `16` |
| `ConnectionConfig` | `buf_ring_slot_size` | provided buffer ring 单 slot 字节数 | `4096` |
| `ConnectionConfig` | `buf_ring_entries` | provided buffer ring slot 数 | `256` |
| `ConnectionConfig` | `send_buffer_initial_capacity` | socket/TLS outbound staging `send_buf` | `buf_ring_slot_size` |
| `ConnectionConfig` | `tls_pending_out_initial_capacity` | in-flight TLS ciphertext staging | `buf_ring_slot_size` |
| `ConnectionConfig` | `ws_config` | 注入 `WsConfig`，host/path 仍由连接 endpoint 覆盖 | none |
| `WsConfig` | `max_message_size` | fragmented message 协议上限 | `8 MiB` |
| `WsConfig` | `max_frame_payload` | 单 frame payload 协议上限 | `16 MiB` |
| `WsConfig` | `initial_recv_buffer_capacity` | `WsClient.recv_buf: CursorBuf` 初始容量 | `max_message_size + MAX_HEADER_LEN` |
| `WsConfig` | `initial_message_buffer_capacity` | `WsClient.msg_buf: Vec<u8>` 初始容量 | `max_message_size` |
| `WsConfig` | `initial_tx_buffer_capacity` | `WsClient.tx_buf: Vec<u8>` 初始容量 | `max_message_size + MAX_HEADER_LEN` |
| `WsConfig` | `auto_pong` | Ping/Pong control frame 行为 | `true` |
| `ws::MASK_POOL_BYTES` | constant only | outbound mask key 内联数组大小 | `256` |

`ws::MASK_POOL_BYTES` 当前不是运行时变量。它是 `WsClient` 内联数组大小；改成运行时容量需要把该字段 heap 化或引入 const generic，市场行情订阅的稳态 outbound frame 很少，优先级低于 ingress buffer/ring/batch 扫描。

## Bench CLI

已接入同一套 talaris 调参参数的 bench：

- `binance_futures_live`
- `binance_live`
- `ws_ingress_single`
- `ws_ingress_tls`
- `ws_ingress_json`
- `ws_ingress_fanout`

通用参数：

```bash
--proactor-entries N
--buf-size BYTES
--buf-entries N
--pool-conn-cap N
--completion-batch-cap N
--send-buf-cap BYTES
--tls-pending-out-cap BYTES
--ws-max-message-size BYTES
--ws-max-frame-payload BYTES
--ws-recv-buf-cap BYTES
--ws-msg-buf-cap BYTES
--ws-tx-buf-cap BYTES
```

`ws_ingress_raw` 直接压 `BufferRing`，仍只暴露 `--buf-size` / `--buf-entries`，因为它不经过 `ConnectionConfig` / `PoolConfig` / `WsClient`。

## 建议扫描顺序

1. 先扫 `--buf-size` / `--buf-entries`，看 CQE/frame 与 ring exhaustion。
2. 再扫 `--ws-recv-buf-cap` / `--ws-msg-buf-cap`，验证小 frame 场景降低初始 heap footprint 是否影响 tail。
3. 高 fanout 下扫 `--pool-conn-cap` / `--completion-batch-cap`。
4. 最后扫 `--proactor-entries` 与 SQ_POLL CPU 拓扑；这类变量更依赖机器和内核。
