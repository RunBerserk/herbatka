# Herbatka TCP wire protocol

This document specifies the **legacy line protocol** and **framed wire v1** used by the `herbatka` TCP server. It is the contract for client implementations and future evolution (e.g. Protobuf inside frame bodies).

## Connections

- TCP, configured by `listen_addr` in `herbatka.toml` (default `127.0.0.1:7000`).
- One broker request per client message in both modes; server sends exactly one response frame or one response line per request (sequential, no pipelining assumption required for v1).

## Legacy text mode (backward compatible)

Used when the **first line** from the client is **not** the framed-mode handshake (see below).

- Messages are **newline-delimited** (`\n` LF). Optional `\r` before `\n` should be handled by trimming; clients should send `\n`.
- **PRODUCE:** `PRODUCE <topic><space><payload...>\n` — only the first space separates topic from payload; the remainder of the line is the payload (may contain spaces).
- **FETCH:** `FETCH <topic> <offset>\n` — topic must not contain whitespace; offset is decimal `u64`.
- **Responses:** single line each, newline-terminated:
  - `OK <u64 offset>\n`
  - `MSG <u64 offset> <payload>\n` — payload is the rest of the line after the second space; see **Message body encoding** above (lossy UTF-8 when formatting from stored bytes).
  - `NONE\n`
  - `ERR <reason>\n` — human-readable reason.

**Limits:** The server reads the first line with a bounded buffer (64 KiB including newline). Oversize lines are rejected.

**Interoperability caveat:** Binary payloads are **not** reliably representable in legacy `MSG`/`PRODUCE` lines; use framed v1 for opaque bytes. Legacy [`parse_request`](../../crates/herbatka-wire/src/tcp/command.rs) builds PRODUCE payloads from a Unicode line slice (`&str`), so arbitrary binary cannot be submitted on this path anyway.

### Message body encoding (authoritative vs display)

Cross-cutting rule for implementations:

- **Authoritative representation:** a stored/fetched message **body** is **raw bytes** on disk and in framed **Message** responses (`body: [u8]`). Nothing in the broker re-encodes the body for framed clients.
- **Legacy `MSG` lines:** when the server formats a line-mode `MSG`, the embedded payload segment is produced with a **lossy UTF-8 decode** (invalid sequences become U+FFFD). Payloads containing **NUL** or **newlines** are still unsafe for line-oriented clients; use framed v1 for opaque or binary data.
- **CLI / UI:** tools that print lines or feed text-oriented parsers (e.g. JSON in the current fleet UI) may apply the same **lossy decode for display or parsing**. That can **change** bytes (replacement characters) without surfacing an error. Structured consumers (e.g. future Protobuf-in-body) should decode from **`Vec<u8>`** or raw frame payload, not from a lossy `String`.
- **Current fleet JSON producers** (simulator, typical `PRODUCE` text) should send **valid UTF-8** JSON. Non–UTF-8 message bodies are **out of contract** for the JSON-based UI; they may increment parse errors or mis-display.

Reference implementation: [`lossy_utf8_message_body_for_display`](../../crates/herbatka-wire/src/tcp/encoding.rs) in `herbatka-wire` (used for legacy `MSG` formatting and human-oriented clients).

---

## Framed wire v1

### Handshake (client first, then server)

After TCP connect, the client sends **exactly** one line (UTF-8):

```text
HERBATKA WIRE/1\n
```

The server responds with **exactly**:

```text
HERBATKA OK/1\n
```

After this, **all further data** on the connection uses **binary frames** described below. No interleaved line protocol.

If the first line is not the framed handshake (including optional `\r` before `\n`, i.e. `HERBATKA WIRE/1\r\n` is accepted), the server treats it as the first **legacy** command and never enters framed mode on that connection.

### Frame envelope (both directions)

Multi-byte integers are **little-endian** unless noted.

| Offset | Size | Field |
|--------|------|--------|
| 0 | 1 | `version` — must be `1` for wire v1 |
| 1 | 1 | `op` — opcode (see below) |
| 2 | 2 | `flags` — reserved, must be `0` in v1 |
| 4 | 4 | `payload_len` — byte length of payload following the header |

**Header size:** 8 bytes.

**Limits:** `payload_len` must not exceed **16 MiB** (`16_777_216`). The server rejects larger values without allocating the payload. Topics in request bodies are additionally capped to **4096 UTF-8 bytes** (enforced when decoding produce/fetch bodies).

Invalid `version` or non-zero `flags` (v1) result in the server closing the connection or returning an error frame, depending on recoverability; implementations should treat framing errors as fatal for the connection.

### Client → server ops and bodies

| `op` | Name | `payload` layout |
|------|------|------------------|
| `1` | Produce | `topic_len: u16 LE`, `topic: UTF-8` (`topic_len` bytes), `body_len: u32 LE`, `body: [u8]` (`body_len` bytes). `body` may be empty only if `body_len == 0`; `topic` must not be empty. |
| `2` | Fetch | `topic_len: u16 LE`, `topic: UTF-8`, `offset: u64 LE` |
| `3` | TopicBounds | `topic_len: u16 LE`, `topic: UTF-8` only (framed-only; no legacy line form). Returns readable offset range for the topic. |

### Server → client ops and bodies

| `op` | Name | Meaning | `payload` layout |
|------|------|---------|------------------|
| `16` | OkOffset | Produce accepted | `offset: u64 LE` |
| `17` | Message | Fetch returned a record | `offset: u64 LE`, `body_len: u32 LE`, `body` (`body_len` bytes) — raw message bytes |
| `18` | None | No message at offset | empty |
| `19` | Error | Request failed | `reason_len: u32 LE`, `reason: UTF-8` (`reason_len` bytes) |
| `20` | TopicRange | Reply to TopicBounds | `min_offset: u64 LE`, `exclusive_end: u64 LE` — valid `FETCH` offsets satisfy `min_offset <= offset < exclusive_end`; `exclusive_end` is the next offset assigned on produce. |

Responses use the same 8-byte envelope as requests.

### Error handling

- Legacy: `ERR …` line with free-form text.
- Framed: **Error** opcode with UTF-8 reason. Clients should not assume nested framing after an error unless a future version specifies it.

**TopicBounds in legacy mode:** there is no line command for `TopicBounds`. If a line-mode response would need that shape, the server responds with `ERR topic bounds not available in line mode\n` (see [`format_response`](../../crates/herbatka-wire/src/tcp/command.rs)).

### Reference clients (framed v1)

These use `herbatka_wire::tcp::frame::perform_client_handshake` (or the same bytes) and framed produce/fetch:

- [`crates/herbatka/src/bin/producer.rs`](../../crates/herbatka/src/bin/producer.rs) — single produce
- [`crates/herbatka/src/bin/consumer.rs`](../../crates/herbatka/src/bin/consumer.rs) — fetch loop
- [`crates/herbatka-simulator/src/transport.rs`](../../crates/herbatka-simulator/src/transport.rs) — load generator
- [`crates/herbatka-ui/src/broker_client.rs`](../../crates/herbatka-ui/src/broker_client.rs) — UI broker access

### Minimal framed client flow

1. `TcpStream::connect(listen_addr)`.
2. Write `HERBATKA WIRE/1\n`, read one line; expect `HERBATKA OK/1` (optional `\r` before `\n` on either side). In Rust, `perform_client_handshake` wraps this.
3. For each request: write one full frame (8-byte header + payload), `flush`, read one full response frame (same envelope), decode op + payload.
4. Use **Produce** / **Fetch** / **TopicBounds** request opcodes as in the tables above; interpret **OkOffset**, **Message**, **None**, **Error**, **TopicRange** responses.

---

## Implementation notes (herbatka server)

**Accept loop:** The **`herbatka`** binary uses **Tokio** ([`run`](../../crates/herbatka/src/tcp/server.rs)) for **`TcpListener::bind`** / **`accept`** (listener upgraded from a **non-blocking** `std::net::TcpListener` via **`from_std`**), then **`into_std()`** on each accepted socket, **`set_nonblocking(false)`**, and a **dedicated `std::thread`** for sync [`handle_client`](../../crates/herbatka/src/tcp/server.rs). Integration tests use blocking [`serve`](../../crates/herbatka/src/tcp/server.rs). Broker access uses **[`SharedBroker`](../../crates/herbatka/src/tcp/server.rs)** (`Arc<RwLock<Broker>>`): framed **Fetch** / **TopicBounds** take a **read** lock (so concurrent reads can overlap); **Produce** and topic creation take a **write** lock (writes remain globally serialized at the broker layer).

Behavior of [`run_framed_connection`](../../crates/herbatka/src/tcp/server.rs) in v1:

| Situation | Server behavior |
|-----------|-----------------|
| Framed request decodes, broker returns error | One **Error** response frame; connection stays open for the next frame. |
| Framed request fails **frame decode** (`decode_client_frame`, e.g. unknown `op`, bad layout) | One **Error** response frame with a text reason; connection stays open. |
| **read_frame** fails with I/O error or clean EOF before a complete frame | No response; connection ends (client disconnect or truncated stream). |
| **read_frame** fails for other reasons (e.g. unsupported `version`/`flags`, `payload_len` over 16 MiB, length mismatch) | Server attempts one **Error** response frame, then closes the framed loop (connection may be half-closed from the client’s perspective). |

**Legacy:** parse failures and broker errors become a single `ERR …` line; unknown-topic **fetch** returns `ERR unknown topic` (broker does not auto-create topics on fetch).

**Oversize first line** (including legacy or handshake line): [`read_first_line`](../../crates/herbatka-wire/src/tcp/frame.rs) errors when the line exceeds **64 KiB** (including newline); `handle_client` surfaces that as an I/O error and closes the connection.

---

## Compatibility and future work

- **Protobuf** (or similar) may be embedded in Produce `body` / Message `body` without changing this envelope once clients agree on serialization. Example message layouts keyed by logical topic suffix (**heartbeat / control / telemetry**) and generated Rust bindings live in `herbatka-wire` under [`proto/`](../../crates/herbatka-wire/proto/herbatka_fleet.proto) and [`generated_schemas.rs`](../../crates/herbatka-wire/src/generated_schemas.rs); see [logical-channels.md — Protobuf payloads](logical-channels.md#protobuf-payloads-optional).
- **Legacy mode** remains available for debugging and trivial clients (`telnet`/netcat style).
- Negotiation rule: handshake line is deterministic; strings `PRODUCE` and `FETCH` do **not** collide with `HERBATKA WIRE/1`.
