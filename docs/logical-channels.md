# Logical channels (topic naming)

Applications can treat Herbatka as **three logical lanes**—**heartbeat**, **control**, and **telemetry**—without any broker API for “channel.” The broker only knows **topic names** and **opaque payloads**; semantics are a **convention** between producers and consumers (fleet, IoT, or any other domain).

## Lanes (intent)

| Lane | Role |
|------|------|
| **Heartbeat** | Periodic liveness / cadence (“still here”), small volume. |
| **Control** | Commands, acks, errors, warnings, critical signals; prefer **small** payloads and tight operational lifecycle (enforced by **policy and retention** once per-topic limits exist). |
| **Telemetry** | Event-driven data, **full** payloads (sensors, telemetry JSON, etc.). |

## Topic naming (canonical)

Use **three separate topics** per scope, with a fixed suffix:

`<scope>.heartbeat`  
`<scope>.control`  
`<scope>.telemetry`

- **`<scope>`** is any stable string for your deployment: tenant, environment, product, or fleet slice (e.g. `acme.prod`, `fleet1`, `region-eu`).
- The broker does **not** parse `scope` or suffix; it routes purely by the full topic string.

### Alternative (product prefix first)

If you prefer a shared product prefix: `myapp.<scope>.heartbeat`, or flat names like `events-heartbeat` / `events-control` / `events-telemetry`, that is fine—as long as producers and consumers agree. The important part is **three distinct topics**, not the exact spelling.

## Relation to other docs

- Transport: [TCP wire protocol](tcp-wire-protocol.md) (unchanged; channel is not on the wire—only in topic choice and payload).
- Default simulator/UI topic `events` in examples is **telemetry-style** naming (single topic); splitting into three topics is an application choice documented here, not required for MVP demos.

## Caveats (today)

- **`max_topic_bytes`** in [`herbatka.toml`](../herbatka.toml) applies **globally** across topics when set—not per lane. Tighter retention for **control** vs **telemetry** is a **future broker/config** change; see [status.md](status.md) **Next Up**.
- **Protobuf** (or other schemas) per lane is optional and lives **inside** framed message bodies; see **Next Up** in status.
