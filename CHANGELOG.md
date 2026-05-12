# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Herbatka v1.0 definition of done (`docs/status/v1.md`): single-node scope, CI verification checklist, explicit not-in-v1.0 features, pointers to roadmap and risks.
- TCP protocol documentation: reference clients, minimal framed-client flow, and server implementation notes (`docs/reference/tcp-wire-protocol.md`).
- Integration tests for TCP legacy error lines, CRLF handshake, framed decode recovery, and oversize framing (`tcp_server_smoke`).

### Fixed

- Removed startup panic in `load_topic_state` when the trusted-skip invariant is violated; the broker now returns `BrokerError::Io(InvalidData)` instead of crashing during topic recovery.
