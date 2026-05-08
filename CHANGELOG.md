# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Removed startup panic in `load_topic_state` when the trusted-skip invariant is violated; the broker now returns `BrokerError::Io(InvalidData)` instead of crashing during topic recovery.
