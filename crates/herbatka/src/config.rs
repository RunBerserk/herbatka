//! Broker configuration model and TOML loader.
//! Defines runtime settings for data directory, segmentation, retention, fsync mode, and TCP bind.
//! Applies defaults and validates user-provided values before startup.

use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FsyncPolicy {
    Always,
    Never,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerConfig {
    pub data_dir: PathBuf,
    pub segment_max_bytes: u64,
    pub max_topic_bytes: Option<u64>,
    /// Byte cap overrides by **exact topic name**. When a topic appears here, this value wins;
    /// otherwise [`Self::topic_retention_byte_limit`] falls back to `max_topic_bytes`.
    pub per_topic_max_bytes: HashMap<String, u64>,
    pub fsync_policy: FsyncPolicy,
    /// `"host:port"` accepted by [`SocketAddr`]; used by `herbatka` binary only (library tests ignore).
    pub listen_addr: String,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data/logs"),
            segment_max_bytes: 1024 * 1024,
            max_topic_bytes: None,
            per_topic_max_bytes: HashMap::new(),
            fsync_policy: FsyncPolicy::Always,
            listen_addr: "127.0.0.1:7000".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
struct RawBrokerConfig {
    data_dir: Option<PathBuf>,
    segment_max_bytes: Option<u64>,
    max_topic_bytes: Option<u64>,
    #[serde(default)]
    per_topic_max_bytes: HashMap<String, u64>,
    fsync_policy: Option<FsyncPolicy>,
    listen_addr: Option<String>,
}

pub fn load_broker_config(path: &Path) -> io::Result<BrokerConfig> {
    let raw = fs::read_to_string(path)?;
    let parsed: RawBrokerConfig = toml::from_str(&raw)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("invalid config: {e}")))?;
    let mut config = BrokerConfig::default();
    if let Some(data_dir) = parsed.data_dir {
        config.data_dir = data_dir;
    }
    if let Some(segment_max_bytes) = parsed.segment_max_bytes {
        config.segment_max_bytes = segment_max_bytes;
    }
    if let Some(max_topic_bytes) = parsed.max_topic_bytes {
        config.max_topic_bytes = Some(max_topic_bytes);
    }
    config.per_topic_max_bytes = parsed.per_topic_max_bytes;
    if let Some(fsync_policy) = parsed.fsync_policy {
        config.fsync_policy = fsync_policy;
    }
    if let Some(listen_addr) = parsed.listen_addr {
        config.listen_addr = listen_addr.trim().to_string();
    }
    validate_broker_config(&config)?;
    Ok(config)
}

impl BrokerConfig {
    /// Effective retained size cap for `topic`: per-topic override, else [`Self::max_topic_bytes`].
    pub fn topic_retention_byte_limit(&self, topic: &str) -> Option<u64> {
        self.per_topic_max_bytes
            .get(topic)
            .copied()
            .or(self.max_topic_bytes)
    }
}

pub fn validate_broker_config(config: &BrokerConfig) -> io::Result<()> {
    if config.segment_max_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment_max_bytes must be > 0",
        ));
    }
    if let Some(max_topic_bytes) = config.max_topic_bytes
        && max_topic_bytes == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "max_topic_bytes must be > 0 when set",
        ));
    }
    for (topic, bytes) in &config.per_topic_max_bytes {
        if topic.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "per_topic_max_bytes topic keys must not be empty",
            ));
        }
        if *bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("per_topic_max_bytes[{topic:?}] must be > 0"),
            ));
        }
    }
    let trimmed = config.listen_addr.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "listen_addr must not be empty",
        ));
    }
    SocketAddr::from_str(trimmed).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("listen_addr must be a valid socket address (e.g. 127.0.0.1:7000): {e}"),
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn default_config_is_valid() {
        let cfg = BrokerConfig::default();
        validate_broker_config(&cfg).unwrap();
    }

    #[test]
    fn load_config_overrides_defaults() {
        let dir = std::env::temp_dir().join(format!(
            "herbatka_cfg_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("herbatka.toml");
        fs::write(
            &path,
            r#"
data_dir = "tmp/data"
segment_max_bytes = 8192
max_topic_bytes = 65536
fsync_policy = "never"
listen_addr = "0.0.0.0:9092"
"#,
        )
        .unwrap();

        let cfg = load_broker_config(&path).unwrap();
        assert_eq!(cfg.data_dir, PathBuf::from("tmp/data"));
        assert_eq!(cfg.segment_max_bytes, 8192);
        assert_eq!(cfg.max_topic_bytes, Some(65536));
        assert_eq!(cfg.fsync_policy, FsyncPolicy::Never);
        assert_eq!(cfg.listen_addr, "0.0.0.0:9092");
    }

    #[test]
    fn invalid_segment_size_is_rejected() {
        let cfg = BrokerConfig {
            segment_max_bytes: 0,
            ..BrokerConfig::default()
        };
        assert!(validate_broker_config(&cfg).is_err());
    }

    #[test]
    fn invalid_listen_addr_is_rejected() {
        let cfg = BrokerConfig {
            listen_addr: "not-a-socket-address".into(),
            ..BrokerConfig::default()
        };
        assert!(validate_broker_config(&cfg).is_err());
    }

    #[test]
    fn load_config_per_topic_max_bytes() {
        let dir = std::env::temp_dir().join(format!(
            "herbatka_cfg_topics_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("herbatka.toml");
        fs::write(
            &path,
            r#"
listen_addr = "127.0.0.1:7000"
[per_topic_max_bytes]
a = 100
"b.c" = 200
"#,
        )
        .unwrap();

        let cfg = load_broker_config(&path).unwrap();
        assert_eq!(cfg.per_topic_max_bytes.get("a"), Some(&100));
        assert_eq!(cfg.per_topic_max_bytes.get("b.c"), Some(&200));
        assert_eq!(cfg.topic_retention_byte_limit("a"), Some(100));
        assert_eq!(cfg.topic_retention_byte_limit("b.c"), Some(200));
        assert_eq!(cfg.topic_retention_byte_limit("other"), None);
    }

    #[test]
    fn topic_retention_falls_back_to_global() {
        let cfg = BrokerConfig {
            max_topic_bytes: Some(999),
            per_topic_max_bytes: [("only".into(), 111)].into_iter().collect(),
            ..BrokerConfig::default()
        };
        validate_broker_config(&cfg).unwrap();
        assert_eq!(cfg.topic_retention_byte_limit("only"), Some(111));
        assert_eq!(cfg.topic_retention_byte_limit("x"), Some(999));
    }

    #[test]
    fn per_topic_zero_is_rejected() {
        let mut bad = BrokerConfig::default();
        bad.per_topic_max_bytes.insert("t".into(), 0);
        assert!(validate_broker_config(&bad).is_err());
    }

    #[test]
    fn per_topic_empty_key_is_rejected() {
        let mut bad = BrokerConfig::default();
        bad.per_topic_max_bytes.insert(String::new(), 64);
        assert!(validate_broker_config(&bad).is_err());
    }
}
