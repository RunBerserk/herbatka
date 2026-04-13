use serde::Deserialize;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

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
    pub fsync_policy: FsyncPolicy,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data/logs"),
            segment_max_bytes: 1024 * 1024,
            max_topic_bytes: None,
            fsync_policy: FsyncPolicy::Always,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
struct RawBrokerConfig {
    data_dir: Option<PathBuf>,
    segment_max_bytes: Option<u64>,
    max_topic_bytes: Option<u64>,
    fsync_policy: Option<FsyncPolicy>,
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
    if let Some(fsync_policy) = parsed.fsync_policy {
        config.fsync_policy = fsync_policy;
    }
    validate_broker_config(&config)?;
    Ok(config)
}

pub fn validate_broker_config(config: &BrokerConfig) -> io::Result<()> {
    if config.segment_max_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment_max_bytes must be > 0",
        ));
    }
    if let Some(max_topic_bytes) = config.max_topic_bytes {
        if max_topic_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_topic_bytes must be > 0 when set",
            ));
        }
    }
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
"#,
        )
        .unwrap();

        let cfg = load_broker_config(&path).unwrap();
        assert_eq!(cfg.data_dir, PathBuf::from("tmp/data"));
        assert_eq!(cfg.segment_max_bytes, 8192);
        assert_eq!(cfg.max_topic_bytes, Some(65536));
        assert_eq!(cfg.fsync_policy, FsyncPolicy::Never);
    }

    #[test]
    fn invalid_segment_size_is_rejected() {
        let cfg = BrokerConfig {
            segment_max_bytes: 0,
            ..BrokerConfig::default()
        };
        assert!(validate_broker_config(&cfg).is_err());
    }
}
