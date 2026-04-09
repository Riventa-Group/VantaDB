use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::{fs, io};

/// VantaDB configuration, loaded from TOML file with sensible defaults.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub rate_limit: RateLimitConfig,
    pub scheduler: SchedulerConfig,
    pub changefeed: ChangefeedConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub port: u16,
    pub tls: bool,
    pub data_dir: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    pub jwt_ttl_hours: i64,
    pub password_min_length: usize,
    pub lockout_max_attempts: u32,
    pub lockout_window_secs: u64,
    pub lockout_duration_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RateLimitConfig {
    pub global_rps: f64,
    pub per_ip_rps: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SchedulerConfig {
    pub reap_interval_secs: u64,
    pub gc_interval_secs: u64,
    pub compact_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ChangefeedConfig {
    pub buffer_size: usize,
}

// ---- Defaults -----------------------------------------------

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            auth: AuthConfig::default(),
            rate_limit: RateLimitConfig::default(),
            scheduler: SchedulerConfig::default(),
            changefeed: ChangefeedConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 5432,
            tls: true,
            data_dir: default_data_dir(),
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            jwt_ttl_hours: 24,
            password_min_length: 12,
            lockout_max_attempts: 5,
            lockout_window_secs: 300,
            lockout_duration_secs: 900,
        }
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            global_rps: 10.0,
            per_ip_rps: 3.0,
        }
    }
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            reap_interval_secs: 10,
            gc_interval_secs: 60,
            compact_interval_secs: 300,
        }
    }
}

impl Default for ChangefeedConfig {
    fn default() -> Self {
        Self {
            buffer_size: 10_000,
        }
    }
}

// ---- Loading ------------------------------------------------

impl Config {
    /// Load config from file, or return defaults if no file found.
    /// Search order: explicit path > ./vantadb.toml > ~/.config/vantadb/vantadb.toml
    pub fn load(explicit_path: Option<&str>) -> Self {
        if let Some(path) = explicit_path {
            match Self::from_file(Path::new(path)) {
                Ok(config) => return config,
                Err(e) => {
                    eprintln!("  warning: failed to load config from {}: {}", path, e);
                    return Self::default();
                }
            }
        }

        let candidates = [
            PathBuf::from("vantadb.toml"),
            dirs::config_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join("vantadb")
                .join("vantadb.toml"),
        ];

        for path in &candidates {
            if path.exists() {
                match Self::from_file(path) {
                    Ok(config) => return config,
                    Err(e) => {
                        eprintln!("  warning: failed to load config from {}: {}", path.display(), e);
                    }
                }
            }
        }

        Self::default()
    }

    fn from_file(path: &Path) -> Result<Self, String> {
        let content = fs::read_to_string(path)
            .map_err(|e| format!("read error: {}", e))?;
        toml::from_str(&content)
            .map_err(|e| format!("parse error: {}", e))
    }

    /// Dump config as TOML string.
    pub fn dump(&self) -> String {
        let mut out = String::new();
        out.push_str("[server]\n");
        out.push_str(&format!("port = {}\n", self.server.port));
        out.push_str(&format!("tls = {}\n", self.server.tls));
        out.push_str(&format!("data_dir = \"{}\"\n", self.server.data_dir));
        out.push_str("\n[auth]\n");
        out.push_str(&format!("jwt_ttl_hours = {}\n", self.auth.jwt_ttl_hours));
        out.push_str(&format!("password_min_length = {}\n", self.auth.password_min_length));
        out.push_str(&format!("lockout_max_attempts = {}\n", self.auth.lockout_max_attempts));
        out.push_str(&format!("lockout_window_secs = {}\n", self.auth.lockout_window_secs));
        out.push_str(&format!("lockout_duration_secs = {}\n", self.auth.lockout_duration_secs));
        out.push_str("\n[rate_limit]\n");
        out.push_str(&format!("global_rps = {}\n", self.rate_limit.global_rps));
        out.push_str(&format!("per_ip_rps = {}\n", self.rate_limit.per_ip_rps));
        out.push_str("\n[scheduler]\n");
        out.push_str(&format!("reap_interval_secs = {}\n", self.scheduler.reap_interval_secs));
        out.push_str(&format!("gc_interval_secs = {}\n", self.scheduler.gc_interval_secs));
        out.push_str(&format!("compact_interval_secs = {}\n", self.scheduler.compact_interval_secs));
        out.push_str("\n[changefeed]\n");
        out.push_str(&format!("buffer_size = {}\n", self.changefeed.buffer_size));
        out
    }
}

fn default_data_dir() -> String {
    dirs::data_dir()
        .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")))
        .join("vantadb")
        .to_string_lossy()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.port, 5432);
        assert!(config.server.tls);
        assert_eq!(config.auth.jwt_ttl_hours, 24);
        assert_eq!(config.auth.password_min_length, 12);
        assert_eq!(config.rate_limit.global_rps, 10.0);
        assert_eq!(config.scheduler.reap_interval_secs, 10);
        assert_eq!(config.changefeed.buffer_size, 10_000);
    }

    #[test]
    fn test_parse_partial_toml() {
        let toml_str = r#"
[server]
port = 9999

[auth]
jwt_ttl_hours = 48
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.server.port, 9999);
        assert_eq!(config.auth.jwt_ttl_hours, 48);
        // Unset values get defaults
        assert_eq!(config.rate_limit.global_rps, 10.0);
        assert_eq!(config.scheduler.gc_interval_secs, 60);
    }

    #[test]
    fn test_load_missing_file_returns_defaults() {
        let config = Config::load(Some("/nonexistent/path/vantadb.toml"));
        assert_eq!(config.server.port, 5432);
    }

    #[test]
    fn test_dump_roundtrip() {
        let config = Config::default();
        let dumped = config.dump();
        assert!(dumped.contains("port = 5432"));
        assert!(dumped.contains("jwt_ttl_hours = 24"));
        assert!(dumped.contains("buffer_size = 10000"));
    }
}
