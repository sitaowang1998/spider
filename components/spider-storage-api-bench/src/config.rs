use std::{net::SocketAddr, path::Path};

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use spider_storage::DatabaseConfig;

/// Top-level benchmark configuration loaded from TOML.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BenchConfig {
    pub server: ServerConfig,
    pub database: BenchDatabaseConfig,
    pub benchmark: BenchmarkConfig,
}

impl BenchConfig {
    /// Loads a benchmark config from a TOML file.
    ///
    /// # Returns
    ///
    /// The loaded benchmark config on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`std::fs::read_to_string`]'s return values on failure.
    /// * Forwards [`toml::from_str`]'s return values on failure.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&content)?)
    }

    /// Converts the benchmark database config into the storage-layer database config.
    ///
    /// # Returns
    ///
    /// A [`DatabaseConfig`] suitable for [`spider_storage::state::create_server_runtime`].
    #[must_use]
    pub fn database_config(&self) -> DatabaseConfig {
        DatabaseConfig {
            host: self.database.host.clone(),
            port: self.database.port,
            name: self.database.name.clone(),
            username: self.database.username.clone(),
            password: SecretString::from(self.database.password.clone()),
            max_connections: self.database.max_connections,
        }
    }
}

/// Server address defaults.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ServerConfig {
    pub rest_bind: SocketAddr,
    pub grpc_bind: SocketAddr,
    pub rest_target: String,
    pub grpc_target: String,
}

/// Database defaults used by helper scripts and the Rust server.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BenchDatabaseConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub username: String,
    pub password: String,
    pub max_connections: u32,
}

impl From<&DatabaseConfig> for BenchDatabaseConfig {
    fn from(config: &DatabaseConfig) -> Self {
        Self {
            host: config.host.clone(),
            port: config.port,
            name: config.name.clone(),
            username: config.username.clone(),
            password: config.password.expose_secret().to_owned(),
            max_connections: config.max_connections,
        }
    }
}

/// Benchmark workload and measurement defaults.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BenchmarkConfig {
    pub task_count: usize,
    pub job_count: usize,
    pub payload_bytes: usize,
    pub client_count: usize,
    pub worker_count: usize,
    pub poll_batch: usize,
    pub poll_wait_ms: u64,
    pub warmup_sec: u64,
    pub duration_sec: u64,
    pub flat_percent: u8,
    pub output_dir: String,
}

impl BenchmarkConfig {
    /// Validates user-tunable benchmark settings.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * `flat_percent` is greater than 100.
    /// * a required positive count is zero.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.flat_percent > 100 {
            anyhow::bail!("flat_percent must be between 0 and 100");
        }
        if self.task_count == 0 {
            anyhow::bail!("task_count must be greater than 0");
        }
        if self.job_count == 0 {
            anyhow::bail!("job_count must be greater than 0");
        }
        if self.client_count == 0 {
            anyhow::bail!("client_count must be greater than 0");
        }
        if self.worker_count == 0 {
            anyhow::bail!("worker_count must be greater than 0");
        }
        if self.poll_batch == 0 {
            anyhow::bail!("poll_batch must be greater than 0");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::BenchConfig;

    #[test]
    fn default_config_loads_and_validates() -> anyhow::Result<()> {
        let config = BenchConfig::load("config/default.toml".as_ref())?;
        config.benchmark.validate()?;
        assert_eq!(80, config.benchmark.flat_percent);
        assert_eq!(8, config.benchmark.client_count);
        assert_eq!(16, config.benchmark.worker_count);
        assert_eq!("spider-db", config.database.name);
        Ok(())
    }

    #[test]
    fn flat_percent_rejects_values_above_100() {
        let mut config =
            BenchConfig::load("config/default.toml".as_ref()).expect("default config should load");
        config.benchmark.flat_percent = 101;
        let err = config
            .benchmark
            .validate()
            .expect_err("invalid flat_percent should fail");
        assert!(
            err.to_string().contains("flat_percent"),
            "error should mention flat_percent"
        );
    }
}
