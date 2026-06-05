use std::{net::SocketAddr, path::Path};

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use spider_storage::{DatabaseConfig, DatabaseSslMode};

/// Top-level benchmark configuration loaded from TOML.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BenchConfig {
    pub server: ServerConfig,
    pub database: BenchDatabaseConfig,
    pub benchmark: BenchmarkConfig,
    #[serde(default)]
    pub distributed: Option<DistributedConfig>,
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
            ssl_mode: self.database.ssl_mode,
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
    #[serde(default)]
    pub ssl_mode: DatabaseSslMode,
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
            ssl_mode: config.ssl_mode,
        }
    }
}

/// Distributed benchmark controller defaults.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DistributedConfig {
    pub agent_timeout_sec: u64,
    pub poll_interval_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduler: Option<DistributedAgentConfig>,
    pub submitter: DistributedAgentConfig,
    pub workers: Vec<DistributedAgentConfig>,
}

impl DistributedConfig {
    /// Validates distributed benchmark settings.
    ///
    /// # Errors
    ///
    /// Returns an error if no agents are configured or an agent/control timeout is invalid.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.agent_timeout_sec == 0 {
            anyhow::bail!("distributed.agent_timeout_sec must be greater than 0");
        }
        if self.poll_interval_ms == 0 {
            anyhow::bail!("distributed.poll_interval_ms must be greater than 0");
        }
        if self.workers.is_empty() {
            anyhow::bail!("distributed.workers must contain at least one agent");
        }

        let mut ids = std::collections::HashSet::new();
        for agent in self
            .scheduler
            .iter()
            .chain(std::iter::once(&self.submitter))
            .chain(self.workers.iter())
        {
            if agent.id.trim().is_empty() {
                anyhow::bail!("distributed agent id must not be empty");
            }
            if agent.url.trim().is_empty() {
                anyhow::bail!("distributed agent `{}` url must not be empty", agent.id);
            }
            if !ids.insert(agent.id.as_str()) {
                anyhow::bail!("duplicate distributed agent id `{}`", agent.id);
            }
        }
        Ok(())
    }
}

/// One distributed benchmark client agent.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DistributedAgentConfig {
    pub id: String,
    pub url: String,
}

/// Benchmark workload and measurement defaults.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BenchmarkConfig {
    pub task_count: usize,
    pub job_count: usize,
    pub payload_bytes: usize,
    #[serde(default = "default_task_sleep_ms")]
    pub task_sleep_ms: u64,
    pub client_count: usize,
    pub worker_count: usize,
    pub worker_poll_wait_ms: u64,
    #[serde(default = "default_worker_empty_poll_sleep_min_ms")]
    pub worker_empty_poll_sleep_min_ms: u64,
    #[serde(default = "default_worker_empty_poll_sleep_max_ms")]
    pub worker_empty_poll_sleep_max_ms: u64,
    pub job_poll_wait_ms: u64,
    #[serde(default = "default_scheduler_active_job_pool_capacity")]
    pub scheduler_active_job_pool_capacity: usize,
    #[serde(default = "default_scheduler_dispatch_queue_capacity")]
    pub scheduler_dispatch_queue_capacity: usize,
    #[serde(default = "default_scheduler_ready_task_capacity")]
    pub scheduler_ready_task_capacity: usize,
    #[serde(default = "default_scheduler_commit_ready_task_capacity")]
    pub scheduler_commit_ready_task_capacity: usize,
    #[serde(default = "default_scheduler_cleanup_ready_task_capacity")]
    pub scheduler_cleanup_ready_task_capacity: usize,
    #[serde(default = "default_scheduler_max_serving_requests")]
    pub scheduler_max_serving_requests: usize,
    #[serde(default = "default_scheduler_tick_interval_ms")]
    pub scheduler_tick_interval_ms: u64,
    #[serde(default = "default_scheduler_storage_poll_wait_ms")]
    pub scheduler_storage_poll_wait_ms: u64,
    pub warmup_sec: u64,
    pub duration_sec: u64,
    pub flat_percent: u8,
    pub output_dir: String,
}

const fn default_task_sleep_ms() -> u64 {
    3
}

const fn default_worker_empty_poll_sleep_min_ms() -> u64 {
    1
}

const fn default_worker_empty_poll_sleep_max_ms() -> u64 {
    100
}

const fn default_scheduler_active_job_pool_capacity() -> usize {
    1024
}

const fn default_scheduler_dispatch_queue_capacity() -> usize {
    1024
}

const fn default_scheduler_ready_task_capacity() -> usize {
    1024
}

const fn default_scheduler_commit_ready_task_capacity() -> usize {
    1024
}

const fn default_scheduler_cleanup_ready_task_capacity() -> usize {
    1024
}

const fn default_scheduler_max_serving_requests() -> usize {
    1024
}

const fn default_scheduler_tick_interval_ms() -> u64 {
    10
}

const fn default_scheduler_storage_poll_wait_ms() -> u64 {
    20
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
        if self.worker_empty_poll_sleep_min_ms == 0 {
            anyhow::bail!("worker_empty_poll_sleep_min_ms must be greater than 0");
        }
        if self.worker_empty_poll_sleep_max_ms < self.worker_empty_poll_sleep_min_ms {
            anyhow::bail!(
                "worker_empty_poll_sleep_max_ms must be greater than or equal to \
                 worker_empty_poll_sleep_min_ms"
            );
        }
        if self.scheduler_active_job_pool_capacity == 0 {
            anyhow::bail!("scheduler_active_job_pool_capacity must be greater than 0");
        }
        if self.scheduler_dispatch_queue_capacity == 0 {
            anyhow::bail!("scheduler_dispatch_queue_capacity must be greater than 0");
        }
        if self.scheduler_ready_task_capacity == 0 {
            anyhow::bail!("scheduler_ready_task_capacity must be greater than 0");
        }
        if self.scheduler_commit_ready_task_capacity == 0 {
            anyhow::bail!("scheduler_commit_ready_task_capacity must be greater than 0");
        }
        if self.scheduler_cleanup_ready_task_capacity == 0 {
            anyhow::bail!("scheduler_cleanup_ready_task_capacity must be greater than 0");
        }
        if self.scheduler_max_serving_requests == 0 {
            anyhow::bail!("scheduler_max_serving_requests must be greater than 0");
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
        assert_eq!(3, config.benchmark.task_sleep_ms);
        assert_eq!(8, config.benchmark.client_count);
        assert_eq!(16, config.benchmark.worker_count);
        assert_eq!(1, config.benchmark.worker_empty_poll_sleep_min_ms);
        assert_eq!(100, config.benchmark.worker_empty_poll_sleep_max_ms);
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

    #[test]
    fn worker_empty_poll_sleep_range_rejects_invalid_values() {
        let mut config =
            BenchConfig::load("config/default.toml".as_ref()).expect("default config should load");

        config.benchmark.worker_empty_poll_sleep_min_ms = 0;
        let err = config
            .benchmark
            .validate()
            .expect_err("zero worker empty poll sleep min should fail");
        assert!(
            err.to_string().contains("worker_empty_poll_sleep_min_ms"),
            "error should mention worker empty poll sleep min"
        );

        config.benchmark.worker_empty_poll_sleep_min_ms = 10;
        config.benchmark.worker_empty_poll_sleep_max_ms = 5;
        let err = config
            .benchmark
            .validate()
            .expect_err("worker empty poll sleep max below min should fail");
        assert!(
            err.to_string().contains("worker_empty_poll_sleep_max_ms"),
            "error should mention worker empty poll sleep max"
        );
    }

    #[test]
    fn default_config_exposes_scheduler_core_settings() -> anyhow::Result<()> {
        let config = BenchConfig::load("config/default.toml".as_ref())?;
        assert_eq!(1024, config.benchmark.scheduler_active_job_pool_capacity);
        assert_eq!(32, config.benchmark.scheduler_dispatch_queue_capacity);
        assert_eq!(11_000, config.benchmark.scheduler_ready_task_capacity);
        assert_eq!(1024, config.benchmark.scheduler_commit_ready_task_capacity);
        assert_eq!(1024, config.benchmark.scheduler_cleanup_ready_task_capacity);
        assert_eq!(1024, config.benchmark.scheduler_max_serving_requests);
        assert_eq!(10, config.benchmark.scheduler_tick_interval_ms);
        assert_eq!(20, config.benchmark.scheduler_storage_poll_wait_ms);
        Ok(())
    }

    #[test]
    fn scheduler_core_settings_reject_zero_capacities() {
        let mut config =
            BenchConfig::load("config/default.toml".as_ref()).expect("default config should load");

        config.benchmark.scheduler_dispatch_queue_capacity = 0;

        let err = config
            .benchmark
            .validate()
            .expect_err("zero scheduler dispatch queue capacity should fail");
        assert!(
            err.to_string()
                .contains("scheduler_dispatch_queue_capacity"),
            "error should mention scheduler dispatch queue capacity"
        );
    }

    #[test]
    fn scheduler_serving_request_limit_rejects_zero() {
        let mut config =
            BenchConfig::load("config/default.toml".as_ref()).expect("default config should load");

        config.benchmark.scheduler_max_serving_requests = 0;

        let err = config
            .benchmark
            .validate()
            .expect_err("zero scheduler max serving requests should fail");
        assert!(
            err.to_string().contains("scheduler_max_serving_requests"),
            "error should mention scheduler max serving requests"
        );
    }

    #[test]
    fn default_config_includes_distributed_agent() -> anyhow::Result<()> {
        let config = BenchConfig::load("config/default.toml".as_ref())?;
        let distributed = config
            .distributed
            .as_ref()
            .expect("default config should include distributed benchmark defaults");
        distributed.validate()?;
        assert_eq!(1800, distributed.agent_timeout_sec);
        assert_eq!(1000, distributed.poll_interval_ms);
        assert_eq!("submitter-10-1-0-7", distributed.submitter.id);
        assert_eq!("http://10.1.0.7:19091", distributed.submitter.url);
        assert_eq!(1, distributed.workers.len());
        assert_eq!("worker-10-1-0-8", distributed.workers[0].id);
        assert_eq!("http://10.1.0.8:19091", distributed.workers[0].url);
        Ok(())
    }

    #[test]
    fn distributed_config_validates_agents() -> anyhow::Result<()> {
        let config: BenchConfig = toml::from_str(
            r#"
[server]
rest_bind = "127.0.0.1:8091"
grpc_bind = "127.0.0.1:50051"
rest_target = "http://127.0.0.1:8091"
grpc_target = "http://127.0.0.1:50051"

[database]
host = "127.0.0.1"
port = 3306
name = "spider-db"
username = "spider-user"
password = "spider-password"
max_connections = 32

[benchmark]
task_count = 10
job_count = 4
payload_bytes = 16
client_count = 2
worker_count = 2
worker_poll_wait_ms = 10
job_poll_wait_ms = 10
warmup_sec = 0
duration_sec = 0
flat_percent = 80
output_dir = "data/"

[distributed]
agent_timeout_sec = 120
poll_interval_ms = 500

[distributed.submitter]
id = "submitter-a"
url = "http://127.0.0.1:19091"

[[distributed.workers]]
id = "worker-a"
url = "http://127.0.0.1:19092"
"#,
        )?;
        config
            .distributed
            .as_ref()
            .expect("distributed config should exist")
            .validate()?;
        Ok(())
    }
}
