//! Docker-compose fault injection for end-to-end recovery tests.
//!
//! [`ComposeFaultController`] shells out to `docker compose` to stop, start, or restart individual
//! stack services, so a recovery test can kill a component mid-job and bring it back. The
//! controller reads the compose file path and project name from the environment, keeping the test
//! binary decoupled from the Taskfile's on-disk layout.

use std::path::PathBuf;
use std::process::Command;

use anyhow::Context;
use anyhow::bail;

/// Environment variable holding the absolute path to the compose file controlling the stack.
const COMPOSE_FILE_ENV_VAR: &str = "SPIDER_COMPOSE_FILE";

/// Environment variable holding the `docker compose -p` project name for the stack.
const COMPOSE_PROJECT_ENV_VAR: &str = "SPIDER_COMPOSE_PROJECT";

/// Drives fault injection for a docker-compose Spider stack by invoking `docker compose`.
///
/// Each method blocks until `docker compose` reports the operation complete. The blocking
/// `Command` runs on a `spawn_blocking` thread so the surrounding tokio runtime stays free while
/// `docker compose` stops or starts containers.
pub struct ComposeFaultController {
    /// Absolute path to the compose file passed to `docker compose -f`.
    compose_file: PathBuf,

    /// Compose project name passed to `docker compose -p`.
    compose_project: String,
}

impl ComposeFaultController {
    /// Builds a controller from [`COMPOSE_FILE_ENV_VAR`] and [`COMPOSE_PROJECT_ENV_VAR`].
    ///
    /// # Errors
    ///
    /// Returns an error if either environment variable is unset.
    pub fn from_env() -> anyhow::Result<Self> {
        let compose_file = std::env::var(COMPOSE_FILE_ENV_VAR)
            .with_context(|| format!("{COMPOSE_FILE_ENV_VAR} is not set"))?;
        let compose_project = std::env::var(COMPOSE_PROJECT_ENV_VAR)
            .with_context(|| format!("{COMPOSE_PROJECT_ENV_VAR} is not set"))?;
        Ok(Self {
            compose_file: PathBuf::from(compose_file),
            compose_project,
        })
    }

    /// Halts `service` without removing it; a `restart` policy does not revive a manually-stopped
    /// container, so the service stays down until [`Self::start`] is called.
    ///
    /// # Errors
    ///
    /// Forwards [`ComposeFaultController::compose`]'s return values on failure.
    pub async fn stop(&self, service: &str) -> anyhow::Result<()> {
        self.compose("stop", service).await
    }

    /// Starts a previously-stopped `service`.
    ///
    /// # Errors
    ///
    /// Forwards [`ComposeFaultController::compose`]'s return values on failure.
    pub async fn start(&self, service: &str) -> anyhow::Result<()> {
        self.compose("start", service).await
    }

    /// Stops and then starts `service` in one operation.
    ///
    /// # Errors
    ///
    /// Forwards [`ComposeFaultController::compose`]'s return values on failure.
    pub async fn restart(&self, service: &str) -> anyhow::Result<()> {
        self.compose("restart", service).await
    }

    /// Runs `docker compose -f <file> -p <project> <verb> <service>` and fails on a non-zero exit.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * The `docker` process cannot be spawned.
    /// * The `spawn_blocking` task panics.
    /// * `docker compose` exits with a non-zero status.
    async fn compose(&self, verb: &str, service: &str) -> anyhow::Result<()> {
        let compose_file = self.compose_file.clone();
        let compose_project = self.compose_project.clone();
        let service = service.to_owned();
        let verb = verb.to_owned();

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let output = Command::new("docker")
                .args([
                    "compose",
                    "-f",
                    &compose_file.to_string_lossy(),
                    "-p",
                    &compose_project,
                    &verb,
                    &service,
                ])
                .output()
                .with_context(|| format!("failed to spawn `docker compose {verb}`"))?;
            if !output.status.success() {
                bail!(
                    "`docker compose {verb} {service}` failed (exit {:?}): {}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr).trim(),
                );
            }
            Ok(())
        })
        .await
        .context("`docker compose` spawn_blocking task panicked")?
    }
}
