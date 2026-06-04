//! The scheduler's view of the storage layer.

use std::time::Duration;

use async_trait::async_trait;
use spider_core::{
    job::JobState,
    types::id::{JobId, SessionId},
};

use crate::{error::StorageClientError, types::InboundEntry};

/// The scheduler's view of the storage layer.
#[async_trait]
pub trait SchedulerStorageClient: Send + Sync + Clone {
    /// Polls the regular-task lane of the storage-owned inbound queue.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageClientError`] if the storage poll fails.
    async fn poll_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError>;

    /// Polls the commit-task lane of the storage-owned inbound queue.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageClientError`] if the storage poll fails.
    async fn poll_commit_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError>;

    /// Polls the cleanup-task lane of the storage-owned inbound queue.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageClientError`] if the storage poll fails.
    async fn poll_cleanup_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError>;

    /// Reads the current state of a job.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageClientError`] if the job cannot be read.
    async fn job_state(&self, job_id: JobId) -> Result<JobState, StorageClientError>;
}
