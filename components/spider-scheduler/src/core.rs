//! The abstract core of a Spider scheduler.

use async_trait::async_trait;

use crate::{
    dispatch_queue::DispatchQueueSink,
    error::SchedulerError,
    storage_client::SchedulerStorageClient,
};

/// An abstracted core for a scheduling algorithm.
#[async_trait]
pub trait SchedulerCore: Send {
    /// The storage client used by the core to poll and read for placement decisions.
    type StorageClient: SchedulerStorageClient;

    /// The dispatch sink the core writes assignments to.
    type Sink: DispatchQueueSink;

    /// Runs the scheduling loop until `cancellation_token` is triggered.
    ///
    /// # Errors
    ///
    /// Returns a [`SchedulerError`] instance indicating an irrecoverable error.
    async fn run(
        self,
        storage_client: Self::StorageClient,
        sink: Self::Sink,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Result<(), SchedulerError>;
}
