//! Trait and type abstractions for the Spider scheduler.
//!
//! The scheduler is the serial decision maker that turns ready tasks discovered by the storage
//! layer into assignments for execution managers.

pub mod core;
pub mod core_impl;
pub mod dispatch_queue;
pub mod error;
pub mod storage_client;
pub mod types;

pub use crate::{
    core::SchedulerCore,
    dispatch_queue::{DispatchQueueSink, DispatchQueueSource},
    error::{SchedulerError, StorageClientError},
    storage_client::SchedulerStorageClient,
    types::{InboundEntry, TaskAssignment},
};
