//! The data types the scheduler exchanges with the storage layer and execution managers.

use spider_core::types::id::{JobId, ResourceGroupId};
use spider_storage::cache::TaskId;

/// A ready task drained from the storage-owned inbound queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboundEntry {
    pub resource_group_id: ResourceGroupId,
    pub job_id: JobId,
    pub task_id: TaskId,
}

/// A task placement decision written by the scheduler core to the dispatching queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskAssignment {
    pub resource_group_id: ResourceGroupId,
    pub job_id: JobId,
    pub task_id: TaskId,
}
