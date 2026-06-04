//! The dispatching queue between the scheduler core and workers.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use spider_core::types::id::SessionId;
use tokio::sync::RwLock;

use crate::{error::SchedulerError, types::TaskAssignment};

/// The writer side of the dispatching queue used by the scheduler core.
#[async_trait]
pub trait DispatchQueueSink: Send + Sync + Clone {
    /// Enqueues a task assignment for execution managers to consume.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::DispatchQueueClosed`] if the queue is closed.
    async fn enqueue(&self, assignment: TaskAssignment) -> Result<(), SchedulerError>;

    /// Bumps the session ID and invalidates all queued task assignments.
    ///
    /// # Errors
    ///
    /// Returns a [`SchedulerError`] if the queue is closed or the session ID is invalid.
    async fn bump_session_id(&self, new_session_id: SessionId) -> Result<(), SchedulerError>;

    /// # Returns
    ///
    /// The current size of the dispatch queue.
    fn size(&self) -> usize;
}

/// The reader side of the dispatching queue, drained by workers.
#[async_trait]
pub trait DispatchQueueSource: Send + Sync + Clone {
    /// Dequeues the next task assignment for an execution manager to execute.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::DispatchQueueClosed`] if the queue is closed.
    async fn dequeue(
        &self,
        wait_time: Duration,
    ) -> Result<Option<(SessionId, TaskAssignment)>, SchedulerError>;
}

/// A cloneable writer handle for the dispatching queue.
#[derive(Clone)]
pub struct DispatchQueueWriter {
    inner: Arc<DispatchQueueWriterInner>,
}

#[async_trait]
impl DispatchQueueSink for DispatchQueueWriter {
    async fn enqueue(&self, assignment: TaskAssignment) -> Result<(), SchedulerError> {
        self.inner
            .assignment_sender
            .send(assignment)
            .await
            .map_err(|_| SchedulerError::DispatchQueueClosed)
    }

    async fn bump_session_id(&self, new_session_id: SessionId) -> Result<(), SchedulerError> {
        let mut session_id_guard = self.inner.session_id.write().await;
        if new_session_id <= *session_id_guard {
            return Err(SchedulerError::InvalidSessionId(new_session_id));
        }
        *session_id_guard = new_session_id;
        drop(session_id_guard);
        while self.inner.assignment_receiver.try_recv().is_ok() {}
        Ok(())
    }

    fn size(&self) -> usize {
        self.inner.assignment_sender.len()
    }
}

/// A cloneable reader handle for the dispatching queue.
#[derive(Clone)]
pub struct DispatchQueueReader {
    inner: Arc<DispatchQueueReaderInner>,
}

#[async_trait]
impl DispatchQueueSource for DispatchQueueReader {
    async fn dequeue(
        &self,
        wait_time: Duration,
    ) -> Result<Option<(SessionId, TaskAssignment)>, SchedulerError> {
        {
            let session_id_guard = self.inner.session_id.read().await;
            if let Ok(assignment) = self.inner.assignment_receiver.try_recv() {
                return Ok(Some((*session_id_guard, assignment)));
            }
        }
        if wait_time.is_zero() {
            return Ok(None);
        }
        match tokio::time::timeout(wait_time, self.inner.assignment_receiver.recv()).await {
            Ok(Ok(assignment)) => {
                let session_id_guard = self.inner.session_id.read().await;
                Ok(Some((*session_id_guard, assignment)))
            }
            Ok(Err(_)) => Err(SchedulerError::DispatchQueueClosed),
            Err(_) => Ok(None),
        }
    }
}

/// Dispatch queue factory.
///
/// # Returns
///
/// A tuple containing the writer and reader handles.
#[must_use]
pub fn create_dispatch_queue(
    capacity: usize,
    init_session_id: SessionId,
) -> (DispatchQueueWriter, DispatchQueueReader) {
    let (assignment_sender, assignment_receiver) = async_channel::bounded(capacity);
    let session_id = Arc::new(RwLock::new(init_session_id));
    let writer_inner = Arc::new(DispatchQueueWriterInner {
        session_id: session_id.clone(),
        assignment_sender,
        assignment_receiver: assignment_receiver.clone(),
    });
    let reader_inner = Arc::new(DispatchQueueReaderInner {
        session_id,
        assignment_receiver,
    });
    (
        DispatchQueueWriter {
            inner: writer_inner,
        },
        DispatchQueueReader {
            inner: reader_inner,
        },
    )
}

struct DispatchQueueWriterInner {
    session_id: Arc<RwLock<SessionId>>,
    assignment_sender: async_channel::Sender<TaskAssignment>,
    assignment_receiver: async_channel::Receiver<TaskAssignment>,
}

struct DispatchQueueReaderInner {
    session_id: Arc<RwLock<SessionId>>,
    assignment_receiver: async_channel::Receiver<TaskAssignment>,
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use anyhow::Result;
    use spider_core::types::id::{JobId, ResourceGroupId, SessionId};
    use spider_storage::cache::TaskId;

    use super::*;

    fn next_task_id() -> TaskId {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        TaskId::Index(COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    fn make_assignment() -> TaskAssignment {
        TaskAssignment {
            resource_group_id: ResourceGroupId::new(),
            job_id: JobId::new(),
            task_id: next_task_id(),
        }
    }

    #[tokio::test]
    async fn post_bump_items_pair_with_new_session() -> Result<()> {
        let (writer, reader) = create_dispatch_queue(8, 1);
        writer.bump_session_id(2).await?;
        let assignment = make_assignment();
        writer.enqueue(assignment).await?;

        let (session, received) = reader
            .dequeue(Duration::from_secs(1))
            .await?
            .expect("expected an assignment");

        assert_eq!(session, 2);
        assert_eq!(received, assignment);
        Ok(())
    }

    #[tokio::test]
    async fn pre_bump_items_are_not_delivered() -> Result<()> {
        let (writer, reader) = create_dispatch_queue(8, 1);
        writer.enqueue(make_assignment()).await?;
        writer.bump_session_id(2).await?;

        let result = reader.dequeue(Duration::from_millis(100)).await?;

        assert_eq!(result, None);
        Ok(())
    }

    #[tokio::test]
    async fn dequeue_load_balances_across_consumers() -> Result<()> {
        const ASSIGNMENT_COUNT: usize = 100;
        const READER_COUNT: usize = 4;
        let (writer, reader) = create_dispatch_queue(32, 1);
        let mut reader_handles = Vec::new();
        for _ in 0..READER_COUNT {
            let reader = reader.clone();
            reader_handles.push(tokio::spawn(async move {
                let mut count = 0;
                loop {
                    match reader.dequeue(Duration::from_millis(500)).await {
                        Ok(Some(_)) => count += 1,
                        Ok(None) => {}
                        Err(_) => break,
                    }
                }
                count
            }));
        }
        drop(reader);

        for _ in 0..ASSIGNMENT_COUNT {
            writer.enqueue(make_assignment()).await?;
        }
        drop(writer);

        let mut total = 0;
        for reader_handle in reader_handles {
            total += reader_handle.await?;
        }

        assert_eq!(total, ASSIGNMENT_COUNT);
        Ok(())
    }

    #[tokio::test]
    async fn bump_same_session_id_returns_invalid() -> Result<()> {
        const SESSION_ID: SessionId = 5;
        let (writer, _reader) = create_dispatch_queue(8, SESSION_ID);

        let result = writer.bump_session_id(SESSION_ID).await;

        assert!(matches!(result, Err(SchedulerError::InvalidSessionId(5))));
        Ok(())
    }
}
