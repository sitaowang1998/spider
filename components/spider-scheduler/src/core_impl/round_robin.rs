//! Round-robin scheduler.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use async_trait::async_trait;
use serde::Deserialize;
use spider_core::types::id::{JobId, ResourceGroupId, SessionId};
use spider_storage::cache::TaskId;
use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::{
    DispatchQueueSink,
    InboundEntry,
    SchedulerCore,
    SchedulerError,
    SchedulerStorageClient,
    StorageClientError,
    TaskAssignment,
};

/// Configuration for the round-robin scheduler core.
#[derive(Deserialize)]
pub struct RoundRobinConfig<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> {
    pub active_job_pool_capacity: usize,
    pub dispatch_queue_capacity: usize,
    pub ready_task_capacity: usize,
    pub commit_ready_task_capacity: usize,
    pub cleanup_ready_task_capacity: usize,
    pub storage_polling_wait_time_ms: u64,
    pub tick_interval_ms: u64,
    #[serde(skip)]
    _marker: std::marker::PhantomData<(SchedulerStorageClientType, DispatchQueueSinkType)>,
}

impl<StorageClientType: SchedulerStorageClient + 'static, SinkType: DispatchQueueSink>
    RoundRobinConfig<StorageClientType, SinkType>
{
    /// Creates a new round-robin scheduler config.
    #[must_use]
    pub const fn new(
        active_job_pool_capacity: usize,
        dispatch_queue_capacity: usize,
        ready_task_capacity: usize,
        commit_ready_task_capacity: usize,
        cleanup_ready_task_capacity: usize,
        storage_polling_wait_time_ms: u64,
        tick_interval_ms: u64,
    ) -> Self {
        Self {
            active_job_pool_capacity,
            dispatch_queue_capacity,
            ready_task_capacity,
            commit_ready_task_capacity,
            cleanup_ready_task_capacity,
            storage_polling_wait_time_ms,
            tick_interval_ms,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<StorageClientType: SchedulerStorageClient + 'static, SinkType: DispatchQueueSink> SchedulerCore
    for RoundRobinConfig<StorageClientType, SinkType>
{
    type Sink = SinkType;
    type StorageClient = StorageClientType;

    async fn run(
        self,
        storage_client: Self::StorageClient,
        sink: Self::Sink,
        cancellation_token: CancellationToken,
    ) -> Result<(), SchedulerError> {
        RoundRobin::new(0, storage_client, sink, cancellation_token, self)
            .run()
            .await
    }
}

struct JobEntry {
    job_id: JobId,
    resource_group_id: ResourceGroupId,
    task_ids: VecDeque<TaskId>,
}

impl JobEntry {
    fn new(job_id: JobId, resource_group_id: ResourceGroupId, init_task_id: TaskId) -> Self {
        Self {
            job_id,
            resource_group_id,
            task_ids: VecDeque::from([init_task_id]),
        }
    }

    fn enqueue(&mut self, task_id: TaskId) {
        self.task_ids.push_back(task_id);
    }

    fn dequeue(&mut self) -> Option<TaskId> {
        self.task_ids.pop_front()
    }
}

#[derive(Clone)]
enum ActiveJobQueueEntry {
    Ready(JobId),
    CommitReady,
    CleanupReady,
}

struct RoundRobin<StorageClientType: SchedulerStorageClient + 'static, SinkType: DispatchQueueSink>
{
    sink: SinkType,
    cancellation_token: CancellationToken,
    config: RoundRobinConfig<StorageClientType, SinkType>,
    storage_session_id: SessionId,
    ready_set: HashSet<(JobId, TaskId)>,
    active_jobs: HashMap<JobId, JobEntry>,
    active_job_queue: Vec<ActiveJobQueueEntry>,
    active_job_queue_cursor: usize,
    pending_jobs: HashMap<JobId, JobEntry>,
    pending_job_queue: VecDeque<JobId>,
    commit_ready_queue: VecDeque<(JobId, ResourceGroupId)>,
    cleanup_ready_queue: VecDeque<(JobId, ResourceGroupId)>,
    terminal_ready_jobs: HashSet<JobId>,
    inbound_queue_reader: AsyncInboundQueueReader<StorageClientType>,
}

impl<StorageClientType: SchedulerStorageClient + 'static, SinkType: DispatchQueueSink>
    RoundRobin<StorageClientType, SinkType>
{
    fn new(
        storage_session_id: SessionId,
        storage_client: StorageClientType,
        sink: SinkType,
        cancellation_token: CancellationToken,
        config: RoundRobinConfig<StorageClientType, SinkType>,
    ) -> Self {
        let ready_set = HashSet::with_capacity(config.ready_task_capacity);
        let active_jobs = HashMap::with_capacity(config.active_job_pool_capacity);
        let active_job_queue = Self::new_active_job_queue(config.active_job_pool_capacity);
        let pending_jobs = HashMap::with_capacity(config.active_job_pool_capacity);
        let pending_job_queue = VecDeque::with_capacity(config.active_job_pool_capacity);
        let commit_ready_queue = VecDeque::with_capacity(config.commit_ready_task_capacity);
        let cleanup_ready_queue = VecDeque::with_capacity(config.cleanup_ready_task_capacity);
        let terminal_ready_jobs = HashSet::with_capacity(
            config.commit_ready_task_capacity + config.cleanup_ready_task_capacity,
        );
        let inbound_queue_reader = AsyncInboundQueueReader::new(storage_client);
        Self {
            sink,
            cancellation_token,
            config,
            storage_session_id,
            ready_set,
            active_jobs,
            active_job_queue,
            active_job_queue_cursor: 0,
            pending_jobs,
            pending_job_queue,
            commit_ready_queue,
            cleanup_ready_queue,
            terminal_ready_jobs,
            inbound_queue_reader,
        }
    }

    fn new_active_job_queue(active_job_pool_capacity: usize) -> Vec<ActiveJobQueueEntry> {
        let mut active_job_queue = Vec::with_capacity(active_job_pool_capacity + 2);
        active_job_queue.push(ActiveJobQueueEntry::CommitReady);
        active_job_queue.push(ActiveJobQueueEntry::CleanupReady);
        active_job_queue
    }

    async fn run(mut self) -> Result<(), SchedulerError> {
        let tick_interval = Duration::from_millis(self.config.tick_interval_ms);
        loop {
            let now = tokio::time::Instant::now();
            let cancellation_token = self.cancellation_token.clone();
            select! {
                () = cancellation_token.cancelled() => return Ok(()),
                result = self.tick() => result?,
            }
            let sleep_time = tick_interval.saturating_sub(now.elapsed());
            if sleep_time.is_zero() {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(sleep_time).await;
            }
        }
    }

    fn clear_all_placement(&mut self) {
        self.ready_set.clear();
        self.active_jobs.clear();
        self.pending_jobs.clear();
        self.pending_job_queue.clear();
        self.commit_ready_queue.clear();
        self.cleanup_ready_queue.clear();
        self.terminal_ready_jobs.clear();
        self.active_job_queue = Self::new_active_job_queue(self.config.active_job_pool_capacity);
        self.active_job_queue_cursor = 0;
    }

    fn remove_active_job_and_dequeue_next_pending_job(
        &mut self,
        job_id: JobId,
    ) -> Result<(), SchedulerError> {
        if let Some(index) = self.active_job_queue.iter().position(|entry| match entry {
            ActiveJobQueueEntry::Ready(id) => *id == job_id,
            _ => false,
        }) {
            self.active_job_queue.swap_remove(index);
        } else {
            return Err(SchedulerError::Internal(format!(
                "attempt to remove a non-existing active job: {job_id:?}"
            )));
        }

        if let Some(entry_to_remove) = self.active_jobs.remove(&job_id) {
            self.destroy_job_entry(entry_to_remove);
        } else {
            return Err(SchedulerError::Internal(format!(
                "attempt to destroy a non-existing active job: {job_id:?}"
            )));
        }

        if let Some(next_pending_job) = self.next_pending_job() {
            self.active_job_queue
                .push(ActiveJobQueueEntry::Ready(next_pending_job.job_id));
            self.active_jobs
                .insert(next_pending_job.job_id, next_pending_job);
        }
        Ok(())
    }

    fn next_pending_job(&mut self) -> Option<JobEntry> {
        loop {
            let job_id = self.pending_job_queue.pop_front()?;
            if let Some(pending_job) = self.pending_jobs.remove(&job_id) {
                return Some(pending_job);
            }
        }
    }

    fn destroy_job_entry(&mut self, job_entry: JobEntry) {
        for task_id in job_entry.task_ids {
            self.ready_set.remove(&(job_entry.job_id, task_id));
        }
    }

    async fn tick(&mut self) -> Result<(), SchedulerError> {
        self.poll_inbound_queue_result().await?;
        self.make_schedule_decision().await?;
        Ok(())
    }

    async fn load_inbound_queue_result(
        &mut self,
        curr_session_id: SessionId,
        storage_session_id: SessionId,
        ready_entries: Vec<InboundEntry>,
        commit_ready_entries: Vec<InboundEntry>,
        cleanup_ready_entries: Vec<InboundEntry>,
    ) -> Result<(), SchedulerError> {
        if storage_session_id < curr_session_id {
            return Err(SchedulerError::InvalidSessionId(storage_session_id));
        }
        if storage_session_id > curr_session_id {
            self.storage_session_id = storage_session_id;
            self.clear_all_placement();
            self.sink.bump_session_id(storage_session_id).await?;
        }

        self.load_terminal_entries(commit_ready_entries, TaskId::Commit)?;
        self.load_terminal_entries(cleanup_ready_entries, TaskId::Cleanup)?;
        self.load_ready_entries(ready_entries);
        Ok(())
    }

    fn load_terminal_entries(
        &mut self,
        entries: Vec<InboundEntry>,
        expected_task_id: TaskId,
    ) -> Result<(), SchedulerError> {
        for inbound_entry in entries {
            if !self
                .ready_set
                .insert((inbound_entry.job_id, inbound_entry.task_id))
            {
                continue;
            }
            self.terminal_ready_jobs.insert(inbound_entry.job_id);
            match expected_task_id {
                TaskId::Commit => self
                    .commit_ready_queue
                    .push_back((inbound_entry.job_id, inbound_entry.resource_group_id)),
                TaskId::Cleanup => self
                    .cleanup_ready_queue
                    .push_back((inbound_entry.job_id, inbound_entry.resource_group_id)),
                TaskId::Index(_) => {
                    return Err(SchedulerError::Internal(
                        "regular task passed as terminal task".to_owned(),
                    ));
                }
            }
            if self.active_jobs.contains_key(&inbound_entry.job_id) {
                self.remove_active_job_and_dequeue_next_pending_job(inbound_entry.job_id)?;
            } else if let Some(job_entry) = self.pending_jobs.remove(&inbound_entry.job_id) {
                self.destroy_job_entry(job_entry);
            }
        }
        Ok(())
    }

    fn load_ready_entries(&mut self, ready_entries: Vec<InboundEntry>) {
        for inbound_entry in ready_entries {
            if self.terminal_ready_jobs.contains(&inbound_entry.job_id)
                || !self
                    .ready_set
                    .insert((inbound_entry.job_id, inbound_entry.task_id))
            {
                continue;
            }
            if let Some(active_job) = self.active_jobs.get_mut(&inbound_entry.job_id) {
                active_job.enqueue(inbound_entry.task_id);
            } else if let Some(pending_job) = self.pending_jobs.get_mut(&inbound_entry.job_id) {
                pending_job.enqueue(inbound_entry.task_id);
            } else if self.active_jobs.len() < self.config.active_job_pool_capacity {
                self.active_jobs.insert(
                    inbound_entry.job_id,
                    JobEntry::new(
                        inbound_entry.job_id,
                        inbound_entry.resource_group_id,
                        inbound_entry.task_id,
                    ),
                );
                self.active_job_queue
                    .push(ActiveJobQueueEntry::Ready(inbound_entry.job_id));
            } else {
                self.pending_jobs.insert(
                    inbound_entry.job_id,
                    JobEntry::new(
                        inbound_entry.job_id,
                        inbound_entry.resource_group_id,
                        inbound_entry.task_id,
                    ),
                );
                self.pending_job_queue.push_back(inbound_entry.job_id);
            }
        }
    }

    async fn poll_inbound_queue_result(&mut self) -> Result<(), SchedulerError> {
        let curr_session_id = self.storage_session_id;
        let inbound_queue_result = self
            .inbound_queue_reader
            .poll_ready(curr_session_id)
            .await?;
        match inbound_queue_result {
            InboundQueueResult::Result {
                session_id: storage_session_id,
                ready_entries,
                commit_ready_entries,
                cleanup_ready_entries,
            } => {
                self.load_inbound_queue_result(
                    curr_session_id,
                    storage_session_id,
                    ready_entries,
                    commit_ready_entries,
                    cleanup_ready_entries,
                )
                .await?;
                self.spawn_inbound_queue_reader();
            }
            InboundQueueResult::ResultNotReady => {}
            InboundQueueResult::HandleNotSpawned => self.spawn_inbound_queue_reader(),
        }
        Ok(())
    }

    async fn make_schedule_decision(&mut self) -> Result<(), SchedulerError> {
        let mut dispatch_queue_slots = self
            .config
            .dispatch_queue_capacity
            .saturating_sub(self.sink.size());
        while dispatch_queue_slots > 0 && !self.ready_set.is_empty() {
            if self.active_job_queue_cursor >= self.active_job_queue.len() {
                self.active_job_queue_cursor = 0;
            }
            let active_job_queue_entry =
                match self.active_job_queue.get(self.active_job_queue_cursor) {
                    Some(entry) => entry.clone(),
                    None => {
                        return Err(SchedulerError::Internal(
                            "active job queue cursor is corrupted".to_owned(),
                        ));
                    }
                };
            self.active_job_queue_cursor += 1;
            match active_job_queue_entry {
                ActiveJobQueueEntry::CleanupReady => {
                    let Some((job_id, resource_group_id)) = self.cleanup_ready_queue.pop_front()
                    else {
                        continue;
                    };
                    self.enqueue_assignment(job_id, resource_group_id, TaskId::Cleanup)
                        .await?;
                    self.terminal_ready_jobs.remove(&job_id);
                    dispatch_queue_slots -= 1;
                }
                ActiveJobQueueEntry::CommitReady => {
                    let Some((job_id, resource_group_id)) = self.commit_ready_queue.pop_front()
                    else {
                        continue;
                    };
                    self.enqueue_assignment(job_id, resource_group_id, TaskId::Commit)
                        .await?;
                    self.terminal_ready_jobs.remove(&job_id);
                    dispatch_queue_slots -= 1;
                }
                ActiveJobQueueEntry::Ready(job_id) => {
                    let Some(job_entry) = self.active_jobs.get_mut(&job_id) else {
                        return Err(SchedulerError::Internal(format!(
                            "attempt to schedule a non-existing active job: {job_id:?}"
                        )));
                    };
                    if let Some(task_id) = job_entry.dequeue() {
                        let resource_group_id = job_entry.resource_group_id;
                        self.enqueue_assignment(job_id, resource_group_id, task_id)
                            .await?;
                        dispatch_queue_slots -= 1;
                    } else {
                        self.remove_active_job_and_dequeue_next_pending_job(job_id)?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn enqueue_assignment(
        &mut self,
        job_id: JobId,
        resource_group_id: ResourceGroupId,
        task_id: TaskId,
    ) -> Result<(), SchedulerError> {
        self.sink
            .enqueue(TaskAssignment {
                resource_group_id,
                job_id,
                task_id,
            })
            .await?;
        self.ready_set.remove(&(job_id, task_id));
        Ok(())
    }

    fn spawn_inbound_queue_reader(&mut self) {
        let num_commit_ready_tasks = self.commit_ready_queue.len();
        let num_cleanup_ready_tasks = self.cleanup_ready_queue.len();
        let max_commit_ready_to_poll = self
            .config
            .commit_ready_task_capacity
            .saturating_sub(num_commit_ready_tasks);
        let max_cleanup_ready_to_poll = self
            .config
            .cleanup_ready_task_capacity
            .saturating_sub(num_cleanup_ready_tasks);
        let regular_ready_count = self
            .ready_set
            .len()
            .saturating_sub(num_commit_ready_tasks + num_cleanup_ready_tasks);
        let max_ready_to_poll = self
            .config
            .ready_task_capacity
            .saturating_sub(regular_ready_count);
        self.inbound_queue_reader.spawn(
            Duration::from_millis(self.config.storage_polling_wait_time_ms),
            max_ready_to_poll,
            max_commit_ready_to_poll,
            max_cleanup_ready_to_poll,
        );
    }
}

enum InboundQueueResult {
    Result {
        session_id: SessionId,
        ready_entries: Vec<InboundEntry>,
        commit_ready_entries: Vec<InboundEntry>,
        cleanup_ready_entries: Vec<InboundEntry>,
    },
    ResultNotReady,
    HandleNotSpawned,
}

struct InboundQueuePollingHandle {
    ready: tokio::task::JoinHandle<Result<(SessionId, Vec<InboundEntry>), StorageClientError>>,
    commit: tokio::task::JoinHandle<Result<(SessionId, Vec<InboundEntry>), StorageClientError>>,
    cleanup: tokio::task::JoinHandle<Result<(SessionId, Vec<InboundEntry>), StorageClientError>>,
}

impl InboundQueuePollingHandle {
    async fn poll_ready(
        &mut self,
        curr_session_id: SessionId,
    ) -> Result<InboundQueueResult, SchedulerError> {
        if !self.ready.is_finished() || !self.commit.is_finished() || !self.cleanup.is_finished() {
            return Ok(InboundQueueResult::ResultNotReady);
        }

        let (ready_session_id, ready_entries) = (&mut self.ready)
            .await
            .map_err(|err| SchedulerError::Internal(err.to_string()))??;
        let (commit_session_id, commit_ready_entries) = (&mut self.commit)
            .await
            .map_err(|err| SchedulerError::Internal(err.to_string()))??;
        let (cleanup_session_id, cleanup_ready_entries) = (&mut self.cleanup)
            .await
            .map_err(|err| SchedulerError::Internal(err.to_string()))??;

        let latest_session_id = curr_session_id
            .max(ready_session_id)
            .max(commit_session_id)
            .max(cleanup_session_id);

        Ok(InboundQueueResult::Result {
            session_id: latest_session_id,
            ready_entries: Self::drop_if_stale(ready_session_id, latest_session_id, ready_entries),
            commit_ready_entries: Self::drop_if_stale(
                commit_session_id,
                latest_session_id,
                commit_ready_entries,
            ),
            cleanup_ready_entries: Self::drop_if_stale(
                cleanup_session_id,
                latest_session_id,
                cleanup_ready_entries,
            ),
        })
    }

    fn drop_if_stale(
        session_id: SessionId,
        latest_session_id: SessionId,
        entries: Vec<InboundEntry>,
    ) -> Vec<InboundEntry> {
        if session_id == latest_session_id {
            entries
        } else {
            Vec::new()
        }
    }
}

struct AsyncInboundQueueReader<StorageClientType: SchedulerStorageClient + 'static> {
    storage_client: StorageClientType,
    handle: Option<InboundQueuePollingHandle>,
}

impl<StorageClientType: SchedulerStorageClient + 'static>
    AsyncInboundQueueReader<StorageClientType>
{
    const fn new(storage_client: StorageClientType) -> Self {
        Self {
            storage_client,
            handle: None,
        }
    }

    async fn poll_ready(
        &mut self,
        curr_session_id: SessionId,
    ) -> Result<InboundQueueResult, SchedulerError> {
        match &mut self.handle {
            None => Ok(InboundQueueResult::HandleNotSpawned),
            Some(handle) => {
                let inbound_queue_result = handle.poll_ready(curr_session_id).await?;
                if !matches!(inbound_queue_result, InboundQueueResult::ResultNotReady) {
                    self.handle = None;
                }
                Ok(inbound_queue_result)
            }
        }
    }

    fn spawn(
        &mut self,
        storage_polling_wait_time: Duration,
        max_ready_entries: usize,
        max_commit_ready_entries: usize,
        max_cleanup_ready_entries: usize,
    ) {
        let ready_storage_client = self.storage_client.clone();
        let ready_handle = tokio::task::spawn(async move {
            ready_storage_client
                .poll_ready(max_ready_entries, storage_polling_wait_time)
                .await
        });

        let commit_ready_storage_client = self.storage_client.clone();
        let commit_ready_handle = tokio::task::spawn(async move {
            commit_ready_storage_client
                .poll_commit_ready(max_commit_ready_entries, storage_polling_wait_time)
                .await
        });

        let cleanup_ready_storage_client = self.storage_client.clone();
        let cleanup_ready_handle = tokio::task::spawn(async move {
            cleanup_ready_storage_client
                .poll_cleanup_ready(max_cleanup_ready_entries, storage_polling_wait_time)
                .await
        });

        self.handle = Some(InboundQueuePollingHandle {
            ready: ready_handle,
            commit: commit_ready_handle,
            cleanup: cleanup_ready_handle,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use spider_core::{
        job::JobState,
        types::id::{JobId, ResourceGroupId, SessionId},
    };
    use tokio::sync::Mutex;

    use super::*;
    use crate::{DispatchQueueSource, dispatch_queue::create_dispatch_queue};

    #[derive(Clone)]
    struct MockStorageClient {
        ready_entries: Arc<Mutex<VecDeque<InboundEntry>>>,
    }

    #[async_trait]
    impl SchedulerStorageClient for MockStorageClient {
        async fn poll_ready(
            &self,
            max_items: usize,
            _wait: Duration,
        ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
            let mut ready_entries = self.ready_entries.lock().await;
            let mut batch = Vec::new();
            for _ in 0..max_items {
                let Some(entry) = ready_entries.pop_front() else {
                    break;
                };
                batch.push(entry);
            }
            drop(ready_entries);
            Ok((1, batch))
        }

        async fn poll_commit_ready(
            &self,
            _max_items: usize,
            _wait: Duration,
        ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
            Ok((1, Vec::new()))
        }

        async fn poll_cleanup_ready(
            &self,
            _max_items: usize,
            _wait: Duration,
        ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
            Ok((1, Vec::new()))
        }

        async fn job_state(&self, _job_id: JobId) -> Result<JobState, StorageClientError> {
            Err(StorageClientError::JobNotFound(JobId::new()))
        }
    }

    fn ready_entry(
        resource_group_id: ResourceGroupId,
        job_id: JobId,
        task_index: usize,
    ) -> InboundEntry {
        InboundEntry {
            resource_group_id,
            job_id,
            task_id: TaskId::Index(task_index),
        }
    }

    #[tokio::test]
    async fn round_robin_interleaves_active_jobs() -> anyhow::Result<()> {
        let resource_group_id = ResourceGroupId::new();
        let job_a = JobId::new();
        let job_b = JobId::new();
        let ready_entries = VecDeque::from([
            ready_entry(resource_group_id, job_a, 0),
            ready_entry(resource_group_id, job_a, 1),
            ready_entry(resource_group_id, job_b, 0),
            ready_entry(resource_group_id, job_b, 1),
        ]);
        let storage_client = MockStorageClient {
            ready_entries: Arc::new(Mutex::new(ready_entries)),
        };
        let (writer, reader) = create_dispatch_queue(8, 0);
        let cancellation_token = CancellationToken::new();
        let core = RoundRobinConfig::new(8, 8, 8, 8, 8, 0, 1);
        let run_token = cancellation_token.clone();
        let handle = tokio::spawn(async move { core.run(storage_client, writer, run_token).await });

        let mut assignments = Vec::new();
        for _ in 0..4 {
            let (_session_id, assignment) = reader
                .dequeue(Duration::from_secs(1))
                .await?
                .expect("expected assignment");
            assignments.push((assignment.job_id, assignment.task_id));
        }
        cancellation_token.cancel();
        handle.await??;

        assert_eq!(
            assignments,
            vec![
                (job_a, TaskId::Index(0)),
                (job_b, TaskId::Index(0)),
                (job_a, TaskId::Index(1)),
                (job_b, TaskId::Index(1)),
            ]
        );
        Ok(())
    }
}
