use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Json,
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use spider_core::{
    job::JobState,
    types::id::{JobId, SessionId},
};
use spider_scheduler::{
    DispatchQueueSource,
    SchedulerCore,
    SchedulerStorageClient,
    StorageClientError,
    TaskAssignment,
    core_impl::RoundRobinConfig,
    dispatch_queue::{DispatchQueueReader, DispatchQueueWriter, create_dispatch_queue},
};
use spider_storage::cache::TaskId;
use spider_storage_api_bench::{
    api::{
        GetSessionRequest,
        JobIdRequest,
        PollReadyTasksRequest,
        ReadyTaskEntryDto,
        TaskIdDto,
        TerminationTaskEntryDto,
    },
    client::StorageApiClient,
    metrics::{RequestCategory, RequestLatencySample, summarize_requests},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    AgentArgs,
    BenchmarkReport,
    ScheduledTaskDto,
    SchedulerPollReadyTaskRequest,
    SchedulerReadyTaskResponse,
    SchedulerReadyTasksClient,
    distributed::{AgentRole, AgentRunAccepted, AgentRunRequest, AgentRunState, AgentRunStatus},
    run_submitter_workload,
    run_worker_workload,
};

#[derive(Clone)]
struct AgentState {
    agent_id: String,
    role: AgentRole,
    config_path: std::path::PathBuf,
    runs: Arc<Mutex<HashMap<String, AgentRunRecord>>>,
}

#[derive(Clone)]
struct AgentRunRecord {
    status: AgentRunState,
    report: Option<BenchmarkReport>,
    error: Option<String>,
    stop_requested: Option<Arc<AtomicBool>>,
    scheduler: Option<Arc<SchedulerQueue>>,
}

struct SchedulerQueue {
    writer: DispatchQueueWriter,
    reader: DispatchQueueReader,
    worker_poll_latency: Mutex<Vec<RequestLatencySample>>,
    storage_poll_latency: Arc<Mutex<Vec<RequestLatencySample>>>,
}

#[derive(Serialize)]
struct HealthResponse {
    agent_id: String,
    status: &'static str,
}

pub async fn run_agent(args: AgentArgs) -> anyhow::Result<()> {
    let state = AgentState {
        agent_id: args.agent_id,
        role: args.role,
        config_path: args.config,
        runs: Arc::new(Mutex::new(HashMap::new())),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/runs", post(start_run))
        .route("/runs/:run_id", get(get_run))
        .route("/runs/:run_id/stop", post(stop_run))
        .route(
            "/scheduler/runs/:run_id/ready-tasks",
            post(poll_scheduler_ready_tasks),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<AgentState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        agent_id: state.agent_id,
        status: "ok",
    })
}

async fn start_run(
    State(state): State<AgentState>,
    Json(request): Json<AgentRunRequest>,
) -> Result<Json<AgentRunAccepted>, AgentError> {
    let mut runs = state.runs.lock().await;
    if runs.values().any(|run| {
        matches!(
            run.status,
            AgentRunState::Accepted | AgentRunState::Running | AgentRunState::Stopping
        )
    }) {
        return Err(AgentError::conflict(
            "agent already has a running benchmark",
        ));
    }
    if run_id_is_active(&runs, &request.run_id) {
        return Err(AgentError::conflict(format!(
            "run `{}` already exists",
            request.run_id
        )));
    }
    if request.role != state.role {
        return Err(AgentError::bad_request(format!(
            "agent `{}` is {:?}, request is {:?}",
            state.agent_id, state.role, request.role
        )));
    }
    let stop_requested = if matches!(request.role, AgentRole::Worker | AgentRole::Scheduler) {
        Some(Arc::new(AtomicBool::new(false)))
    } else {
        None
    };
    let scheduler = create_scheduler_queue_for_run(&state, request.role)?;
    runs.insert(
        request.run_id.clone(),
        AgentRunRecord {
            status: AgentRunState::Running,
            report: None,
            error: None,
            stop_requested,
            scheduler,
        },
    );
    drop(runs);

    let run_id = request.run_id.clone();
    let task_run_id = run_id.clone();
    let task_state = state.clone();
    tokio::spawn(async move {
        tracing::info!(
            agent_id = %task_state.agent_id,
            run_id = %task_run_id,
            role = ?request.role,
            "agent_run_start"
        );
        let result = execute_run(&task_state, request).await;
        let mut runs = task_state.runs.lock().await;
        if let Some(record) = runs.get_mut(&task_run_id) {
            match result {
                Ok(report) => {
                    record.status = AgentRunState::Succeeded;
                    record.report = Some(report);
                    tracing::info!(
                        agent_id = %task_state.agent_id,
                        run_id = %task_run_id,
                        "agent_run_complete"
                    );
                }
                Err(err) => {
                    record.status = AgentRunState::Failed;
                    let error = err.to_string();
                    record.error = Some(error.clone());
                    tracing::error!(
                        agent_id = %task_state.agent_id,
                        run_id = %task_run_id,
                        error = %error,
                        "agent_run_failed"
                    );
                }
            }
        }
    });

    Ok(Json(AgentRunAccepted {
        run_id,
        status: AgentRunState::Accepted,
    }))
}

fn create_scheduler_queue_for_run(
    state: &AgentState,
    role: AgentRole,
) -> Result<Option<Arc<SchedulerQueue>>, AgentError> {
    if role != AgentRole::Scheduler {
        return Ok(None);
    }
    let config = spider_storage_api_bench::config::BenchConfig::load(&state.config_path)
        .map_err(|err| AgentError::bad_request(format!("scheduler config load failed: {err}")))?;
    config
        .benchmark
        .validate()
        .map_err(|err| AgentError::bad_request(format!("scheduler config invalid: {err}")))?;
    let (writer, reader) = create_dispatch_queue(
        config.benchmark.scheduler_dispatch_queue_capacity,
        SessionId::default(),
    );
    Ok(Some(Arc::new(SchedulerQueue {
        writer,
        reader,
        worker_poll_latency: Mutex::new(Vec::new()),
        storage_poll_latency: Arc::new(Mutex::new(Vec::new())),
    })))
}

fn run_id_is_active(runs: &HashMap<String, AgentRunRecord>, run_id: &str) -> bool {
    runs.get(run_id).is_some_and(|run| {
        matches!(
            run.status,
            AgentRunState::Accepted | AgentRunState::Running | AgentRunState::Stopping
        )
    })
}

async fn execute_run(
    state: &AgentState,
    request: AgentRunRequest,
) -> anyhow::Result<BenchmarkReport> {
    let mut config = spider_storage_api_bench::config::BenchConfig::load(&state.config_path)?;
    config.benchmark.job_count = request.job_count;
    config.benchmark.flat_percent = request.flat_percent;
    config.benchmark.validate()?;
    match request.role {
        AgentRole::Scheduler => run_scheduler_report(state, request, config).await,
        AgentRole::Submitter => run_submitter_report(state, request, config).await,
        AgentRole::Worker => run_worker_report(state, request, config).await,
    }
}

async fn run_scheduler_report(
    state: &AgentState,
    request: AgentRunRequest,
    config: spider_storage_api_bench::config::BenchConfig,
) -> anyhow::Result<BenchmarkReport> {
    let (stop_requested, scheduler) = {
        let runs = state.runs.lock().await;
        let record = runs
            .get(&request.run_id)
            .ok_or_else(|| anyhow::anyhow!("scheduler run missing record"))?;
        let stop_requested = record
            .stop_requested
            .clone()
            .ok_or_else(|| anyhow::anyhow!("scheduler run missing stop flag"))?;
        let scheduler = record
            .scheduler
            .clone()
            .ok_or_else(|| anyhow::anyhow!("scheduler run missing queue"))?;
        drop(runs);
        (stop_requested, scheduler)
    };
    match request.protocol {
        spider_storage_api_bench::server::ServerProtocol::Rest => {
            let client =
                spider_storage_api_bench::rest::RestStorageApiClient::new(&request.target)?;
            run_scheduler_loop(client, &config, scheduler.clone(), stop_requested).await?;
        }
        spider_storage_api_bench::server::ServerProtocol::Grpc => {
            run_scheduler_loop(
                crate::connect_grpc_clients(&request.target, 1)
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing scheduler gRPC client"))?,
                &config,
                scheduler.clone(),
                stop_requested,
            )
            .await?;
        }
    }
    let request_latency = scheduler.storage_poll_latency.lock().await.clone();
    Ok(BenchmarkReport {
        setup: crate::BenchmarkSetup::new(
            request.protocol,
            request.target,
            request.workload,
            &config,
        ),
        job_latency: spider_storage_api_bench::metrics::summarize(&[]),
        request_latency: spider_storage_api_bench::metrics::summarize_requests(&request_latency),
        server_metrics: crate::empty_server_metrics_report(),
        scheduler_metrics: summarize_requests(&scheduler.worker_poll_latency.lock().await),
        job_latency_samples: Vec::new(),
        request_latency_samples: request_latency,
        worker_activity_samples: Vec::new(),
        distributed: None,
    })
}

async fn run_scheduler_loop<ClientType: StorageApiClient>(
    client: ClientType,
    config: &spider_storage_api_bench::config::BenchConfig,
    scheduler: Arc<SchedulerQueue>,
    stop_requested: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let cancellation_token = CancellationToken::new();
    let stop_token = cancellation_token.clone();
    let stop_handle = tokio::spawn(async move {
        while !stop_requested.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        stop_token.cancel();
    });
    let storage_client = BenchSchedulerStorageClient {
        client,
        request_latency: scheduler.storage_poll_latency.clone(),
    };
    let core = RoundRobinConfig::new(
        config.benchmark.scheduler_active_job_pool_capacity,
        config.benchmark.scheduler_dispatch_queue_capacity,
        config.benchmark.scheduler_ready_task_capacity,
        config.benchmark.scheduler_commit_ready_task_capacity,
        config.benchmark.scheduler_cleanup_ready_task_capacity,
        config.benchmark.scheduler_storage_poll_wait_ms,
        config.benchmark.scheduler_tick_interval_ms,
    );
    let result = core
        .run(storage_client, scheduler.writer.clone(), cancellation_token)
        .await;
    stop_handle.abort();
    match result {
        Ok(()) => Ok(()),
        Err(err) => Err(anyhow::anyhow!("scheduler core failed: {err}")),
    }
}

#[derive(Clone)]
struct BenchSchedulerStorageClient<ClientType: StorageApiClient> {
    client: ClientType,
    request_latency: Arc<Mutex<Vec<RequestLatencySample>>>,
}

#[async_trait::async_trait]
impl<ClientType: StorageApiClient> SchedulerStorageClient
    for BenchSchedulerStorageClient<ClientType>
{
    async fn poll_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<spider_scheduler::InboundEntry>), StorageClientError> {
        let session_id = self.get_session_id().await?;
        let start_time = Instant::now();
        let result = self
            .client
            .poll_ready_tasks(PollReadyTasksRequest {
                max_tasks: max_items,
                wait_ms: duration_millis_u64(wait),
            })
            .await;
        let latency = start_time.elapsed();
        self.record_storage_poll("scheduler_poll_ready_tasks", result.is_ok(), latency)
            .await;
        let response = result.map_err(|err| storage_api_error(&err))?;
        let entries = response
            .tasks
            .iter()
            .map(ready_task_to_inbound_entry)
            .collect::<Result<_, _>>()?;
        Ok((session_id, entries))
    }

    async fn poll_commit_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<spider_scheduler::InboundEntry>), StorageClientError> {
        let session_id = self.get_session_id().await?;
        let start_time = Instant::now();
        let result = self
            .client
            .poll_commit_ready_tasks(PollReadyTasksRequest {
                max_tasks: max_items,
                wait_ms: duration_millis_u64(wait),
            })
            .await;
        let latency = start_time.elapsed();
        self.record_storage_poll("scheduler_poll_commit_ready_tasks", result.is_ok(), latency)
            .await;
        let response = result.map_err(|err| storage_api_error(&err))?;
        let entries = response
            .tasks
            .iter()
            .map(|task| termination_task_to_inbound_entry(task, TaskId::Commit))
            .collect::<Result<_, _>>()?;
        Ok((session_id, entries))
    }

    async fn poll_cleanup_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<spider_scheduler::InboundEntry>), StorageClientError> {
        let session_id = self.get_session_id().await?;
        let start_time = Instant::now();
        let result = self
            .client
            .poll_cleanup_ready_tasks(PollReadyTasksRequest {
                max_tasks: max_items,
                wait_ms: duration_millis_u64(wait),
            })
            .await;
        let latency = start_time.elapsed();
        self.record_storage_poll(
            "scheduler_poll_cleanup_ready_tasks",
            result.is_ok(),
            latency,
        )
        .await;
        let response = result.map_err(|err| storage_api_error(&err))?;
        let entries = response
            .tasks
            .iter()
            .map(|task| termination_task_to_inbound_entry(task, TaskId::Cleanup))
            .collect::<Result<_, _>>()?;
        Ok((session_id, entries))
    }

    async fn job_state(&self, job_id: JobId) -> Result<JobState, StorageClientError> {
        let response = self
            .client
            .get_job_state(JobIdRequest {
                job_id: job_id.to_string(),
            })
            .await
            .map_err(|err| storage_api_error(&err))?;
        parse_job_state(&response.state)
    }
}

impl<ClientType: StorageApiClient> BenchSchedulerStorageClient<ClientType> {
    async fn get_session_id(&self) -> Result<SessionId, StorageClientError> {
        self.client
            .get_session(GetSessionRequest {})
            .await
            .map(|response| response.session_id)
            .map_err(|err| storage_api_error(&err))
    }

    async fn record_storage_poll(&self, operation: &'static str, success: bool, latency: Duration) {
        let sample = if success {
            RequestLatencySample::success(operation, RequestCategory::Blocking, latency)
        } else {
            RequestLatencySample::failure(operation, RequestCategory::Blocking, latency)
        };
        self.request_latency.lock().await.push(sample);
    }
}

fn ready_task_to_inbound_entry(
    task: &ReadyTaskEntryDto,
) -> Result<spider_scheduler::InboundEntry, StorageClientError> {
    Ok(spider_scheduler::InboundEntry {
        resource_group_id: parse_scheduler_id(&task.resource_group_id)?,
        job_id: parse_scheduler_id(&task.job_id)?,
        task_id: TaskId::Index(task.task_index),
    })
}

fn termination_task_to_inbound_entry(
    task: &TerminationTaskEntryDto,
    task_id: TaskId,
) -> Result<spider_scheduler::InboundEntry, StorageClientError> {
    Ok(spider_scheduler::InboundEntry {
        resource_group_id: parse_scheduler_id(&task.resource_group_id)?,
        job_id: parse_scheduler_id(&task.job_id)?,
        task_id,
    })
}

fn parse_scheduler_id<IdType>(value: &str) -> Result<IdType, StorageClientError>
where
    IdType: FromStr,
    IdType::Err: std::fmt::Display, {
    IdType::from_str(value).map_err(|err| {
        StorageClientError::Internal(format!("invalid scheduler id `{value}`: {err}"))
    })
}

fn parse_job_state(value: &str) -> Result<JobState, StorageClientError> {
    match value {
        "Ready" => Ok(JobState::Ready),
        "Running" => Ok(JobState::Running),
        "CommitReady" => Ok(JobState::CommitReady),
        "CleanupReady" => Ok(JobState::CleanupReady),
        "Succeeded" => Ok(JobState::Succeeded),
        "Failed" => Ok(JobState::Failed),
        "Cancelled" => Ok(JobState::Cancelled),
        _ => Err(StorageClientError::Internal(format!(
            "invalid job state `{value}`"
        ))),
    }
}

fn storage_api_error(error: &spider_storage_api_bench::api::ApiError) -> StorageClientError {
    StorageClientError::Internal(error.to_string())
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

async fn run_submitter_report(
    _state: &AgentState,
    request: AgentRunRequest,
    config: spider_storage_api_bench::config::BenchConfig,
) -> anyhow::Result<BenchmarkReport> {
    let resource_group_id = request
        .resource_group_id
        .ok_or_else(|| anyhow::anyhow!("submitter run requires resource_group_id"))?;
    let measurements = match request.protocol {
        spider_storage_api_bench::server::ServerProtocol::Rest => {
            let client =
                spider_storage_api_bench::rest::RestStorageApiClient::new(&request.target)?;
            run_submitter_workload(
                vec![client; config.benchmark.client_count],
                request.workload,
                resource_group_id,
                &config,
            )
            .await?
        }
        spider_storage_api_bench::server::ServerProtocol::Grpc => {
            let clients =
                crate::connect_grpc_clients(&request.target, config.benchmark.client_count).await?;
            run_submitter_workload(clients, request.workload, resource_group_id, &config).await?
        }
    };
    Ok(BenchmarkReport {
        setup: crate::BenchmarkSetup::new(
            request.protocol,
            request.target,
            request.workload,
            &config,
        ),
        job_latency: spider_storage_api_bench::metrics::summarize(&measurements.job_latency),
        request_latency: spider_storage_api_bench::metrics::summarize_requests(
            &measurements.request_latency,
        ),
        server_metrics: crate::empty_server_metrics_report(),
        scheduler_metrics: Vec::new(),
        job_latency_samples: measurements.job_latency,
        request_latency_samples: measurements.request_latency,
        worker_activity_samples: measurements.worker_activity,
        distributed: None,
    })
}

async fn run_worker_report(
    state: &AgentState,
    request: AgentRunRequest,
    config: spider_storage_api_bench::config::BenchConfig,
) -> anyhow::Result<BenchmarkReport> {
    let session_id = request
        .session_id
        .ok_or_else(|| anyhow::anyhow!("worker run requires session_id"))?;
    let stop_requested = {
        let runs = state.runs.lock().await;
        runs.get(&request.run_id)
            .and_then(|run| run.stop_requested.clone())
            .ok_or_else(|| anyhow::anyhow!("worker run missing stop flag"))?
    };
    let scheduler = match (&request.scheduler_url, &request.scheduler_run_id) {
        (Some(url), Some(run_id)) => Some(SchedulerReadyTasksClient::new(url, run_id)),
        (None, None) => None,
        _ => anyhow::bail!("worker scheduler_url and scheduler_run_id must be set together"),
    };
    let measurements = match request.protocol {
        spider_storage_api_bench::server::ServerProtocol::Rest => {
            let client =
                spider_storage_api_bench::rest::RestStorageApiClient::new(&request.target)?;
            run_worker_workload(
                vec![client; config.benchmark.worker_count],
                state.agent_id.clone(),
                session_id,
                &config,
                stop_requested,
                scheduler,
            )
            .await?
        }
        spider_storage_api_bench::server::ServerProtocol::Grpc => {
            let clients =
                crate::connect_grpc_clients(&request.target, config.benchmark.worker_count).await?;
            run_worker_workload(
                clients,
                state.agent_id.clone(),
                session_id,
                &config,
                stop_requested,
                scheduler,
            )
            .await?
        }
    };
    Ok(BenchmarkReport {
        setup: crate::BenchmarkSetup::new(
            request.protocol,
            request.target,
            request.workload,
            &config,
        ),
        job_latency: spider_storage_api_bench::metrics::summarize(&measurements.job_latency),
        request_latency: spider_storage_api_bench::metrics::summarize_requests(
            &measurements.request_latency,
        ),
        server_metrics: crate::empty_server_metrics_report(),
        scheduler_metrics: Vec::new(),
        job_latency_samples: measurements.job_latency,
        request_latency_samples: measurements.request_latency,
        worker_activity_samples: measurements.worker_activity,
        distributed: None,
    })
}

async fn get_run(
    State(state): State<AgentState>,
    Path(run_id): Path<String>,
) -> Result<Json<AgentRunStatus>, AgentError> {
    let record = {
        let runs = state.runs.lock().await;
        runs.get(&run_id)
            .cloned()
            .ok_or_else(|| AgentError::not_found(format!("run `{run_id}` not found")))?
    };
    Ok(Json(AgentRunStatus {
        run_id,
        agent_id: state.agent_id,
        status: record.status,
        report: record.report.clone(),
        error: record.error,
    }))
}

async fn poll_scheduler_ready_tasks(
    State(state): State<AgentState>,
    Path(run_id): Path<String>,
    Json(request): Json<SchedulerPollReadyTaskRequest>,
) -> Result<Json<SchedulerReadyTaskResponse>, AgentError> {
    let scheduler = {
        let runs = state.runs.lock().await;
        let record = runs
            .get(&run_id)
            .ok_or_else(|| AgentError::not_found(format!("run `{run_id}` not found")))?;
        if !matches!(
            record.status,
            AgentRunState::Running | AgentRunState::Stopping
        ) {
            return Err(AgentError::bad_request(format!(
                "scheduler run `{run_id}` is {:?}",
                record.status
            )));
        }
        let scheduler = record
            .scheduler
            .clone()
            .ok_or_else(|| AgentError::bad_request(format!("run `{run_id}` is not a scheduler")))?;
        drop(runs);
        scheduler
    };
    let start_time = Instant::now();
    let task = scheduler_poll_ready_task(&scheduler, Duration::from_millis(request.wait_ms))
        .await
        .map_err(|err| AgentError::internal(format!("scheduler dispatch failed: {err}")))?;
    let latency = start_time.elapsed();
    let mut worker_poll_latency = scheduler.worker_poll_latency.lock().await;
    worker_poll_latency.push(RequestLatencySample::success(
        "worker_poll_ready_tasks",
        RequestCategory::Blocking,
        latency,
    ));
    if task.is_some() {
        worker_poll_latency.push(RequestLatencySample::success(
            "worker_poll_ready_tasks_returned",
            RequestCategory::Blocking,
            latency,
        ));
    }
    drop(worker_poll_latency);
    Ok(Json(SchedulerReadyTaskResponse { task }))
}

async fn scheduler_poll_ready_task(
    scheduler: &SchedulerQueue,
    wait: Duration,
) -> Result<Option<ScheduledTaskDto>, spider_scheduler::SchedulerError> {
    scheduler
        .reader
        .dequeue(wait)
        .await
        .map(|assignment| assignment.map(|(_session_id, task)| task_assignment_to_dto(task)))
}

fn task_assignment_to_dto(task: TaskAssignment) -> ScheduledTaskDto {
    ScheduledTaskDto {
        resource_group: task.resource_group_id.to_string(),
        job: task.job_id.to_string(),
        task: match task.task_id {
            TaskId::Index(task_index) => TaskIdDto::Index { task_index },
            TaskId::Commit => TaskIdDto::Commit,
            TaskId::Cleanup => TaskIdDto::Cleanup,
        },
    }
}

async fn stop_run(
    State(state): State<AgentState>,
    Path(run_id): Path<String>,
) -> Result<Json<AgentRunStatus>, AgentError> {
    let record = {
        let mut runs = state.runs.lock().await;
        let record = runs
            .get_mut(&run_id)
            .ok_or_else(|| AgentError::not_found(format!("run `{run_id}` not found")))?;
        if let Some(stop_requested) = &record.stop_requested {
            stop_requested.store(true, Ordering::Relaxed);
        }
        if matches!(record.status, AgentRunState::Running) {
            record.status = AgentRunState::Stopping;
        }
        let record = record.clone();
        drop(runs);
        record
    };
    Ok(Json(AgentRunStatus {
        run_id,
        agent_id: state.agent_id,
        status: record.status,
        report: record.report,
        error: record.error,
    }))
}

struct AgentError {
    status: StatusCode,
    message: String,
}

impl AgentError {
    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for AgentError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{AgentRunRecord, run_id_is_active};
    use crate::distributed::AgentRunState;

    #[test]
    fn completed_run_id_can_be_reused() {
        let mut runs = HashMap::new();
        runs.insert(
            "grpc_flat_agent".to_owned(),
            AgentRunRecord {
                status: AgentRunState::Succeeded,
                report: None,
                error: None,
                stop_requested: None,
                scheduler: None,
            },
        );

        assert!(!run_id_is_active(&runs, "grpc_flat_agent"));
    }

    #[test]
    fn running_run_id_cannot_be_reused() {
        let mut runs = HashMap::new();
        runs.insert(
            "grpc_flat_agent".to_owned(),
            AgentRunRecord {
                status: AgentRunState::Running,
                report: None,
                error: None,
                stop_requested: None,
                scheduler: None,
            },
        );

        assert!(run_id_is_active(&runs, "grpc_flat_agent"));
    }
}
