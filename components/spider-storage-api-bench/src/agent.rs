use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
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
use spider_storage_api_bench::{
    api::{PollReadyTasksRequest, ReadyTaskEntryDto, ReadyTasksResponse},
    client::StorageApiClient,
    metrics::{RequestCategory, RequestLatencySample},
};
use tokio::sync::{Mutex, Notify};

use crate::{
    AgentArgs,
    BenchmarkReport,
    SchedulerReadyTasksClient,
    distributed::{AgentRole, AgentRunAccepted, AgentRunRequest, AgentRunState, AgentRunStatus},
    record_timed_request,
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
    sender: async_channel::Sender<ReadyTaskEntryDto>,
    receiver: async_channel::Receiver<ReadyTaskEntryDto>,
    refill: Notify,
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
    let scheduler = if request.role == AgentRole::Scheduler {
        let (sender, receiver) = async_channel::unbounded();
        Some(Arc::new(SchedulerQueue {
            sender,
            receiver,
            refill: Notify::new(),
        }))
    } else {
        None
    };
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
        let result = execute_run(&task_state, request).await;
        let mut runs = task_state.runs.lock().await;
        if let Some(record) = runs.get_mut(&task_run_id) {
            match result {
                Ok(report) => {
                    record.status = AgentRunState::Succeeded;
                    record.report = Some(report);
                }
                Err(err) => {
                    record.status = AgentRunState::Failed;
                    record.error = Some(err.to_string());
                }
            }
        }
    });

    Ok(Json(AgentRunAccepted {
        run_id,
        status: AgentRunState::Accepted,
    }))
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
    let request_latency = match request.protocol {
        spider_storage_api_bench::server::ServerProtocol::Rest => {
            let client =
                spider_storage_api_bench::rest::RestStorageApiClient::new(&request.target)?;
            run_scheduler_loop(client, &config, scheduler, stop_requested).await?
        }
        spider_storage_api_bench::server::ServerProtocol::Grpc => {
            let client = crate::connect_grpc_clients(&request.target, 1)
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing scheduler gRPC client"))?;
            run_scheduler_loop(client, &config, scheduler, stop_requested).await?
        }
    };
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
) -> anyhow::Result<Vec<RequestLatencySample>> {
    let mut request_latency = Vec::new();
    let refill_interval = Duration::from_millis(config.benchmark.scheduler_refill_interval_ms);
    while !stop_requested.load(Ordering::Relaxed) {
        if scheduler_queue_len(&scheduler) >= config.benchmark.scheduler_refill_threshold {
            tokio::select! {
                () = scheduler.refill.notified() => {}
                () = tokio::time::sleep(refill_interval) => {}
            }
            continue;
        }
        let (ready, _) = record_timed_request(
            &mut request_latency,
            "scheduler_poll_ready_tasks",
            RequestCategory::Blocking,
            client.poll_ready_tasks(PollReadyTasksRequest {
                max_tasks: config.benchmark.scheduler_poll_batch,
                wait_ms: config.benchmark.scheduler_poll_wait_ms,
            }),
        )
        .await?;
        if !ready.tasks.is_empty() {
            scheduler_push_tasks(&scheduler, ready.tasks).await?;
        }
    }
    Ok(request_latency)
}

fn scheduler_queue_len(scheduler: &SchedulerQueue) -> usize {
    scheduler.receiver.len()
}

async fn scheduler_push_tasks(
    scheduler: &SchedulerQueue,
    tasks: Vec<ReadyTaskEntryDto>,
) -> anyhow::Result<()> {
    for task in tasks {
        scheduler
            .sender
            .send(task)
            .await
            .map_err(|_| anyhow::anyhow!("scheduler ready-task queue is closed"))?;
    }
    Ok(())
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
    Json(request): Json<PollReadyTasksRequest>,
) -> Result<Json<ReadyTasksResponse>, AgentError> {
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
    Ok(Json(
        scheduler_poll_ready_tasks(
            &scheduler,
            request.max_tasks,
            Duration::from_millis(request.wait_ms),
        )
        .await,
    ))
}

async fn scheduler_poll_ready_tasks(
    scheduler: &SchedulerQueue,
    max_tasks: usize,
    wait: Duration,
) -> ReadyTasksResponse {
    if max_tasks == 0 {
        scheduler.refill.notify_one();
        return ReadyTasksResponse { tasks: Vec::new() };
    }

    let mut tasks = scheduler_drain_available_tasks(scheduler, max_tasks);
    if tasks.is_empty() && !wait.is_zero() {
        scheduler.refill.notify_one();
        if let Ok(Ok(task)) = tokio::time::timeout(wait, scheduler.receiver.recv()).await {
            tasks.push(task);
            tasks.extend(scheduler_drain_available_tasks(
                scheduler,
                max_tasks - tasks.len(),
            ));
        }
    }

    scheduler.refill.notify_one();
    ReadyTasksResponse { tasks }
}

fn scheduler_drain_available_tasks(
    scheduler: &SchedulerQueue,
    max_tasks: usize,
) -> Vec<ReadyTaskEntryDto> {
    let mut tasks = Vec::with_capacity(max_tasks);
    while tasks.len() < max_tasks {
        match scheduler.receiver.try_recv() {
            Ok(task) => tasks.push(task),
            Err(async_channel::TryRecvError::Empty | async_channel::TryRecvError::Closed) => break,
        }
    }
    tasks
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
