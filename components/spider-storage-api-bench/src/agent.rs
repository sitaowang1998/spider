use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    process::Command,
    sync::{
        Arc,
        Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
    api::{PollReadyTasksRequest, ReadyTaskEntryDto},
    client::StorageApiClient,
    metrics::{RequestCategory, RequestLatencySample, summarize_requests},
};
use tokio::sync::Mutex;

use crate::{
    AgentArgs,
    BenchmarkReport,
    SchedulerPollReadyTaskRequest,
    SchedulerReadyTaskResponse,
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
    worker_poll_latency: Mutex<Vec<RequestLatencySample>>,
    trace: Option<Arc<SchedulerTrace>>,
}

struct SchedulerTrace {
    path: PathBuf,
    s3_uri: Option<String>,
    next_request_id: AtomicU64,
    writer: StdMutex<BufWriter<File>>,
}

#[derive(Serialize)]
struct SchedulerTraceRecord<'a> {
    request_id: u64,
    request: &'a str,
    run_id: &'a str,
    agent_id: &'a str,
    start_epoch_us: u128,
    end_epoch_us: u128,
    latency_us: u128,
    succeeded: bool,
    task_found: Option<bool>,
    task_count: Option<usize>,
}

struct SchedulerTraceFinish {
    request_id: u64,
    start_epoch_us: u128,
    start: Instant,
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

#[allow(clippy::too_many_lines)]
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
        let trace = match &request.scheduler_trace_path {
            Some(path) => Some(Arc::new(
                SchedulerTrace::new(path, request.scheduler_trace_s3_uri.clone()).map_err(
                    |error| {
                        AgentError::internal(format!("failed to open scheduler trace: {error}"))
                    },
                )?,
            )),
            None => None,
        };
        Some(Arc::new(SchedulerQueue {
            sender,
            receiver,
            worker_poll_latency: Mutex::new(Vec::new()),
            trace,
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
            run_scheduler_loop(
                client,
                &config,
                &state.agent_id,
                &request,
                scheduler.clone(),
                stop_requested,
            )
            .await?
        }
        spider_storage_api_bench::server::ServerProtocol::Grpc => {
            let client = crate::connect_grpc_clients(&request.target, 1)
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing scheduler gRPC client"))?;
            run_scheduler_loop(
                client,
                &config,
                &state.agent_id,
                &request,
                scheduler.clone(),
                stop_requested,
            )
            .await?
        }
    };
    if let Some(trace) = &scheduler.trace {
        trace.flush()?;
        trace.upload()?;
    }
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
        request_latency_samples: Vec::new(),
        worker_activity_samples: Vec::new(),
        distributed: None,
    })
}

async fn run_scheduler_loop<ClientType: StorageApiClient>(
    client: ClientType,
    config: &spider_storage_api_bench::config::BenchConfig,
    agent_id: &str,
    request: &AgentRunRequest,
    scheduler: Arc<SchedulerQueue>,
    stop_requested: Arc<AtomicBool>,
) -> anyhow::Result<Vec<RequestLatencySample>> {
    let mut request_latency = Vec::new();
    let refill_interval = Duration::from_millis(config.benchmark.scheduler_refill_interval_ms);
    while !stop_requested.load(Ordering::Relaxed) {
        let trace_finish = scheduler.trace.as_ref().map(|trace| trace.start_request());
        let ready_result = record_timed_request(
            &mut request_latency,
            "scheduler_poll_ready_tasks",
            RequestCategory::Blocking,
            client.poll_ready_tasks(PollReadyTasksRequest {
                max_tasks: config.benchmark.scheduler_poll_batch,
                wait_ms: config.benchmark.scheduler_poll_wait_ms,
            }),
        )
        .await;
        let ready = match ready_result {
            Ok((ready, _)) => {
                if let (Some(trace), Some(trace_finish)) = (&scheduler.trace, trace_finish) {
                    trace.record(&SchedulerTraceRecord {
                        request_id: trace_finish.request_id,
                        request: "scheduler_poll_ready_tasks",
                        run_id: &request.run_id,
                        agent_id,
                        start_epoch_us: trace_finish.start_epoch_us,
                        end_epoch_us: unix_epoch_micros(),
                        latency_us: trace_finish.start.elapsed().as_micros(),
                        succeeded: true,
                        task_found: None,
                        task_count: Some(ready.tasks.len()),
                    })?;
                }
                ready
            }
            Err(error) => {
                if let (Some(trace), Some(trace_finish)) = (&scheduler.trace, trace_finish) {
                    trace.record(&SchedulerTraceRecord {
                        request_id: trace_finish.request_id,
                        request: "scheduler_poll_ready_tasks",
                        run_id: &request.run_id,
                        agent_id,
                        start_epoch_us: trace_finish.start_epoch_us,
                        end_epoch_us: unix_epoch_micros(),
                        latency_us: trace_finish.start.elapsed().as_micros(),
                        succeeded: false,
                        task_found: None,
                        task_count: None,
                    })?;
                }
                return Err(error.into());
            }
        };
        if !ready.tasks.is_empty() {
            scheduler_push_tasks(&scheduler, ready.tasks).await?;
        }
        tokio::time::sleep(refill_interval).await;
    }
    Ok(request_latency)
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
        scheduler_metrics: Vec::new(),
        job_latency_samples: measurements.job_latency,
        request_latency_samples: Vec::new(),
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
        job_latency_samples: Vec::new(),
        request_latency_samples: Vec::new(),
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
    let trace_finish = scheduler.trace.as_ref().map(|trace| trace.start_request());
    let start_time = Instant::now();
    let task = scheduler_poll_ready_task(&scheduler, Duration::from_millis(request.wait_ms)).await;
    if let (Some(trace), Some(trace_finish)) = (&scheduler.trace, trace_finish) {
        trace
            .record(&SchedulerTraceRecord {
                request_id: trace_finish.request_id,
                request: "worker_poll_ready_tasks",
                run_id: &run_id,
                agent_id: &state.agent_id,
                start_epoch_us: trace_finish.start_epoch_us,
                end_epoch_us: unix_epoch_micros(),
                latency_us: trace_finish.start.elapsed().as_micros(),
                succeeded: true,
                task_found: Some(task.is_some()),
                task_count: None,
            })
            .map_err(|error| {
                AgentError::internal(format!("failed to write scheduler trace: {error}"))
            })?;
    }
    if task.is_some() {
        scheduler
            .worker_poll_latency
            .lock()
            .await
            .push(RequestLatencySample::success(
                "worker_poll_ready_tasks",
                RequestCategory::Blocking,
                start_time.elapsed(),
            ));
    }
    Ok(Json(SchedulerReadyTaskResponse { task }))
}

async fn scheduler_poll_ready_task(
    scheduler: &SchedulerQueue,
    wait: Duration,
) -> Option<ReadyTaskEntryDto> {
    match scheduler.receiver.try_recv() {
        Ok(task) => Some(task),
        Err(async_channel::TryRecvError::Closed) => None,
        Err(async_channel::TryRecvError::Empty) => {
            if wait.is_zero() {
                None
            } else {
                match tokio::time::timeout(wait, scheduler.receiver.recv()).await {
                    Ok(Ok(task)) => Some(task),
                    Ok(Err(_)) | Err(_) => None,
                }
            }
        }
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
    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

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

impl SchedulerTrace {
    fn new(path: &str, s3_uri: Option<String>) -> anyhow::Result<Self> {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(&path)?;
        Ok(Self {
            path,
            s3_uri,
            next_request_id: AtomicU64::new(1),
            writer: StdMutex::new(BufWriter::new(file)),
        })
    }

    fn start_request(&self) -> SchedulerTraceFinish {
        SchedulerTraceFinish {
            request_id: self.next_request_id.fetch_add(1, Ordering::Relaxed),
            start_epoch_us: unix_epoch_micros(),
            start: Instant::now(),
        }
    }

    fn record(&self, record: &SchedulerTraceRecord<'_>) -> anyhow::Result<()> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| anyhow::anyhow!("scheduler trace writer lock poisoned"))?;
        serde_json::to_writer(&mut *writer, &record)?;
        writer.write_all(b"\n")?;
        drop(writer);
        Ok(())
    }

    fn flush(&self) -> anyhow::Result<()> {
        self.writer
            .lock()
            .map_err(|_| anyhow::anyhow!("scheduler trace writer lock poisoned"))?
            .flush()?;
        Ok(())
    }

    fn upload(&self) -> anyhow::Result<()> {
        let Some(s3_uri) = &self.s3_uri else {
            return Ok(());
        };
        let status = Command::new("aws")
            .args(["s3", "cp"])
            .arg(&self.path)
            .arg(s3_uri)
            .status()?;
        if !status.success() {
            anyhow::bail!("scheduler trace upload failed with status {status}");
        }
        Ok(())
    }
}

fn unix_epoch_micros() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_micros())
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
