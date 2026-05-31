use std::{
    collections::{HashSet, VecDeque},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use spider_storage_api_bench::{
    api::{
        AddResourceGroupRequest,
        ApiError,
        CreateTaskInstanceRequest,
        EndMetricsSessionRequest,
        GetSessionRequest,
        JobIdRequest,
        PollReadyTasksRequest,
        ReadyTaskEntryDto,
        RegisterExecutionManagerRequest,
        RegisterJobRequest,
        StartMetricsSessionRequest,
        SucceedTaskInstanceRequest,
    },
    client::StorageApiClient,
    config::BenchConfig,
    grpc::GrpcStorageApiClient,
    metrics::{
        JobLatencySample,
        JobLatencySummary,
        RequestCategory,
        RequestLatencySample,
        RequestLatencySummary,
        ServerMetricsSessionReport,
        render_request_summary,
        render_summary,
        summarize,
        summarize_requests,
    },
    rest::RestStorageApiClient,
    server::{ServerProtocol, run_server},
    workload::{JobPayload, WorkloadKind, build_jobs},
};
use spider_tdl::wire::TaskOutputsSerializer;
use tokio::{
    sync::{Barrier, Mutex},
    task::JoinSet,
};

mod agent;
mod controller;
mod distributed;

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Server(ServerArgs),
    Client(ClientArgs),
    Agent(AgentArgs),
    Controller(ControllerArgs),
}

#[derive(Debug, Parser)]
struct ServerArgs {
    #[arg(long)]
    protocol: ServerProtocol,
    #[arg(
        long,
        default_value = "components/spider-storage-api-bench/config/default.toml"
    )]
    config: PathBuf,
    #[arg(long)]
    bind: Option<SocketAddr>,
}

#[derive(Debug, Parser)]
struct ClientArgs {
    #[arg(long)]
    protocol: ServerProtocol,
    #[arg(long)]
    workload: WorkloadKind,
    #[arg(
        long,
        default_value = "components/spider-storage-api-bench/config/default.toml"
    )]
    config: PathBuf,
    #[arg(long)]
    target: Option<String>,
    #[arg(long)]
    flat_percent: Option<u8>,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Parser)]
struct AgentArgs {
    #[arg(long)]
    bind: SocketAddr,
    #[arg(
        long,
        default_value = "components/spider-storage-api-bench/config/default.toml"
    )]
    config: PathBuf,
    #[arg(long)]
    agent_id: String,
    #[arg(long)]
    role: distributed::AgentRole,
}

#[derive(Debug, Parser)]
struct ControllerArgs {
    #[arg(long)]
    pub(crate) protocol: ServerProtocol,
    #[arg(long)]
    pub(crate) workload: WorkloadKind,
    #[arg(
        long,
        default_value = "components/spider-storage-api-bench/config/default.toml"
    )]
    config: PathBuf,
    #[arg(long, default_value = "data/distributed")]
    data_dir: PathBuf,
    #[arg(long)]
    flat_percent: Option<u8>,
    #[arg(long)]
    scheduler_trace_dir: Option<PathBuf>,
    #[arg(long)]
    scheduler_trace_s3_prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BenchmarkReport {
    pub(crate) setup: BenchmarkSetup,
    pub(crate) job_latency: JobLatencySummary,
    pub(crate) request_latency: Vec<RequestLatencySummary>,
    pub(crate) server_metrics: ServerMetricsSessionReport,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) scheduler_metrics: Vec<RequestLatencySummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) scheduler_queue: Option<SchedulerQueueSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) job_latency_samples: Vec<JobLatencySample>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) request_latency_samples: Vec<RequestLatencySample>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) worker_activity_samples: Vec<WorkerActivitySample>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) distributed: Option<distributed::DistributedReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct SchedulerQueueSummary {
    pub(crate) worker_poll_limit: usize,
    pub(crate) worker_poll_count: u64,
    pub(crate) max_queue_depth: usize,
    pub(crate) avg_queue_depth: f64,
    pub(crate) max_queue_wait_us: u64,
    pub(crate) avg_queue_wait_us: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkerActivitySample {
    pub(crate) agent_id: String,
    pub(crate) worker_index: usize,
    pub(crate) execution_manager_id: String,
    pub(crate) task_count: usize,
    pub(crate) first_valid_request_start_us: Option<u128>,
    pub(crate) last_valid_request_end_us: Option<u128>,
    pub(crate) first_task_execution_start_us: Option<u128>,
    pub(crate) last_task_execution_end_us: Option<u128>,
    pub(crate) active_window_us: u128,
    pub(crate) task_execution_us: u128,
    pub(crate) request_us: u128,
    pub(crate) idle_us: u128,
    pub(crate) empty_poll_us: u128,
    pub(crate) empty_poll_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BenchmarkSetup {
    pub(crate) protocol: String,
    pub(crate) target: String,
    pub(crate) workload: WorkloadKind,
    pub(crate) flat_percent: u8,
    pub(crate) task_count: usize,
    pub(crate) job_count: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) task_sleep_ms: u64,
    pub(crate) client_count: usize,
    pub(crate) worker_count: usize,
    pub(crate) channel_count: usize,
    pub(crate) worker_poll_wait_ms: u64,
    pub(crate) job_poll_wait_ms: u64,
    pub(crate) scheduler_poll_batch: usize,
    pub(crate) scheduler_refill_interval_ms: u64,
    pub(crate) scheduler_poll_wait_ms: u64,
    pub(crate) scheduler_worker_poll_concurrency: usize,
    pub(crate) database_host: String,
    pub(crate) database_port: u16,
    pub(crate) database_name: String,
    pub(crate) database_username: String,
    pub(crate) database_max_connections: u32,
}

struct WorkloadMeasurements {
    job_latency: Vec<JobLatencySample>,
    request_latency: Vec<RequestLatencySample>,
    worker_activity: Vec<WorkerActivitySample>,
    server_metrics: ServerMetricsSessionReport,
}

pub(crate) struct ClientWorkloadMeasurements {
    job_latency: Vec<JobLatencySample>,
    request_latency: Vec<RequestLatencySample>,
    worker_activity: Vec<WorkerActivitySample>,
}

pub(crate) struct PreparedWorkload {
    pub(crate) session_id: u64,
    pub(crate) resource_group_id: String,
    pub(crate) request_latency: Vec<RequestLatencySample>,
}

pub(crate) struct ClientRunOptions {
    pub(crate) protocol: ServerProtocol,
    pub(crate) workload: WorkloadKind,
    pub(crate) config: BenchConfig,
    pub(crate) target: String,
    pub(crate) server_metrics: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Server(args) => {
            let config = BenchConfig::load(&args.config)?;
            run_server(args.protocol, config, args.bind).await
        }
        Command::Client(args) => run_client(args).await,
        Command::Agent(args) => agent::run_agent(args).await,
        Command::Controller(args) => controller::run_controller(args).await,
    }
}

async fn run_client(args: ClientArgs) -> anyhow::Result<()> {
    let mut config = BenchConfig::load(&args.config)?;
    if let Some(flat_percent) = args.flat_percent {
        config.benchmark.flat_percent = flat_percent;
    }
    config.benchmark.validate()?;

    let target = args.target.unwrap_or_else(|| match args.protocol {
        ServerProtocol::Rest => config.server.rest_target.clone(),
        ServerProtocol::Grpc => config.server.grpc_target.clone(),
    });

    let report = run_client_report(ClientRunOptions {
        protocol: args.protocol,
        workload: args.workload,
        config,
        target,
        server_metrics: true,
    })
    .await?;

    print_report(&report);

    if let Some(output_path) = args.output {
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(output_path, serde_json::to_vec_pretty(&report)?)?;
    }

    Ok(())
}

pub(crate) async fn run_client_report(
    options: ClientRunOptions,
) -> anyhow::Result<BenchmarkReport> {
    let config = options.config;
    println!(
        "protocol={:?} workload={:?} flat/deep={}/{} tasks={} jobs={} payload_bytes={} \
         task_sleep_ms={} clients={} workers={} channels={}",
        options.protocol,
        options.workload,
        config.benchmark.flat_percent,
        100 - config.benchmark.flat_percent,
        config.benchmark.task_count,
        config.benchmark.job_count,
        config.benchmark.payload_bytes,
        config.benchmark.task_sleep_ms,
        config.benchmark.client_count,
        config.benchmark.worker_count,
        total_connection_count(&config)
    );

    let measurements = match options.protocol {
        ServerProtocol::Rest => {
            let client = RestStorageApiClient::new(&options.target)?;
            let clients = vec![client; total_connection_count(&config)];
            run_measured_workload(
                clients,
                options.protocol,
                options.workload,
                &config,
                options.server_metrics,
            )
            .await?
        }
        ServerProtocol::Grpc => {
            let clients =
                connect_grpc_clients(&options.target, total_connection_count(&config)).await?;
            run_measured_workload(
                clients,
                options.protocol,
                options.workload,
                &config,
                options.server_metrics,
            )
            .await?
        }
    };
    let job_latency = summarize(&measurements.job_latency);
    let request_latency = summarize_requests(&measurements.request_latency);
    Ok(BenchmarkReport {
        setup: BenchmarkSetup::new(options.protocol, options.target, options.workload, &config),
        job_latency,
        request_latency,
        server_metrics: measurements.server_metrics,
        scheduler_metrics: Vec::new(),
        scheduler_queue: None,
        job_latency_samples: measurements.job_latency,
        request_latency_samples: measurements.request_latency,
        worker_activity_samples: measurements.worker_activity,
        distributed: None,
    })
}

fn print_report(report: &BenchmarkReport) {
    println!("job_e2e_latency");
    println!("{}", render_summary(&report.job_latency));
    println!("client_storage_request_latency");
    println!("{}", render_request_summary(&report.request_latency));
    println!("server_storage_request_latency");
    println!(
        "{}",
        render_request_summary(&report.server_metrics.request_latency)
    );
    println!("server_job_execution_latency");
    println!(
        "{}",
        render_summary(&report.server_metrics.job_execution_latency)
    );
    if !report.scheduler_metrics.is_empty() {
        println!("scheduler_request_latency");
        println!("{}", render_request_summary(&report.scheduler_metrics));
    }
    if let Some(scheduler_queue) = &report.scheduler_queue {
        println!("scheduler_queue");
        println!(
            "{}",
            concat!(
                "worker_poll_limit\tworker_poll_count\tmax_queue_depth\tavg_queue_depth",
                "\tmax_queue_wait_us\tavg_queue_wait_us"
            )
        );
        println!(
            "{}\t{}\t{}\t{:.2}\t{}\t{:.2}",
            scheduler_queue.worker_poll_limit,
            scheduler_queue.worker_poll_count,
            scheduler_queue.max_queue_depth,
            scheduler_queue.avg_queue_depth,
            scheduler_queue.max_queue_wait_us,
            scheduler_queue.avg_queue_wait_us
        );
    }
}

impl BenchmarkSetup {
    pub(crate) fn new(
        protocol: ServerProtocol,
        target: String,
        workload: WorkloadKind,
        config: &BenchConfig,
    ) -> Self {
        Self {
            protocol: format!("{protocol:?}"),
            target,
            workload,
            flat_percent: config.benchmark.flat_percent,
            task_count: config.benchmark.task_count,
            job_count: config.benchmark.job_count,
            payload_bytes: config.benchmark.payload_bytes,
            task_sleep_ms: config.benchmark.task_sleep_ms,
            client_count: config.benchmark.client_count,
            worker_count: config.benchmark.worker_count,
            channel_count: total_connection_count(config),
            worker_poll_wait_ms: config.benchmark.worker_poll_wait_ms,
            job_poll_wait_ms: config.benchmark.job_poll_wait_ms,
            scheduler_poll_batch: config.benchmark.scheduler_poll_batch,
            scheduler_refill_interval_ms: config.benchmark.scheduler_refill_interval_ms,
            scheduler_poll_wait_ms: config.benchmark.scheduler_poll_wait_ms,
            scheduler_worker_poll_concurrency: config.benchmark.scheduler_worker_poll_concurrency,
            database_host: config.database.host.clone(),
            database_port: config.database.port,
            database_name: config.database.name.clone(),
            database_username: config.database.username.clone(),
            database_max_connections: config.database.max_connections,
        }
    }
}

async fn run_measured_workload<ClientType: StorageApiClient>(
    clients: Vec<ClientType>,
    protocol: ServerProtocol,
    workload_kind: WorkloadKind,
    config: &BenchConfig,
    server_metrics: bool,
) -> anyhow::Result<WorkloadMeasurements> {
    let control_client = clients
        .first()
        .ok_or_else(|| anyhow::anyhow!("client_count plus worker_count must be greater than 0"))?
        .clone();
    let metrics_session = if server_metrics {
        Some(
            control_client
                .start_metrics_session(StartMetricsSessionRequest {
                    label: Some(format!("{protocol:?}_{workload_kind:?}")),
                })
                .await?,
        )
    } else {
        None
    };
    let client_measurements = run_workload(clients, workload_kind, config).await;
    let server_metrics = if let Some(metrics_session) = metrics_session {
        control_client
            .end_metrics_session(EndMetricsSessionRequest {
                metrics_session_id: metrics_session.metrics_session_id,
            })
            .await?
    } else {
        empty_server_metrics_report()
    };
    let client_measurements = client_measurements?;
    Ok(WorkloadMeasurements {
        job_latency: client_measurements.job_latency,
        request_latency: client_measurements.request_latency,
        worker_activity: client_measurements.worker_activity,
        server_metrics,
    })
}

pub(crate) const fn empty_server_metrics_report() -> ServerMetricsSessionReport {
    ServerMetricsSessionReport {
        metrics_session_id: String::new(),
        label: None,
        elapsed_micros: 0,
        request_latency: Vec::new(),
        low_count_request_latency: Vec::new(),
        request_sizes: Vec::new(),
        job_execution_latency: JobLatencySummary {
            count: 0,
            failed_jobs: 0,
            avg_us: 0,
            p50_us: 0,
            p90_us: 0,
            p99_us: 0,
            max_us: 0,
        },
    }
}

async fn run_workload<ClientType: StorageApiClient>(
    mut clients: Vec<ClientType>,
    workload_kind: WorkloadKind,
    config: &BenchConfig,
) -> anyhow::Result<ClientWorkloadMeasurements> {
    let worker_clients = clients.split_off(config.benchmark.client_count);
    let submit_clients = clients;
    let setup_client = submit_clients
        .first()
        .ok_or_else(|| anyhow::anyhow!("client_count must be greater than 0"))?;
    let mut prepared = prepare_workload(setup_client).await?;

    let jobs = build_jobs(
        workload_kind,
        config.benchmark.job_count,
        config.benchmark.task_count,
        config.benchmark.payload_bytes,
        config.benchmark.flat_percent,
    )?;
    let completed = Arc::new(Mutex::new(HashSet::new()));
    let job_queue = Arc::new(Mutex::new(VecDeque::from(jobs)));
    let started_jobs = Arc::new(Mutex::new(VecDeque::new()));
    let mut worker_tasks = spawn_worker_tasks(
        worker_clients,
        WorkerSpawnOptions {
            agent_id: "local",
            completed: &completed,
            job_count: config.benchmark.job_count,
            session_id: prepared.session_id,
            config,
            stop_requested: None,
            scheduler: None,
        },
    );
    let mut measurements = match run_submit_clients(
        submit_clients,
        job_queue,
        started_jobs,
        &completed,
        prepared.resource_group_id,
        config.benchmark.job_poll_wait_ms,
    )
    .await
    {
        Ok(samples) => samples,
        Err(err) => {
            worker_tasks.abort_all();
            return Err(err);
        }
    };
    while let Some(result) = worker_tasks.join_next().await {
        let result = result??;
        prepared.request_latency.extend(result.request_latency);
        measurements.worker_activity.push(result.worker_activity);
    }
    prepared
        .request_latency
        .append(&mut measurements.request_latency);
    Ok(ClientWorkloadMeasurements {
        job_latency: measurements.job_latency,
        request_latency: prepared.request_latency,
        worker_activity: measurements.worker_activity,
    })
}

pub(crate) async fn prepare_workload<ClientType: StorageApiClient>(
    setup_client: &ClientType,
) -> anyhow::Result<PreparedWorkload> {
    let mut request_latency = Vec::new();
    let session_id = record_request(
        &mut request_latency,
        "get_session",
        RequestCategory::NonBlocking,
        setup_client.get_session(GetSessionRequest {}),
    )
    .await?
    .session_id;
    let resource_group = record_request(
        &mut request_latency,
        "add_resource_group",
        RequestCategory::NonBlocking,
        setup_client.add_resource_group(AddResourceGroupRequest {
            external_id: format!("storage-api-bench-{}", uuid::Uuid::new_v4()),
            password: b"storage-api-bench".to_vec(),
        }),
    )
    .await?;
    Ok(PreparedWorkload {
        session_id,
        resource_group_id: resource_group.resource_group_id,
        request_latency,
    })
}

pub(crate) async fn run_submitter_workload<ClientType: StorageApiClient>(
    clients: Vec<ClientType>,
    workload_kind: WorkloadKind,
    resource_group_id: String,
    config: &BenchConfig,
) -> anyhow::Result<ClientWorkloadMeasurements> {
    let jobs = build_jobs(
        workload_kind,
        config.benchmark.job_count,
        config.benchmark.task_count,
        config.benchmark.payload_bytes,
        config.benchmark.flat_percent,
    )?;
    let completed = Arc::new(Mutex::new(HashSet::new()));
    let job_queue = Arc::new(Mutex::new(VecDeque::from(jobs)));
    let started_jobs = Arc::new(Mutex::new(VecDeque::new()));
    run_submit_clients(
        clients,
        job_queue,
        started_jobs,
        &completed,
        resource_group_id,
        config.benchmark.job_poll_wait_ms,
    )
    .await
}

pub(crate) async fn run_worker_workload<ClientType: StorageApiClient>(
    clients: Vec<ClientType>,
    agent_id: String,
    session_id: u64,
    config: &BenchConfig,
    stop_requested: Arc<AtomicBool>,
    scheduler: Option<SchedulerReadyTasksClient>,
) -> anyhow::Result<ClientWorkloadMeasurements> {
    let completed = Arc::new(Mutex::new(HashSet::new()));
    let mut worker_tasks = spawn_worker_tasks(
        clients,
        WorkerSpawnOptions {
            agent_id: &agent_id,
            completed: &completed,
            job_count: usize::MAX,
            session_id,
            config,
            stop_requested: Some(&stop_requested),
            scheduler: scheduler.as_ref(),
        },
    );
    let mut request_latency = Vec::new();
    let mut worker_activity = Vec::new();
    while let Some(result) = worker_tasks.join_next().await {
        let result = result??;
        request_latency.extend(result.request_latency);
        worker_activity.push(result.worker_activity);
    }
    Ok(ClientWorkloadMeasurements {
        job_latency: Vec::new(),
        request_latency,
        worker_activity,
    })
}

struct ClientWorker<ClientType: StorageApiClient> {
    client: ClientType,
    agent_id: String,
    worker_index: usize,
    completed: Arc<Mutex<HashSet<String>>>,
    execution_manager_id: String,
    job_count: usize,
    worker_poll_wait_ms: u64,
    scheduler: Option<SchedulerReadyTasksClient>,
    task_sleep_ms: u64,
    session_id: u64,
    stop_requested: Option<Arc<AtomicBool>>,
}

#[derive(Clone)]
pub(crate) struct SchedulerReadyTasksClient {
    http: reqwest::Client,
    base_url: String,
    run_id: String,
}

impl SchedulerReadyTasksClient {
    pub(crate) fn new(base_url: impl Into<String>, run_id: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            run_id: run_id.into(),
        }
    }

    async fn poll_ready_task(
        &self,
        wait_ms: u64,
    ) -> spider_storage_api_bench::api::ApiResult<Option<ReadyTaskEntryDto>> {
        let response = self
            .http
            .post(format!(
                "{}/scheduler/runs/{}/ready-tasks",
                self.base_url, self.run_id
            ))
            .json(&SchedulerPollReadyTaskRequest { wait_ms })
            .send()
            .await
            .map_err(|err| ApiError::internal(format!("scheduler request failed: {err}")))?
            .error_for_status()
            .map_err(|err| ApiError::internal(format!("scheduler request failed: {err}")))?
            .json::<SchedulerReadyTaskResponse>()
            .await
            .map_err(|err| {
                ApiError::internal(format!("scheduler response decode failed: {err}"))
            })?;
        Ok(response.task)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SchedulerPollReadyTaskRequest {
    pub(crate) wait_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SchedulerReadyTaskResponse {
    pub(crate) task: Option<ReadyTaskEntryDto>,
}

struct WorkerRunResult {
    request_latency: Vec<RequestLatencySample>,
    worker_activity: WorkerActivitySample,
}

#[derive(Default)]
struct WorkerActivityTracker {
    first_valid_request_start_us: Option<u128>,
    last_valid_request_end_us: Option<u128>,
    first_task_execution_start_us: Option<u128>,
    last_task_execution_end_us: Option<u128>,
    task_count: usize,
    task_execution_us: u128,
    request_us: u128,
    empty_poll_us: u128,
    empty_poll_count: usize,
    pending_empty_poll_us: u128,
    pending_empty_poll_count: usize,
}

impl WorkerActivityTracker {
    fn record_valid_request(
        &mut self,
        loop_started_at: Instant,
        request_started_at: Instant,
        latency: Duration,
    ) {
        let request_start_us = duration_micros(request_started_at.duration_since(loop_started_at));
        self.first_valid_request_start_us
            .get_or_insert(request_start_us);
        self.empty_poll_us += self.pending_empty_poll_us;
        self.empty_poll_count += self.pending_empty_poll_count;
        self.pending_empty_poll_us = 0;
        self.pending_empty_poll_count = 0;
        self.request_us += duration_micros(latency);
        self.last_valid_request_end_us = Some(duration_micros(loop_started_at.elapsed()));
    }

    const fn record_empty_poll(&mut self, latency: Duration) {
        if self.first_valid_request_start_us.is_some() {
            self.pending_empty_poll_count += 1;
            self.pending_empty_poll_us += duration_micros(latency);
        }
    }

    fn active_window_us(&self) -> u128 {
        self.first_valid_request_start_us
            .zip(self.last_valid_request_end_us)
            .map_or(0, |(start_us, end_us)| end_us.saturating_sub(start_us))
    }
}

struct SubmitClient<ClientType: StorageApiClient> {
    client: ClientType,
    completed: Arc<Mutex<HashSet<String>>>,
    job_queue: Arc<Mutex<VecDeque<JobPayload>>>,
    started_jobs: Arc<Mutex<VecDeque<StartedJob>>>,
    submit_barrier: Arc<Barrier>,
    job_poll_wait_ms: u64,
    resource_group_id: String,
}

struct StartedJob {
    job_id: String,
    start_time: Instant,
}

#[derive(Clone, Copy)]
struct WorkerSpawnOptions<'a> {
    agent_id: &'a str,
    completed: &'a Arc<Mutex<HashSet<String>>>,
    job_count: usize,
    session_id: u64,
    config: &'a BenchConfig,
    stop_requested: Option<&'a Arc<AtomicBool>>,
    scheduler: Option<&'a SchedulerReadyTasksClient>,
}

fn spawn_worker_tasks<ClientType: StorageApiClient>(
    clients: Vec<ClientType>,
    options: WorkerSpawnOptions<'_>,
) -> JoinSet<anyhow::Result<WorkerRunResult>> {
    let mut workers = JoinSet::new();
    for (worker_index, client) in clients.into_iter().enumerate() {
        workers.spawn(run_client_worker(ClientWorker {
            client,
            agent_id: options.agent_id.to_owned(),
            worker_index,
            completed: Arc::clone(options.completed),
            execution_manager_id: String::new(),
            job_count: options.job_count,
            worker_poll_wait_ms: options.config.benchmark.worker_poll_wait_ms,
            scheduler: options.scheduler.cloned(),
            task_sleep_ms: options.config.benchmark.task_sleep_ms,
            session_id: options.session_id,
            stop_requested: options.stop_requested.cloned(),
        }));
    }
    workers
}

async fn run_submit_clients<ClientType: StorageApiClient>(
    clients: Vec<ClientType>,
    job_queue: Arc<Mutex<VecDeque<JobPayload>>>,
    started_jobs: Arc<Mutex<VecDeque<StartedJob>>>,
    completed: &Arc<Mutex<HashSet<String>>>,
    resource_group_id: String,
    job_poll_wait_ms: u64,
) -> anyhow::Result<ClientWorkloadMeasurements> {
    if clients.is_empty() {
        anyhow::bail!("submitter requires at least one client");
    }
    let mut submit_tasks = JoinSet::new();
    let submit_barrier = Arc::new(Barrier::new(clients.len()));
    for client in clients {
        submit_tasks.spawn(run_submit_client(SubmitClient {
            client,
            completed: Arc::clone(completed),
            job_queue: Arc::clone(&job_queue),
            started_jobs: Arc::clone(&started_jobs),
            submit_barrier: Arc::clone(&submit_barrier),
            job_poll_wait_ms,
            resource_group_id: resource_group_id.clone(),
        }));
    }
    let mut measurements = ClientWorkloadMeasurements {
        job_latency: Vec::new(),
        request_latency: Vec::new(),
        worker_activity: Vec::new(),
    };
    while let Some(result) = submit_tasks.join_next().await {
        let mut task_measurements = result??;
        measurements
            .job_latency
            .append(&mut task_measurements.job_latency);
        measurements
            .request_latency
            .append(&mut task_measurements.request_latency);
    }
    Ok(measurements)
}

async fn run_submit_client<ClientType: StorageApiClient>(
    client: SubmitClient<ClientType>,
) -> anyhow::Result<ClientWorkloadMeasurements> {
    let mut measurements = ClientWorkloadMeasurements {
        job_latency: Vec::new(),
        request_latency: Vec::new(),
        worker_activity: Vec::new(),
    };
    let submit_result = submit_all_jobs(&client, &mut measurements).await;
    client.submit_barrier.wait().await;
    submit_result?;
    while let Some(job) = pop_started_job(&client.started_jobs).await {
        let succeeded = monitor_job(
            &client.client,
            &mut measurements.request_latency,
            &job.job_id,
            client.job_poll_wait_ms,
        )
        .await?;
        client.completed.lock().await.insert(job.job_id);
        let latency = job.start_time.elapsed();
        measurements.job_latency.push(if succeeded {
            JobLatencySample::success(latency)
        } else {
            JobLatencySample::failure(latency)
        });
    }
    Ok(measurements)
}

async fn submit_all_jobs<ClientType: StorageApiClient>(
    client: &SubmitClient<ClientType>,
    measurements: &mut ClientWorkloadMeasurements,
) -> anyhow::Result<()> {
    while let Some(job) = pop_job(&client.job_queue).await {
        let start_time = Instant::now();
        let job_id = record_request(
            &mut measurements.request_latency,
            "register_job",
            RequestCategory::NonBlocking,
            client.client.register_job(RegisterJobRequest {
                resource_group_id: client.resource_group_id.clone(),
                compressed_task_graph: job.compressed_task_graph,
                task_graph_uncompressed_bytes: job.task_graph_uncompressed_bytes,
                compressed_inputs: job.compressed_inputs,
                job_inputs_uncompressed_bytes: job.job_inputs_uncompressed_bytes,
            }),
        )
        .await?
        .job_id;
        record_request(
            &mut measurements.request_latency,
            "start_job",
            RequestCategory::NonBlocking,
            client.client.start_job(JobIdRequest {
                job_id: job_id.clone(),
            }),
        )
        .await?;
        client
            .started_jobs
            .lock()
            .await
            .push_back(StartedJob { job_id, start_time });
    }
    Ok(())
}

async fn run_client_worker<ClientType: StorageApiClient>(
    mut worker: ClientWorker<ClientType>,
) -> anyhow::Result<WorkerRunResult> {
    let mut request_latency_samples = Vec::new();
    let loop_started_at = Instant::now();
    let mut activity = WorkerActivityTracker::default();
    let register_started_at = Instant::now();
    let (execution_manager, register_latency) = record_timed_request(
        &mut request_latency_samples,
        "register_execution_manager",
        RequestCategory::NonBlocking,
        worker
            .client
            .register_execution_manager(RegisterExecutionManagerRequest {
                ip_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            }),
    )
    .await
    .map_err(|error| {
        anyhow::anyhow!(
            "worker {}:{} register_execution_manager failed: {error}",
            worker.agent_id,
            worker.worker_index
        )
    })?;
    activity.record_valid_request(loop_started_at, register_started_at, register_latency);
    worker.execution_manager_id = execution_manager.execution_manager_id;
    let outputs = TaskOutputsSerializer::from_tuple(&(Vec::<u8>::new(),))?;
    while should_worker_continue(&worker).await {
        let task = poll_ready_task_for_worker(
            &worker,
            loop_started_at,
            &mut request_latency_samples,
            &mut activity,
        )
        .await
        .map_err(|error| {
            anyhow::anyhow!(
                "worker {}:{} poll_ready_tasks failed: {error}",
                worker.agent_id,
                worker.worker_index
            )
        })?;
        if let Some(task) = task {
            execute_ready_task(
                &worker,
                task,
                &outputs,
                loop_started_at,
                &mut request_latency_samples,
                &mut activity,
            )
            .await?;
        }
    }
    let active_window_us = activity.active_window_us();
    let idle_us = active_window_us.saturating_sub(activity.request_us + activity.task_execution_us);
    Ok(WorkerRunResult {
        request_latency: request_latency_samples,
        worker_activity: WorkerActivitySample {
            agent_id: worker.agent_id,
            worker_index: worker.worker_index,
            execution_manager_id: worker.execution_manager_id,
            task_count: activity.task_count,
            first_valid_request_start_us: activity.first_valid_request_start_us,
            last_valid_request_end_us: activity.last_valid_request_end_us,
            first_task_execution_start_us: activity.first_task_execution_start_us,
            last_task_execution_end_us: activity.last_task_execution_end_us,
            active_window_us,
            task_execution_us: activity.task_execution_us,
            request_us: activity.request_us,
            idle_us,
            empty_poll_us: activity.empty_poll_us,
            empty_poll_count: activity.empty_poll_count,
        },
    })
}

async fn poll_ready_task_for_worker<ClientType: StorageApiClient>(
    worker: &ClientWorker<ClientType>,
    loop_started_at: Instant,
    request_latency_samples: &mut Vec<RequestLatencySample>,
    activity: &mut WorkerActivityTracker,
) -> spider_storage_api_bench::api::ApiResult<Option<ReadyTaskEntryDto>> {
    let poll_started_at = Instant::now();
    let result = poll_ready_task(worker).await;
    let poll_latency = poll_started_at.elapsed();
    if result.is_err() {
        request_latency_samples.push(RequestLatencySample::failure(
            "poll_ready_tasks",
            RequestCategory::Blocking,
            poll_latency,
        ));
    }
    let task = result?;
    request_latency_samples.push(RequestLatencySample::success(
        "poll_ready_tasks",
        RequestCategory::Blocking,
        poll_latency,
    ));
    if task.is_none() {
        activity.record_empty_poll(poll_latency);
    } else {
        request_latency_samples.push(RequestLatencySample::success(
            "poll_ready_tasks_returned",
            RequestCategory::Blocking,
            poll_latency,
        ));
        activity.record_valid_request(loop_started_at, poll_started_at, poll_latency);
    }
    Ok(task)
}

async fn poll_ready_task<ClientType: StorageApiClient>(
    worker: &ClientWorker<ClientType>,
) -> spider_storage_api_bench::api::ApiResult<Option<ReadyTaskEntryDto>> {
    if let Some(scheduler) = &worker.scheduler {
        scheduler.poll_ready_task(worker.worker_poll_wait_ms).await
    } else {
        worker
            .client
            .poll_ready_tasks(PollReadyTasksRequest {
                max_tasks: 1,
                wait_ms: worker.worker_poll_wait_ms,
            })
            .await
            .map(|mut ready| ready.tasks.pop())
    }
}

async fn execute_ready_task<ClientType: StorageApiClient>(
    worker: &ClientWorker<ClientType>,
    task: ReadyTaskEntryDto,
    outputs: &[u8],
    loop_started_at: Instant,
    request_latency_samples: &mut Vec<RequestLatencySample>,
    activity: &mut WorkerActivityTracker,
) -> anyhow::Result<()> {
    let create_started_at = Instant::now();
    let job_id = task.job_id.clone();
    let task_index = task.task_index;
    let (context, create_latency) = record_timed_request(
        request_latency_samples,
        "create_task_instance",
        RequestCategory::NonBlocking,
        worker
            .client
            .create_task_instance(CreateTaskInstanceRequest {
                session_id: worker.session_id,
                job_id: task.job_id.clone(),
                task_id: spider_storage_api_bench::api::TaskIdDto::Index {
                    task_index: task.task_index,
                },
                execution_manager_id: worker.execution_manager_id.clone(),
            }),
    )
    .await
    .map_err(|error| {
        anyhow::anyhow!(
            "worker {}:{} create_task_instance failed for job={} task_index={}: {error}",
            worker.agent_id,
            worker.worker_index,
            job_id,
            task_index
        )
    })?;
    activity.record_valid_request(loop_started_at, create_started_at, create_latency);
    record_task_execution(worker.task_sleep_ms, loop_started_at, activity).await;
    let succeed_started_at = Instant::now();
    let (_state, succeed_latency) = record_timed_request(
        request_latency_samples,
        "succeed_task_instance",
        RequestCategory::NonBlocking,
        worker
            .client
            .succeed_task_instance(SucceedTaskInstanceRequest {
                session_id: worker.session_id,
                job_id: task.job_id,
                task_instance_id: context.task_instance_id,
                task_index: task.task_index,
                serialized_outputs: outputs.to_vec(),
            }),
    )
    .await
    .map_err(|error| {
        anyhow::anyhow!(
            "worker {}:{} succeed_task_instance failed for job={} task_index={} \
             task_instance_id={}: {error}",
            worker.agent_id,
            worker.worker_index,
            job_id,
            task_index,
            context.task_instance_id
        )
    })?;
    activity.record_valid_request(loop_started_at, succeed_started_at, succeed_latency);
    Ok(())
}

async fn record_task_execution(
    task_sleep_ms: u64,
    loop_started_at: Instant,
    activity: &mut WorkerActivityTracker,
) {
    let task_execution_started_at = Instant::now();
    let task_execution_start_us =
        duration_micros(task_execution_started_at.duration_since(loop_started_at));
    activity
        .first_task_execution_start_us
        .get_or_insert(task_execution_start_us);
    tokio::time::sleep(Duration::from_millis(task_sleep_ms)).await;
    let task_execution_latency = task_execution_started_at.elapsed();
    activity.task_count += 1;
    activity.task_execution_us += duration_micros(task_execution_latency);
    activity.last_task_execution_end_us = Some(duration_micros(loop_started_at.elapsed()));
}

async fn should_worker_continue<ClientType: StorageApiClient>(
    worker: &ClientWorker<ClientType>,
) -> bool {
    if worker
        .stop_requested
        .as_ref()
        .is_some_and(|stop| stop.load(Ordering::Relaxed))
    {
        return false;
    }
    !all_jobs_completed(&worker.completed, worker.job_count).await
}

async fn monitor_job<ClientType: StorageApiClient>(
    client: &ClientType,
    request_latency_samples: &mut Vec<RequestLatencySample>,
    job_id: &str,
    job_poll_wait_ms: u64,
) -> anyhow::Result<bool> {
    loop {
        let state = record_request(
            request_latency_samples,
            "get_job_state",
            RequestCategory::NonBlocking,
            client.get_job_state(JobIdRequest {
                job_id: job_id.to_owned(),
            }),
        )
        .await?;
        if is_terminal(&state.state) {
            return Ok(state.state == "Succeeded");
        }
        tokio::time::sleep(Duration::from_millis(job_poll_wait_ms)).await;
    }
}

async fn record_request<ResponseType, FutureType>(
    samples: &mut Vec<RequestLatencySample>,
    operation: &'static str,
    category: RequestCategory,
    future: FutureType,
) -> spider_storage_api_bench::api::ApiResult<ResponseType>
where
    FutureType:
        std::future::Future<Output = spider_storage_api_bench::api::ApiResult<ResponseType>>, {
    record_timed_request(samples, operation, category, future)
        .await
        .map(|(response, _)| response)
}

pub(crate) async fn record_timed_request<ResponseType, FutureType>(
    samples: &mut Vec<RequestLatencySample>,
    operation: &'static str,
    category: RequestCategory,
    future: FutureType,
) -> spider_storage_api_bench::api::ApiResult<(ResponseType, Duration)>
where
    FutureType:
        std::future::Future<Output = spider_storage_api_bench::api::ApiResult<ResponseType>>, {
    let start_time = Instant::now();
    let result = future.await;
    let latency = start_time.elapsed();
    samples.push(if result.is_ok() {
        RequestLatencySample::success(operation, category, latency)
    } else {
        RequestLatencySample::failure(operation, category, latency)
    });
    result.map(|response| (response, latency))
}

const fn duration_micros(duration: Duration) -> u128 {
    duration.as_micros()
}

async fn pop_job(job_queue: &Arc<Mutex<VecDeque<JobPayload>>>) -> Option<JobPayload> {
    job_queue.lock().await.pop_front()
}

async fn pop_started_job(job_queue: &Arc<Mutex<VecDeque<StartedJob>>>) -> Option<StartedJob> {
    job_queue.lock().await.pop_front()
}

async fn all_jobs_completed(completed: &Arc<Mutex<HashSet<String>>>, job_count: usize) -> bool {
    completed.lock().await.len() >= job_count
}

fn is_terminal(state: &str) -> bool {
    matches!(state, "Succeeded" | "Failed" | "Cancelled")
}

const fn execution_manager_worker_count(config: &BenchConfig) -> usize {
    config.benchmark.worker_count
}

const fn total_connection_count(config: &BenchConfig) -> usize {
    config.benchmark.client_count + execution_manager_worker_count(config)
}

pub(crate) async fn connect_grpc_clients(
    target: &str,
    count: usize,
) -> spider_storage_api_bench::api::ApiResult<Vec<GrpcStorageApiClient>> {
    let mut clients = Vec::with_capacity(count);
    for _ in 0..count {
        clients.push(GrpcStorageApiClient::connect(target.to_owned()).await?);
    }
    Ok(clients)
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;
    use spider_storage_api_bench::{
        metrics::{
            JobLatencySummary,
            RequestLatencySummary,
            ServerMetricsSessionReport,
            render_request_summary,
        },
        workload::WorkloadKind,
    };

    use super::{
        BenchConfig,
        BenchmarkReport,
        BenchmarkSetup,
        Cli,
        SchedulerQueueSummary,
        ServerProtocol,
        execution_manager_worker_count,
        total_connection_count,
    };

    #[test]
    fn client_and_worker_counts_drive_connection_count() -> anyhow::Result<()> {
        let mut config = BenchConfig::load("config/default.toml".as_ref())?;
        config.benchmark.client_count = 3;
        config.benchmark.worker_count = 5;
        assert_eq!(5, execution_manager_worker_count(&config));
        assert_eq!(8, total_connection_count(&config));
        Ok(())
    }

    #[test]
    fn controller_requires_single_protocol_and_workload() {
        let result =
            Cli::try_parse_from(["bench", "controller", "--config", "config/default.toml"]);
        assert!(result.is_err());
    }

    #[test]
    fn controller_accepts_single_protocol_and_workload() {
        let result = Cli::try_parse_from([
            "bench",
            "controller",
            "--config",
            "config/default.toml",
            "--protocol",
            "grpc",
            "--workload",
            "flat",
        ]);
        assert!(result.is_ok());
    }

    #[test]
    fn benchmark_setup_reports_effective_config_without_password() -> anyhow::Result<()> {
        let config = BenchConfig::load("config/default.toml".as_ref())?;
        let setup = BenchmarkSetup::new(
            ServerProtocol::Rest,
            "http://127.0.0.1:8080".to_owned(),
            WorkloadKind::Mixed,
            &config,
        );
        let value = serde_json::to_value(setup)?;
        assert_eq!(value["client_count"], config.benchmark.client_count);
        assert_eq!(value["worker_count"], config.benchmark.worker_count);
        assert_eq!(value["task_sleep_ms"], config.benchmark.task_sleep_ms);
        assert_eq!(
            value["scheduler_worker_poll_concurrency"],
            config.benchmark.scheduler_worker_poll_concurrency
        );
        assert!(value.get("database_password").is_none());
        Ok(())
    }

    #[test]
    fn benchmark_report_serializes_server_metrics() -> anyhow::Result<()> {
        let config = BenchConfig::load("config/default.toml".as_ref())?;
        let server_row = RequestLatencySummary {
            category: "non_blocking".to_owned(),
            operation: "register_job".to_owned(),
            count: 7,
            errors: 1,
            avg_us: 15,
            p50_us: 10,
            p90_us: 20,
            p99_us: 30,
            max_us: 40,
        };
        let report = BenchmarkReport {
            setup: BenchmarkSetup::new(
                ServerProtocol::Grpc,
                "http://127.0.0.1:50051".to_owned(),
                WorkloadKind::Flat,
                &config,
            ),
            job_latency: JobLatencySummary {
                count: 1,
                failed_jobs: 0,
                avg_us: 100,
                p50_us: 100,
                p90_us: 100,
                p99_us: 100,
                max_us: 100,
            },
            request_latency: Vec::new(),
            server_metrics: ServerMetricsSessionReport {
                metrics_session_id: "session-1".to_owned(),
                label: Some("Grpc_Flat".to_owned()),
                elapsed_micros: 1234,
                request_latency: vec![server_row],
                low_count_request_latency: Vec::new(),
                request_sizes: Vec::new(),
                job_execution_latency: JobLatencySummary {
                    count: 1,
                    failed_jobs: 0,
                    avg_us: 200,
                    p50_us: 200,
                    p90_us: 200,
                    p99_us: 200,
                    max_us: 200,
                },
            },
            scheduler_metrics: vec![RequestLatencySummary {
                category: "blocking".to_owned(),
                operation: "worker_poll_ready_tasks".to_owned(),
                count: 3,
                errors: 0,
                avg_us: 25,
                p50_us: 20,
                p90_us: 30,
                p99_us: 35,
                max_us: 40,
            }],
            scheduler_queue: Some(SchedulerQueueSummary {
                worker_poll_limit: 512,
                worker_poll_count: 3,
                max_queue_depth: 2,
                avg_queue_depth: 1.5,
                max_queue_wait_us: 40,
                avg_queue_wait_us: 20.0,
            }),
            job_latency_samples: Vec::new(),
            request_latency_samples: Vec::new(),
            worker_activity_samples: Vec::new(),
            distributed: None,
        };

        let value = serde_json::to_value(report)?;
        assert_eq!("session-1", value["server_metrics"]["metrics_session_id"]);
        assert_eq!("Grpc_Flat", value["server_metrics"]["label"]);
        assert_eq!(1234, value["server_metrics"]["elapsed_micros"]);
        assert_eq!(15, value["server_metrics"]["request_latency"][0]["avg_us"]);
        assert_eq!(
            "register_job",
            value["server_metrics"]["request_latency"][0]["operation"]
        );
        assert_eq!(7, value["server_metrics"]["request_latency"][0]["count"]);
        assert_eq!(
            200,
            value["server_metrics"]["job_execution_latency"]["avg_us"]
        );
        assert_eq!(
            "worker_poll_ready_tasks",
            value["scheduler_metrics"][0]["operation"]
        );
        assert_eq!(25, value["scheduler_metrics"][0]["avg_us"]);
        assert_eq!(512, value["scheduler_queue"]["worker_poll_limit"]);
        assert_eq!(2, value["scheduler_queue"]["max_queue_depth"]);
        Ok(())
    }

    #[test]
    fn server_metrics_render_as_console_table() {
        let table = render_request_summary(&[RequestLatencySummary {
            category: "blocking".to_owned(),
            operation: "poll_ready_tasks".to_owned(),
            count: 3,
            errors: 0,
            avg_us: 12,
            p50_us: 11,
            p90_us: 22,
            p99_us: 33,
            max_us: 44,
        }]);
        assert!(table.contains("poll_ready_tasks"));
        assert!(table.contains("blocking"));
        assert!(table.contains("avg_us"));
        assert!(table.contains("p99_us"));
    }
}
