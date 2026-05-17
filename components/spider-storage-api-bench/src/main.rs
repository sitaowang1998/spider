use std::{
    collections::{HashSet, VecDeque},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::{Parser, Subcommand};
use serde::Serialize;
use spider_storage_api_bench::{
    api::{
        AddResourceGroupRequest,
        CreateTaskInstanceRequest,
        GetSessionRequest,
        JobIdRequest,
        PollReadyTasksRequest,
        RegisterExecutionManagerRequest,
        RegisterJobRequest,
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
use tokio::{sync::Mutex, task::JoinSet};

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

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    setup: BenchmarkSetup,
    optimal_baseline_e2e_latency: JobLatencySummary,
    job_latency: JobLatencySummary,
    request_latency: Vec<RequestLatencySummary>,
}

#[derive(Debug, Serialize)]
struct BenchmarkSetup {
    protocol: String,
    target: String,
    workload: WorkloadKind,
    flat_percent: u8,
    task_count: usize,
    job_count: usize,
    payload_bytes: usize,
    client_count: usize,
    worker_count: usize,
    channel_count: usize,
    poll_batch: usize,
    poll_wait_ms: u64,
    database_host: String,
    database_port: u16,
    database_name: String,
    database_username: String,
    database_max_connections: u32,
}

struct WorkloadMeasurements {
    optimal_baseline: Vec<JobLatencySample>,
    job_latency: Vec<JobLatencySample>,
    request_latency: Vec<RequestLatencySample>,
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

    println!(
        "protocol={:?} workload={:?} flat/deep={}/{} tasks={} jobs={} payload_bytes={} clients={} \
         workers={} channels={}",
        args.protocol,
        args.workload,
        config.benchmark.flat_percent,
        100 - config.benchmark.flat_percent,
        config.benchmark.task_count,
        config.benchmark.job_count,
        config.benchmark.payload_bytes,
        config.benchmark.client_count,
        config.benchmark.worker_count,
        total_connection_count(&config)
    );

    let measurements = match args.protocol {
        ServerProtocol::Rest => {
            let client = RestStorageApiClient::new(&target)?;
            let clients = vec![client; total_connection_count(&config)];
            run_workload(clients, args.workload, &config).await?
        }
        ServerProtocol::Grpc => {
            let clients = connect_grpc_clients(&target, total_connection_count(&config)).await?;
            run_workload(clients, args.workload, &config).await?
        }
    };
    let optimal_baseline_e2e_latency = summarize(&measurements.optimal_baseline);
    let job_latency = summarize(&measurements.job_latency);
    let request_latency = summarize_requests(&measurements.request_latency);
    let report = BenchmarkReport {
        setup: BenchmarkSetup::new(args.protocol, target, args.workload, &config),
        optimal_baseline_e2e_latency,
        job_latency,
        request_latency,
    };
    println!("optimal_baseline_e2e_latency");
    println!("{}", render_summary(&report.optimal_baseline_e2e_latency));
    println!("job_e2e_latency");
    println!("{}", render_summary(&report.job_latency));
    println!("storage_request_latency");
    println!("{}", render_request_summary(&report.request_latency));

    if let Some(output_path) = args.output {
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(output_path, serde_json::to_vec_pretty(&report)?)?;
    }

    Ok(())
}

impl BenchmarkSetup {
    fn new(
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
            client_count: config.benchmark.client_count,
            worker_count: config.benchmark.worker_count,
            channel_count: total_connection_count(config),
            poll_batch: config.benchmark.poll_batch,
            poll_wait_ms: config.benchmark.poll_wait_ms,
            database_host: config.database.host.clone(),
            database_port: config.database.port,
            database_name: config.database.name.clone(),
            database_username: config.database.username.clone(),
            database_max_connections: config.database.max_connections,
        }
    }
}

async fn run_workload<ClientType: StorageApiClient>(
    mut clients: Vec<ClientType>,
    workload_kind: WorkloadKind,
    config: &BenchConfig,
) -> anyhow::Result<WorkloadMeasurements> {
    let mut request_latency_samples = Vec::new();
    let worker_clients = clients.split_off(config.benchmark.client_count);
    let submit_clients = clients;
    let setup_client = submit_clients
        .first()
        .ok_or_else(|| anyhow::anyhow!("client_count must be greater than 0"))?;
    let session_id = record_request(
        &mut request_latency_samples,
        "get_session",
        RequestCategory::NonBlocking,
        setup_client.get_session(GetSessionRequest {}),
    )
    .await?
    .session_id;
    let resource_group = record_request(
        &mut request_latency_samples,
        "add_resource_group",
        RequestCategory::NonBlocking,
        setup_client.add_resource_group(AddResourceGroupRequest {
            external_id: format!("storage-api-bench-{}", uuid::Uuid::new_v4()),
            password: b"storage-api-bench".to_vec(),
        }),
    )
    .await?;

    let jobs = build_jobs(
        workload_kind,
        config.benchmark.job_count,
        config.benchmark.task_count,
        config.benchmark.payload_bytes,
        config.benchmark.flat_percent,
    )?;
    let optimal_baseline_samples =
        optimal_baseline_e2e_samples(&jobs, config.benchmark.poll_wait_ms);
    let completed = Arc::new(Mutex::new(HashSet::new()));
    let job_queue = Arc::new(Mutex::new(VecDeque::from(jobs)));
    let mut worker_tasks = spawn_worker_tasks(
        worker_clients,
        &completed,
        config.benchmark.job_count,
        session_id,
        config,
    );
    let mut measurements = match run_submit_clients(
        submit_clients,
        job_queue,
        &completed,
        resource_group.resource_group_id,
        config.benchmark.poll_wait_ms,
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
        request_latency_samples.extend(result??);
    }
    request_latency_samples.append(&mut measurements.request_latency);
    Ok(WorkloadMeasurements {
        optimal_baseline: optimal_baseline_samples,
        job_latency: measurements.job_latency,
        request_latency: request_latency_samples,
    })
}

struct ClientWorker<ClientType: StorageApiClient> {
    client: ClientType,
    completed: Arc<Mutex<HashSet<String>>>,
    execution_manager_id: String,
    job_count: usize,
    poll_batch: usize,
    poll_wait_ms: u64,
    session_id: u64,
}

struct SubmitClient<ClientType: StorageApiClient> {
    client: ClientType,
    completed: Arc<Mutex<HashSet<String>>>,
    job_queue: Arc<Mutex<VecDeque<JobPayload>>>,
    poll_wait_ms: u64,
    resource_group_id: String,
}

fn spawn_worker_tasks<ClientType: StorageApiClient>(
    clients: Vec<ClientType>,
    completed: &Arc<Mutex<HashSet<String>>>,
    job_count: usize,
    session_id: u64,
    config: &BenchConfig,
) -> JoinSet<anyhow::Result<Vec<RequestLatencySample>>> {
    let mut workers = JoinSet::new();
    for client in clients {
        workers.spawn(run_client_worker(ClientWorker {
            client,
            completed: Arc::clone(completed),
            execution_manager_id: String::new(),
            job_count,
            poll_batch: config.benchmark.poll_batch,
            poll_wait_ms: config.benchmark.poll_wait_ms,
            session_id,
        }));
    }
    workers
}

async fn run_submit_clients<ClientType: StorageApiClient>(
    clients: Vec<ClientType>,
    job_queue: Arc<Mutex<VecDeque<JobPayload>>>,
    completed: &Arc<Mutex<HashSet<String>>>,
    resource_group_id: String,
    poll_wait_ms: u64,
) -> anyhow::Result<WorkloadMeasurements> {
    let mut submit_tasks = JoinSet::new();
    for client in clients {
        submit_tasks.spawn(run_submit_client(SubmitClient {
            client,
            completed: Arc::clone(completed),
            job_queue: Arc::clone(&job_queue),
            poll_wait_ms,
            resource_group_id: resource_group_id.clone(),
        }));
    }
    let mut measurements = WorkloadMeasurements {
        optimal_baseline: Vec::new(),
        job_latency: Vec::new(),
        request_latency: Vec::new(),
    };
    while let Some(result) = submit_tasks.join_next().await {
        let mut task_measurements = result??;
        measurements
            .optimal_baseline
            .append(&mut task_measurements.optimal_baseline);
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
) -> anyhow::Result<WorkloadMeasurements> {
    let mut measurements = WorkloadMeasurements {
        optimal_baseline: Vec::new(),
        job_latency: Vec::new(),
        request_latency: Vec::new(),
    };
    while let Some(job) = pop_job(&client.job_queue).await {
        let job_id = record_request(
            &mut measurements.request_latency,
            "register_job",
            RequestCategory::NonBlocking,
            client.client.register_job(RegisterJobRequest {
                resource_group_id: client.resource_group_id.clone(),
                serialized_task_graph: job.serialized_task_graph,
                serialized_inputs: job.serialized_inputs,
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
        let start_time = Instant::now();
        let succeeded = monitor_job(
            &client.client,
            &mut measurements.request_latency,
            &job_id,
            client.poll_wait_ms,
        )
        .await?;
        client.completed.lock().await.insert(job_id);
        let latency = start_time.elapsed();
        measurements.job_latency.push(if succeeded {
            JobLatencySample::success(latency)
        } else {
            JobLatencySample::failure(latency)
        });
    }
    Ok(measurements)
}

async fn run_client_worker<ClientType: StorageApiClient>(
    mut worker: ClientWorker<ClientType>,
) -> anyhow::Result<Vec<RequestLatencySample>> {
    let mut request_latency_samples = Vec::new();
    worker.execution_manager_id = record_request(
        &mut request_latency_samples,
        "register_execution_manager",
        RequestCategory::NonBlocking,
        worker
            .client
            .register_execution_manager(RegisterExecutionManagerRequest {
                ip_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            }),
    )
    .await?
    .execution_manager_id;
    let outputs = TaskOutputsSerializer::from_tuple(&(Vec::<u8>::new(),))?;
    while !all_jobs_completed(&worker.completed, worker.job_count).await {
        let ready = record_request(
            &mut request_latency_samples,
            "poll_ready_tasks",
            RequestCategory::Blocking,
            worker.client.poll_ready_tasks(PollReadyTasksRequest {
                max_tasks: worker.poll_batch,
                wait_ms: worker.poll_wait_ms,
            }),
        )
        .await?;
        if ready.tasks.is_empty() {
            continue;
        }
        for task in ready.tasks {
            let context = record_request(
                &mut request_latency_samples,
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
            .await?;
            record_request(
                &mut request_latency_samples,
                "succeed_task_instance",
                RequestCategory::NonBlocking,
                worker
                    .client
                    .succeed_task_instance(SucceedTaskInstanceRequest {
                        session_id: worker.session_id,
                        job_id: task.job_id,
                        task_instance_id: context.task_instance_id,
                        task_index: task.task_index,
                        serialized_outputs: outputs.clone(),
                    }),
            )
            .await?;
        }
    }
    Ok(request_latency_samples)
}

async fn monitor_job<ClientType: StorageApiClient>(
    client: &ClientType,
    request_latency_samples: &mut Vec<RequestLatencySample>,
    job_id: &str,
    poll_wait_ms: u64,
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
        tokio::time::sleep(Duration::from_millis(poll_wait_ms)).await;
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
    let start_time = Instant::now();
    let result = future.await;
    let latency = start_time.elapsed();
    samples.push(if result.is_ok() {
        RequestLatencySample::success(operation, category, latency)
    } else {
        RequestLatencySample::failure(operation, category, latency)
    });
    result
}

fn optimal_baseline_e2e_samples(jobs: &[JobPayload], poll_wait_ms: u64) -> Vec<JobLatencySample> {
    jobs.iter()
        .map(|job| {
            let dependency_waves = match job.workload_kind {
                WorkloadKind::Flat => 1,
                WorkloadKind::Deep => job.task_count,
                WorkloadKind::Mixed => {
                    unreachable!("mixed is expanded before job construction")
                }
            };
            let dependency_waves =
                u64::try_from(dependency_waves).expect("task count should fit in u64");
            JobLatencySample::success(Duration::from_millis(
                poll_wait_ms
                    .checked_mul(dependency_waves)
                    .expect("baseline duration should fit in u64 milliseconds"),
            ))
        })
        .collect()
}

async fn pop_job(job_queue: &Arc<Mutex<VecDeque<JobPayload>>>) -> Option<JobPayload> {
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

async fn connect_grpc_clients(
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
    use spider_storage_api_bench::workload::{WorkloadKind, build_deep_job, build_flat_job};

    use super::{
        BenchConfig,
        BenchmarkSetup,
        ServerProtocol,
        execution_manager_worker_count,
        optimal_baseline_e2e_samples,
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
        assert!(value.get("database_password").is_none());
        Ok(())
    }

    #[test]
    fn optimal_baseline_uses_dependency_waves_and_poll_wait() -> anyhow::Result<()> {
        let jobs = vec![build_flat_job(8, 16)?, build_deep_job(8, 16)?];
        let samples = optimal_baseline_e2e_samples(&jobs, 10);
        assert_eq!(2, samples.len());
        assert_eq!(10_000, samples[0].latency_micros);
        assert_eq!(80_000, samples[1].latency_micros);
        Ok(())
    }
}
