use std::{
    collections::{HashSet, VecDeque},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::{Parser, Subcommand};
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
    metrics::{JobLatencySample, render_summary, summarize},
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

    let samples = match args.protocol {
        ServerProtocol::Rest => {
            let client = RestStorageApiClient::new(&target)?;
            let clients = vec![client; total_connection_count(&config)];
            run_workload(clients, args.workload, &config).await?
        }
        ServerProtocol::Grpc => {
            let clients = connect_grpc_clients(target, total_connection_count(&config)).await?;
            run_workload(clients, args.workload, &config).await?
        }
    };
    let summary = summarize(&samples);
    println!("{}", render_summary(&summary));

    if let Some(output_path) = args.output {
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(output_path, serde_json::to_vec_pretty(&summary)?)?;
    }

    Ok(())
}

async fn run_workload<ClientType: StorageApiClient>(
    mut clients: Vec<ClientType>,
    workload_kind: WorkloadKind,
    config: &BenchConfig,
) -> anyhow::Result<Vec<JobLatencySample>> {
    let worker_clients = clients.split_off(config.benchmark.client_count);
    let submit_clients = clients;
    let setup_client = submit_clients
        .first()
        .ok_or_else(|| anyhow::anyhow!("client_count must be greater than 0"))?;
    let session_id = setup_client
        .get_session(GetSessionRequest {})
        .await?
        .session_id;
    let resource_group = setup_client
        .add_resource_group(AddResourceGroupRequest {
            external_id: format!("storage-api-bench-{}", uuid::Uuid::new_v4()),
            password: b"storage-api-bench".to_vec(),
        })
        .await?;

    let jobs = build_jobs(
        workload_kind,
        config.benchmark.job_count,
        config.benchmark.task_count,
        config.benchmark.payload_bytes,
        config.benchmark.flat_percent,
    )?;
    let completed = Arc::new(Mutex::new(HashSet::new()));
    let job_queue = Arc::new(Mutex::new(VecDeque::from(jobs)));
    let mut worker_tasks = spawn_worker_tasks(
        worker_clients,
        &completed,
        config.benchmark.job_count,
        session_id,
        config,
    );
    let samples = match run_submit_clients(
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
        result??;
    }
    Ok(samples)
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
) -> JoinSet<anyhow::Result<()>> {
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
) -> anyhow::Result<Vec<JobLatencySample>> {
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
    let mut samples = Vec::new();
    while let Some(result) = submit_tasks.join_next().await {
        samples.extend(result??);
    }
    Ok(samples)
}

async fn run_submit_client<ClientType: StorageApiClient>(
    client: SubmitClient<ClientType>,
) -> anyhow::Result<Vec<JobLatencySample>> {
    let mut samples = Vec::new();
    while let Some(job) = pop_job(&client.job_queue).await {
        let job_id = client
            .client
            .register_job(RegisterJobRequest {
                resource_group_id: client.resource_group_id.clone(),
                serialized_task_graph: job.serialized_task_graph,
                serialized_inputs: job.serialized_inputs,
            })
            .await?
            .job_id;
        client
            .client
            .start_job(JobIdRequest {
                job_id: job_id.clone(),
            })
            .await?;
        let start_time = Instant::now();
        let succeeded = monitor_job(&client.client, &job_id, client.poll_wait_ms).await?;
        client.completed.lock().await.insert(job_id);
        let latency = start_time.elapsed();
        samples.push(if succeeded {
            JobLatencySample::success(latency)
        } else {
            JobLatencySample::failure(latency)
        });
    }
    Ok(samples)
}

async fn run_client_worker<ClientType: StorageApiClient>(
    mut worker: ClientWorker<ClientType>,
) -> anyhow::Result<()> {
    worker.execution_manager_id = worker
        .client
        .register_execution_manager(RegisterExecutionManagerRequest {
            ip_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
        })
        .await?
        .execution_manager_id;
    let outputs = TaskOutputsSerializer::from_tuple(&(Vec::<u8>::new(),))?;
    while !all_jobs_completed(&worker.completed, worker.job_count).await {
        let ready = worker
            .client
            .poll_ready_tasks(PollReadyTasksRequest {
                max_tasks: worker.poll_batch,
                wait_ms: worker.poll_wait_ms,
            })
            .await?;
        if ready.tasks.is_empty() {
            continue;
        }
        for task in ready.tasks {
            let context = worker
                .client
                .create_task_instance(CreateTaskInstanceRequest {
                    session_id: worker.session_id,
                    job_id: task.job_id.clone(),
                    task_id: spider_storage_api_bench::api::TaskIdDto::Index {
                        task_index: task.task_index,
                    },
                    execution_manager_id: worker.execution_manager_id.clone(),
                })
                .await?;
            worker
                .client
                .succeed_task_instance(SucceedTaskInstanceRequest {
                    session_id: worker.session_id,
                    job_id: task.job_id,
                    task_instance_id: context.task_instance_id,
                    task_index: task.task_index,
                    serialized_outputs: outputs.clone(),
                })
                .await?;
        }
    }
    Ok(())
}

async fn monitor_job<ClientType: StorageApiClient>(
    client: &ClientType,
    job_id: &str,
    poll_wait_ms: u64,
) -> anyhow::Result<bool> {
    loop {
        let state = client
            .get_job_state(JobIdRequest {
                job_id: job_id.to_owned(),
            })
            .await?;
        if is_terminal(&state.state) {
            return Ok(state.state == "Succeeded");
        }
        tokio::time::sleep(Duration::from_millis(poll_wait_ms)).await;
    }
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
    target: String,
    count: usize,
) -> spider_storage_api_bench::api::ApiResult<Vec<GrpcStorageApiClient>> {
    let mut clients = Vec::with_capacity(count);
    for _ in 0..count {
        clients.push(GrpcStorageApiClient::connect(target.clone()).await?);
    }
    Ok(clients)
}

#[cfg(test)]
mod tests {
    use super::{BenchConfig, execution_manager_worker_count, total_connection_count};

    #[test]
    fn client_and_worker_counts_drive_connection_count() -> anyhow::Result<()> {
        let mut config = BenchConfig::load("config/default.toml".as_ref())?;
        config.benchmark.client_count = 3;
        config.benchmark.worker_count = 5;
        assert_eq!(5, execution_manager_worker_count(&config));
        assert_eq!(8, total_connection_count(&config));
        Ok(())
    }
}
