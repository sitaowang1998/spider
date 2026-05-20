use std::time::{Duration, Instant};

use spider_storage_api_bench::{
    api::{EndMetricsSessionRequest, StartMetricsSessionRequest},
    client::StorageApiClient,
    config::{BenchConfig, DistributedAgentConfig},
    grpc::GrpcStorageApiClient,
    metrics::ServerMetricsSessionReport,
    rest::RestStorageApiClient,
    server::ServerProtocol,
    workload::WorkloadKind,
};

use crate::{
    BenchmarkReport,
    BenchmarkSetup,
    ControllerArgs,
    distributed::{
        AgentJobAllocation,
        AgentRunAccepted,
        AgentRunRequest,
        AgentRunState,
        AgentRunStatus,
        allocate_jobs,
        merge_agent_reports,
    },
};

pub async fn run_controller(args: ControllerArgs) -> anyhow::Result<()> {
    let mut config = BenchConfig::load(&args.config)?;
    if let Some(flat_percent) = args.flat_percent {
        config.benchmark.flat_percent = flat_percent;
    }
    config.benchmark.validate()?;
    let distributed = config
        .distributed
        .clone()
        .ok_or_else(|| anyhow::anyhow!("controller requires [distributed] config"))?;
    distributed.validate()?;

    for protocol in [ServerProtocol::Grpc, ServerProtocol::Rest] {
        for workload in [WorkloadKind::Flat, WorkloadKind::Deep, WorkloadKind::Mixed] {
            run_controller_workload(
                &config,
                &distributed.agents,
                protocol,
                workload,
                &args.data_dir,
            )
            .await?;
        }
    }
    Ok(())
}

async fn run_controller_workload(
    config: &BenchConfig,
    agents: &[DistributedAgentConfig],
    protocol: ServerProtocol,
    workload: WorkloadKind,
    data_dir: &std::path::Path,
) -> anyhow::Result<()> {
    let target = match protocol {
        ServerProtocol::Rest => config.server.rest_target.clone(),
        ServerProtocol::Grpc => config.server.grpc_target.clone(),
    };
    let agent_ids = agents
        .iter()
        .map(|agent| agent.id.clone())
        .collect::<Vec<_>>();
    let allocations = allocate_jobs(config.benchmark.job_count, &agent_ids)?;
    let run_name = format!("{}_{}", protocol_name(protocol), workload_name(workload));
    let metrics_session = start_server_metrics(protocol, &target, &run_name).await?;
    let wall_start = Instant::now();
    let distributed = config
        .distributed
        .as_ref()
        .expect("distributed config should exist");
    let agent_result = run_agents(AgentDispatch {
        agents,
        allocations: &allocations,
        protocol,
        workload,
        target: &target,
        flat_percent: config.benchmark.flat_percent,
        run_name: &run_name,
        timeout: Duration::from_secs(distributed.agent_timeout_sec),
        poll_interval: Duration::from_millis(distributed.poll_interval_ms),
    })
    .await;
    let server_metrics = end_server_metrics(protocol, &target, metrics_session).await;
    let agent_reports = agent_result?;
    let server_metrics = server_metrics?;
    let mut merged_setup = BenchmarkSetup::new(protocol, target, workload, config);
    merged_setup.job_count = allocations
        .iter()
        .map(|allocation| allocation.job_count)
        .sum();
    let merged = merge_agent_reports(
        merged_setup,
        agent_reports.clone(),
        server_metrics,
        allocations,
        wall_start.elapsed(),
    );
    write_reports(data_dir, &run_name, &agent_reports, &merged)?;
    Ok(())
}

struct AgentDispatch<'a> {
    agents: &'a [DistributedAgentConfig],
    allocations: &'a [AgentJobAllocation],
    protocol: ServerProtocol,
    workload: WorkloadKind,
    target: &'a str,
    flat_percent: u8,
    run_name: &'a str,
    timeout: Duration,
    poll_interval: Duration,
}

async fn run_agents(dispatch: AgentDispatch<'_>) -> anyhow::Result<Vec<(String, BenchmarkReport)>> {
    let http = reqwest::Client::new();
    for allocation in dispatch
        .allocations
        .iter()
        .filter(|allocation| allocation.job_count > 0)
    {
        let agent = dispatch
            .agents
            .iter()
            .find(|agent| agent.id == allocation.agent_id)
            .ok_or_else(|| anyhow::anyhow!("agent `{}` missing", allocation.agent_id))?;
        let request = AgentRunRequest {
            run_id: format!("{}_{}", dispatch.run_name, agent.id),
            protocol: dispatch.protocol,
            workload: dispatch.workload,
            target: dispatch.target.to_owned(),
            job_count: allocation.job_count,
            flat_percent: dispatch.flat_percent,
        };
        http.post(format!("{}/runs", agent_url(agent)))
            .json(&request)
            .send()
            .await?
            .error_for_status()?
            .json::<AgentRunAccepted>()
            .await?;
    }

    let deadline = Instant::now() + dispatch.timeout;
    let mut reports = Vec::new();
    let mut remaining = dispatch
        .allocations
        .iter()
        .filter(|allocation| allocation.job_count > 0)
        .map(|allocation| allocation.agent_id.clone())
        .collect::<Vec<_>>();
    while !remaining.is_empty() {
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for distributed benchmark agents");
        }
        let mut next_remaining = Vec::new();
        for agent_id in remaining {
            let agent = dispatch
                .agents
                .iter()
                .find(|agent| agent.id == agent_id)
                .ok_or_else(|| anyhow::anyhow!("agent `{agent_id}` missing"))?;
            let status = http
                .get(format!(
                    "{}/runs/{}_{}",
                    agent_url(agent),
                    dispatch.run_name,
                    agent.id
                ))
                .send()
                .await?
                .error_for_status()?
                .json::<AgentRunStatus>()
                .await?;
            match status.status {
                AgentRunState::Succeeded => {
                    let report = status.report.ok_or_else(|| {
                        anyhow::anyhow!("agent `{}` succeeded without a report", agent.id)
                    })?;
                    reports.push((agent.id.clone(), report));
                }
                AgentRunState::Failed => {
                    anyhow::bail!(
                        "agent `{}` failed: {}",
                        agent.id,
                        status.error.unwrap_or_else(|| "unknown error".to_owned())
                    );
                }
                AgentRunState::Accepted | AgentRunState::Running => {
                    next_remaining.push(agent.id.clone());
                }
            }
        }
        remaining = next_remaining;
        if !remaining.is_empty() {
            tokio::time::sleep(dispatch.poll_interval).await;
        }
    }
    Ok(reports)
}

async fn start_server_metrics(
    protocol: ServerProtocol,
    target: &str,
    label: &str,
) -> anyhow::Result<String> {
    let label = Some(label.to_owned());
    match protocol {
        ServerProtocol::Rest => {
            let client = RestStorageApiClient::new(target)?;
            Ok(client
                .start_metrics_session(StartMetricsSessionRequest { label })
                .await?
                .metrics_session_id)
        }
        ServerProtocol::Grpc => {
            let client = GrpcStorageApiClient::connect(target.to_owned()).await?;
            Ok(client
                .start_metrics_session(StartMetricsSessionRequest { label })
                .await?
                .metrics_session_id)
        }
    }
}

async fn end_server_metrics(
    protocol: ServerProtocol,
    target: &str,
    metrics_session_id: String,
) -> anyhow::Result<ServerMetricsSessionReport> {
    match protocol {
        ServerProtocol::Rest => {
            let client = RestStorageApiClient::new(target)?;
            Ok(client
                .end_metrics_session(EndMetricsSessionRequest { metrics_session_id })
                .await?)
        }
        ServerProtocol::Grpc => {
            let client = GrpcStorageApiClient::connect(target.to_owned()).await?;
            Ok(client
                .end_metrics_session(EndMetricsSessionRequest { metrics_session_id })
                .await?)
        }
    }
}

fn write_reports(
    data_dir: &std::path::Path,
    run_name: &str,
    agent_reports: &[(String, BenchmarkReport)],
    merged: &BenchmarkReport,
) -> anyhow::Result<()> {
    let client_dir = data_dir.join("clients").join(run_name);
    std::fs::create_dir_all(&client_dir)?;
    for (agent_id, report) in agent_reports {
        std::fs::write(
            client_dir.join(format!("{agent_id}.json")),
            serde_json::to_vec_pretty(report)?,
        )?;
    }
    std::fs::create_dir_all(data_dir)?;
    let merged_path = data_dir.join(format!("{run_name}.json"));
    std::fs::write(merged_path, serde_json::to_vec_pretty(merged)?)?;
    Ok(())
}

fn agent_url(agent: &DistributedAgentConfig) -> String {
    agent.url.trim_end_matches('/').to_owned()
}

const fn protocol_name(protocol: ServerProtocol) -> &'static str {
    match protocol {
        ServerProtocol::Rest => "rest",
        ServerProtocol::Grpc => "grpc",
    }
}

const fn workload_name(workload: WorkloadKind) -> &'static str {
    match workload {
        WorkloadKind::Flat => "flat",
        WorkloadKind::Deep => "deep",
        WorkloadKind::Mixed => "mixed",
    }
}
