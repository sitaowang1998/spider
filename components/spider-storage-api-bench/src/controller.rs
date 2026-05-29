use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
        AgentRole,
        AgentRunAccepted,
        AgentRunRequest,
        AgentRunState,
        AgentRunStatus,
        merge_agent_reports,
    },
    prepare_workload,
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

    run_controller_workload(
        &config,
        distributed.scheduler.as_ref(),
        &distributed.submitter,
        &distributed.workers,
        args.protocol,
        args.workload,
        &args.data_dir,
    )
    .await?;
    Ok(())
}

async fn run_controller_workload(
    config: &BenchConfig,
    scheduler: Option<&DistributedAgentConfig>,
    submitter: &DistributedAgentConfig,
    workers: &[DistributedAgentConfig],
    protocol: ServerProtocol,
    workload: WorkloadKind,
    data_dir: &std::path::Path,
) -> anyhow::Result<()> {
    let target = match protocol {
        ServerProtocol::Rest => config.server.rest_target.clone(),
        ServerProtocol::Grpc => config.server.grpc_target.clone(),
    };
    let allocations = scheduler
        .into_iter()
        .map(|agent| AgentJobAllocation {
            agent_id: agent.id.clone(),
            job_count: 0,
        })
        .chain(std::iter::once(AgentJobAllocation {
            agent_id: submitter.id.clone(),
            job_count: config.benchmark.job_count,
        }))
        .chain(workers.iter().map(|worker| AgentJobAllocation {
            agent_id: worker.id.clone(),
            job_count: 0,
        }))
        .collect::<Vec<_>>();
    let run_name = format!("{}_{}", protocol_name(protocol), workload_name(workload));
    log_controller_event(format_args!(
        "benchmark_start protocol={} workload={} jobs={} workers={}",
        protocol_name(protocol),
        workload_name(workload),
        config.benchmark.job_count,
        workers.len(),
    ));
    let metrics_session = start_server_metrics(protocol, &target, &run_name).await?;
    let wall_start = Instant::now();
    let distributed = config
        .distributed
        .as_ref()
        .expect("distributed config should exist");
    let prepared = prepare_distributed_workload(protocol, &target).await;
    let agent_result = async {
        let prepared = prepared?;
        run_agents(AgentDispatch {
            scheduler,
            submitter,
            workers,
            protocol,
            workload,
            target: &target,
            job_count: config.benchmark.job_count,
            task_count: config.benchmark.task_count,
            flat_percent: config.benchmark.flat_percent,
            run_name: &run_name,
            timeout: Duration::from_secs(distributed.agent_timeout_sec),
            poll_interval: Duration::from_millis(distributed.poll_interval_ms),
            session_id: prepared.session_id,
            resource_group_id: &prepared.resource_group_id,
        })
        .await
        .map(|reports| (reports, prepared.request_latency))
    }
    .await;
    let server_metrics = end_server_metrics(protocol, &target, metrics_session).await;
    let (agent_reports, controller_request_samples) = agent_result?;
    let server_metrics = server_metrics?;
    let mut merged_setup = BenchmarkSetup::new(protocol, target, workload, config);
    merged_setup.job_count = config.benchmark.job_count;
    let merged = merge_agent_reports(
        merged_setup,
        agent_reports.clone(),
        server_metrics,
        allocations,
        wall_start.elapsed(),
        controller_request_samples,
    );
    write_reports(data_dir, &run_name, &agent_reports, &merged)?;
    log_controller_event(format_args!(
        "benchmark_complete protocol={} workload={} elapsed_sec={:.3}",
        protocol_name(protocol),
        workload_name(workload),
        wall_start.elapsed().as_secs_f64(),
    ));
    Ok(())
}

fn log_controller_event(args: std::fmt::Arguments<'_>) {
    println!("[controller] {} {args}", unix_epoch_millis());
}

fn unix_epoch_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

struct AgentDispatch<'a> {
    scheduler: Option<&'a DistributedAgentConfig>,
    submitter: &'a DistributedAgentConfig,
    workers: &'a [DistributedAgentConfig],
    protocol: ServerProtocol,
    workload: WorkloadKind,
    target: &'a str,
    job_count: usize,
    task_count: usize,
    flat_percent: u8,
    run_name: &'a str,
    timeout: Duration,
    poll_interval: Duration,
    session_id: u64,
    resource_group_id: &'a str,
}

async fn run_agents(dispatch: AgentDispatch<'_>) -> anyhow::Result<Vec<(String, BenchmarkReport)>> {
    let http = reqwest::Client::new();
    let scheduler_run_id = start_scheduler_agent(&http, &dispatch).await?;
    for worker in dispatch.workers {
        start_worker_agent(&http, &dispatch, worker, scheduler_run_id.clone()).await?;
    }
    let submitter_run_id = start_submitter_agent(&http, &dispatch).await?;

    let deadline = Instant::now() + dispatch.timeout;
    let submitter_report = match wait_for_agent(
        &http,
        dispatch.submitter,
        &submitter_run_id,
        deadline,
        dispatch.poll_interval,
    )
    .await
    {
        Ok(report) => {
            validate_submitter_report(&report, dispatch.job_count)?;
            log_controller_event(format_args!(
                "submitter_complete jobs={} failed_jobs={}",
                report.job_latency.count, report.job_latency.failed_jobs,
            ));
            report
        }
        Err(err) => {
            stop_workers(&http, dispatch.workers, dispatch.run_name).await;
            stop_scheduler(&http, dispatch.scheduler, dispatch.run_name).await;
            return Err(err);
        }
    };
    stop_workers(&http, dispatch.workers, dispatch.run_name).await;
    let mut reports = Vec::new();
    reports.push((dispatch.submitter.id.clone(), submitter_report));
    for worker in dispatch.workers {
        let worker_run_id = format!("{}_{}", dispatch.run_name, worker.id);
        let report = wait_for_agent(
            &http,
            worker,
            &worker_run_id,
            deadline,
            dispatch.poll_interval,
        )
        .await?;
        reports.push((worker.id.clone(), report));
    }
    let worker_task_count = worker_task_count(&reports);
    let expected_task_count = dispatch.job_count * dispatch.task_count;
    if worker_task_count != expected_task_count {
        anyhow::bail!(
            "worker task execution count mismatch: expected {expected_task_count}, got \
             {worker_task_count}"
        );
    }
    log_controller_event(format_args!(
        "workers_complete tasks_executed={worker_task_count}"
    ));
    if let Some(scheduler) = dispatch.scheduler {
        stop_scheduler(&http, dispatch.scheduler, dispatch.run_name).await;
        let scheduler_run_id = format!("{}_{}", dispatch.run_name, scheduler.id);
        let report = wait_for_agent(
            &http,
            scheduler,
            &scheduler_run_id,
            deadline,
            dispatch.poll_interval,
        )
        .await?;
        reports.push((scheduler.id.clone(), report));
    }
    Ok(reports)
}

fn validate_submitter_report(
    report: &BenchmarkReport,
    expected_job_count: usize,
) -> anyhow::Result<()> {
    if report.job_latency.count != expected_job_count {
        anyhow::bail!(
            "submitter job count mismatch: expected {}, got {}",
            expected_job_count,
            report.job_latency.count
        );
    }
    if report.job_latency.failed_jobs != 0 {
        anyhow::bail!(
            "submitter reported {} failed jobs",
            report.job_latency.failed_jobs
        );
    }
    Ok(())
}

fn worker_task_count(reports: &[(String, BenchmarkReport)]) -> usize {
    reports
        .iter()
        .flat_map(|(_, report)| &report.worker_activity_samples)
        .map(|sample| sample.task_count)
        .sum()
}

async fn start_scheduler_agent(
    http: &reqwest::Client,
    dispatch: &AgentDispatch<'_>,
) -> anyhow::Result<Option<String>> {
    let Some(scheduler) = dispatch.scheduler else {
        return Ok(None);
    };
    let run_id = format!("{}_{}", dispatch.run_name, scheduler.id);
    let request = AgentRunRequest {
        run_id: run_id.clone(),
        role: AgentRole::Scheduler,
        protocol: dispatch.protocol,
        workload: dispatch.workload,
        target: dispatch.target.to_owned(),
        job_count: dispatch.job_count,
        flat_percent: dispatch.flat_percent,
        session_id: Some(dispatch.session_id),
        resource_group_id: None,
        scheduler_url: None,
        scheduler_run_id: None,
    };
    post_agent_run(http, scheduler, &request).await?;
    Ok(Some(run_id))
}

async fn start_worker_agent(
    http: &reqwest::Client,
    dispatch: &AgentDispatch<'_>,
    worker: &DistributedAgentConfig,
    scheduler_run_id: Option<String>,
) -> anyhow::Result<()> {
    let request = AgentRunRequest {
        run_id: format!("{}_{}", dispatch.run_name, worker.id),
        role: AgentRole::Worker,
        protocol: dispatch.protocol,
        workload: dispatch.workload,
        target: dispatch.target.to_owned(),
        job_count: dispatch.job_count,
        flat_percent: dispatch.flat_percent,
        session_id: Some(dispatch.session_id),
        resource_group_id: None,
        scheduler_url: dispatch.scheduler.map(agent_url),
        scheduler_run_id,
    };
    post_agent_run(http, worker, &request).await
}

async fn start_submitter_agent(
    http: &reqwest::Client,
    dispatch: &AgentDispatch<'_>,
) -> anyhow::Result<String> {
    let run_id = format!("{}_{}", dispatch.run_name, dispatch.submitter.id);
    let request = AgentRunRequest {
        run_id: run_id.clone(),
        role: AgentRole::Submitter,
        protocol: dispatch.protocol,
        workload: dispatch.workload,
        target: dispatch.target.to_owned(),
        job_count: dispatch.job_count,
        flat_percent: dispatch.flat_percent,
        session_id: None,
        resource_group_id: Some(dispatch.resource_group_id.to_owned()),
        scheduler_url: None,
        scheduler_run_id: None,
    };
    post_agent_run(http, dispatch.submitter, &request).await?;
    Ok(run_id)
}

async fn post_agent_run(
    http: &reqwest::Client,
    agent: &DistributedAgentConfig,
    request: &AgentRunRequest,
) -> anyhow::Result<()> {
    http.post(format!("{}/runs", agent_url(agent)))
        .json(request)
        .send()
        .await?
        .error_for_status()?
        .json::<AgentRunAccepted>()
        .await?;
    Ok(())
}

async fn wait_for_agent(
    http: &reqwest::Client,
    agent: &DistributedAgentConfig,
    run_id: &str,
    deadline: Instant,
    poll_interval: Duration,
) -> anyhow::Result<BenchmarkReport> {
    loop {
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for distributed benchmark agent `{}`",
                agent.id
            );
        }
        let status = http
            .get(format!("{}/runs/{run_id}", agent_url(agent)))
            .send()
            .await?
            .error_for_status()?
            .json::<AgentRunStatus>()
            .await?;
        match status.status {
            AgentRunState::Succeeded => {
                return status.report.ok_or_else(|| {
                    anyhow::anyhow!("agent `{}` succeeded without a report", agent.id)
                });
            }
            AgentRunState::Failed => {
                anyhow::bail!(
                    "agent `{}` failed: {}",
                    agent.id,
                    status.error.unwrap_or_else(|| "unknown error".to_owned())
                );
            }
            AgentRunState::Accepted | AgentRunState::Running | AgentRunState::Stopping => {
                tokio::time::sleep(poll_interval).await;
            }
        }
    }
}

async fn stop_agent(
    http: &reqwest::Client,
    agent: &DistributedAgentConfig,
    run_name: &str,
) -> anyhow::Result<()> {
    http.post(format!(
        "{}/runs/{}_{}/stop",
        agent_url(agent),
        run_name,
        agent.id
    ))
    .send()
    .await?
    .error_for_status()?;
    Ok(())
}

async fn stop_workers(http: &reqwest::Client, workers: &[DistributedAgentConfig], run_name: &str) {
    for worker in workers {
        if let Err(err) = stop_agent(http, worker, run_name).await {
            tracing::warn!("failed to stop worker agent `{}`: {err}", worker.id);
        }
    }
}

async fn stop_scheduler(
    http: &reqwest::Client,
    scheduler: Option<&DistributedAgentConfig>,
    run_name: &str,
) {
    let Some(scheduler) = scheduler else {
        return;
    };
    if let Err(err) = stop_agent(http, scheduler, run_name).await {
        tracing::warn!("failed to stop scheduler agent `{}`: {err}", scheduler.id);
    }
}

async fn prepare_distributed_workload(
    protocol: ServerProtocol,
    target: &str,
) -> anyhow::Result<crate::PreparedWorkload> {
    match protocol {
        ServerProtocol::Rest => {
            let client = RestStorageApiClient::new(target)?;
            prepare_workload(&client).await
        }
        ServerProtocol::Grpc => {
            let client = GrpcStorageApiClient::connect(target.to_owned()).await?;
            prepare_workload(&client).await
        }
    }
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
