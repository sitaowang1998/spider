use std::{collections::HashMap, str::FromStr, time::Duration};

use serde::{Deserialize, Serialize};
use spider_storage_api_bench::{
    metrics::{
        JobLatencySummary,
        RequestLatencySample,
        RequestLatencySummary,
        ServerMetricsSessionReport,
        summarize,
        summarize_requests,
    },
    server::ServerProtocol,
    workload::WorkloadKind,
};

use crate::{BenchmarkReport, BenchmarkSetup};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentRunRequest {
    pub(crate) run_id: String,
    pub(crate) role: AgentRole,
    pub(crate) protocol: ServerProtocol,
    pub(crate) workload: WorkloadKind,
    pub(crate) target: String,
    pub(crate) job_count: usize,
    pub(crate) flat_percent: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) session_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) resource_group_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) scheduler_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) scheduler_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) scheduler_trace_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) scheduler_trace_s3_uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentRunAccepted {
    pub(crate) run_id: String,
    pub(crate) status: AgentRunState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRunStatus {
    pub(crate) run_id: String,
    pub(crate) agent_id: String,
    pub(crate) status: AgentRunState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) report: Option<BenchmarkReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRunState {
    Accepted,
    Running,
    Stopping,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRole {
    Scheduler,
    Submitter,
    Worker,
}

impl FromStr for AgentRole {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "scheduler" => Ok(Self::Scheduler),
            "submitter" => Ok(Self::Submitter),
            "worker" => Ok(Self::Worker),
            _ => anyhow::bail!("agent role must be scheduler, submitter, or worker"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedReport {
    pub(crate) agent_count: usize,
    pub(crate) agents: Vec<String>,
    pub(crate) job_allocation: Vec<AgentJobAllocation>,
    pub(crate) controller_wall_time_us: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentJobAllocation {
    pub(crate) agent_id: String,
    pub(crate) job_count: usize,
}

#[cfg(test)]
fn allocate_jobs(
    total_jobs: usize,
    agent_ids: &[String],
) -> anyhow::Result<Vec<AgentJobAllocation>> {
    if agent_ids.is_empty() {
        anyhow::bail!("at least one distributed agent is required");
    }
    let base = total_jobs / agent_ids.len();
    let remainder = total_jobs % agent_ids.len();
    Ok(agent_ids
        .iter()
        .enumerate()
        .map(|(index, agent_id)| AgentJobAllocation {
            agent_id: agent_id.clone(),
            job_count: base + usize::from(index < remainder),
        })
        .collect())
}

pub fn merge_agent_reports(
    setup: BenchmarkSetup,
    agent_reports: Vec<(String, BenchmarkReport)>,
    server_metrics: ServerMetricsSessionReport,
    job_allocation: Vec<AgentJobAllocation>,
    controller_wall_time: Duration,
    controller_request_samples: &[RequestLatencySample],
) -> BenchmarkReport {
    let mut job_samples = Vec::new();
    let mut worker_activity_samples = Vec::new();
    let mut scheduler_metrics = Vec::new();
    let mut scheduler_queues = Vec::new();

    for (_, report) in &agent_reports {
        job_samples.extend(report.job_latency_samples.iter().cloned());
        worker_activity_samples.extend(report.worker_activity_samples.iter().cloned());
        scheduler_metrics.extend(report.scheduler_metrics.iter().cloned());
        if let Some(scheduler_queue) = &report.scheduler_queue {
            scheduler_queues.push(scheduler_queue.clone());
        }
    }

    let job_latency = if job_samples.is_empty() {
        merge_job_summaries(agent_reports.iter().map(|(_, report)| &report.job_latency))
    } else {
        summarize(&job_samples)
    };
    let request_latency = merge_request_summaries(
        summarize_requests(controller_request_samples)
            .into_iter()
            .chain(
                agent_reports
                    .iter()
                    .flat_map(|(_, report)| report.request_latency.iter().cloned()),
            ),
    );
    let agents = agent_reports
        .into_iter()
        .map(|(agent_id, _)| agent_id)
        .collect::<Vec<_>>();

    BenchmarkReport {
        setup,
        job_latency,
        request_latency,
        server_metrics,
        scheduler_metrics: merge_request_summaries(scheduler_metrics.into_iter()),
        scheduler_queue: merge_scheduler_queue_summaries(&scheduler_queues),
        job_latency_samples: job_samples,
        request_latency_samples: Vec::new(),
        worker_activity_samples,
        distributed: Some(DistributedReport {
            agent_count: agents.len(),
            agents,
            job_allocation,
            controller_wall_time_us: controller_wall_time.as_micros(),
        }),
    }
}

fn merge_job_summaries<'a>(
    summaries: impl Iterator<Item = &'a JobLatencySummary>,
) -> JobLatencySummary {
    let mut count = 0;
    let mut failed_jobs = 0;
    let mut max_us = 0;
    for summary in summaries {
        count += summary.count;
        failed_jobs += summary.failed_jobs;
        max_us = max_us.max(summary.max_us);
    }
    JobLatencySummary {
        count,
        failed_jobs,
        avg_us: 0,
        p50_us: 0,
        p90_us: 0,
        p99_us: 0,
        max_us,
    }
}

fn merge_request_summaries(
    summaries: impl Iterator<Item = RequestLatencySummary>,
) -> Vec<RequestLatencySummary> {
    let mut by_key: HashMap<(String, String), Vec<RequestLatencySummary>> = HashMap::new();
    for summary in summaries {
        by_key
            .entry((summary.category.clone(), summary.operation.clone()))
            .or_default()
            .push(summary);
    }

    let mut merged = by_key
        .into_iter()
        .map(|((category, operation), rows)| {
            let count: usize = rows.iter().map(|row| row.count).sum();
            let errors: usize = rows.iter().map(|row| row.errors).sum();
            let weighted_avg_total: u128 =
                rows.iter().map(|row| row.avg_us * row.count as u128).sum();
            RequestLatencySummary {
                category,
                operation,
                count,
                errors,
                avg_us: if count == 0 {
                    0
                } else {
                    weighted_avg_total / count as u128
                },
                p50_us: weighted_percentile_like(&rows, count, |row| row.p50_us),
                p90_us: weighted_percentile_like(&rows, count, |row| row.p90_us),
                p99_us: weighted_percentile_like(&rows, count, |row| row.p99_us),
                max_us: rows.iter().map(|row| row.max_us).max().unwrap_or(0),
            }
        })
        .collect::<Vec<_>>();
    merged.sort_by(|left, right| {
        (left.category.as_str(), left.operation.as_str())
            .cmp(&(right.category.as_str(), right.operation.as_str()))
    });
    merged
}

fn merge_scheduler_queue_summaries(
    summaries: &[crate::SchedulerQueueSummary],
) -> Option<crate::SchedulerQueueSummary> {
    if summaries.is_empty() {
        return None;
    }
    let worker_poll_count = summaries
        .iter()
        .map(|summary| summary.worker_poll_count)
        .sum::<u64>();
    let total_queue_depth = summaries
        .iter()
        .map(|summary| summary.avg_queue_depth * u64_as_f64(summary.worker_poll_count))
        .sum::<f64>();
    let total_queue_wait_us = summaries
        .iter()
        .map(|summary| summary.avg_queue_wait_us * u64_as_f64(summary.worker_poll_count))
        .sum::<f64>();
    Some(crate::SchedulerQueueSummary {
        worker_poll_limit: summaries
            .iter()
            .map(|summary| summary.worker_poll_limit)
            .max()
            .unwrap_or(0),
        worker_poll_count,
        max_queue_depth: summaries
            .iter()
            .map(|summary| summary.max_queue_depth)
            .max()
            .unwrap_or(0),
        avg_queue_depth: if worker_poll_count == 0 {
            0.0
        } else {
            total_queue_depth / u64_as_f64(worker_poll_count)
        },
        max_queue_wait_us: summaries
            .iter()
            .map(|summary| summary.max_queue_wait_us)
            .max()
            .unwrap_or(0),
        avg_queue_wait_us: if worker_poll_count == 0 {
            0.0
        } else {
            total_queue_wait_us / u64_as_f64(worker_poll_count)
        },
    })
}

#[allow(clippy::cast_precision_loss)]
const fn u64_as_f64(value: u64) -> f64 {
    value as f64
}

fn weighted_percentile_like(
    rows: &[RequestLatencySummary],
    count: usize,
    value: fn(&RequestLatencySummary) -> u128,
) -> u128 {
    if count == 0 {
        return 0;
    }
    rows.iter()
        .map(|row| value(row) * row.count as u128)
        .sum::<u128>()
        / count as u128
}

#[cfg(test)]
mod tests {
    use spider_storage_api_bench::metrics::{
        JobLatencySummary,
        RequestLatencySummary,
        ServerMetricsSessionReport,
    };

    use super::{allocate_jobs, merge_agent_reports, merge_request_summaries};
    use crate::{BenchmarkReport, BenchmarkSetup};

    #[test]
    fn allocate_jobs_spreads_remainder() -> anyhow::Result<()> {
        let allocations = allocate_jobs(10, &["a".to_owned(), "b".to_owned(), "c".to_owned()])?;
        assert_eq!(4, allocations[0].job_count);
        assert_eq!(3, allocations[1].job_count);
        assert_eq!(3, allocations[2].job_count);
        Ok(())
    }

    #[test]
    fn allocate_jobs_rejects_zero_agents() {
        let result = allocate_jobs(10, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn merge_request_summaries_uses_weighted_average() {
        let rows = merge_request_summaries(
            vec![
                RequestLatencySummary {
                    category: "non_blocking".to_owned(),
                    operation: "register_job".to_owned(),
                    count: 2,
                    errors: 0,
                    avg_us: 10,
                    p50_us: 10,
                    p90_us: 20,
                    p99_us: 30,
                    max_us: 40,
                },
                RequestLatencySummary {
                    category: "non_blocking".to_owned(),
                    operation: "register_job".to_owned(),
                    count: 6,
                    errors: 1,
                    avg_us: 30,
                    p50_us: 30,
                    p90_us: 40,
                    p99_us: 50,
                    max_us: 60,
                },
            ]
            .into_iter(),
        );
        assert_eq!(1, rows.len());
        assert_eq!(8, rows[0].count);
        assert_eq!(25, rows[0].avg_us);
        assert_eq!(1, rows[0].errors);
        assert_eq!(60, rows[0].max_us);
    }

    #[test]
    fn merge_agent_reports_includes_scheduler_metrics() {
        let setup = BenchmarkSetup {
            protocol: "Rest".to_owned(),
            target: "http://127.0.0.1:8080".to_owned(),
            workload: spider_storage_api_bench::workload::WorkloadKind::Flat,
            flat_percent: 100,
            task_count: 1,
            job_count: 1,
            payload_bytes: 0,
            task_sleep_ms: 0,
            client_count: 1,
            worker_count: 1,
            channel_count: 1,
            worker_poll_wait_ms: 1,
            job_poll_wait_ms: 1,
            scheduler_poll_batch: 1,
            scheduler_refill_interval_ms: 1,
            scheduler_poll_wait_ms: 1,
            scheduler_worker_poll_concurrency: 1,
            database_host: "127.0.0.1".to_owned(),
            database_port: 3306,
            database_name: "spider".to_owned(),
            database_username: "spider".to_owned(),
            database_max_connections: 1,
        };
        let report = BenchmarkReport {
            setup: setup.clone(),
            job_latency: JobLatencySummary::default(),
            request_latency: Vec::new(),
            server_metrics: ServerMetricsSessionReport {
                metrics_session_id: String::new(),
                label: None,
                elapsed_micros: 0,
                request_latency: Vec::new(),
                low_count_request_latency: Vec::new(),
                request_sizes: Vec::new(),
                job_execution_latency: JobLatencySummary::default(),
            },
            scheduler_metrics: vec![RequestLatencySummary {
                category: "blocking".to_owned(),
                operation: "worker_poll_ready_tasks".to_owned(),
                count: 2,
                errors: 0,
                avg_us: 10,
                p50_us: 10,
                p90_us: 10,
                p99_us: 10,
                max_us: 10,
            }],
            scheduler_queue: Some(crate::SchedulerQueueSummary {
                worker_poll_limit: 1,
                worker_poll_count: 2,
                max_queue_depth: 1,
                avg_queue_depth: 0.5,
                max_queue_wait_us: 20,
                avg_queue_wait_us: 10.0,
            }),
            job_latency_samples: Vec::new(),
            request_latency_samples: Vec::new(),
            worker_activity_samples: Vec::new(),
            distributed: None,
        };

        let merged = merge_agent_reports(
            setup,
            vec![("scheduler".to_owned(), report)],
            ServerMetricsSessionReport {
                metrics_session_id: String::new(),
                label: None,
                elapsed_micros: 0,
                request_latency: Vec::new(),
                low_count_request_latency: Vec::new(),
                request_sizes: Vec::new(),
                job_execution_latency: JobLatencySummary::default(),
            },
            Vec::new(),
            std::time::Duration::from_secs(1),
            &[],
        );

        assert_eq!(1, merged.scheduler_metrics.len());
        assert_eq!(
            "worker_poll_ready_tasks",
            merged.scheduler_metrics[0].operation
        );
        assert_eq!(10, merged.scheduler_metrics[0].avg_us);
        let scheduler_queue = merged
            .scheduler_queue
            .expect("merged scheduler queue summary should be present");
        assert_eq!(2, scheduler_queue.worker_poll_count);
        assert_eq!(1, scheduler_queue.max_queue_depth);
        assert!((scheduler_queue.avg_queue_wait_us - 10.0).abs() < f64::EPSILON);
    }
}
