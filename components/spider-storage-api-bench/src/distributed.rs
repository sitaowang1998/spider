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
    Submitter,
    Worker,
}

impl FromStr for AgentRole {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "submitter" => Ok(Self::Submitter),
            "worker" => Ok(Self::Worker),
            _ => anyhow::bail!("agent role must be submitter or worker"),
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
    controller_request_samples: Vec<RequestLatencySample>,
) -> BenchmarkReport {
    let mut job_samples = Vec::new();
    let mut request_samples = controller_request_samples;
    let mut worker_activity_samples = Vec::new();

    for (_, report) in &agent_reports {
        job_samples.extend(report.job_latency_samples.iter().cloned());
        request_samples.extend(report.request_latency_samples.iter().cloned());
        worker_activity_samples.extend(report.worker_activity_samples.iter().cloned());
    }

    let job_latency = if job_samples.is_empty() {
        merge_job_summaries(agent_reports.iter().map(|(_, report)| &report.job_latency))
    } else {
        summarize(&job_samples)
    };
    let request_latency = if request_samples.is_empty() {
        merge_request_summaries(
            agent_reports
                .iter()
                .flat_map(|(_, report)| report.request_latency.iter().cloned()),
        )
    } else {
        summarize_requests(&request_samples)
    };
    let agents = agent_reports
        .into_iter()
        .map(|(agent_id, _)| agent_id)
        .collect::<Vec<_>>();

    BenchmarkReport {
        setup,
        job_latency,
        request_latency,
        server_metrics,
        job_latency_samples: job_samples,
        request_latency_samples: request_samples,
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
    use spider_storage_api_bench::metrics::RequestLatencySummary;

    use super::{allocate_jobs, merge_request_summaries};

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
}
