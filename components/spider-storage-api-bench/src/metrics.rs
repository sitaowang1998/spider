use serde::{Deserialize, Serialize};
use tabled::{Table, Tabled};

/// Whether a storage request can wait inside the storage service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestCategory {
    NonBlocking,
    Blocking,
}

impl RequestCategory {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NonBlocking => "non_blocking",
            Self::Blocking => "blocking",
        }
    }
}

/// Completed latency observation for one storage request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestLatencySample {
    pub operation: &'static str,
    pub category: &'static str,
    pub latency_micros: u128,
    pub succeeded: bool,
}

impl RequestLatencySample {
    #[must_use]
    pub const fn success(
        operation: &'static str,
        category: RequestCategory,
        latency: std::time::Duration,
    ) -> Self {
        Self {
            operation,
            category: category.as_str(),
            latency_micros: latency.as_micros(),
            succeeded: true,
        }
    }

    #[must_use]
    pub const fn failure(
        operation: &'static str,
        category: RequestCategory,
        latency: std::time::Duration,
    ) -> Self {
        Self {
            operation,
            category: category.as_str(),
            latency_micros: latency.as_micros(),
            succeeded: false,
        }
    }
}

/// Completed end-to-end latency observation for one benchmark job.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobLatencySample {
    pub latency_micros: u128,
    pub succeeded: bool,
}

impl JobLatencySample {
    #[must_use]
    pub const fn success(latency: std::time::Duration) -> Self {
        Self {
            latency_micros: latency.as_micros(),
            succeeded: true,
        }
    }

    #[must_use]
    pub const fn failure(latency: std::time::Duration) -> Self {
        Self {
            latency_micros: latency.as_micros(),
            succeeded: false,
        }
    }
}

/// End-to-end latency summary printed to the console.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Tabled)]
pub struct JobLatencySummary {
    pub count: usize,
    pub failed_jobs: usize,
    pub p50_us: u128,
    pub p90_us: u128,
    pub p99_us: u128,
    pub max_us: u128,
}

/// Storage request latency summary printed to the console.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Tabled)]
pub struct RequestLatencySummary {
    pub category: String,
    pub operation: String,
    pub count: usize,
    pub errors: usize,
    pub p50_us: u128,
    pub p90_us: u128,
    pub p99_us: u128,
    pub max_us: u128,
}

/// Aggregates job latency samples into a summary row.
#[must_use]
pub fn summarize(samples: &[JobLatencySample]) -> JobLatencySummary {
    let mut latencies: Vec<u128> = samples.iter().map(|sample| sample.latency_micros).collect();
    latencies.sort_unstable();
    JobLatencySummary {
        count: latencies.len(),
        failed_jobs: samples.iter().filter(|sample| !sample.succeeded).count(),
        p50_us: percentile(&latencies, 50),
        p90_us: percentile(&latencies, 90),
        p99_us: percentile(&latencies, 99),
        max_us: latencies.last().copied().unwrap_or(0),
    }
}

/// Aggregates storage request latency samples into table rows.
#[must_use]
pub fn summarize_requests(samples: &[RequestLatencySample]) -> Vec<RequestLatencySummary> {
    let mut groups: Vec<(&str, &str)> = samples
        .iter()
        .map(|sample| (sample.category, sample.operation))
        .collect();
    groups.sort_unstable();
    groups.dedup();

    groups
        .into_iter()
        .map(|(category, operation)| {
            let mut latencies: Vec<u128> = samples
                .iter()
                .filter(|sample| sample.category == category && sample.operation == operation)
                .map(|sample| sample.latency_micros)
                .collect();
            latencies.sort_unstable();
            RequestLatencySummary {
                category: category.to_owned(),
                operation: operation.to_owned(),
                count: latencies.len(),
                errors: samples
                    .iter()
                    .filter(|sample| {
                        sample.category == category
                            && sample.operation == operation
                            && !sample.succeeded
                    })
                    .count(),
                p50_us: percentile(&latencies, 50),
                p90_us: percentile(&latencies, 90),
                p99_us: percentile(&latencies, 99),
                max_us: latencies.last().copied().unwrap_or(0),
            }
        })
        .collect()
}

/// Renders the job latency summary as a console table.
#[must_use]
pub fn render_summary(summary: &JobLatencySummary) -> String {
    Table::new([summary]).to_string()
}

/// Renders request latency summary rows as a console table.
#[must_use]
pub fn render_request_summary(rows: &[RequestLatencySummary]) -> String {
    Table::new(rows).to_string()
}

fn percentile(sorted_values: &[u128], percentile_value: usize) -> u128 {
    if sorted_values.is_empty() {
        return 0;
    }
    let index = ((sorted_values.len() - 1) * percentile_value).div_ceil(100);
    sorted_values[index]
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{JobLatencySample, summarize};

    #[test]
    fn summarize_computes_job_latency_percentiles_and_failures() {
        let samples = vec![
            JobLatencySample::success(Duration::from_micros(10)),
            JobLatencySample::success(Duration::from_micros(20)),
            JobLatencySample::failure(Duration::from_micros(100)),
        ];
        let summary = summarize(&samples);
        assert_eq!(3, summary.count);
        assert_eq!(1, summary.failed_jobs);
        assert_eq!(20, summary.p50_us);
        assert_eq!(100, summary.p90_us);
        assert_eq!(100, summary.p99_us);
    }

    #[test]
    fn summarize_requests_groups_by_operation_and_category() {
        let samples = vec![
            super::RequestLatencySample::success(
                "register_job",
                super::RequestCategory::NonBlocking,
                Duration::from_micros(10),
            ),
            super::RequestLatencySample::failure(
                "register_job",
                super::RequestCategory::NonBlocking,
                Duration::from_micros(20),
            ),
            super::RequestLatencySample::success(
                "poll_ready_tasks",
                super::RequestCategory::Blocking,
                Duration::from_micros(100),
            ),
        ];
        let rows = super::summarize_requests(&samples);
        assert_eq!(2, rows.len());
        assert_eq!("blocking", rows[0].category);
        assert_eq!("poll_ready_tasks", rows[0].operation);
        assert_eq!(1, rows[0].count);
        assert_eq!(0, rows[0].errors);
        assert_eq!("non_blocking", rows[1].category);
        assert_eq!("register_job", rows[1].operation);
        assert_eq!(2, rows[1].count);
        assert_eq!(1, rows[1].errors);
    }
}
