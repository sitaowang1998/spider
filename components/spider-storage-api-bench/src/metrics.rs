use serde::{Deserialize, Serialize};
use tabled::{Table, Tabled};

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

/// Renders the job latency summary as a console table.
#[must_use]
pub fn render_summary(summary: &JobLatencySummary) -> String {
    Table::new([summary]).to_string()
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
}
