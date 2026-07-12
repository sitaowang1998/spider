//! End-to-end fault-recovery tests: a layered `neuron::dense_*` task graph must still complete and
//! match the in-process simulation when an execution-manager worker (one, several, or all) or the
//! scheduler is killed mid-job and restarted.

use std::time::Duration;

use anyhow::bail;
use e2e::JobSubmission;
use e2e::SpiderTestDriver;
use e2e::TerminationResult;
use e2e::decode_output;
use e2e::encode_input;
use e2e::fault::ComposeFaultController;
use e2e::nn::NeuralNetwork;
use e2e::nn::Neuron;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

/// Relative-tolerance float comparison (matches `tests/nn.rs`).
const REL_TOL: f64 = 1.0e-12;

/// Number of layers in the test network (matches `tests/nn.rs`).
const NUM_LAYERS: usize = 10;

/// Neurons per layer in the test network (matches `tests/nn.rs`).
const LAYER_SIZE: usize = 1000;

/// Seconds to let the job run before killing any service, so tasks have dispatched across the
/// workers.
const PRE_FAILURE_DELAY_SEC: u64 = 3;

/// Seconds the targeted service stays down. For a worker failure this must exceed the storage's
/// `task_instance_pool_config.execution_manager_stale_cutoff_sec` (10) plus `gc_interval_sec` (2)
/// so the storage garbage-collects the dead execution manager and reassigns its in-flight tasks
/// before the worker restarts. For a scheduler failure it must stay under the scheduler's
/// `em_registry.dead_em_cutoff_sec` (default 60) so the still-running execution managers are not
/// marked dead on restart. 15 satisfies both (12 < 15 < 60). The down window starts after the last
/// `stop` returns, so every targeted service is down for at least this long.
const OUTAGE_DURATION_SEC: u64 = 15;

/// Distinct resource-group ids keep each scenario's jobs isolated in the shared persistent
/// database (`add_resource_group` returns `ALREADY_EXISTS` for a duplicate external id).
const RESOURCE_GROUP_SINGLE: &str = "e2e-fault-single";
const RESOURCE_GROUP_MULTI: &str = "e2e-fault-multi";
const RESOURCE_GROUP_ALL: &str = "e2e-fault-all";
const RESOURCE_GROUP_SCHEDULER: &str = "e2e-fault-scheduler";

#[tokio::test]
#[serial_test::file_serial(compose_fault)]
async fn test_single_worker_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(RESOURCE_GROUP_SINGLE, inject_single_worker_failure).await
}

#[tokio::test]
#[serial_test::file_serial(compose_fault)]
async fn test_multiple_worker_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(RESOURCE_GROUP_MULTI, inject_multiple_worker_failure).await
}

#[tokio::test]
#[serial_test::file_serial(compose_fault)]
async fn test_all_worker_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(RESOURCE_GROUP_ALL, inject_all_worker_failure).await
}

#[tokio::test]
#[serial_test::file_serial(compose_fault)]
async fn test_scheduler_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(RESOURCE_GROUP_SCHEDULER, inject_scheduler_failure).await
}

/// Runs the standard NN job, injects a failure mid-job via `inject_failure`, and asserts the job
/// still completes with outputs matching the in-process simulation.
async fn run_fault_recovery_scenario<InjectFailure>(
    resource_group_id: &'static str,
    inject_failure: InjectFailure,
) -> anyhow::Result<()>
where
    InjectFailure: AsyncFnOnce(&ComposeFaultController) -> anyhow::Result<()>, {
    if std::env::var("SPIDER_ENDPOINT").is_err() {
        bail!("SPIDER_ENDPOINT is not set");
    }
    let controller = ComposeFaultController::from_env()?;
    let (job, expected) = build_nn_fault_job(resource_group_id)?;

    SpiderTestDriver::run_exclusive(
        job,
        Duration::from_secs(300),
        async move |_job_id| inject_failure(&controller).await,
        async move |_job_id, result| check_nn_outputs(result, &expected),
    )
    .await?;

    Ok(())
}

/// Failure injection: kills `worker-1` mid-job, holds it down past the storage's stale-cutoff + gc
/// interval so its execution manager is garbage-collected, then restarts it. The three surviving
/// workers pick up its reassigned in-flight tasks during the outage.
async fn inject_single_worker_failure(controller: &ComposeFaultController) -> anyhow::Result<()> {
    kill_then_restart(controller, &["worker-1"]).await
}

/// Failure injection: kills `worker-1` and `worker-2` mid-job, waits out the gc window, then
/// restarts them. The two surviving workers pick up the reassigned in-flight tasks.
async fn inject_multiple_worker_failure(controller: &ComposeFaultController) -> anyhow::Result<()> {
    kill_then_restart(controller, &["worker-1", "worker-2"]).await
}

/// Failure injection: kills all four workers mid-job, waits out the gc window, then restarts them.
/// With no workers running, the job makes no progress during the outage and resumes once the
/// workers return and re-register with the storage and scheduler.
async fn inject_all_worker_failure(controller: &ComposeFaultController) -> anyhow::Result<()> {
    kill_then_restart(
        controller,
        &["worker-1", "worker-2", "worker-3", "worker-4"],
    )
    .await
}

/// Failure injection: kills the scheduler mid-job, holds it down, then restarts it. Execution
/// managers keep heartbeating to storage and finish any in-flight task, reporting its outcome
/// directly to storage (the storage service is still up), but cannot fetch new tasks while the
/// scheduler is down -- so the job stalls. The outage stays under the scheduler's
/// `em_registry.dead_em_cutoff_sec` so the scheduler does not mark the still-running execution
/// managers dead on restart. Once the scheduler returns it re-polls storage's inbound queues,
/// re-registers the execution managers from their heartbeats, and resumes dispatch and commit.
async fn inject_scheduler_failure(controller: &ComposeFaultController) -> anyhow::Result<()> {
    kill_then_restart(controller, &["scheduler"]).await
}

/// Lets tasks dispatch, then stops every `targets` service, holds it down for
/// `OUTAGE_DURATION_SEC`, and restarts it.
async fn kill_then_restart(
    controller: &ComposeFaultController,
    targets: &[&str],
) -> anyhow::Result<()> {
    tokio::time::sleep(Duration::from_secs(PRE_FAILURE_DELAY_SEC)).await;
    for service in targets {
        controller.stop(service).await?;
    }
    tokio::time::sleep(Duration::from_secs(OUTAGE_DURATION_SEC)).await;
    for service in targets {
        controller.start(service).await?;
    }
    Ok(())
}

/// Builds the standard 10×1000 NN job and its expected outputs for a fault test, keyed by
/// `resource_group_id`.
fn build_nn_fault_job(resource_group_id: &str) -> anyhow::Result<(JobSubmission, Vec<f64>)> {
    let layer_specs = (0..NUM_LAYERS)
        .map(|i| {
            (
                LAYER_SIZE,
                match i % 3 {
                    0 => Neuron::Relu,
                    1 => Neuron::Sigmoid,
                    _ => Neuron::Identity,
                },
            )
        })
        .collect::<Vec<_>>();
    let nn = NeuralNetwork::new(layer_specs, 0)?;
    let inputs = random_f64s(nn.num_graph_inputs(), 0);
    let expected = nn.simulate(&inputs)?;
    let task_graph = nn.to_task_graph()?;
    let job = JobSubmission {
        resource_group_id: resource_group_id.to_owned(),
        task_graph,
        inputs: inputs
            .iter()
            .map(encode_input)
            .collect::<anyhow::Result<Vec<_>>>()?,
    };
    Ok((job, expected))
}

/// Asserts a job terminated successfully with outputs matching `expected` within `REL_TOL`.
fn check_nn_outputs(result: TerminationResult, expected: &[f64]) -> anyhow::Result<()> {
    let outputs = match result {
        TerminationResult::Success(outputs) => outputs,
        TerminationResult::Failure(message) => bail!("job failed: {message}"),
        TerminationResult::Cancelled => bail!("job cancelled"),
    };
    let actual: Vec<f64> = outputs
        .iter()
        .map(decode_output)
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(
        actual.len() == expected.len(),
        "expected {} outputs, got {}",
        expected.len(),
        actual.len(),
    );
    for (&got, &exp) in actual.iter().zip(expected.iter()) {
        let diff = (got - exp).abs();
        let tol = REL_TOL * (1.0 + exp.abs());
        assert!(
            got.is_finite() && exp.is_finite() && diff <= tol,
            "output mismatch: got={got}, expected={exp}, diff={diff}, tol={tol}",
        );
    }
    Ok(())
}

/// # Returns
///
/// `count` number of deterministic random `f64` values seeded by `seed` (matches `tests/nn.rs`).
fn random_f64s(count: usize, seed: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count).map(|_| rng.random::<f64>()).collect()
}
