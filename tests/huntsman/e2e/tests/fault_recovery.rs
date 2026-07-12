//! End-to-end fault-recovery test: a layered `neuron::dense_*` task graph must still complete and
//! match the in-process simulation when a single execution-manager worker is killed mid-job and
//! restarted.

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

/// Seconds to let the job run before killing the worker, so tasks have dispatched to `worker-1`.
const PRE_FAILURE_DELAY_SEC: u64 = 3;

/// Seconds `worker-1` stays down. Must exceed the storage's
/// `task_instance_pool_config.execution_manager_stale_cutoff_sec` (10) plus `gc_interval_sec` (2)
/// so the storage garbage-collects the dead execution manager and reassigns its in-flight tasks to
/// the surviving workers before the worker is restarted.
const EM_DOWN_DURATION_SEC: u64 = 15;

/// The worker service that gets killed and restarted. Keep in sync with the compose file's
/// `worker-1` service name.
const FAULT_TARGET: &str = "worker-1";

/// Distinct resource-group id so this test does not collide with other fault tests sharing one
/// persistent database (see `add_resource_group` returning `ALREADY_EXISTS` for a duplicate
/// external id).
const RESOURCE_GROUP_ID: &str = "e2e-fault-single";

#[tokio::test]
async fn test_single_worker_failure_recovery() -> anyhow::Result<()> {
    if std::env::var("SPIDER_ENDPOINT").is_err() {
        bail!("SPIDER_ENDPOINT is not set");
    }
    let controller = ComposeFaultController::from_env()?;

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
        resource_group_id: RESOURCE_GROUP_ID.to_owned(),
        task_graph,
        inputs: inputs
            .iter()
            .map(encode_input)
            .collect::<anyhow::Result<Vec<_>>>()?,
    };

    SpiderTestDriver::run_exclusive(
        job,
        Duration::from_secs(300),
        async move |_job_id| {
            // Let tasks dispatch across the workers before killing one.
            tokio::time::sleep(Duration::from_secs(PRE_FAILURE_DELAY_SEC)).await;
            controller.stop(FAULT_TARGET).await?;
            // Hold the worker down past the storage's stale-cutoff + gc interval so its
            // execution manager is garbage-collected and its in-flight tasks are reassigned.
            tokio::time::sleep(Duration::from_secs(EM_DOWN_DURATION_SEC)).await;
            controller.start(FAULT_TARGET).await?;
            Ok(())
        },
        async move |_job_id, result| {
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
        },
    )
    .await?;

    Ok(())
}

/// # Returns
///
/// `count` number of deterministic random `f64` values seeded by `seed` (matches `tests/nn.rs`).
fn random_f64s(count: usize, seed: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count).map(|_| rng.random::<f64>()).collect()
}
