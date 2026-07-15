//! End-to-end fault-recovery tests: a layered `neuron::dense_*` task graph must still complete and
//! match the in-process simulation when an execution-manager worker (one, several, or all), the
//! scheduler, the storage server, the MariaDB database, or the storage server and scheduler
//! together is killed mid-job and restarted.

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
use spider_client::SpiderClient;
use spider_core::job::JobState;
use spider_core::types::id::JobId;

/// Relative-tolerance float comparison (matches `tests/nn.rs`).
const REL_TOL: f64 = 1.0e-12;

/// Number of layers in the test network (matches `tests/nn.rs`).
const NUM_LAYERS: usize = 10;

/// Neurons per layer in the test network (matches `tests/nn.rs`).
const LAYER_SIZE: usize = 1000;

/// Timeout for [`wait_until_job_active`]: a started job should reach an active execution phase well
/// within this window.
const JOB_ACTIVE_TIMEOUT_SEC: u64 = 60;

/// Seconds the targeted service stays down. For a worker failure this must exceed the storage's
/// `task_instance_pool_config.execution_manager_stale_cutoff_sec` (10) plus `gc_interval_sec` (2)
/// so the storage garbage-collects the dead execution manager and reassigns its in-flight tasks
/// before the worker restarts. For a scheduler or storage failure it must stay under the
/// scheduler's `em_registry.dead_em_cutoff_sec` (default 60) so the still-running execution
/// managers are not marked dead while the scheduler or storage is down. 15 satisfies both
/// (12 < 15 < 60). The down window starts after the last `stop` returns, so every targeted service
/// is down for at least this long.
const OUTAGE_DURATION_SEC: u64 = 15;

/// Seconds the MariaDB database stays down in the database failure scenario. This exceeds the
/// storage's `sqlx::MySqlPool` acquire timeout (default 30s), so a database operation inside the
/// task-instance pool's coroutine fails while MariaDB is down and the coroutine dies; the next
/// `register_task_instance` then surfaces a cache-internal error that storage's strict error
/// handler treats as fatal, cancelling and restarting the storage service. Storage crash-loops
/// until MariaDB is healthy and then recovers via `recover_job_cache` -- exercising the same
/// restart recovery path as [`inject_storage_failure`]. It must stay under the scheduler's
/// `em_registry.dead_em_cutoff_sec` (default 60) so the still-running execution managers are not
/// marked dead while the database and storage are down. 40 satisfies both (30 < 40 < 60).
const DATABASE_OUTAGE_DURATION_SEC: u64 = 40;

/// Distinct resource-group ids keep each scenario's jobs isolated in the shared persistent
/// database (`add_resource_group` returns `ALREADY_EXISTS` for a duplicate external id).
const RESOURCE_GROUP_SINGLE: &str = "e2e-fault-single";
const RESOURCE_GROUP_MULTI: &str = "e2e-fault-multi";
const RESOURCE_GROUP_ALL: &str = "e2e-fault-all";
const RESOURCE_GROUP_SCHEDULER: &str = "e2e-fault-scheduler";
const RESOURCE_GROUP_STORAGE: &str = "e2e-fault-storage";
const RESOURCE_GROUP_DATABASE: &str = "e2e-fault-database";
const RESOURCE_GROUP_STORAGE_AND_SCHEDULER: &str = "e2e-fault-storage-scheduler";

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

#[tokio::test]
#[serial_test::file_serial(compose_fault)]
async fn test_storage_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(RESOURCE_GROUP_STORAGE, inject_storage_failure).await
}

#[tokio::test]
#[serial_test::file_serial(compose_fault)]
async fn test_database_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(RESOURCE_GROUP_DATABASE, inject_database_failure).await
}

#[tokio::test]
#[serial_test::file_serial(compose_fault)]
async fn test_storage_and_scheduler_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(
        RESOURCE_GROUP_STORAGE_AND_SCHEDULER,
        inject_storage_and_scheduler_failure,
    )
    .await
}

/// Runs the standard NN job, injects a failure mid-job via `inject_failure`, and asserts the job
/// still completes with outputs matching the in-process simulation.
async fn run_fault_recovery_scenario<InjectFailure>(
    resource_group_id: &'static str,
    inject_failure: InjectFailure,
) -> anyhow::Result<()>
where
    InjectFailure: AsyncFnOnce(&ComposeFaultController, JobId, &SpiderClient) -> anyhow::Result<()>,
{
    if std::env::var("SPIDER_ENDPOINT").is_err() {
        bail!("SPIDER_ENDPOINT is not set");
    }
    let controller = ComposeFaultController::from_env()?;
    let (job, expected) = build_nn_fault_job(resource_group_id)?;

    SpiderTestDriver::run_exclusive(
        job,
        Duration::from_secs(300),
        async move |job_id, client| inject_failure(&controller, job_id, client).await,
        async move |_job_id, result| check_nn_outputs(result, &expected),
    )
    .await?;

    Ok(())
}

/// Failure injection: kills `worker-1` mid-job, holds it down past the storage's stale-cutoff + gc
/// interval so its execution manager is garbage-collected, then restarts it. The three surviving
/// workers pick up its reassigned in-flight tasks during the outage.
async fn inject_single_worker_failure(
    controller: &ComposeFaultController,
    job_id: JobId,
    client: &SpiderClient,
) -> anyhow::Result<()> {
    wait_until_job_active(client, job_id).await?;
    kill_then_restart(
        controller,
        &["worker-1"],
        Duration::from_secs(OUTAGE_DURATION_SEC),
    )
    .await
}

/// Failure injection: kills `worker-1` and `worker-2` mid-job, waits out the gc window, then
/// restarts them. The two surviving workers pick up the reassigned in-flight tasks.
async fn inject_multiple_worker_failure(
    controller: &ComposeFaultController,
    job_id: JobId,
    client: &SpiderClient,
) -> anyhow::Result<()> {
    wait_until_job_active(client, job_id).await?;
    kill_then_restart(
        controller,
        &["worker-1", "worker-2"],
        Duration::from_secs(OUTAGE_DURATION_SEC),
    )
    .await
}

/// Failure injection: kills all four workers mid-job, waits out the gc window, then restarts them.
/// With no workers running, the job makes no progress during the outage and resumes once the
/// workers return and re-register with the storage and scheduler.
async fn inject_all_worker_failure(
    controller: &ComposeFaultController,
    job_id: JobId,
    client: &SpiderClient,
) -> anyhow::Result<()> {
    wait_until_job_active(client, job_id).await?;
    kill_then_restart(
        controller,
        &["worker-1", "worker-2", "worker-3", "worker-4"],
        Duration::from_secs(OUTAGE_DURATION_SEC),
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
async fn inject_scheduler_failure(
    controller: &ComposeFaultController,
    job_id: JobId,
    client: &SpiderClient,
) -> anyhow::Result<()> {
    wait_until_job_active(client, job_id).await?;
    kill_then_restart(
        controller,
        &["scheduler"],
        Duration::from_secs(OUTAGE_DURATION_SEC),
    )
    .await
}

/// Failure injection: kills the storage server mid-job, holds it down, then restarts it. Storage
/// is the source of truth (backed by MariaDB), so a storage restart re-loads recoverable jobs from
/// the database and re-enqueues their ready tasks, and bumps a fresh session id so any in-flight
/// task assignments whose outcomes the execution managers dropped during the outage (storage was
/// unreachable) are invalidated and re-dispatched. Execution managers keep running their in-flight
/// tasks but their outcome reports to storage fail and are dropped while storage is down; once
/// storage returns the job re-executes the incomplete tasks from the recovered graph. The outage
/// stays under the scheduler's `em_registry.dead_em_cutoff_sec` (default 60) so the still-running
/// execution managers are not marked dead.
async fn inject_storage_failure(
    controller: &ComposeFaultController,
    job_id: JobId,
    client: &SpiderClient,
) -> anyhow::Result<()> {
    wait_until_job_active(client, job_id).await?;
    kill_then_restart(
        controller,
        &["storage"],
        Duration::from_secs(OUTAGE_DURATION_SEC),
    )
    .await
}

/// Failure injection: kills the MariaDB database mid-job, holds it down past the storage's
/// `sqlx::MySqlPool` acquire timeout (default 30s; see [`DATABASE_OUTAGE_DURATION_SEC`]), then
/// restarts it. While MariaDB is down a database operation inside the task-instance pool's
/// coroutine fails and the coroutine dies (its channel closes); the execution managers' next
/// `register_task_instance` calls then surface this as a cache-internal error
/// (`task instance pool corrupted: ... coroutine is dead: channel closed`), which the
/// task-instance-management service maps through its strict error handler, cancelling the storage
/// service. `restart: unless-stopped` then revives storage, but its `connect` fails while MariaDB
/// is still down, so storage crash-loops until the database returns. Once MariaDB is healthy,
/// storage reconnects, re-runs `recover_job_cache` to reload recoverable jobs, and resends their
/// ready tasks when the scheduler re-registers -- the same restart recovery path as
/// [`inject_storage_failure`]. The job-orchestration `get_job_state` path serves from the in-memory
/// cache while the job is cached, so the test driver's state poller keeps reporting the cached
/// state through the outage rather than surfacing database errors. The outage stays under the
/// scheduler's `em_registry.dead_em_cutoff_sec` (default 60) so the still-running execution
/// managers are not marked dead while the database and storage are down.
async fn inject_database_failure(
    controller: &ComposeFaultController,
    job_id: JobId,
    client: &SpiderClient,
) -> anyhow::Result<()> {
    wait_until_job_active(client, job_id).await?;
    kill_then_restart(
        controller,
        &["mariadb"],
        Duration::from_secs(DATABASE_OUTAGE_DURATION_SEC),
    )
    .await
}

/// Failure injection: kills the storage server and the scheduler simultaneously mid-job, holds them
/// both down, then restarts them together. This takes down the entire control plane at once: while
/// storage is down the execution managers lose their storage gRPC connection (their outcome reports
/// are dropped and they restart-loop until storage returns) and the scheduler is unavailable to
/// dispatch new tasks, so the job makes no progress during the outage. On restart, storage re-loads
/// recoverable jobs from the database via `recover_job_cache` and re-enqueues their ready tasks via
/// the startup resend, and the scheduler re-registers and re-polls storage's inbound queues; the
/// revived execution managers re-register from their heartbeats, and dispatch and commit resume.
/// The outage stays under the scheduler's `em_registry.dead_em_cutoff_sec` (default 60) so the
/// execution managers are not marked dead while the control plane is down.
async fn inject_storage_and_scheduler_failure(
    controller: &ComposeFaultController,
    job_id: JobId,
    client: &SpiderClient,
) -> anyhow::Result<()> {
    wait_until_job_active(client, job_id).await?;
    kill_then_restart(
        controller,
        &["storage", "scheduler"],
        Duration::from_secs(OUTAGE_DURATION_SEC),
    )
    .await
}

/// Polls the job until it enters an active execution phase (`Running`, `CommitReady`, or
/// `CleanupReady`), then returns so the caller injects a failure mid-execution.
///
/// Gating on job state (instead of a fixed delay) guarantees the failure lands inside the job's
/// running window -- which is ample for the 10x1000 task graph -- rather than during its dispatch
/// ramp-up or after it has already terminated. If the job reaches a terminal state first, this
/// bails loudly: a job too short to sustain a running phase cannot exercise mid-job failure and
/// must not silently pass by injecting a no-op failure after completion.
///
/// # Errors
///
/// Returns an error if:
///
/// * The job reaches a terminal state before any active state.
/// * The job does not reach an active state within [`JOB_ACTIVE_TIMEOUT_SEC`].
/// * Forwards [`SpiderClient::get_job_state`]'s return values on failure.
async fn wait_until_job_active(client: &SpiderClient, job_id: JobId) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(JOB_ACTIVE_TIMEOUT_SEC);
    loop {
        let state = client.get_job_state(job_id).await?;
        if matches!(
            state,
            JobState::Running | JobState::CommitReady | JobState::CleanupReady
        ) {
            return Ok(());
        }
        if state.is_terminal() {
            bail!(
                "job reached terminal state {state} before failure injection; the job is too \
                 short to test mid-execution failure"
            );
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("job did not reach an active execution state within {JOB_ACTIVE_TIMEOUT_SEC}s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Stops every `targets` service, holds it down for `outage`, and restarts it. The caller gates
/// this on the job being mid-execution via [`wait_until_job_active`] and picks an `outage` that
/// satisfies the scenario's recovery constraints (see [`OUTAGE_DURATION_SEC`] and
/// [`DATABASE_OUTAGE_DURATION_SEC`]).
async fn kill_then_restart(
    controller: &ComposeFaultController,
    targets: &[&str],
    outage: Duration,
) -> anyhow::Result<()> {
    for service in targets {
        controller.stop(service).await?;
    }
    tokio::time::sleep(outage).await;
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
