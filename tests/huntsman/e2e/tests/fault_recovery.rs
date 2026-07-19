//! End-to-end recovery tests for failures in a Helm-managed Kubernetes deployment.

use std::process::Command;
use std::time::Duration;

use anyhow::Context;
use anyhow::bail;
use e2e::JobSubmission;
use e2e::SpiderTestDriver;
use e2e::TerminationResult;
use e2e::decode_output;
use e2e::encode_input;
use e2e::nn::NeuralNetwork;
use e2e::nn::Neuron;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use serial_test::file_serial;
use spider_client::SpiderClient;
use spider_core::job::JobState;
use spider_core::types::id::JobId;

const CONTAINER_RESTART_TIMEOUT: Duration = Duration::from_secs(180);
const DATABASE_OUTAGE_DURATION: Duration = Duration::from_secs(40);
const JOB_ACTIVE_TIMEOUT: Duration = Duration::from_secs(60);
const JOB_TIMEOUT: Duration = Duration::from_secs(300);
const LAYER_SIZE: usize = 1000;
const NUM_LAYERS: usize = 10;
const OUTAGE_DURATION: Duration = Duration::from_secs(15);
const REL_TOL: f64 = 1.0e-12;
const RUN_ID_ENV_VAR: &str = "SPIDER_E2E_RUN_ID";

const RESOURCE_GROUP_ALL_EXECUTION_MANAGERS: &str = "e2e-fault-all-execution-managers";
const RESOURCE_GROUP_DATABASE: &str = "e2e-fault-database";
const RESOURCE_GROUP_MIXED: &str = "e2e-fault-mixed";
const RESOURCE_GROUP_SCHEDULER: &str = "e2e-fault-scheduler";
const RESOURCE_GROUP_SINGLE_EXECUTION_MANAGER: &str = "e2e-fault-single-execution-manager";
const RESOURCE_GROUP_STORAGE: &str = "e2e-fault-storage";

#[derive(Clone, Copy)]
enum Component {
    ExecutionManager,
    Scheduler,
    Storage,
    Database,
}

impl Component {
    /// # Returns
    ///
    /// The component name used by the Helm chart.
    const fn chart_name(self) -> &'static str {
        match self {
            Self::ExecutionManager => "worker",
            Self::Scheduler => "scheduler",
            Self::Storage => "storage",
            Self::Database => "database",
        }
    }
}

struct KubernetesFaultController {
    context: String,
    namespace: String,
    release_name: String,
}

impl KubernetesFaultController {
    /// Creates a controller from the Kubernetes e2e environment variables.
    ///
    /// # Returns
    ///
    /// A controller targeting the configured Helm release on success.
    ///
    /// # Errors
    ///
    /// Returns an error if a required environment variable is unset.
    fn from_env() -> anyhow::Result<Self> {
        const CONTEXT_ENV_VAR: &str = "SPIDER_KUBERNETES_CONTEXT";
        const NAMESPACE_ENV_VAR: &str = "SPIDER_KUBERNETES_NAMESPACE";
        const RELEASE_ENV_VAR: &str = "SPIDER_HELM_RELEASE";

        let context = std::env::var(CONTEXT_ENV_VAR)
            .with_context(|| format!("{CONTEXT_ENV_VAR} is not set"))?;
        let namespace = std::env::var(NAMESPACE_ENV_VAR)
            .with_context(|| format!("{NAMESPACE_ENV_VAR} is not set"))?;
        let release_name = std::env::var(RELEASE_ENV_VAR)
            .with_context(|| format!("{RELEASE_ENV_VAR} is not set"))?;
        Ok(Self {
            context,
            namespace,
            release_name,
        })
    }

    /// Terminates one execution-manager container and waits for Kubernetes to restart it.
    ///
    /// # Errors
    ///
    /// Returns an error if a Kubernetes command fails or no execution-manager pod exists.
    async fn restart_one_execution_manager(&self) -> anyhow::Result<()> {
        let pod_names = self.pod_names(Component::ExecutionManager).await?;
        let Some(pod_name) = pod_names.first() else {
            bail!("no execution-manager pod is available for failure injection");
        };
        self.restart_containers(std::slice::from_ref(pod_name), OUTAGE_DURATION)
            .await
    }

    /// Terminates every container belonging to the selected components and waits for recovery.
    ///
    /// # Errors
    ///
    /// Forwards [`Self::pod_names`] and [`Self::restart_containers`]'s return values on failure.
    async fn restart_components(
        &self,
        components: &[Component],
        outage_duration: Duration,
    ) -> anyhow::Result<()> {
        let mut pod_names = Vec::new();
        for &component in components {
            pod_names.extend(self.pod_names(component).await?);
        }
        self.restart_containers(&pod_names, outage_duration).await
    }

    /// Terminates the selected pods' main containers through the kind node's CRI and waits for
    /// each restart to become ready.
    ///
    /// # Errors
    ///
    /// Forwards [`Self::restart_count`], [`Self::container_id`], [`Self::node_name`],
    /// [`Self::stop_container`], and [`Self::wait_for_restart`]'s return values on failure.
    async fn restart_containers(
        &self,
        pod_names: &[String],
        outage_duration: Duration,
    ) -> anyhow::Result<()> {
        let mut previous_restart_counts = Vec::with_capacity(pod_names.len());
        for pod_name in pod_names {
            previous_restart_counts.push(self.restart_count(pod_name).await?);
            let container_id = self.container_id(pod_name).await?;
            let node_name = self.node_name(pod_name).await?;
            self.stop_container(&node_name, &container_id).await?;
        }
        tokio::time::sleep(outage_duration).await;
        for (pod_name, previous_restart_count) in pod_names.iter().zip(previous_restart_counts) {
            self.wait_for_restart(pod_name, previous_restart_count)
                .await?;
        }
        Ok(())
    }

    /// Reads the main container ID for a pod.
    ///
    /// # Returns
    ///
    /// The runtime-qualified container ID on success.
    ///
    /// # Errors
    ///
    /// Forwards [`Self::kubectl_output`]'s return values on failure.
    async fn container_id(&self, pod_name: &str) -> anyhow::Result<String> {
        self.kubectl_output(vec![
            "get".to_owned(),
            "pod".to_owned(),
            pod_name.to_owned(),
            "--output".to_owned(),
            "jsonpath={.status.containerStatuses[0].containerID}".to_owned(),
        ])
        .await
    }

    /// Reads the kind node hosting a pod.
    ///
    /// # Returns
    ///
    /// The node name on success.
    ///
    /// # Errors
    ///
    /// Forwards [`Self::kubectl_output`]'s return values on failure.
    async fn node_name(&self, pod_name: &str) -> anyhow::Result<String> {
        self.kubectl_output(vec![
            "get".to_owned(),
            "pod".to_owned(),
            pod_name.to_owned(),
            "--output".to_owned(),
            "jsonpath={.spec.nodeName}".to_owned(),
        ])
        .await
    }

    /// Stops a container through the CRI running inside its kind node.
    ///
    /// # Errors
    ///
    /// Returns an error if the Docker command cannot be spawned, its blocking task panics, or it
    /// exits unsuccessfully.
    async fn stop_container(&self, node_name: &str, container_id: &str) -> anyhow::Result<()> {
        let container_id = container_id
            .strip_prefix("containerd://")
            .map_or(container_id, |container_id| container_id)
            .to_owned();
        let node_name = node_name.to_owned();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let output = Command::new("docker")
                .args([
                    "exec",
                    &node_name,
                    "crictl",
                    "stop",
                    "--timeout=1",
                    &container_id,
                ])
                .output()
                .context("failed to spawn docker")?;
            if !output.status.success() {
                bail!(
                    "docker exec crictl stop failed (exit {:?}): {}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr).trim(),
                );
            }
            Ok(())
        })
        .await
        .context("docker blocking task panicked")?
    }

    /// Lists the pods belonging to a component in the Helm release.
    ///
    /// # Returns
    ///
    /// The matching pod names on success.
    ///
    /// # Errors
    ///
    /// Forwards [`Self::kubectl_output`]'s return values on failure.
    async fn pod_names(&self, component: Component) -> anyhow::Result<Vec<String>> {
        let output = self
            .kubectl_output(vec![
                "get".to_owned(),
                "pod".to_owned(),
                "--selector".to_owned(),
                self.selector(component),
                "--output".to_owned(),
                "jsonpath={range .items[*]}{.metadata.name}{'\\n'}{end}".to_owned(),
            ])
            .await?;
        Ok(output.lines().map(str::to_owned).collect())
    }

    /// Reads the main container's restart count for a pod.
    ///
    /// # Returns
    ///
    /// The restart count on success.
    ///
    /// # Errors
    ///
    /// Returns an error if Kubernetes returns an invalid count. Forwards
    /// [`Self::kubectl_output`]'s return values on failure.
    async fn restart_count(&self, pod_name: &str) -> anyhow::Result<u32> {
        self.kubectl_output(vec![
            "get".to_owned(),
            "pod".to_owned(),
            pod_name.to_owned(),
            "--output".to_owned(),
            "jsonpath={.status.containerStatuses[0].restartCount}".to_owned(),
        ])
        .await?
        .parse()
        .context("invalid Kubernetes container restart count")
    }

    /// Waits for a pod's main container to restart and become ready.
    ///
    /// # Errors
    ///
    /// Returns an error if the restart does not become ready before
    /// [`CONTAINER_RESTART_TIMEOUT`]. Forwards [`Self::restart_count`] and
    /// [`Self::kubectl_output`]'s return values on failure.
    async fn wait_for_restart(
        &self,
        pod_name: &str,
        previous_restart_count: u32,
    ) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + CONTAINER_RESTART_TIMEOUT;
        loop {
            let restart_count = self.restart_count(pod_name).await?;
            let ready = self
                .kubectl_output(vec![
                    "get".to_owned(),
                    "pod".to_owned(),
                    pod_name.to_owned(),
                    "--output".to_owned(),
                    "jsonpath={.status.containerStatuses[0].ready}".to_owned(),
                ])
                .await?;
            if restart_count > previous_restart_count && ready == "true" {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("container in pod {pod_name} did not restart and become ready in time");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Runs a Kubernetes command and returns its trimmed standard output.
    ///
    /// # Returns
    ///
    /// The command's standard output on success.
    ///
    /// # Errors
    ///
    /// Forwards [`Self::run_kubectl`]'s return values on failure.
    async fn kubectl_output(&self, args: Vec<String>) -> anyhow::Result<String> {
        self.run_kubectl(args).await
    }

    /// Runs a Kubernetes command against the configured Helm namespace.
    ///
    /// # Returns
    ///
    /// The command's trimmed standard output on success.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be spawned, its blocking task panics, or it exits
    /// unsuccessfully.
    async fn run_kubectl(&self, args: Vec<String>) -> anyhow::Result<String> {
        let context = self.context.clone();
        let namespace = self.namespace.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<String> {
            let output = Command::new("kubectl")
                .args(["--context", &context, "--namespace", &namespace])
                .args(&args)
                .output()
                .context("failed to spawn kubectl")?;
            if !output.status.success() {
                bail!(
                    "kubectl command failed (exit {:?}): {}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr).trim(),
                );
            }
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
        })
        .await
        .context("kubectl blocking task panicked")?
    }

    /// # Returns
    ///
    /// The Kubernetes label selector for a component's pods.
    fn selector(&self, component: Component) -> String {
        format!(
            "app.kubernetes.io/instance={},app.kubernetes.io/component={}",
            self.release_name,
            component.chart_name(),
        )
    }
}

#[tokio::test]
#[file_serial(single_execution_manager_failure)]
async fn test_single_execution_manager_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(
        RESOURCE_GROUP_SINGLE_EXECUTION_MANAGER,
        async |controller| controller.restart_one_execution_manager().await,
    )
    .await
}

#[tokio::test]
#[file_serial(all_execution_managers_failure)]
async fn test_all_execution_managers_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(RESOURCE_GROUP_ALL_EXECUTION_MANAGERS, async |controller| {
        controller
            .restart_components(&[Component::ExecutionManager], OUTAGE_DURATION)
            .await
    })
    .await
}

#[tokio::test]
#[file_serial(scheduler_failure)]
async fn test_scheduler_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(RESOURCE_GROUP_SCHEDULER, async |controller| {
        controller
            .restart_components(&[Component::Scheduler], OUTAGE_DURATION)
            .await
    })
    .await
}

#[tokio::test]
#[file_serial(storage_failure)]
async fn test_storage_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(RESOURCE_GROUP_STORAGE, async |controller| {
        controller
            .restart_components(&[Component::Storage], OUTAGE_DURATION)
            .await
    })
    .await
}

#[tokio::test]
#[file_serial(database_failure)]
async fn test_database_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(RESOURCE_GROUP_DATABASE, async |controller| {
        controller
            .restart_components(&[Component::Database], DATABASE_OUTAGE_DURATION)
            .await
    })
    .await
}

#[tokio::test]
#[file_serial(mixed_component_failure)]
async fn test_mixed_component_failure_recovery() -> anyhow::Result<()> {
    run_fault_recovery_scenario(RESOURCE_GROUP_MIXED, async |controller| {
        controller
            .restart_components(
                &[
                    Component::ExecutionManager,
                    Component::Scheduler,
                    Component::Storage,
                    Component::Database,
                ],
                OUTAGE_DURATION,
            )
            .await
    })
    .await
}

/// Runs an NN job, injects a failure during execution, and verifies the recovered result.
///
/// # Type Parameters
///
/// * `InjectFailureType` - The operation that injects the scenario's Kubernetes failure.
///
/// # Errors
///
/// Returns an error if the environment is incomplete, job construction fails, failure injection
/// fails, or the recovered job result is incorrect.
async fn run_fault_recovery_scenario<InjectFailureType>(
    resource_group_prefix: &str,
    inject_failure: InjectFailureType,
) -> anyhow::Result<()>
where
    InjectFailureType: AsyncFnOnce(&KubernetesFaultController) -> anyhow::Result<()>, {
    let controller = KubernetesFaultController::from_env()?;
    let run_id = std::env::var(RUN_ID_ENV_VAR)
        .map_err(|_| anyhow::anyhow!("{RUN_ID_ENV_VAR} is not set"))?;
    let resource_group_id = format!("{resource_group_prefix}-{run_id}");
    let (job, expected) = build_nn_fault_job(resource_group_id)?;

    SpiderTestDriver::run_exclusive(
        job,
        JOB_TIMEOUT,
        async move |job_id, client| {
            wait_until_job_active(client, job_id).await?;
            inject_failure(&controller).await
        },
        async move |_job_id, result| check_nn_outputs(result, &expected),
    )
    .await
}

/// Waits until the job reaches an active execution state.
///
/// # Errors
///
/// Returns an error if the job terminates or the timeout expires before it becomes active.
/// Forwards [`SpiderClient::get_job_state`]'s return values on failure.
async fn wait_until_job_active(client: &SpiderClient, job_id: JobId) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + JOB_ACTIVE_TIMEOUT;
    loop {
        let state = client.get_job_state(job_id).await?;
        if matches!(
            state,
            JobState::Running | JobState::CommitReady | JobState::CleanupReady
        ) {
            return Ok(());
        }
        if state.is_terminal() {
            bail!("job reached terminal state {state} before failure injection");
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("job did not become active before failure-injection timeout");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Builds the NN job and expected outputs used by a recovery scenario.
///
/// # Returns
///
/// The job submission and its expected outputs on success.
///
/// # Errors
///
/// Returns an error if the neural network or its serialized inputs cannot be constructed.
fn build_nn_fault_job(resource_group_id: String) -> anyhow::Result<(JobSubmission, Vec<f64>)> {
    let layer_specs = (0..NUM_LAYERS)
        .map(|index| {
            (
                LAYER_SIZE,
                match index % 3 {
                    0 => Neuron::Relu,
                    1 => Neuron::Sigmoid,
                    _ => Neuron::Identity,
                },
            )
        })
        .collect::<Vec<_>>();
    let neural_network = NeuralNetwork::new(layer_specs, 0)?;
    let inputs = random_f64s(neural_network.num_graph_inputs(), 0);
    let expected = neural_network.simulate(&inputs)?;
    let task_graph = neural_network.to_task_graph()?;
    let job = JobSubmission {
        resource_group_id,
        task_graph,
        inputs: inputs
            .iter()
            .map(encode_input)
            .collect::<anyhow::Result<Vec<_>>>()?,
    };
    Ok((job, expected))
}

/// Verifies that a job succeeded with the expected NN outputs.
///
/// # Errors
///
/// Returns an error if the job did not succeed or its outputs differ from the expected values.
fn check_nn_outputs(result: TerminationResult, expected: &[f64]) -> anyhow::Result<()> {
    let outputs = match result {
        TerminationResult::Success(outputs) => outputs,
        TerminationResult::Failure(message) => bail!("job failed: {message}"),
        TerminationResult::Cancelled => bail!("job cancelled"),
    };
    let actual = outputs
        .iter()
        .map(decode_output)
        .collect::<anyhow::Result<Vec<f64>>>()?;
    anyhow::ensure!(
        actual.len() == expected.len(),
        "expected {} outputs, got {}",
        expected.len(),
        actual.len(),
    );
    for (&actual_value, &expected_value) in actual.iter().zip(expected.iter()) {
        let difference = (actual_value - expected_value).abs();
        let tolerance = REL_TOL * (1.0 + expected_value.abs());
        anyhow::ensure!(
            actual_value.is_finite() && expected_value.is_finite() && difference <= tolerance,
            "output mismatch: actual={actual_value}, expected={expected_value}, \
             difference={difference}, tolerance={tolerance}",
        );
    }
    Ok(())
}

/// # Returns
///
/// The requested number of deterministic random values.
fn random_f64s(count: usize, seed: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count).map(|_| rng.random::<f64>()).collect()
}
