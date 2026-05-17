use std::{net::SocketAddr, sync::Arc};

use spider_storage::{
    db::MariaDbStorageConnector,
    ready_queue::ReadyQueueSenderHandle,
    state::{ServiceState, create_server_runtime},
    task_instance_pool::TaskInstancePoolHandle,
};

use crate::{
    api::{
        AddResourceGroupRequest,
        ApiResult,
        CreateTaskInstanceRequest,
        EmptyResponse,
        ExecutionContextResponse,
        ExecutionManagerRequest,
        ExecutionManagerResponse,
        FailTaskInstanceRequest,
        GetSessionRequest,
        JobErrorResponse,
        JobIdRequest,
        JobIdResponse,
        JobOutputsResponse,
        JobStateResponse,
        PollReadyTasksRequest,
        ReadyTasksResponse,
        RegisterExecutionManagerRequest,
        RegisterJobRequest,
        ResourceGroupResponse,
        SessionResponse,
        SucceedTaskInstanceRequest,
        SucceedTerminationTaskInstanceRequest,
        TerminationTasksResponse,
        VerifyResourceGroupRequest,
        format_id,
        job_state_response,
        parse_execution_manager_id,
        parse_job_id,
        parse_resource_group_id,
    },
    config::BenchConfig,
};

pub type StorageServiceState =
    ServiceState<ReadyQueueSenderHandle, MariaDbStorageConnector, TaskInstancePoolHandle>;

/// Shared storage API implementation used by REST and gRPC transports.
#[derive(Clone)]
pub struct StorageApiService {
    state: StorageServiceState,
}

impl StorageApiService {
    #[must_use]
    pub const fn new(state: StorageServiceState) -> Self {
        Self { state }
    }

    pub(crate) fn get_session(&self, _request: GetSessionRequest) -> SessionResponse {
        SessionResponse {
            session_id: self.state.session_id(),
        }
    }

    pub(crate) async fn add_resource_group(
        &self,
        request: AddResourceGroupRequest,
    ) -> ApiResult<ResourceGroupResponse> {
        let id = self
            .state
            .add_resource_group(request.external_id, request.password)
            .await?;
        Ok(ResourceGroupResponse {
            resource_group_id: format_id(&id),
        })
    }

    pub(crate) async fn verify_resource_group(
        &self,
        request: VerifyResourceGroupRequest,
    ) -> ApiResult<EmptyResponse> {
        let resource_group_id = parse_resource_group_id(&request.resource_group_id)?;
        self.state
            .verify_resource_group(resource_group_id, &request.password)
            .await?;
        Ok(EmptyResponse {})
    }

    pub(crate) async fn register_job(
        &self,
        request: RegisterJobRequest,
    ) -> ApiResult<JobIdResponse> {
        let resource_group_id = parse_resource_group_id(&request.resource_group_id)?;
        let job_id = self
            .state
            .register_job(
                resource_group_id,
                request.serialized_task_graph,
                request.serialized_inputs,
            )
            .await?;
        Ok(JobIdResponse {
            job_id: format_id(&job_id),
        })
    }

    pub(crate) async fn start_job(&self, request: JobIdRequest) -> ApiResult<EmptyResponse> {
        let job_id = parse_job_id(&request.job_id)?;
        self.state.start_job(job_id).await?;
        Ok(EmptyResponse {})
    }

    pub(crate) async fn cancel_job(&self, request: JobIdRequest) -> ApiResult<JobStateResponse> {
        let job_id = parse_job_id(&request.job_id)?;
        let state = self.state.cancel_job(job_id).await?;
        Ok(job_state_response(state))
    }

    pub(crate) async fn get_job_state(&self, request: JobIdRequest) -> ApiResult<JobStateResponse> {
        let job_id = parse_job_id(&request.job_id)?;
        let state = self.state.get_job_state(job_id).await?;
        Ok(job_state_response(state))
    }

    pub(crate) async fn get_job_outputs(
        &self,
        request: JobIdRequest,
    ) -> ApiResult<JobOutputsResponse> {
        let job_id = parse_job_id(&request.job_id)?;
        let outputs = self.state.get_job_outputs(job_id).await?;
        Ok(JobOutputsResponse { outputs })
    }

    pub(crate) async fn get_job_error(&self, request: JobIdRequest) -> ApiResult<JobErrorResponse> {
        let job_id = parse_job_id(&request.job_id)?;
        let error = self.state.get_job_error(job_id).await?;
        Ok(JobErrorResponse { error })
    }

    pub(crate) async fn poll_ready_tasks(
        &self,
        request: PollReadyTasksRequest,
    ) -> ApiResult<ReadyTasksResponse> {
        let tasks = self
            .state
            .poll_ready_tasks(request.max_tasks, request.wait())
            .await?
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(ReadyTasksResponse { tasks })
    }

    pub(crate) async fn poll_commit_ready_tasks(
        &self,
        request: PollReadyTasksRequest,
    ) -> ApiResult<TerminationTasksResponse> {
        let tasks = self
            .state
            .poll_commit_ready_tasks(request.max_tasks, request.wait())
            .await?
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(TerminationTasksResponse { tasks })
    }

    pub(crate) async fn poll_cleanup_ready_tasks(
        &self,
        request: PollReadyTasksRequest,
    ) -> ApiResult<TerminationTasksResponse> {
        let tasks = self
            .state
            .poll_cleanup_ready_tasks(request.max_tasks, request.wait())
            .await?
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(TerminationTasksResponse { tasks })
    }

    pub(crate) async fn create_task_instance(
        &self,
        request: CreateTaskInstanceRequest,
    ) -> ApiResult<ExecutionContextResponse> {
        let job_id = parse_job_id(&request.job_id)?;
        let execution_manager_id = parse_execution_manager_id(&request.execution_manager_id)?;
        let context = self
            .state
            .create_task_instance(
                request.session_id,
                job_id,
                request.task_id.into(),
                execution_manager_id,
            )
            .await?;
        Ok(context.into())
    }

    pub(crate) async fn succeed_task_instance(
        &self,
        request: SucceedTaskInstanceRequest,
    ) -> ApiResult<JobStateResponse> {
        let job_id = parse_job_id(&request.job_id)?;
        let state = self
            .state
            .succeed_task_instance(
                request.session_id,
                job_id,
                request.task_instance_id,
                request.task_index,
                request.serialized_outputs,
            )
            .await?;
        Ok(job_state_response(state))
    }

    pub(crate) async fn succeed_commit_task_instance(
        &self,
        request: SucceedTerminationTaskInstanceRequest,
    ) -> ApiResult<JobStateResponse> {
        let job_id = parse_job_id(&request.job_id)?;
        let state = self
            .state
            .succeed_commit_task_instance(request.session_id, job_id, request.task_instance_id)
            .await?;
        Ok(job_state_response(state))
    }

    pub(crate) async fn succeed_cleanup_task_instance(
        &self,
        request: SucceedTerminationTaskInstanceRequest,
    ) -> ApiResult<JobStateResponse> {
        let job_id = parse_job_id(&request.job_id)?;
        let state = self
            .state
            .succeed_cleanup_task_instance(request.session_id, job_id, request.task_instance_id)
            .await?;
        Ok(job_state_response(state))
    }

    pub(crate) async fn fail_task_instance(
        &self,
        request: FailTaskInstanceRequest,
    ) -> ApiResult<JobStateResponse> {
        let job_id = parse_job_id(&request.job_id)?;
        let state = self
            .state
            .fail_task_instance(
                request.session_id,
                job_id,
                request.task_instance_id,
                request.task_id.into(),
                request.error,
            )
            .await?;
        Ok(job_state_response(state))
    }

    pub(crate) async fn register_execution_manager(
        &self,
        request: RegisterExecutionManagerRequest,
    ) -> ApiResult<ExecutionManagerResponse> {
        let execution_manager_id = self
            .state
            .register_execution_manager(request.ip_address)
            .await?;
        Ok(ExecutionManagerResponse {
            execution_manager_id: format_id(&execution_manager_id),
        })
    }

    pub(crate) async fn update_execution_manager_heartbeat(
        &self,
        request: ExecutionManagerRequest,
    ) -> ApiResult<EmptyResponse> {
        let execution_manager_id = parse_execution_manager_id(&request.execution_manager_id)?;
        self.state
            .update_execution_manager_heartbeat(execution_manager_id)
            .await?;
        Ok(EmptyResponse {})
    }
}

/// Runs one storage API server until interrupted.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`create_server_runtime`]'s return values on failure.
/// * Forwards [`crate::rest::serve`]'s return values on failure.
/// * Forwards [`crate::grpc::serve`]'s return values on failure.
/// * Forwards [`spider_storage::state::ServerRuntime::stop_background_tasks`]'s return values on
///   failure.
pub async fn run_server(
    protocol: ServerProtocol,
    config: BenchConfig,
    bind_override: Option<SocketAddr>,
) -> anyhow::Result<()> {
    let runtime = create_server_runtime(&config.database_config()).await?;
    let state = runtime.service_state();
    let service = Arc::new(StorageApiService::new(state));
    let bind = bind_override.unwrap_or(match protocol {
        ServerProtocol::Rest => config.server.rest_bind,
        ServerProtocol::Grpc => config.server.grpc_bind,
    });

    match protocol {
        ServerProtocol::Rest => crate::rest::serve(bind, service).await?,
        ServerProtocol::Grpc => crate::grpc::serve(bind, service).await?,
    }

    runtime.stop_background_tasks().await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerProtocol {
    Rest,
    Grpc,
}

impl std::str::FromStr for ServerProtocol {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "rest" => Ok(Self::Rest),
            "grpc" => Ok(Self::Grpc),
            _ => anyhow::bail!("unknown protocol `{value}`"),
        }
    }
}
