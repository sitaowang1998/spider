use async_trait::async_trait;

use crate::api::{
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
};

/// Protocol-neutral storage API client used by benchmark workloads.
#[async_trait]
pub trait StorageApiClient: Clone + Send + Sync + 'static {
    async fn get_session(&self, request: GetSessionRequest) -> ApiResult<SessionResponse>;

    async fn add_resource_group(
        &self,
        request: AddResourceGroupRequest,
    ) -> ApiResult<ResourceGroupResponse>;

    async fn verify_resource_group(
        &self,
        request: VerifyResourceGroupRequest,
    ) -> ApiResult<EmptyResponse>;

    async fn register_job(&self, request: RegisterJobRequest) -> ApiResult<JobIdResponse>;

    async fn start_job(&self, request: JobIdRequest) -> ApiResult<EmptyResponse>;

    async fn cancel_job(&self, request: JobIdRequest) -> ApiResult<JobStateResponse>;

    async fn get_job_state(&self, request: JobIdRequest) -> ApiResult<JobStateResponse>;

    async fn get_job_outputs(&self, request: JobIdRequest) -> ApiResult<JobOutputsResponse>;

    async fn get_job_error(&self, request: JobIdRequest) -> ApiResult<JobErrorResponse>;

    async fn poll_ready_tasks(
        &self,
        request: PollReadyTasksRequest,
    ) -> ApiResult<ReadyTasksResponse>;

    async fn poll_commit_ready_tasks(
        &self,
        request: PollReadyTasksRequest,
    ) -> ApiResult<TerminationTasksResponse>;

    async fn poll_cleanup_ready_tasks(
        &self,
        request: PollReadyTasksRequest,
    ) -> ApiResult<TerminationTasksResponse>;

    async fn create_task_instance(
        &self,
        request: CreateTaskInstanceRequest,
    ) -> ApiResult<ExecutionContextResponse>;

    async fn succeed_task_instance(
        &self,
        request: SucceedTaskInstanceRequest,
    ) -> ApiResult<JobStateResponse>;

    async fn succeed_commit_task_instance(
        &self,
        request: SucceedTerminationTaskInstanceRequest,
    ) -> ApiResult<JobStateResponse>;

    async fn succeed_cleanup_task_instance(
        &self,
        request: SucceedTerminationTaskInstanceRequest,
    ) -> ApiResult<JobStateResponse>;

    async fn fail_task_instance(
        &self,
        request: FailTaskInstanceRequest,
    ) -> ApiResult<JobStateResponse>;

    async fn register_execution_manager(
        &self,
        request: RegisterExecutionManagerRequest,
    ) -> ApiResult<ExecutionManagerResponse>;

    async fn update_execution_manager_heartbeat(
        &self,
        request: ExecutionManagerRequest,
    ) -> ApiResult<EmptyResponse>;
}
