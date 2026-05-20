use std::{collections::HashMap, sync::Arc};

use axum::{
    Json,
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::{
    AgentArgs,
    BenchmarkReport,
    ClientRunOptions,
    distributed::{AgentRunAccepted, AgentRunRequest, AgentRunState, AgentRunStatus},
    run_client_report,
};

#[derive(Clone)]
struct AgentState {
    agent_id: String,
    config_path: std::path::PathBuf,
    runs: Arc<Mutex<HashMap<String, AgentRunRecord>>>,
}

#[derive(Clone)]
struct AgentRunRecord {
    status: AgentRunState,
    report: Option<BenchmarkReport>,
    error: Option<String>,
}

#[derive(Serialize)]
struct HealthResponse {
    agent_id: String,
    status: &'static str,
}

pub async fn run_agent(args: AgentArgs) -> anyhow::Result<()> {
    let state = AgentState {
        agent_id: args.agent_id,
        config_path: args.config,
        runs: Arc::new(Mutex::new(HashMap::new())),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/runs", post(start_run))
        .route("/runs/:run_id", get(get_run))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<AgentState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        agent_id: state.agent_id,
        status: "ok",
    })
}

async fn start_run(
    State(state): State<AgentState>,
    Json(request): Json<AgentRunRequest>,
) -> Result<Json<AgentRunAccepted>, AgentError> {
    let mut runs = state.runs.lock().await;
    if runs
        .values()
        .any(|run| matches!(run.status, AgentRunState::Running))
    {
        return Err(AgentError::conflict(
            "agent already has a running benchmark",
        ));
    }
    if runs.contains_key(&request.run_id) {
        return Err(AgentError::conflict(format!(
            "run `{}` already exists",
            request.run_id
        )));
    }
    runs.insert(
        request.run_id.clone(),
        AgentRunRecord {
            status: AgentRunState::Running,
            report: None,
            error: None,
        },
    );
    drop(runs);

    let run_id = request.run_id.clone();
    let task_run_id = run_id.clone();
    let task_state = state.clone();
    tokio::spawn(async move {
        let result = execute_run(&task_state, request).await;
        let mut runs = task_state.runs.lock().await;
        if let Some(record) = runs.get_mut(&task_run_id) {
            match result {
                Ok(report) => {
                    record.status = AgentRunState::Succeeded;
                    record.report = Some(report);
                }
                Err(err) => {
                    record.status = AgentRunState::Failed;
                    record.error = Some(err.to_string());
                }
            }
        }
    });

    Ok(Json(AgentRunAccepted {
        run_id,
        status: AgentRunState::Accepted,
    }))
}

async fn execute_run(
    state: &AgentState,
    request: AgentRunRequest,
) -> anyhow::Result<BenchmarkReport> {
    let mut config = spider_storage_api_bench::config::BenchConfig::load(&state.config_path)?;
    config.benchmark.job_count = request.job_count;
    config.benchmark.flat_percent = request.flat_percent;
    config.benchmark.validate()?;
    run_client_report(ClientRunOptions {
        protocol: request.protocol,
        workload: request.workload,
        config,
        target: request.target,
        server_metrics: false,
    })
    .await
}

async fn get_run(
    State(state): State<AgentState>,
    Path(run_id): Path<String>,
) -> Result<Json<AgentRunStatus>, AgentError> {
    let record = {
        let runs = state.runs.lock().await;
        runs.get(&run_id)
            .cloned()
            .ok_or_else(|| AgentError::not_found(format!("run `{run_id}` not found")))?
    };
    Ok(Json(AgentRunStatus {
        run_id,
        agent_id: state.agent_id,
        status: record.status,
        report: record.report.clone(),
        error: record.error,
    }))
}

struct AgentError {
    status: StatusCode,
    message: String,
}

impl AgentError {
    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl IntoResponse for AgentError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}
