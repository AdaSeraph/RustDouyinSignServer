use crate::config::ServerConfig;
use crate::ecs::components::{JsWorkerEntity, ServiceNode};
use crate::engine::JsSignEngine;
use crate::models::{ApiResultModel, DouyinSignWssRequest};
use bevy_ecs::prelude::Resource;
use serde::Serialize;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::oneshot;

/// ECS Resource holding all authorized API keys.
#[derive(Resource, Debug)]
pub struct ApiKeyRegistry {
    pub keys: HashSet<String>,
}

impl ApiKeyRegistry {
    pub fn new(initial_keys: Vec<String>) -> Self {
        Self {
            keys: initial_keys.into_iter().collect(),
        }
    }

    pub fn sorted_keys(&self) -> Vec<String> {
        let mut list: Vec<String> = self.keys.iter().cloned().collect();
        list.sort();
        list
    }
}

/// ECS Resource wrapping the Boa JS VM worker pool.
#[derive(Resource)]
pub struct SignEngineResource {
    pub engine: JsSignEngine,
}

/// ECS Resource providing a thread-safe callback to wake the ECS runner immediately
/// when a background JS VM worker finishes signing a job.
#[derive(Resource, Clone)]
pub struct EcsWakeResource {
    pub wake: Arc<dyn Fn() + Send + Sync>,
}

/// ECS Resource tracking server & ECS pipeline metrics.
#[derive(Resource, Debug)]
pub struct MetricsResource {
    pub started_at: Instant,
    pub ecs_ticks: u64,
    pub total_requests: u64,
    pub success_count: u64,
    pub error_count: u64,
    pub total_latency_us: u64,
    pub active_entities: usize,
}

impl Default for MetricsResource {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            ecs_ticks: 0,
            total_requests: 0,
            success_count: 0,
            error_count: 0,
            total_latency_us: 0,
            active_entities: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestLogEntry {
    pub request_id: String,
    pub timestamp: String,
    pub client_ip: String,
    pub endpoint: String,
    pub api_key_preview: String,
    pub room_id: String,
    pub user_unique_id: String,
    pub code: i32,
    pub status: String,
    pub signature: Option<String>,
    pub message: Option<String>,
    pub latency_ms: f64,
}

/// Bounded ring buffer of recent requests for live GUI inspection.
#[derive(Resource, Debug)]
pub struct RequestLogBuffer {
    pub entries: VecDeque<RequestLogEntry>,
    pub capacity: usize,
}

impl Default for RequestLogBuffer {
    fn default() -> Self {
        Self {
            entries: VecDeque::with_capacity(200),
            capacity: 200,
        }
    }
}

impl RequestLogBuffer {
    pub fn push(&mut self, entry: RequestLogEntry) {
        if self.entries.len() >= self.capacity {
            self.entries.pop_back();
        }
        self.entries.push_front(entry);
    }
}

/// Pending snapshot replies queued during `ingest_commands_system` and served at the end of the tick.
#[derive(Resource, Default)]
pub struct PendingSnapshots {
    pub waiters: Vec<oneshot::Sender<EcsWorldSnapshot>>,
}

/// Commands sent from Axum HTTP handlers into the ECS World.
pub enum EcsCommand {
    DouyinSign {
        client_ip: String,
        payload: DouyinSignWssRequest,
        reply: oneshot::Sender<ApiResultModel>,
    },
    RecordExternalRequest {
        client_ip: String,
        endpoint: String,
        target_id: String,
        code: i32,
        message: Option<String>,
        latency_ms: f64,
    },
    AddApiKey {
        api_key: String,
        reply: oneshot::Sender<Result<Vec<String>, String>>,
    },
    RemoveApiKey {
        api_key: String,
        reply: oneshot::Sender<Result<Vec<String>, String>>,
    },
    ToggleBusySimulation {
        enabled: bool,
        reply: oneshot::Sender<bool>,
    },
    GetSnapshot {
        reply: oneshot::Sender<EcsWorldSnapshot>,
    },
    /// Lightweight notification sent by a JS VM worker when a sign job completes.
    Wake,
}

/// Staging buffer populated by the ECS runner before `schedule.run(&mut world)`.
#[derive(Resource, Default)]
pub struct CommandBatch {
    pub commands: Vec<EcsCommand>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SystemStageInfo {
    pub order: usize,
    pub name: &'static str,
    pub role: &'static str,
    pub components: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub uptime_secs: u64,
    pub ecs_ticks: u64,
    pub total_requests: u64,
    pub success_count: u64,
    pub error_count: u64,
    pub avg_latency_ms: f64,
    pub active_entities: usize,
}

/// Full ECS World Snapshot exposed via `GET /api/ecs/snapshot` for the GUI console.
#[derive(Debug, Clone, Serialize)]
pub struct EcsWorldSnapshot {
    pub architecture: String,
    pub engine_name: String,
    pub config: ServerConfig,
    pub metrics: MetricsSnapshot,
    pub systems_pipeline: Vec<SystemStageInfo>,
    pub service_entities: Vec<ServiceNode>,
    pub worker_entities: Vec<JsWorkerEntity>,
    pub api_keys: Vec<String>,
    pub recent_logs: Vec<RequestLogEntry>,
}

pub fn mask_api_key(key: &str) -> String {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        return "<empty>".to_string();
    }
    if trimmed.len() <= 12 {
        return trimmed.to_string();
    }
    format!("{}...{}", &trimmed[..8], &trimmed[trimmed.len() - 4..])
}
