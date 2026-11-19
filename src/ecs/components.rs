use crate::models::ApiResultModel;
use bevy_ecs::prelude::Component;
use serde::Serialize;
use std::time::Instant;
use tokio::sync::oneshot;

/// Metadata attached to every transient Request Entity spawned in the ECS World.
#[derive(Component, Debug)]
pub struct RequestMeta {
    pub request_id: String,
    pub client_ip: String,
    pub endpoint: String,
    pub created_at: Instant,
    pub timestamp_str: String,
}

/// API Key authentication component on a Request Entity.
#[derive(Component, Debug)]
pub struct ApiKeyAuth {
    pub api_key: String,
}

/// Payload component for Douyin WSS signing requests.
#[derive(Component, Debug)]
pub struct DouyinSignPayload {
    pub room_id: String,
    pub user_unique_id: String,
    pub browser_name: String,
    pub browser_version: String,
    pub live_url: Option<String>,
}

/// Lifecycle validation state of a Request Entity.
#[derive(Component, Debug, Clone)]
pub enum ValidationState {
    Pending,
    AuthPassed,
    Validated,
    Rejected { code: i32, reason: String },
}

/// Intermediate component produced by `validate_douyin_payload_system`.
#[derive(Component, Debug, Clone)]
pub struct ComputedStub {
    #[allow(dead_code)]
    pub params_str: String,
    pub md5_hex: String,
}

/// In-flight signing job dispatched to the Boa JS VM worker pool.
#[derive(Component)]
pub struct PendingSignJob(pub oneshot::Receiver<Result<String, String>>);

/// Final signing output component produced by `collect_douyin_sign_system`.
#[derive(Component, Debug, Clone)]
pub struct SignResult {
    pub signature: String,
    pub wss_url: String,
}

/// Oneshot response channel connecting the ECS pipeline back to the async Axum handler.
#[derive(Component)]
pub struct ResponseChannel(pub Option<oneshot::Sender<ApiResultModel>>);

/// Marker component indicating the Request Entity has replied and can be despawned.
#[derive(Component)]
pub struct CompletedTag;

/// Long-lived ECS Entity representing an exposed HTTP service endpoint.
#[derive(Component, Debug, Clone, Serialize)]
pub struct ServiceNode {
    pub name: String,
    pub route: String,
    pub method: String,
    pub description: String,
}

/// Long-lived ECS Entity representing a persistent JS VM Worker thread.
#[derive(Component, Debug, Clone, Serialize)]
pub struct JsWorkerEntity {
    pub worker_id: usize,
    pub status: String,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
}
