pub mod handlers;

use crate::web::handlers::{
    AppState, add_api_key, bench_live_pipeline, bench_sign_server, douyin_sign_wss, ecs_snapshot,
    gui_index, health_check, kwai_uid_to_eid, live_resolve, live_stream_sse, proto_selftest,
    remove_api_key, toggle_busy_simulation,
};
use axum::Router;
use axum::routing::{delete, get, post};
use tower_http::cors::CorsLayer;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(gui_index))
        // Original C# compatible endpoints (exact case + lowercase aliases)
        .route("/Douyin/SignWss", post(douyin_sign_wss))
        .route("/douyin/signwss", post(douyin_sign_wss))
        .route("/Kwai/UidToEid", post(kwai_uid_to_eid))
        .route("/kwai/uidtoeid", post(kwai_uid_to_eid))
        // Live room Protobuf message streaming & selftest
        .route("/api/live/resolve", post(live_resolve))
        .route("/api/live/stream", get(live_stream_sse))
        .route("/api/proto/selftest", post(proto_selftest))
        // High-concurrency stress testing (10k-100k viewer scenarios)
        .route("/api/bench/sign", post(bench_sign_server))
        .route("/api/bench/live-pipeline", post(bench_live_pipeline))
        // Modern observability & GUI management endpoints
        .route("/api/health", get(health_check))
        .route("/api/ecs/snapshot", get(ecs_snapshot))
        .route("/api/keys", post(add_api_key))
        .route("/api/keys", delete(remove_api_key))
        .route("/api/config/busy-simulation", post(toggle_busy_simulation))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
