use crate::config::ServerConfig;
use crate::ecs::EcsHandle;
use crate::kwai;
use crate::live::{LiveStreamEvent, resolve_live_room, spawn_live_stream_task};
use crate::models::{
    ApiResultModel, DouyinSignWssRequest, KwaiUidToEidRequest, ManageApiKeyRequest,
};
use crate::proto::run_protobuf_selftest;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::Html;
use axum::Json;
use futures_util::Stream;
use serde::Deserialize;
use serde_json::json;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::StreamExt as TokioStreamExt;
use tokio_stream::wrappers::ReceiverStream;

const GUI_HTML: &str = include_str!("../../static/index.html");

#[derive(Clone)]
pub struct AppState {
    pub ecs: EcsHandle,
    pub http_client: reqwest::Client,
    pub config: ServerConfig,
}

fn extract_client_ip(headers: &HeaderMap, addr: SocketAddr) -> String {
    if let Some(forwarded) = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return forwarded.to_string();
    }
    addr.ip().to_string()
}

/// Serve the interactive Web GUI Test & ECS Inspector Console.
pub async fn gui_index() -> Html<&'static str> {
    Html(GUI_HTML)
}

/// `POST /Douyin/SignWss` — compatible with the original C# `DouyinController.SignWss`.
pub async fn douyin_sign_wss(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(payload): Json<DouyinSignWssRequest>,
) -> Json<ApiResultModel> {
    let ip = extract_client_ip(&headers, addr);
    let result = state.ecs.sign_douyin_wss(ip, payload).await;
    Json(result)
}

/// `POST /Kwai/UidToEid` — ported from the original C# `KwaiController.UidToEid`.
pub async fn kwai_uid_to_eid(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(payload): Json<KwaiUidToEidRequest>,
) -> Json<ApiResultModel> {
    let ip = extract_client_ip(&headers, addr);
    let started = Instant::now();
    let target_id = payload.user_id.clone().unwrap_or_default();
    let result = kwai::uid_to_eid(&state.http_client, &state.config, &payload).await;
    let latency_ms = (started.elapsed().as_micros() as f64) / 1000.0;

    state.ecs.record_external_request(
        ip,
        "/Kwai/UidToEid".to_string(),
        target_id,
        result.code,
        result.msg.clone(),
        latency_ms,
    );

    Json(result)
}

/// `GET /api/health` — quick health check.
pub async fn health_check(State(state): State<AppState>) -> Json<serde_json::Value> {
    match state.ecs.get_snapshot().await {
        Ok(snap) => Json(json!({
            "status": "healthy",
            "architecture": snap.architecture,
            "engine": snap.engine_name,
            "uptime_secs": snap.metrics.uptime_secs,
            "ecs_ticks": snap.metrics.ecs_ticks,
            "total_requests": snap.metrics.total_requests,
            "educational_notice": "仅供学习与技术研究使用 (For Educational & Learning Purposes Only)"
        })),
        Err(err) => Json(json!({
            "status": "degraded",
            "error": err
        })),
    }
}

/// `GET /api/ecs/snapshot` — returns the live ECS World state for the GUI dashboard.
pub async fn ecs_snapshot(State(state): State<AppState>) -> Json<ApiResultModel> {
    match state.ecs.get_snapshot().await {
        Ok(snap) => Json(ApiResultModel::ok(
            serde_json::to_value(snap).unwrap_or_default(),
        )),
        Err(err) => Json(ApiResultModel::internal_error(err)),
    }
}

/// `POST /api/keys` — add an API key into the ECS `ApiKeyRegistry` resource.
pub async fn add_api_key(
    State(state): State<AppState>,
    Json(req): Json<ManageApiKeyRequest>,
) -> Json<ApiResultModel> {
    match state.ecs.add_api_key(req.api_key).await {
        Ok(keys) => Json(ApiResultModel::ok(json!({ "api_keys": keys }))),
        Err(err) => Json(ApiResultModel::business_error(err)),
    }
}

/// `DELETE /api/keys` — remove an API key from the ECS `ApiKeyRegistry` resource.
pub async fn remove_api_key(
    State(state): State<AppState>,
    Json(req): Json<ManageApiKeyRequest>,
) -> Json<ApiResultModel> {
    match state.ecs.remove_api_key(req.api_key).await {
        Ok(keys) => Json(ApiResultModel::ok(json!({ "api_keys": keys }))),
        Err(err) => Json(ApiResultModel::business_error(err)),
    }
}

#[derive(Debug, Deserialize)]
pub struct ToggleBusyRequest {
    pub enabled: bool,
}

/// `POST /api/config/busy-simulation` — toggle test-key busy throttling in ECS `ServerConfig`.
pub async fn toggle_busy_simulation(
    State(state): State<AppState>,
    Json(req): Json<ToggleBusyRequest>,
) -> Json<ApiResultModel> {
    match state.ecs.toggle_busy_simulation(req.enabled).await {
        Ok(enabled) => Json(ApiResultModel::ok(json!({
            "simulate_test_key_busy": enabled
        }))),
        Err(err) => Json(ApiResultModel::internal_error(err)),
    }
}

#[derive(Debug, Deserialize)]
pub struct LiveResolveRequest {
    pub live_url: String,
    pub ttwid: Option<String>,
    pub room_id: Option<String>,
}

/// `POST /api/live/resolve` — resolve a Douyin live URL into room_id, anchor name, title, and ttwid.
pub async fn live_resolve(
    State(state): State<AppState>,
    Json(req): Json<LiveResolveRequest>,
) -> Json<ApiResultModel> {
    match resolve_live_room(
        &state.http_client,
        &req.live_url,
        req.ttwid.as_deref(),
        req.room_id.as_deref(),
    )
    .await
    {
        Ok(info) => Json(ApiResultModel::ok(
            serde_json::to_value(info).unwrap_or_default(),
        )),
        Err(err) => Json(ApiResultModel::business_error(err)),
    }
}

#[derive(Debug, Deserialize)]
pub struct LiveStreamQuery {
    pub live_url: String,
    pub api_key: Option<String>,
    pub ttwid: Option<String>,
    pub room_id: Option<String>,
}

/// `GET /api/live/stream` — SSE stream connecting to Douyin Live WSS and pushing Protobuf-decoded messages.
pub async fn live_stream_sse(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<LiveStreamQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let ip = extract_client_ip(&headers, addr);
    let api_key = q
        .api_key
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "demo-apikey-learning-only".to_string());

    let (tx, rx) = mpsc::channel::<LiveStreamEvent>(4096);
    spawn_live_stream_task(
        state.http_client.clone(),
        state.ecs.clone(),
        ip,
        q.live_url,
        api_key,
        q.ttwid,
        q.room_id,
        tx,
    );

    let stream = ReceiverStream::new(rx).map(|ev| {
        let payload = serde_json::to_string(&ev).unwrap_or_else(|_| "{}".to_string());
        Ok(Event::default().data(payload))
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// `POST /api/proto/selftest` — run offline Protobuf encode -> gzip -> decode round-trip self-test.
pub async fn proto_selftest() -> Json<ApiResultModel> {
    match run_protobuf_selftest() {
        Ok(report) => Json(ApiResultModel::ok(
            serde_json::to_value(report).unwrap_or_default(),
        )),
        Err(err) => Json(ApiResultModel::internal_error(err)),
    }
}
