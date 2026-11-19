use crate::config::ServerConfig;
use crate::ecs::EcsHandle;
use crate::kwai;
use crate::live::{LiveStreamEvent, resolve_live_room, spawn_live_stream_task};
use crate::models::{
    ApiResultModel, DouyinSignWssRequest, KwaiUidToEidRequest, ManageApiKeyRequest,
};
use crate::proto::{run_live_pipeline_benchmark, run_protobuf_selftest};
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

#[derive(Debug, Deserialize)]
pub struct BenchSignRequest {
    pub total: Option<usize>,
    pub concurrency: Option<usize>,
    pub api_key: Option<String>,
    pub user_unique_id: Option<String>,
}

/// `POST /api/bench/sign` — server-side true-concurrency stress test for ECS + Boa JS VM signing
/// (bypasses browser HTTP/1.1 6-connection-per-origin limit).
pub async fn bench_sign_server(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<BenchSignRequest>,
) -> Json<ApiResultModel> {
    let total = req.total.unwrap_or(1000).clamp(1, 100_000);
    let concurrency = req.concurrency.unwrap_or(64).clamp(1, 512);
    let api_key = req
        .api_key
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "demo-apikey-learning-only".to_string());
    let uid = req
        .user_unique_id
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "7410618736569206322".to_string());
    let ip = extract_client_ip(&headers, addr);

    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));
    let wall_start = Instant::now();
    let mut handles = Vec::with_capacity(total);

    for i in 0..total {
        let permit = match sem.clone().acquire_owned().await {
            Ok(p) => p,
            Err(e) => return Json(ApiResultModel::internal_error(format!("semaphore error: {e}"))),
        };
        let ecs = state.ecs.clone();
        let ip_c = ip.clone();
        let key_c = api_key.clone();
        let uid_c = uid.clone();

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let t0 = Instant::now();
            let res = ecs
                .sign_douyin_wss(
                    ip_c,
                    DouyinSignWssRequest {
                        api_key: Some(key_c),
                        room_id: Some(format!("{}", 7415791943831046900u64 + i as u64)),
                        user_unique_id: Some(uid_c),
                        browser_name: None,
                        browser_version: None,
                        live_url: None,
                    },
                )
                .await;
            let dt_ms = t0.elapsed().as_secs_f64() * 1000.0;
            (res.code == 0, dt_ms)
        }));
    }

    let mut success = 0usize;
    let mut latencies = Vec::with_capacity(total);
    for h in handles {
        if let Ok((ok, dt_ms)) = h.await {
            if ok {
                success += 1;
            }
            latencies.push(dt_ms);
        }
    }

    let wall_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let qps = if wall_ms > 0.0 {
        (total as f64) / (wall_ms / 1000.0)
    } else {
        0.0
    };

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pct = |p: f64| -> f64 {
        if latencies.is_empty() {
            return 0.0;
        }
        let idx = ((latencies.len() as f64 - 1.0) * p).round() as usize;
        latencies[idx.min(latencies.len() - 1)]
    };
    let min_ms = latencies.first().copied().unwrap_or(0.0);
    let max_ms = latencies.last().copied().unwrap_or(0.0);
    let avg_ms = if latencies.is_empty() {
        0.0
    } else {
        latencies.iter().sum::<f64>() / latencies.len() as f64
    };

    Json(ApiResultModel::ok(json!({
        "mode": "server_parallel",
        "total": total,
        "concurrency": concurrency,
        "success": success,
        "failed": total.saturating_sub(success),
        "wall_ms": wall_ms,
        "qps": qps,
        "min_ms": min_ms,
        "avg_ms": avg_ms,
        "p50_ms": pct(0.50),
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
        "max_ms": max_ms
    })))
}

#[derive(Debug, Deserialize)]
pub struct BenchLivePipelineRequest {
    pub total_messages: Option<usize>,
    pub batch_size: Option<usize>,
    pub concurrency: Option<usize>,
}

/// `POST /api/bench/live-pipeline` — stress-test the 3-stage Protobuf live room pipeline under
/// 10k–100k viewer burst traffic and verify zero message loss.
pub async fn bench_live_pipeline(
    Json(req): Json<BenchLivePipelineRequest>,
) -> Json<ApiResultModel> {
    let total_messages = req.total_messages.unwrap_or(100_000);
    let batch_size = req.batch_size.unwrap_or(200);
    let concurrency = req.concurrency.unwrap_or(64);

    match run_live_pipeline_benchmark(total_messages, batch_size, concurrency).await {
        Ok(report) => Json(ApiResultModel::ok(
            serde_json::to_value(report).unwrap_or_default(),
        )),
        Err(err) => Json(ApiResultModel::internal_error(err)),
    }
}
