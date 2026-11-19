use crate::config::ServerConfig;
use crate::ecs::components::{
    ApiKeyAuth, CompletedTag, ComputedStub, DouyinSignPayload, JsWorkerEntity, RequestMeta,
    ResponseChannel, ServiceNode, SignResult, ValidationState,
};
use crate::ecs::resources::{
    ApiKeyRegistry, CommandBatch, EcsCommand, EcsWorldSnapshot, MetricsResource, MetricsSnapshot,
    PendingSnapshots, RequestLogBuffer, RequestLogEntry, SignEngineResource, SystemStageInfo,
    mask_api_key,
};
use crate::engine::{build_sign_params, build_wss_url, md5_hex};
use crate::models::{ApiResultModel, DouyinSignWssData};
use bevy_ecs::prelude::*;
use chrono::Local;
use rand::Rng;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::Instant;
use tracing::{error, info};
use uuid::Uuid;

const DEFAULT_BROWSER_NAME: &str = "Mozilla";
const DEFAULT_BROWSER_VERSION: &str = "5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
const LEGACY_TEST_API_KEY: &str =
    "test-apikey-de9991ea-bf2b-454c-7982-adddfe0581ac-96c0642e-7d1d-87a7-08b2-eff81edae4d3";

/// Stage 1: Drain queued commands from the HTTP layer and spawn Request Entities or mutate Resources.
pub fn ingest_commands_system(
    mut commands: Commands,
    mut batch: ResMut<CommandBatch>,
    mut registry: ResMut<ApiKeyRegistry>,
    mut config: ResMut<ServerConfig>,
    mut metrics: ResMut<MetricsResource>,
    mut log_buffer: ResMut<RequestLogBuffer>,
    mut pending_snapshots: ResMut<PendingSnapshots>,
) {
    let items = std::mem::take(&mut batch.commands);
    for cmd in items {
        match cmd {
            EcsCommand::DouyinSign {
                client_ip,
                payload,
                reply,
            } => {
                let now_str = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                let req_id = Uuid::new_v4().to_string()[..8].to_string();

                let api_key = payload.api_key.unwrap_or_default().trim().to_string();
                let room_id = payload.room_id.unwrap_or_default().trim().to_string();
                let user_unique_id = payload.user_unique_id.unwrap_or_default().trim().to_string();
                let browser_name = payload
                    .browser_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(DEFAULT_BROWSER_NAME)
                    .to_string();
                let browser_version = payload
                    .browser_version
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(DEFAULT_BROWSER_VERSION)
                    .to_string();

                commands.spawn((
                    RequestMeta {
                        request_id: req_id,
                        client_ip,
                        endpoint: "/Douyin/SignWss".to_string(),
                        created_at: Instant::now(),
                        timestamp_str: now_str,
                    },
                    ApiKeyAuth { api_key },
                    DouyinSignPayload {
                        room_id,
                        user_unique_id,
                        browser_name,
                        browser_version,
                        live_url: payload.live_url,
                    },
                    ValidationState::Pending,
                    ResponseChannel(Some(reply)),
                ));
            }
            EcsCommand::RecordExternalRequest {
                client_ip,
                endpoint,
                target_id,
                code,
                message,
                latency_ms,
            } => {
                metrics.total_requests += 1;
                if code == 0 {
                    metrics.success_count += 1;
                } else {
                    metrics.error_count += 1;
                }
                metrics.total_latency_us += (latency_ms * 1000.0) as u64;

                log_buffer.push(RequestLogEntry {
                    request_id: Uuid::new_v4().to_string()[..8].to_string(),
                    timestamp: Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                    client_ip,
                    endpoint,
                    api_key_preview: "-".to_string(),
                    room_id: target_id,
                    user_unique_id: "-".to_string(),
                    code,
                    status: if code == 0 {
                        "OK".to_string()
                    } else {
                        "ERR".to_string()
                    },
                    signature: None,
                    message,
                    latency_ms,
                });
            }
            EcsCommand::AddApiKey { api_key, reply } => {
                let trimmed = api_key.trim().to_string();
                if trimmed.is_empty() {
                    let _ = reply.send(Err("API Key 不能为空".to_string()));
                } else {
                    registry.keys.insert(trimmed);
                    let _ = reply.send(Ok(registry.sorted_keys()));
                }
            }
            EcsCommand::RemoveApiKey { api_key, reply } => {
                let trimmed = api_key.trim();
                if registry.keys.remove(trimmed) {
                    let _ = reply.send(Ok(registry.sorted_keys()));
                } else {
                    let _ = reply.send(Err("未找到指定的 API Key".to_string()));
                }
            }
            EcsCommand::ToggleBusySimulation { enabled, reply } => {
                config.simulate_test_key_busy = enabled;
                let _ = reply.send(config.simulate_test_key_busy);
            }
            EcsCommand::GetSnapshot { reply } => {
                pending_snapshots.waiters.push(reply);
            }
        }
    }
}

/// Stage 2: Authenticate Request Entities against `ApiKeyRegistry` Resource.
pub fn validate_auth_system(
    registry: Res<ApiKeyRegistry>,
    config: Res<ServerConfig>,
    mut query: Query<(&ApiKeyAuth, &mut ValidationState), Without<CompletedTag>>,
) {
    for (auth, mut state) in &mut query {
        if !matches!(*state, ValidationState::Pending) {
            continue;
        }

        if auth.api_key.is_empty() {
            *state = ValidationState::Rejected {
                code: -1,
                reason: "ApiKey is required.".to_string(),
            };
            continue;
        }

        if !registry.keys.contains(&auth.api_key) {
            *state = ValidationState::Rejected {
                code: -1,
                reason: "ApiKey is invalid.".to_string(),
            };
            continue;
        }

        // Optional simulation of the original C# random busy throttle on the temp test key.
        if config.simulate_test_key_busy && auth.api_key.contains(LEGACY_TEST_API_KEY) {
            let num: u8 = rand::rng().random_range(0..100);
            if (20..=60).contains(&num) {
                *state = ValidationState::Rejected {
                    code: -1,
                    reason: "The sign server is too busy.".to_string(),
                };
                continue;
            }
        }

        *state = ValidationState::AuthPassed;
    }
}

/// Stage 3: Validate Douyin payload fields and compute the MD5 stub component.
pub fn validate_douyin_payload_system(
    mut commands: Commands,
    mut query: Query<(Entity, &DouyinSignPayload, &mut ValidationState), Without<CompletedTag>>,
) {
    for (entity, payload, mut state) in &mut query {
        if !matches!(*state, ValidationState::AuthPassed) {
            continue;
        }

        if payload.room_id.is_empty() {
            *state = ValidationState::Rejected {
                code: -1,
                reason: "RoomId is required.".to_string(),
            };
            continue;
        }

        if payload.user_unique_id.is_empty() {
            *state = ValidationState::Rejected {
                code: -1,
                reason: "UserUniqueId is required.".to_string(),
            };
            continue;
        }

        let params_str = build_sign_params(&payload.room_id, &payload.user_unique_id);
        let md5 = md5_hex(&params_str);

        commands.entity(entity).insert(ComputedStub {
            params_str,
            md5_hex: md5,
        });
        *state = ValidationState::Validated;
    }
}

/// Stage 4: Execute JS frontier signing for validated Douyin Request Entities.
pub fn execute_douyin_sign_system(
    mut commands: Commands,
    sign_engine: Res<SignEngineResource>,
    mut query: Query<
        (
            Entity,
            &DouyinSignPayload,
            &ComputedStub,
            &mut ValidationState,
        ),
        (Without<SignResult>, Without<CompletedTag>),
    >,
) {
    for (entity, payload, stub, mut state) in &mut query {
        if !matches!(*state, ValidationState::Validated) {
            continue;
        }

        match sign_engine.engine.sign_stub_blocking(stub.md5_hex.clone()) {
            Ok(signature) => {
                let wss_url = build_wss_url(
                    &payload.room_id,
                    &payload.user_unique_id,
                    &payload.browser_name,
                    &payload.browser_version,
                    &signature,
                );
                commands.entity(entity).insert(SignResult { signature, wss_url });
            }
            Err(err) => {
                error!(error = %err, "JS frontier signing failed");
                *state = ValidationState::Rejected {
                    code: 500,
                    reason: format!("接口访问内部错误: {err}"),
                };
            }
        }
    }
}

/// Stage 5: Send HTTP response back through `ResponseChannel`, record metrics & logs, and mark entity completed.
#[allow(clippy::too_many_arguments)]
pub fn dispatch_response_and_log_system(
    mut commands: Commands,
    config: Res<ServerConfig>,
    mut metrics: ResMut<MetricsResource>,
    mut log_buffer: ResMut<RequestLogBuffer>,
    mut query: Query<
        (
            Entity,
            &RequestMeta,
            &ApiKeyAuth,
            &DouyinSignPayload,
            &ValidationState,
            Option<&ComputedStub>,
            Option<&SignResult>,
            &mut ResponseChannel,
        ),
        Without<CompletedTag>,
    >,
) {
    for (entity, meta, auth, payload, state, stub, sign_result, mut channel) in &mut query {
        let elapsed_us = meta.created_at.elapsed().as_micros() as u64;
        let latency_ms = (elapsed_us as f64) / 1000.0;

        let (response, code, status, signature_opt, msg_opt) = match (state, sign_result, stub) {
            (ValidationState::Validated, Some(res), Some(stub_comp)) => {
                let data = serde_json::to_value(DouyinSignWssData {
                    wss_url: res.wss_url.clone(),
                    signature: res.signature.clone(),
                    stub_md5: stub_comp.md5_hex.clone(),
                })
                .unwrap_or_default();
                (
                    ApiResultModel::ok(data),
                    0,
                    "OK".to_string(),
                    Some(res.signature.clone()),
                    None,
                )
            }
            (ValidationState::Rejected { code, reason }, _, _) => {
                let resp = if *code == 500 {
                    ApiResultModel::internal_error(reason.clone())
                } else {
                    ApiResultModel::business_error(reason.clone())
                };
                (resp, *code, "REJECTED".to_string(), None, Some(reason.clone()))
            }
            _ => (
                ApiResultModel::internal_error("接口访问内部错误"),
                500,
                "ERROR".to_string(),
                None,
                Some("Unexpected pipeline state".to_string()),
            ),
        };

        if let Some(tx) = channel.0.take() {
            let _ = tx.send(response);
        }

        metrics.total_requests += 1;
        metrics.total_latency_us += elapsed_us;
        if code == 0 {
            metrics.success_count += 1;
        } else {
            metrics.error_count += 1;
        }

        log_buffer.push(RequestLogEntry {
            request_id: meta.request_id.clone(),
            timestamp: meta.timestamp_str.clone(),
            client_ip: meta.client_ip.clone(),
            endpoint: meta.endpoint.clone(),
            api_key_preview: mask_api_key(&auth.api_key),
            room_id: payload.room_id.clone(),
            user_unique_id: payload.user_unique_id.clone(),
            code,
            status,
            signature: signature_opt,
            message: msg_opt,
            latency_ms,
        });

        if config.log_to_file {
            append_daily_log(
                &config.log_dir,
                &meta.timestamp_str,
                &meta.client_ip,
                &auth.api_key,
                &payload.room_id,
                &payload.user_unique_id,
                payload.live_url.as_deref(),
            );
        }

        info!(
            req_id = %meta.request_id,
            ip = %meta.client_ip,
            room_id = %payload.room_id,
            code,
            latency_ms = format!("{latency_ms:.2}"),
            "Douyin SignWss completed"
        );

        commands.entity(entity).insert(CompletedTag);
    }
}

/// Stage 6: Synchronize long-lived `JsWorkerEntity` components with live JS worker pool telemetry.
pub fn sync_worker_entities_system(
    sign_engine: Res<SignEngineResource>,
    mut query: Query<&mut JsWorkerEntity>,
) {
    let snapshots = sign_engine.engine.worker_snapshots();
    for mut worker in &mut query {
        if let Some(snap) = snapshots.iter().find(|s| s.worker_id == worker.worker_id) {
            worker.status = snap.status.clone();
            worker.tasks_completed = snap.tasks_completed;
            worker.tasks_failed = snap.tasks_failed;
        }
    }
}

/// Stage 7: Despawn completed Request Entities and update ECS tick & entity counters.
pub fn cleanup_completed_entities_system(
    mut commands: Commands,
    mut metrics: ResMut<MetricsResource>,
    completed_query: Query<Entity, With<CompletedTag>>,
    all_entities: Query<Entity>,
) {
    let mut despawned = 0usize;
    for entity in &completed_query {
        commands.entity(entity).despawn();
        despawned += 1;
    }
    metrics.ecs_ticks += 1;
    metrics.active_entities = all_entities.iter().count().saturating_sub(despawned);
}

/// Stage 8: Serve any pending `GetSnapshot` queries with a consistent view of the ECS World.
#[allow(clippy::too_many_arguments)]
pub fn serve_snapshots_system(
    mut pending: ResMut<PendingSnapshots>,
    config: Res<ServerConfig>,
    registry: Res<ApiKeyRegistry>,
    sign_engine: Res<SignEngineResource>,
    metrics: Res<MetricsResource>,
    log_buffer: Res<RequestLogBuffer>,
    services_query: Query<&ServiceNode>,
    workers_query: Query<&JsWorkerEntity>,
) {
    if pending.waiters.is_empty() {
        return;
    }

    let avg_latency_ms = if metrics.total_requests > 0 {
        (metrics.total_latency_us as f64) / (metrics.total_requests as f64) / 1000.0
    } else {
        0.0
    };

    let mut worker_entities: Vec<JsWorkerEntity> = workers_query.iter().cloned().collect();
    worker_entities.sort_by_key(|w| w.worker_id);

    let snapshot = EcsWorldSnapshot {
        architecture: "Rust + bevy_ecs 0.15 (Entity-Component-System)".to_string(),
        engine_name: sign_engine.engine.engine_name().to_string(),
        config: config.clone(),
        metrics: MetricsSnapshot {
            uptime_secs: metrics.started_at.elapsed().as_secs(),
            ecs_ticks: metrics.ecs_ticks,
            total_requests: metrics.total_requests,
            success_count: metrics.success_count,
            error_count: metrics.error_count,
            avg_latency_ms,
            active_entities: metrics.active_entities,
        },
        systems_pipeline: vec![
            SystemStageInfo {
                order: 1,
                name: "ingest_commands_system",
                role: "接收 HTTP 指令并生成 Request Entity",
                components: "RequestMeta, ApiKeyAuth, DouyinSignPayload, ValidationState, ResponseChannel",
            },
            SystemStageInfo {
                order: 2,
                name: "validate_auth_system",
                role: "校验 ApiKeyRegistry 权限与限流模拟",
                components: "ApiKeyAuth, ValidationState",
            },
            SystemStageInfo {
                order: 3,
                name: "validate_douyin_payload_system",
                role: "校验 RoomId/UserUniqueId 并计算 MD5 Stub",
                components: "DouyinSignPayload, ValidationState -> ComputedStub",
            },
            SystemStageInfo {
                order: 4,
                name: "execute_douyin_sign_system",
                role: "调用 Boa JS VM 执行 webmssdk.vm.js 签名并拼装 WSS",
                components: "DouyinSignPayload, ComputedStub -> SignResult",
            },
            SystemStageInfo {
                order: 5,
                name: "dispatch_response_and_log_system",
                role: "回传 HTTP 响应、更新 MetricsResource 与请求日志",
                components: "ResponseChannel, SignResult -> CompletedTag",
            },
            SystemStageInfo {
                order: 6,
                name: "sync_worker_entities_system",
                role: "同步常驻 JS VM Worker Entity 状态",
                components: "JsWorkerEntity",
            },
            SystemStageInfo {
                order: 7,
                name: "cleanup_completed_entities_system",
                role: "回收已完成的 Request Entity 并更新 Tick 计数",
                components: "CompletedTag (Despawn)",
            },
            SystemStageInfo {
                order: 8,
                name: "serve_snapshots_system",
                role: "导出 ECS World 实时快照供 GUI 监控面板展示",
                components: "ServiceNode, JsWorkerEntity, Resources",
            },
        ],
        service_entities: services_query.iter().cloned().collect(),
        worker_entities,
        api_keys: registry.sorted_keys(),
        recent_logs: log_buffer.entries.iter().cloned().collect(),
    };

    for waiter in pending.waiters.drain(..) {
        let _ = waiter.send(snapshot.clone());
    }
}

fn append_daily_log(
    log_dir: &str,
    timestamp: &str,
    ip: &str,
    api_key: &str,
    room_id: &str,
    user_unique_id: &str,
    live_url: Option<&str>,
) {
    let dir = Path::new(log_dir);
    if let Err(e) = fs::create_dir_all(dir) {
        error!("Failed to create log directory '{}': {e}", dir.display());
        return;
    }

    let date_tag = Local::now().format("%Y%m%d");
    let log_file = dir.join(format!("logs_{date_tag}.txt"));
    let line = format!(
        "[{timestamp}] IP: {ip}, Key: {}, RoomId: {room_id}, UserUniqueId: {user_unique_id}\n",
        mask_api_key(api_key)
    );

    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&log_file) {
        let _ = f.write_all(line.as_bytes());
    }

    if let Some(url) = live_url.map(str::trim).filter(|s| !s.is_empty()) {
        let live_file = dir.join("LiveUrl.txt");
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&live_file) {
            let _ = writeln!(f, "[{timestamp}] {url}");
        }
    }
}
