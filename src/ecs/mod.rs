pub mod components;
pub mod resources;
pub mod systems;

use crate::config::ServerConfig;
use crate::ecs::components::{JsWorkerEntity, ServiceNode};
use crate::ecs::resources::{
    ApiKeyRegistry, CommandBatch, EcsCommand, EcsWorldSnapshot, MetricsResource, PendingSnapshots,
    RequestLogBuffer, SignEngineResource,
};
use crate::ecs::systems::{
    cleanup_completed_entities_system, dispatch_response_and_log_system,
    execute_douyin_sign_system, ingest_commands_system, serve_snapshots_system,
    sync_worker_entities_system, validate_auth_system, validate_douyin_payload_system,
};
use crate::engine::JsSignEngine;
use crate::models::{ApiResultModel, DouyinSignWssRequest};
use bevy_ecs::prelude::*;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::info;

/// Cloneable handle used by Axum handlers to dispatch work into the ECS World.
#[derive(Clone)]
pub struct EcsHandle {
    tx: mpsc::Sender<EcsCommand>,
}

impl EcsHandle {
    /// Start the dedicated ECS World & Schedule runner thread.
    pub fn spawn(config: ServerConfig, engine: JsSignEngine) -> Result<Self, String> {
        let (tx, rx) = mpsc::channel::<EcsCommand>();

        let worker_snapshots = engine.worker_snapshots();
        let mut world = World::new();

        // Insert ECS Resources
        let initial_keys = config.initial_api_keys.clone();
        world.insert_resource(config);
        world.insert_resource(ApiKeyRegistry::new(initial_keys));
        world.insert_resource(SignEngineResource { engine });
        world.insert_resource(MetricsResource::default());
        world.insert_resource(RequestLogBuffer::default());
        world.insert_resource(PendingSnapshots::default());
        world.insert_resource(CommandBatch::default());

        // Spawn long-lived ServiceNode Entities
        world.spawn(ServiceNode {
            name: "DouyinWssSignService".to_string(),
            route: "/Douyin/SignWss".to_string(),
            method: "POST".to_string(),
            description: "抖音直播间 WebSocket 签名生成（webmssdk.vm.js X-Bogus FrontierSign）"
                .to_string(),
        });
        world.spawn(ServiceNode {
            name: "KwaiUidToEidService".to_string(),
            route: "/Kwai/UidToEid".to_string(),
            method: "POST".to_string(),
            description: "快手 UID 转 EID 兼容接口（移植自原 C# KwaiController）".to_string(),
        });
        world.spawn(ServiceNode {
            name: "EcsInspectorService".to_string(),
            route: "/api/ecs/snapshot".to_string(),
            method: "GET".to_string(),
            description: "实时导出 ECS World 的 Entity、Component、Resource 与 System 状态"
                .to_string(),
        });
        world.spawn(ServiceNode {
            name: "DouyinLiveProtobufStream".to_string(),
            route: "/api/live/stream".to_string(),
            method: "GET (SSE)".to_string(),
            description: "输入抖音直播地址 -> ECS签名 -> WSS连接 -> Protobuf实时弹幕/礼物解码推送"
                .to_string(),
        });

        // Spawn long-lived JsWorkerEntity instances
        for w in worker_snapshots {
            world.spawn(JsWorkerEntity {
                worker_id: w.worker_id,
                status: w.status,
                tasks_completed: w.tasks_completed,
                tasks_failed: w.tasks_failed,
            });
        }

        // Build deterministic ECS Schedule pipeline
        let mut schedule = Schedule::default();
        schedule.add_systems(
            (
                ingest_commands_system,
                apply_deferred,
                validate_auth_system,
                validate_douyin_payload_system,
                apply_deferred,
                execute_douyin_sign_system,
                apply_deferred,
                dispatch_response_and_log_system,
                sync_worker_entities_system,
                cleanup_completed_entities_system,
                apply_deferred,
                serve_snapshots_system,
            )
                .chain(),
        );

        // Run one initial tick so metrics/active_entities are populated immediately
        schedule.run(&mut world);

        thread::Builder::new()
            .name("ecs-world-runner".to_string())
            .spawn(move || {
                info!("ECS World runner thread started (bevy_ecs)");
                loop {
                    match rx.recv_timeout(Duration::from_millis(500)) {
                        Ok(first_cmd) => {
                            let mut batch = vec![first_cmd];
                            while let Ok(next_cmd) = rx.try_recv() {
                                batch.push(next_cmd);
                            }
                            world.resource_mut::<CommandBatch>().commands = batch;
                            schedule.run(&mut world);
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            // Periodic idle housekeeping tick to refresh worker entity stats
                            schedule.run(&mut world);
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            info!("ECS World runner shutting down");
                            break;
                        }
                    }
                }
            })
            .map_err(|e| format!("Failed to spawn ECS runner thread: {e}"))?;

        Ok(Self { tx })
    }

    pub async fn sign_douyin_wss(
        &self,
        client_ip: String,
        payload: DouyinSignWssRequest,
    ) -> ApiResultModel {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .tx
            .send(EcsCommand::DouyinSign {
                client_ip,
                payload,
                reply: reply_tx,
            })
            .is_err()
        {
            return ApiResultModel::internal_error("ECS runtime channel closed");
        }

        reply_rx
            .await
            .unwrap_or_else(|_| ApiResultModel::internal_error("ECS pipeline dropped response"))
    }

    pub fn record_external_request(
        &self,
        client_ip: String,
        endpoint: String,
        target_id: String,
        code: i32,
        message: Option<String>,
        latency_ms: f64,
    ) {
        let _ = self.tx.send(EcsCommand::RecordExternalRequest {
            client_ip,
            endpoint,
            target_id,
            code,
            message,
            latency_ms,
        });
    }

    pub async fn get_snapshot(&self) -> Result<EcsWorldSnapshot, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EcsCommand::GetSnapshot { reply: reply_tx })
            .map_err(|_| "ECS runtime channel closed".to_string())?;
        reply_rx
            .await
            .map_err(|_| "ECS snapshot response dropped".to_string())
    }

    pub async fn add_api_key(&self, api_key: String) -> Result<Vec<String>, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EcsCommand::AddApiKey {
                api_key,
                reply: reply_tx,
            })
            .map_err(|_| "ECS runtime channel closed".to_string())?;
        reply_rx
            .await
            .map_err(|_| "ECS response dropped".to_string())?
    }

    pub async fn remove_api_key(&self, api_key: String) -> Result<Vec<String>, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EcsCommand::RemoveApiKey {
                api_key,
                reply: reply_tx,
            })
            .map_err(|_| "ECS runtime channel closed".to_string())?;
        reply_rx
            .await
            .map_err(|_| "ECS response dropped".to_string())?
    }

    pub async fn toggle_busy_simulation(&self, enabled: bool) -> Result<bool, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EcsCommand::ToggleBusySimulation {
                enabled,
                reply: reply_tx,
            })
            .map_err(|_| "ECS runtime channel closed".to_string())?;
        reply_rx
            .await
            .map_err(|_| "ECS response dropped".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ecs_sign_pipeline_end_to_end() {
        let mut cfg = ServerConfig::default();
        cfg.log_to_file = false;
        cfg.js_worker_count = 1;

        let engine = JsSignEngine::new(&cfg.script_path, 1).expect("init engine");
        let ecs = EcsHandle::spawn(cfg, engine).expect("spawn ecs");

        // 1. Valid request
        let ok_res = ecs
            .sign_douyin_wss(
                "127.0.0.1".to_string(),
                DouyinSignWssRequest {
                    api_key: Some("demo-apikey-learning-only".to_string()),
                    room_id: Some("7415791943831046921".to_string()),
                    user_unique_id: Some("7410618736569206322".to_string()),
                    browser_name: None,
                    browser_version: None,
                    live_url: None,
                },
            )
            .await;
        assert_eq!(ok_res.code, 0);
        let data = ok_res.data.expect("data present");
        let wss = data["WssUrl"].as_str().expect("WssUrl present");
        let sig = data["Signature"].as_str().expect("Signature present");
        assert!(wss.starts_with("wss://webcast5-ws-web-lf.douyin.com/webcast/im/push/v2/"));
        assert!(wss.ends_with(&format!("&signature={sig}")));
        assert!(!sig.is_empty());

        // 2. Invalid API key
        let bad_key = ecs
            .sign_douyin_wss(
                "127.0.0.1".to_string(),
                DouyinSignWssRequest {
                    api_key: Some("wrong-key".to_string()),
                    room_id: Some("123".to_string()),
                    user_unique_id: Some("456".to_string()),
                    browser_name: None,
                    browser_version: None,
                    live_url: None,
                },
            )
            .await;
        assert_eq!(bad_key.code, -1);
        assert_eq!(bad_key.msg.as_deref(), Some("ApiKey is invalid."));

        // 3. Snapshot check
        let snap = ecs.get_snapshot().await.expect("snapshot");
        assert_eq!(snap.metrics.total_requests, 2);
        assert_eq!(snap.metrics.success_count, 1);
        assert_eq!(snap.metrics.error_count, 1);
        assert_eq!(snap.recent_logs.len(), 2);
    }
}
