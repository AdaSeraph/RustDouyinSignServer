mod config;
mod ecs;
mod engine;
mod kwai;
mod live;
mod models;
mod proto;
mod web;

use crate::config::ServerConfig;
use crate::ecs::EcsHandle;
use crate::engine::JsSignEngine;
use crate::web::build_router;
use crate::web::handlers::AppState;
use std::net::SocketAddr;
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,douyin_sign_server_rust=debug")),
        )
        .init();

    let config = ServerConfig::load();

    info!("============================================================");
    info!("  DouyinSignServerRust (Rust + bevy_ecs + Boa JS Engine)");
    info!("============================================================");
    tracing::warn!("【免责声明 / DISCLAIMER】");
    tracing::warn!("  本项目仅供个人学习与非商业技术研究，严禁商业及任何违法违规用途。");
    tracing::warn!("  使用本项目产生的一切后果与法律责任由使用者本人承担，与作者无关。");
    tracing::warn!("  本项目与抖音/字节跳动/快手等平台无关联。继续运行即视为同意 LICENSE 全部条款。");
    tracing::warn!("  For learning & non-commercial research ONLY. Users bear sole legal");
    tracing::warn!("  responsibility. Running this software means you accept the LICENSE.");
    info!("============================================================");
    info!(
        script = %config.script_path,
        workers = config.js_worker_count,
        "Initializing persistent Boa JS VM worker pool..."
    );

    let engine = match JsSignEngine::new(&config.script_path, config.js_worker_count) {
        Ok(engine) => engine,
        Err(err) => {
            error!("Failed to initialize JS Sign Engine: {err}");
            std::process::exit(1);
        }
    };

    let ecs = match EcsHandle::spawn(config.clone(), engine) {
        Ok(handle) => handle,
        Err(err) => {
            error!("Failed to start ECS World runtime: {err}");
            std::process::exit(1);
        }
    };

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .expect("Failed to construct reqwest HTTP client");

    let state = AppState {
        ecs,
        http_client,
        config: config.clone(),
    };

    let app = build_router(state);
    let bind_addr = format!("{}:{}", config.host, config.port);

    let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
        Ok(l) => l,
        Err(err) => {
            error!("Failed to bind {bind_addr}: {err}");
            std::process::exit(1);
        }
    };

    info!("HTTP Server listening on http://{bind_addr}");
    info!(
        "Interactive GUI Test Console -> http://127.0.0.1:{}/",
        config.port
    );
    info!(
        "Douyin SignWss API           -> POST http://127.0.0.1:{}/Douyin/SignWss",
        config.port
    );
    info!(
        "ECS World Snapshot API       -> GET  http://127.0.0.1:{}/api/ecs/snapshot",
        config.port
    );

    if let Err(err) = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    {
        error!("Server error: {err}");
    }
}
