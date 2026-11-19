use boa_engine::{Context, Source};
use md5::{Digest, Md5};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::sync::oneshot;
use tracing::{error, info, warn};

/// Compute lowercase hex MD5 digest matching C# `ScriptsExecuter.Md5Hex`.
pub fn md5_hex(input: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

/// Build the parameter string used for Douyin frontier signing.
pub fn build_sign_params(room_id: &str, user_unique_id: &str) -> String {
    format!(
        "live_id=1,aid=6383,version_code=180800,webcast_sdk_version=1.0.12,room_id={room_id},sub_room_id=,sub_channel_id=,did_rule=3,user_unique_id={user_unique_id},device_platform=web,device_type=,ac=,identity=audience"
    )
}

/// Build the Douyin WSS URL matching the original C# `DouyinController.SignWss`.
pub fn build_wss_url(
    room_id: &str,
    user_unique_id: &str,
    browser_name: &str,
    browser_version: &str,
    signature: &str,
) -> String {
    let encoded_version = urlencoding::encode(browser_version);
    format!(
        "wss://webcast5-ws-web-lf.douyin.com/webcast/im/push/v2/\
        ?app_name=douyin_web\
        &version_code=180800\
        &webcast_sdk_version=1.0.12\
        &update_version_code=1.0.12\
        &compress=gzip\
        &device_platform=web\
        &cookie_enabled=true\
        &screen_width=1440\
        &screen_height=960\
        &browser_language=zh-CN\
        &browser_platform=Win32\
        &browser_name={browser_name}\
        &browser_version={encoded_version}\
        &browser_online=true\
        &tz_name=Asia/Shanghai\
        &host=https://live.douyin.com\
        &aid=6383\
        &live_id=1\
        &did_rule=3\
        &endpoint=live_pc\
        &support_wrds=1\
        &user_unique_id={user_unique_id}\
        &im_path=/webcast/im/fetch/\
        &identity=audience\
        &room_id={room_id}\
        &heartbeatDuration=0\
        &signature={signature}"
    )
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerSnapshot {
    pub worker_id: usize,
    pub status: String,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
}

struct SignJob {
    md5_stub: String,
    reply: oneshot::Sender<Result<String, String>>,
}

struct WorkerStats {
    worker_id: usize,
    status: Mutex<String>,
    tasks_completed: AtomicU64,
    tasks_failed: AtomicU64,
}

/// Thread-pool backed JS signing engine using `boa_engine`.
/// Each worker thread initializes `webmssdk.vm.js` once and reuses its VM context.
#[derive(Clone)]
pub struct JsSignEngine {
    sender: std::sync::mpsc::Sender<SignJob>,
    workers: Arc<Vec<Arc<WorkerStats>>>,
    engine_name: String,
}

impl JsSignEngine {
    pub fn new(script_path: impl AsRef<Path>, worker_count: usize) -> Result<Self, String> {
        let script_path: PathBuf = script_path.as_ref().to_path_buf();
        let script_content = std::fs::read_to_string(&script_path)
            .map_err(|e| format!("Failed to read JS script '{}': {e}", script_path.display()))?;

        let worker_count = worker_count.max(1);
        let (tx, rx) = std::sync::mpsc::channel::<SignJob>();
        let shared_rx = Arc::new(Mutex::new(rx));
        let mut workers = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let rx_clone = Arc::clone(&shared_rx);
            let script_clone = script_content.clone();
            let stats = Arc::new(WorkerStats {
                worker_id,
                status: Mutex::new("initializing".to_string()),
                tasks_completed: AtomicU64::new(0),
                tasks_failed: AtomicU64::new(0),
            });
            let stats_clone = Arc::clone(&stats);
            workers.push(stats);

            let (init_tx, init_rx) = std::sync::mpsc::channel::<Result<(), String>>();

            thread::Builder::new()
                .name(format!("js-vm-worker-{worker_id}"))
                .stack_size(16 * 1024 * 1024)
                .spawn(move || {
                    let mut context = Context::default();
                    let console_shim = r#"
                        if (typeof globalThis.console === 'undefined') {
                            globalThis.console = {
                                log: function() {},
                                info: function() {},
                                warn: function() {},
                                error: function() {},
                                debug: function() {},
                                trace: function() {},
                                table: function() {},
                                dir: function() {}
                            };
                        }
                    "#;
                    let _ = context.eval(Source::from_bytes(console_shim.as_bytes()));
                    match context.eval(Source::from_bytes(script_clone.as_bytes())) {
                        Ok(_) => {
                            if let Ok(mut s) = stats_clone.status.lock() {
                                *s = "ready".to_string();
                            }
                            let _ = init_tx.send(Ok(()));
                            info!(worker_id, "Boa JS VM worker initialized webmssdk.vm.js");
                        }
                        Err(err) => {
                            let msg = format!("Worker {worker_id} failed to load webmssdk.vm.js: {err}");
                            if let Ok(mut s) = stats_clone.status.lock() {
                                *s = format!("error: {err}");
                            }
                            error!("{msg}");
                            let _ = init_tx.send(Err(msg));
                            return;
                        }
                    }

                    loop {
                        let job = {
                            let guard = match rx_clone.lock() {
                                Ok(g) => g,
                                Err(_) => break,
                            };
                            match guard.recv() {
                                Ok(job) => job,
                                Err(_) => break,
                            }
                        };

                        if let Ok(mut s) = stats_clone.status.lock() {
                            *s = "signing".to_string();
                        }

                        let expr = format!(
                            "window.byted_acrawler.frontierSign({{'X-MS-STUB':'{}'}})['X-Bogus']",
                            job.md5_stub
                        );

                        let result = match context.eval(Source::from_bytes(expr.as_bytes())) {
                            Ok(val) => {
                                if val.is_null_or_undefined() {
                                    stats_clone.tasks_failed.fetch_add(1, Ordering::Relaxed);
                                    warn!(worker_id, "frontierSign returned null/undefined");
                                    Ok("00000000".to_string())
                                } else {
                                    match val.to_string(&mut context) {
                                        Ok(s) => {
                                            stats_clone.tasks_completed.fetch_add(1, Ordering::Relaxed);
                                            Ok(s.to_std_string_escaped())
                                        }
                                        Err(e) => {
                                            stats_clone.tasks_failed.fetch_add(1, Ordering::Relaxed);
                                            Err(format!("Failed to convert signature to string: {e}"))
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                stats_clone.tasks_failed.fetch_add(1, Ordering::Relaxed);
                                Err(format!("JS evaluation error: {err}"))
                            }
                        };

                        if let Ok(mut s) = stats_clone.status.lock() {
                            *s = "ready".to_string();
                        }

                        let _ = job.reply.send(result);
                    }
                })
                .map_err(|e| format!("Failed to spawn JS worker thread: {e}"))?;

            init_rx
                .recv()
                .map_err(|e| format!("JS worker init channel closed: {e}"))??;
        }

        Ok(Self {
            sender: tx,
            workers: Arc::new(workers),
            engine_name: format!("boa_engine ({worker_count} persistent VM workers)"),
        })
    }

    /// Synchronously sign an MD5 stub using the worker pool (blocking wait on oneshot).
    pub fn sign_stub_blocking(&self, md5_stub: String) -> Result<String, String> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(SignJob {
                md5_stub,
                reply: tx,
            })
            .map_err(|e| format!("Sign engine queue unavailable: {e}"))?;
        rx.blocking_recv()
            .map_err(|e| format!("Sign worker dropped response: {e}"))?
    }

    pub fn engine_name(&self) -> &str {
        &self.engine_name
    }

    pub fn worker_snapshots(&self) -> Vec<WorkerSnapshot> {
        self.workers
            .iter()
            .map(|w| WorkerSnapshot {
                worker_id: w.worker_id,
                status: w
                    .status
                    .lock()
                    .map(|s| s.clone())
                    .unwrap_or_else(|_| "unknown".to_string()),
                tasks_completed: w.tasks_completed.load(Ordering::Relaxed),
                tasks_failed: w.tasks_failed.load(Ordering::Relaxed),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_md5_and_boa_frontier_sign() {
        let params = build_sign_params("7415791943831046921", "7410618736569206322");
        let stub = md5_hex(&params);
        assert_eq!(stub.len(), 32);

        let engine = JsSignEngine::new("scripts/webmssdk.vm.js", 1)
            .expect("Boa engine should initialize webmssdk.vm.js");
        let sig = engine
            .sign_stub_blocking(stub)
            .expect("frontierSign should succeed");
        println!("Generated signature: {sig}");
        assert!(!sig.is_empty());
        assert_ne!(sig, "00000000");
    }
}
