use bevy_ecs::prelude::Resource;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::Path;
use tracing::{info, warn};

/// Runtime configuration stored as an ECS Resource.
/// Loaded from `config.toml` (or `SIGN_CONFIG_PATH`), with environment variables taking precedence.
#[derive(Debug, Clone, Resource, Serialize, Deserialize)]
pub struct ServerConfig {
    pub config_source: String,
    pub host: String,
    pub port: u16,
    pub script_path: String,
    pub js_worker_count: usize,
    pub log_to_file: bool,
    pub log_dir: String,
    pub simulate_test_key_busy: bool,
    #[serde(skip_serializing)]
    pub initial_api_keys: Vec<String>,
    pub kwai_sign_endpoint: String,
    #[serde(skip_serializing)]
    pub kwai_api_key: String,
    #[serde(skip_serializing)]
    pub kwai_default_token: String,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    server: ServerSection,
    #[serde(default)]
    logging: LoggingSection,
    #[serde(default)]
    auth: AuthSection,
    #[serde(default)]
    kwai: KwaiSection,
}

#[derive(Debug, Default, Deserialize)]
struct ServerSection {
    host: Option<String>,
    port: Option<u16>,
    script_path: Option<String>,
    js_worker_count: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct LoggingSection {
    log_to_file: Option<bool>,
    log_dir: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct AuthSection {
    simulate_test_key_busy: Option<bool>,
    api_keys: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
struct KwaiSection {
    sign_endpoint: Option<String>,
    api_key: Option<String>,
    default_token: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            config_source: "built-in defaults".to_string(),
            host: "0.0.0.0".to_string(),
            port: 5289,
            script_path: "scripts/webmssdk.vm.js".to_string(),
            js_worker_count: 2,
            log_to_file: true,
            log_dir: "logs".to_string(),
            simulate_test_key_busy: false,
            initial_api_keys: vec![
                "demo-apikey-learning-only".to_string(),
                "test-apikey-de9991ea-bf2b-454c-7982-adddfe0581ac-96c0642e-7d1d-87a7-08b2-eff81edae4d3"
                    .to_string(),
            ],
            kwai_sign_endpoint: String::new(),
            kwai_api_key: String::new(),
            kwai_default_token: String::new(),
        }
    }
}

impl ServerConfig {
    /// Load configuration with priority: Environment Variables > `config.toml` > Built-in defaults.
    pub fn load() -> Self {
        let mut cfg = Self::default();
        let path_str = env::var("SIGN_CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());
        let path = Path::new(&path_str);

        if path.exists() {
            match fs::read_to_string(path) {
                Ok(content) => match toml::from_str::<FileConfig>(&content) {
                    Ok(file_cfg) => {
                        cfg.config_source = path_str.clone();
                        if let Some(v) = file_cfg.server.host {
                            cfg.host = v;
                        }
                        if let Some(v) = file_cfg.server.port {
                            cfg.port = v;
                        }
                        if let Some(v) = file_cfg.server.script_path {
                            cfg.script_path = v;
                        }
                        if let Some(v) = file_cfg.server.js_worker_count {
                            cfg.js_worker_count = v.max(1);
                        }
                        if let Some(v) = file_cfg.logging.log_to_file {
                            cfg.log_to_file = v;
                        }
                        if let Some(v) = file_cfg.logging.log_dir {
                            cfg.log_dir = v;
                        }
                        if let Some(v) = file_cfg.auth.simulate_test_key_busy {
                            cfg.simulate_test_key_busy = v;
                        }
                        if let Some(keys) = file_cfg.auth.api_keys {
                            let cleaned: Vec<String> = keys
                                .into_iter()
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                                .collect();
                            if !cleaned.is_empty() {
                                cfg.initial_api_keys = cleaned;
                            }
                        }
                        if let Some(v) = file_cfg.kwai.sign_endpoint {
                            cfg.kwai_sign_endpoint = v.trim().to_string();
                        }
                        if let Some(v) = file_cfg.kwai.api_key {
                            cfg.kwai_api_key = v.trim().to_string();
                        }
                        if let Some(v) = file_cfg.kwai.default_token {
                            cfg.kwai_default_token = v.trim().to_string();
                        }
                        info!(config = %path_str, "Loaded configuration from TOML file");
                    }
                    Err(err) => {
                        warn!(config = %path_str, error = %err, "Failed to parse TOML config, falling back to defaults");
                    }
                },
                Err(err) => {
                    warn!(config = %path_str, error = %err, "Failed to read config file, falling back to defaults");
                }
            }
        } else {
            info!(
                config = %path_str,
                "Config file not found; using built-in defaults (copy config.example.toml to config.toml to customize)"
            );
        }

        // Apply environment variable overrides
        if let Ok(v) = env::var("SIGN_SERVER_HOST") {
            cfg.host = v;
        }
        if let Ok(v) = env::var("SIGN_SERVER_PORT") {
            if let Ok(port) = v.parse() {
                cfg.port = port;
            }
        }
        if let Ok(v) = env::var("SIGN_SCRIPT_PATH") {
            cfg.script_path = v;
        }
        if let Ok(v) = env::var("JS_WORKER_COUNT") {
            if let Ok(cnt) = v.parse::<usize>() {
                cfg.js_worker_count = cnt.max(1);
            }
        }
        if let Ok(v) = env::var("SIGN_LOG_TO_FILE") {
            cfg.log_to_file = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Ok(v) = env::var("SIGN_LOG_DIR") {
            cfg.log_dir = v;
        }
        if let Ok(v) = env::var("SIMULATE_TEST_KEY_BUSY") {
            cfg.simulate_test_key_busy = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Ok(raw) = env::var("SIGN_API_KEYS") {
            let parsed: Vec<String> = raw
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !parsed.is_empty() {
                cfg.initial_api_keys = parsed;
            }
        }
        if let Ok(v) = env::var("KWAI_SIGN_ENDPOINT") {
            cfg.kwai_sign_endpoint = v.trim().to_string();
        }
        if let Ok(v) = env::var("KWAI_API_KEY") {
            cfg.kwai_api_key = v.trim().to_string();
        }
        if let Ok(v) = env::var("KWAI_DEFAULT_TOKEN") {
            cfg.kwai_default_token = v.trim().to_string();
        }

        cfg
    }
}
