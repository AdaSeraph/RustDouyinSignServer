use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Standard API response envelope matching C# `SignServer.Common.Model.ApiResultModel`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ApiResultModel {
    pub code: i32,
    pub msg: Option<String>,
    pub data: Option<Value>,
}

impl ApiResultModel {
    pub fn ok(data: Value) -> Self {
        Self {
            code: 0,
            msg: None,
            data: Some(data),
        }
    }

    pub fn business_error(msg: impl Into<String>) -> Self {
        Self {
            code: -1,
            msg: Some(msg.into()),
            data: None,
        }
    }

    pub fn internal_error(msg: impl Into<String>) -> Self {
        Self {
            code: 500,
            msg: Some(msg.into()),
            data: None,
        }
    }
}

/// Request DTO for `POST /Douyin/SignWss`.
/// Accepts PascalCase (C# compatibility), camelCase, and snake_case.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DouyinSignWssRequest {
    #[serde(alias = "ApiKey", alias = "apiKey", alias = "api_key")]
    pub api_key: Option<String>,

    #[serde(alias = "BrowserName", alias = "browserName", alias = "browser_name")]
    pub browser_name: Option<String>,

    #[serde(
        alias = "BrowserVersion",
        alias = "browserVersion",
        alias = "browser_version"
    )]
    pub browser_version: Option<String>,

    #[serde(alias = "UserUniqueId", alias = "userUniqueId", alias = "user_unique_id")]
    pub user_unique_id: Option<String>,

    #[serde(alias = "RoomId", alias = "roomId", alias = "room_id")]
    pub room_id: Option<String>,

    #[serde(alias = "LiveUrl", alias = "liveUrl", alias = "live_url")]
    pub live_url: Option<String>,
}

/// Response payload inside `ApiResultModel.Data` for `POST /Douyin/SignWss`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DouyinSignWssData {
    pub wss_url: String,
    pub signature: String,
    pub stub_md5: String,
}

/// Request DTO for `POST /Kwai/UidToEid`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KwaiUidToEidRequest {
    #[serde(alias = "UserId", alias = "userId", alias = "user_id")]
    pub user_id: Option<String>,

    #[serde(alias = "LiveStreamId", alias = "liveStreamId", alias = "live_stream_id")]
    pub live_stream_id: Option<String>,

    #[serde(alias = "Token", alias = "token")]
    pub token: Option<String>,
}

/// Request DTO for dynamic API key management in the GUI.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ManageApiKeyRequest {
    pub api_key: String,
}
