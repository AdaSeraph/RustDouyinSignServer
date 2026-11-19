use crate::config::ServerConfig;
use crate::models::{ApiResultModel, KwaiUidToEidRequest};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
struct NameValueModel {
    name: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct KwaiFeedResponse {
    result: i32,
    error_msg: Option<String>,
    #[serde(rename = "feedInfo")]
    feed_info: Option<KwaiFeedInfo>,
}

#[derive(Debug, Deserialize)]
struct KwaiFeedInfo {
    #[serde(rename = "profileUrl")]
    profile_url: Option<String>,
}

/// Async port of C# `KwaiController.UidToEid`.
pub async fn uid_to_eid(
    client: &reqwest::Client,
    config: &ServerConfig,
    req: &KwaiUidToEidRequest,
) -> ApiResultModel {
    let user_id = match req.user_id.as_deref().map(str::trim) {
        Some(v) if !v.is_empty() => v,
        _ => return ApiResultModel::business_error("UserId is required."),
    };

    let live_stream_id = match req.live_stream_id.as_deref().map(str::trim) {
        Some(v) if !v.is_empty() => v,
        _ => return ApiResultModel::business_error("LiveStreamId is required."),
    };

    if config.kwai_sign_endpoint.trim().is_empty() {
        return ApiResultModel::business_error(
            "未配置 [kwai].sign_endpoint，请在 config.toml 或环境变量 KWAI_SIGN_ENDPOINT 中设置。",
        );
    }

    let url_params = vec![
        NameValueModel {
            name: "appver".into(),
            value: "5.78.1.3231".into(),
        },
        NameValueModel {
            name: "sys".into(),
            value: "PC_11".into(),
        },
        NameValueModel {
            name: "did".into(),
            value: "".into(),
        },
        NameValueModel {
            name: "client_key".into(),
            value: "74901a18".into(),
        },
        NameValueModel {
            name: "mobileCountryCode".into(),
            value: "".into(),
        },
        NameValueModel {
            name: "country_code".into(),
            value: "cn".into(),
        },
        NameValueModel {
            name: "language".into(),
            value: "zh-Hans-CN;q=1".into(),
        },
        NameValueModel {
            name: "kpn".into(),
            value: "KUAISHOU_LIVE_MATE".into(),
        },
        NameValueModel {
            name: "kpf".into(),
            value: "WINDOWS_PC".into(),
        },
    ];

    let form_params = vec![
        NameValueModel {
            name: "liveStreamId".into(),
            value: live_stream_id.to_string(),
        },
        NameValueModel {
            name: "userId".into(),
            value: user_id.to_string(),
        },
    ];

    let mut sign_params = Vec::with_capacity(url_params.len() + form_params.len());
    sign_params.extend(url_params.clone());
    sign_params.extend(form_params);

    let sign_body = json!({
        "ApiKey": config.kwai_api_key,
        "Params": sign_params,
    });

    let sig_resp = match client
        .post(&config.kwai_sign_endpoint)
        .json(&sign_body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return ApiResultModel::internal_error(format!("查询用户信息获取签名失败: {e}"));
        }
    };

    let sig_model: ApiResultModel = match sig_resp.json().await {
        Ok(m) => m,
        Err(e) => {
            return ApiResultModel::internal_error(format!("解析签名服务响应失败: {e}"));
        }
    };

    if sig_model.code != 0 {
        return ApiResultModel::internal_error("查询用户信息获取签名失败");
    }

    let sig = match sig_model
        .data
        .as_ref()
        .and_then(|d| d.as_str())
        .filter(|s| !s.trim().is_empty())
    {
        Some(s) => s.to_string(),
        None => return ApiResultModel::internal_error("查询用户信息获取签名失败"),
    };

    let mut query_str = url_params
        .iter()
        .map(|p| format!("{}={}", p.name, p.value))
        .collect::<Vec<_>>()
        .join("&");
    query_str.push_str(&format!("&sig={sig}"));

    let token = req
        .token
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&config.kwai_default_token);

    let target_url = format!(
        "https://apijs2.gifshow.com/rest/n/live/mate/pc/profile/feed?{query_str}"
    );
    let body = format!("liveStreamId={live_stream_id}&userId={user_id}");

    let feed_resp = match client
        .post(&target_url)
        .header("Accept", "*/*")
        .header("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
        .header("User-Agent", "kuaishou 5.78.1.3231")
        .header("Cookie", format!("token={token};"))
        .body(body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return ApiResultModel::internal_error(format!("请求快手接口失败: {e}")),
    };

    match feed_resp.json::<KwaiFeedResponse>().await {
        Ok(model) => {
            if model.result != 1 {
                return ApiResultModel::business_error(
                    model.error_msg.unwrap_or_else(|| "其他原因".to_string()),
                );
            }
            if let Some(profile_url) = model.feed_info.and_then(|f| f.profile_url) {
                if !profile_url.trim().is_empty() {
                    let eid = profile_url.replace("https://www.kuaishou.com/profile/", "");
                    return ApiResultModel::ok(json!(eid));
                }
            }
            ApiResultModel {
                code: -1,
                msg: None,
                data: Some(json!("Not Found")),
            }
        }
        Err(e) => ApiResultModel::internal_error(format!("解析快手接口响应失败: {e}")),
    }
}
