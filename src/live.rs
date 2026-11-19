use crate::ecs::EcsHandle;
use crate::models::DouyinSignWssRequest;
use crate::proto::{
    DecodedLiveEvent, decode_push_frame, decode_webcast_message, encode_ack_frame,
    encode_heartbeat_frame,
};
use chrono::Local;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use regex::Regex;
use serde::Serialize;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{COOKIE, ORIGIN, USER_AGENT};
use tracing::{debug, info, warn};

const BROWSER_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

#[derive(Debug, Clone, Serialize)]
pub struct LiveRoomInfo {
    pub live_url: String,
    pub web_rid: String,
    pub room_id: String,
    pub user_unique_id: String,
    pub ttwid: String,
    pub title: String,
    pub anchor_name: String,
    pub status: i64,
    pub status_text: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LiveStreamEvent {
    Status {
        timestamp: String,
        message: String,
    },
    RoomInfo {
        room: LiveRoomInfo,
        wss_url: String,
        signature: String,
    },
    LiveMessage {
        event: DecodedLiveEvent,
    },
    Error {
        timestamp: String,
        message: String,
    },
}

fn now_hms() -> String {
    Local::now().format("%H:%M:%S").to_string()
}

fn generate_user_unique_id() -> String {
    let mut rng = rand::rng();
    let suffix: u64 = rng.random_range(100_000_000_000_000..999_999_999_999_999);
    format!("7410{suffix}")
}

/// Extract `web_rid` from inputs such as:
/// - `https://live.douyin.com/80017709309`
/// - `https://live.douyin.com/80017709309?enter_from_merge=...`
/// - `80017709309`
pub fn extract_web_rid(input: &str) -> Result<String, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("请输入抖音直播间地址或房间号".to_string());
    }

    if trimmed.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && !trimmed.contains('.')
    {
        return Ok(trimmed.to_string());
    }

    let re = Regex::new(r"live\.douyin\.com/([A-Za-z0-9_-]+)").unwrap();
    if let Some(cap) = re.captures(trimmed) {
        return Ok(cap[1].to_string());
    }

    Err(format!(
        "无法从地址 '{trimmed}' 提取抖音直播间 web_rid（示例：https://live.douyin.com/80017709309）"
    ))
}

/// Fetch `ttwid` cookie from Douyin Live homepage if the caller didn't supply one.
async fn fetch_ttwid(client: &reqwest::Client) -> Result<String, String> {
    let resp = client
        .get("https://live.douyin.com/")
        .header("User-Agent", BROWSER_UA)
        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        .header("Accept-Language", "zh-CN,zh;q=0.9")
        .header("Cookie", "__ac_nonce=0123407cc00a9e438deb4")
        .send()
        .await
        .map_err(|e| format!("请求 live.douyin.com 获取 ttwid 失败: {e}"))?;

    for val in resp.headers().get_all("set-cookie") {
        if let Ok(s) = val.to_str() {
            if let Some(rest) = s.strip_prefix("ttwid=") {
                let token = rest.split(';').next().unwrap_or("").trim();
                if !token.is_empty() {
                    return Ok(token.to_string());
                }
            }
        }
    }

    Err("未能从响应头中提取 ttwid Cookie（可在表单中手动填写 ttwid 后重试）".to_string())
}

/// Resolve a Douyin live URL / web_rid into `room_id`, `ttwid`, anchor name, and live status.
pub async fn resolve_live_room(
    client: &reqwest::Client,
    live_url_or_rid: &str,
    custom_ttwid: Option<&str>,
    custom_room_id: Option<&str>,
) -> Result<LiveRoomInfo, String> {
    let web_rid = extract_web_rid(live_url_or_rid)?;
    let normalized_url = format!("https://live.douyin.com/{web_rid}");

    let ttwid = match custom_ttwid.map(str::trim).filter(|s| !s.is_empty()) {
        Some(t) => t.to_string(),
        None => fetch_ttwid(client).await?,
    };

    if let Some(rid) = custom_room_id.map(str::trim).filter(|s| !s.is_empty()) {
        return Ok(LiveRoomInfo {
            live_url: normalized_url,
            web_rid,
            room_id: rid.to_string(),
            user_unique_id: generate_user_unique_id(),
            ttwid,
            title: "（手动指定 RoomId）".to_string(),
            anchor_name: "未知主播".to_string(),
            status: 2,
            status_text: "手动指定 RoomId".to_string(),
        });
    }

    // Primary: call Douyin Webcast enter API
    let enter_url = format!(
        "https://live.douyin.com/webcast/room/web/enter/?aid=6383&app_name=douyin_web&live_id=1&device_platform=web&enter_from=web_live&cookie_enabled=true&browser_language=zh-CN&browser_platform=Win32&browser_name=Chrome&browser_version=126.0.0.0&web_rid={web_rid}"
    );

    let api_res = client
        .get(&enter_url)
        .header("User-Agent", BROWSER_UA)
        .header("Referer", &normalized_url)
        .header("Accept", "application/json, text/plain, */*")
        .header("Cookie", format!("ttwid={ttwid}; __ac_nonce=0123407cc00a9e438deb4"))
        .send()
        .await;

    if let Ok(resp) = api_res {
        if let Ok(json_val) = resp.json::<serde_json::Value>().await {
            if let Some(room) = json_val
                .get("data")
                .and_then(|d| d.get("data"))
                .and_then(|arr| arr.as_array())
                .and_then(|arr| arr.first())
            {
                let room_id = room
                    .get("id_str")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .or_else(|| room.get("id").and_then(|v| v.as_u64()).map(|n| n.to_string()))
                    .unwrap_or_default();

                if !room_id.is_empty() {
                    let title = room
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("无标题")
                        .to_string();
                    let status = room.get("status").and_then(|v| v.as_i64()).unwrap_or(0);
                    let anchor_name = json_val
                        .get("data")
                        .and_then(|d| d.get("user"))
                        .and_then(|u| u.get("nickname"))
                        .and_then(|v| v.as_str())
                        .or_else(|| {
                            room.get("owner")
                                .and_then(|o| o.get("nickname"))
                                .and_then(|v| v.as_str())
                        })
                        .unwrap_or("未知主播")
                        .to_string();
                    let status_text = match status {
                        2 => "直播中".to_string(),
                        4 => "未开播".to_string(),
                        _ => format!("状态({status})"),
                    };

                    return Ok(LiveRoomInfo {
                        live_url: normalized_url,
                        web_rid,
                        room_id,
                        user_unique_id: generate_user_unique_id(),
                        ttwid,
                        title,
                        anchor_name,
                        status,
                        status_text,
                    });
                }
            }
        }
    }

    // Fallback: parse roomId from HTML page
    let html_resp = client
        .get(&normalized_url)
        .header("User-Agent", BROWSER_UA)
        .header("Cookie", format!("ttwid={ttwid}; __ac_nonce=0123407cc00a9e438deb4"))
        .send()
        .await
        .map_err(|e| format!("请求直播间页面失败: {e}"))?;

    let html = html_resp
        .text()
        .await
        .map_err(|e| format!("读取直播间页面失败: {e}"))?;

    let re_room = Regex::new(r#"\\?"roomId\\?"\s*:\s*\\?"(\d{15,22})\\?""#).unwrap();
    let room_id = re_room
        .captures(&html)
        .map(|c| c[1].to_string())
        .ok_or_else(|| {
            "未能自动解析出 room_id（可能触发了抖音风控验证码，或主播未开播；可在界面中手动填入 room_id 与 ttwid）"
                .to_string()
        })?;

    Ok(LiveRoomInfo {
        live_url: normalized_url,
        web_rid,
        room_id,
        user_unique_id: generate_user_unique_id(),
        ttwid,
        title: "直播间（HTML解析）".to_string(),
        anchor_name: "未知主播".to_string(),
        status: 2,
        status_text: "已解析 RoomId".to_string(),
    })
}

/// Spawn a live WebSocket listener task that resolves the room, signs WSS via ECS, connects,
/// decodes Protobuf frames, and pushes `LiveStreamEvent`s to `tx`.
pub fn spawn_live_stream_task(
    client: reqwest::Client,
    ecs: EcsHandle,
    client_ip: String,
    live_url: String,
    api_key: String,
    custom_ttwid: Option<String>,
    custom_room_id: Option<String>,
    tx: mpsc::Sender<LiveStreamEvent>,
) {
    tokio::spawn(async move {
        let _ = tx
            .send(LiveStreamEvent::Status {
                timestamp: now_hms(),
                message: format!("正在解析抖音直播间地址: {live_url} ..."),
            })
            .await;

        let room = match resolve_live_room(
            &client,
            &live_url,
            custom_ttwid.as_deref(),
            custom_room_id.as_deref(),
        )
        .await
        {
            Ok(r) => r,
            Err(err) => {
                let _ = tx
                    .send(LiveStreamEvent::Error {
                        timestamp: now_hms(),
                        message: err,
                    })
                    .await;
                return;
            }
        };

        if room.status == 4 {
            let _ = tx
                .send(LiveStreamEvent::Error {
                    timestamp: now_hms(),
                    message: format!(
                        "主播「{}」当前未开播（room_id={}），请更换正在直播的直播间地址",
                        room.anchor_name, room.room_id
                    ),
                })
                .await;
            return;
        }

        let _ = tx
            .send(LiveStreamEvent::Status {
                timestamp: now_hms(),
                message: format!(
                    "解析成功：主播「{}」| 标题「{}」| RoomId={}，正在调用 ECS 管线签名...",
                    room.anchor_name, room.title, room.room_id
                ),
            })
            .await;

        // Sign WSS URL via our ECS pipeline
        let sign_res = ecs
            .sign_douyin_wss(
                client_ip,
                DouyinSignWssRequest {
                    api_key: Some(api_key),
                    room_id: Some(room.room_id.clone()),
                    user_unique_id: Some(room.user_unique_id.clone()),
                    browser_name: None,
                    browser_version: None,
                    live_url: Some(room.live_url.clone()),
                },
            )
            .await;

        if sign_res.code != 0 {
            let _ = tx
                .send(LiveStreamEvent::Error {
                    timestamp: now_hms(),
                    message: format!(
                        "ECS 签名失败 (Code={}): {}",
                        sign_res.code,
                        sign_res.msg.unwrap_or_else(|| "未知错误".to_string())
                    ),
                })
                .await;
            return;
        }

        let data = match sign_res.data {
            Some(d) => d,
            None => {
                let _ = tx
                    .send(LiveStreamEvent::Error {
                        timestamp: now_hms(),
                        message: "ECS 签名未返回数据".to_string(),
                    })
                    .await;
                return;
            }
        };

        let wss_url = data["WssUrl"].as_str().unwrap_or("").to_string();
        let signature = data["Signature"].as_str().unwrap_or("").to_string();

        let _ = tx
            .send(LiveStreamEvent::RoomInfo {
                room: room.clone(),
                wss_url: wss_url.clone(),
                signature: signature.clone(),
            })
            .await;

        let _ = tx
            .send(LiveStreamEvent::Status {
                timestamp: now_hms(),
                message: format!("签名成功（X-Bogus={signature}），正在建立 WebSocket 推流连接..."),
            })
            .await;

        let mut ws_req = match wss_url.as_str().into_client_request() {
            Ok(r) => r,
            Err(e) => {
                let _ = tx
                    .send(LiveStreamEvent::Error {
                        timestamp: now_hms(),
                        message: format!("构造 WebSocket 请求失败: {e}"),
                    })
                    .await;
                return;
            }
        };

        if let Ok(v) = BROWSER_UA.parse() {
            ws_req.headers_mut().insert(USER_AGENT, v);
        }
        if let Ok(v) = format!("ttwid={}", room.ttwid).parse() {
            ws_req.headers_mut().insert(COOKIE, v);
        }
        if let Ok(v) = "https://live.douyin.com".parse() {
            ws_req.headers_mut().insert(ORIGIN, v);
        }

        let (ws_stream, _) = match tokio_tungstenite::connect_async(ws_req).await {
            Ok(pair) => pair,
            Err(e) => {
                let _ = tx
                    .send(LiveStreamEvent::Error {
                        timestamp: now_hms(),
                        message: format!("WebSocket 握手连接失败: {e}"),
                    })
                    .await;
                return;
            }
        };

        info!(room_id = %room.room_id, "Connected to Douyin Webcast WSS push stream");
        let _ = tx
            .send(LiveStreamEvent::Status {
                timestamp: now_hms(),
                message: "WebSocket 连接已建立！正在实时解码 Protobuf 直播间消息...".to_string(),
            })
            .await;

        let (mut ws_sink, mut ws_read) = ws_stream.split();
        let mut hb_interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                _ = tx.closed() => {
                    info!(room_id = %room.room_id, "SSE client disconnected; closing Douyin WSS");
                    let _ = ws_sink.close().await;
                    break;
                }
                _ = hb_interval.tick() => {
                    let hb = encode_heartbeat_frame();
                    if let Err(e) = ws_sink.send(WsMessage::Binary(hb.into())).await {
                        warn!(error = %e, "Failed to send WebSocket heartbeat");
                        break;
                    }
                    debug!(room_id = %room.room_id, "Sent Protobuf heartbeat frame");
                }
                maybe_msg = ws_read.next() => {
                    match maybe_msg {
                        Some(Ok(WsMessage::Binary(bin))) => {
                            match decode_push_frame(&bin) {
                                Ok((frame, Some(resp))) => {
                                    if resp.need_ack {
                                        let ack = encode_ack_frame(frame.log_id, &resp.internal_ext);
                                        if let Err(e) = ws_sink.send(WsMessage::Binary(ack.into())).await {
                                            warn!(error = %e, "Failed to send ACK frame");
                                        }
                                    }
                                    for msg in &resp.messages_list {
                                        let decoded = decode_webcast_message(msg);
                                        if tx.send(LiveStreamEvent::LiveMessage { event: decoded }).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                Ok((_, None)) => {}
                                Err(err) => {
                                    debug!(error = %err, "Failed to decode PushFrame");
                                }
                            }
                        }
                        Some(Ok(WsMessage::Close(frame))) => {
                            let _ = tx.send(LiveStreamEvent::Status {
                                timestamp: now_hms(),
                                message: format!("抖音服务端关闭了 WebSocket 连接: {frame:?}"),
                            }).await;
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            let _ = tx.send(LiveStreamEvent::Error {
                                timestamp: now_hms(),
                                message: format!("WebSocket 读取异常: {e}"),
                            }).await;
                            break;
                        }
                        None => {
                            let _ = tx.send(LiveStreamEvent::Status {
                                timestamp: now_hms(),
                                message: "WebSocket 数据流已结束".to_string(),
                            }).await;
                            break;
                        }
                    }
                }
            }
        }
    });
}
