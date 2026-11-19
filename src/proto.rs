use chrono::Local;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use prost::Message as ProstMessage;
use serde::Serialize;
use std::io::{Read, Write};

/// Auto-generated Rust structs from `proto/douyin_webcast.proto` via `prost-build`.
pub mod webcast {
    include!(concat!(env!("OUT_DIR"), "/douyin.webcast.rs"));
}

use webcast::{
    ChatMessage, ControlMessage, GiftMessage, GiftStruct, HeadersList, LikeMessage, MemberMessage,
    Message, PushFrame, Response, RoomUserSeqMessage, SocialMessage, User,
};

/// Unified JSON-friendly event emitted after decoding Douyin Webcast Protobuf messages.
#[derive(Debug, Clone, Serialize)]
pub struct DecodedLiveEvent {
    pub timestamp: String,
    pub event_type: String,
    pub method: String,
    pub user: Option<String>,
    pub user_id: Option<u64>,
    pub content: String,
    pub extra: Option<serde_json::Value>,
}

/// Decode a raw binary WebSocket frame into `PushFrame` and (if payload is a message batch) `Response`.
pub fn decode_push_frame(raw: &[u8]) -> Result<(PushFrame, Option<Response>), String> {
    let frame = PushFrame::decode(raw).map_err(|e| format!("PushFrame decode error: {e}"))?;

    if frame.payload.is_empty() {
        return Ok((frame, None));
    }

    let is_gzip_header = frame
        .headers_list
        .iter()
        .any(|h| h.key.eq_ignore_ascii_case("compress_type") && h.value.eq_ignore_ascii_case("gzip"));
    let is_gzip_magic = frame.payload.len() >= 2 && frame.payload[0] == 0x1f && frame.payload[1] == 0x8b;

    let payload_bytes = if is_gzip_header || is_gzip_magic {
        let mut decoder = GzDecoder::new(&frame.payload[..]);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| format!("Gzip decompress error: {e}"))?;
        decompressed
    } else {
        frame.payload.clone()
    };

    if frame.payload_type == "msg" || frame.payload_type.is_empty() {
        let resp = Response::decode(&payload_bytes[..])
            .map_err(|e| format!("Response decode error: {e}"))?;
        Ok((frame, Some(resp)))
    } else {
        Ok((frame, None))
    }
}

/// Encode an ACK `PushFrame` required by Douyin Webcast push servers when `need_ack == true`.
pub fn encode_ack_frame(log_id: u64, internal_ext: &str) -> Vec<u8> {
    let ack = PushFrame {
        log_id,
        payload_type: "ack".to_string(),
        payload: internal_ext.as_bytes().to_vec(),
        ..Default::default()
    };
    ack.encode_to_vec()
}

/// Encode a heartbeat `PushFrame`.
pub fn encode_heartbeat_frame() -> Vec<u8> {
    let hb = PushFrame {
        payload_type: "hb".to_string(),
        ..Default::default()
    };
    hb.encode_to_vec()
}

/// Decode an individual Protobuf `Message` inside `Response.messages_list`.
pub fn decode_webcast_message(msg: &Message) -> DecodedLiveEvent {
    let now = Local::now().format("%H:%M:%S").to_string();
    let method = msg.method.clone();

    match method.as_str() {
        "WebcastChatMessage" => match ChatMessage::decode(&msg.payload[..]) {
            Ok(chat) => {
                let (nick, uid) = extract_user(chat.user.as_ref());
                DecodedLiveEvent {
                    timestamp: now,
                    event_type: "chat".to_string(),
                    method,
                    user: Some(nick),
                    user_id: uid,
                    content: chat.content,
                    extra: None,
                }
            }
            Err(e) => decode_err_event(now, method, e),
        },
        "WebcastGiftMessage" => match GiftMessage::decode(&msg.payload[..]) {
            Ok(gift) => {
                let (nick, uid) = extract_user(gift.user.as_ref());
                let gift_name = gift
                    .gift
                    .as_ref()
                    .map(|g| g.name.clone())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| format!("礼物#{}", gift.gift_id));
                let count = gift.combo_count.max(gift.repeat_count).max(1);
                let diamonds = gift.gift.as_ref().map(|g| g.diamond_count).unwrap_or(0);
                DecodedLiveEvent {
                    timestamp: now,
                    event_type: "gift".to_string(),
                    method,
                    user: Some(nick),
                    user_id: uid,
                    content: format!("送出 {gift_name} x{count}"),
                    extra: Some(serde_json::json!({
                        "gift_id": gift.gift_id,
                        "gift_name": gift_name,
                        "count": count,
                        "diamond_count": diamonds
                    })),
                }
            }
            Err(e) => decode_err_event(now, method, e),
        },
        "WebcastMemberMessage" => match MemberMessage::decode(&msg.payload[..]) {
            Ok(member) => {
                let (nick, uid) = extract_user(member.user.as_ref());
                let desc = if member.action_description.is_empty() {
                    "来了".to_string()
                } else {
                    member.action_description
                };
                DecodedLiveEvent {
                    timestamp: now,
                    event_type: "member".to_string(),
                    method,
                    user: Some(nick),
                    user_id: uid,
                    content: format!("{desc}（当前场观/人数: {}）", member.member_count),
                    extra: Some(serde_json::json!({ "member_count": member.member_count })),
                }
            }
            Err(e) => decode_err_event(now, method, e),
        },
        "WebcastLikeMessage" => match LikeMessage::decode(&msg.payload[..]) {
            Ok(like) => {
                let (nick, uid) = extract_user(like.user.as_ref());
                DecodedLiveEvent {
                    timestamp: now,
                    event_type: "like".to_string(),
                    method,
                    user: Some(nick),
                    user_id: uid,
                    content: format!("点赞 x{}（总点赞: {}）", like.count, like.total),
                    extra: Some(serde_json::json!({ "count": like.count, "total": like.total })),
                }
            }
            Err(e) => decode_err_event(now, method, e),
        },
        "WebcastSocialMessage" => match SocialMessage::decode(&msg.payload[..]) {
            Ok(social) => {
                let (nick, uid) = extract_user(social.user.as_ref());
                let action_text = match social.action {
                    1 => "关注了主播",
                    3 => "分享了直播间",
                    _ => "触发了社交互动",
                };
                DecodedLiveEvent {
                    timestamp: now,
                    event_type: "social".to_string(),
                    method,
                    user: Some(nick),
                    user_id: uid,
                    content: action_text.to_string(),
                    extra: Some(serde_json::json!({
                        "action": social.action,
                        "follow_count": social.follow_count
                    })),
                }
            }
            Err(e) => decode_err_event(now, method, e),
        },
        "WebcastRoomUserSeqMessage" => match RoomUserSeqMessage::decode(&msg.payload[..]) {
            Ok(seq) => DecodedLiveEvent {
                timestamp: now,
                event_type: "stats".to_string(),
                method,
                user: None,
                user_id: None,
                content: format!(
                    "当前在线: {} | 累计观看: {}",
                    seq.total,
                    if seq.total_pv_for_anchor.is_empty() {
                        seq.total_user.to_string()
                    } else {
                        seq.total_pv_for_anchor.clone()
                    }
                ),
                extra: Some(serde_json::json!({
                    "online": seq.total,
                    "total_user": seq.total_user,
                    "total_pv_for_anchor": seq.total_pv_for_anchor
                })),
            },
            Err(e) => decode_err_event(now, method, e),
        },
        "WebcastControlMessage" => match ControlMessage::decode(&msg.payload[..]) {
            Ok(ctrl) => {
                let status_text = match ctrl.status {
                    1 => "直播暂停",
                    2 => "直播恢复",
                    3 => "直播已结束",
                    other => &format!("状态码 {other}"),
                };
                DecodedLiveEvent {
                    timestamp: now,
                    event_type: "control".to_string(),
                    method,
                    user: None,
                    user_id: None,
                    content: format!("直播间控制事件: {status_text}"),
                    extra: Some(serde_json::json!({ "status": ctrl.status })),
                }
            }
            Err(e) => decode_err_event(now, method, e),
        },
        _ => DecodedLiveEvent {
            timestamp: now,
            event_type: "other".to_string(),
            method,
            user: None,
            user_id: None,
            content: format!("收到未解析扩展消息 ({} bytes)", msg.payload.len()),
            extra: None,
        },
    }
}

fn extract_user(user: Option<&User>) -> (String, Option<u64>) {
    match user {
        Some(u) => (
            if u.nick_name.is_empty() {
                format!("用户{}", u.id)
            } else {
                u.nick_name.clone()
            },
            Some(u.id),
        ),
        None => ("匿名用户".to_string(), None),
    }
}

fn decode_err_event(timestamp: String, method: String, err: prost::DecodeError) -> DecodedLiveEvent {
    DecodedLiveEvent {
        timestamp,
        event_type: "error".to_string(),
        method,
        user: None,
        user_id: None,
        content: format!("Protobuf 解码失败: {err}"),
        extra: None,
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtobufSelfTestReport {
    pub proto_file: String,
    pub encoded_frame_bytes: usize,
    pub encoded_frame_hex_preview: String,
    pub decoded_messages: Vec<DecodedLiveEvent>,
}

/// Offline round-trip self-test proving `prost-build` Protobuf generation, Gzip compression, and decoding.
pub fn run_protobuf_selftest() -> Result<ProtobufSelfTestReport, String> {
    let chat = ChatMessage {
        user: Some(User {
            id: 10001,
            nick_name: "Rust学习者".to_string(),
            ..Default::default()
        }),
        content: "你好，这是通过 prost-build 自动生成的 Protobuf 弹幕测试消息！".to_string(),
        ..Default::default()
    };

    let gift = GiftMessage {
        gift_id: 5655,
        combo_count: 3,
        user: Some(User {
            id: 10002,
            nick_name: "ECS架构师".to_string(),
            ..Default::default()
        }),
        gift: Some(GiftStruct {
            id: 5655,
            name: "小心心".to_string(),
            diamond_count: 1,
            describe: "测试礼物".to_string(),
        }),
        ..Default::default()
    };

    let resp = Response {
        messages_list: vec![
            Message {
                method: "WebcastChatMessage".to_string(),
                payload: chat.encode_to_vec(),
                msg_id: 1,
                ..Default::default()
            },
            Message {
                method: "WebcastGiftMessage".to_string(),
                payload: gift.encode_to_vec(),
                msg_id: 2,
                ..Default::default()
            },
        ],
        need_ack: true,
        internal_ext: "selftest_ack_ext".to_string(),
        ..Default::default()
    };

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&resp.encode_to_vec())
        .map_err(|e| format!("gzip encode error: {e}"))?;
    let gz_payload = encoder.finish().map_err(|e| format!("gzip finish error: {e}"))?;

    let frame = PushFrame {
        seq_id: 1,
        log_id: 999999,
        payload_type: "msg".to_string(),
        headers_list: vec![HeadersList {
            key: "compress_type".to_string(),
            value: "gzip".to_string(),
        }],
        payload: gz_payload,
        ..Default::default()
    };

    let raw_bytes = frame.encode_to_vec();
    let (_, decoded_resp) = decode_push_frame(&raw_bytes)?;
    let decoded_messages = decoded_resp
        .map(|r| r.messages_list.iter().map(decode_webcast_message).collect())
        .unwrap_or_default();

    let hex_full = hex::encode(&raw_bytes);
    let preview = if hex_full.len() > 96 {
        format!("{}...", &hex_full[..96])
    } else {
        hex_full
    };

    Ok(ProtobufSelfTestReport {
        proto_file: "proto/douyin_webcast.proto".to_string(),
        encoded_frame_bytes: raw_bytes.len(),
        encoded_frame_hex_preview: preview,
        decoded_messages,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protobuf_pushframe_roundtrip() {
        let report = run_protobuf_selftest().expect("protobuf selftest should pass");
        assert_eq!(report.decoded_messages.len(), 2);
        assert_eq!(report.decoded_messages[0].event_type, "chat");
        assert_eq!(report.decoded_messages[0].user.as_deref(), Some("Rust学习者"));
        assert_eq!(report.decoded_messages[1].event_type, "gift");
        assert!(report.decoded_messages[1].content.contains("小心心"));
    }
}
