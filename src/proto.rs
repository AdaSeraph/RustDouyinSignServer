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
    decode_webcast_message_with_ts(msg, &now)
}

/// Decode a batch of Protobuf `Message`s sharing one formatted timestamp (avoids repeated clock formatting under high throughput).
pub fn decode_webcast_messages_batch(messages: &[Message]) -> Vec<DecodedLiveEvent> {
    if messages.is_empty() {
        return Vec::new();
    }
    let now = Local::now().format("%H:%M:%S").to_string();
    let mut out = Vec::with_capacity(messages.len());
    for msg in messages {
        out.push(decode_webcast_message_with_ts(msg, &now));
    }
    out
}

/// Decode an individual Protobuf `Message` using a pre-formatted timestamp string.
pub fn decode_webcast_message_with_ts(msg: &Message, timestamp: &str) -> DecodedLiveEvent {
    let now = timestamp.to_string();
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

#[derive(Debug, Clone, Serialize)]
pub struct LivePipelineBenchReport {
    pub total_messages: usize,
    pub decoded_messages: usize,
    pub dropped_messages: usize,
    pub loss_rate_pct: f64,
    pub frames_count: usize,
    pub batch_size: usize,
    pub concurrency: usize,
    pub wall_ms: f64,
    pub throughput_msg_per_sec: f64,
    pub p50_frame_ms: f64,
    pub p95_frame_ms: f64,
    pub p99_frame_ms: f64,
}

fn percentile_ms(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Simulate a hot 10w-viewer live room pushing gzipped Protobuf `PushFrame` batches concurrently
/// through the same decode + bounded mpsc pipeline used by `live.rs`.
pub async fn run_live_pipeline_benchmark(
    total_messages: usize,
    batch_size: usize,
    concurrency: usize,
) -> Result<LivePipelineBenchReport, String> {
    let total_messages = total_messages.clamp(100, 200_000);
    let batch_size = batch_size.clamp(10, 500);
    let concurrency = concurrency.clamp(1, 256);

    // Pre-build a realistic gzipped PushFrame containing `batch_size` mixed chat/gift/like messages
    let mut template_messages = Vec::with_capacity(batch_size);
    for i in 0..batch_size {
        let msg = match i % 5 {
            0 => {
                let gift = GiftMessage {
                    gift_id: 5655,
                    combo_count: (i as u64 % 10) + 1,
                    user: Some(User {
                        id: 200_000 + i as u64,
                        nick_name: format!("土豪粉丝{i}"),
                        ..Default::default()
                    }),
                    gift: Some(GiftStruct {
                        id: 5655,
                        name: "小心心".to_string(),
                        diamond_count: 1,
                        describe: String::new(),
                    }),
                    ..Default::default()
                };
                Message {
                    method: "WebcastGiftMessage".to_string(),
                    payload: gift.encode_to_vec(),
                    msg_id: i as i64,
                    ..Default::default()
                }
            }
            1 => {
                let like = LikeMessage {
                    count: 15,
                    total: 100_000 + i as u64,
                    user: Some(User {
                        id: 300_000 + i as u64,
                        nick_name: format!("点赞观众{i}"),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                Message {
                    method: "WebcastLikeMessage".to_string(),
                    payload: like.encode_to_vec(),
                    msg_id: i as i64,
                    ..Default::default()
                }
            }
            _ => {
                let chat = ChatMessage {
                    user: Some(User {
                        id: 100_000 + i as u64,
                        nick_name: format!("10w热榜观众{i}"),
                        ..Default::default()
                    }),
                    content: format!("666 主播太强了！高并发压测弹幕 #{i}"),
                    ..Default::default()
                };
                Message {
                    method: "WebcastChatMessage".to_string(),
                    payload: chat.encode_to_vec(),
                    msg_id: i as i64,
                    ..Default::default()
                }
            }
        };
        template_messages.push(msg);
    }

    let resp = Response {
        messages_list: template_messages,
        need_ack: true,
        internal_ext: "bench_internal_ext".to_string(),
        ..Default::default()
    };
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&resp.encode_to_vec())
        .map_err(|e| format!("gzip encode error: {e}"))?;
    let gz_payload = encoder
        .finish()
        .map_err(|e| format!("gzip finish error: {e}"))?;

    let template_frame = PushFrame {
        seq_id: 1,
        log_id: 999_999,
        payload_type: "msg".to_string(),
        headers_list: vec![HeadersList {
            key: "compress_type".to_string(),
            value: "gzip".to_string(),
        }],
        payload: gz_payload,
        ..Default::default()
    };
    let raw_frame = std::sync::Arc::new(template_frame.encode_to_vec());

    let frames_count = total_messages.div_ceil(batch_size);
    let actual_total = frames_count * batch_size;

    let (decode_tx, mut decode_rx) = tokio::sync::mpsc::channel::<Vec<Message>>(4096);
    let (sse_tx, mut sse_rx) = tokio::sync::mpsc::channel::<Vec<DecodedLiveEvent>>(4096);

    let wall_start = std::time::Instant::now();

    // Stage 3: SSE consumer task
    let consumer_handle = tokio::spawn(async move {
        let mut received = 0usize;
        while let Some(batch) = sse_rx.recv().await {
            received += batch.len();
        }
        received
    });

    // Stage 2: Batch decoder task
    let decoder_handle = tokio::spawn(async move {
        while let Some(raw_messages) = decode_rx.recv().await {
            let events = decode_webcast_messages_batch(&raw_messages);
            if sse_tx.send(events).await.is_err() {
                break;
            }
        }
    });

    // Stage 1: Concurrent WSS frame readers
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut producer_handles = Vec::with_capacity(frames_count);

    for _ in 0..frames_count {
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| format!("semaphore error: {e}"))?;
        let frame_bytes = std::sync::Arc::clone(&raw_frame);
        let tx = decode_tx.clone();
        let dropped_ref = std::sync::Arc::clone(&dropped);

        producer_handles.push(tokio::spawn(async move {
            let _permit = permit;
            let t0 = std::time::Instant::now();
            if let Ok((_, Some(decoded_resp))) = decode_push_frame(&frame_bytes) {
                let len = decoded_resp.messages_list.len();
                if tx.try_send(decoded_resp.messages_list).is_err() {
                    dropped_ref.fetch_add(len, std::sync::atomic::Ordering::Relaxed);
                }
            }
            t0.elapsed().as_secs_f64() * 1000.0
        }));
    }

    let mut frame_latencies = Vec::with_capacity(frames_count);
    for h in producer_handles {
        if let Ok(ms) = h.await {
            frame_latencies.push(ms);
        }
    }
    drop(decode_tx);
    let _ = decoder_handle.await;
    let decoded_messages = consumer_handle.await.unwrap_or(0);

    let wall_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let dropped_messages = dropped.load(std::sync::atomic::Ordering::Relaxed);
    let loss_rate_pct = if actual_total > 0 {
        ((actual_total.saturating_sub(decoded_messages)) as f64 / actual_total as f64) * 100.0
    } else {
        0.0
    };
    let throughput_msg_per_sec = if wall_ms > 0.0 {
        (decoded_messages as f64) / (wall_ms / 1000.0)
    } else {
        0.0
    };

    frame_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    Ok(LivePipelineBenchReport {
        total_messages: actual_total,
        decoded_messages,
        dropped_messages,
        loss_rate_pct,
        frames_count,
        batch_size,
        concurrency,
        wall_ms,
        throughput_msg_per_sec,
        p50_frame_ms: percentile_ms(&frame_latencies, 0.50),
        p95_frame_ms: percentile_ms(&frame_latencies, 0.95),
        p99_frame_ms: percentile_ms(&frame_latencies, 0.99),
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

    #[test]
    fn test_high_concurrency_batch_decode() {
        let mut messages_list = Vec::with_capacity(500);
        for i in 0..500u64 {
            let chat = ChatMessage {
                user: Some(User {
                    id: 10_000 + i,
                    nick_name: format!("观众{i}"),
                    ..Default::default()
                }),
                content: format!("高并发弹幕 #{i}"),
                ..Default::default()
            };
            messages_list.push(Message {
                method: "WebcastChatMessage".to_string(),
                payload: chat.encode_to_vec(),
                msg_id: i as i64,
                ..Default::default()
            });
        }

        let resp = Response {
            messages_list,
            need_ack: true,
            internal_ext: "batch_ext".to_string(),
            ..Default::default()
        };

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(&resp.encode_to_vec())
            .expect("gzip encode");
        let gz_payload = encoder.finish().expect("gzip finish");

        let frame = PushFrame {
            seq_id: 42,
            log_id: 123456,
            payload_type: "msg".to_string(),
            headers_list: vec![HeadersList {
                key: "compress_type".to_string(),
                value: "gzip".to_string(),
            }],
            payload: gz_payload,
            ..Default::default()
        };

        let (_, decoded_resp) = decode_push_frame(&frame.encode_to_vec()).expect("decode frame");
        let resp = decoded_resp.expect("response present");
        let batch = decode_webcast_messages_batch(&resp.messages_list);
        assert_eq!(batch.len(), 500);
        assert_eq!(batch[0].content, "高并发弹幕 #0");
        assert_eq!(batch[499].content, "高并发弹幕 #499");
    }

    #[tokio::test]
    async fn test_live_pipeline_benchmark_zero_loss() {
        let report = run_live_pipeline_benchmark(10_000, 200, 32)
            .await
            .expect("pipeline benchmark should succeed");
        assert_eq!(report.total_messages, 10_000);
        assert_eq!(report.decoded_messages, 10_000);
        assert_eq!(report.dropped_messages, 0);
        assert_eq!(report.loss_rate_pct, 0.0);
    }
}
