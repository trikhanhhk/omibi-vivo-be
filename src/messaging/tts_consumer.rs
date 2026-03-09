use futures_lite::StreamExt;
use lapin::{Channel, options::BasicAckOptions, options::BasicConsumeOptions, types::FieldTable};
use serde::Deserialize;
use sqlx::PgPool;

use crate::infra::rabbitmq::{QUEUE_TTS_COMPLETE, QUEUE_TTS_ERROR};
use crate::models::tts_audio::TtsAudioStatus;
use crate::repositories::tts_audio_repository::TtsAudioRepository;

/// Message published by Python TTS service on successful synthesis
#[derive(Deserialize, Debug)]
pub struct TtsCompleteMessage {
    pub audio_id: i64,
    pub audio_url: String,
    pub status: String,
}

/// Message published by Python TTS service on failure
#[derive(Deserialize, Debug)]
pub struct TtsErrorMessage {
    pub audio_id: i64,
    pub error: String,
    pub status: String,
}

/// Spawn background consumers for tts_complete and tts_error queues.
/// These update the database when the Python TTS service finishes processing.
pub async fn spawn_tts_consumers(channel: Channel, pool: PgPool) {
    let repo = TtsAudioRepository::new(pool);

    // Consumer for completion messages
    let complete_channel = channel.clone();
    let complete_repo = repo.clone();
    tokio::spawn(async move {
        consume_complete_queue(complete_channel, complete_repo).await;
    });

    // Consumer for error messages
    let error_repo = repo.clone();
    tokio::spawn(async move {
        consume_error_queue(channel, error_repo).await;
    });
}

async fn consume_complete_queue(channel: Channel, repo: TtsAudioRepository) {
    let mut consumer = channel
        .basic_consume(
            QUEUE_TTS_COMPLETE,
            "rust-complete-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to start consuming tts_complete queue");

    println!("🟢 Listening for TTS completion messages on '{QUEUE_TTS_COMPLETE}'");

    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                match serde_json::from_slice::<TtsCompleteMessage>(&delivery.data) {
                    Ok(msg) => {
                        println!(
                            "✅ TTS complete: audio_id={} audio_url={}",
                            msg.audio_id, msg.audio_url
                        );

                        // Update audio_url and status in database
                        match repo
                            .update_audio_url_and_status(
                                msg.audio_id,
                                &msg.audio_url,
                                TtsAudioStatus::Completed,
                            )
                            .await
                        {
                            Ok(_) => println!("   📝 Updated DB for audio_id={}", msg.audio_id),
                            Err(e) => eprintln!(
                                "   ❌ Failed to update DB for audio_id={}: {}",
                                msg.audio_id, e
                            ),
                        }
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to parse completion message: {}", e);
                    }
                }

                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("Failed to ack completion message");
            }
            Err(e) => {
                eprintln!("❌ Error receiving from tts_complete: {}", e);
            }
        }
    }
}

async fn consume_error_queue(channel: Channel, repo: TtsAudioRepository) {
    let mut consumer = channel
        .basic_consume(
            QUEUE_TTS_ERROR,
            "rust-error-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to start consuming tts_error queue");

    println!("🟢 Listening for TTS error messages on '{QUEUE_TTS_ERROR}'");

    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                match serde_json::from_slice::<TtsErrorMessage>(&delivery.data) {
                    Ok(msg) => {
                        eprintln!(
                            "❌ TTS failed: audio_id={} error={}",
                            msg.audio_id, msg.error
                        );

                        // Update status to Failed in database
                        match repo
                            .update_status(msg.audio_id, TtsAudioStatus::Failed)
                            .await
                        {
                            Ok(_) => println!(
                                "   📝 Updated DB status=Failed for audio_id={}",
                                msg.audio_id
                            ),
                            Err(e) => eprintln!(
                                "   ❌ Failed to update DB for audio_id={}: {}",
                                msg.audio_id, e
                            ),
                        }
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to parse error message: {}", e);
                    }
                }

                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("Failed to ack error message");
            }
            Err(e) => {
                eprintln!("❌ Error receiving from tts_error: {}", e);
            }
        }
    }
}
