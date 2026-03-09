use futures_lite::StreamExt;
use lapin::{
    Channel,
    options::{BasicAckOptions, BasicConsumeOptions},
    types::FieldTable,
};

use crate::{
    infra::rabbitmq::QUEUE_TTS_ERROR,
    models::{tts_audio::TtsAudioStatus, tts_message::TtsErrorMessage},
    services::tts_audio_service::TtsAudioService,
};

pub async fn consume_error_queue(channel: Channel, service: TtsAudioService) {
    let mut consumer = channel
        .basic_consume(
            QUEUE_TTS_ERROR,
            "rust-error-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to start consuming tts_error queue");

    println!("Listening for TTS error messages on '{QUEUE_TTS_ERROR}'");

    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                match serde_json::from_slice::<TtsErrorMessage>(&delivery.data) {
                    Ok(msg) => {
                        eprintln!("TTS failed: audio_id={} error={}", msg.audio_id, msg.error);

                        // Update status to Failed in database
                        match service
                            .update_status(msg.audio_id, TtsAudioStatus::Failed)
                            .await
                        {
                            Ok(_) => {
                                println!("Updated DB status=Failed for audio_id={}", msg.audio_id)
                            }
                            Err(e) => eprintln!(
                                "Failed to update DB for audio_id={}: {:?}",
                                msg.audio_id, e
                            ),
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to parse error message: {}", e);
                    }
                }

                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("Failed to ack error message");
            }
            Err(e) => {
                eprintln!("Error receiving from tts_error: {}", e);
            }
        }
    }
}
