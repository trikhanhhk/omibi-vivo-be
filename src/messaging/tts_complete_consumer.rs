use crate::{
    infra::rabbitmq::QUEUE_TTS_COMPLETE,
    models::{tts_audio::TtsAudioStatus, tts_message::TtsCompleteMessage},
    services::tts_audio_service::TtsAudioService,
};
use futures_lite::StreamExt;
use lapin::{
    Channel,
    options::{BasicAckOptions, BasicConsumeOptions},
    types::FieldTable,
};

pub async fn consume_complete_queue(channel: Channel, service: TtsAudioService) {
    let mut consumer = channel
        .basic_consume(
            QUEUE_TTS_COMPLETE,
            "rust-complete-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to start consuming tts_complete queue");

    println!("Listening for TTS completion messages on '{QUEUE_TTS_COMPLETE}'");

    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                match serde_json::from_slice::<TtsCompleteMessage>(&delivery.data) {
                    Ok(msg) => {
                        println!(
                            "TTS complete: audio_id={} audio_url={}",
                            msg.audio_id, msg.audio_url
                        );

                        // Update audio_url and status in database
                        match service
                            .update_audio_url_and_status(
                                msg.audio_id,
                                &msg.audio_url,
                                TtsAudioStatus::Completed,
                            )
                            .await
                        {
                            Ok(_) => {
                                println!(
                                    "Updated DB audio_url and status=Completed for audio_id={}",
                                    msg.audio_id
                                )
                            }
                            Err(e) => eprintln!(
                                "Failed to update DB for audio_id={}: {:?}",
                                msg.audio_id, e
                            ),
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to parse completion message: {}", e);
                    }
                }

                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("Failed to ack completion message");
            }
            Err(e) => {
                eprintln!("Error receiving from tts_complete: {}", e);
            }
        }
    }
}
