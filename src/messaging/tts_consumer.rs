use lapin::Channel;

use crate::{
    messaging::{
        tts_complete_consumer::consume_complete_queue, tts_error_consumer::consume_error_queue,
    },
    services::tts_audio_service::TtsAudioService,
};

/// Spawn background consumers for tts_complete and tts_error queues.
/// These update the database when the Python TTS service finishes processing.
pub async fn spawn_tts_consumers(channel: Channel, service: TtsAudioService) {
    // Consumer for completion messages
    let complete_channel = channel.clone();
    let complete_service = service.clone();
    tokio::spawn(async move {
        consume_complete_queue(complete_channel, complete_service).await;
    });

    // Consumer for error messages
    tokio::spawn(async move {
        consume_error_queue(channel, service).await;
    });
}
