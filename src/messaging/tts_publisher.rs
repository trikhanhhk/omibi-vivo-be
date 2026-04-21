use lapin::{BasicProperties, Channel, options::BasicPublishOptions};

use crate::infra::rabbitmq::QUEUE_TTS_REQUEST;
use crate::models::tts_job::TtsJob;

#[derive(Clone)]
pub struct TtsPublisher {
    channel: Channel,
}

impl TtsPublisher {
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }

    pub async fn publish(&self, job: &TtsJob) -> Result<(), Box<dyn std::error::Error>> {
        let payload = serde_json::to_vec(job)?;

        self.channel
            .basic_publish(
                "",
                QUEUE_TTS_REQUEST,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default(),
            )
            .await?
            .await?;

        Ok(())
    }
}
