use std::time::Duration;

use lapin::{Channel, Connection, ConnectionProperties};
use tokio::time::sleep;

pub const QUEUE_TTS_REQUEST: &str = "tts_queue";
pub const QUEUE_TTS_COMPLETE: &str = "tts_complete";
pub const QUEUE_TTS_ERROR: &str = "tts_error";

pub async fn create_channel_with_retry() -> Channel {
    let rabbitmq_url = std::env::var("RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/".to_string());

    let mut retry = 0;

    loop {
        match Connection::connect(&rabbitmq_url, ConnectionProperties::default()).await {
            Ok(conn) => match conn.create_channel().await {
                Ok(channel) => {
                    println!("Successfully connected to RabbitMQ and created channel");
                    return channel;
                }
                Err(e) => {
                    eprintln!("Failed to create RabbitMQ channel: {}. Retrying...", e);
                }
            },
            Err(e) => {
                eprintln!("Failed to connect to RabbitMQ: {}. Retrying...", e);
            }
        }
        retry += 1;
        let backoff = (2_u64).pow(retry.min(5)); // Exponential backoff with a max of 32 seconds
        eprintln!("Waiting for {} seconds before retrying...", backoff);
        sleep(Duration::from_secs(backoff)).await;
    }
}

pub async fn create_channel() -> Channel {
    create_channel_with_retry().await
}

pub async fn setup_queue(channel: &Channel) {
    // Declare the request queue (Rust publishes, Python consumes)
    channel
        .queue_declare(
            QUEUE_TTS_REQUEST,
            lapin::options::QueueDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .unwrap();

    // Declare the completion queue (Python publishes, Rust consumes)
    channel
        .queue_declare(
            QUEUE_TTS_COMPLETE,
            lapin::options::QueueDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .unwrap();

    // Declare the error queue (Python publishes, Rust consumes)
    channel
        .queue_declare(
            QUEUE_TTS_ERROR,
            lapin::options::QueueDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .unwrap();
}
