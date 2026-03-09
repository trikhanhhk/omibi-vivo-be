use lapin::{Channel, Connection, ConnectionProperties};

pub const QUEUE_TTS_REQUEST: &str = "tts_queue";
pub const QUEUE_TTS_COMPLETE: &str = "tts_complete";
pub const QUEUE_TTS_ERROR: &str = "tts_error";

pub async fn create_channel() -> Channel {
    let rabbitmq_url = std::env::var("RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/".to_string());
    let conn = Connection::connect(&rabbitmq_url, ConnectionProperties::default())
        .await
        .expect("Failed to connect to RabbitMQ");
    conn.create_channel()
        .await
        .expect("Failed to create RabbitMQ channel")
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
