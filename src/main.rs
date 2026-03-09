use dotenvy::dotenv;
use ominihub_vivoice::{
    app::{AppState, create_app},
    config::database,
    infra::rabbitmq::{create_channel, setup_queue},
    messaging::tts_consumer::spawn_tts_consumers,
};

#[tokio::main]
async fn main() {
    dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = database::connect_db(&database_url)
        .await
        .expect("Failed to connect to the database");

    // Setup RabbitMQ consumer channel and spawn background consumers
    let consumer_channel = create_channel().await;
    setup_queue(&consumer_channel).await;
    spawn_tts_consumers(consumer_channel, pool.clone()).await;

    let app = create_app().with_state(AppState::new(pool).await);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();

    println!("🚀 Server running on http://0.0.0.0:8000");
    axum::serve(listener, app).await.unwrap();
}
