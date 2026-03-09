import os


class RabbitMQConfig:
    """RabbitMQ Configuration - matches Rust backend queue names."""
    HOST = os.getenv('RABBITMQ_HOST', 'localhost')
    PORT = int(os.getenv('RABBITMQ_PORT', 5672))
    USERNAME = os.getenv('RABBITMQ_USER', 'guest')
    PASSWORD = os.getenv('RABBITMQ_PASS', 'guest')
    VHOST = os.getenv('RABBITMQ_VHOST', '/')

    # Queues - must match Rust backend
    QUEUE_TTS_REQUEST = 'tts_queue'        # Rust publishes TtsJob here
    QUEUE_TTS_COMPLETE = 'tts_complete'    # Python publishes completion here
    QUEUE_TTS_ERROR = 'tts_error'          # Python publishes errors here

    # Output directory for generated audio files
    OUTPUT_DIR = os.getenv('TTS_OUTPUT_DIR', 'outputs')
