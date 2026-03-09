"""
TTS RabbitMQ Service

Consumes TtsJob messages from the Rust backend (tts_queue),
runs VieNeu-TTS inference, saves WAV to output directory,
and publishes completion/error messages back.

Message format from Rust backend (TtsJob):
    {"audio_id": 123, "text": "Xin chào", "tts_model": "ngochuyen"}

Completion message (published to tts_complete):
    {"audio_id": 123, "audio_url": "outputs/123.wav", "status": "Completed"}

Error message (published to tts_error):
    {"audio_id": 123, "error": "...", "status": "Failed"}

Usage:
    python -m rabbitmq.rabitmq_service
    # or from tts-service/:
    python rabbitmq/rabitmq_service.py --mode standard
"""

import argparse
import io
import json
import logging
import os
import signal
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pika
import soundfile as sf

# Add parent dir to path so we can import vieneu
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rabbitmq.rabbitmq_config import RabbitMQConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("TTS.RabbitMQ")


class TTSRabbitMQService:
    """Service for handling RabbitMQ interactions for TTS audio generation.
    
    Consumes TtsJob from Rust backend, runs TTS, saves output,
    and publishes completion/error messages.
    """

    def __init__(
        self,
        model_key: str = "ngochuyen",
        voice_key: str = "ngoc",
        tts_mode: str = "standard",
        output_dir: str = None,
        **tts_kwargs,
    ):
        self.model_key = model_key
        self.voice_key = voice_key
        self.tts_mode = tts_mode
        self.tts_kwargs = tts_kwargs
        self.output_dir = output_dir or RabbitMQConfig.OUTPUT_DIR

        self.connection: Optional[pika.BlockingConnection] = None
        self.channel = None
        self.publish_channel = None
        self.tts_engine = None
        self._running = False

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # TTS Engine
    # ------------------------------------------------------------------
    def _init_tts(self) -> None:
        """Lazily initialize the TTS engine."""
        if self.tts_engine is not None:
            return

        from vieneu import Vieneu

        logger.info(f"🚀 Initializing VieNeu-TTS engine (mode={self.tts_mode}) ...")
        self.tts_engine = Vieneu(mode=self.tts_mode, **self.tts_kwargs)
        logger.info("✅ TTS engine ready.")

    def _synthesize(self, text: str, voice_name: Optional[str] = None) -> bytes:
        """Run TTS inference and return WAV bytes."""
        self._init_tts()

        voice_data = None
        if voice_name:
            try:
                voice_data = self.tts_engine.get_preset_voice(voice_name)
            except ValueError:
                logger.warning(f"Voice '{voice_name}' not found, using default.")

        audio: np.ndarray = self.tts_engine.infer(text=text, voice=voice_data)

        # Encode as WAV into an in-memory buffer
        buf = io.BytesIO()
        sf.write(buf, audio, self.tts_engine.sample_rate, format="WAV")
        buf.seek(0)
        return buf.read()

    # ------------------------------------------------------------------
    # RabbitMQ Connection
    # ------------------------------------------------------------------
    def connect(self):
        """Establish connection to RabbitMQ and declare necessary queues."""
        try:
            credentials = pika.PlainCredentials(
                RabbitMQConfig.USERNAME, RabbitMQConfig.PASSWORD
            )
            parameters = pika.ConnectionParameters(
                host=RabbitMQConfig.HOST,
                port=RabbitMQConfig.PORT,
                virtual_host=RabbitMQConfig.VHOST,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # Create separate channel for publishing (avoid deadlocks)
            self.publish_channel = self.connection.channel()

            self._setup_queues()

            logger.info(
                f"🔗 Connected to RabbitMQ at "
                f"{RabbitMQConfig.HOST}:{RabbitMQConfig.PORT}"
            )
        except Exception as e:
            logger.error(f"❌ Failed to connect to RabbitMQ: {e}")
            raise

    def _setup_queues(self):
        """Declare all queues used by the service."""
        queues = [
            RabbitMQConfig.QUEUE_TTS_REQUEST,   # tts_queue   (consume from)
            RabbitMQConfig.QUEUE_TTS_COMPLETE,   # tts_complete (publish to)
            RabbitMQConfig.QUEUE_TTS_ERROR,      # tts_error   (publish to)
        ]

        for queue in queues:
            self.channel.queue_declare(queue=queue, durable=False)

        # Only process one message at a time
        self.channel.basic_qos(prefetch_count=1)

        logger.info("✅ Queues declared: " + ", ".join(queues))

    # ------------------------------------------------------------------
    # Publishing helpers
    # ------------------------------------------------------------------
    def _publish_complete(self, audio_id: int, audio_url: str):
        """Publish a completion message to tts_complete queue."""
        message = {
            "audio_id": audio_id,
            "audio_url": audio_url,
            "status": "Completed",
        }
        self.publish_channel.basic_publish(
            exchange="",
            routing_key=RabbitMQConfig.QUEUE_TTS_COMPLETE,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # persistent
            ),
            body=json.dumps(message).encode("utf-8"),
        )
        logger.info(f"   📤 Published completion for audio_id={audio_id}")

    def _publish_error(self, audio_id: int, error: str):
        """Publish an error message to tts_error queue."""
        message = {
            "audio_id": audio_id,
            "error": error,
            "status": "Failed",
        }
        self.publish_channel.basic_publish(
            exchange="",
            routing_key=RabbitMQConfig.QUEUE_TTS_ERROR,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
            body=json.dumps(message).encode("utf-8"),
        )
        logger.info(f"   📤 Published error for audio_id={audio_id}")

    # ------------------------------------------------------------------
    # Message Handler
    # ------------------------------------------------------------------
    def _on_request(self, ch, method, properties, body):
        """
        Handle an incoming TTS request from the Rust backend.

        Expected JSON body (TtsJob):
        {
            "audio_id": 123,
            "text": "Xin chào Việt Nam",
            "tts_model": "ngochuyen"
        }
        """
        delivery_tag = method.delivery_tag
        audio_id = None

        try:
            payload: Dict[str, Any] = json.loads(body)
            audio_id = payload.get("audio_id")
            text = payload.get("text", "").strip()
            tts_model = payload.get("tts_model", "")

            if not text:
                raise ValueError("Missing or empty 'text' field in TtsJob.")
            if audio_id is None:
                raise ValueError("Missing 'audio_id' field in TtsJob.")

            logger.info(
                f"📩 Received TtsJob [audio_id={audio_id}] "
                f"text={text[:80]}{'...' if len(text) > 80 else ''} "
                f"model={tts_model}"
            )

            # Determine voice from model key
            voice_name = self._resolve_voice(tts_model)

            # Run TTS synthesis
            start = time.perf_counter()
            wav_bytes = self._synthesize(text, voice_name)
            elapsed = time.perf_counter() - start

            logger.info(
                f"   ✅ Synthesized {len(wav_bytes)} bytes in {elapsed:.2f}s"
            )

            # Save to output file
            output_filename = f"{audio_id}.wav"
            output_path = os.path.join(self.output_dir, output_filename)
            with open(output_path, "wb") as f:
                f.write(wav_bytes)

            logger.info(f"   💾 Saved audio → {output_path}")

            # Publish completion message
            audio_url = f"{self.output_dir}/{output_filename}"
            self._publish_complete(audio_id, audio_url)

        except Exception as exc:
            logger.error(f"   ❌ Error processing TtsJob: {exc}")
            traceback.print_exc()

            # Publish error message if we have the audio_id
            if audio_id is not None:
                self._publish_error(audio_id, str(exc))

        finally:
            ch.basic_ack(delivery_tag=delivery_tag)

    def _resolve_voice(self, tts_model: str) -> Optional[str]:
        """Map tts_model string to a preset voice name."""
        # Map model keys to voice presets
        voice_map = {
            "ngochuyen": "ngoc",
            "ngoc": "ngoc",
            "tuyen": "Tuyen",
            "binh": "Binh",
        }
        voice = voice_map.get(tts_model.lower()) if tts_model else None
        if voice:
            logger.info(f"   🎤 Using voice preset: {voice}")
        return voice

    # ------------------------------------------------------------------
    # Main Loop
    # ------------------------------------------------------------------
    def start(self):
        """Start consuming messages from tts_queue. Blocks until stopped."""
        self._init_tts()
        self.connect()

        queue_name = RabbitMQConfig.QUEUE_TTS_REQUEST
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=self._on_request,
        )

        self._running = True
        logger.info(f"🟢 TTS Worker listening on '{queue_name}'. Press Ctrl+C to stop.")

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("⏹️  Shutting down gracefully ...")
        finally:
            self.stop()

    def stop(self):
        """Clean shutdown."""
        self._running = False
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass

        if self.tts_engine is not None:
            try:
                self.tts_engine.close()
            except Exception:
                pass
            self.tts_engine = None

        logger.info("👋 TTS Worker stopped.")


# ======================================================================
# CLI entry-point
# ======================================================================
def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="TTS RabbitMQ Service - consume TTS jobs from Rust backend"
    )
    parser.add_argument(
        "--mode", type=str, default="standard",
        choices=["standard", "fast", "gpu", "remote", "xpu"],
        help="TTS engine mode (default: standard)",
    )
    parser.add_argument(
        "--model-key", type=str, default="ngochuyen",
        help="Default model key (default: ngochuyen)",
    )
    parser.add_argument(
        "--voice-key", type=str, default="ngoc",
        help="Default voice key (default: ngoc)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory for generated audio files",
    )
    parser.add_argument(
        "--backbone-repo", type=str, default=None,
        help="Override backbone model repo",
    )
    parser.add_argument(
        "--codec-repo", type=str, default=None,
        help="Override codec repo",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    # Build TTS kwargs
    tts_kwargs: Dict[str, Any] = {}
    if args.backbone_repo:
        tts_kwargs["backbone_repo"] = args.backbone_repo
    if args.codec_repo:
        tts_kwargs["codec_repo"] = args.codec_repo

    service = TTSRabbitMQService(
        model_key=args.model_key,
        voice_key=args.voice_key,
        tts_mode=args.mode,
        output_dir=args.output_dir,
        **tts_kwargs,
    )

    # Handle SIGTERM for graceful shutdown (Docker, systemd, etc.)
    def _signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, stopping ...")
        service.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    service.start()


if __name__ == "__main__":
    main()