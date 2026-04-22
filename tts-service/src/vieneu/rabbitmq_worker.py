"""
VieNeu-TTS RabbitMQ Worker

Listens to a RabbitMQ queue for TTS requests. Each message contains:
  - text: The text to synthesize
  - model: (optional) TTS mode/backbone to use
  - voice: (optional) Preset voice name

After synthesis, the worker replies with the WAV audio bytes
via the RPC reply pattern (reply_to + correlation_id).

Usage:
    uv run vieneu-rabbitmq
    # or
    python -m vieneu.rabbitmq_worker --host localhost --queue tts_queue
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
import yaml
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("Vieneu.RabbitMQ")

# Maps tts_model keys (sent by Rust) to HuggingFace backbone repos
AVAILABLE_MODELS: Dict[str, str] = {
    "q4": "pnnbao-ump/VieNeu-TTS-0.3B-q4-gguf",
    "q8": "pnnbao-ump/VieNeu-TTS-0.3B-q8-gguf",
    "ngochuyen": "pnnbao-ump/VieNeu-TTS-0.3B-ngoc-huyen-gguf-Q4_0",
}


def load_rabbitmq_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load RabbitMQ settings from config.yaml, with sensible defaults."""
    defaults = {
        "host": "localhost",
        "port": 5672,
        "username": "guest",
        "password": "guest",
        "virtual_host": "/",
        "queue": "tts_queue",
        "queue_complete": "tts_complete",
        "queue_error": "tts_error",
        "prefetch_count": 1,
        "durable": True,
    }

    try:
        cfg_path = Path(config_path)
        if cfg_path.exists():
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            rmq = cfg.get("rabbitmq", {})
            defaults.update({k: v for k, v in rmq.items() if v is not None})
    except Exception as e:
        logger.warning(f"Could not load config from {config_path}: {e}. Using defaults.")

    return defaults


class TTSWorker:
    """RabbitMQ consumer that processes TTS requests and replies with audio."""

    def __init__(self, rmq_config: Dict[str, Any], tts_mode: str = "standard", **tts_kwargs):
        self.rmq_config = rmq_config
        self.tts_mode = tts_mode
        self.tts_kwargs = tts_kwargs

        self.connection: Optional[pika.BlockingConnection] = None
        self.channel = None
        self.tts_engine = None
        self._running = False
        self._current_model_key: Optional[str] = None

        # MinIO client
        minio_endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000").removeprefix("http://").removeprefix("https://")
        minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
        minio_secure = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000").startswith("https")
        self.minio_bucket = os.environ.get("MINIO_BUCKET", "ominihub")
        self.minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=minio_secure,
        )

    # ------------------------------------------------------------------
    # TTS engine
    # ------------------------------------------------------------------
    def _init_tts(self) -> None:
        """Lazily initialize the TTS engine."""
        if self.tts_engine is not None:
            return

        from vieneu import Vieneu

        logger.info(f"🚀 Initializing VieNeu-TTS engine (mode={self.tts_mode}) …")
        self.tts_engine = Vieneu(mode=self.tts_mode, **self.tts_kwargs)
        logger.info("✅ TTS engine ready.")

    def _load_model(self, model_key: str) -> None:
        """Load a specific model by key, reloading only when it changes."""
        if model_key == self._current_model_key and self.tts_engine is not None:
            return

        repo_id = AVAILABLE_MODELS.get(model_key)
        if repo_id is None:
            logger.warning(f"Unknown tts_model '{model_key}', falling back to default init.")
            self._init_tts()
            return

        from vieneu import Vieneu

        # Close previous engine if any
        if self.tts_engine is not None:
            try:
                self.tts_engine.close()
            except Exception:
                pass
            self.tts_engine = None

        logger.info(f"🔄 Loading model key='{model_key}' repo='{repo_id}' …")
        kwargs = {**self.tts_kwargs, "backbone_repo": repo_id}
        self.tts_engine = Vieneu(mode=self.tts_mode, **kwargs)
        self._current_model_key = model_key
        logger.info(f"✅ Model '{model_key}' ready.")

    def _synthesize(self, text: str, voice_name: Optional[str] = None) -> bytes:
        """
        Run TTS inference and return WAV bytes.

        Args:
            text: The text to synthesize.
            voice_name: Optional preset voice name.

        Returns:
            bytes: WAV file content.
        """
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
    # RabbitMQ connection
    # ------------------------------------------------------------------
    def _connect(self) -> None:
        """Establish connection and declare the queue."""
        credentials = pika.PlainCredentials(
            self.rmq_config["username"],
            self.rmq_config["password"],
        )
        params = pika.ConnectionParameters(
            host=self.rmq_config["host"],
            port=int(self.rmq_config["port"]),
            virtual_host=self.rmq_config["virtual_host"],
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )

        logger.info(
            f"🔗 Connecting to RabbitMQ at "
            f"{self.rmq_config['host']}:{self.rmq_config['port']} …"
        )
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        durable = self.rmq_config.get("durable", True)
        for q_key in ("queue", "queue_complete", "queue_error"):
            q_name = self.rmq_config.get(q_key)
            if q_name:
                self.channel.queue_declare(queue=q_name, durable=durable)

        queue_name = self.rmq_config["queue"]
        self.channel.basic_qos(prefetch_count=int(self.rmq_config.get("prefetch_count", 1)))

        logger.info(f"📥 Listening on queue: '{queue_name}' (durable={durable})")

    # ------------------------------------------------------------------
    # Message handler
    # ------------------------------------------------------------------
    def _publish_complete(self, ch, audio_id: int, audio_url: str) -> None:
        queue_complete = self.rmq_config.get("queue_complete", "tts_complete")
        body = json.dumps({
            "audio_id": audio_id,
            "audio_url": audio_url,
            "status": "completed",
        }).encode("utf-8")
        ch.basic_publish(
            exchange="",
            routing_key=queue_complete,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
            body=body,
        )
        logger.info(f"   📤 Published complete → '{queue_complete}' audio_id={audio_id} url={audio_url}")

    def _publish_error(self, ch, audio_id: int, error: str) -> None:
        queue_error = self.rmq_config.get("queue_error", "tts_error")
        body = json.dumps({
            "audio_id": audio_id,
            "error": error,
            "status": "failed",
        }).encode("utf-8")
        ch.basic_publish(
            exchange="",
            routing_key=queue_error,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
            body=body,
        )
        logger.info(f"   📤 Published error → '{queue_error}' audio_id={audio_id}")

    def _on_request(self, ch, method, properties, body):
        """
        Handle an incoming TTS request.

        Two modes depending on the message payload:

        1. Async mode (from tts_audio_service) — payload has ``audio_id``:
           { "audio_id": 123, "text": "...", "tts_model": "ngochuyen" }
           → saves WAV to output/audio_{id}.wav
           → publishes {audio_id, audio_url, status} to tts_complete queue

        2. RPC mode (from audio_merge_service) — no ``audio_id``, ``reply_to`` set:
           { "text": "...", "tts_model": "ngochuyen" }
           → replies with raw WAV bytes (content_type=audio/wav) to reply_to queue
           → on error replies JSON {"error": "..."} (content_type=application/json)
        """
        delivery_tag = method.delivery_tag
        audio_id: Optional[int] = None

        try:
            payload: Dict[str, Any] = json.loads(body)
            audio_id_raw = payload.get("audio_id")
            text = payload.get("text", "").strip()
            tts_model = payload.get("tts_model")

            if not text:
                raise ValueError("Missing or empty 'text' field in request.")

            # Load / switch model
            if tts_model:
                self._load_model(tts_model)
            else:
                self._init_tts()

            start = time.perf_counter()
            wav_bytes = self._synthesize(text)
            elapsed = time.perf_counter() - start

            if audio_id_raw is not None:
                # ── Async mode ──────────────────────────────────────────────
                audio_id = int(audio_id_raw)
                logger.info(
                    f"📩 [async] audio_id={audio_id} model={tts_model} "
                    f"text={text[:80]}{'…' if len(text) > 80 else ''}"
                )
                logger.info(f"   ✅ Synthesized {len(wav_bytes)} bytes in {elapsed:.2f}s")

                minio_key = f"tts/audio_{audio_id}.wav"
                wav_stream = io.BytesIO(wav_bytes)
                if not self.minio_client.bucket_exists(self.minio_bucket):
                    self.minio_client.make_bucket(self.minio_bucket)
                self.minio_client.put_object(
                    self.minio_bucket,
                    minio_key,
                    wav_stream,
                    length=len(wav_bytes),
                    content_type="audio/wav",
                )
                logger.info(f"   ☁️  Uploaded to MinIO: {self.minio_bucket}/{minio_key}")

                self._publish_complete(ch, audio_id, minio_key)

            else:
                # ── RPC mode ─────────────────────────────────────────────────
                reply_to = properties.reply_to
                correlation_id = properties.correlation_id or ""
                logger.info(
                    f"📩 [rpc] corr={correlation_id} model={tts_model} "
                    f"text={text[:80]}{'…' if len(text) > 80 else ''}"
                )
                logger.info(f"   ✅ Synthesized {len(wav_bytes)} bytes in {elapsed:.2f}s")

                if reply_to:
                    ch.basic_publish(
                        exchange="",
                        routing_key=reply_to,
                        properties=pika.BasicProperties(
                            correlation_id=correlation_id,
                            content_type="audio/wav",
                            delivery_mode=1,  # transient – reply queue is auto-delete
                        ),
                        body=wav_bytes,
                    )
                    logger.info(f"   📤 RPC reply → '{reply_to}'")
                else:
                    logger.warning("   ⚠️  RPC mode but no reply_to set – result discarded.")

        except Exception as exc:
            logger.error(f"   ❌ Error processing request: {exc}")
            traceback.print_exc()

            if audio_id is not None:
                self._publish_error(ch, audio_id, str(exc))
            elif properties.reply_to:
                # RPC error path
                error_body = json.dumps({"error": str(exc)}).encode("utf-8")
                ch.basic_publish(
                    exchange="",
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id or "",
                        content_type="application/json",
                        delivery_mode=1,
                    ),
                    body=error_body,
                )

        finally:
            ch.basic_ack(delivery_tag=delivery_tag)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Start consuming messages. Blocks until stopped."""
        self._init_tts()
        self._connect()

        queue_name = self.rmq_config["queue"]
        self.channel.basic_consume(queue=queue_name, on_message_callback=self._on_request)

        self._running = True
        logger.info("🟢 Worker is running. Press Ctrl+C to stop.")

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("⏹️  Shutting down gracefully …")
        finally:
            self.stop()

    def stop(self) -> None:
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

        logger.info("👋 Worker stopped.")


# ======================================================================
# CLI entry-point
# ======================================================================
def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="VieNeu-TTS RabbitMQ Worker – consume TTS requests from a queue"
    )
    parser.add_argument("--host", type=str, default=None, help="RabbitMQ host (default: from config)")
    parser.add_argument("--port", type=int, default=None, help="RabbitMQ port (default: from config)")
    parser.add_argument("--user", type=str, default=None, help="RabbitMQ username")
    parser.add_argument("--password", type=str, default=None, help="RabbitMQ password")
    parser.add_argument("--vhost", type=str, default=None, help="RabbitMQ virtual host")
    parser.add_argument("--queue", type=str, default=None, help="Queue name to consume from")
    parser.add_argument("--mode", type=str, default="standard",
                        choices=["standard", "fast", "gpu", "remote", "xpu"],
                        help="TTS engine mode (default: standard)")
    parser.add_argument("--backbone-repo", type=str, default=None, help="Override backbone model repo")
    parser.add_argument("--codec-repo", type=str, default=None, help="Override codec repo")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to config.yaml")
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    # Load base config, then override with CLI args
    rmq_config = load_rabbitmq_config(args.config)

    cli_overrides = {
        "host": args.host,
        "port": args.port,
        "username": args.user,
        "password": args.password,
        "virtual_host": args.vhost,
        "queue": args.queue,
    }
    for k, v in cli_overrides.items():
        if v is not None:
            rmq_config[k] = v

    # Build TTS kwargs
    tts_kwargs: Dict[str, Any] = {}
    if args.backbone_repo:
        tts_kwargs["backbone_repo"] = args.backbone_repo
    if args.codec_repo:
        tts_kwargs["codec_repo"] = args.codec_repo

    worker = TTSWorker(rmq_config=rmq_config, tts_mode=args.mode, **tts_kwargs)

    # Handle SIGTERM for graceful shutdown (Docker, systemd, etc.)
    def _signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, stopping …")
        worker.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    worker.start()


if __name__ == "__main__":
    main()
