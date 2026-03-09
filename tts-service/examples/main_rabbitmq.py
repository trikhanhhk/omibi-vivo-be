"""
VieNeu-TTS RabbitMQ Client Example

Demonstrates how the "other side" sends a TTS request to the worker
and receives the synthesized WAV audio back via the RPC pattern.

Requirements:
    pip install pika

Usage:
    python examples/main_rabbitmq.py
"""

import json
import uuid
import sys
import pika


class TTSRpcClient:
    """
    RPC client that sends text to the VieNeu-TTS RabbitMQ worker
    and waits for the WAV audio response.
    """

    def __init__(self, host="localhost", port=5672, queue="tts_queue",
                 username="guest", password="guest"):
        credentials = pika.PlainCredentials(username, password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port, credentials=credentials)
        )
        self.channel = self.connection.channel()

        # Declare an exclusive callback queue for replies
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self._on_response,
            auto_ack=True,
        )

        self.queue = queue
        self.response = None
        self.corr_id = None
        self.content_type = None

    def _on_response(self, ch, method, properties, body):
        if self.corr_id == properties.correlation_id:
            self.response = body
            self.content_type = properties.content_type

    def call(self, text: str, voice: str = None, timeout: int = 120) -> bytes:
        """
        Send a TTS request and wait for the response.

        Args:
            text: The text to synthesize.
            voice: Optional preset voice name (e.g. "Tuyen", "Ngoc", "Binh").
            timeout: Max seconds to wait for response.

        Returns:
            bytes: WAV audio content, or raises on error.
        """
        self.response = None
        self.corr_id = str(uuid.uuid4())

        payload = {"text": text}
        if voice:
            payload["voice"] = voice

        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
                content_type="application/json",
                delivery_mode=2,
            ),
            body=json.dumps(payload).encode("utf-8"),
        )

        # Wait for response
        deadline = self.connection.add_callback_threadsafe(lambda: None)
        self.connection.process_data_events(time_limit=timeout)

        # Poll until response arrives
        import time
        start = time.time()
        while self.response is None:
            self.connection.process_data_events(time_limit=1)
            if time.time() - start > timeout:
                raise TimeoutError(f"No response within {timeout}s")

        # Check if it's an error response
        if self.content_type == "application/json":
            err = json.loads(self.response)
            raise RuntimeError(f"TTS Worker error: {err.get('error', 'unknown')}")

        return self.response

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()


def main():
    print("🔗 Connecting to RabbitMQ …")
    client = TTSRpcClient(host="localhost", queue="tts_queue")

    try:
        # ── Example 1: Default voice ─────────────────────────────────
        text = "Xin chào, đây là bài test gọi VieNeu TTS qua RabbitMQ."
        print(f"📤 Sending: {text}")

        wav_bytes = client.call(text=text)
        output_path = "outputs/rabbitmq_output.wav"
        with open(output_path, "wb") as f:
            f.write(wav_bytes)
        print(f"💾 Saved audio ({len(wav_bytes)} bytes) → {output_path}")

        # ── Example 2: Specific voice ────────────────────────────────
        text2 = "Tôi đang nói bằng giọng của Tuyên qua hàng đợi RabbitMQ."
        print(f"\n📤 Sending (voice=Tuyen): {text2}")

        wav_bytes2 = client.call(text=text2, voice="Tuyen")
        output_path2 = "outputs/rabbitmq_output_tuyen.wav"
        with open(output_path2, "wb") as f:
            f.write(wav_bytes2)
        print(f"💾 Saved audio ({len(wav_bytes2)} bytes) → {output_path2}")

    except TimeoutError as e:
        print(f"⏰ Timeout: {e}", file=sys.stderr)
    except RuntimeError as e:
        print(f"❌ Error: {e}", file=sys.stderr)
    finally:
        client.close()
        print("👋 Done.")


if __name__ == "__main__":
    main()
