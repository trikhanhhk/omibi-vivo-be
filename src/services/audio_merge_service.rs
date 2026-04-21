use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::{StreamExt, stream};
use lapin::{
    BasicProperties, Channel,
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use serde_json::json;
use sqlx::PgPool;
use tokio::fs;
use tokio::process::Command;
use uuid::Uuid;

use crate::{
    common::response::ApiError,
    dto::audio_merge::merge_audio_request::{AudioSegmentRequest, MergeAudioRequest},
    infra::rabbitmq::{QUEUE_TTS_REQUEST, create_channel, setup_queue},
    models::audio_merge_job::{AudioMergeJob, AudioMergeStatus},
    repositories::audio_merge_job_repository::AudioMergeJobRepository,
};

/// Directory where merged audio files are persisted.
const AUDIO_OUTPUT_DIR: &str = "output";

/// Timeout waiting for the TTS worker to reply via RabbitMQ RPC.
const TTS_RPC_TIMEOUT_SECS: u64 = 120;

#[derive(Clone)]
pub struct AudioMergeService {
    channel: Channel,
    job_repo: AudioMergeJobRepository,
}

impl AudioMergeService {
    pub async fn new(pool: PgPool) -> Self {
        let channel = create_channel().await;
        setup_queue(&channel).await;
        Self {
            channel,
            job_repo: AudioMergeJobRepository::new(pool),
        }
    }

    /// Create a background merge job and return it immediately.
    /// The actual audio generation runs in a spawned task.
    pub async fn enqueue_merge_audio(
        &self,
        request: MergeAudioRequest,
    ) -> Result<AudioMergeJob, ApiError> {
        let segments = &request.segments;

        // Validate segment timing
        for (i, seg) in segments.iter().enumerate() {
            if seg.end_time <= seg.start_time {
                return Err(ApiError::bad_request(format!(
                    "Segment {}: end_time ({}) must be greater than start_time ({})",
                    i, seg.end_time, seg.start_time
                )));
            }
        }

        let job = self
            .job_repo
            .create(
                &request.metadata.file_name,
                request.metadata.model.as_deref(),
            )
            .await
            .map_err(|e| ApiError::internal_with("Failed to create merge job", e))?;

        // Spawn background processing
        let service = self.clone();
        let job_id = job.id;
        tokio::spawn(async move {
            service.process_merge_job(job_id, request).await;
        });

        Ok(job)
    }

    /// List all merge jobs (for simplicity, no pagination here).
    pub async fn list_jobs(&self) -> Result<Vec<AudioMergeJob>, ApiError> {
        self.job_repo
            .list()
            .await
            .map_err(|e| ApiError::internal_with("Failed to list merge jobs", e))
    }

    /// Fetch a job by id.
    pub async fn get_job(&self, job_id: i64) -> Result<Option<AudioMergeJob>, ApiError> {
        self.job_repo
            .get_by_id(job_id)
            .await
            .map_err(|e| ApiError::internal_with("Failed to fetch merge job", e))
    }

    /// Read completed audio file bytes for download.
    pub async fn get_audio_bytes(&self, job_id: i64) -> Result<Vec<u8>, ApiError> {
        let job = self
            .get_job(job_id)
            .await?
            .ok_or_else(|| ApiError::not_found("Merge job not found"))?;

        if job.status != AudioMergeStatus::Completed {
            return Err(ApiError::bad_request("Merge job is not completed yet"));
        }

        let path = job
            .audio_url
            .ok_or_else(|| ApiError::internal("Completed job has no audio path"))?;

        fs::read(&path)
            .await
            .map_err(|e| ApiError::internal_with("Failed to read audio file", e))
    }

    /// Background worker: generate audio then update job status.
    async fn process_merge_job(&self, job_id: i64, request: MergeAudioRequest) {
        if let Err(e) = self
            .job_repo
            .update_status(job_id, AudioMergeStatus::Processing)
            .await
        {
            eprintln!(
                "[audio_merge] job_id={} Failed to set job to Processing: {}",
                job_id, e
            );
            return;
        }

        match self.run_merge(&request).await {
            Ok(output_path) => {
                let url = output_path.to_string_lossy().to_string();
                if let Err(e) = self.job_repo.complete(job_id, &url).await {
                    eprintln!(
                        "[audio_merge] job_id={} Failed to mark job Completed: {}",
                        job_id, e
                    );
                }
            }
            Err(e) => {
                eprintln!("[audio_merge] job_id={} Merge job failed: {:?}", job_id, e);
                let _ = self
                    .job_repo
                    .update_status(job_id, AudioMergeStatus::Failed)
                    .await;
            }
        }
    }

    /// Core merge logic — returns the path of the saved output file.
    async fn run_merge(&self, request: &MergeAudioRequest) -> Result<PathBuf, ApiError> {
        let segments = &request.segments;

        let session_id = Uuid::new_v4();
        let temp_dir = PathBuf::from(format!("/tmp/audio_merge_{}", session_id));
        fs::create_dir_all(&temp_dir)
            .await
            .map_err(|e| ApiError::internal_with("Failed to create temp directory", e))?;

        let concurrency = 20;
        let service = self.clone();
        let owned_segments: Vec<(usize, String, Option<String>)> = segments
            .iter()
            .enumerate()
            .map(|(i, seg)| (i, seg.text.clone(), request.metadata.model.clone()))
            .collect();

        let segment_files: Vec<PathBuf> = stream::iter(owned_segments)
            .map(|(i, text, model)| {
                let temp_dir = temp_dir.clone();
                let service = service.clone();
                async move {
                    let audio_bytes = service
                        .generate_segment_audio(&text, model.as_deref())
                        .await?;
                    let seg_path = temp_dir.join(format!("seg_{}.wav", i));
                    fs::write(&seg_path, &audio_bytes)
                        .await
                        .map_err(|e| ApiError::internal_with("Failed to write segment audio", e))?;
                    Ok::<_, ApiError>(seg_path)
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<Result<PathBuf, ApiError>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<PathBuf>, ApiError>>()?;

        let first_start = segments[0].start_time;
        let total_duration = segments.last().unwrap().end_time - first_start;

        // Persist output to a stable directory so it can be served later
        let output_dir = PathBuf::from(AUDIO_OUTPUT_DIR);
        fs::create_dir_all(&output_dir)
            .await
            .map_err(|e| ApiError::internal_with("Failed to create output directory", e))?;
        let output_path = output_dir.join(&request.metadata.file_name);

        self.run_ffmpeg_merge(
            &segment_files,
            segments,
            first_start,
            total_duration,
            &output_path,
        )
        .await?;

        // Clean up temp segment files
        let _ = fs::remove_dir_all(&temp_dir).await;

        Ok(output_path)
    }

    /// Call the TTS worker via RabbitMQ RPC and return the raw WAV bytes.
    /// Publishes a message with `reply_to` + `correlation_id` to `tts_queue`;
    /// the Python worker replies with WAV bytes (or JSON error) on the reply queue.
    async fn generate_segment_audio(
        &self,
        text: &str,
        tts_model: Option<&str>,
    ) -> Result<Vec<u8>, ApiError> {
        // Exclusive, auto-delete reply queue for this single RPC call
        let reply_queue = self
            .channel
            .queue_declare(
                "",
                QueueDeclareOptions {
                    exclusive: true,
                    auto_delete: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| ApiError::internal_with("Failed to declare RPC reply queue", e))?;
        let reply_queue_name = reply_queue.name().to_string();

        let correlation_id = Uuid::new_v4().to_string();
        let payload = json!({ "text": text, "tts_model": tts_model });
        let payload_bytes = serde_json::to_vec(&payload)
            .map_err(|e| ApiError::internal_with("Failed to serialize TTS RPC request", e))?;

        self.channel
            .basic_publish(
                "",
                QUEUE_TTS_REQUEST,
                BasicPublishOptions::default(),
                &payload_bytes,
                BasicProperties::default()
                    .with_reply_to(reply_queue_name.clone().into())
                    .with_correlation_id(correlation_id.clone().into()),
            )
            .await
            .map_err(|e| ApiError::internal_with("Failed to publish TTS RPC request", e))?
            .await
            .map_err(|e| ApiError::internal_with("Failed to confirm TTS RPC publish", e))?;

        let mut consumer = self
            .channel
            .basic_consume(
                &reply_queue_name,
                "",
                BasicConsumeOptions {
                    no_ack: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| ApiError::internal_with("Failed to start RPC reply consumer", e))?;

        let rpc_fut = async {
            match consumer.next().await {
                Some(Ok(delivery)) => {
                    let content_type = delivery
                        .properties
                        .content_type()
                        .as_ref()
                        .map(|s| s.as_str().to_string());

                    if content_type.as_deref() == Some("application/json") {
                        let body = String::from_utf8_lossy(&delivery.data);
                        return Err(ApiError::internal(format!("TTS worker error: {}", body)));
                    }

                    Ok(delivery.data)
                }
                Some(Err(e)) => Err(ApiError::internal_with("RPC delivery error", e)),
                None => Err(ApiError::internal("TTS RPC consumer closed unexpectedly")),
            }
        };

        tokio::time::timeout(Duration::from_secs(TTS_RPC_TIMEOUT_SECS), rpc_fut)
            .await
            .map_err(|_| ApiError::internal("TTS RPC timed out"))?
    }

    /// Build and run the ffmpeg command that:
    ///   • delays each segment by its `start_time` offset,
    ///   • mixes all streams together, and
    ///   • trims the output to `total_duration` seconds.
    async fn run_ffmpeg_merge(
        &self,
        segment_files: &[PathBuf],
        segments: &[AudioSegmentRequest],
        first_start: f64,
        total_duration: f64,
        output_path: &Path,
    ) -> Result<(), ApiError> {
        let n = segment_files.len();
        let mut cmd = Command::new("ffmpeg");
        cmd.arg("-y"); // overwrite output if exists

        for file in segment_files {
            cmd.arg("-i").arg(file);
        }

        // Build filter_complex:
        //   [N:a]adelay=DELAY_MS|DELAY_MS[aN];
        //   ...
        //   [a0][a1]...amix=inputs=N:duration=longest:normalize=0
        let mut filter = String::new();
        for (i, seg) in segments.iter().enumerate() {
            let delay_ms = ((seg.start_time - first_start) * 1000.0).round() as u64;
            filter.push_str(&format!(
                "[{}:a]adelay={}|{}[a{}];",
                i, delay_ms, delay_ms, i
            ));
        }
        let stream_labels: String = (0..n).map(|i| format!("[a{}]", i)).collect();
        filter.push_str(&format!(
            "{}amix=inputs={}:duration=longest:normalize=0",
            stream_labels, n
        ));

        cmd.arg("-filter_complex").arg(&filter);
        // Trim output to the exact total duration
        cmd.arg("-t").arg(format!("{:.3}", total_duration));
        cmd.arg(output_path);

        let output = cmd
            .output()
            .await
            .map_err(|e| ApiError::internal_with("Failed to spawn ffmpeg process", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ApiError::internal(format!(
                "ffmpeg failed: {}",
                stderr.trim()
            )));
        }

        Ok(())
    }
}
