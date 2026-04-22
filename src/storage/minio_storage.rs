use std::path::Path;

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    Client,
    config::Builder,
    primitives::ByteStream,
};
use axum::body::Body;
use tokio_util::io::ReaderStream;

use crate::common::response::ApiError;

#[derive(Clone)]
pub struct MinioStorage {
    client: Client,
    bucket: String,
}

impl MinioStorage {
    pub fn new(endpoint: &str, access_key: &str, secret_key: &str, bucket: &str) -> Self {
        let credentials = Credentials::new(access_key, secret_key, None, None, "minio");

        let config = Builder::new()
            .endpoint_url(endpoint)
            .credentials_provider(credentials)
            .region(Region::new("us-east-1"))
            .force_path_style(true)
            .behavior_version_latest()
            .build();

        Self {
            client: Client::from_conf(config),
            bucket: bucket.to_string(),
        }
    }

    pub fn from_env() -> Self {
        let endpoint =
            std::env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
        let access_key =
            std::env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
        let secret_key =
            std::env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
        let bucket = std::env::var("MINIO_BUCKET").unwrap_or_else(|_| "ominihub".to_string());

        Self::new(&endpoint, &access_key, &secret_key, &bucket)
    }

    /// Upload raw bytes — suitable for small in-memory payloads (e.g. single TTS segments).
    pub async fn upload(&self, key: &str, bytes: Vec<u8>, content_type: &str) -> Result<(), ApiError> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes))
            .content_type(content_type)
            .send()
            .await
            .map_err(|e| ApiError::internal_with("Failed to upload to MinIO", e))?;
        Ok(())
    }

    /// Upload by streaming directly from a file — no full-file buffering in RAM.
    /// Use this for large files (e.g. FFmpeg-merged audio output).
    pub async fn upload_from_path(&self, key: &str, path: &Path, content_type: &str) -> Result<(), ApiError> {
        let stream = ByteStream::from_path(path)
            .await
            .map_err(|e| ApiError::internal_with("Failed to open file for MinIO upload", e))?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(stream)
            .content_type(content_type)
            .send()
            .await
            .map_err(|e| ApiError::internal_with("Failed to upload to MinIO", e))?;
        Ok(())
    }

    /// Download with Range support — forwards the Range header to MinIO so the browser
    /// can seek inside a video without downloading the entire file.
    /// Returns `(body, content_length, content_range)`.  When `range` is `None` the
    /// full object is returned with a 200 status; callers should use 206 when a range
    /// was requested.
    pub async fn download_range(
        &self,
        key: &str,
        range: Option<&str>,
    ) -> Result<(Body, Option<i64>, Option<String>), ApiError> {
        let mut req = self.client.get_object().bucket(&self.bucket).key(key);
        if let Some(r) = range {
            req = req.range(r);
        }
        let output = req
            .send()
            .await
            .map_err(|e| ApiError::internal_with("Failed to download from MinIO", e))?;

        let content_length = output.content_length();
        let content_range = output.content_range().map(|s| s.to_string());
        let reader = output.body.into_async_read();
        Ok((Body::from_stream(ReaderStream::new(reader)), content_length, content_range))
    }

    /// Download as a streaming HTTP body — data flows from MinIO to the caller without
    /// buffering the entire file in RAM.
    pub async fn download_stream(&self, key: &str) -> Result<Body, ApiError> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| ApiError::internal_with("Failed to download from MinIO", e))?;

        // into_async_read() converts ByteStream to AsyncRead; ReaderStream wraps it
        // as a futures Stream<Item=Result<Bytes,_>> for axum Body — no full-file buffering.
        let reader = output.body.into_async_read();
        Ok(Body::from_stream(ReaderStream::new(reader)))
    }
}