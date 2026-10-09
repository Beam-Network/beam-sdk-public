//! Copy an S3 object to Cloudflare R2 and a MinIO bucket with locally signed URLs.
//!
//! Run with `BEAM_API_KEY=b1m_... cargo run --example provider_transfer`.

use beam_network_sdk::{
    BeamApiError, BeamClient, BeamClientOptions, ProviderDestinationConfig, ProviderSourceConfig,
    ProviderTransferCreateInput, R2ProviderDestination, S3CompatibleProviderDestination,
    S3ProviderSource, WaitForTransferOptions,
};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), BeamApiError> {
    let beam = BeamClient::new(BeamClientOptions {
        api_key: std::env::var("BEAM_API_KEY").unwrap_or_default(),
        ..Default::default()
    })?;

    let source = S3ProviderSource {
        bucket: "source-bucket".into(),
        key: "exports/report.parquet".into(),
        region: Some("us-east-1".into()),
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default(),
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
        ..Default::default()
    }
    .create()?;
    let r2 = R2ProviderDestination {
        bucket: "destination-bucket".into(),
        key: "imports/report.parquet".into(),
        account_id: Some("cloudflare-account-id".into()),
        access_key_id: std::env::var("R2_ACCESS_KEY_ID").unwrap_or_default(),
        secret_access_key: std::env::var("R2_SECRET_ACCESS_KEY").unwrap_or_default(),
        ..Default::default()
    }
    .create()?;
    let minio = S3CompatibleProviderDestination {
        provider: "minio".into(),
        bucket: "archive".into(),
        key: "imports/report.parquet".into(),
        endpoint_url: Some("https://minio.example.com".into()),
        access_key_id: std::env::var("MINIO_ACCESS_KEY").unwrap_or_default(),
        secret_access_key: std::env::var("MINIO_SECRET_KEY").unwrap_or_default(),
        ..Default::default()
    }
    .create()?;

    let prepared = beam
        .prepare_provider_transfer(ProviderTransferCreateInput {
            sources: vec![ProviderSourceConfig::S3(source)],
            destinations: vec![
                ProviderDestinationConfig::R2(r2),
                ProviderDestinationConfig::S3Compatible(minio),
            ],
            name: Some("s3-to-r2-and-minio".into()),
            idempotency_key: Some("daily-report-2026-07-15".into()),
            // Persist these identities to resume after a restart.
            on_multipart_group_ready: Some(Arc::new(|group| {
                Box::pin(async move {
                    println!(
                        "multipart upload {} for {}",
                        group.upload_id, group.object_key
                    );
                    Ok(())
                })
            })),
            ..Default::default()
        })
        .await?;

    let status = beam
        .wait_for_transfer_with_options(
            &prepared.transfer_id,
            WaitForTransferOptions {
                timeout: Some(Duration::from_secs(600)),
                ..Default::default()
            },
        )
        .await?;
    println!("{} {}", status.transfer_id, status.status);
    beam.close().await
}
