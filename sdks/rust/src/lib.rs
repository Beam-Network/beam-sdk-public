//! Rust SDK for BEAM transfer creation and management.

mod client;
mod error;
pub mod huggingface;
mod models;
pub mod multipart_limits;
mod nats_control;
mod performance;
pub use performance::{SdkPerformanceCounters, SdkPerformanceMeasurement, SdkPerformanceSummary};
mod provider_config;
mod provider_flow;
pub mod provider_signing;
mod route_stream;
mod s3;
mod sigv4;
#[cfg(test)]
mod test_http;

pub use client::{
    beam_dev_url, prepare_provider_destination_config, AttachSignedUrlsInput, BeamClient,
    BeamClientOptions, ManualRouteRecovery, ManualRouteRecoveryFuture,
    TransferTerminalSignalWaiter, BEAM_PROD_URL, DEFAULT_MAX_PAYLOAD_BYTES,
    DEFAULT_MULTIPART_CONTROL_CONCURRENCY,
};
pub use error::{BeamApiError, StorageAccessCode};
pub use models::*;
pub use nats_control::BEAM_ROUTE_TARGET_PAYLOAD_BYTES;
pub use provider_config::HUGGINGFACE_REPO_TYPES;
pub use provider_flow::{
    BeforeTransferPrepareCallback, MultipartGroupReadyCallback, ProviderCallbackFuture,
    ProviderTransferCreateInput, ProviderTransferResumeInput, ThrowIfCancelledCallback,
    TransferPreparedCallback,
};
