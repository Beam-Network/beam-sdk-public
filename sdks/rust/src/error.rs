use std::{fmt, sync::Arc, time::Duration};
use thiserror::Error;

/// Failure codes BeamCore reports at the start of a failed transfer's `error_message` when the
/// source or destination storage refused Beam's requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageAccessCode {
    /// `source_access_denied`: the source storage refused reads.
    SourceAccessDenied,
    /// `destination_access_denied`: the destination storage refused writes.
    DestinationAccessDenied,
}

impl StorageAccessCode {
    /// The wire code, for example `destination_access_denied`.
    pub fn as_str(self) -> &'static str {
        match self {
            StorageAccessCode::SourceAccessDenied => "source_access_denied",
            StorageAccessCode::DestinationAccessDenied => "destination_access_denied",
        }
    }

    /// The code at the start of an `error_message` (the text before the first `:`), if it is a
    /// storage access code.
    pub fn from_error_message(error_message: &str) -> Option<Self> {
        match error_message.split(':').next().unwrap_or_default().trim() {
            "source_access_denied" => Some(StorageAccessCode::SourceAccessDenied),
            "destination_access_denied" => Some(StorageAccessCode::DestinationAccessDenied),
            _ => None,
        }
    }
}

impl fmt::Display for StorageAccessCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Errors returned by the BEAM SDK.
///
/// The enum is `#[non_exhaustive]`: match the variants you handle and keep a wildcard arm.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BeamApiError {
    #[error("api_key is required")]
    MissingApiKey,
    #[error("invalid {0}")]
    InvalidId(&'static str),
    /// A caller-supplied option or provider config failed validation before any request.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// A non-lifecycle HTTP call (for example the Hippius API) returned a failure status.
    #[error("BEAM API request failed with status {status}")]
    HttpStatus { status: u16 },
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("NATS lifecycle transport failed: {0}")]
    Nats(String),
    /// BeamCore answered a lifecycle request with `ok: false`. `body` is the JSON-encoded
    /// reply `error` field; `code` and `message` are read from it when present.
    #[error("Beam lifecycle request failed with {status}: {body}")]
    Lifecycle {
        status: u16,
        code: Option<String>,
        message: Option<String>,
        body: String,
    },
    #[error("JSON encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    /// BeamCore reported the transfer as failed. The message is its `error_message`, verbatim.
    #[error("transfer failed: {0}")]
    TransferFailed(String),
    /// The source or destination storage refused Beam's requests. `message` is BeamCore's
    /// `error_message`, verbatim.
    #[error("transfer failed: {message}")]
    StorageAccessDenied {
        code: StorageAccessCode,
        message: String,
    },
    #[error("transfer cancelled")]
    TransferCancelled,
    #[error("transfer {transfer_id} did not complete within {timeout:?}")]
    Timeout {
        transfer_id: String,
        timeout: Duration,
    },
    #[error("provider signing failed: {0}")]
    ProviderSigning(String),
    /// An object-storage control call (for example S3 `CreateMultipartUpload`) failed. The
    /// message never contains signed URLs or credentials.
    #[error("{provider} {operation} failed{}{}", status.map(|status| format!(" with status {status}")).unwrap_or_default(), code.as_deref().map(|code| format!(" ({code})")).unwrap_or_default())]
    ProviderRequest {
        provider: String,
        operation: &'static str,
        status: Option<u16>,
        code: Option<String>,
        message: Option<String>,
    },
    /// The ownership cancellation token fired; signing and replay stopped without cancelling
    /// the transfer (a replacement owner may continue it).
    #[error("provider transfer ownership was cancelled")]
    Aborted,
    #[error(
        "transfer {transfer_id} is prepared and route recovery is continuing in the background"
    )]
    RouteRecoveryPending {
        transfer_id: String,
        #[source]
        source: Box<BeamApiError>,
    },
    /// A provider-backed transfer failed after BeamCore prepared it. The SDK attempted to
    /// cancel the transfer and abort every multipart upload it created; the flags report the
    /// outcome. `errors` lists the cause first, then the cancellation and cleanup failures.
    #[error("provider transfer failed for {transfer_id} (transfer_cancelled={transfer_cancelled}, multipart_cleanup_complete={multipart_cleanup_complete})")]
    ProviderTransfer {
        transfer_id: String,
        transfer_cancelled: bool,
        multipart_cleanup_complete: bool,
        #[source]
        cause: Arc<BeamApiError>,
        errors: Vec<Arc<BeamApiError>>,
    },
    /// Several independent operations failed (for example multipart cleanup).
    #[error("{message}")]
    Multiple {
        message: String,
        errors: Vec<BeamApiError>,
    },
}

impl BeamApiError {
    /// The error for a failed transfer: [`BeamApiError::StorageAccessDenied`] when
    /// `error_message` starts with a storage access code, otherwise
    /// [`BeamApiError::TransferFailed`].
    pub fn transfer_failed(error_message: Option<String>) -> Self {
        let message = error_message.unwrap_or_else(|| "unknown error".to_string());
        match StorageAccessCode::from_error_message(&message) {
            Some(code) => BeamApiError::StorageAccessDenied { code, message },
            None => BeamApiError::TransferFailed(message),
        }
    }

    /// A short, credential-free identifier for the error, with the status when known, such as
    /// `Lifecycle:status=503`. Mirrors the TypeScript SDK's `safeErrorCode`.
    pub fn safe_code(&self) -> String {
        let (code, status): (String, Option<u16>) = match self {
            BeamApiError::MissingApiKey => ("MissingApiKey".into(), None),
            BeamApiError::InvalidId(_) => ("InvalidId".into(), None),
            BeamApiError::InvalidArgument(_) => ("InvalidArgument".into(), None),
            BeamApiError::HttpStatus { status } => ("HttpStatus".into(), Some(*status)),
            BeamApiError::Request(error) => (
                "Request".into(),
                error.status().map(|status| status.as_u16()),
            ),
            BeamApiError::Nats(_) => ("Nats".into(), None),
            BeamApiError::Lifecycle { status, code, .. } => (
                code.as_deref()
                    .filter(|code| is_safe_code(code))
                    .unwrap_or("Lifecycle")
                    .to_string(),
                Some(*status),
            ),
            BeamApiError::Json(_) => ("Json".into(), None),
            BeamApiError::TransferFailed(_) => ("TransferFailed".into(), None),
            BeamApiError::StorageAccessDenied { code, .. } => (code.as_str().into(), None),
            BeamApiError::TransferCancelled => ("TransferCancelled".into(), None),
            BeamApiError::Timeout { .. } => ("Timeout".into(), None),
            BeamApiError::ProviderSigning(_) => ("ProviderSigning".into(), None),
            BeamApiError::ProviderRequest { status, code, .. } => (
                code.as_deref()
                    .filter(|code| is_safe_code(code))
                    .unwrap_or("ProviderRequest")
                    .to_string(),
                *status,
            ),
            BeamApiError::Aborted => ("Aborted".into(), None),
            BeamApiError::RouteRecoveryPending { .. } => ("RouteRecoveryPending".into(), None),
            BeamApiError::ProviderTransfer { .. } => ("ProviderTransfer".into(), None),
            BeamApiError::Multiple { .. } => ("Multiple".into(), None),
        };
        match status.filter(|status| (100..=599).contains(status)) {
            Some(status) => format!("{code}:status={status}"),
            None => code,
        }
    }

    pub(crate) fn variant_name(&self) -> &'static str {
        match self {
            BeamApiError::MissingApiKey => "MissingApiKey",
            BeamApiError::InvalidId(_) => "InvalidId",
            BeamApiError::InvalidArgument(_) => "InvalidArgument",
            BeamApiError::HttpStatus { .. } => "HttpStatus",
            BeamApiError::Request(_) => "Request",
            BeamApiError::Nats(_) => "Nats",
            BeamApiError::Lifecycle { .. } => "Lifecycle",
            BeamApiError::Json(_) => "Json",
            BeamApiError::TransferFailed(_) => "TransferFailed",
            BeamApiError::StorageAccessDenied { .. } => "StorageAccessDenied",
            BeamApiError::TransferCancelled => "TransferCancelled",
            BeamApiError::Timeout { .. } => "Timeout",
            BeamApiError::ProviderSigning(_) => "ProviderSigning",
            BeamApiError::ProviderRequest { .. } => "ProviderRequest",
            BeamApiError::Aborted => "Aborted",
            BeamApiError::RouteRecoveryPending { .. } => "RouteRecoveryPending",
            BeamApiError::ProviderTransfer { .. } => "ProviderTransfer",
            BeamApiError::Multiple { .. } => "Multiple",
        }
    }

    /// The error's own message without the variant prefix, used for summaries.
    fn detail(&self) -> String {
        match self {
            BeamApiError::InvalidArgument(message)
            | BeamApiError::Nats(message)
            | BeamApiError::TransferFailed(message)
            | BeamApiError::StorageAccessDenied { message, .. }
            | BeamApiError::ProviderSigning(message) => message.clone(),
            other => other.to_string(),
        }
    }
}

fn is_safe_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, '_' | '.' | '-'))
}

/// A diagnostic summary safe to surface to callers: at most 200 characters, and reduced to the
/// error kind whenever the message could contain a URL or a credential.
pub(crate) fn sanitized_error_summary(error: &BeamApiError) -> String {
    let message = error.detail();
    let message = message.trim();
    let lowered = message.to_ascii_lowercase();
    let sensitive = [
        "http://",
        "https://",
        "x-amz",
        "password",
        "secret",
        "token",
        "authorization",
        "credential",
    ]
    .iter()
    .any(|marker| lowered.contains(marker));
    if message.is_empty() || sensitive {
        return error.variant_name().to_string();
    }
    message.chars().take(200).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summaries_hide_urls_and_credentials() {
        assert_eq!(
            sanitized_error_summary(&BeamApiError::ProviderSigning(
                "integrity audit signer unavailable".into()
            )),
            "integrity audit signer unavailable"
        );
        assert_eq!(
            sanitized_error_summary(&BeamApiError::ProviderSigning(
                "failed to sign https://private.example/?X-Amz-Signature=secret".into()
            )),
            "ProviderSigning"
        );
        assert_eq!(
            sanitized_error_summary(&BeamApiError::Nats("x".repeat(500))).len(),
            200
        );
    }

    #[test]
    fn failed_transfers_classify_storage_access_codes_and_keep_the_message() {
        let message = String::from("destination_access_denied: refused (403 AccessDenied).");
        match BeamApiError::transfer_failed(Some(message.clone())) {
            BeamApiError::StorageAccessDenied { code, message: kept } => {
                assert_eq!(code, StorageAccessCode::DestinationAccessDenied);
                assert_eq!(kept, message);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        let source = BeamApiError::transfer_failed(Some("source_access_denied: refused".into()));
        assert_eq!(source.safe_code(), "source_access_denied");
        assert_eq!(source.to_string(), "transfer failed: source_access_denied: refused");
        for message in [
            "upstream said source_access_denied: later",
            "destination_access_denied_extra: x",
        ] {
            assert!(matches!(
                BeamApiError::transfer_failed(Some(message.into())),
                BeamApiError::TransferFailed(ref kept) if kept == message
            ));
        }
        assert!(matches!(
            BeamApiError::transfer_failed(None),
            BeamApiError::TransferFailed(ref kept) if kept == "unknown error"
        ));
    }

    #[test]
    fn safe_codes_carry_status_without_messages() {
        let lifecycle = BeamApiError::Lifecycle {
            status: 503,
            code: Some("runtime_unavailable".into()),
            message: Some("https://secret".into()),
            body: "{}".into(),
        };
        assert_eq!(lifecycle.safe_code(), "runtime_unavailable:status=503");
        let throttled = BeamApiError::ProviderRequest {
            provider: "r2".into(),
            operation: "AbortMultipartUpload",
            status: Some(503),
            code: Some("SlowDown".into()),
            message: None,
        };
        assert_eq!(throttled.safe_code(), "SlowDown:status=503");
        assert_eq!(BeamApiError::Nats("secret".into()).safe_code(), "Nats");
    }

    #[test]
    fn provider_transfer_errors_retain_nested_causes_and_cleanup_outcomes() {
        let cause = Arc::new(BeamApiError::ProviderRequest {
            provider: "r2".into(),
            operation: "UploadPart",
            status: Some(503),
            code: Some("SlowDown".into()),
            message: None,
        });
        let cleanup = Arc::new(BeamApiError::Multiple {
            message: "failed to abort 1 multipart upload(s)".into(),
            errors: vec![BeamApiError::HttpStatus { status: 503 }],
        });
        let error = BeamApiError::ProviderTransfer {
            transfer_id: "transfer-1".into(),
            transfer_cancelled: true,
            multipart_cleanup_complete: false,
            cause: cause.clone(),
            errors: vec![cause.clone(), cleanup.clone()],
        };
        assert!(error.to_string().contains("transfer_cancelled=true"));
        assert!(error
            .to_string()
            .contains("multipart_cleanup_complete=false"));
        match &error {
            BeamApiError::ProviderTransfer {
                errors, cause: c, ..
            } => {
                assert!(Arc::ptr_eq(&errors[0], &cause));
                assert!(Arc::ptr_eq(&errors[1], &cleanup));
                assert!(Arc::ptr_eq(c, &cause));
            }
            _ => unreachable!(),
        }
        assert!(std::error::Error::source(&error).is_some());
    }
}
