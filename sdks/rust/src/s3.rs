//! S3 API access for S3, R2 and S3-compatible stores: endpoint resolution, SigV4 presigning,
//! and the few control calls the SDK makes itself (multipart create/abort/list/complete and
//! HEAD). Object bytes are never read or written here.

use crate::error::BeamApiError;
use crate::sigv4::{self, Credentials, Scope, SignableRequest, EMPTY_PAYLOAD_SHA256};
use crate::{
    ProviderDestinationConfig, ProviderSourceConfig, R2ProviderDestination, R2ProviderSource,
    S3CompatibleProviderDestination, S3CompatibleProviderSource, S3ProviderDestination,
    S3ProviderSource,
};
use reqwest::Client as HttpClient;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::IpAddr,
    time::{Duration, SystemTime},
};
use uuid::Uuid;

/// Attempts per control call, matching the TypeScript SDK's S3 client (`maxAttempts: 5`).
const MAX_ATTEMPTS: u32 = 5;

/// Resolved connection settings for any S3 API-compatible config.
#[derive(Clone)]
pub(crate) struct S3Settings {
    pub storage_location: Option<String>,
    pub provider: String,
    pub bucket: String,
    pub key: String,
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub account_id: Option<String>,
    pub force_path_style: Option<bool>,
    credentials: Credentials,
}

impl std::fmt::Debug for S3Settings {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3Settings")
            .field("provider", &self.provider)
            .field("bucket", &self.bucket)
            .field("endpoint_url", &self.endpoint_url)
            .finish_non_exhaustive()
    }
}

macro_rules! s3_settings {
    ($config:expr, $provider:expr, $region:expr, $session:expr, $path_style:expr, $account:expr) => {
        S3Settings {
            storage_location: $config.storage_location.clone(),
            provider: $provider,
            bucket: $config.bucket.clone(),
            key: $config.key.clone(),
            region: $region,
            endpoint_url: $config.endpoint_url.clone(),
            account_id: $account,
            force_path_style: $path_style,
            credentials: Credentials {
                access_key_id: $config.access_key_id.clone(),
                secret_access_key: $config.secret_access_key.clone(),
                session_token: $session,
            },
        }
    };
}

impl S3Settings {
    fn from_s3(config: &S3ProviderSource) -> Self {
        s3_settings!(
            config,
            "s3".to_string(),
            config.region.clone(),
            config.session_token.clone(),
            None,
            None
        )
    }

    fn from_s3_destination(config: &S3ProviderDestination) -> Self {
        s3_settings!(
            config,
            "s3".to_string(),
            config.region.clone(),
            config.session_token.clone(),
            None,
            None
        )
    }

    fn from_r2(config: &R2ProviderSource) -> Self {
        s3_settings!(
            config,
            "r2".to_string(),
            None,
            None,
            None,
            config.account_id.clone()
        )
    }

    fn from_r2_destination(config: &R2ProviderDestination) -> Self {
        s3_settings!(
            config,
            "r2".to_string(),
            None,
            None,
            None,
            config.account_id.clone()
        )
    }

    fn from_compatible(config: &S3CompatibleProviderSource) -> Self {
        s3_settings!(
            config,
            config.provider.clone(),
            config.region.clone(),
            config.session_token.clone(),
            config.force_path_style,
            config.account_id.clone()
        )
    }

    fn from_compatible_destination(config: &S3CompatibleProviderDestination) -> Self {
        s3_settings!(
            config,
            config.provider.clone(),
            config.region.clone(),
            config.session_token.clone(),
            config.force_path_style,
            config.account_id.clone()
        )
    }

    pub(crate) fn from_source(source: &ProviderSourceConfig) -> Option<Self> {
        match source {
            ProviderSourceConfig::S3(config) => Some(Self::from_s3(config)),
            ProviderSourceConfig::R2(config) => Some(Self::from_r2(config)),
            ProviderSourceConfig::S3Compatible(config) => Some(Self::from_compatible(config)),
            _ => None,
        }
    }

    pub(crate) fn from_destination(destination: &ProviderDestinationConfig) -> Option<Self> {
        match destination {
            ProviderDestinationConfig::S3(config) => Some(Self::from_s3_destination(config)),
            ProviderDestinationConfig::R2(config) => Some(Self::from_r2_destination(config)),
            ProviderDestinationConfig::S3Compatible(config) => {
                Some(Self::from_compatible_destination(config))
            }
            _ => None,
        }
    }

    /// The endpoint URL, or `None` for AWS S3's regional endpoint.
    pub(crate) fn endpoint(&self) -> Result<Option<String>, BeamApiError> {
        if let Some(endpoint) = self.endpoint_url.as_deref().filter(|value| has_text(value)) {
            return Ok(Some(endpoint.to_string()));
        }
        if self.provider == "r2" {
            return match self.account_id.as_deref().filter(|value| has_text(value)) {
                Some(account_id) => Ok(Some(format!(
                    "https://{account_id}.r2.cloudflarestorage.com"
                ))),
                None => Err(BeamApiError::InvalidArgument(
                    "r2 config requires account_id or endpoint_url.".to_string(),
                )),
            };
        }
        if self.provider != "s3" {
            return Err(BeamApiError::InvalidArgument(format!(
                "{} config requires endpoint_url.",
                self.provider
            )));
        }
        Ok(None)
    }

    pub(crate) fn region(&self) -> String {
        if let Some(region) = self.region.as_deref().filter(|value| has_text(value)) {
            return region.to_string();
        }
        if self.provider == "r2" {
            return "auto".to_string();
        }
        "us-east-1".to_string()
    }

    /// `Some(true)` forces path-style addressing; `None` lets S3 choose virtual hosting.
    pub(crate) fn force_path_style(&self, endpoint: Option<&str>) -> Option<bool> {
        if let Some(value) = self.force_path_style {
            return Some(value);
        }
        if self.provider == "s3" {
            return None;
        }
        Some(endpoint.is_some())
    }

    pub(crate) fn metadata(&self, endpoint: Option<&str>) -> HashMap<String, Value> {
        let mut metadata = HashMap::from([
            ("driver".to_string(), json!("s3-compatible")),
            ("bucket".to_string(), json!(self.bucket)),
            ("key".to_string(), json!(self.key)),
            ("region".to_string(), json!(self.region())),
        ]);
        if let Some(location) = &self.storage_location {
            metadata.insert("storage_location".to_string(), json!(location));
        }
        if let Some(endpoint) = endpoint {
            metadata.insert("endpoint_url".to_string(), json!(endpoint));
        }
        if let Some(account_id) = &self.account_id {
            metadata.insert("account_id".to_string(), json!(account_id));
        }
        metadata
    }

    /// Where requests for `object_key` go: `(origin, host header, encoded path)`.
    ///
    /// Keys with a `.` or `..` path segment are rejected. S3 stores them literally and signs the
    /// literal path, but WHATWG URL parsing (reqwest here, `fetch` in most workers) removes dot
    /// segments, even percent-encoded ones, so the request would target a different object than
    /// the one signed. No URL form survives that normalization.
    fn object_location(&self, object_key: &str) -> Result<(String, String, String), BeamApiError> {
        if object_key
            .split('/')
            .any(|segment| segment == "." || segment == "..")
        {
            return Err(BeamApiError::InvalidArgument(format!(
                "{} object key {object_key:?} contains a '.' or '..' path segment; HTTP clients \
                 normalize such paths, so it cannot be signed or transferred",
                self.provider
            )));
        }
        let endpoint = self.endpoint()?;
        let force_path_style = self.force_path_style(endpoint.as_deref());
        let encoded_key = sigv4::uri_encode(object_key, true);
        let (scheme, host, port, base_path, is_ip) = match &endpoint {
            None => {
                let region = self.region();
                let suffix = if region.starts_with("cn-") {
                    "amazonaws.com.cn"
                } else {
                    "amazonaws.com"
                };
                (
                    "https".to_string(),
                    format!("s3.{region}.{suffix}"),
                    None,
                    String::new(),
                    false,
                )
            }
            Some(endpoint) => {
                let url = reqwest::Url::parse(endpoint).map_err(|_| {
                    BeamApiError::InvalidArgument(format!(
                        "{} config has an invalid endpoint_url.",
                        self.provider
                    ))
                })?;
                let host = url.host_str().unwrap_or_default().to_string();
                let is_ip = host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<IpAddr>()
                    .is_ok();
                (
                    url.scheme().to_string(),
                    host,
                    url.port(),
                    url.path().trim_end_matches('/').to_string(),
                    is_ip,
                )
            }
        };
        let virtual_hosted = force_path_style != Some(true)
            && !is_ip
            && is_virtual_hostable_bucket(&self.bucket, scheme == "http");
        let host = if virtual_hosted {
            format!("{}.{host}", self.bucket)
        } else {
            host
        };
        let host = match port {
            Some(port) => format!("{host}:{port}"),
            None => host,
        };
        let path = if virtual_hosted {
            format!("{base_path}/{encoded_key}")
        } else {
            format!(
                "{base_path}/{}/{encoded_key}",
                sigv4::uri_encode(&self.bucket, false)
            )
        };
        Ok((format!("{scheme}://{host}"), host, path))
    }

    fn request(
        &self,
        operation: &S3Operation<'_>,
        object_key: &str,
    ) -> Result<SignableRequest, BeamApiError> {
        let (origin, host, path) = self.object_location(object_key)?;
        let (method, query, headers) = operation.parts();
        Ok(SignableRequest {
            method,
            origin,
            host,
            path,
            query,
            headers,
        })
    }

    fn scope(&self, time: SystemTime) -> (String, SystemTime) {
        (self.region(), time)
    }

    /// Presign `operation` on `object_key`, valid for `expires_in` from `now`.
    pub(crate) fn presign_at(
        &self,
        operation: &S3Operation<'_>,
        object_key: &str,
        expires_in: Duration,
        now: SystemTime,
    ) -> Result<String, BeamApiError> {
        let request = self.request(operation, object_key)?;
        let (region, time) = self.scope(now);
        sigv4::presign(
            &request,
            &self.credentials,
            &Scope {
                region: &region,
                service: "s3",
                time,
            },
            expires_in,
            true,
        )
    }

    pub(crate) fn presign(
        &self,
        operation: &S3Operation<'_>,
        object_key: &str,
        expires_in: Duration,
    ) -> Result<String, BeamApiError> {
        self.presign_at(operation, object_key, expires_in, SystemTime::now())
    }

    /// Perform a control call with retries for throttling, 5xx and transport failures.
    pub(crate) async fn send(
        &self,
        http: &HttpClient,
        operation: &S3Operation<'_>,
        object_key: &str,
        body: Vec<u8>,
    ) -> Result<S3Response, BeamApiError> {
        let name = operation.name();
        let payload_sha256 = if body.is_empty() {
            EMPTY_PAYLOAD_SHA256.to_string()
        } else {
            sigv4::sha256_hex(&body)
        };
        let mut attempt = 0;
        loop {
            attempt += 1;
            let request = self.request(operation, object_key)?;
            let (region, time) = self.scope(SystemTime::now());
            let (url, headers) = sigv4::sign_headers(
                &request,
                &self.credentials,
                &Scope {
                    region: &region,
                    service: "s3",
                    time,
                },
                &payload_sha256,
            );
            let method = reqwest::Method::from_bytes(request.method.as_bytes())
                .expect("S3 methods are valid HTTP methods");
            let mut builder = http.request(method, url);
            for (header, value) in headers {
                if header != "host" {
                    builder = builder.header(header, value);
                }
            }
            if !body.is_empty() {
                builder = builder.body(body.clone());
            }
            let error = match builder.send().await {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let response_headers = response.headers().clone();
                    let bytes = match response.bytes().await {
                        Ok(bytes) => bytes.to_vec(),
                        Err(error) => {
                            if attempt < MAX_ATTEMPTS {
                                retry_delay(attempt).await;
                                continue;
                            }
                            return Err(BeamApiError::Request(error));
                        }
                    };
                    let text = String::from_utf8_lossy(&bytes).into_owned();
                    // S3 can report a failed CompleteMultipartUpload with 200 and an <Error> body.
                    let embedded_error = status < 300 && text.contains("<Error>");
                    if status < 300 && !embedded_error {
                        return Ok(S3Response {
                            headers: response_headers,
                            body: text,
                        });
                    }
                    let code = xml_text(&text, "Code");
                    let error = BeamApiError::ProviderRequest {
                        provider: self.provider.clone(),
                        operation: name,
                        status: Some(status),
                        code: code.clone(),
                        message: xml_text(&text, "Message"),
                    };
                    if !is_retryable_s3_failure(status, code.as_deref(), embedded_error) {
                        return Err(error);
                    }
                    error
                }
                Err(error) => {
                    if !(error.is_timeout() || error.is_connect() || error.is_request()) {
                        return Err(BeamApiError::Request(error));
                    }
                    BeamApiError::Request(error)
                }
            };
            if attempt >= MAX_ATTEMPTS {
                return Err(error);
            }
            retry_delay(attempt).await;
        }
    }
}

pub(crate) struct S3Response {
    pub headers: reqwest::header::HeaderMap,
    pub body: String,
}

impl S3Response {
    pub(crate) fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    }
}

/// One S3 API operation on an object.
pub(crate) enum S3Operation<'a> {
    GetObject {
        range: Option<&'a str>,
        if_match: Option<&'a str>,
        version_id: Option<&'a str>,
    },
    HeadObject,
    PutObject {
        content_md5: Option<&'a str>,
    },
    UploadPart {
        upload_id: &'a str,
        part_number: u64,
        content_md5: Option<&'a str>,
    },
    CreateMultipartUpload {
        metadata: &'a HashMap<String, String>,
    },
    CompleteMultipartUpload {
        upload_id: &'a str,
    },
    AbortMultipartUpload {
        upload_id: &'a str,
    },
    ListParts {
        upload_id: &'a str,
        max_parts: Option<u64>,
        part_number_marker: Option<u64>,
    },
    /// Copies `copy_source` (`bucket/encoded-key`) into a part. The copy-source headers stay
    /// signed headers, so the caller must send them unchanged.
    UploadPartCopy {
        upload_id: &'a str,
        part_number: u64,
        copy_source: &'a str,
        copy_source_if_match: Option<&'a str>,
    },
    DeleteObject,
    /// Bucket-level listing; sign it with an empty object key.
    ListObjectsV2 {
        prefix: &'a str,
        continuation_token: Option<&'a str>,
        max_keys: Option<u64>,
    },
}

type RequestParts = (&'static str, Vec<(String, String)>, Vec<(String, String)>);

impl S3Operation<'_> {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            S3Operation::GetObject { .. } => "GetObject",
            S3Operation::HeadObject => "HeadObject",
            S3Operation::PutObject { .. } => "PutObject",
            S3Operation::UploadPart { .. } => "UploadPart",
            S3Operation::CreateMultipartUpload { .. } => "CreateMultipartUpload",
            S3Operation::CompleteMultipartUpload { .. } => "CompleteMultipartUpload",
            S3Operation::AbortMultipartUpload { .. } => "AbortMultipartUpload",
            S3Operation::ListParts { .. } => "ListParts",
            S3Operation::UploadPartCopy { .. } => "UploadPartCopy",
            S3Operation::DeleteObject => "DeleteObject",
            S3Operation::ListObjectsV2 { .. } => "ListObjectsV2",
        }
    }

    /// Method, query and signed headers, following the S3 REST bindings (including the
    /// `x-id` operation marker the AWS SDKs add).
    fn parts(&self) -> RequestParts {
        let pair = |key: &str, value: &str| (key.to_string(), value.to_string());
        let x_id = |name: &str| pair("x-id", name);
        match self {
            S3Operation::GetObject {
                range,
                if_match,
                version_id,
            } => {
                let mut query = Vec::new();
                if let Some(version_id) = version_id {
                    query.push(pair("versionId", version_id));
                }
                query.push(x_id("GetObject"));
                let mut headers = Vec::new();
                if let Some(if_match) = if_match {
                    headers.push(pair("if-match", if_match));
                }
                if let Some(range) = range {
                    headers.push(pair("range", range));
                }
                ("GET", query, headers)
            }
            S3Operation::HeadObject => ("HEAD", Vec::new(), Vec::new()),
            S3Operation::PutObject { content_md5 } => (
                "PUT",
                vec![x_id("PutObject")],
                content_md5
                    .map(|md5| vec![pair("content-md5", md5)])
                    .unwrap_or_default(),
            ),
            S3Operation::UploadPart {
                upload_id,
                part_number,
                content_md5,
            } => (
                "PUT",
                vec![
                    pair("partNumber", &part_number.to_string()),
                    pair("uploadId", upload_id),
                    x_id("UploadPart"),
                ],
                content_md5
                    .map(|md5| vec![pair("content-md5", md5)])
                    .unwrap_or_default(),
            ),
            S3Operation::CreateMultipartUpload { metadata } => {
                let mut headers = metadata
                    .iter()
                    .map(|(key, value)| {
                        (
                            format!("x-amz-meta-{}", key.to_ascii_lowercase()),
                            value.clone(),
                        )
                    })
                    .collect::<Vec<_>>();
                headers.sort();
                (
                    "POST",
                    vec![pair("uploads", ""), x_id("CreateMultipartUpload")],
                    headers,
                )
            }
            S3Operation::CompleteMultipartUpload { upload_id } => (
                "POST",
                vec![pair("uploadId", upload_id)],
                vec![pair("content-type", "application/xml")],
            ),
            S3Operation::AbortMultipartUpload { upload_id } => (
                "DELETE",
                vec![pair("uploadId", upload_id), x_id("AbortMultipartUpload")],
                Vec::new(),
            ),
            S3Operation::ListParts {
                upload_id,
                max_parts,
                part_number_marker,
            } => {
                let mut query = Vec::new();
                if let Some(max_parts) = max_parts {
                    query.push(pair("max-parts", &max_parts.to_string()));
                }
                if let Some(marker) = part_number_marker {
                    query.push(pair("part-number-marker", &marker.to_string()));
                }
                query.push(pair("uploadId", upload_id));
                query.push(x_id("ListParts"));
                ("GET", query, Vec::new())
            }
            S3Operation::UploadPartCopy {
                upload_id,
                part_number,
                copy_source,
                copy_source_if_match,
            } => {
                let mut headers = vec![pair("x-amz-copy-source", copy_source)];
                if let Some(condition) = copy_source_if_match {
                    headers.push(pair("x-amz-copy-source-if-match", condition));
                }
                (
                    "PUT",
                    vec![
                        pair("partNumber", &part_number.to_string()),
                        pair("uploadId", upload_id),
                        x_id("UploadPartCopy"),
                    ],
                    headers,
                )
            }
            S3Operation::DeleteObject => ("DELETE", vec![x_id("DeleteObject")], Vec::new()),
            S3Operation::ListObjectsV2 {
                prefix,
                continuation_token,
                max_keys,
            } => {
                let mut query = vec![pair("list-type", "2")];
                if let Some(max_keys) = max_keys {
                    query.push(pair("max-keys", &max_keys.to_string()));
                }
                query.push(pair("prefix", prefix));
                if let Some(token) = continuation_token {
                    query.push(pair("continuation-token", token));
                }
                ("GET", query, Vec::new())
            }
        }
    }
}

fn is_retryable_s3_failure(status: u16, code: Option<&str>, embedded_error: bool) -> bool {
    const RETRYABLE_CODES: &[&str] = &[
        "SlowDown",
        "Throttling",
        "ThrottlingException",
        "RequestTimeout",
        "RequestLimitExceeded",
        "InternalError",
        "ServiceUnavailable",
        "TooManyRequestsException",
    ];
    matches!(status, 429 | 500 | 502 | 503 | 504)
        || code.is_some_and(|code| RETRYABLE_CODES.contains(&code))
        || (embedded_error && code.is_none())
}

async fn retry_delay(attempt: u32) {
    // Full jitter over an exponential base, capped at 20 s (the AWS SDK standard strategy).
    let base_ms = (100_u64 << attempt.min(8)).min(20_000);
    let jitter = (Uuid::new_v4().as_u128() % 1_000) as u64;
    tokio::time::sleep(Duration::from_millis((base_ms * jitter / 1_000).max(1))).await;
}

/// Buckets that can be addressed as a host label (DNS-compatible, 3-63 characters).
fn is_virtual_hostable_bucket(bucket: &str, allow_dots: bool) -> bool {
    if bucket.len() < 3 || bucket.len() > 63 || bucket.parse::<IpAddr>().is_ok() {
        return false;
    }
    let label_ok = |label: &str| {
        !label.is_empty()
            && label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            && !label.starts_with('-')
            && !label.ends_with('-')
    };
    if bucket.contains('.') {
        allow_dots && bucket.split('.').all(label_ok)
    } else {
        label_ok(bucket)
    }
}

pub(crate) fn has_text(value: &str) -> bool {
    !value.trim().is_empty()
}

/// The text of the first `<tag>` element, XML-unescaped.
pub(crate) fn xml_text(body: &str, tag: &str) -> Option<String> {
    xml_elements(body, tag).into_iter().next().map(xml_unescape)
}

/// The inner text of every `<tag>` element, in document order.
pub(crate) fn xml_elements<'a>(body: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let mut elements = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        let after_name = &rest[start + open.len()..];
        // Skip longer tag names sharing the prefix (e.g. <PartNumber> when looking for <Part>).
        if !after_name.starts_with('>') && !after_name.starts_with(' ') {
            rest = after_name;
            continue;
        }
        let Some(content_start) = after_name.find('>') else {
            break;
        };
        let content = &after_name[content_start + 1..];
        let Some(end) = content.find(&close) else {
            break;
        };
        elements.push(&content[..end]);
        rest = &content[end + close.len()..];
    }
    elements
}

pub(crate) fn xml_unescape(value: &str) -> String {
    if !value.contains('&') {
        return value.to_string();
    }
    let mut output = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find('&') {
        output.push_str(&rest[..start]);
        let entity_rest = &rest[start..];
        let Some(end) = entity_rest.find(';') else {
            output.push_str(entity_rest);
            return output;
        };
        let entity = &entity_rest[1..end];
        let decoded = match entity {
            "quot" => Some('"'),
            "amp" => Some('&'),
            "apos" => Some('\''),
            "lt" => Some('<'),
            "gt" => Some('>'),
            _ if entity.starts_with("#x") => u32::from_str_radix(&entity[2..], 16)
                .ok()
                .and_then(char::from_u32),
            _ if entity.starts_with('#') => entity[1..].parse().ok().and_then(char::from_u32),
            _ => None,
        };
        match decoded {
            Some(character) => output.push(character),
            None => output.push_str(&entity_rest[..=end]),
        }
        rest = &entity_rest[end + 1..];
    }
    output.push_str(rest);
    output
}

pub(crate) fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(provider: &str, endpoint: Option<&str>, force: Option<bool>) -> S3Settings {
        S3Settings {
            storage_location: None,
            provider: provider.to_string(),
            bucket: "bucket".to_string(),
            key: "file.bin".to_string(),
            region: None,
            endpoint_url: endpoint.map(str::to_string),
            account_id: None,
            force_path_style: force,
            credentials: Credentials {
                access_key_id: "ak".to_string(),
                secret_access_key: "sk".to_string(),
                session_token: None,
            },
        }
    }

    #[test]
    fn aws_s3_uses_regional_virtual_hosting() {
        let (origin, host, path) = settings("s3", None, None)
            .object_location("dir/a b+c.bin")
            .unwrap();
        assert_eq!(origin, "https://bucket.s3.us-east-1.amazonaws.com");
        assert_eq!(host, "bucket.s3.us-east-1.amazonaws.com");
        assert_eq!(path, "/dir/a%20b%2Bc.bin");
        let dotted = S3Settings {
            bucket: "my.bucket".to_string(),
            ..settings("s3", None, None)
        };
        assert_eq!(
            dotted.object_location("k").unwrap().2,
            "/my.bucket/k",
            "dotted buckets cannot be TLS virtual hosts"
        );
    }

    #[test]
    fn object_keys_with_dot_segments_are_rejected_before_signing() {
        let s3 = settings("s3", Some("http://127.0.0.1:9000"), None);
        for key in ["a/../b", "../b", "a/..", "..", "./b", "a/./b", "a/.", "."] {
            let error = s3
                .presign(
                    &S3Operation::PutObject { content_md5: None },
                    key,
                    Duration::from_secs(60),
                )
                .unwrap_err();
            assert!(
                matches!(&error, BeamApiError::InvalidArgument(message) if message.contains("path segment")),
                "{key}: {error:?}"
            );
        }
        // Dots inside a segment are ordinary key characters.
        for key in ["a/.../b", "a..b", ".hidden/x", "x/.y", "dir/..z"] {
            let url = s3
                .presign(
                    &S3Operation::PutObject { content_md5: None },
                    key,
                    Duration::from_secs(60),
                )
                .unwrap();
            let parsed = reqwest::Url::parse(&url).unwrap();
            assert_eq!(parsed.path(), format!("/bucket/{key}"), "{key}");
        }
    }

    #[test]
    fn custom_endpoints_use_path_style_for_ips_and_non_s3_providers() {
        let (origin, host, path) = settings("s3", Some("http://127.0.0.1:9000"), None)
            .object_location("k")
            .unwrap();
        assert_eq!(origin, "http://127.0.0.1:9000");
        assert_eq!(host, "127.0.0.1:9000");
        assert_eq!(path, "/bucket/k");
        let (_, host, path) = settings("wasabi", Some("https://s3.wasabisys.com"), None)
            .object_location("k")
            .unwrap();
        assert_eq!(
            (host.as_str(), path.as_str()),
            ("s3.wasabisys.com", "/bucket/k")
        );
        let (_, host, path) = settings("wasabi", Some("https://s3.wasabisys.com"), Some(false))
            .object_location("k")
            .unwrap();
        assert_eq!(
            (host.as_str(), path.as_str()),
            ("bucket.s3.wasabisys.com", "/k")
        );
    }

    #[test]
    fn xml_helpers_read_s3_responses() {
        let body =
            "<?xml version=\"1.0\"?><ListPartsResult xmlns=\"x\"><IsTruncated>true</IsTruncated>\
            <Part><PartNumber>1</PartNumber><ETag>&quot;a&quot;</ETag><Size>6</Size></Part>\
            <Part><PartNumber>2</PartNumber><ETag>b</ETag><Size>6</Size></Part></ListPartsResult>";
        let parts = xml_elements(body, "Part");
        assert_eq!(parts.len(), 2);
        assert_eq!(xml_text(parts[0], "ETag").as_deref(), Some("\"a\""));
        assert_eq!(xml_text(parts[1], "PartNumber").as_deref(), Some("2"));
        assert_eq!(xml_text(body, "IsTruncated").as_deref(), Some("true"));
        assert_eq!(xml_escape("\"a&b\""), "&quot;a&amp;b&quot;");
        assert_eq!(xml_unescape("&#34;x&#x27;"), "\"x'");
    }

    #[test]
    fn presigned_operations_carry_their_s3_bindings() {
        let settings = settings("r2", Some("https://account.r2.cloudflarestorage.com"), None);
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let url = settings
            .presign_at(
                &S3Operation::ListParts {
                    upload_id: "upload/1",
                    max_parts: Some(1_000),
                    part_number_marker: Some(1_000),
                },
                "k",
                Duration::from_secs(600),
                now,
            )
            .unwrap();
        assert!(url.starts_with("https://account.r2.cloudflarestorage.com/bucket/k?"));
        assert!(url.contains("x-id=ListParts"));
        assert!(url.contains("uploadId=upload%2F1"));
        assert!(url.contains("part-number-marker=1000"));
        assert!(url.contains("%2Fauto%2Fs3%2Faws4_request"));
        // Deterministic for a fixed clock and credentials.
        assert_eq!(
            url,
            settings
                .presign_at(
                    &S3Operation::ListParts {
                        upload_id: "upload/1",
                        max_parts: Some(1_000),
                        part_number_marker: Some(1_000),
                    },
                    "k",
                    Duration::from_secs(600),
                    now,
                )
                .unwrap()
        );
    }
}
