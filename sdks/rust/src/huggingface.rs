//! Hugging Face Hub protocol helpers.
//!
//! The Hub is reached over plain HTTP: a resolve URL that redirects to a presigned CDN URL for
//! reads, and the preupload / LFS batch / completion / commit sequence for writes. The token
//! never leaves this process; only the presigned URLs the Hub hands back are passed on.

use std::collections::HashMap;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use md5::Md5;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::models::{HuggingFaceProviderDestination, HuggingFaceProviderSource};

pub const HUGGINGFACE_DEFAULT_ENDPOINT: &str = "https://huggingface.co";
pub const HUGGINGFACE_DEFAULT_REVISION: &str = "main";
pub const HUGGINGFACE_DEFAULT_REPO_TYPE: &str = "model";

const LFS_CONTENT_TYPE: &str = "application/vnd.git-lfs+json";

/// Mirrors `constants.REPO_TYPES_URL_PREFIXES`; models carry no prefix.
fn repo_type_url_prefix(repo_type: &str) -> &'static str {
    match repo_type {
        "dataset" => "datasets/",
        "space" => "spaces/",
        "kernel" => "kernels/",
        "bucket" => "buckets/",
        _ => "",
    }
}

/// The subset of the source and destination configs the protocol needs.
#[derive(Debug, Clone)]
pub struct HuggingFaceConfig {
    pub repo_id: String,
    pub path: String,
    pub repo_type: String,
    pub revision: String,
    pub token: String,
    pub endpoint: String,
    pub commit_message: Option<String>,
    pub commit_description: Option<String>,
    pub create_pr: bool,
}

impl HuggingFaceConfig {
    pub fn from_source(source: &HuggingFaceProviderSource) -> Self {
        Self {
            repo_id: source.repo_id.clone(),
            path: source.path.clone(),
            repo_type: source
                .repo_type
                .clone()
                .unwrap_or_else(|| HUGGINGFACE_DEFAULT_REPO_TYPE.to_string()),
            revision: source
                .revision
                .clone()
                .unwrap_or_else(|| HUGGINGFACE_DEFAULT_REVISION.to_string()),
            token: source.token.clone(),
            endpoint: normalize_endpoint(source.endpoint.as_deref()),
            commit_message: None,
            commit_description: None,
            create_pr: false,
        }
    }

    pub fn from_destination(destination: &HuggingFaceProviderDestination) -> Self {
        Self {
            repo_id: destination.repo_id.clone(),
            path: destination.path.clone(),
            repo_type: destination
                .repo_type
                .clone()
                .unwrap_or_else(|| HUGGINGFACE_DEFAULT_REPO_TYPE.to_string()),
            revision: destination
                .revision
                .clone()
                .unwrap_or_else(|| HUGGINGFACE_DEFAULT_REVISION.to_string()),
            token: destination.token.clone(),
            endpoint: normalize_endpoint(destination.endpoint.as_deref()),
            commit_message: destination.commit_message.clone(),
            commit_description: destination.commit_description.clone(),
            create_pr: destination.create_pr,
        }
    }

    pub fn describe(&self) -> String {
        format!(
            "{} {}@{}/{}",
            self.repo_type, self.repo_id, self.revision, self.path
        )
    }

    /// `{endpoint}/{prefix}{repo_id}/resolve/{revision}/{path}`, as built by `hf_hub_url`.
    ///
    /// Buckets are unversioned and take no revision segment, and the Hub escapes their whole key
    /// as one component — see `HfApi.get_bucket_file_metadata`.
    pub fn resolve_url(&self) -> String {
        if self.repo_type == "bucket" {
            return format!(
                "{}/{}{}/resolve/{}",
                self.endpoint,
                repo_type_url_prefix(&self.repo_type),
                self.repo_id,
                percent_encode_all(&self.path)
            );
        }
        format!(
            "{}/{}{}/resolve/{}/{}",
            self.endpoint,
            repo_type_url_prefix(&self.repo_type),
            self.repo_id,
            percent_encode_all(&self.revision),
            self.path
                .split('/')
                .map(percent_encode_all)
                .collect::<Vec<_>>()
                .join("/")
        )
    }

    /// `{endpoint}/api/{repo_type}s/{repo_id}`.
    pub fn api_base(&self) -> String {
        format!("{}/api/{}s/{}", self.endpoint, self.repo_type, self.repo_id)
    }

    /// `{endpoint}/{prefix}{repo_id}.git/info/lfs/objects/batch`.
    pub fn lfs_batch_url(&self) -> String {
        format!(
            "{}/{}{}.git/info/lfs/objects/batch",
            self.endpoint,
            repo_type_url_prefix(&self.repo_type),
            self.repo_id
        )
    }

    fn query_suffix(&self) -> &'static str {
        if self.create_pr {
            "?create_pr=1"
        } else {
            ""
        }
    }
}

/// What a HEAD on the resolve URL tells us about a file.
#[derive(Debug, Clone)]
pub struct HuggingFaceFileMetadata {
    /// Credential-free presigned CDN URL the workers read from.
    pub url: String,
    pub size: u64,
    /// sha256 for an LFS blob, git sha1 otherwise.
    pub etag: Option<String>,
    pub commit_hash: Option<String>,
}

/// Upload instructions for one object, as returned by the LFS batch endpoint.
#[derive(Debug, Clone, Default)]
pub struct HuggingFaceUploadPlan {
    pub oid: String,
    pub size: u64,
    /// Multipart completion endpoint, or the single-part PUT target. `None` when the Hub
    /// already stores this content and no upload is needed.
    pub upload_href: Option<String>,
    /// Part size the Hub requires. `None` for a single-part upload.
    pub chunk_size: Option<u64>,
    /// Presigned part PUT URLs, ordered by part number.
    pub part_urls: Vec<String>,
    pub verify_href: Option<String>,
}

/// Resolve the path an upload commits to, expanding a trailing-slash prefix.
pub fn target_path(destination_path: &str, source_filename: Option<&str>) -> Result<String, String> {
    let trimmed = destination_path.trim_start_matches('/');
    if !trimmed.ends_with('/') {
        return Ok(trimmed.to_string());
    }
    match source_filename {
        Some(filename) if !filename.is_empty() => Ok(format!("{trimmed}{filename}")),
        _ => Err(format!(
            "huggingface path {destination_path} is a folder and the source has no filename"
        )),
    }
}

pub fn metadata(
    config: &HuggingFaceConfig,
    etag: Option<&str>,
    commit_hash: Option<&str>,
) -> HashMap<String, Value> {
    let mut metadata = HashMap::from([
        ("driver".to_string(), json!("huggingface")),
        ("repo_id".to_string(), json!(config.repo_id)),
        ("repo_type".to_string(), json!(config.repo_type)),
        ("revision".to_string(), json!(config.revision)),
        ("path".to_string(), json!(config.path)),
        ("endpoint".to_string(), json!(config.endpoint)),
    ]);
    // For an LFS blob the Hub's linked ETag is the object's sha256.
    if let Some(etag) = etag {
        metadata.insert("sha256".to_string(), json!(etag));
    }
    if let Some(commit_hash) = commit_hash {
        metadata.insert("commit_hash".to_string(), json!(commit_hash));
    }
    metadata
}

/// The error message a caller sees when the Hub serves a file inline instead of redirecting.
pub fn no_redirect_error(config: &HuggingFaceConfig) -> String {
    format!(
        "Hugging Face did not redirect {} to a presigned CDN URL. Beam reads this file over plain \
         HTTP without forwarding your token, and the Hub only redirects for large-file (LFS or \
         Xet) content. A small regular file is served inline from the Hub instead and cannot be \
         transferred",
        config.describe()
    )
}

/// Build the LFS batch request body for one object.
pub fn lfs_batch_body(config: &HuggingFaceConfig, oid: &str, size: u64) -> Value {
    json!({
        "operation": "upload",
        "transfers": ["basic", "multipart"],
        "hash_algo": "sha256",
        "ref": { "name": config.revision },
        "objects": [{ "oid": oid, "size": size }],
    })
}

/// Build the preupload request body for one path.
pub fn preupload_body(config: &HuggingFaceConfig, size: u64, sample: &str) -> Value {
    json!({ "files": [{ "path": config.path, "sample": sample, "size": size }] })
}

/// Build the multipart completion body: `{oid, parts:[{partNumber, etag}]}`.
pub fn completion_body(oid: &str, etags: &[String]) -> Value {
    json!({
        "oid": oid,
        "parts": etags
            .iter()
            .enumerate()
            .map(|(index, etag)| json!({ "partNumber": index + 1, "etag": etag }))
            .collect::<Vec<_>>(),
    })
}

/// Build the NDJSON commit body: a header line then one `lfsFile` line.
pub fn commit_ndjson(config: &HuggingFaceConfig, oid: &str, size: u64) -> String {
    let summary = config
        .commit_message
        .clone()
        .unwrap_or_else(|| format!("Upload {} with Beam", config.path));
    let lines = [
        json!({
            "key": "header",
            "value": {
                "summary": summary,
                "description": config.commit_description.clone().unwrap_or_default(),
            },
        }),
        json!({
            "key": "lfsFile",
            "value": { "path": config.path, "algo": "sha256", "oid": oid, "size": size },
        }),
    ];
    lines
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn preupload_url(config: &HuggingFaceConfig) -> String {
    format!(
        "{}/preupload/{}{}",
        config.api_base(),
        percent_encode_all(&config.revision),
        config.query_suffix()
    )
}

pub fn commit_url(config: &HuggingFaceConfig) -> String {
    format!(
        "{}/commit/{}{}",
        config.api_base(),
        percent_encode_all(&config.revision),
        config.query_suffix()
    )
}

pub fn lfs_headers(token: &str) -> [(&'static str, String); 3] {
    [
        ("Authorization", format!("Bearer {token}")),
        ("Accept", LFS_CONTENT_TYPE.to_string()),
        ("Content-Type", LFS_CONTENT_TYPE.to_string()),
    ]
}

/// Read the file metadata out of a HEAD response's headers.
///
/// `location` must be a cross-host redirect target: it is presigned and carries no credential,
/// so it is the only form of this URL that may be handed to BeamCore and the workers.
pub fn file_metadata_from_headers(
    config: &HuggingFaceConfig,
    resolve_url: &str,
    location: Option<&str>,
    linked_size: Option<&str>,
    content_length: Option<&str>,
    linked_etag: Option<&str>,
    etag: Option<&str>,
    commit_hash: Option<&str>,
) -> Result<HuggingFaceFileMetadata, String> {
    let location = match location {
        Some(location) if !same_host(resolve_url, location) => location,
        _ => return Err(no_redirect_error(config)),
    };

    let size = linked_size
        .or(content_length)
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|size| *size > 0)
        .ok_or_else(|| {
            format!(
                "Hugging Face did not report a size for {}",
                config.describe()
            )
        })?;

    Ok(HuggingFaceFileMetadata {
        url: location.to_string(),
        size,
        etag: linked_etag.or(etag).map(normalize_etag),
        commit_hash: commit_hash.map(str::to_string),
    })
}

/// Read the upload plan out of an LFS batch response body.
pub fn upload_plan_from_batch(
    config: &HuggingFaceConfig,
    payload: &Value,
    size: u64,
) -> Result<HuggingFaceUploadPlan, String> {
    let object = payload
        .get("objects")
        .and_then(Value::as_array)
        .and_then(|objects| objects.first())
        .ok_or_else(|| {
            format!(
                "Hugging Face LFS batch returned a malformed response for {}",
                config.describe()
            )
        })?;

    if let Some(error) = object.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown error");
        return Err(format!(
            "Hugging Face LFS batch rejected {}: {message}",
            config.describe()
        ));
    }

    let oid = object
        .get("oid")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!(
                "Hugging Face LFS batch returned a malformed response for {}",
                config.describe()
            )
        })?
        .to_string();

    let upload = match object.get("actions").and_then(|actions| actions.get("upload")) {
        Some(upload) => upload,
        // No actions means the Hub already stores this content; only the commit is left to do.
        None => return Ok(HuggingFaceUploadPlan { oid, size, ..Default::default() }),
    };
    let href = upload
        .get("href")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!(
                "Hugging Face LFS batch returned no upload href for {}",
                config.describe()
            )
        })?
        .to_string();

    let header = upload.get("header").and_then(Value::as_object);
    let chunk_size = match header.and_then(|header| header.get("chunk_size")) {
        Some(raw) => {
            let parsed = raw
                .as_str()
                .and_then(|value| value.parse::<u64>().ok())
                .or_else(|| raw.as_u64())
                .filter(|value| *value > 0);
            Some(parsed.ok_or_else(|| {
                format!(
                    "Hugging Face LFS batch returned a malformed chunk_size '{raw}' for {}",
                    config.describe()
                )
            })?)
        }
        None => None,
    };

    let mut parts: Vec<(u32, String)> = header
        .map(|header| {
            header
                .iter()
                .filter_map(|(key, value)| {
                    let number = key.parse::<u32>().ok()?;
                    Some((number, value.as_str()?.to_string()))
                })
                .collect()
        })
        .unwrap_or_default();
    parts.sort_by_key(|(number, _)| *number);
    let part_urls: Vec<String> = parts.into_iter().map(|(_, url)| url).collect();

    if let Some(chunk_size) = chunk_size {
        let expected = size.div_ceil(chunk_size) as usize;
        if part_urls.len() != expected {
            return Err(format!(
                "Hugging Face returned {} part URLs for {}, expected {expected} at chunk_size {chunk_size}",
                part_urls.len(),
                config.describe()
            ));
        }
    }

    Ok(HuggingFaceUploadPlan {
        oid,
        size,
        upload_href: Some(href),
        chunk_size,
        part_urls,
        verify_href: object
            .get("actions")
            .and_then(|actions| actions.get("verify"))
            .and_then(|verify| verify.get("href"))
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

/// Read whether a preupload response marks a path as an LFS blob.
pub fn upload_mode_from_preupload(
    config: &HuggingFaceConfig,
    payload: &Value,
) -> Result<(String, bool), String> {
    let file = payload
        .get("files")
        .and_then(Value::as_array)
        .and_then(|files| files.first())
        .ok_or_else(|| {
            format!(
                "Hugging Face preupload returned a malformed response for {}",
                config.describe()
            )
        })?;
    let mode = file
        .get("uploadMode")
        .and_then(Value::as_str)
        .filter(|mode| *mode == "lfs" || *mode == "regular")
        .ok_or_else(|| {
            format!(
                "Hugging Face preupload returned a malformed response for {}",
                config.describe()
            )
        })?;
    let should_ignore = file
        .get("shouldIgnore")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok((mode.to_string(), should_ignore))
}

/// Incrementally hash a streamed body: the sha256 of the whole object, and the MD5 of every part.
///
/// The Hub will not issue upload URLs without the sha256, and the per-part MD5 is the ETag the
/// completion payload has to quote.
pub struct SourceHasher {
    whole: Option<Sha256>,
    part_size: Option<u64>,
    part: Md5,
    part_bytes: u64,
    part_etags: Vec<String>,
}

impl SourceHasher {
    pub fn new(part_size: Option<u64>, whole_sha256: bool) -> Self {
        Self {
            whole: whole_sha256.then(Sha256::new),
            part_size,
            part: Md5::new(),
            part_bytes: 0,
            part_etags: Vec::new(),
        }
    }

    pub fn update(&mut self, mut chunk: &[u8]) {
        if let Some(whole) = self.whole.as_mut() {
            whole.update(chunk);
        }
        let Some(part_size) = self.part_size else {
            return;
        };
        while !chunk.is_empty() {
            let room = (part_size - self.part_bytes) as usize;
            if chunk.len() < room {
                self.part.update(chunk);
                self.part_bytes += chunk.len() as u64;
                return;
            }
            self.part.update(&chunk[..room]);
            self.part_etags
                .push(hex(&std::mem::take(&mut self.part).finalize()));
            self.part_bytes = 0;
            chunk = &chunk[room..];
        }
    }

    pub fn finish(mut self) -> (Option<String>, Vec<String>) {
        if self.part_size.is_some() && self.part_bytes > 0 {
            self.part_etags.push(hex(&self.part.finalize()));
        }
        let digest = self.whole.map(|whole| hex(&whole.finalize()));
        (digest, self.part_etags)
    }
}

pub fn encode_sample(bytes: &[u8]) -> String {
    BASE64.encode(bytes)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn normalize_etag(value: &str) -> String {
    value
        .strip_prefix("W/")
        .unwrap_or(value)
        .trim_matches('"')
        .to_string()
}

fn normalize_endpoint(endpoint: Option<&str>) -> String {
    endpoint
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(HUGGINGFACE_DEFAULT_ENDPOINT)
        .trim_end_matches('/')
        .to_string()
}

fn same_host(left: &str, right: &str) -> bool {
    match (host_of(left), host_of(right)) {
        (Some(left), Some(right)) => left == right,
        // A relative Location resolves against the request URL, so it is the same host.
        (Some(_), None) => true,
        _ => true,
    }
}

fn host_of(url: &str) -> Option<&str> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    Some(rest.split(['/', '?', '#']).next().unwrap_or(rest))
}

/// Percent-encode one path or query segment, escaping every reserved character.
fn percent_encode_all(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset_config() -> HuggingFaceConfig {
        HuggingFaceConfig::from_source(&HuggingFaceProviderSource {
            source_id: None,
            repo_id: "acme/corpus".to_string(),
            path: "data/train.parquet".to_string(),
            repo_type: Some("dataset".to_string()),
            revision: Some("refs/pr/4".to_string()),
            token: "hf_test_token".to_string(),
            endpoint: None,
        })
    }

    #[test]
    fn urls_follow_the_repo_type_prefix_and_revision_encoding() {
        let config = dataset_config();
        assert_eq!(
            config.resolve_url(),
            "https://huggingface.co/datasets/acme/corpus/resolve/refs%2Fpr%2F4/data/train.parquet"
        );
        assert_eq!(
            config.api_base(),
            "https://huggingface.co/api/datasets/acme/corpus"
        );
        assert_eq!(
            config.lfs_batch_url(),
            "https://huggingface.co/datasets/acme/corpus.git/info/lfs/objects/batch"
        );

        // A model repo carries no prefix and defaults to `main`.
        let model = HuggingFaceConfig::from_source(&HuggingFaceProviderSource {
            source_id: None,
            repo_id: "acme/net".to_string(),
            path: "model.safetensors".to_string(),
            repo_type: None,
            revision: None,
            token: "hf_test_token".to_string(),
            endpoint: None,
        });
        assert_eq!(
            model.resolve_url(),
            "https://huggingface.co/acme/net/resolve/main/model.safetensors"
        );
        assert_eq!(model.api_base(), "https://huggingface.co/api/models/acme/net");
    }

    #[test]
    fn a_bucket_is_unversioned_and_escapes_its_whole_key() {
        let bucket = HuggingFaceConfig::from_source(&HuggingFaceProviderSource {
            source_id: None,
            repo_id: "acme/store".to_string(),
            path: "nested/data.bin".to_string(),
            repo_type: Some("bucket".to_string()),
            revision: Some("v2".to_string()),
            token: "hf_test_token".to_string(),
            endpoint: None,
        });
        // No revision segment, and the key is escaped whole — see HfApi.get_bucket_file_metadata.
        assert_eq!(
            bucket.resolve_url(),
            "https://huggingface.co/buckets/acme/store/resolve/nested%2Fdata.bin"
        );
    }

    #[test]
    fn cdn_redirect_is_accepted_and_linked_size_wins() {
        let config = dataset_config();
        let metadata = file_metadata_from_headers(
            &config,
            &config.resolve_url(),
            Some("https://cdn-lfs.hf.co/repos/blob?sig=abc"),
            Some("4096"),
            Some("0"),
            Some("\"aaaa\""),
            None,
            Some("deadbeef"),
        )
        .expect("metadata");
        assert_eq!(metadata.size, 4096);
        assert_eq!(metadata.etag.as_deref(), Some("aaaa"));
        assert_eq!(metadata.commit_hash.as_deref(), Some("deadbeef"));
    }

    #[test]
    fn inline_response_is_rejected_naming_the_regular_blob_case() {
        let config = dataset_config();
        let error = file_metadata_from_headers(
            &config,
            &config.resolve_url(),
            None,
            None,
            Some("512"),
            None,
            Some("\"0123\""),
            None,
        )
        .expect_err("expected a rejection");
        assert!(error.contains("A small regular file is served inline from the Hub"), "{error}");
    }

    #[test]
    fn same_host_redirect_is_rejected() {
        let config = dataset_config();
        let error = file_metadata_from_headers(
            &config,
            &config.resolve_url(),
            Some("https://huggingface.co/datasets/acme/corpus/resolve/main/renamed.parquet"),
            Some("4096"),
            None,
            None,
            None,
            None,
        )
        .expect_err("expected a rejection");
        assert!(error.contains("presigned CDN URL"), "{error}");
    }

    #[test]
    fn part_urls_sort_numerically_and_the_count_is_checked() {
        let config = dataset_config();
        // Emitted out of order, with zero padding that would sort wrong lexically past 9.
        let payload = json!({
            "objects": [{
                "oid": "b".repeat(64),
                "size": 3000,
                "actions": {
                    "upload": {
                        "href": "https://hub/lfs/complete",
                        "header": {
                            "chunk_size": "1024",
                            "00003": "https://s3/part/3",
                            "00001": "https://s3/part/1",
                            "00002": "https://s3/part/2",
                        },
                    },
                    "verify": { "href": "https://hub/lfs/verify" },
                },
            }],
        });
        let plan = upload_plan_from_batch(&config, &payload, 3000).expect("plan");
        assert_eq!(plan.chunk_size, Some(1024));
        assert_eq!(
            plan.part_urls,
            vec!["https://s3/part/1", "https://s3/part/2", "https://s3/part/3"]
        );
        assert_eq!(plan.verify_href.as_deref(), Some("https://hub/lfs/verify"));

        let short = json!({
            "objects": [{
                "oid": "c".repeat(64),
                "size": 3000,
                "actions": { "upload": {
                    "href": "https://hub/lfs/complete",
                    "header": { "chunk_size": "1024", "00001": "https://s3/part/1" },
                }},
            }],
        });
        let error = upload_plan_from_batch(&config, &short, 3000).expect_err("expected a rejection");
        assert!(error.contains("expected 3 at chunk_size 1024"), "{error}");
    }

    #[test]
    fn an_already_stored_object_has_no_upload_actions() {
        let config = dataset_config();
        let payload = json!({ "objects": [{ "oid": "d".repeat(64), "size": 3000 }] });
        let plan = upload_plan_from_batch(&config, &payload, 3000).expect("plan");
        assert!(plan.upload_href.is_none());
        assert_eq!(plan.oid, "d".repeat(64));
    }

    #[test]
    fn request_bodies_match_the_hub_protocol() {
        let config = dataset_config();
        assert_eq!(
            lfs_batch_body(&config, &"b".repeat(64), 3000),
            json!({
                "operation": "upload",
                "transfers": ["basic", "multipart"],
                "hash_algo": "sha256",
                "ref": { "name": "refs/pr/4" },
                "objects": [{ "oid": "b".repeat(64), "size": 3000 }],
            })
        );
        assert_eq!(
            preupload_body(&config, 3000, "AAAA"),
            json!({ "files": [{ "path": "data/train.parquet", "sample": "AAAA", "size": 3000 }] })
        );
        assert_eq!(
            completion_body(&"b".repeat(64), &["e1".to_string(), "e2".to_string()]),
            json!({
                "oid": "b".repeat(64),
                "parts": [
                    { "partNumber": 1, "etag": "e1" },
                    { "partNumber": 2, "etag": "e2" },
                ],
            })
        );

        let lines: Vec<Value> = commit_ndjson(&config, &"b".repeat(64), 3000)
            .lines()
            .map(|line| serde_json::from_str(line).expect("ndjson line"))
            .collect();
        assert_eq!(
            lines,
            vec![
                json!({
                    "key": "header",
                    "value": {
                        "summary": "Upload data/train.parquet with Beam",
                        "description": "",
                    },
                }),
                json!({
                    "key": "lfsFile",
                    "value": {
                        "path": "data/train.parquet",
                        "algo": "sha256",
                        "oid": "b".repeat(64),
                        "size": 3000,
                    },
                }),
            ]
        );
    }

    #[test]
    fn the_hash_pass_yields_the_sha256_and_one_md5_per_part() {
        let body: Vec<u8> = (0..2500u32).map(|index| (index % 251) as u8).collect();

        let mut whole = SourceHasher::new(None, true);
        // Feed it in uneven slices, the way a streamed body arrives.
        for chunk in body.chunks(333) {
            whole.update(chunk);
        }
        let (digest, parts) = whole.finish();
        assert_eq!(digest, Some(hex(&Sha256::digest(&body))));
        assert!(parts.is_empty());

        let mut parted = SourceHasher::new(Some(1024), false);
        for chunk in body.chunks(333) {
            parted.update(chunk);
        }
        let (digest, parts) = parted.finish();
        assert!(digest.is_none());
        assert_eq!(
            parts,
            vec![
                hex(&Md5::digest(&body[..1024])),
                hex(&Md5::digest(&body[1024..2048])),
                hex(&Md5::digest(&body[2048..])),
            ]
        );
    }

    #[test]
    fn a_folder_path_takes_the_source_filename() {
        assert_eq!(target_path("data/out.parquet", None).unwrap(), "data/out.parquet");
        assert_eq!(
            target_path("/data/", Some("train.parquet")).unwrap(),
            "data/train.parquet"
        );
        assert!(target_path("data/", None).is_err());
    }
}
