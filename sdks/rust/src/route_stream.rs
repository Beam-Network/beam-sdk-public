//! Route-stream submission and compact-plan helpers shared by every signing flow.

use crate::client::BeamClient;
use crate::error::BeamApiError;
use crate::multipart_limits::{multipart_part_number, MULTIPART_MAX_PART_NUMBER};
use crate::nats_control::{compact_signed_route_values, new_transfer_id};
use crate::performance::Collector;
use crate::{
    AttachSignedUrlsResponse, ChunkDestinationSigningTarget, ChunkSigningPlanItem,
    CompactTransferPlanDescriptor, MultipartGroupManifest, SignedChunkRoute,
};
use futures_util::future::try_join_all;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub(crate) const ROUTE_STREAM_BATCH_ROUTES: usize = 1_024;

/// Stream identity shared by the route sender and concurrent manifest publishers.
#[derive(Clone)]
pub(crate) struct RouteStreamHandle {
    pub(crate) telemetry: Arc<Mutex<Collector>>,
    client: BeamClient,
    transfer_id: String,
    stream_id: String,
    route_generation_id: String,
}

impl RouteStreamHandle {
    /// Publish multipart group manifests, one `transfer.route_stream.manifest` request per
    /// group so each group is acknowledged independently.
    pub(crate) async fn add_manifest_groups(
        &self,
        groups: &[MultipartGroupManifest],
    ) -> Result<(), BeamApiError> {
        let started = Instant::now();
        try_join_all(groups.iter().map(|group| async move {
            let identity = stable_manifest_batch_identity(std::slice::from_ref(group));
            let idempotency_key = format!(
                "transfer:{}:route-stream:{}:manifest:{}",
                self.transfer_id, self.stream_id, identity
            );
            let _: Value = self
                .client
                .lifecycle_request(
                    "transfer.route_stream.manifest",
                    json!({
                        "transfer_id": self.transfer_id,
                        "route_generation_id": self.route_generation_id,
                        "stream_id": self.stream_id,
                        "manifest_batch_id": identity,
                        "groups": [group],
                    }),
                    Some(&self.transfer_id),
                    Some(&idempotency_key),
                )
                .await?;
            Ok::<(), BeamApiError>(())
        }))
        .await?;
        self.telemetry
            .lock()
            .unwrap()
            .observe("sdk.manifest_ack", started);
        self.telemetry.lock().unwrap().mark("first_manifest_ms");
        Ok(())
    }
}

pub(crate) struct RouteStreamSender {
    pub(crate) telemetry: Arc<Mutex<Collector>>,
    handle: RouteStreamHandle,
    total_routes: usize,
    total_chunks: usize,
    auto_distribute: bool,
    urls_expires_at: Option<String>,
    signed_url_flow: &'static str,
    checksum: RouteKeysChecksum,
    batch: Vec<Value>,
    seen_delivery_indices: HashSet<u64>,
    batch_index: usize,
    route_count: usize,
    send_tail: Option<JoinHandle<Result<(), BeamApiError>>>,
}

impl RouteStreamSender {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: &BeamClient,
        transfer_id: String,
        total_routes: usize,
        total_chunks: usize,
        auto_distribute: bool,
        urls_expires_at: Option<String>,
        plan_identity: String,
        route_generation_id: String,
    ) -> Self {
        let signed_url_flow = "signed_url";
        let stream_id = stable_route_stream_id(
            &transfer_id,
            &plan_identity,
            total_routes,
            total_chunks,
            signed_url_flow,
        );
        let telemetry = Arc::new(Mutex::new(Collector::new()));
        Self {
            telemetry: telemetry.clone(),
            handle: RouteStreamHandle {
                telemetry,
                client: client.clone(),
                transfer_id,
                stream_id,
                route_generation_id,
            },
            total_routes,
            total_chunks,
            auto_distribute,
            urls_expires_at,
            signed_url_flow,
            checksum: RouteKeysChecksum::default(),
            batch: Vec::with_capacity(ROUTE_STREAM_BATCH_ROUTES),
            seen_delivery_indices: HashSet::new(),
            batch_index: 0,
            route_count: 0,
            send_tail: None,
        }
    }

    pub(crate) fn handle(&self) -> RouteStreamHandle {
        self.handle.clone()
    }

    pub(crate) async fn begin(&self) -> Result<(), BeamApiError> {
        let handle = &self.handle;
        let mut payload = json!({
            "transfer_id": handle.transfer_id,
            "route_generation_id": handle.route_generation_id,
            "stream_id": handle.stream_id,
            "total_routes": self.total_routes,
            "total_chunks": self.total_chunks,
            "route_contract_version": self.signed_url_flow,
            "signed_url_flow": self.signed_url_flow,
            "auto_distribute": self.auto_distribute,
        });
        if let Some(expires_at) = &self.urls_expires_at {
            payload["urls_expires_at"] = json!(expires_at);
        }
        let idempotency_key = format!(
            "transfer:{}:route-stream:{}:begin",
            handle.transfer_id, handle.stream_id
        );
        let _: Value = handle
            .client
            .lifecycle_request(
                "transfer.route_stream.begin",
                payload,
                Some(&handle.transfer_id),
                Some(&idempotency_key),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn add_manifest_groups(
        &self,
        groups: &[MultipartGroupManifest],
    ) -> Result<(), BeamApiError> {
        self.handle.add_manifest_groups(groups).await
    }

    pub(crate) async fn add_route(&mut self, route: SignedChunkRoute) -> Result<(), BeamApiError> {
        let started = Instant::now();
        self.telemetry.lock().unwrap().mark("first_route_ms");
        let delivery_index = signed_route_delivery_index(&route).ok_or_else(|| {
            BeamApiError::ProviderSigning("route delivery_index is required".to_string())
        })?;
        if delivery_index >= self.total_routes as u64 {
            return Err(BeamApiError::ProviderSigning(format!(
                "route delivery_index {delivery_index} is outside the declared route stream"
            )));
        }
        if !self.seen_delivery_indices.insert(delivery_index) {
            return Err(BeamApiError::ProviderSigning(format!(
                "duplicate route delivery_index {delivery_index}"
            )));
        }
        self.route_count += 1;
        let mut route = route;
        route.delivery_index = Some(delivery_index);
        self.checksum.add(&route_key_for_signed_route(&route));
        self.batch.push(serde_json::to_value(route)?);
        self.telemetry
            .lock()
            .unwrap()
            .observe("sdk.route_assembly", started);
        self.telemetry.lock().unwrap().gauge(
            "buffered_batches_peak",
            1.0 + if self.send_tail.is_some() { 1.0 } else { 0.0 },
        );
        if self.batch.len() >= ROUTE_STREAM_BATCH_ROUTES {
            self.enqueue_flush().await?;
        }
        Ok(())
    }

    pub(crate) async fn complete(&mut self) -> Result<AttachSignedUrlsResponse, BeamApiError> {
        self.telemetry.lock().unwrap().mark("final_flush_ms");
        if self.route_count != self.total_routes
            || self.seen_delivery_indices.len() != self.total_routes
        {
            return Err(BeamApiError::ProviderSigning(format!(
                "route stream has incomplete or duplicate delivery indices: received {} of {} routes",
                self.route_count, self.total_routes
            )));
        }
        self.enqueue_flush().await?;
        self.await_send_tail().await?;
        let v2 = self
            .handle
            .client
            .supports_performance_v2(&self.handle.transfer_id)
            .await;
        let summary = {
            let mut metrics = self.telemetry.lock().unwrap();
            let started = metrics.started;
            metrics.observe("sdk.preparation", started);
            metrics.mark("prepared_ms");
            metrics.snapshot_for(v2)
        };
        self.handle.client.diagnostics.emit(summary.clone());
        let handle = &self.handle;
        let idempotency_key = format!(
            "transfer:{}:route-stream:{}:complete",
            handle.transfer_id, handle.stream_id
        );
        handle
            .client
            .lifecycle_request(
                "transfer.route_stream.complete",
                json!({
                                   "transfer_id": handle.transfer_id,
                                   "route_generation_id": handle.route_generation_id,
                                   "stream_id": handle.stream_id,
                                   "sdk_performance": summary,
                "expected_batches": self.batch_index,
                                   "expected_routes": self.total_routes,
                                   "route_keys_checksum": self.checksum.value(),
                               }),
                Some(&handle.transfer_id),
                Some(&idempotency_key),
            )
            .await
    }

    pub(crate) async fn abort(&mut self) {
        if let Some(send_tail) = self.send_tail.take() {
            send_tail.abort();
            let _ = send_tail.await;
        }
    }

    async fn await_send_tail(&mut self) -> Result<(), BeamApiError> {
        if let Some(send_tail) = self.send_tail.take() {
            let started = Instant::now();
            send_tail.await.map_err(|error| {
                BeamApiError::Nats(format!("route stream sender task failed: {error}"))
            })??;
            self.telemetry
                .lock()
                .unwrap()
                .observe("sdk.buffer_wait", started);
        }
        Ok(())
    }

    async fn enqueue_flush(&mut self) -> Result<(), BeamApiError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        self.await_send_tail().await?;
        let routes = std::mem::take(&mut self.batch);
        let handle = self.handle.clone();
        let base_payload = json!({
            "transfer_id": handle.transfer_id,
            "route_generation_id": handle.route_generation_id,
            "stream_id": handle.stream_id,
            "batch_id": format!("{}:estimate", handle.stream_id),
            "batch_index": self.batch_index,
        });
        let encode_started = Instant::now();
        let chunks = handle.client.control().split_routes_for_payload(
            "transfer.route_stream.batch",
            &base_payload,
            &routes,
        )?;
        self.telemetry
            .lock()
            .unwrap()
            .observe("sdk.batch_encode", encode_started);
        let mut scheduled = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let batch_index = self.batch_index;
            self.batch_index += 1;
            let checksum_started = Instant::now();
            let route_keys_checksum = route_keys_checksum_for_values(&chunk)?;
            self.telemetry
                .lock()
                .unwrap()
                .observe("sdk.batch_checksum", checksum_started);
            scheduled.push((batch_index, chunk, route_keys_checksum));
        }
        let telemetry = self.telemetry.clone();
        let queued_at = Instant::now();
        self.send_tail = Some(tokio::spawn(crate::performance::CURRENT.scope(telemetry.clone(), async move {
            let RouteStreamHandle {
                client,
                transfer_id,
                stream_id,
                route_generation_id,
                ..
            } = handle;
            for (batch_index, chunk, route_keys_checksum) in scheduled {
                telemetry
                    .lock()
                    .unwrap()
                    .observe("sdk.batch_queue", queued_at);
                let route_count = chunk.len();
                telemetry
                    .lock()
                    .unwrap()
                    .gauge("batch_routes_max", route_count as f64);
                telemetry.lock().unwrap().mark("first_batch_ms");
                let coordinate_checksum = route_coordinate_checksum_for_values(&chunk)?;
                let batch_id = format!("{stream_id}:{batch_index}:{coordinate_checksum}");
                let idempotency_key = format!(
                    "transfer:{transfer_id}:route-stream:{stream_id}:batch:{batch_index}:{coordinate_checksum}"
                );
                let started = Instant::now();
                let _: Value = client
                    .lifecycle_request(
                        "transfer.route_stream.batch",
                        json!({
                            "transfer_id": transfer_id,
                            "route_generation_id": route_generation_id,
                            "stream_id": stream_id,
                            "batch_id": batch_id,
                            "batch_index": batch_index,
                            "route_batch": compact_signed_route_values(&chunk)?,
                            "route_count": route_count,
                            "route_keys_checksum": route_keys_checksum,
                        }),
                        Some(&transfer_id),
                        Some(&idempotency_key),
                    )
                    .await?;
                let mut metrics = telemetry.lock().unwrap();
                metrics.observe("sdk.batch_ack", started);
                metrics.batch();
            }
            Ok(())
        })));
        Ok(())
    }
}

impl Drop for RouteStreamSender {
    fn drop(&mut self) {
        if let Some(send_tail) = self.send_tail.take() {
            send_tail.abort();
        }
    }
}

#[derive(Default)]
struct RouteKeysChecksum {
    bytes: [u8; 32],
    count: usize,
}

impl RouteKeysChecksum {
    fn add(&mut self, route_key: &str) {
        let digest = Sha256::digest(route_key.as_bytes());
        for (index, value) in digest.iter().enumerate() {
            self.bytes[index] ^= value;
        }
        self.count += 1;
    }

    fn value(&self) -> String {
        format!("sha256-xor-v1:{}:{}", self.count, hex_bytes(&self.bytes))
    }
}

fn route_key_for_signed_route(route: &SignedChunkRoute) -> String {
    format!(
        "{}:{}:{}",
        route.source_id, route.destination_id, route.chunk_index
    )
}

pub(crate) fn signed_route_delivery_index(route: &SignedChunkRoute) -> Option<u64> {
    route
        .metadata
        .get("delivery_index")
        .and_then(Value::as_u64)
        .or(route.delivery_index)
}

/// Order routes by delivery index, then by route key, as the TypeScript SDK does.
pub(crate) fn sort_routes_by_delivery(routes: &mut [SignedChunkRoute]) {
    routes.sort_by(|left, right| {
        match (
            signed_route_delivery_index(left),
            signed_route_delivery_index(right),
        ) {
            (Some(left_index), Some(right_index)) if left_index != right_index => {
                left_index.cmp(&right_index)
            }
            _ => route_key_for_signed_route(left).cmp(&route_key_for_signed_route(right)),
        }
    });
}

pub(crate) fn route_coordinate_checksum_for_routes(routes: &[SignedChunkRoute]) -> String {
    let mut checksum = RouteKeysChecksum::default();
    for route in routes {
        checksum.add(&format!(
            "{}:{}",
            route_key_for_signed_route(route),
            signed_route_delivery_index(route)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "missing".to_string())
        ));
    }
    checksum.value()
}

fn stable_route_stream_id(
    transfer_id: &str,
    plan_identity: &str,
    total_routes: usize,
    total_chunks: usize,
    signed_url_flow: &str,
) -> String {
    stable_uuid_from_identity(&format!(
        "beam:route-stream:{signed_url_flow}:{transfer_id}:{plan_identity}:{total_routes}:{total_chunks}"
    ))
}

fn stable_manifest_batch_identity(groups: &[MultipartGroupManifest]) -> String {
    let mut identities = groups
        .iter()
        .map(|group| {
            format!(
                "{}:{}:{}",
                group.multipart_group_id, group.source_id, group.destination_id
            )
        })
        .collect::<Vec<_>>();
    identities.sort();
    hex_bytes(&Sha256::digest(identities.join("\n").as_bytes())[..16])
}

fn route_key_for_value(route: &Value) -> Result<String, BeamApiError> {
    let source_id = route
        .get("source_id")
        .and_then(Value::as_str)
        .ok_or_else(|| BeamApiError::ProviderSigning("route missing source_id".to_string()))?;
    let destination_id = route
        .get("destination_id")
        .and_then(Value::as_str)
        .ok_or_else(|| BeamApiError::ProviderSigning("route missing destination_id".to_string()))?;
    let chunk_index = route
        .get("chunk_index")
        .and_then(Value::as_u64)
        .ok_or_else(|| BeamApiError::ProviderSigning("route missing chunk_index".to_string()))?;
    Ok(format!("{}:{}:{}", source_id, destination_id, chunk_index))
}

fn route_keys_checksum_for_values(routes: &[Value]) -> Result<String, BeamApiError> {
    let mut checksum = RouteKeysChecksum::default();
    for route in routes {
        checksum.add(&route_key_for_value(route)?);
    }
    Ok(checksum.value())
}

fn route_coordinate_checksum_for_values(routes: &[Value]) -> Result<String, BeamApiError> {
    let mut checksum = RouteKeysChecksum::default();
    for route in routes {
        let delivery_index = route
            .get("delivery_index")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                BeamApiError::ProviderSigning("route missing delivery_index".to_string())
            })?;
        checksum.add(&format!("{}:{delivery_index}", route_key_for_value(route)?));
    }
    Ok(checksum.value())
}

pub(crate) fn count_distinct_route_chunks(routes: &[SignedChunkRoute]) -> usize {
    routes
        .iter()
        .map(|route| format!("{}:{}", route.source_id, route.chunk_index))
        .collect::<HashSet<_>>()
        .len()
}

/// Every plan chunk in source order, materialized from the compact descriptor.
pub(crate) struct CompactPlanChunkIter<'a> {
    descriptor: &'a CompactTransferPlanDescriptor,
    transfer_id: &'a str,
    source_index: usize,
    source_chunk_index: u64,
}

impl<'a> CompactPlanChunkIter<'a> {
    pub(crate) fn new(descriptor: &'a CompactTransferPlanDescriptor, transfer_id: &'a str) -> Self {
        Self {
            descriptor,
            transfer_id,
            source_index: 0,
            source_chunk_index: 0,
        }
    }
}

impl Iterator for CompactPlanChunkIter<'_> {
    type Item = Result<ChunkSigningPlanItem, BeamApiError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(source) = self.descriptor.sources.get(self.source_index) {
            if self.source_chunk_index >= source.chunk_count {
                self.source_index += 1;
                self.source_chunk_index = 0;
                continue;
            }
            let chunk_index = source.global_chunk_start + self.source_chunk_index;
            self.source_chunk_index += 1;
            return Some(materialize_plan_chunk(
                self.descriptor,
                self.transfer_id,
                &source.source.source_id,
                chunk_index,
            ));
        }
        None
    }
}

/// Expand one chunk of the compact plan into its source range and per-destination targets.
pub(crate) fn materialize_plan_chunk(
    descriptor: &CompactTransferPlanDescriptor,
    _transfer_id: &str,
    source_id: &str,
    chunk_index: u64,
) -> Result<ChunkSigningPlanItem, BeamApiError> {
    let source = descriptor
        .sources
        .iter()
        .find(|candidate| candidate.source.source_id == source_id)
        .ok_or_else(|| {
            BeamApiError::ProviderSigning(format!("plan source not found: {source_id}"))
        })?;
    let source_chunk_index = chunk_index
        .checked_sub(source.global_chunk_start)
        .filter(|index| *index < source.chunk_count)
        .ok_or_else(|| {
            BeamApiError::ProviderSigning(format!(
                "plan chunk is outside source range: {source_id}:{chunk_index}"
            ))
        })?;
    let source_offset = source_chunk_index * descriptor.chunk_size;
    let chunk_size = descriptor
        .chunk_size
        .min(source.source.size.saturating_sub(source_offset));
    let part_number = multipart_part_number(source_chunk_index, 0)?;
    let mut destinations = Vec::with_capacity(descriptor.destinations.len());
    for destination in &descriptor.destinations {
        let final_object_key = destination
            .final_object_keys
            .get(source_id)
            .cloned()
            .ok_or_else(|| {
                BeamApiError::ProviderSigning(format!(
                    "plan final object key not found: {source_id}:{}",
                    destination.destination.destination_id
                ))
            })?;
        let delivery_index =
            chunk_index * descriptor.destinations.len() as u64 + destination.destination_index;
        let mut metadata = destination.destination.metadata.clone();
        metadata.insert("final_object_key".to_string(), json!(final_object_key));
        metadata.insert("part_number".to_string(), json!(part_number));
        metadata.insert("logical_attempt_index".to_string(), json!(0));
        metadata.insert("attempt_slot".to_string(), json!(0));
        metadata.insert(
            "route_generation_id".to_string(),
            json!(format!(
                "initial-{chunk_index}-{}",
                destination.destination.destination_id
            )),
        );
        metadata.insert("delivery_index".to_string(), json!(delivery_index));
        let object_key = if destination
            .destination
            .provider
            .eq_ignore_ascii_case("hippius")
        {
            format!(
                "{}/{}/chunk-{:06}",
                final_object_key, descriptor.plan_nonce, source_chunk_index
            )
        } else {
            final_object_key.clone()
        };
        destinations.push(ChunkDestinationSigningTarget {
            destination_id: destination.destination.destination_id.clone(),
            provider: Some(destination.destination.provider.clone()),
            object_key: Some(object_key),
            metadata,
        });
    }
    Ok(ChunkSigningPlanItem {
        chunk_index,
        source_id: source_id.to_string(),
        source_chunk_index,
        source_offset,
        chunk_size,
        source_url: source.source.url.clone(),
        destinations,
    })
}

pub(crate) fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{:02x}", byte)).collect()
}

/// Validate multipart group manifests against the TypeScript SDK's contract.
pub(crate) fn validate_multipart_group_manifest(
    manifest: &[MultipartGroupManifest],
    transfer_id: &str,
) -> Result<(), BeamApiError> {
    let mut group_ids = HashSet::with_capacity(manifest.len());
    for group in manifest {
        let id = group.multipart_group_id.as_str();
        if id.is_empty() || !group_ids.insert(id) {
            return Err(BeamApiError::ProviderSigning(format!(
                "multipart_group_id must be non-empty and unique: {id}"
            )));
        }
        for (field, value) in [
            ("source_id", &group.source_id),
            ("destination_id", &group.destination_id),
            ("final_object_key", &group.final_object_key),
            ("upload_id", &group.upload_id),
            ("complete_url", &group.complete_url),
            ("abort_url", &group.abort_url),
            ("final_head_url", &group.final_head_url),
            ("urls_expires_at", &group.urls_expires_at),
        ] {
            if value.is_empty() {
                return Err(BeamApiError::ProviderSigning(format!(
                    "multipart group {id} requires {field}"
                )));
            }
        }
        if group.expected_part_count < 1 || group.expected_part_count > MULTIPART_MAX_PART_NUMBER {
            return Err(BeamApiError::ProviderSigning(format!(
                "multipart group {id} has invalid expected_part_count"
            )));
        }
        if group.max_part_number != group.expected_part_count
            || group.max_part_number > MULTIPART_MAX_PART_NUMBER
        {
            return Err(BeamApiError::ProviderSigning(format!(
                "multipart group {id} has invalid max_part_number"
            )));
        }
        if group.expected_object_size == 0 {
            return Err(BeamApiError::ProviderSigning(format!(
                "multipart group {id} has invalid expected_object_size"
            )));
        }
        let expected_list_page_count = group.max_part_number.div_ceil(1_000) as usize;
        let unique_list_page_count = group.list_page_urls.iter().collect::<HashSet<_>>().len();
        if group.list_page_urls.len() != expected_list_page_count
            || unique_list_page_count != expected_list_page_count
            || group.list_page_urls.iter().any(String::is_empty)
        {
            return Err(BeamApiError::ProviderSigning(format!(
                "multipart group {id} requires {expected_list_page_count} unique list_page_urls"
            )));
        }
        if group.final_object_metadata.len() != 1
            || group
                .final_object_metadata
                .get("beam-transfer-id")
                .map(String::as_str)
                != Some(transfer_id)
        {
            return Err(BeamApiError::ProviderSigning(format!(
                "multipart group {id} has invalid final_object_metadata"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_multipart_part_number(
    part_number: u64,
    manifest: &MultipartGroupManifest,
) -> Result<(), BeamApiError> {
    if part_number < 1 || part_number > manifest.max_part_number {
        return Err(BeamApiError::ProviderSigning(format!(
            "multipart part_number {part_number} is outside group {} range 1-{}",
            manifest.multipart_group_id, manifest.max_part_number
        )));
    }
    Ok(())
}

/// Every route that names an upload must reference a manifest group whose identity it matches.
pub(crate) fn validate_signed_route_manifest_contract(
    routes: &[SignedChunkRoute],
    manifest: &[MultipartGroupManifest],
) -> Result<(), BeamApiError> {
    let groups = manifest
        .iter()
        .map(|group| (group.multipart_group_id.as_str(), group))
        .collect::<HashMap<_, _>>();
    for route in routes {
        let upload_id = route
            .metadata
            .get("upload_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        let Some(group_id) = route
            .metadata
            .get("multipart_group_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        else {
            if upload_id.is_some() {
                return Err(BeamApiError::ProviderSigning(format!(
                    "signed multipart route {}:{}:{} is missing multipart_group_id",
                    route.source_id, route.destination_id, route.chunk_index
                )));
            }
            continue;
        };
        let group = groups.get(group_id).ok_or_else(|| {
            BeamApiError::ProviderSigning(format!(
                "signed route references unknown multipart group {group_id}"
            ))
        })?;
        if route.source_id != group.source_id || route.destination_id != group.destination_id {
            return Err(BeamApiError::ProviderSigning(format!(
                "signed route identity does not match multipart group {group_id}"
            )));
        }
        if upload_id != Some(group.upload_id.as_str())
            || route
                .metadata
                .get("final_object_key")
                .and_then(Value::as_str)
                != Some(group.final_object_key.as_str())
        {
            return Err(BeamApiError::ProviderSigning(format!(
                "signed route upload identity does not match multipart group {group_id}"
            )));
        }
        let part_number = route
            .metadata
            .get("part_number")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                BeamApiError::ProviderSigning(format!(
                    "signed route for multipart group {group_id} is missing part_number"
                ))
            })?;
        validate_multipart_part_number(part_number, group)?;
    }
    Ok(())
}

pub(crate) fn validate_compact_transfer_plan(
    signed_url_flow: &str,
    descriptor: &CompactTransferPlanDescriptor,
) -> Result<(), BeamApiError> {
    if signed_url_flow != "signed_url" {
        return Err(BeamApiError::ProviderSigning(format!(
            "BeamCore returned unsupported signed_url_flow: {signed_url_flow}"
        )));
    }
    if descriptor.version != "compact-transfer-plan/v1" {
        return Err(BeamApiError::ProviderSigning(format!(
            "BeamCore returned unsupported plan version: {}",
            descriptor.version
        )));
    }
    if descriptor.multipart_attempt_slots != 1 {
        return Err(BeamApiError::ProviderSigning(
            "BeamCore returned unsupported multipart attempt slot count".to_string(),
        ));
    }
    let formulas = &descriptor.formulas;
    if formulas.source_offset != "source_chunk_index * chunk_size"
        || formulas.delivery_index != "chunk_index * destination_count + destination_index"
        || formulas.part_number != "source_chunk_index + 1"
        || formulas.route_generation_id != "initial-{chunk_index}-{destination_id}"
    {
        return Err(BeamApiError::ProviderSigning(
            "BeamCore returned unsupported compact transfer plan formulas".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn transfer_id_for_idempotency_key(idempotency_key: Option<&str>) -> String {
    let Some(key) = idempotency_key
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return new_transfer_id();
    };
    stable_uuid_from_identity(&format!("beam-transfer:{key}"))
}

pub(crate) fn route_generation_id_for_prepare_idempotency_key(idempotency_key: &str) -> String {
    stable_uuid_from_identity(&format!("beam-route-generation:{}", idempotency_key.trim()))
}

fn stable_uuid_from_identity(identity: &str) -> String {
    let digest = Sha256::digest(identity.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes).to_string()
}

pub(crate) fn validate_id(value: &str, name: &'static str) -> Result<(), BeamApiError> {
    if value.is_empty() || value.contains('/') || value.contains("..") {
        return Err(BeamApiError::InvalidId(name));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn multipart_route(metadata: Value) -> SignedChunkRoute {
        SignedChunkRoute {
            source_id: "src".to_string(),
            destination_id: "dst".to_string(),
            chunk_index: 0,
            delivery_index: None,
            source_url: "https://source.example/file.bin".to_string(),
            dest_url: "https://dest.example/part-1".to_string(),
            source_offset: 0,
            chunk_size: 512,
            expires_at: None,
            headers: None,
            dest_headers: None,
            metadata: serde_json::from_value(metadata).unwrap(),
        }
    }

    pub(crate) fn manifest(transfer_id: &str) -> MultipartGroupManifest {
        MultipartGroupManifest {
            multipart_group_id: "group".to_string(),
            source_id: "src".to_string(),
            destination_id: "dst".to_string(),
            final_object_key: "file.bin".to_string(),
            upload_id: "upload".to_string(),
            expected_object_size: 1024,
            expected_part_count: 2,
            max_part_number: 2,
            complete_url: "https://dest.example/complete".to_string(),
            abort_url: "https://dest.example/abort".to_string(),
            list_page_urls: vec!["https://dest.example/list".to_string()],
            final_head_url: "https://dest.example/head".to_string(),
            final_object_metadata: HashMap::from([(
                "beam-transfer-id".to_string(),
                transfer_id.to_string(),
            )]),
            urls_expires_at: "2026-07-16T00:00:00.000Z".to_string(),
        }
    }

    #[test]
    fn multipart_upload_route_requires_group_identity() {
        let route = multipart_route(
            json!({"upload_id": "upload", "final_object_key": "file.bin", "part_number": 1}),
        );
        let error = validate_signed_route_manifest_contract(&[route], &[])
            .expect_err("multipart upload route without a group must fail");
        assert!(error.to_string().contains("missing multipart_group_id"));
    }

    #[test]
    fn multipart_routes_require_part_numbers_inside_the_group() {
        let group = manifest("transfer");
        let missing = multipart_route(json!({
            "multipart_group_id": "group", "upload_id": "upload", "final_object_key": "file.bin"
        }));
        assert!(
            validate_signed_route_manifest_contract(&[missing], std::slice::from_ref(&group))
                .unwrap_err()
                .to_string()
                .contains("missing part_number")
        );
        let outside = multipart_route(json!({
            "multipart_group_id": "group", "upload_id": "upload", "final_object_key": "file.bin",
            "part_number": 3
        }));
        assert!(
            validate_signed_route_manifest_contract(&[outside], std::slice::from_ref(&group))
                .unwrap_err()
                .to_string()
                .contains("outside group group range 1-2")
        );
    }

    #[test]
    fn manifest_max_part_number_equals_the_consecutive_part_count() {
        let mut group = manifest("transfer");
        validate_multipart_group_manifest(std::slice::from_ref(&group), "transfer").unwrap();
        for invalid in [1, 3, 6] {
            group.max_part_number = invalid;
            assert!(
                validate_multipart_group_manifest(std::slice::from_ref(&group), "transfer")
                    .is_err()
            );
        }
        group.max_part_number = 10_001;
        assert!(
            validate_multipart_group_manifest(std::slice::from_ref(&group), "transfer").is_err()
        );
        let mut group = manifest("transfer");
        group
            .final_object_metadata
            .insert("extra".into(), "x".into());
        assert!(validate_multipart_group_manifest(&[group], "transfer").is_err());
        assert!(validate_multipart_group_manifest(&[manifest("other")], "transfer").is_err());
    }

    #[test]
    fn logical_route_batches_are_maximally_filled() {
        let sizes = (0..5_121)
            .collect::<Vec<_>>()
            .chunks(ROUTE_STREAM_BATCH_ROUTES)
            .map(<[usize]>::len)
            .collect::<Vec<_>>();
        assert_eq!(sizes, vec![1_024, 1_024, 1_024, 1_024, 1_024, 1]);
    }

    #[test]
    fn idempotent_prepare_reuses_route_generation() {
        let transfer_id = transfer_id_for_idempotency_key(Some("studio-step-retry"));
        assert_eq!(
            transfer_id,
            transfer_id_for_idempotency_key(Some(" studio-step-retry "))
        );
        let prepare_idempotency_key = format!("transfer:{transfer_id}:prepare");
        assert_eq!(
            route_generation_id_for_prepare_idempotency_key(&prepare_idempotency_key),
            route_generation_id_for_prepare_idempotency_key(&prepare_idempotency_key),
        );
    }
}
