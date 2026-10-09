use super::*;
use crate::nats_control::lifecycle_request_id;
use crate::nats_control::testing::{fake_control, FakeBroker, RecordedRequest, Responder};
use crate::test_http::{RecordedHttp, TestResponse, TestServer};
use crate::{CompactTransferPlanDescriptor, TransferCreateRequest};
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn client_options_debug_output_redacts_the_api_key() {
    let options = BeamClientOptions {
        api_key: "b1m_secret_key_value".into(),
        ..Default::default()
    };
    let debug = format!("{options:?}");
    assert!(!debug.contains("b1m_secret_key_value"));
    assert!(debug.contains("<redacted>"));
}

pub(crate) fn client_with(broker: Arc<FakeBroker>) -> BeamClient {
    client_with_concurrency(broker, 64, 2)
}

pub(crate) fn client_with_concurrency(
    broker: Arc<FakeBroker>,
    route_signing_concurrency: usize,
    multipart_control_concurrency: usize,
) -> BeamClient {
    BeamClient::from_parts(
        HttpClient::new(),
        fake_control(broker, DEFAULT_MAX_PAYLOAD_BYTES),
        route_signing_concurrency,
        false,
        multipart_control_concurrency,
    )
}

/// A compact plan for every prepared source and destination, like BeamCore's.
pub(crate) fn plan_descriptor(payload: &Value, chunk_size: u64, reverse_sources: bool) -> Value {
    let mut sources = payload["sources"].as_array().cloned().unwrap_or_default();
    if reverse_sources {
        sources.reverse();
    }
    let destinations = payload["destinations"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut global_chunk_start = 0;
    let mut planned_sources = Vec::new();
    for source in &sources {
        let size = source["size"].as_u64().unwrap_or(1024);
        let chunk_count = size.div_ceil(chunk_size).max(1);
        let mut planned = source.clone();
        planned["global_chunk_start"] = json!(global_chunk_start);
        planned["chunk_count"] = json!(chunk_count);
        planned_sources.push(planned);
        global_chunk_start += chunk_count;
    }
    let planned_destinations = destinations
        .iter()
        .enumerate()
        .map(|(index, destination)| {
            let mut planned = destination.clone();
            planned["destination_index"] = json!(index);
            let final_key = destination["logical_prefix"]
                .as_str()
                .filter(|value| !value.is_empty())
                .unwrap_or("out/file.bin")
                .to_string();
            planned["final_object_keys"] = Value::Object(
                sources
                    .iter()
                    .map(|source| {
                        let key = if sources.len() > 1 {
                            format!("{final_key}/{}", source["filename"].as_str().unwrap_or("f"))
                        } else {
                            final_key.clone()
                        };
                        (
                            source["source_id"].as_str().unwrap().to_string(),
                            json!(key),
                        )
                    })
                    .collect(),
            );
            planned
        })
        .collect::<Vec<_>>();
    json!({
        "version": "compact-transfer-plan/v1",
        "plan_nonce": "testplan",
        "chunk_size": chunk_size,
        "sources": planned_sources,
        "destinations": planned_destinations,
        "logical_chunk_count": global_chunk_start,
        "delivery_route_count": global_chunk_start * destinations.len() as u64,
        "multipart_attempt_slots": 1,
        "formulas": {
            "source_offset": "source_chunk_index * chunk_size",
            "delivery_index": "chunk_index * destination_count + destination_index",
            "part_number": "source_chunk_index + 1",
            "route_generation_id": "initial-{chunk_index}-{destination_id}"
        }
    })
}

pub(crate) fn fake_beamcore_with(reverse_sources: bool) -> Responder {
    Arc::new(move |message_type, payload| {
        Ok(match message_type {
            "transfer.integrity_audit_grants" => json!({"published": true}),
            "transfer.create" => json!({
                "success": true,
                "transfer_id": payload["transfer_id"],
                "total_chunks": 2,
                "total_sources": payload["sources"].as_array().map(Vec::len),
                "total_destinations": payload["destinations"].as_array().map(Vec::len),
            }),
            "transfer.plan" => {
                let chunk_size: u64 = 5_242_880;
                json!({
                    "success": true,
                    "chunk_size": chunk_size,
                    "signed_url_flow": "signed_url",
                    "plan_fingerprint": "a".repeat(64),
                    "coordinate_checksum": format!("sha256-xor-v1:1:{}", "0".repeat(64)),
                    "plan_descriptor": plan_descriptor(payload, chunk_size, reverse_sources),
                })
            }
            "transfer.prepare" => {
                // Beam chooses the chunk size, except that a provider-dictated part size is
                // planned exactly.
                let chunk_size = payload["provider_part_size"].as_u64().unwrap_or(1024);
                json!({
                    "success": true,
                    "transfer_id": payload["transfer_id"],
                    "transfer_key": "tk_test",
                    "chunk_size": chunk_size,
                    "signed_url_flow": "signed_url",
                    "plan_fingerprint": "a".repeat(64),
                    "coordinate_checksum": format!("sha256-xor-v1:1:{}", "0".repeat(64)),
                    "route_generation_id": payload["route_generation_id"],
                    "plan_descriptor": plan_descriptor(payload, chunk_size, reverse_sources),
                })
            }
            "transfer.route_stream.complete" => json!({
                "success": true, "transfer_id": payload["transfer_id"], "total_routes_received": 2
            }),
            "transfer.distribute" => json!({
                "success": true, "transfer_id": payload["transfer_id"], "orchestrators_assigned": 1
            }),
            "transfer.status" => json!({
                "transfer_id": payload["transfer_id"],
                "status": "completed",
                "error_message": null,
                "source_bytes_total": 1024,
                "started_at": "2026-06-22T22:40:20.000Z",
                "completed_at": "2026-06-22T22:41:20.000Z",
            }),
            "transfer.cancel" => json!({"success": true, "message": "cancelled"}),
            _ => json!({"success": true}),
        })
    })
}

pub(crate) fn fake_beamcore() -> Responder {
    fake_beamcore_with(false)
}

fn manual_recovery(routes: Vec<SignedChunkRoute>) -> ManualRouteRecovery {
    ManualRouteRecovery {
        plan_fingerprint: "a".repeat(64),
        coordinate_checksum: format!("sha256-xor-v1:1:{}", "0".repeat(64)),
        regenerate: Arc::new(move |_| {
            let routes = routes.clone();
            Box::pin(async move { Ok((routes, Vec::new(), None)) })
        }),
    }
}

fn route(chunk_index: u64, metadata: Value) -> SignedChunkRoute {
    SignedChunkRoute {
        source_id: "src_0".to_string(),
        destination_id: "dst_0".to_string(),
        chunk_index,
        delivery_index: None,
        source_url: format!("https://source.example/file.bin?chunk={chunk_index}"),
        dest_url: format!("https://dest.example/file.bin.part{chunk_index}"),
        source_offset: chunk_index * 512,
        chunk_size: 512,
        expires_at: None,
        headers: None,
        dest_headers: None,
        metadata: serde_json::from_value(metadata).unwrap(),
    }
}

fn group_manifest(transfer_id: &str, group: &str) -> MultipartGroupManifest {
    MultipartGroupManifest {
        multipart_group_id: group.to_string(),
        source_id: "src_0".to_string(),
        destination_id: "dst_0".to_string(),
        final_object_key: "file.bin".to_string(),
        upload_id: "upload-0".to_string(),
        expected_object_size: 1024,
        expected_part_count: 2,
        max_part_number: 2,
        complete_url: "https://dest.example/complete".to_string(),
        abort_url: "https://dest.example/abort".to_string(),
        list_page_urls: vec![format!("https://dest.example/list/{group}")],
        final_head_url: "https://dest.example/final-head".to_string(),
        final_object_metadata: HashMap::from([(
            "beam-transfer-id".to_string(),
            transfer_id.to_string(),
        )]),
        urls_expires_at: "2026-07-16T00:00:00.000Z".to_string(),
    }
}

fn prepared_source(size: u64) -> PreparedHttpSource {
    PreparedHttpSource {
        source_id: "src_0".to_string(),
        source_type: "http".to_string(),
        provider: None,
        url: "https://source.example/file.bin".to_string(),
        size,
        filename: None,
        headers: None,
        expires_at: None,
        metadata: HashMap::new(),
    }
}

fn http_destination() -> PreparedDestination {
    PreparedDestination {
        destination_id: "dst_0".to_string(),
        provider: "http".to_string(),
        mode: Some("http_chunks".to_string()),
        logical_prefix: Some("out/file.bin".to_string()),
        metadata: HashMap::new(),
    }
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..400 {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for condition");
}

fn request_ids(requests: &[RecordedRequest]) -> Vec<String> {
    requests
        .iter()
        .map(|request| request.envelope["request_id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn lifecycle_operations_flow_through_nats_control_messages() {
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let created = client
        .create_raw_transfer(TransferCreateRequest {
            transfer_id: None,
            idempotency_key: Some("caller-key".to_string()),
            sources: vec![HashMap::from([("type".to_string(), json!("http"))])],
            destinations: vec![HashMap::from([("type".to_string(), json!("http"))])],
            total_size: 10_485_760,
            name: None,
            merkle_root: None,
            chunk_hashes: None,
            callbacks: None,
            progressive_mode: false,
            signed_url_flow: None,
        })
        .await
        .unwrap();
    assert!(created.success);
    assert_eq!(
        created.transfer_id,
        transfer_id_for_idempotency_key(Some("caller-key"))
    );
    let distributed = client
        .distribute_transfer(&created.transfer_id)
        .await
        .unwrap();
    assert_eq!(distributed.orchestrators_assigned, 1);
    let status = client
        .wait_for_transfer_with_options(
            &created.transfer_id,
            WaitForTransferOptions {
                timeout: Some(Duration::from_millis(100)),
                poll_interval: Some(Duration::from_millis(1)),
                max_poll_interval: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(status.status, "completed");
    assert!(
        client
            .cancel_transfer(&created.transfer_id)
            .await
            .unwrap()
            .success
    );

    assert_eq!(
        broker.message_types(),
        vec![
            "transfer.create",
            "transfer.distribute",
            "transfer.status",
            "transfer.cancel"
        ]
    );
    let create = &broker.requests()[0];
    assert!(create.payload().get("chunk_size").is_none());
    assert_eq!(create.payload()["signed_url_flow"], "signed_url");
    // The caller key only derives the transfer id; the lifecycle key is transfer-scoped.
    assert_eq!(
        create.envelope["request_id"],
        json!(lifecycle_request_id(
            "transfer.create",
            Some(&format!("transfer:{}:create", created.transfer_id))
        ))
    );
    client.close().await.unwrap();
}

#[tokio::test]
async fn idempotent_prepare_reuses_transfer_id_and_route_generation() {
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let input = TransferPrepareInput {
        sources: vec![prepared_source(4096)],
        destinations: vec![http_destination()],
        idempotency_key: Some("studio-step-retry".to_string()),
        ..Default::default()
    };
    let first = client
        .prepare_transfer_with_options(input.clone())
        .await
        .unwrap();
    let second = client.prepare_transfer_with_options(input).await.unwrap();
    assert_eq!(first.transfer_id, second.transfer_id);
    let prepares = broker.of_type("transfer.prepare");
    assert_eq!(prepares.len(), 2);
    for prepare in &prepares {
        assert!(prepare.payload().get("chunk_size").is_none());
        assert!(prepare.payload().get("provider_part_size").is_none());
    }
    assert_eq!(
        prepares[0].payload()["route_generation_id"],
        prepares[1].payload()["route_generation_id"]
    );
    let ids = request_ids(&prepares);
    assert_eq!(ids[0], ids[1]);
    let prepare_key = format!("transfer:{}:prepare", first.transfer_id);
    assert_eq!(
        ids[0],
        lifecycle_request_id("transfer.prepare", Some(&prepare_key))
    );
    assert_eq!(
        prepares[0].payload()["route_generation_id"],
        json!(route_generation_id_for_prepare_idempotency_key(
            &prepare_key
        ))
    );
}

#[tokio::test]
async fn attach_streams_each_manifest_group_separately_and_honors_auto_distribute() {
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let prepared = client
        .prepare_transfer_with_options(TransferPrepareInput {
            sources: vec![prepared_source(1024)],
            destinations: vec![http_destination()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(prepared.transfer_key.as_deref(), Some("tk_test"));
    let transfer_id = prepared.transfer_id.clone();
    let routes = vec![
        route(
            0,
            json!({"multipart_group_id": "group-0", "upload_id": "upload-0",
                   "final_object_key": "file.bin", "bucket": "dest-bucket",
                   "part_number": 1, "delivery_index": 0}),
        ),
        route(
            1,
            json!({"multipart_group_id": "group-1", "upload_id": "upload-0",
                   "final_object_key": "file.bin", "bucket": "dest-bucket",
                   "part_number": 2, "delivery_index": 1}),
        ),
    ];
    let attached = client
        .attach_signed_urls_with_options(
            &transfer_id,
            AttachSignedUrlsInput {
                chunk_routes: routes.clone(),
                multipart_group_manifest: vec![
                    group_manifest(&transfer_id, "group-0"),
                    group_manifest(&transfer_id, "group-1"),
                ],
                transfer_key: prepared.transfer_key.clone(),
                urls_expires_at: None,
                route_generation_id: prepared.route_generation_id.clone(),
                recovery: manual_recovery(routes),
                auto_distribute: Some(false),
            },
        )
        .await
        .unwrap();
    assert!(attached.success);
    let types = broker.message_types();
    assert_eq!(
        types,
        vec![
            "transfer.prepare",
            "transfer.route_stream.begin",
            "transfer.route_stream.manifest",
            "transfer.route_stream.manifest",
            "transfer.route_stream.batch",
            "transfer.route_stream.complete"
        ]
    );
    let begin = &broker.of_type("transfer.route_stream.begin")[0];
    assert_eq!(begin.payload()["auto_distribute"], false);
    assert_eq!(begin.payload()["total_routes"], 2);
    assert_eq!(begin.payload()["route_contract_version"], "signed_url");
    let manifests = broker.of_type("transfer.route_stream.manifest");
    let mut groups = manifests
        .iter()
        .map(|call| {
            assert_eq!(call.payload()["groups"].as_array().unwrap().len(), 1);
            call.payload()["groups"][0]["multipart_group_id"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    groups.sort();
    assert_eq!(groups, vec!["group-0", "group-1"]);
    for call in broker
        .requests()
        .iter()
        .filter(|call| call.message_type.starts_with("transfer.route_stream"))
    {
        assert_eq!(
            call.payload()["route_generation_id"],
            json!(prepared.route_generation_id)
        );
    }
    let batch = &broker.of_type("transfer.route_stream.batch")[0];
    assert_eq!(batch.payload()["batch_index"], 0);
    assert_eq!(batch.payload()["route_count"], 2);
    assert!(batch.payload()["route_batch"]
        .get("multipart_groups")
        .is_none());
    let compact_routes = batch.payload()["route_batch"]["routes"].as_array().unwrap();
    assert_eq!(compact_routes[0]["multipart_group_id"], "group-0");
    assert_eq!(compact_routes[0]["metadata"], json!({"part_number": 1}));
    assert_eq!(compact_routes[1]["metadata"], json!({"part_number": 2}));
    let complete = &broker.of_type("transfer.route_stream.complete")[0];
    assert!(complete.payload()["route_keys_checksum"]
        .as_str()
        .unwrap()
        .starts_with("sha256-xor-v1:2:"));
    assert!(client.control().has_recovery_lease(&transfer_id).await);
    client.close().await.unwrap();
}

#[tokio::test]
async fn attach_emits_1024_route_batches_and_flushes_the_final_remainder() {
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let routes = (0..5_121)
        .map(|index| route(index, json!({})))
        .collect::<Vec<_>>();
    client
        .attach_signed_urls(
            "11111111-1111-4111-8111-111111111111",
            routes.clone(),
            Vec::new(),
            Some("tk_test".to_string()),
            None,
            "22222222-2222-4222-8222-222222222222".to_string(),
            manual_recovery(routes),
        )
        .await
        .unwrap();
    let batches = broker.of_type("transfer.route_stream.batch");
    assert_eq!(
        batches
            .iter()
            .map(|call| call.payload()["batch_index"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4, 5]
    );
    assert_eq!(
        batches
            .iter()
            .map(|call| call.payload()["route_count"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![1_024, 1_024, 1_024, 1_024, 1_024, 1]
    );
    let complete = broker.requests().last().cloned().unwrap();
    assert_eq!(complete.message_type, "transfer.route_stream.complete");
    assert_eq!(complete.payload()["expected_batches"], 6);
    let begin = &broker.of_type("transfer.route_stream.begin")[0];
    assert_eq!(begin.payload()["auto_distribute"], true);
}

#[tokio::test]
async fn attach_rejects_multipart_routes_without_their_manifest_group_identity() {
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let routes = vec![route(
        0,
        json!({"upload_id": "upload-0", "final_object_key": "file.bin", "part_number": 1}),
    )];
    let error = client
        .attach_signed_urls(
            "11111111-1111-4111-8111-111111111111",
            routes.clone(),
            Vec::new(),
            None,
            None,
            "22222222-2222-4222-8222-222222222222".to_string(),
            manual_recovery(routes),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("missing multipart_group_id"));
    assert!(broker.requests().is_empty());
}

async fn attach_with_failure_at(boundary: &str) {
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let transfer_id = "11111111-1111-4111-8111-111111111111";
    let routes = vec![route(0, json!({"delivery_index": 0}))];
    broker.fail_next(boundary);
    let error = client
        .attach_signed_urls(
            transfer_id,
            routes.clone(),
            Vec::new(),
            None,
            None,
            "22222222-2222-4222-8222-222222222222".to_string(),
            manual_recovery(routes),
        )
        .await
        .unwrap_err();
    match error {
        BeamApiError::RouteRecoveryPending {
            transfer_id: pending,
            source,
        } => {
            assert_eq!(pending, transfer_id);
            assert!(source.to_string().contains("connection closed"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(client.control().has_recovery_lease(transfer_id).await);
    // Background recovery asks Runtime whether the routes need replaying.
    wait_until(|| !broker.of_type("transfer.resume").is_empty()).await;
    client.close().await.unwrap();
}

#[tokio::test]
async fn attach_retains_recovery_when_transport_stops_at_route_stream_begin() {
    attach_with_failure_at("transfer.route_stream.begin").await;
}

#[tokio::test]
async fn attach_retains_recovery_when_transport_stops_at_route_stream_batch() {
    attach_with_failure_at("transfer.route_stream.batch").await;
}

#[tokio::test]
async fn cancel_releases_recovery_and_integrity_state_even_when_rejected() {
    let broker = FakeBroker::new(fake_beamcore());
    broker.set_responder(Arc::new(|message_type, _| {
        Ok(match message_type {
            "transfer.cancel" => json!({"success": false, "message": "already terminal"}),
            _ => json!({}),
        })
    }));
    let client = client_with(broker.clone());
    let transfer_id = "44444444-4444-4444-8444-444444444444";
    let disposed = Arc::new(AtomicUsize::new(0));
    let disposed_for_lease = disposed.clone();
    client
        .control()
        .register_recovery_lease(RecoveryLease {
            transfer_id: transfer_id.to_string(),
            plan_fingerprint: "a".repeat(64),
            coordinate_checksum: "checksum".to_string(),
            replay_routes: Arc::new(|_| Box::pin(async { Ok(()) })),
            dispose: Some(Arc::new(move || {
                disposed_for_lease.fetch_add(1, Ordering::SeqCst);
            })),
        })
        .await;
    let cancelled = client.cancel_transfer(transfer_id).await.unwrap();
    assert!(!cancelled.success);
    assert!(!client.control().has_recovery_lease(transfer_id).await);
    assert_eq!(disposed.load(Ordering::SeqCst), 1);
    client.close().await.unwrap();
}

#[tokio::test]
async fn integrity_grant_failures_remain_visible_without_a_signer() {
    let transfer_id = "c61fdfd2-3c09-44c7-bbb4-048a9d0e9c7c";
    let broker = FakeBroker::new(Arc::new(move |_, _| {
        Ok(json!({
            "transfer_id": transfer_id,
            "status": "in_progress",
            "source_bytes_total": 1,
            "integrity_audit_challenge": {"transfer_id": transfer_id, "audit_id": "audit-1", "chunks": []}
        }))
    }));
    let client = client_with(broker);
    let status = client.transfer_status(transfer_id).await.unwrap();
    assert_eq!(
        status.integrity_audit_submission_error.as_deref(),
        Some("integrity audit signer unavailable")
    );
    client.close().await.unwrap();
}

#[tokio::test]
async fn wait_for_transfer_validates_durations() {
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    for options in [
        WaitForTransferOptions {
            timeout: Some(Duration::ZERO),
            ..Default::default()
        },
        WaitForTransferOptions {
            poll_interval: Some(Duration::ZERO),
            ..Default::default()
        },
        WaitForTransferOptions {
            max_poll_interval: Some(Duration::ZERO),
            ..Default::default()
        },
    ] {
        assert!(matches!(
            client
                .wait_for_transfer_with_options("transfer-1", options)
                .await,
            Err(BeamApiError::InvalidArgument(_))
        ));
    }
    assert!(broker.requests().is_empty());
}

#[tokio::test]
async fn wait_for_transfer_backs_off_to_max_poll_interval() {
    let polls = Arc::new(AtomicUsize::new(0));
    let polls_for_broker = polls.clone();
    let broker = FakeBroker::new(Arc::new(move |_, payload| {
        let count = polls_for_broker.fetch_add(1, Ordering::SeqCst);
        Ok(json!({
            "transfer_id": payload["transfer_id"],
            "status": if count >= 3 { "completed" } else { "in_progress" },
            "source_bytes_total": 1,
        }))
    }));
    let client = client_with(broker);
    let started = Instant::now();
    client
        .wait_for_transfer_with_options(
            "transfer-1",
            WaitForTransferOptions {
                timeout: Some(Duration::from_secs(5)),
                poll_interval: Some(Duration::from_millis(20)),
                max_poll_interval: Some(Duration::from_millis(25)),
            },
        )
        .await
        .unwrap();
    // 20ms, then capped at 25ms twice (with +-20% jitter): well under the uncapped 20/30/45ms.
    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(polls.load(Ordering::SeqCst), 4);
}

fn hippius_server() -> crate::test_http::Handler {
    Arc::new(|request: &RecordedHttp| {
        assert_eq!(request.header("authorization"), Some("Token hippius-token"));
        if request.path().ends_with("/objects/") {
            return TestResponse::status(200).json(json!({"Contents": [{"Size": 1024}]}));
        }
        let key = request
            .target
            .split("key=")
            .nth(1)
            .and_then(|rest| rest.split('&').next())
            .unwrap_or("")
            .to_string();
        let action = if request.target.contains("action=put") {
            "put"
        } else {
            "get"
        };
        TestResponse::status(200)
            .json(json!({"url": format!("https://hippius.example/{action}/{key}?sig=1")}))
    })
}

fn hippius_source(id: &str, key: &str, base_url: &str) -> HippiusProviderSource {
    HippiusProviderSource {
        storage_location: None,
        source_id: Some(id.to_string()),
        bucket: "bucket".to_string(),
        key: key.to_string(),
        api_token: "hippius-token".to_string(),
        base_url: Some(base_url.to_string()),
    }
}

fn hippius_destination(base_url: &str) -> HippiusProviderDestination {
    HippiusProviderDestination {
        storage_location: None,
        destination_id: None,
        bucket: "out-bucket".to_string(),
        key: "out/".to_string(),
        api_token: "hippius-token".to_string(),
        base_url: Some(base_url.to_string()),
    }
}

#[tokio::test]
async fn hippius_wire_output_matches_typescript_and_integrity_uses_source_ids() {
    let server = TestServer::start(hippius_server()).await;
    // BeamCore returns the plan's sources in the opposite order from the input.
    let broker = FakeBroker::new(fake_beamcore_with(true));
    let client = client_with(broker.clone());
    let prepared = client
        .prepare_hippius_provider_transfer(
            vec![
                hippius_source("src_a", "in/a.bin", &server.url),
                hippius_source("src_b", "in/b.bin", &server.url),
            ],
            vec![hippius_destination(&server.url)],
            None,
            Duration::from_secs(600),
            true,
            None,
            None,
        )
        .await
        .unwrap();
    let prepare = &broker.of_type("transfer.prepare")[0];
    let source = &prepare.payload()["sources"][0];
    assert_eq!(source["provider"], "hippius");
    assert!(source["expires_at"].is_string());
    let destination = &prepare.payload()["destinations"][0];
    assert!(destination.get("mode").is_none());
    assert_eq!(destination["logical_prefix"], "out");

    let batch = &broker.of_type("transfer.route_stream.batch")[0];
    let route_batch = &batch.payload()["route_batch"];
    let source_chunk = &route_batch["source_chunks"][0];
    assert!(source_chunk["headers"]["Range"]
        .as_str()
        .unwrap()
        .starts_with("bytes=0-"));
    assert!(source_chunk["expires_at"].is_string());
    let compact_route = &route_batch["routes"][0];
    assert!(compact_route["expires_at"].is_string());
    let metadata = compact_route["metadata"].as_object().unwrap();
    let mut keys = metadata.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "attempt_slot",
            "logical_attempt_index",
            "part_number",
            "route_generation_id"
        ]
    );

    // Answer an integrity challenge for the source planned second but configured first.
    let transfer_id = prepared.transfer_id.clone();
    broker.set_responder(Arc::new(move |message_type, _| {
        Ok(match message_type {
            "transfer.integrity_audit_grants" => json!({"published": true}),
            "transfer.status" => json!({
                "transfer_id": transfer_id,
                "status": "in_progress",
                "source_bytes_total": 2048,
                "integrity_audit_challenge": {
                    "audit_id": "audit-1",
                    "transfer_id": transfer_id,
                    "chunks": [{
                        "challenge_id": "c1", "task_id": "t1", "attempt_id": null,
                        "orchestrator_id": "o1", "orchestrator_hotkey": "hk", "worker_id": "w1",
                        "source_id": "src_a", "destination_id": "dst_0",
                        "route_chunk_index": 1, "delivery_index": 1,
                        "source_offset": 0, "destination_offset": 0, "range_length": 16,
                        "final_object_key": "out/a.bin", "final_object_etag": null
                    }]
                }
            }),
            _ => json!({"success": true}),
        })
    }));
    let status = client.transfer_status(&prepared.transfer_id).await.unwrap();
    assert!(
        status.integrity_audit_submission_error.is_none(),
        "{:?}",
        status.integrity_audit_submission_error
    );
    // A repeated challenge for the same audit is not submitted twice.
    client.transfer_status(&prepared.transfer_id).await.unwrap();
    let grants = broker.of_type("transfer.integrity_audit_grants");
    assert_eq!(grants.len(), 1);
    let chunk = &grants[0].payload()["chunks"][0];
    let source_url = chunk["source"]["url"].as_str().unwrap();
    assert!(source_url.contains("a.bin") && !source_url.contains("b.bin"));
    assert_eq!(chunk["source"]["headers"]["Range"], "bytes=0-15");
    assert!(chunk["destination"]["url"]
        .as_str()
        .unwrap()
        .contains("a.bin"));
    assert!(chunk.get("orchestrator_id").is_none());
    assert!(chunk.get("worker_id").is_none());
    assert_eq!(chunk["challenge_id"], "c1");
    client.close().await.unwrap();
}

/// A fake Hub serving a 1024-byte LFS file. With `multipart`, the LFS batch dictates 512-byte
/// parts and presigns two part URLs; otherwise it issues a single-part upload.
fn huggingface_hub(
    state: Arc<StdMutex<Vec<String>>>,
    multipart: bool,
) -> crate::test_http::Handler {
    Arc::new(move |request: &RecordedHttp| {
        state
            .lock()
            .unwrap()
            .push(format!("{} {}", request.method, request.path()));
        let port = request
            .header("host")
            .and_then(|host| host.rsplit(':').next())
            .unwrap_or("0")
            .to_string();
        let path = request.path();
        if request.method == "HEAD" && path.contains("/resolve/") {
            return TestResponse::status(302)
                .header("Location", format!("http://localhost:{port}/cdn/model.bin"))
                .header("X-Linked-Size", "1024")
                .header("X-Linked-Etag", format!("\"{}\"", "ab".repeat(32)))
                .header("X-Repo-Commit", "commit-1");
        }
        if request.method == "GET" && path == "/cdn/model.bin" {
            return TestResponse::status(206).body(vec![7_u8; 512]);
        }
        if path.ends_with("/preupload/main") {
            return TestResponse::status(200)
                .json(json!({"files": [{"path": "out/model.bin", "uploadMode": "lfs", "shouldIgnore": false}]}));
        }
        if path.ends_with(".git/info/lfs/objects/batch") {
            let mut upload = json!({"href": format!("http://127.0.0.1:{port}/lfs-upload")});
            if multipart {
                upload["header"] = json!({
                    "chunk_size": "512",
                    "00001": format!("http://127.0.0.1:{port}/lfs-part/1"),
                    "00002": format!("http://127.0.0.1:{port}/lfs-part/2"),
                });
            }
            return TestResponse::status(200).json(json!({"objects": [{
                "oid": "ab".repeat(32), "size": 1024,
                "actions": {"upload": upload}
            }]}));
        }
        if path.ends_with("/commit/main") {
            return TestResponse::status(200).json(json!({"commitOid": "c2"}));
        }
        TestResponse::status(404)
    })
}

fn huggingface_source(endpoint: &str) -> HuggingFaceProviderSource {
    HuggingFaceProviderSource {
        storage_location: None,
        source_id: None,
        repo_id: "org/source".to_string(),
        path: "model.bin".to_string(),
        repo_type: None,
        revision: None,
        token: "hf-token".to_string(),
        endpoint: Some(endpoint.to_string()),
    }
}

fn huggingface_destination(endpoint: &str) -> HuggingFaceProviderDestination {
    HuggingFaceProviderDestination {
        storage_location: None,
        destination_id: None,
        repo_id: "org/destination".to_string(),
        path: "out/model.bin".to_string(),
        repo_type: None,
        revision: None,
        token: "hf-token".to_string(),
        endpoint: Some(endpoint.to_string()),
        commit_message: None,
        commit_description: None,
        create_pr: false,
        allow_source_rehash: false,
    }
}

#[tokio::test]
async fn huggingface_transfer_keeps_a_recovery_lease_and_wait_commits_on_completion() {
    let calls = Arc::new(StdMutex::new(Vec::new()));
    let hub = TestServer::start(huggingface_hub(calls.clone(), false)).await;
    let broker = FakeBroker::new(fake_beamcore());
    broker.fail_next("transfer.route_stream.complete");
    let client = client_with(broker.clone());
    let error = client
        .prepare_huggingface_provider_transfer(
            vec![huggingface_source(&hub.url)],
            vec![huggingface_destination(&hub.url)],
            None,
            Duration::from_secs(600),
            true,
            None,
            None,
        )
        .await
        .unwrap_err();
    let BeamApiError::RouteRecoveryPending { transfer_id, .. } = error else {
        panic!("unexpected error: {error:?}");
    };
    assert!(client.control().has_recovery_lease(&transfer_id).await);
    // A single-part Hub upload sends no size: Beam chooses it and the plan must have one chunk.
    let prepare = &broker.of_type("transfer.prepare")[0];
    assert!(prepare.payload().get("chunk_size").is_none());
    assert!(prepare.payload().get("provider_part_size").is_none());
    let batch = &broker.of_type("transfer.route_stream.batch")[0];
    let compact_route = &batch.payload()["route_batch"]["routes"][0];
    assert!(compact_route["metadata"].get("part_number").is_none());
    assert!(compact_route["metadata"].get("transfer_id").is_none());
    assert!(compact_route["dest_url"]
        .as_str()
        .unwrap()
        .ends_with("/lfs-upload"));

    // Completion publishes the Hub commit before returning.
    client
        .wait_for_transfer_with_options(
            &transfer_id,
            WaitForTransferOptions {
                timeout: Some(Duration::from_secs(2)),
                poll_interval: Some(Duration::from_millis(5)),
                max_poll_interval: None,
            },
        )
        .await
        .unwrap();
    assert!(calls
        .lock()
        .unwrap()
        .iter()
        .any(|call| call == "POST /api/models/org/destination/commit/main"));
    assert!(!client.control().has_recovery_lease(&transfer_id).await);
    client.close().await.unwrap();
}

#[tokio::test]
async fn huggingface_part_size_is_sent_as_provider_part_size_and_the_plan_must_match_it() {
    let calls = Arc::new(StdMutex::new(Vec::new()));
    let hub = TestServer::start(huggingface_hub(calls, true)).await;
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let prepared = client
        .prepare_huggingface_provider_transfer(
            vec![huggingface_source(&hub.url)],
            vec![huggingface_destination(&hub.url)],
            None,
            Duration::from_secs(600),
            false,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(prepared.success);
    assert_eq!(prepared.plan_descriptor.chunk_size, 512);
    let prepare = &broker.of_type("transfer.prepare")[0];
    assert_eq!(prepare.payload()["provider_part_size"], 512);
    assert!(prepare.payload().get("chunk_size").is_none());
    // Chunk N is uploaded to the Hub's presigned URL for part N + 1.
    let mut dest_urls = broker
        .of_type("transfer.route_stream.batch")
        .iter()
        .flat_map(|call| {
            call.payload()["route_batch"]["routes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|route| route["dest_url"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    dest_urls.sort();
    assert_eq!(dest_urls.len(), 2, "{dest_urls:?}");
    assert!(dest_urls[0].ends_with("/lfs-part/1"), "{dest_urls:?}");
    assert!(dest_urls[1].ends_with("/lfs-part/2"), "{dest_urls:?}");

    // A plan that does not use the Hub's part size is rejected.
    let base = fake_beamcore();
    broker.set_responder(Arc::new(move |message_type: &str, payload: &Value| {
        let mut payload = payload.clone();
        if let Some(object) = payload.as_object_mut() {
            object.remove("provider_part_size");
        }
        base(message_type, &payload)
    }));
    let error = client
        .prepare_huggingface_provider_transfer(
            vec![huggingface_source(&hub.url)],
            vec![huggingface_destination(&hub.url)],
            None,
            Duration::from_secs(600),
            false,
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("the Hub requires 512-byte parts"),
        "{error}"
    );
    client.close().await.unwrap();
}

#[test]
fn plan_descriptor_fixture_is_a_valid_compact_plan() {
    let payload = json!({
        "sources": [{"source_id": "src_0", "type": "http", "url": "u", "size": 4096}],
        "destinations": [{"destination_id": "dst_0", "provider": "http"}]
    });
    let descriptor: CompactTransferPlanDescriptor =
        serde_json::from_value(plan_descriptor(&payload, 1024, false)).unwrap();
    validate_compact_transfer_plan("signed_url", &descriptor).unwrap();
    assert_eq!(descriptor.logical_chunk_count, 4);
}

#[tokio::test]
async fn plan_transfer_requests_a_validated_compact_plan() {
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let planned = client
        .plan_transfer(TransferPlanInput {
            sources: vec![crate::PlanningHttpSource::from(prepared_source(1024))],
            destinations: vec![http_destination()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(planned.success);
    assert_eq!(planned.plan_descriptor.version, "compact-transfer-plan/v1");
    let plan = &broker.of_type("transfer.plan")[0];
    assert!(plan.payload().get("transfer_id").is_none());
    assert!(plan.payload().get("chunk_size").is_none());
    assert!(plan.payload().get("provider_part_size").is_none());
    assert_eq!(plan.payload()["signed_url_flow"], "signed_url");

    broker.set_responder(Arc::new(|_, payload| {
        let mut descriptor = plan_descriptor(payload, 1024, false);
        descriptor["formulas"]["part_number"] = json!("source_chunk_index * 3 + attempt_slot + 1");
        Ok(json!({"success": true, "signed_url_flow": "signed_url", "plan_descriptor": descriptor}))
    }));
    let error = client
        .plan_transfer(TransferPlanInput {
            sources: vec![crate::PlanningHttpSource::from(prepared_source(1024))],
            destinations: vec![http_destination()],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("unsupported compact transfer plan formulas"));

    // v6 reserved three attempt slots per chunk; v7 plans have exactly one.
    broker.set_responder(Arc::new(|_, payload| {
        let mut descriptor = plan_descriptor(payload, 1024, false);
        descriptor["multipart_attempt_slots"] = json!(3);
        Ok(json!({"success": true, "signed_url_flow": "signed_url", "plan_descriptor": descriptor}))
    }));
    let error = client
        .plan_transfer(TransferPlanInput {
            sources: vec![crate::PlanningHttpSource::from(prepared_source(1024))],
            destinations: vec![http_destination()],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("unsupported multipart attempt slot count"));
}

#[tokio::test]
async fn failed_prepare_without_a_plan_is_returned_not_raised() {
    let broker = FakeBroker::new(Arc::new(|_, _| {
        Ok(json!({"success": false, "error": "quota_exceeded", "message": "quota exceeded"}))
    }));
    let client = client_with(broker);
    let prepared = client
        .prepare_transfer_with_options(TransferPrepareInput {
            sources: vec![prepared_source(1024)],
            destinations: vec![http_destination()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!prepared.success);
    assert_eq!(prepared.error.as_deref(), Some("quota_exceeded"));
}

#[test]
fn client_options_are_validated_like_typescript() {
    for options in [
        BeamClientOptions {
            api_key: "b1m_test".into(),
            transfer_runtime_shard_count: Some(0),
            ..Default::default()
        },
        BeamClientOptions {
            api_key: "b1m_test".into(),
            route_signing_concurrency: Some(0),
            ..Default::default()
        },
        BeamClientOptions {
            api_key: "b1m_test".into(),
            multipart_control_concurrency: Some(0),
            ..Default::default()
        },
        BeamClientOptions {
            api_key: "b1m_test".into(),
            max_payload_bytes: Some(0),
            ..Default::default()
        },
    ] {
        assert!(matches!(
            BeamClient::new(options),
            Err(BeamApiError::InvalidArgument(_))
        ));
    }
    assert!(matches!(
        BeamClient::new(BeamClientOptions::default()),
        Err(BeamApiError::MissingApiKey)
    ));
    assert!(BeamClient::new(BeamClientOptions {
        api_key: "b1m_test".into(),
        nats_url: Some("nats://127.0.0.1:4222".into()),
        ..Default::default()
    })
    .is_ok());
}

#[tokio::test]
async fn s3_integrity_grants_bind_reads_to_the_planned_etags() {
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let transfer_id = "55555555-5555-4555-8555-555555555555".to_string();
    let mut source = prepared_source(1024);
    source.metadata = HashMap::from([
        ("etag".to_string(), json!("\"source-etag\"")),
        ("version_id".to_string(), json!("v7")),
    ]);
    let payload = json!({
        "sources": [source],
        "destinations": [{"destination_id": "dst_0", "provider": "r2", "logical_prefix": "out/file.bin"}]
    });
    let prepared: TransferPrepareResponse = serde_json::from_value(json!({
        "success": true, "transfer_id": transfer_id, "signed_url_flow": "signed_url",
        "plan_fingerprint": "a".repeat(64), "coordinate_checksum": "c", "route_generation_id": "g",
        "plan_descriptor": plan_descriptor(&payload, 1024, false),
    }))
    .unwrap();
    let credentials = |endpoint: &str| (endpoint.to_string(), "ak".to_string(), "sk".to_string());
    let (endpoint, access_key_id, secret_access_key) = credentials("https://account.r2.example");
    client.remember_integrity_context(IntegrityContext {
        prepared,
        sources: HashMap::from([(
            "src_0".to_string(),
            ProviderSourceConfig::R2(crate::R2ProviderSource {
                bucket: "in".into(),
                key: "file.bin".into(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                endpoint_url: Some(endpoint.clone()),
                ..Default::default()
            }),
        )]),
        destinations: HashMap::from([(
            "dst_0".to_string(),
            ProviderDestinationConfig::R2(crate::R2ProviderDestination {
                bucket: "out".into(),
                key: "out/file.bin".into(),
                access_key_id,
                secret_access_key,
                endpoint_url: Some(endpoint),
                ..Default::default()
            }),
        )]),
        expires_in: Duration::from_secs(600),
    });
    let status_transfer_id = transfer_id.clone();
    broker.set_responder(Arc::new(move |message_type, _| {
        Ok(match message_type {
            "transfer.integrity_audit_grants" => json!({"published": true}),
            "transfer.status" => json!({
                "transfer_id": status_transfer_id, "status": "in_progress", "source_bytes_total": 1024,
                "integrity_audit_challenge": {"audit_id": "audit-s3", "transfer_id": status_transfer_id, "chunks": [{
                    "challenge_id": "c1", "task_id": "t1", "attempt_id": "a1",
                    "source_id": "src_0", "destination_id": "dst_0", "route_chunk_index": 0,
                    "delivery_index": 0, "source_offset": 100, "destination_offset": 100,
                    "range_length": 50, "final_object_key": "out/file.bin",
                    "final_object_etag": "\"final-etag\""
                }]}
            }),
            _ => json!({"success": true}),
        })
    }));
    let status = client.transfer_status(&transfer_id).await.unwrap();
    assert!(status.integrity_audit_submission_error.is_none());
    let grants = broker.of_type("transfer.integrity_audit_grants");
    let grant = &grants[0].payload()["chunks"][0];
    assert_eq!(grant["source"]["headers"]["Range"], "bytes=100-149");
    assert_eq!(grant["source"]["headers"]["If-Match"], "\"source-etag\"");
    let source_url = grant["source"]["url"].as_str().unwrap();
    assert!(source_url.contains("versionId=v7"));
    assert!(source_url.contains("if-match"));
    assert_eq!(
        grant["destination"]["headers"]["If-Match"],
        "\"final-etag\""
    );
    assert!(grant["destination"]["url"]
        .as_str()
        .unwrap()
        .starts_with("https://account.r2.example/out/out/file.bin?"));
    assert_eq!(grant["attempt_id"], "a1");
}
