use super::*;
use crate::client::tests::{client_with, client_with_concurrency, fake_beamcore};
use crate::nats_control::testing::FakeBroker;
use crate::nats_control::TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION;
use crate::test_http::{RecordedHttp, TestResponse, TestServer};
use crate::{HippiusProviderDestination, R2ProviderDestination, R2ProviderSource};
use bytes::Bytes;
use rmp_serde::{from_slice, to_vec_named};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

fn r2_source(endpoint: &str, access_key_id: &str) -> ProviderSourceConfig {
    ProviderSourceConfig::R2(R2ProviderSource {
        bucket: "source".into(),
        key: "input.bin".into(),
        access_key_id: access_key_id.into(),
        secret_access_key: "sk".into(),
        endpoint_url: Some(endpoint.to_string()),
        ..Default::default()
    })
}

fn r2_destination(endpoint: &str, access_key_id: &str, key: &str) -> ProviderDestinationConfig {
    ProviderDestinationConfig::R2(R2ProviderDestination {
        bucket: "destination".into(),
        key: key.into(),
        access_key_id: access_key_id.into(),
        secret_access_key: "sk".into(),
        endpoint_url: Some(endpoint.to_string()),
        ..Default::default()
    })
}

fn signer_subject(transfer_id: &str) -> String {
    format!("beam.transfer.client.prod.sdk.b1m_test.transfer.{transfer_id}.route_recovery_sign")
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..600 {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for condition");
}

/// Ask the SDK's route recovery signer to re-sign one route and return the decoded reply.
async fn request_recovery_signature(
    broker: &FakeBroker,
    transfer_id: &str,
    payload: Value,
    reply: &str,
) -> Option<Value> {
    let request = json!({
        "message_id": "runtime-message", "schema_version": TRANSFER_CLIENT_CONTROL_SCHEMA_VERSION,
        "environment": "prod", "key_prefix": "b1m_test", "transfer_id": transfer_id,
        "message_type": "transfer.route_recovery.sign", "request_id": "runtime-request",
        "occurred_at": iso_now(), "producer": "transfer-runtime", "payload": payload,
    });
    if !broker.deliver(
        &signer_subject(transfer_id),
        Bytes::from(to_vec_named(&request).unwrap()),
        Some(reply.to_string()),
    ) {
        return None;
    }
    for _ in 0..200 {
        let published = broker.published.lock().unwrap().clone();
        if let Some((_, payload)) = published.iter().find(|(subject, _)| subject == reply) {
            return Some(from_slice(payload).unwrap());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    None
}

#[tokio::test]
async fn multipart_provider_control_is_bounded_independently_from_route_signing() {
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let created = Arc::new(AtomicUsize::new(0));
    let (active_server, max_server, created_server) =
        (active.clone(), max_active.clone(), created.clone());
    let server = TestServer::start(Arc::new(move |request: &RecordedHttp| {
        if request.method == "HEAD" {
            return TestResponse::status(200).header("Content-Length", "1024");
        }
        if request.method == "POST" && request.target.contains("uploads") {
            let now = active_server.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            max_server.fetch_max(now, AtomicOrdering::SeqCst);
            let upload = created_server.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            let active = active_server.clone();
            return TestResponse::status(200)
                .xml(&format!(
                    "<CreateMultipartUploadResult><UploadId>upload-{upload}</UploadId></CreateMultipartUploadResult>"
                ))
                .delay(Duration::from_millis(30))
                .after(move || {
                    active.fetch_sub(1, AtomicOrdering::SeqCst);
                });
        }
        TestResponse::status(404)
    }))
    .await;
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with_concurrency(
        broker.clone(),
        64,
        crate::DEFAULT_MULTIPART_CONTROL_CONCURRENCY,
    );
    let identities = Arc::new(StdMutex::new(Vec::new()));
    let recorded = identities.clone();
    let result = client
        .create_provider_transfer(ProviderTransferCreateInput {
            sources: vec![r2_source(&server.url, "ak")],
            destinations: (0..5)
                .map(|index| r2_destination(&server.url, "ak", &format!("output-{index}.bin")))
                .collect(),
            distribute: Some(false),
            on_multipart_group_ready: Some(Arc::new(move |identity| {
                recorded.lock().unwrap().push(identity);
                Box::pin(async { Ok(()) })
            })),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(result.success);
    assert_eq!(crate::DEFAULT_MULTIPART_CONTROL_CONCURRENCY, 2);
    assert_eq!(created.load(AtomicOrdering::SeqCst), 5);
    assert_eq!(max_active.load(AtomicOrdering::SeqCst), 2);
    let identities = identities.lock().unwrap().clone();
    assert_eq!(identities.len(), 5);
    let mut upload_ids = identities
        .iter()
        .map(|identity| identity.upload_id.clone())
        .collect::<Vec<_>>();
    upload_ids.sort();
    assert_eq!(
        upload_ids,
        (1..=5)
            .map(|index| format!("upload-{index}"))
            .collect::<Vec<_>>()
    );
    for identity in &identities {
        let serialized = serde_json::to_string(identity).unwrap();
        for secret in ["ak", "sk", "http://", "https://"] {
            assert!(
                !serialized.contains(&format!("\"{secret}\"")) && !serialized.contains("http"),
                "{serialized}"
            );
        }
    }
    let begin = &broker.of_type("transfer.route_stream.begin")[0];
    assert_eq!(begin.payload()["auto_distribute"], false);
    let mut manifest_groups = broker
        .of_type("transfer.route_stream.manifest")
        .iter()
        .flat_map(|call| {
            call.payload()["groups"]
                .as_array()
                .unwrap()
                .iter()
                .map(|group| group["multipart_group_id"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut route_groups = broker
        .of_type("transfer.route_stream.batch")
        .iter()
        .flat_map(|call| {
            call.payload()["route_batch"]["routes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|route| route["multipart_group_id"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    manifest_groups.sort();
    route_groups.sort();
    assert_eq!(manifest_groups.len(), 5);
    assert_eq!(route_groups, manifest_groups);
    // Every S3 source read is bound to its own route range.
    let batch = &broker.of_type("transfer.route_stream.batch")[0];
    let source_chunk = &batch.payload()["route_batch"]["source_chunks"][0];
    assert!(source_chunk["source_url"]
        .as_str()
        .unwrap()
        .contains("x-id=GetObject"));
    assert_eq!(source_chunk["headers"]["Range"], "bytes=0-1023");
    client.close().await.unwrap();
}

#[tokio::test]
async fn non_recoverable_route_failure_preserves_scoped_authority_through_multipart_cleanup() {
    let aborts = Arc::new(StdMutex::new(Vec::<String>::new()));
    let recorded = aborts.clone();
    let server = TestServer::start(Arc::new(move |request: &RecordedHttp| {
        if request.method == "HEAD" {
            return TestResponse::status(200).header("Content-Length", "1024");
        }
        if request.method == "POST" && request.target.contains("uploads") {
            return TestResponse::status(200).xml(
                "<CreateMultipartUploadResult><UploadId>upload-cleanup</UploadId></CreateMultipartUploadResult>",
            );
        }
        if request.method == "DELETE" && request.target.contains("uploadId=upload-cleanup") {
            recorded
                .lock()
                .unwrap()
                .push(request.header("authorization").unwrap_or_default().to_string());
            return TestResponse::status(204);
        }
        TestResponse::status(404)
    }))
    .await;
    let broker = FakeBroker::new(fake_beamcore());
    let base = fake_beamcore();
    broker.set_responder(Arc::new(move |message_type, payload| {
        if message_type == "transfer.route_stream.complete" {
            return Err((
                400,
                json!({"code": "route_contract_rejected", "message": "rejected"}),
            ));
        }
        base(message_type, payload)
    }));
    let client = client_with(broker.clone());
    let error = client
        .create_provider_transfer(ProviderTransferCreateInput {
            sources: vec![r2_source(&server.url, "cleanup-access")],
            destinations: vec![r2_destination(&server.url, "cleanup-access", "output.bin")],
            distribute: Some(false),
            ..Default::default()
        })
        .await
        .unwrap_err();
    let transfer_id = match &error {
        BeamApiError::ProviderTransfer {
            transfer_id,
            transfer_cancelled,
            multipart_cleanup_complete,
            cause,
            errors,
        } => {
            assert!(*transfer_cancelled);
            assert!(*multipart_cleanup_complete);
            assert!(matches!(
                **cause,
                BeamApiError::Lifecycle { status: 400, .. }
            ));
            assert_eq!(errors.len(), 1);
            transfer_id.clone()
        }
        other => panic!("unexpected error: {other:?}"),
    };
    let aborts = aborts.lock().unwrap().clone();
    assert_eq!(aborts.len(), 1);
    assert!(aborts[0].contains("Credential=cleanup-access/"));
    assert_eq!(broker.of_type("transfer.cancel").len(), 1);
    assert!(!client.control().has_recovery_lease(&transfer_id).await);
    client.close().await.unwrap();
}

#[tokio::test]
async fn multipart_group_ready_failures_abort_the_upload_and_fail_closed() {
    let deletes = Arc::new(AtomicUsize::new(0));
    let deletes_server = deletes.clone();
    let server = TestServer::start(Arc::new(move |request: &RecordedHttp| {
        match request.method.as_str() {
            "HEAD" => TestResponse::status(200).header("Content-Length", "1024"),
            "POST" => TestResponse::status(200).xml(
                "<CreateMultipartUploadResult><UploadId>upload-callback</UploadId></CreateMultipartUploadResult>",
            ),
            "DELETE" => {
                deletes_server.fetch_add(1, AtomicOrdering::SeqCst);
                TestResponse::status(204)
            }
            _ => TestResponse::status(404),
        }
    }))
    .await;
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let error = client
        .prepare_provider_transfer(ProviderTransferCreateInput {
            sources: vec![r2_source(&server.url, "ak")],
            destinations: vec![r2_destination(&server.url, "ak", "output.bin")],
            on_multipart_group_ready: Some(Arc::new(|_| {
                Box::pin(async {
                    Err(BeamApiError::ProviderSigning(
                        "journal write failed".to_string(),
                    ))
                })
            })),
            ..Default::default()
        })
        .await
        .unwrap_err();
    match error {
        BeamApiError::ProviderTransfer {
            cause,
            transfer_cancelled,
            multipart_cleanup_complete,
            ..
        } => {
            assert!(cause.to_string().contains("journal write failed"));
            assert!(transfer_cancelled);
            assert!(multipart_cleanup_complete);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(deletes.load(AtomicOrdering::SeqCst), 1);
    assert!(broker.of_type("transfer.route_stream.manifest").is_empty());
    assert!(broker.of_type("transfer.route_stream.complete").is_empty());
    client.close().await.unwrap();
}

#[tokio::test]
async fn recoverable_route_failures_keep_the_lease_without_cancelling() {
    let server = TestServer::start(Arc::new(|request: &RecordedHttp| {
        match request.method.as_str() {
            "HEAD" => TestResponse::status(200).header("Content-Length", "1024"),
            "POST" => TestResponse::status(200).xml(
                "<CreateMultipartUploadResult><UploadId>upload-1</UploadId></CreateMultipartUploadResult>",
            ),
            _ => TestResponse::status(404),
        }
    }))
    .await;
    let broker = FakeBroker::new(fake_beamcore());
    broker.fail_next("transfer.route_stream.batch");
    let client = client_with(broker.clone());
    let error = client
        .prepare_provider_transfer(ProviderTransferCreateInput {
            sources: vec![r2_source(&server.url, "ak")],
            destinations: vec![r2_destination(&server.url, "ak", "output.bin")],
            ..Default::default()
        })
        .await
        .unwrap_err();
    let BeamApiError::RouteRecoveryPending { transfer_id, .. } = error else {
        panic!("unexpected error: {error:?}");
    };
    assert!(client.control().has_recovery_lease(&transfer_id).await);
    assert!(broker.of_type("transfer.cancel").is_empty());
    assert!(!server
        .requests()
        .iter()
        .any(|request| request.method == "DELETE"));
    client.close().await.unwrap();
}

#[tokio::test]
async fn callbacks_run_in_typescript_order_and_throw_if_cancelled_hands_off_to_recovery() {
    let server = TestServer::start(Arc::new(|request: &RecordedHttp| {
        match request.method.as_str() {
            "HEAD" => TestResponse::status(200).header("Content-Length", "1024"),
            "POST" => TestResponse::status(200).xml(
                "<CreateMultipartUploadResult><UploadId>upload-1</UploadId></CreateMultipartUploadResult>",
            ),
            _ => TestResponse::status(404),
        }
    }))
    .await;
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let events = Arc::new(StdMutex::new(Vec::<String>::new()));
    let (before, prepared, polled) = (events.clone(), events.clone(), events.clone());
    let error = client
        .prepare_provider_transfer(ProviderTransferCreateInput {
            sources: vec![r2_source(&server.url, "ak")],
            destinations: vec![r2_destination(&server.url, "ak", "output.bin")],
            on_before_transfer_prepare: Some(Arc::new(move || {
                before.lock().unwrap().push("before_prepare".into());
                Box::pin(async { Ok(()) })
            })),
            on_prepared: Some(Arc::new(move |_| {
                prepared.lock().unwrap().push("prepared".into());
                Box::pin(async { Ok(()) })
            })),
            throw_if_cancelled: Some(Arc::new(move |transfer_id| {
                polled
                    .lock()
                    .unwrap()
                    .push(format!("poll:{}", transfer_id.is_some()));
                let cancel = transfer_id.is_some();
                Box::pin(async move {
                    if cancel {
                        Err(BeamApiError::ProviderSigning("caller cancelled".into()))
                    } else {
                        Ok(())
                    }
                })
            })),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("caller cancelled"));
    assert_eq!(
        events.lock().unwrap().clone(),
        vec![
            "poll:false",
            "poll:false",
            "poll:false",
            "before_prepare",
            "prepared",
            "poll:true"
        ]
    );
    let prepare = &broker.of_type("transfer.prepare")[0];
    let transfer_id = prepare.payload()["transfer_id"]
        .as_str()
        .unwrap()
        .to_string();
    // The transfer is not cancelled; recovery continues in the background.
    assert!(broker.of_type("transfer.cancel").is_empty());
    assert!(client.control().has_recovery_lease(&transfer_id).await);
    assert!(broker.of_type("transfer.route_stream.begin").is_empty());
    client.close().await.unwrap();
}

#[tokio::test]
async fn mixed_providers_sign_multipart_and_direct_put_routes_in_one_transfer() {
    let hippius = TestServer::start(Arc::new(|request: &RecordedHttp| {
        let action = if request.target.contains("action=put") {
            "put"
        } else {
            "get"
        };
        TestResponse::status(200).json(json!({"url": format!("https://hippius.example/{action}")}))
    }))
    .await;
    let s3 = TestServer::start(Arc::new(|request: &RecordedHttp| {
        match request.method.as_str() {
            "HEAD" => TestResponse::status(200)
                .header("Content-Length", "2048")
                .header("ETag", "\"src-etag\""),
            "POST" => TestResponse::status(200).xml(
                "<CreateMultipartUploadResult><UploadId>upload-mixed</UploadId></CreateMultipartUploadResult>",
            ),
            _ => TestResponse::status(404),
        }
    }))
    .await;
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let prepared = client
        .prepare_provider_transfer(ProviderTransferCreateInput {
            sources: vec![r2_source(&s3.url, "ak")],
            destinations: vec![
                r2_destination(&s3.url, "ak", "out/multipart.bin"),
                ProviderDestinationConfig::Hippius(HippiusProviderDestination {
                    bucket: "b".into(),
                    key: "out/direct/".into(),
                    api_token: "token".into(),
                    base_url: Some(hippius.url.clone()),
                    ..Default::default()
                }),
            ],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(prepared.plan_descriptor.delivery_route_count, 4);
    let prepare = &broker.of_type("transfer.prepare")[0];
    // Beam chooses the chunk size; only a Hugging Face destination sends its Hub's part size.
    assert!(prepare.payload().get("chunk_size").is_none());
    assert!(prepare.payload().get("provider_part_size").is_none());
    assert_eq!(
        prepare.payload()["sources"][0]["metadata"]["etag"],
        "\"src-etag\""
    );
    let manifests = broker.of_type("transfer.route_stream.manifest");
    assert_eq!(manifests.len(), 1, "only the S3 destination is multipart");
    assert_eq!(manifests[0].payload()["groups"][0]["max_part_number"], 2);
    let routes = broker
        .of_type("transfer.route_stream.batch")
        .iter()
        .flat_map(|call| {
            call.payload()["route_batch"]["routes"]
                .as_array()
                .unwrap()
                .clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(routes.len(), 4);
    for route in &routes {
        if route["destination_id"] == "dst_0" {
            assert_eq!(
                route["multipart_group_id"],
                json!(format!(
                    "{}:dst_0:src_0:out/multipart.bin",
                    prepared.transfer_id
                ))
            );
            assert!(route["dest_url"]
                .as_str()
                .unwrap()
                .contains("x-id=UploadPart"));
        } else {
            assert!(route.get("multipart_group_id").is_none());
            assert_eq!(route["dest_url"], "https://hippius.example/put");
        }
    }
    // Part numbers are consecutive: source chunk i is part i + 1.
    let mut parts = routes
        .iter()
        .filter(|route| route["destination_id"] == "dst_0")
        .map(|route| route["metadata"]["part_number"].as_u64().unwrap())
        .collect::<Vec<_>>();
    parts.sort();
    assert_eq!(parts, vec![1, 2]);
    client.close().await.unwrap();
}

#[tokio::test]
async fn ownership_cancellation_stops_signing_without_cancelling_the_transfer() {
    let token = CancellationToken::new();
    let server_token = token.clone();
    let server = TestServer::start(Arc::new(move |request: &RecordedHttp| {
        match request.method.as_str() {
            "HEAD" => TestResponse::status(200).header("Content-Length", "1024"),
            "POST" => {
                // A replacement owner takes over while the upload is being created.
                server_token.cancel();
                TestResponse::status(200).xml(
                    "<CreateMultipartUploadResult><UploadId>upload-1</UploadId></CreateMultipartUploadResult>",
                )
            }
            _ => TestResponse::status(404),
        }
    }))
    .await;
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let error = client
        .prepare_provider_transfer(ProviderTransferCreateInput {
            cancellation: Some(token),
            sources: vec![r2_source(&server.url, "ak")],
            destinations: vec![r2_destination(&server.url, "ak", "output.bin")],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(error, BeamApiError::Aborted), "{error:?}");
    let transfer_id = broker.of_type("transfer.prepare")[0].payload()["transfer_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(broker.of_type("transfer.cancel").is_empty());
    assert!(!server
        .requests()
        .iter()
        .any(|request| request.method == "DELETE"));
    assert!(!client.control().has_recovery_lease(&transfer_id).await);
    client.close().await.unwrap();
}

#[tokio::test]
async fn resume_provider_transfer_replays_routes_with_retained_uploads_and_fences_replaced_owners()
{
    let heads = Arc::new(AtomicUsize::new(0));
    let creates = Arc::new(AtomicUsize::new(0));
    let (heads_server, creates_server) = (heads.clone(), creates.clone());
    let server = TestServer::start(Arc::new(move |request: &RecordedHttp| {
        if request.method == "HEAD" {
            heads_server.fetch_add(1, AtomicOrdering::SeqCst);
            return TestResponse::status(200).header("Content-Length", "1024");
        }
        if request.method == "POST" && request.target.contains("uploads") {
            creates_server.fetch_add(1, AtomicOrdering::SeqCst);
        }
        TestResponse::status(404)
    }))
    .await;
    let transfer_id = "77777777-7777-4777-8777-777777777777";
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with_concurrency(broker.clone(), 4, 2);
    let group_id = format!("{transfer_id}:dst_0:src_0:out/file.bin");
    let multipart_groups = vec![ProviderMultipartGroupIdentity {
        transfer_id: transfer_id.to_string(),
        multipart_group_id: group_id.clone(),
        source_id: "src_0".into(),
        destination_id: "dst_0".into(),
        object_key: "out/file.bin".into(),
        upload_id: "upload-existing".into(),
        expected_object_size: 1024,
        expected_part_count: 1,
        expires_at: iso_after(Duration::from_secs(60)),
    }];
    let ownership = CancellationToken::new();
    let resume_input = |groups: Vec<ProviderMultipartGroupIdentity>| ProviderTransferResumeInput {
        transfer_id: transfer_id.to_string(),
        multipart_groups: groups,
        cancellation: Some(ownership.clone()),
        sources: vec![r2_source(&server.url, "ak")],
        destinations: vec![r2_destination(&server.url, "ak", "out/file.bin")],
        ..Default::default()
    };
    let begins_at_prepared = Arc::new(AtomicUsize::new(usize::MAX));
    let begins_recorder = begins_at_prepared.clone();
    let broker_for_callback = broker.clone();
    let mut first = resume_input(multipart_groups.clone());
    first.on_prepared = Some(Arc::new(move |_| {
        begins_recorder.store(
            broker_for_callback
                .of_type("transfer.route_stream.begin")
                .len(),
            AtomicOrdering::SeqCst,
        );
        Box::pin(async { Ok(()) })
    }));
    let prepared = client.resume_provider_transfer(first).await.unwrap();
    assert_eq!(prepared.transfer_id, transfer_id);
    assert_eq!(begins_at_prepared.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(heads.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(creates.load(AtomicOrdering::SeqCst), 0);
    assert!(broker
        .subscriptions
        .lock()
        .unwrap()
        .contains_key(&signer_subject(transfer_id)));
    assert!(client.control().has_recovery_lease(transfer_id).await);

    // A second owner of the same control plane resumes the same transfer.
    let second = BeamClient::from_parts(
        reqwest::Client::new(),
        client.control().clone(),
        4,
        false,
        2,
    );
    second
        .resume_provider_transfer(resume_input(multipart_groups.clone()))
        .await
        .unwrap();
    let prepares = broker.of_type("transfer.prepare");
    assert_eq!(prepares.len(), 2);
    assert_ne!(
        prepares[0].envelope["request_id"],
        prepares[1].envelope["request_id"]
    );
    assert_ne!(
        prepares[0].payload()["route_generation_id"],
        prepares[1].payload()["route_generation_id"]
    );

    let reply = request_recovery_signature(
        &broker,
        transfer_id,
        json!({
            "transfer_id": transfer_id,
            "route_generation_id": "recover-1",
            "chunks": [{
                "source_id": "src_0", "destination_id": "dst_0", "chunk_index": 0,
                "delivery_index": 0, "source_offset": 0, "chunk_size": 1024,
                "logical_attempt_index": 1, "attempt_slot": 0, "part_number": 1,
                "route_generation_id": "recover-1", "multipart_group_id": group_id,
                "final_object_key": "out/file.bin", "upload_id": "upload-existing"
            }]
        }),
        "reply.recover-1",
    )
    .await
    .expect("route recovery reply");
    assert_eq!(reply["ok"], true, "{reply}");
    let payload = &reply["payload"];
    assert_eq!(payload["transfer_id"], transfer_id);
    assert_eq!(payload["route_generation_id"], "recover-1");
    let route = &payload["chunk_routes"][0];
    assert_eq!(payload["chunk_routes"].as_array().unwrap().len(), 1);
    assert_eq!(route["metadata"]["upload_id"], "upload-existing");
    assert_eq!(route["metadata"]["multipart_group_id"], json!(group_id));
    assert_eq!(route["metadata"]["part_number"], 1);
    assert!(route["dest_url"]
        .as_str()
        .unwrap()
        .contains("uploadId=upload-existing"));
    assert!(route["metadata"].get("recovery_staging").is_none());

    // A v7 staged recovery request: the worker gets a staging PutObject, and BeamCore gets
    // the copy grant into the original upload and part.
    let attempt_id = "44444444-4444-4444-8444-444444444444";
    let reply = request_recovery_signature(
        &broker,
        transfer_id,
        json!({
            "transfer_id": transfer_id,
            "route_generation_id": "recover-2",
            "chunks": [{
                "source_id": "src_0", "destination_id": "dst_0", "chunk_index": 0,
                "delivery_index": 0, "source_offset": 0, "chunk_size": 1024,
                "logical_attempt_index": 2, "attempt_slot": 0, "part_number": 1,
                "route_generation_id": "recover-2", "multipart_group_id": group_id,
                "final_object_key": "out/file.bin", "upload_id": "upload-existing",
                "recovery": {"operation": "upload", "mode": "staged", "attempt_id": attempt_id}
            }]
        }),
        "reply.recover-2",
    )
    .await
    .expect("staged route recovery reply");
    assert_eq!(reply["ok"], true, "{reply}");
    let route = &reply["payload"]["chunk_routes"][0];
    let staging_key = format!(
        "out/file.bin.beam-recovery/{transfer_id}/{}/1/{attempt_id}",
        crate::sigv4::uri_encode(&group_id, false)
    );
    let staging = &route["metadata"]["recovery_staging"];
    assert_eq!(staging["object_key"], json!(staging_key));
    assert_eq!(route["metadata"]["upload_id"], "upload-existing");
    let dest_url = route["dest_url"].as_str().unwrap();
    assert!(!dest_url.contains("uploadId="), "{dest_url}");
    assert!(dest_url.contains("x-id=PutObject"), "{dest_url}");
    let copy_url = staging["copy_url"].as_str().unwrap();
    assert!(copy_url.contains("uploadId=upload-existing"), "{copy_url}");
    assert!(copy_url.contains("partNumber=1&"), "{copy_url}");
    assert!(staging["copy_headers"]["x-amz-copy-source"]
        .as_str()
        .unwrap()
        .ends_with(&format!("/1/{attempt_id}")));

    // Recovery coordinates must be consecutive: a v6 attempt slot is rejected.
    let reply = request_recovery_signature(
        &broker,
        transfer_id,
        json!({
            "transfer_id": transfer_id,
            "route_generation_id": "recover-3",
            "chunks": [{
                "source_id": "src_0", "destination_id": "dst_0", "chunk_index": 0,
                "delivery_index": 0, "source_offset": 0, "chunk_size": 1024,
                "logical_attempt_index": 1, "attempt_slot": 1, "part_number": 2,
                "route_generation_id": "recover-3", "multipart_group_id": group_id,
                "final_object_key": "out/file.bin", "upload_id": "upload-existing"
            }]
        }),
        "reply.recover-3",
    )
    .await
    .expect("rejected route recovery reply");
    assert_eq!(reply["ok"], false, "{reply}");
    assert_eq!(broker.of_type("transfer.route_stream.complete").len(), 2);

    let lease = client
        .control()
        .recovery_lease(transfer_id)
        .await
        .expect("recovery lease");
    (lease.replay_routes)("33333333-3333-4333-8333-333333333333".to_string())
        .await
        .unwrap();
    assert_eq!(
        creates.load(AtomicOrdering::SeqCst),
        0,
        "replay must reuse the upload"
    );
    assert_eq!(broker.of_type("transfer.route_stream.complete").len(), 3);

    let mut different_object = multipart_groups[0].clone();
    different_object.object_key = "different".into();
    let mut empty_upload = multipart_groups[0].clone();
    empty_upload.upload_id = String::new();
    for invalid in [
        Vec::new(),
        vec![multipart_groups[0].clone(), multipart_groups[0].clone()],
        vec![different_object],
        vec![empty_upload],
    ] {
        let error = second
            .resume_provider_transfer(resume_input(invalid))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("provider_multipart_recovery"),
            "{error}"
        );
    }
    assert_eq!(
        creates.load(AtomicOrdering::SeqCst),
        0,
        "invalid retained state must never create another upload"
    );

    ownership.cancel();
    wait_until(|| {
        !broker
            .subscriptions
            .lock()
            .unwrap()
            .get(&signer_subject(transfer_id))
            .is_some_and(|sender| !sender.is_closed())
    })
    .await;
    assert!(
        request_recovery_signature(&broker, transfer_id, json!({}), "reply.later")
            .await
            .is_none(),
        "a replaced owner must not answer recovery signing"
    );
    assert!(matches!(
        (lease.replay_routes)("44444444-4444-4444-8444-444444444444".to_string()).await,
        Err(BeamApiError::Aborted)
    ));
    assert!(broker.of_type("transfer.cancel").is_empty());
    client.close().await.unwrap();
    second.close().await.unwrap();
}

#[tokio::test]
async fn hippius_route_recovery_signs_the_planned_chunk_key_not_the_final_object_key() {
    let presigned = Arc::new(StdMutex::new(Vec::<(String, String)>::new()));
    let recorder = presigned.clone();
    let hippius = TestServer::start(Arc::new(move |request: &RecordedHttp| {
        let url = reqwest::Url::parse(&format!("http://hippius.test{}", request.target)).unwrap();
        let query = |name: &str| {
            url.query_pairs()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.into_owned())
                .unwrap_or_default()
        };
        let (key, action) = (query("key"), query("action"));
        recorder.lock().unwrap().push((key.clone(), action.clone()));
        TestResponse::status(200)
            .json(json!({"url": format!("https://hippius.example/{key}?action={action}")}))
    }))
    .await;
    let s3 = TestServer::start(Arc::new(|request: &RecordedHttp| {
        match request.method.as_str() {
            "HEAD" => TestResponse::status(200).header("Content-Length", "2048"),
            _ => TestResponse::status(404),
        }
    }))
    .await;
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let prepared = client
        .prepare_provider_transfer(ProviderTransferCreateInput {
            sources: vec![r2_source(&s3.url, "ak")],
            destinations: vec![ProviderDestinationConfig::Hippius(
                HippiusProviderDestination {
                    bucket: "bucket".into(),
                    key: "out/file.bin".into(),
                    api_token: "token".into(),
                    base_url: Some(hippius.url.clone()),
                    ..Default::default()
                },
            )],
            ..Default::default()
        })
        .await
        .unwrap();
    let transfer_id = prepared.transfer_id.clone();
    presigned.lock().unwrap().clear();

    let reply = request_recovery_signature(
        &broker,
        &transfer_id,
        json!({
            "transfer_id": transfer_id,
            "route_generation_id": "recover-hippius",
            "chunks": [{
                "source_id": "src_0", "destination_id": "dst_0", "chunk_index": 1,
                "delivery_index": 1, "source_offset": 1024, "chunk_size": 1024,
                "logical_attempt_index": 1, "attempt_slot": 0,
                "part_number": multipart_part_number(1, 0).unwrap(),
                "route_generation_id": "recover-hippius", "multipart_group_id": "",
                "final_object_key": "out/file.bin", "upload_id": ""
            }]
        }),
        "reply.recover-hippius",
    )
    .await
    .expect("route recovery reply");
    assert_eq!(reply["ok"], true, "{reply}");
    // Initial materialization uses `{final}/{plan_nonce}/chunk-NNNNNN`; recovery must match it.
    let chunk_key = "out/file.bin/testplan/chunk-000001";
    let puts = presigned
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, action)| action == "put")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(puts, vec![(chunk_key.to_string(), "put".to_string())]);
    let routes = reply["payload"]["chunk_routes"].as_array().unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(
        routes[0]["dest_url"],
        json!(format!("https://hippius.example/{chunk_key}?action=put"))
    );
    assert_eq!(routes[0]["chunk_index"], 1);
    client.close().await.unwrap();
}

#[tokio::test]
async fn worker_source_reads_are_pinned_to_the_prepared_source_object() {
    for pinned in [true, false] {
        let hippius = TestServer::start(Arc::new(|_: &RecordedHttp| {
            TestResponse::status(200)
                .json(json!({"url": "https://hippius.example/upload"}))
        }))
        .await;
        let s3 = TestServer::start(Arc::new(move |request: &RecordedHttp| {
            match request.method.as_str() {
                "HEAD" if pinned => TestResponse::status(200)
                    .header("Content-Length", "1024")
                    .header("ETag", "\"source-etag\"")
                    .header("x-amz-version-id", "source-version"),
                "HEAD" => TestResponse::status(200).header("Content-Length", "1024"),
                _ => TestResponse::status(404),
            }
        }))
        .await;
        let broker = FakeBroker::new(fake_beamcore());
        let client = client_with(broker.clone());
        let prepared = client
            .prepare_provider_transfer(ProviderTransferCreateInput {
                sources: vec![r2_source(&s3.url, "ak")],
                destinations: vec![ProviderDestinationConfig::Hippius(
                    HippiusProviderDestination {
                        bucket: "bucket".into(),
                        key: "out/file.bin".into(),
                        api_token: "token".into(),
                        base_url: Some(hippius.url.clone()),
                        ..Default::default()
                    },
                )],
                ..Default::default()
            })
            .await
            .unwrap();
        let transfer_id = prepared.transfer_id.clone();
        let reply = request_recovery_signature(
            &broker,
            &transfer_id,
            json!({
                "transfer_id": transfer_id,
                "route_generation_id": "recover-pinned",
                "chunks": [{
                    "source_id": "src_0", "destination_id": "dst_0", "chunk_index": 0,
                    "delivery_index": 0, "source_offset": 0, "chunk_size": 1024,
                    "logical_attempt_index": 1, "attempt_slot": 0,
                    "part_number": multipart_part_number(0, 0).unwrap(),
                    "route_generation_id": "recover-pinned", "multipart_group_id": "",
                    "final_object_key": "out/file.bin", "upload_id": ""
                }]
            }),
            "reply.recover-pinned",
        )
        .await
        .expect("route recovery reply");
        assert_eq!(reply["ok"], true, "{reply}");
        let batch = &broker.of_type("transfer.route_stream.batch")[0];
        let streamed = &batch.payload()["route_batch"]["source_chunks"][0];
        let recovered = &reply["payload"]["chunk_routes"][0];
        for read in [streamed, recovered] {
            let source_url = read["source_url"].as_str().unwrap();
            let signed_headers = reqwest::Url::parse(source_url)
                .unwrap()
                .query_pairs()
                .find(|(key, _)| key == "X-Amz-SignedHeaders")
                .map(|(_, value)| value.into_owned())
                .unwrap();
            if pinned {
                assert_eq!(
                    read["headers"],
                    json!({"Range": "bytes=0-1023", "If-Match": "\"source-etag\""})
                );
                assert!(
                    source_url.contains("versionId=source-version"),
                    "{source_url}"
                );
                assert!(signed_headers.contains("if-match"), "{signed_headers}");
            } else {
                assert_eq!(read["headers"], json!({"Range": "bytes=0-1023"}));
                assert!(!source_url.contains("versionId="), "{source_url}");
                assert!(!signed_headers.contains("if-match"), "{signed_headers}");
            }
        }
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_fenced_owners_failed_replay_keeps_the_replacement_lease_and_signer_on_the_same_client() {
    let server = TestServer::start(Arc::new(|request: &RecordedHttp| {
        if request.method == "HEAD" {
            return TestResponse::status(200).header("Content-Length", "1024");
        }
        TestResponse::status(404)
    }))
    .await;
    let transfer_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    let broker = FakeBroker::new(fake_beamcore());
    let client = client_with(broker.clone());
    let resume_input = |cancellation: CancellationToken| ProviderTransferResumeInput {
        transfer_id: transfer_id.to_string(),
        multipart_groups: vec![ProviderMultipartGroupIdentity {
            transfer_id: transfer_id.to_string(),
            multipart_group_id: format!("{transfer_id}:dst_0:src_0:out/file.bin"),
            source_id: "src_0".into(),
            destination_id: "dst_0".into(),
            object_key: "out/file.bin".into(),
            upload_id: "upload-existing".into(),
            expected_object_size: 1024,
            expected_part_count: 1,
            expires_at: iso_after(Duration::from_secs(60)),
        }],
        cancellation: Some(cancellation),
        sources: vec![r2_source(&server.url, "ak")],
        destinations: vec![r2_destination(&server.url, "ak", "out/file.bin")],
        ..Default::default()
    };
    let fenced_ownership = CancellationToken::new();
    client
        .resume_provider_transfer(resume_input(fenced_ownership.clone()))
        .await
        .unwrap();
    let fenced_lease = client.control().recovery_lease(transfer_id).await.unwrap();
    client
        .resume_provider_transfer(resume_input(CancellationToken::new()))
        .await
        .unwrap();
    let replacement_lease = client.control().recovery_lease(transfer_id).await.unwrap();
    assert!(!Arc::ptr_eq(&fenced_lease, &replacement_lease));
    let replacement_serving = || {
        broker
            .subscriptions
            .lock()
            .unwrap()
            .get(&signer_subject(transfer_id))
            .is_some_and(|sender| !sender.is_closed())
    };
    assert!(replacement_serving());

    // Owner A is fenced off in the middle of a background route replay.
    let fence = fenced_ownership.clone();
    *broker.request_hook.lock().unwrap() = Some(Arc::new(move |message_type, _| {
        if message_type == "transfer.route_stream.begin" {
            fence.cancel();
        }
        Box::pin(async {})
    }));
    let replayed =
        (fenced_lease.replay_routes)("cccccccc-cccc-4ccc-8ccc-cccccccccccc".to_string()).await;
    *broker.request_hook.lock().unwrap() = None;
    assert!(
        matches!(replayed, Err(BeamApiError::Aborted)),
        "{replayed:?}"
    );
    tokio::time::sleep(Duration::from_millis(20)).await;

    let current = client.control().recovery_lease(transfer_id).await;
    assert!(
        current.is_some_and(|current| Arc::ptr_eq(&current, &replacement_lease)),
        "replacement lease must survive"
    );
    assert!(
        replacement_serving(),
        "replacement recovery signer must keep serving"
    );
    assert!(
        client.has_recovery_signer(transfer_id),
        "replacement signer registration must survive"
    );
    assert!(
        client.has_integrity_context(transfer_id),
        "replacement integrity signer must survive"
    );
    client.close().await.unwrap();
}
