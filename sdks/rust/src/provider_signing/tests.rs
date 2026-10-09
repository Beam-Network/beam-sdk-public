use super::*;
use crate::test_http::{RecordedHttp, TestResponse, TestServer};
use crate::{
    R2ProviderDestination, R2ProviderSource, S3CompatibleProviderDestination,
    S3CompatibleProviderSource, S3ProviderDestination, S3ProviderSource,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[test]
fn source_grant_reuse_cannot_extend_provider_expiry() {
    let url = "https://example.invalid/read?X-Amz-Date=20260926T100000Z&X-Amz-Expires=3600";
    let actual: SystemTime = time::Date::from_calendar_date(2026, time::Month::September, 26)
        .unwrap()
        .with_hms(11, 0, 0)
        .unwrap()
        .assume_utc()
        .into();
    assert_eq!(
        bounded_grant_expiry(url, actual + Duration::from_secs(3600)),
        actual
    );
    assert_eq!(
        bounded_grant_expiry(url, actual - Duration::from_secs(60)),
        actual - Duration::from_secs(60)
    );
}

fn query_param(url: &str, name: &str) -> Option<String> {
    let url = reqwest::Url::parse(url).unwrap();
    url.query_pairs()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

fn s3_destination(endpoint: &str) -> ProviderDestinationConfig {
    ProviderDestinationConfig::S3(S3ProviderDestination {
        bucket: "dest-bucket".into(),
        key: "imports/report.parquet".into(),
        region: Some("us-east-1".into()),
        access_key_id: "ak".into(),
        secret_access_key: "sk".into(),
        endpoint_url: Some(endpoint.to_string()),
        ..Default::default()
    })
}

fn options() -> ProviderSigningOptions {
    ProviderSigningOptions {
        expires_in: Duration::from_secs(600),
        ..Default::default()
    }
}

#[tokio::test]
async fn provider_signing_handles_s3_r2_and_generic_s3_compatible_helpers() {
    let server = TestServer::start(Arc::new(|request: &RecordedHttp| {
        if request.method == "HEAD" {
            return TestResponse::status(200)
                .header("Content-Length", "1234")
                .header("ETag", "\"etag-1\"")
                .header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                .header("x-amz-version-id", "v1");
        }
        if request.method == "POST" && request.target.contains("uploads") {
            return TestResponse::status(200).xml(
                "<CreateMultipartUploadResult><Bucket>dest-bucket</Bucket><Key>imports/report.parquet</Key><UploadId>upload_123</UploadId></CreateMultipartUploadResult>",
            );
        }
        if request.method == "DELETE" {
            return TestResponse::status(204);
        }
        TestResponse::status(404).body("not found")
    }))
    .await;
    let endpoint = server.url.clone();

    let s3_source = ProviderSourceConfig::S3(S3ProviderSource {
        bucket: "source-bucket".into(),
        key: "exports/report.parquet".into(),
        region: Some("us-east-1".into()),
        access_key_id: "ak".into(),
        secret_access_key: "sk".into(),
        endpoint_url: Some(endpoint.clone()),
        ..Default::default()
    });
    let prepared = prepare_provider_source(&s3_source, &options())
        .await
        .unwrap();
    assert_eq!(prepared.provider.as_deref(), Some("s3"));
    assert_eq!(prepared.size, 1234);
    assert!(prepared.url.contains("X-Amz-Signature="));
    assert!(prepared.expires_at.is_some());
    assert_eq!(prepared.metadata["driver"], "s3-compatible");
    assert_eq!(prepared.metadata["content_length"], 1234);
    assert_eq!(prepared.metadata["etag"], "\"etag-1\"");
    assert_eq!(
        prepared.metadata["last_modified"],
        "2015-10-21T07:28:00.000Z"
    );
    assert_eq!(prepared.metadata["version_id"], "v1");
    assert_eq!(prepared.metadata["endpoint_url"], endpoint.as_str());

    let r2_source = ProviderSourceConfig::R2(R2ProviderSource {
        bucket: "source-bucket".into(),
        key: "exports/report.parquet".into(),
        access_key_id: "ak".into(),
        secret_access_key: "sk".into(),
        endpoint_url: Some(endpoint.clone()),
        ..Default::default()
    });
    let prepared_r2 = prepare_provider_source(&r2_source, &options())
        .await
        .unwrap();
    assert_eq!(prepared_r2.provider.as_deref(), Some("r2"));
    assert_eq!(prepared_r2.metadata["region"], "auto");

    let wasabi = ProviderSourceConfig::S3Compatible(
        S3CompatibleProviderSource {
            provider: "wasabi".into(),
            bucket: "custom-source-bucket".into(),
            key: "objects/custom.bin".into(),
            region: Some("us-east-1".into()),
            access_key_id: "ak".into(),
            secret_access_key: "sk".into(),
            endpoint_url: Some(endpoint.clone()),
            ..Default::default()
        }
        .create()
        .unwrap(),
    );
    let prepared_wasabi = prepare_provider_source(&wasabi, &options()).await.unwrap();
    assert_eq!(prepared_wasabi.provider.as_deref(), Some("wasabi"));
    assert_eq!(prepared_wasabi.metadata["bucket"], "custom-source-bucket");
    assert_eq!(prepared_wasabi.metadata["key"], "objects/custom.bin");
    assert!(prepared_wasabi.url.starts_with(&format!(
        "{endpoint}/custom-source-bucket/objects/custom.bin"
    )));

    let destination = s3_destination(&endpoint);
    let prepared_destination = prepare_provider_destination(&destination, 0).unwrap();
    assert_eq!(prepared_destination.provider, "s3");
    assert_eq!(prepared_destination.metadata["driver"], "s3-compatible");

    let minio = ProviderDestinationConfig::S3Compatible(S3CompatibleProviderDestination {
        provider: "minio".into(),
        driver: "s3-compatible".into(),
        bucket: "archive".into(),
        key: "file.bin".into(),
        access_key_id: "ak".into(),
        secret_access_key: "sk".into(),
        endpoint_url: Some(endpoint.clone()),
        ..Default::default()
    });
    let prepared_minio = prepare_provider_destination(&minio, 0).unwrap();
    assert_eq!(prepared_minio.provider, "minio");
    assert_eq!(prepared_minio.logical_prefix.as_deref(), Some("file.bin"));
    assert_eq!(prepared_minio.metadata["endpoint_url"], endpoint.as_str());

    let upload_id = create_multipart_upload(
        &destination,
        "imports/report.parquet",
        &HashMap::from([("beam-transfer-id".to_string(), "transfer_123".to_string())]),
        &options(),
    )
    .await
    .unwrap();
    assert_eq!(upload_id, "upload_123");
    let create = server
        .requests()
        .into_iter()
        .find(|request| request.method == "POST")
        .unwrap();
    assert_eq!(
        create.header("x-amz-meta-beam-transfer-id"),
        Some("transfer_123")
    );
    assert!(create
        .header("authorization")
        .unwrap()
        .contains("Credential=ak/"));
    let expires = Duration::from_secs(600);
    assert!(sign_complete_multipart_upload(
        &destination,
        "imports/report.parquet",
        &upload_id,
        expires
    )
    .unwrap()
    .contains("X-Amz-Signature="));
    assert!(sign_abort_multipart_upload(
        &destination,
        "imports/report.parquet",
        &upload_id,
        expires
    )
    .unwrap()
    .contains("X-Amz-Signature="));
    let list_url = sign_list_multipart_upload(
        &destination,
        "imports/report.parquet",
        &upload_id,
        expires,
        ListPartsPage::default(),
    )
    .unwrap();
    assert!(list_url.contains("X-Amz-Signature="));
    assert!(list_url.contains("x-id=ListParts"));

    let chunk = ChunkSigningPlanItem {
        chunk_index: 0,
        source_id: "src_0".into(),
        source_chunk_index: 0,
        source_offset: 0,
        chunk_size: 1234,
        source_url: "https://source.example/read".into(),
        destinations: Vec::new(),
    };
    let mut input = DestinationRouteInput::new(
        chunk.clone(),
        ChunkDestinationSigningTarget {
            destination_id: "dst_0".into(),
            provider: None,
            object_key: Some("imports/report.parquet".into()),
            metadata: HashMap::from([(
                "final_object_key".to_string(),
                json!("imports/report.parquet"),
            )]),
        },
        destination.clone(),
    );
    input.part_number = Some(1);
    input.upload_id = Some(upload_id.clone());
    input.complete_url = Some("https://dest.example/complete".into());
    input.abort_url = Some("https://dest.example/abort".into());
    input.list_page_url = Some("https://dest.example/list-parts".into());
    input.final_head_url = Some("https://dest.example/head".into());
    input.final_object_key = Some("imports/report.parquet".into());
    input.expected_object_size = Some(1234);
    input.expected_part_count = Some(1);
    input.max_part_number = Some(1);
    input.final_object_metadata = Some(HashMap::from([(
        "beam-transfer-id".to_string(),
        "transfer_123".to_string(),
    )]));
    let route = sign_destination_route(input, &options()).await.unwrap();
    assert!(route.dest_url.contains("X-Amz-Signature="));
    assert!(route.dest_url.contains("UploadPart"));
    assert!(route.dest_url.contains("imports/report.parquet"));
    assert_eq!(route.metadata["upload_id"], json!(upload_id));
    assert_eq!(route.metadata["final_object_key"], "imports/report.parquet");
    assert_eq!(route.metadata["part_number"], 1);
    assert_eq!(route.headers.as_ref().unwrap()["Range"], "bytes=0-1233");
    // Without a source config the plan's source URL is used as-is.
    assert_eq!(route.source_url, "https://source.example/read");

    let custom_route = sign_destination_route(
        DestinationRouteInput::new(
            ChunkSigningPlanItem {
                chunk_index: 1,
                source_chunk_index: 1,
                source_offset: 1234,
                chunk_size: 512,
                ..chunk
            },
            ChunkDestinationSigningTarget {
                destination_id: "dst_custom".into(),
                provider: None,
                object_key: Some("file.bin".into()),
                metadata: HashMap::new(),
            },
            minio,
        ),
        &options(),
    )
    .await
    .unwrap();
    assert!(custom_route
        .dest_url
        .starts_with(&format!("{endpoint}/archive/file.bin")));
    assert!(custom_route.dest_url.contains("x-id=PutObject"));
    assert_eq!(
        custom_route.headers.as_ref().unwrap()["Range"],
        "bytes=1234-1745"
    );

    abort_multipart_upload(
        &destination,
        "imports/report.parquet",
        &upload_id,
        &options(),
    )
    .await
    .unwrap();

    let error = prepare_provider_destination(
        &ProviderDestinationConfig::R2(R2ProviderDestination {
            bucket: "b".into(),
            key: "k".into(),
            access_key_id: "ak".into(),
            secret_access_key: "sk".into(),
            ..Default::default()
        }),
        0,
    )
    .unwrap_err();
    assert!(error.to_string().contains("account_id or endpoint_url"));
    let requests = server.requests();
    assert!(requests.iter().any(|request| request.method == "HEAD"
        && request
            .target
            .starts_with("/custom-source-bucket/objects/custom.bin")));
    assert!(requests.iter().any(|request| request.method == "DELETE"));
}

#[test]
fn s3_compatible_endpoint_region_and_path_style_resolution_is_provider_aware() {
    let r2 = ProviderSourceConfig::R2(R2ProviderSource {
        bucket: "bucket".into(),
        key: "file.bin".into(),
        account_id: Some("account123".into()),
        access_key_id: "ak".into(),
        secret_access_key: "sk".into(),
        ..Default::default()
    });
    assert_eq!(
        r2.s3_compatible_endpoint().unwrap().as_deref(),
        Some("https://account123.r2.cloudflarestorage.com")
    );
    assert_eq!(r2.s3_compatible_region().unwrap(), "auto");
    assert_eq!(r2.s3_compatible_force_path_style().unwrap(), Some(true));

    let custom = S3CompatibleProviderSource {
        provider: "wasabi".into(),
        driver: "s3-compatible".into(),
        bucket: "bucket".into(),
        key: "file.bin".into(),
        endpoint_url: Some("https://s3.us-east-1.wasabisys.com".into()),
        access_key_id: "ak".into(),
        secret_access_key: "sk".into(),
        ..Default::default()
    };
    let wasabi = ProviderSourceConfig::S3Compatible(custom.clone());
    assert_eq!(
        wasabi.s3_compatible_endpoint().unwrap().as_deref(),
        Some("https://s3.us-east-1.wasabisys.com")
    );
    assert_eq!(wasabi.s3_compatible_region().unwrap(), "us-east-1");
    assert_eq!(wasabi.s3_compatible_force_path_style().unwrap(), Some(true));
    let virtual_hosted = ProviderSourceConfig::S3Compatible(S3CompatibleProviderSource {
        force_path_style: Some(false),
        ..custom
    });
    assert_eq!(
        virtual_hosted.s3_compatible_force_path_style().unwrap(),
        Some(false)
    );

    let s3 = ProviderSourceConfig::S3(S3ProviderSource {
        bucket: "bucket".into(),
        key: "file.bin".into(),
        access_key_id: "ak".into(),
        secret_access_key: "sk".into(),
        ..Default::default()
    });
    assert_eq!(s3.s3_compatible_endpoint().unwrap(), None);
    assert_eq!(s3.s3_compatible_region().unwrap(), "us-east-1");
    assert_eq!(s3.s3_compatible_force_path_style().unwrap(), None);
}

#[tokio::test]
async fn s3_compatible_multipart_cleanup_retries_transient_provider_failures() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let server = TestServer::start(Arc::new(move |request: &RecordedHttp| {
        if request.method == "DELETE" {
            if counter.fetch_add(1, Ordering::SeqCst) < 2 {
                return TestResponse::status(503)
                    .xml("<Error><Code>SlowDown</Code><Message>retry later</Message></Error>");
            }
            return TestResponse::status(204);
        }
        TestResponse::status(404)
    }))
    .await;
    abort_multipart_upload(
        &ProviderDestinationConfig::R2(R2ProviderDestination {
            bucket: "destination".into(),
            key: "output.bin".into(),
            endpoint_url: Some(server.url.clone()),
            access_key_id: "ak".into(),
            secret_access_key: "sk".into(),
            ..Default::default()
        }),
        "output.bin",
        "upload-retry",
        &options(),
    )
    .await
    .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn non_retryable_s3_errors_fail_without_leaking_urls() {
    let server = TestServer::start(Arc::new(|_: &RecordedHttp| {
        TestResponse::status(403)
            .xml("<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>")
    }))
    .await;
    let error = abort_multipart_upload(&s3_destination(&server.url), "file", "upload", &options())
        .await
        .unwrap_err();
    assert_eq!(server.requests().len(), 1);
    assert_eq!(error.safe_code(), "AccessDenied:status=403");
    assert!(!error.to_string().contains("http"));
}

#[tokio::test]
async fn hybrid_signing_composes_frozen_source_ranges_and_checksum_bound_uploads_without_reads() {
    // An unreachable HTTP client proves signing is local.
    let no_network = ProviderSigningOptions {
        expires_in: Duration::from_secs(60),
        http_client: Some(
            HttpClient::builder()
                .connect_timeout(Duration::from_millis(1))
                .build()
                .unwrap(),
        ),
        ..Default::default()
    };
    for provider in ["r2", "hippius", "huggingface"] {
        let config = S3CompatibleProviderSource {
            provider: provider.into(),
            driver: "s3-compatible".into(),
            bucket: "bucket".into(),
            key: "file.bin".into(),
            access_key_id: "fixture-key".into(),
            secret_access_key: "fixture-secret".into(),
            region: Some("us-east-1".into()),
            endpoint_url: Some("https://storage.example.test".into()),
            force_path_style: Some(true),
            ..Default::default()
        };
        let source = sign_source_read_range(
            &ProviderSourceConfig::S3Compatible(config.clone()),
            &SourceReadRange {
                offset: 10,
                length: 20,
                if_match: Some("\"frozen-etag\"".into()),
                version_id: Some("version-1".into()),
            },
            &no_network,
        )
        .await
        .unwrap();
        assert_eq!(source.headers["Range"], "bytes=10-29");
        assert_eq!(source.headers["If-Match"], "\"frozen-etag\"");
        assert_eq!(
            query_param(&source.url, "versionId").as_deref(),
            Some("version-1")
        );
        assert!(query_param(&source.url, "X-Amz-SignedHeaders")
            .unwrap()
            .contains("if-match"));

        let destination =
            ProviderDestinationConfig::S3Compatible(S3CompatibleProviderDestination {
                provider: config.provider.clone(),
                driver: config.driver.clone(),
                bucket: config.bucket.clone(),
                key: config.key.clone(),
                access_key_id: config.access_key_id.clone(),
                secret_access_key: config.secret_access_key.clone(),
                region: config.region.clone(),
                endpoint_url: config.endpoint_url.clone(),
                force_path_style: config.force_path_style,
                ..Default::default()
            });
        let url = sign_destination_url(
            &destination,
            &DestinationUrlInput {
                object_key: "final.bin".into(),
                upload_id: Some("upload-1".into()),
                part_number: Some(1),
                content_md5: Some("1B2M2Y8AsgTpgAmY7PhCfg==".into()),
            },
            &no_network,
        )
        .await
        .unwrap();
        let parsed = reqwest::Url::parse(&url).unwrap();
        assert_eq!(parsed.path(), "/bucket/final.bin");
        assert_eq!(query_param(&url, "uploadId").as_deref(), Some("upload-1"));
        assert_eq!(query_param(&url, "partNumber").as_deref(), Some("1"));
        assert!(query_param(&url, "X-Amz-SignedHeaders")
            .unwrap()
            .contains("content-md5"));
    }

    let hippius = ProviderSourceConfig::Hippius(crate::HippiusProviderSource {
        bucket: "b".into(),
        key: "k".into(),
        api_token: "t".into(),
        ..Default::default()
    });
    let error = sign_source_read_range(
        &hippius,
        &SourceReadRange {
            offset: 0,
            length: 1,
            if_match: Some("\"etag\"".into()),
            version_id: None,
        },
        &no_network,
    )
    .await
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("conditional source ranges require S3-compatible storage"));
    let error = sign_destination_url(
        &ProviderDestinationConfig::Hippius(crate::HippiusProviderDestination {
            bucket: "b".into(),
            key: "k".into(),
            api_token: "t".into(),
            ..Default::default()
        }),
        &DestinationUrlInput {
            object_key: "k".into(),
            content_md5: Some("md5".into()),
            ..Default::default()
        },
        &no_network,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("checksum-bound uploads"));
}

#[tokio::test]
async fn worker_source_chunk_grants_pin_s3_compatible_reads_to_the_prepared_object() {
    let source = ProviderSourceConfig::R2(R2ProviderSource {
        bucket: "bucket".into(),
        key: "file.bin".into(),
        access_key_id: "fixture-key".into(),
        secret_access_key: "fixture-secret".into(),
        endpoint_url: Some("https://storage.example.test".into()),
        ..Default::default()
    });
    let chunk = ChunkSigningPlanItem {
        chunk_index: 1,
        source_id: "src_0".into(),
        source_chunk_index: 1,
        source_offset: 1024,
        chunk_size: 1024,
        source_url: "https://source.example/read".into(),
        destinations: Vec::new(),
    };
    let pinned = sign_source_chunk(
        Some(&source),
        &chunk,
        None,
        Some("\"frozen-etag\""),
        Some("version-1"),
        &options(),
    )
    .await
    .unwrap();
    assert_eq!(
        pinned.headers,
        HashMap::from([
            ("Range".to_string(), "bytes=1024-2047".to_string()),
            ("If-Match".to_string(), "\"frozen-etag\"".to_string()),
        ])
    );
    assert_eq!(
        query_param(&pinned.url, "versionId").as_deref(),
        Some("version-1")
    );
    assert!(query_param(&pinned.url, "X-Amz-SignedHeaders")
        .unwrap()
        .contains("if-match"));

    let unpinned = sign_source_chunk(Some(&source), &chunk, None, None, None, &options())
        .await
        .unwrap();
    assert_eq!(
        unpinned.headers,
        HashMap::from([("Range".to_string(), "bytes=1024-2047".to_string())])
    );
    assert_eq!(query_param(&unpinned.url, "versionId"), None);
    assert!(!query_param(&unpinned.url, "X-Amz-SignedHeaders")
        .unwrap()
        .contains("if-match"));
}

#[tokio::test]
async fn provider_verification_paginates_parts_and_completes_without_reading_payloads() {
    let server = TestServer::start(Arc::new(|request: &RecordedHttp| {
        if request.method == "HEAD" {
            return TestResponse::status(200)
                .header("Content-Length", "12")
                .header("ETag", "\"final\"")
                .header("x-amz-meta-beam-room-operation-id", "operation");
        }
        if request.method == "GET" && request.target.contains("uploadId=") {
            let second = request.target.contains("part-number-marker=1");
            let body = format!(
                "<ListPartsResult><IsTruncated>{}</IsTruncated>{}<Part><PartNumber>{}</PartNumber><ETag>{}</ETag><Size>6</Size></Part></ListPartsResult>",
                !second,
                if second { "" } else { "<NextPartNumberMarker>1</NextPartNumberMarker>" },
                if second { 2 } else { 1 },
                if second { "b" } else { "a" },
            );
            return TestResponse::status(200).xml(&body);
        }
        if request.method == "POST" {
            assert!(request.body_text().contains("<Part><ETag>a</ETag><PartNumber>1</PartNumber></Part>"));
            return TestResponse::status(200).xml(
                "<CompleteMultipartUploadResult><ETag>final</ETag></CompleteMultipartUploadResult>",
            );
        }
        TestResponse::status(500)
    }))
    .await;
    let config = ProviderDestinationConfig::S3(S3ProviderDestination {
        bucket: "bucket".into(),
        key: "file".into(),
        endpoint_url: Some(server.url.clone()),
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        ..Default::default()
    });
    let parts = list_multipart_parts(&config, "file", "upload", &options())
        .await
        .unwrap();
    assert_eq!(
        parts,
        vec![
            MultipartPart {
                part_number: 1,
                etag: "a".into(),
                size: 6
            },
            MultipartPart {
                part_number: 2,
                etag: "b".into(),
                size: 6
            }
        ]
    );
    let completed = complete_multipart_upload(
        &config,
        "file",
        "upload",
        &parts
            .iter()
            .rev()
            .map(|part| CompletedPart {
                part_number: part.part_number,
                etag: part.etag.clone(),
            })
            .collect::<Vec<_>>(),
        &options(),
    )
    .await
    .unwrap();
    assert_eq!(completed.etag.as_deref(), Some("final"));
    let head = inspect_destination_object(&config, "file", &options())
        .await
        .unwrap();
    assert_eq!(head.size, Some(12));
    assert_eq!(head.metadata["beam-room-operation-id"], "operation");
    let requests = server.requests();
    assert_eq!(requests.len(), 4);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "GET")
            .count(),
        2
    );
}

#[tokio::test]
async fn provider_metadata_cancellation_stops_every_in_flight_http_operation() {
    type Operation = fn(
        ProviderDestinationConfig,
        ProviderSigningOptions,
    )
        -> std::pin::Pin<Box<dyn Future<Output = Result<(), BeamApiError>> + Send>>;
    let operations: Vec<Operation> = vec![
        |destination, options| {
            Box::pin(async move {
                create_multipart_upload(&destination, "file", &HashMap::new(), &options)
                    .await
                    .map(drop)
            })
        },
        |destination, options| {
            Box::pin(async move {
                list_multipart_parts(&destination, "file", "upload", &options)
                    .await
                    .map(drop)
            })
        },
        |destination, options| {
            Box::pin(async move {
                complete_multipart_upload(
                    &destination,
                    "file",
                    "upload",
                    &[CompletedPart {
                        part_number: 1,
                        etag: "part".into(),
                    }],
                    &options,
                )
                .await
                .map(drop)
            })
        },
        |destination, options| {
            Box::pin(async move {
                inspect_destination_object(&destination, "file", &options)
                    .await
                    .map(drop)
            })
        },
        |destination, options| {
            Box::pin(async move {
                abort_multipart_upload(&destination, "file", "upload", &options).await
            })
        },
    ];
    for operation in operations {
        let token = CancellationToken::new();
        let received = Arc::new(AtomicUsize::new(0));
        let received_by_server = received.clone();
        let token_for_server = token.clone();
        let server = TestServer::start(Arc::new(move |_: &RecordedHttp| {
            received_by_server.fetch_add(1, Ordering::SeqCst);
            token_for_server.cancel();
            TestResponse::status(200).delay(Duration::from_secs(30))
        }))
        .await;
        let options = ProviderSigningOptions {
            cancellation: Some(token),
            ..options()
        };
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            operation(s3_destination(&server.url), options),
        )
        .await
        .expect("cancellation must stop the operation");
        assert!(matches!(result, Err(BeamApiError::Aborted)));
        assert_eq!(
            received.load(Ordering::SeqCst),
            1,
            "request must reach the provider before cancellation"
        );
    }
}

/// Optional check against a real S3 implementation. Start MinIO and set
/// `BEAM_MINIO_ENDPOINT` (for example `http://127.0.0.1:9000`, credentials
/// `minioadmin`/`minioadmin`, bucket `beam-test` created), then run
/// `cargo test -- --ignored minio`.
#[tokio::test]
#[ignore = "requires a MinIO server (BEAM_MINIO_ENDPOINT)"]
async fn minio_accepts_sdk_signatures_end_to_end() {
    let endpoint = std::env::var("BEAM_MINIO_ENDPOINT").expect("BEAM_MINIO_ENDPOINT");
    let access_key = std::env::var("BEAM_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret_key = std::env::var("BEAM_MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());
    let bucket = std::env::var("BEAM_MINIO_BUCKET").unwrap_or_else(|_| "beam-test".into());
    let destination = ProviderDestinationConfig::S3Compatible(
        S3CompatibleProviderDestination {
            provider: "minio".into(),
            bucket: bucket.clone(),
            key: "sdk/object.bin".into(),
            region: Some("us-east-1".into()),
            endpoint_url: Some(endpoint.clone()),
            access_key_id: access_key.clone(),
            secret_access_key: secret_key.clone(),
            ..Default::default()
        }
        .create()
        .unwrap(),
    );
    let http = HttpClient::new();
    let options = ProviderSigningOptions {
        http_client: Some(http.clone()),
        ..options()
    };
    // Whole-object PUT through a presigned URL, then HEAD and a conditional ranged GET.
    let put_url = sign_destination_url(
        &destination,
        &DestinationUrlInput {
            object_key: "sdk/whole.bin".into(),
            ..Default::default()
        },
        &options,
    )
    .await
    .unwrap();
    let put = http
        .put(&put_url)
        .body(vec![1_u8; 32])
        .send()
        .await
        .unwrap();
    assert!(put.status().is_success(), "presigned PUT: {}", put.status());
    let head = inspect_destination_object(&destination, "sdk/whole.bin", &options)
        .await
        .unwrap();
    assert_eq!(head.size, Some(32));
    let read = sign_destination_read_range(
        &destination,
        &DestinationReadRange {
            object_key: "sdk/whole.bin".into(),
            offset: 4,
            length: 8,
            if_match: head.etag.clone(),
        },
        &options,
    )
    .await
    .unwrap();
    let mut request = http.get(&read.url);
    for (name, value) in &read.headers {
        request = request.header(name, value);
    }
    let response = request.send().await.unwrap();
    assert_eq!(response.status().as_u16(), 206);
    assert_eq!(response.bytes().await.unwrap().len(), 8);

    // Multipart: create, presigned UploadPart, list, presigned HEAD, complete.
    let upload_id = create_multipart_upload(
        &destination,
        "sdk/multipart.bin",
        &HashMap::from([("beam-transfer-id".to_string(), "transfer-minio".to_string())]),
        &options,
    )
    .await
    .unwrap();
    let part_url = sign_destination_url(
        &destination,
        &DestinationUrlInput {
            object_key: "sdk/multipart.bin".into(),
            upload_id: Some(upload_id.clone()),
            part_number: Some(1),
            content_md5: None,
        },
        &options,
    )
    .await
    .unwrap();
    let part = http
        .put(&part_url)
        .body(vec![2_u8; 64])
        .send()
        .await
        .unwrap();
    assert!(
        part.status().is_success(),
        "presigned UploadPart: {}",
        part.status()
    );
    let parts = list_multipart_parts(&destination, "sdk/multipart.bin", &upload_id, &options)
        .await
        .unwrap();
    assert_eq!(parts.len(), 1);
    let list_url = sign_list_multipart_upload(
        &destination,
        "sdk/multipart.bin",
        &upload_id,
        Duration::from_secs(600),
        ListPartsPage {
            max_parts: Some(1_000),
            part_number_marker: None,
        },
    )
    .unwrap();
    assert!(http
        .get(&list_url)
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    complete_multipart_upload(
        &destination,
        "sdk/multipart.bin",
        &upload_id,
        &parts
            .iter()
            .map(|part| CompletedPart {
                part_number: part.part_number,
                etag: part.etag.clone(),
            })
            .collect::<Vec<_>>(),
        &options,
    )
    .await
    .unwrap();
    let head_url =
        sign_final_object_head(&destination, "sdk/multipart.bin", Duration::from_secs(600))
            .unwrap();
    let head = http.head(&head_url).send().await.unwrap();
    assert!(head.status().is_success());
    assert_eq!(
        head.headers()
            .get("x-amz-meta-beam-transfer-id")
            .and_then(|value| value.to_str().ok()),
        Some("transfer-minio")
    );
    // Abort on a fresh upload.
    let upload_id =
        create_multipart_upload(&destination, "sdk/aborted.bin", &HashMap::new(), &options)
            .await
            .unwrap();
    abort_multipart_upload(&destination, "sdk/aborted.bin", &upload_id, &options)
        .await
        .unwrap();
}

const RECOVERY_ATTEMPT: &str = "22222222-2222-4222-8222-222222222222";
const RECOVERY_GROUP: &str = "group:one/(a)";

fn recovery_r2_destination() -> ProviderDestinationConfig {
    ProviderDestinationConfig::R2(R2ProviderDestination {
        bucket: "dev-bucket".into(),
        key: "file.bin".into(),
        endpoint_url: Some("https://r2.example".into()),
        access_key_id: "dev-key".into(),
        secret_access_key: "dev-secret".into(),
        ..Default::default()
    })
}

fn recovery_s3_destination() -> ProviderDestinationConfig {
    ProviderDestinationConfig::S3(S3ProviderDestination {
        bucket: "dev-bucket".into(),
        key: "file.bin".into(),
        region: Some("us-east-1".into()),
        access_key_id: "dev-key".into(),
        secret_access_key: "dev-secret".into(),
        ..Default::default()
    })
}

fn recovery_request(operation: MultipartRecoveryOperation) -> MultipartRecoveryRequest {
    MultipartRecoveryRequest {
        operation,
        mode: MultipartRecoveryMode::Staged,
        attempt_id: RECOVERY_ATTEMPT.into(),
        object_key: None,
        etag: None,
        continuation_token: None,
    }
}

fn recovery_route() -> SignedChunkRoute {
    SignedChunkRoute {
        source_id: "src".into(),
        destination_id: "dst".into(),
        chunk_index: 1000,
        delivery_index: Some(1000),
        source_url: "https://source.example/object".into(),
        dest_url: "https://r2.example/direct".into(),
        source_offset: 0,
        chunk_size: 8,
        expires_at: None,
        headers: None,
        dest_headers: None,
        metadata: HashMap::from([
            ("upload_id".to_string(), json!("original")),
            ("final_object_key".to_string(), json!("file.bin")),
            ("part_number".to_string(), json!(1001)),
            ("expected_part_count".to_string(), json!(10_000)),
        ]),
    }
}

fn sign_recovery(
    destination: &ProviderDestinationConfig,
    request: Option<&MultipartRecoveryRequest>,
) -> Result<SignedChunkRoute, BeamApiError> {
    sign_multipart_recovery(
        MultipartRecoverySignInput {
            destination,
            transfer_id: "transfer",
            multipart_group_id: RECOVERY_GROUP,
            final_object_key: "file.bin",
            upload_id: "original",
            part_number: 1001,
            recovery: request,
            expires_in: Duration::from_secs(600),
        },
        recovery_route(),
    )
}

fn metadata_str<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key]
        .as_str()
        .unwrap_or_else(|| panic!("{key} in {value}"))
}

#[test]
fn recovery_signing_keeps_final_identity_separate_and_binds_copy_to_its_original_part() {
    let result = sign_recovery(
        &recovery_r2_destination(),
        Some(&recovery_request(MultipartRecoveryOperation::Upload)),
    )
    .unwrap();
    let staging = &result.metadata["recovery_staging"];
    let object_key =
        format!("file.bin.beam-recovery/transfer/group%3Aone%2F%28a%29/1001/{RECOVERY_ATTEMPT}");
    assert_eq!(metadata_str(staging, "object_key"), object_key);
    assert_eq!(metadata_str(staging, "attempt_id"), RECOVERY_ATTEMPT);
    assert_eq!(result.metadata["upload_id"], "original");
    assert_eq!(result.metadata["part_number"], 1001);

    // The worker receives an ordinary PutObject of the staging object, not an UploadPart.
    assert!(query_param(&result.dest_url, "uploadId").is_none());
    assert_eq!(
        query_param(&result.dest_url, "x-id").as_deref(),
        Some("PutObject")
    );
    assert!(result.dest_url.starts_with(&format!(
        "https://r2.example/dev-bucket/file.bin.beam-recovery/transfer/group%253Aone%252F%2528a%2529/1001/{RECOVERY_ATTEMPT}?"
    )));

    let copy_url = metadata_str(staging, "copy_url");
    assert!(copy_url.starts_with("https://r2.example/dev-bucket/file.bin?"));
    assert_eq!(
        query_param(copy_url, "uploadId").as_deref(),
        Some("original")
    );
    assert_eq!(query_param(copy_url, "partNumber").as_deref(), Some("1001"));
    assert_eq!(
        query_param(copy_url, "x-id").as_deref(),
        Some("UploadPartCopy")
    );
    assert_eq!(
        query_param(copy_url, "X-Amz-SignedHeaders").as_deref(),
        Some("host;x-amz-copy-source")
    );
    assert!(query_param(copy_url, "x-amz-copy-source").is_none());
    // encodeURIComponent per segment, as the TypeScript SDK does: the staging key's own
    // percent-encoding is escaped once more so S3 decodes back to the literal key.
    assert_eq!(
        staging["copy_headers"],
        json!({"x-amz-copy-source": format!(
            "dev-bucket/file.bin.beam-recovery/transfer/group%253Aone%252F%2528a%2529/1001/{RECOVERY_ATTEMPT}"
        )})
    );
    assert_eq!(
        query_param(metadata_str(staging, "delete_url"), "x-id").as_deref(),
        Some("DeleteObject")
    );
    assert!(metadata_str(staging, "head_url").contains(".beam-recovery/"));
    assert!(metadata_str(staging, "expires_at").ends_with('Z'));

    // `controls` re-signs the same grants without replacing the worker's upload URL.
    let controls = sign_recovery(
        &recovery_r2_destination(),
        Some(&recovery_request(MultipartRecoveryOperation::Controls)),
    )
    .unwrap();
    assert_eq!(controls.dest_url, "https://r2.example/direct");
    assert_eq!(
        controls.metadata["recovery_staging"]["object_key"],
        json!(object_key)
    );
}

#[test]
fn each_recovery_listing_page_is_signed_for_the_exact_prefix_and_continuation() {
    let mut request = recovery_request(MultipartRecoveryOperation::List);
    request.continuation_token = Some("a+/=&".into());
    let result = sign_recovery(&recovery_r2_destination(), Some(&request)).unwrap();
    let listing = &result.metadata["recovery_listing"];
    let url = metadata_str(listing, "url");
    let prefix = metadata_str(listing, "prefix");
    assert_eq!(
        prefix,
        "file.bin.beam-recovery/transfer/group%3Aone%2F%28a%29/"
    );
    assert!(url.starts_with("https://r2.example/dev-bucket/?"));
    assert_eq!(
        query_param(url, "continuation-token").as_deref(),
        Some("a+/=&")
    );
    assert_eq!(query_param(url, "prefix").as_deref(), Some(prefix));
    assert_eq!(query_param(url, "list-type").as_deref(), Some("2"));
    assert_eq!(query_param(url, "max-keys").as_deref(), Some("1000"));
    assert!(!result.metadata.contains_key("recovery_staging"));
    assert_eq!(result.dest_url, "https://r2.example/direct");

    request.continuation_token = None;
    let first = sign_recovery(&recovery_r2_destination(), Some(&request)).unwrap();
    let url = metadata_str(&first.metadata["recovery_listing"], "url");
    assert!(query_param(url, "continuation-token").is_none());
}

#[test]
fn recovery_renewal_regenerates_all_list_parts_pages_without_changing_multipart_identity() {
    let mut request = recovery_request(MultipartRecoveryOperation::Renew);
    request.mode = MultipartRecoveryMode::Direct;
    let result = sign_recovery(&recovery_r2_destination(), Some(&request)).unwrap();
    let pages = result.metadata["list_page_urls"].as_array().unwrap();
    assert_eq!(pages.len(), 10);
    for (index, page) in pages.iter().enumerate() {
        let page = page.as_str().unwrap();
        assert_eq!(query_param(page, "uploadId").as_deref(), Some("original"));
        assert_eq!(
            query_param(page, "part-number-marker"),
            Some((index * 1_000).to_string())
        );
        assert_eq!(query_param(page, "max-parts").as_deref(), Some("1000"));
    }
    assert_eq!(result.metadata["list_page_url"], pages[1]);
    assert_eq!(result.metadata["part_number"], 1001);
    assert_eq!(result.metadata["upload_id"], "original");
    for key in ["complete_url", "abort_url", "final_head_url"] {
        assert!(result.metadata[key]
            .as_str()
            .unwrap()
            .starts_with("https://r2.example/dev-bucket/file.bin?"));
    }
    assert_eq!(
        query_param(
            result.metadata["complete_url"].as_str().unwrap(),
            "uploadId"
        )
        .as_deref(),
        Some("original")
    );
    assert!(result.metadata["control_urls_expires_at"].is_string());
    assert!(!result.metadata.contains_key("recovery_staging"));

    // Renewal needs a valid part count on the signed route.
    let mut route = recovery_route();
    route.metadata.remove("expected_part_count");
    let error = sign_multipart_recovery(
        MultipartRecoverySignInput {
            destination: &recovery_r2_destination(),
            transfer_id: "transfer",
            multipart_group_id: RECOVERY_GROUP,
            final_object_key: "file.bin",
            upload_id: "original",
            part_number: 1001,
            recovery: Some(&request),
            expires_in: Duration::from_secs(600),
        },
        route,
    )
    .unwrap_err();
    assert!(error.to_string().contains("invalid multipart count"));
}

#[test]
fn aws_copy_source_conditions_are_quoted_and_unrelated_staging_objects_are_rejected() {
    let mut request = recovery_request(MultipartRecoveryOperation::Controls);
    request.etag = Some("opaque".into());
    let result = sign_recovery(&recovery_s3_destination(), Some(&request)).unwrap();
    let staging = &result.metadata["recovery_staging"];
    assert_eq!(
        staging["copy_headers"]["x-amz-copy-source-if-match"],
        "\"opaque\""
    );
    assert_eq!(
        query_param(metadata_str(staging, "copy_url"), "X-Amz-SignedHeaders").as_deref(),
        Some("host;x-amz-copy-source;x-amz-copy-source-if-match")
    );
    request.etag = Some("\"quoted\"".into());
    let result = sign_recovery(&recovery_s3_destination(), Some(&request)).unwrap();
    assert_eq!(
        result.metadata["recovery_staging"]["copy_headers"]["x-amz-copy-source-if-match"],
        "\"quoted\""
    );
    // R2 does not promise to enforce copy-source conditions, so none is signed.
    let result = sign_recovery(&recovery_r2_destination(), Some(&request)).unwrap();
    assert!(result.metadata["recovery_staging"]["copy_headers"]
        .get("x-amz-copy-source-if-match")
        .is_none());

    let mut request = recovery_request(MultipartRecoveryOperation::Upload);
    request.object_key = Some("another-transfer".into());
    let error = sign_recovery(&recovery_r2_destination(), Some(&request)).unwrap_err();
    assert!(error.to_string().contains("identity"));
    request.object_key = Some(format!(
        "file.bin.beam-recovery/transfer/group%3Aone%2F%28a%29/1001/{RECOVERY_ATTEMPT}"
    ));
    sign_recovery(&recovery_r2_destination(), Some(&request)).unwrap();
}

#[test]
fn recovery_delete_and_direct_requests_keep_the_worker_route() {
    let result = sign_recovery(
        &recovery_r2_destination(),
        Some(&recovery_request(MultipartRecoveryOperation::Delete)),
    )
    .unwrap();
    let delete = &result.metadata["recovery_delete"];
    assert_eq!(
        metadata_str(delete, "object_key"),
        format!("file.bin.beam-recovery/transfer/group%3Aone%2F%28a%29/1001/{RECOVERY_ATTEMPT}")
    );
    assert_eq!(
        query_param(metadata_str(delete, "url"), "x-id").as_deref(),
        Some("DeleteObject")
    );
    assert_eq!(result.dest_url, "https://r2.example/direct");

    let untouched = recovery_route();
    let mut stale = recovery_route();
    stale
        .metadata
        .insert("recovery_staging".into(), json!({"object_key": "stale"}));
    let result = sign_multipart_recovery(
        MultipartRecoverySignInput {
            destination: &recovery_r2_destination(),
            transfer_id: "transfer",
            multipart_group_id: RECOVERY_GROUP,
            final_object_key: "file.bin",
            upload_id: "original",
            part_number: 1001,
            recovery: None,
            expires_in: Duration::from_secs(600),
        },
        stale,
    )
    .unwrap();
    assert_eq!(result.metadata, untouched.metadata);
    assert_eq!(result.dest_url, untouched.dest_url);

    let mut direct = recovery_request(MultipartRecoveryOperation::Upload);
    direct.mode = MultipartRecoveryMode::Direct;
    let result = sign_recovery(&recovery_r2_destination(), Some(&direct)).unwrap();
    assert_eq!(result.metadata, untouched.metadata);
    assert_eq!(result.dest_url, untouched.dest_url);
}

#[test]
fn staged_recovery_requires_s3_compatible_storage_and_a_uuid_attempt() {
    let hippius = ProviderDestinationConfig::Hippius(crate::HippiusProviderDestination {
        bucket: "bucket".into(),
        key: "file.bin".into(),
        api_token: "token".into(),
        ..Default::default()
    });
    let request = recovery_request(MultipartRecoveryOperation::Upload);
    assert!(sign_recovery(&hippius, Some(&request))
        .unwrap_err()
        .to_string()
        .contains("S3-compatible"));
    // Without a recovery request a direct-PUT route passes through unchanged.
    assert!(sign_recovery(&hippius, None).is_ok());
    for attempt in [
        "",
        "not-a-uuid",
        "22222222222242228222222222222222",
        "../escape",
    ] {
        let mut request = recovery_request(MultipartRecoveryOperation::Upload);
        request.attempt_id = attempt.into();
        assert!(sign_recovery(&recovery_r2_destination(), Some(&request))
            .unwrap_err()
            .to_string()
            .contains("invalid staging attempt"));
    }
}

#[test]
fn recovery_requests_use_the_wire_names() {
    let request: MultipartRecoveryRequest = serde_json::from_value(json!({
        "operation": "controls", "mode": "staged", "attempt_id": RECOVERY_ATTEMPT,
        "object_key": "k", "etag": "e", "continuation_token": "t"
    }))
    .unwrap();
    assert_eq!(request.operation, MultipartRecoveryOperation::Controls);
    assert_eq!(request.mode, MultipartRecoveryMode::Staged);
    assert_eq!(request.continuation_token.as_deref(), Some("t"));
    let renew: MultipartRecoveryRequest =
        serde_json::from_value(json!({"operation": "renew", "mode": "direct"})).unwrap();
    assert_eq!(renew.operation, MultipartRecoveryOperation::Renew);
    assert!(renew.attempt_id.is_empty());
    assert_eq!(
        serde_json::to_value(recovery_request(MultipartRecoveryOperation::List)).unwrap(),
        json!({"operation": "list", "mode": "staged", "attempt_id": RECOVERY_ATTEMPT})
    );
}

#[test]
fn encode_uri_component_matches_javascript() {
    assert_eq!(
        encode_uri_component("a-_.!~*'()Z9 /:%+?#&=é"),
        "a-_.!~*'()Z9%20%2F%3A%25%2B%3F%23%26%3D%C3%A9"
    );
    assert_eq!(
        multipart_recovery_prefix("dir/file.bin", "t", "a!b~c*d'e(f)g"),
        "dir/file.bin.beam-recovery/t/a%21b~c%2Ad%27e%28f%29g/"
    );
}

/// Optional end-to-end check of the multipart recovery grants against MinIO (see
/// [`minio_accepts_sdk_signatures_end_to_end`]): stage a part with the worker's PutObject,
/// list and HEAD it, copy it into the original upload (with and without an AWS-style ETag
/// condition), complete, and delete the staging object.
#[tokio::test]
#[ignore = "requires a MinIO server (BEAM_MINIO_ENDPOINT)"]
async fn minio_accepts_multipart_recovery_grants() {
    let endpoint = std::env::var("BEAM_MINIO_ENDPOINT").expect("BEAM_MINIO_ENDPOINT");
    let access_key = std::env::var("BEAM_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret_key = std::env::var("BEAM_MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());
    let bucket = std::env::var("BEAM_MINIO_BUCKET").unwrap_or_else(|_| "beam-test".into());
    // provider "s3" so the copy carries the AWS-only `x-amz-copy-source-if-match` condition.
    let destination = ProviderDestinationConfig::S3Compatible(
        S3CompatibleProviderDestination {
            provider: "s3".into(),
            bucket: bucket.clone(),
            key: "sdk/recovered.bin".into(),
            region: Some("us-east-1".into()),
            endpoint_url: Some(endpoint.clone()),
            access_key_id: access_key,
            secret_access_key: secret_key,
            force_path_style: Some(true),
            ..Default::default()
        }
        .create()
        .unwrap(),
    );
    let http = HttpClient::new();
    let options = ProviderSigningOptions {
        http_client: Some(http.clone()),
        ..options()
    };
    let final_key = "sdk/recovered.bin";
    let upload_id = create_multipart_upload(&destination, final_key, &HashMap::new(), &options)
        .await
        .unwrap();
    let mut route = recovery_route();
    route
        .metadata
        .insert("expected_part_count".into(), json!(1));
    let sign = |request: &MultipartRecoveryRequest| {
        sign_multipart_recovery(
            MultipartRecoverySignInput {
                destination: &destination,
                transfer_id: "transfer-minio",
                multipart_group_id: RECOVERY_GROUP,
                final_object_key: final_key,
                upload_id: &upload_id,
                part_number: 1,
                recovery: Some(request),
                expires_in: Duration::from_secs(600),
            },
            route.clone(),
        )
        .unwrap()
    };

    let staged = sign(&recovery_request(MultipartRecoveryOperation::Upload));
    let put = http
        .put(&staged.dest_url)
        .body(vec![7_u8; 48])
        .send()
        .await
        .unwrap();
    assert!(
        put.status().is_success(),
        "staging PutObject: {}",
        put.status()
    );
    let staging = &staged.metadata["recovery_staging"];
    let object_key = metadata_str(staging, "object_key").to_string();
    let head = http
        .head(metadata_str(staging, "head_url"))
        .send()
        .await
        .unwrap();
    assert!(
        head.status().is_success(),
        "staging HEAD: {}",
        head.status()
    );
    let etag = head
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .unwrap()
        .to_string();

    let listing = sign(&recovery_request(MultipartRecoveryOperation::List));
    let listed = http
        .get(metadata_str(&listing.metadata["recovery_listing"], "url"))
        .send()
        .await
        .unwrap();
    assert!(
        listed.status().is_success(),
        "ListObjectsV2: {}",
        listed.status()
    );
    let body = listed.text().await.unwrap();
    assert!(
        body.contains(&xml_escape(&object_key)),
        "listing lacks {object_key}: {body}"
    );

    let copy_with = |request: MultipartRecoveryRequest| {
        let controls = sign(&request);
        let staging = controls.metadata["recovery_staging"].clone();
        let http = http.clone();
        async move {
            let mut copy = http.put(metadata_str(&staging, "copy_url"));
            for (name, value) in staging["copy_headers"].as_object().unwrap() {
                copy = copy.header(name, value.as_str().unwrap());
            }
            copy.send().await.unwrap()
        }
    };
    let mut wrong = recovery_request(MultipartRecoveryOperation::Controls);
    wrong.etag = Some("0123456789abcdef0123456789abcdef".into());
    let rejected = copy_with(wrong).await;
    assert_eq!(rejected.status().as_u16(), 412, "stale ETag must not copy");
    let mut right = recovery_request(MultipartRecoveryOperation::Controls);
    right.etag = Some(etag);
    right.object_key = Some(object_key.clone());
    let copied = copy_with(right).await;
    let status = copied.status();
    let text = copied.text().await.unwrap();
    assert!(
        status.is_success() && !text.contains("<Error>"),
        "UploadPartCopy: {status} {text}"
    );

    let parts = list_multipart_parts(&destination, final_key, &upload_id, &options)
        .await
        .unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].part_number, 1);
    let mut renew = recovery_request(MultipartRecoveryOperation::Renew);
    renew.mode = MultipartRecoveryMode::Direct;
    let renewed = sign(&renew);
    let page = http
        .get(renewed.metadata["list_page_url"].as_str().unwrap())
        .send()
        .await
        .unwrap();
    assert!(
        page.status().is_success(),
        "renewed ListParts: {}",
        page.status()
    );
    complete_multipart_upload(
        &destination,
        final_key,
        &upload_id,
        &parts
            .iter()
            .map(|part| CompletedPart {
                part_number: part.part_number,
                etag: part.etag.clone(),
            })
            .collect::<Vec<_>>(),
        &options,
    )
    .await
    .unwrap();
    let head = http
        .head(renewed.metadata["final_head_url"].as_str().unwrap())
        .send()
        .await
        .unwrap();
    assert!(head.status().is_success());
    assert_eq!(
        head.headers()
            .get("content-length")
            .and_then(|value| value.to_str().ok()),
        Some("48")
    );

    let deleted = sign(&recovery_request(MultipartRecoveryOperation::Delete));
    let delete = http
        .delete(metadata_str(&deleted.metadata["recovery_delete"], "url"))
        .send()
        .await
        .unwrap();
    assert!(
        delete.status().is_success(),
        "DeleteObject: {}",
        delete.status()
    );
    let gone = http
        .head(metadata_str(staging, "head_url"))
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status().as_u16(), 404);
}
