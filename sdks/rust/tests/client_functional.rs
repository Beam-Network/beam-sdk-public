use std::collections::HashMap;

use beam_network_sdk::{
    BeamApiError, BeamClient, BeamClientOptions, TransferCreateRequest, DEFAULT_MAX_PAYLOAD_BYTES,
};

#[test]
fn route_message_default_and_recovery_error_are_public() {
    assert_eq!(DEFAULT_MAX_PAYLOAD_BYTES, 8 * 1024 * 1024);
    let pending = BeamApiError::RouteRecoveryPending {
        transfer_id: "transfer-1".to_string(),
        source: Box::new(BeamApiError::HttpStatus { status: 409 }),
    };
    assert!(pending.to_string().contains("transfer-1"));
}

#[test]
fn rust_sdk_rejects_http_lifecycle_endpoint() {
    let result = BeamClient::new(BeamClientOptions {
        api_key: "b1m_rust".to_string(),
        nats_url: Some("http://beamcore.test".to_string()),
        environment: None,
        http_client: None,
        transfer_runtime_shard_count: None,
        request_timeout: None,
        max_payload_bytes: None,
        route_signing_concurrency: None,
    });

    match result {
        Ok(_) => panic!("HTTP lifecycle endpoint should be rejected"),
        Err(BeamApiError::Nats(message)) => {
            assert!(message.contains("nats_url must use nats:// or tls://"))
        }
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn rust_sdk_omits_chunk_size_when_caller_does_not_set_it() {
    let request = TransferCreateRequest {
        transfer_id: None,
        idempotency_key: None,
        sources: vec![HashMap::new()],
        destinations: vec![HashMap::new()],
        total_size: 10 * 1024 * 1024,
        chunk_size: None,
        name: None,
        merkle_root: None,
        chunk_hashes: None,
        callbacks: None,
        test_mode: false,
        progressive_mode: false,
    };

    let payload = serde_json::to_value(request).expect("serialize transfer create request");
    assert!(payload.get("chunk_size").is_none());
}
