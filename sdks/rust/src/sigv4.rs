//! AWS Signature Version 4 for S3-compatible requests.
//!
//! Query presigning (for URLs handed to workers) and header signing (for the SDK's own control
//! calls). Validated against the published AWS SigV4 and S3 examples in the tests below.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::BeamApiError;
use crate::nats_control::civil_time;

pub(crate) const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub(crate) const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// SigV4 presigned URLs cannot outlive one week.
pub(crate) const MAX_PRESIGN_EXPIRES: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Clone)]
pub(crate) struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

/// A request to sign. `path` must already be URI-encoded (S3 encodes each key segment once);
/// query pairs and headers are raw values.
#[derive(Debug, Clone)]
pub(crate) struct SignableRequest {
    pub method: &'static str,
    /// `scheme://host[:port]`
    pub origin: String,
    /// `host[:port]` as sent in the Host header.
    pub host: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
}

pub(crate) struct Scope<'a> {
    pub region: &'a str,
    pub service: &'a str,
    pub time: SystemTime,
}

/// URI-encode per SigV4: every byte except the unreserved set, optionally keeping `/`.
pub(crate) fn uri_encode(value: &str, keep_slash: bool) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            b'/' if keep_slash => encoded.push('/'),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

pub(crate) fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// `(YYYYMMDD'T'HHMMSS'Z', YYYYMMDD)`.
pub(crate) fn amz_dates(time: SystemTime) -> (String, String) {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let (year, month, day, hour, minute, second) = civil_time(seconds);
    let date = format!("{year:04}{month:02}{day:02}");
    (format!("{date}T{hour:02}{minute:02}{second:02}Z"), date)
}

fn normalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs = query
        .iter()
        .map(|(key, value)| (uri_encode(key, false), uri_encode(value, false)))
        .collect::<Vec<_>>();
    pairs.sort();
    pairs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Canonical headers (always including `host`) and the signed-headers list.
fn canonical_headers(request: &SignableRequest) -> (String, String) {
    let mut headers = request
        .headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), normalize_header_value(value)))
        .filter(|(name, _)| name != "host")
        .collect::<Vec<_>>();
    headers.push(("host".to_string(), request.host.clone()));
    headers.sort();
    let canonical = headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let signed = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    (canonical, signed)
}

fn signature(
    credentials: &Credentials,
    scope: &Scope<'_>,
    amz_date: &str,
    date: &str,
    canonical_request: &str,
) -> String {
    let credential_scope = format!("{date}/{}/{}/aws4_request", scope.region, scope.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let date_key = hmac(
        format!("AWS4{}", credentials.secret_access_key).as_bytes(),
        date.as_bytes(),
    );
    let region_key = hmac(&date_key, scope.region.as_bytes());
    let service_key = hmac(&region_key, scope.service.as_bytes());
    let signing_key = hmac(&service_key, b"aws4_request");
    hex(&hmac(&signing_key, string_to_sign.as_bytes()))
}

fn render_query(query: &[(String, String)]) -> String {
    query
        .iter()
        .map(|(key, value)| {
            if value.is_empty() {
                uri_encode(key, false)
            } else {
                format!("{}={}", uri_encode(key, false), uri_encode(value, false))
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// Presign `request` with query-string authentication. Every header in `request.headers` is
/// signed and must be sent unchanged with the URL.
pub(crate) fn presign(
    request: &SignableRequest,
    credentials: &Credentials,
    scope: &Scope<'_>,
    expires_in: Duration,
    include_content_sha256: bool,
) -> Result<String, BeamApiError> {
    if expires_in > MAX_PRESIGN_EXPIRES {
        return Err(BeamApiError::InvalidArgument(
            "SigV4 presigned URLs must expire within one week".to_string(),
        ));
    }
    let (amz_date, date) = amz_dates(scope.time);
    let (canonical_headers, signed_headers) = canonical_headers(request);
    let mut query = request.query.clone();
    query.push((
        "X-Amz-Algorithm".to_string(),
        "AWS4-HMAC-SHA256".to_string(),
    ));
    if include_content_sha256 {
        query.push((
            "X-Amz-Content-Sha256".to_string(),
            UNSIGNED_PAYLOAD.to_string(),
        ));
    }
    query.push((
        "X-Amz-Credential".to_string(),
        format!(
            "{}/{date}/{}/{}/aws4_request",
            credentials.access_key_id, scope.region, scope.service
        ),
    ));
    query.push(("X-Amz-Date".to_string(), amz_date.clone()));
    query.push((
        "X-Amz-Expires".to_string(),
        expires_in.as_secs().max(1).to_string(),
    ));
    if let Some(token) = credentials
        .session_token
        .as_deref()
        .filter(|token| !token.is_empty())
    {
        query.push(("X-Amz-Security-Token".to_string(), token.to_string()));
    }
    query.push(("X-Amz-SignedHeaders".to_string(), signed_headers.clone()));
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        request.path,
        canonical_query(&query),
        canonical_headers,
        signed_headers,
        UNSIGNED_PAYLOAD
    );
    let signature = signature(credentials, scope, &amz_date, &date, &canonical_request);
    query.push(("X-Amz-Signature".to_string(), signature));
    Ok(format!(
        "{}{}?{}",
        request.origin,
        request.path,
        render_query(&query)
    ))
}

/// Sign `request` with an `Authorization` header. Returns the headers to send (the request's
/// own headers plus `x-amz-date`, `x-amz-content-sha256`, the session token and
/// `authorization`) and the URL.
pub(crate) fn sign_headers(
    request: &SignableRequest,
    credentials: &Credentials,
    scope: &Scope<'_>,
    payload_sha256: &str,
) -> (String, Vec<(String, String)>) {
    let (amz_date, date) = amz_dates(scope.time);
    let mut signed_request = request.clone();
    signed_request
        .headers
        .push(("x-amz-date".to_string(), amz_date.clone()));
    signed_request.headers.push((
        "x-amz-content-sha256".to_string(),
        payload_sha256.to_string(),
    ));
    if let Some(token) = credentials
        .session_token
        .as_deref()
        .filter(|token| !token.is_empty())
    {
        signed_request
            .headers
            .push(("x-amz-security-token".to_string(), token.to_string()));
    }
    let (canonical_headers, signed_headers) = canonical_headers(&signed_request);
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        request.path,
        canonical_query(&request.query),
        canonical_headers,
        signed_headers,
        payload_sha256
    );
    let signature = signature(credentials, scope, &amz_date, &date, &canonical_request);
    let (_, credential_date) = amz_dates(scope.time);
    let mut headers = signed_request.headers;
    headers.push((
        "authorization".to_string(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{credential_date}/{}/{}/aws4_request, SignedHeaders={signed_headers}, Signature={signature}",
            credentials.access_key_id, scope.region, scope.service
        ),
    ));
    let url = if request.query.is_empty() {
        format!("{}{}", request.origin, request.path)
    } else {
        format!(
            "{}{}?{}",
            request.origin,
            request.path,
            render_query(&request.query)
        )
    };
    (url, headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(amz: &str) -> SystemTime {
        // Parse YYYYMMDDTHHMMSSZ for the fixtures.
        let year: i64 = amz[0..4].parse().unwrap();
        let month: i64 = amz[4..6].parse().unwrap();
        let day: i64 = amz[6..8].parse().unwrap();
        let hour: u64 = amz[9..11].parse().unwrap();
        let minute: u64 = amz[11..13].parse().unwrap();
        let second: u64 = amz[13..15].parse().unwrap();
        // Days from civil (Howard Hinnant).
        let y = if month <= 2 { year - 1 } else { year };
        let era = y.div_euclid(400);
        let yoe = y - era * 400;
        let mp = (month + 9) % 12;
        let doy = (153 * mp + 2) / 5 + day - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let days = (era * 146_097 + doe - 719_468) as u64;
        UNIX_EPOCH + Duration::from_secs(days * 86_400 + hour * 3_600 + minute * 60 + second)
    }

    fn aws_example_credentials() -> Credentials {
        Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
        }
    }

    fn example_bucket(method: &'static str, path: &str) -> SignableRequest {
        SignableRequest {
            method,
            origin: "https://examplebucket.s3.amazonaws.com".to_string(),
            host: "examplebucket.s3.amazonaws.com".to_string(),
            path: path.to_string(),
            query: Vec::new(),
            headers: Vec::new(),
        }
    }

    fn authorization(headers: &[(String, String)]) -> String {
        headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .map(|(_, value)| value.clone())
            .unwrap()
    }

    #[test]
    fn dates_are_formatted_for_the_credential_scope() {
        assert_eq!(
            amz_dates(at("20130524T000000Z")),
            ("20130524T000000Z".to_string(), "20130524".to_string())
        );
        assert_eq!(amz_dates(at("20150830T123600Z")).0, "20150830T123600Z");
    }

    /// AWS S3 documentation: "Example: Presigned URL" (query-string authentication).
    #[test]
    fn matches_the_aws_s3_presigned_url_example() {
        let url = presign(
            &example_bucket("GET", "/test.txt"),
            &aws_example_credentials(),
            &Scope {
                region: "us-east-1",
                service: "s3",
                time: at("20130524T000000Z"),
            },
            Duration::from_secs(86_400),
            false,
        )
        .unwrap();
        assert_eq!(
            url,
            "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256\
             &X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\
             &X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
    }

    /// AWS S3 documentation: "Example: GET Object" (header authentication with Range).
    #[test]
    fn matches_the_aws_s3_get_object_example() {
        let mut request = example_bucket("GET", "/test.txt");
        request
            .headers
            .push(("range".to_string(), "bytes=0-9".to_string()));
        let (_, headers) = sign_headers(
            &request,
            &aws_example_credentials(),
            &Scope {
                region: "us-east-1",
                service: "s3",
                time: at("20130524T000000Z"),
            },
            EMPTY_PAYLOAD_SHA256,
        );
        assert_eq!(
            authorization(&headers),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// AWS S3 documentation: "Example: PUT Object".
    #[test]
    fn matches_the_aws_s3_put_object_example() {
        let mut request =
            example_bucket("PUT", &format!("/{}", uri_encode("test$file.text", true)));
        request.headers.push((
            "date".to_string(),
            "Fri, 24 May 2013 00:00:00 GMT".to_string(),
        ));
        request.headers.push((
            "x-amz-storage-class".to_string(),
            "REDUCED_REDUNDANCY".to_string(),
        ));
        let (_, headers) = sign_headers(
            &request,
            &aws_example_credentials(),
            &Scope {
                region: "us-east-1",
                service: "s3",
                time: at("20130524T000000Z"),
            },
            &sha256_hex(b"Welcome to Amazon S3."),
        );
        assert_eq!(
            authorization(&headers),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class, \
             Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
    }

    /// AWS S3 documentation: "Example: GET Bucket (List Objects)".
    #[test]
    fn matches_the_aws_s3_list_objects_example() {
        let mut request = example_bucket("GET", "/");
        request.query = vec![
            ("max-keys".to_string(), "2".to_string()),
            ("prefix".to_string(), "J".to_string()),
        ];
        let (url, headers) = sign_headers(
            &request,
            &aws_example_credentials(),
            &Scope {
                region: "us-east-1",
                service: "s3",
                time: at("20130524T000000Z"),
            },
            EMPTY_PAYLOAD_SHA256,
        );
        assert_eq!(
            url,
            "https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J"
        );
        assert!(authorization(&headers).ends_with(
            "Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        ));
    }

    /// AWS SigV4 test suite: `get-vanilla`.
    #[test]
    fn matches_the_sigv4_test_suite_get_vanilla() {
        let request = SignableRequest {
            method: "GET",
            origin: "https://example.amazonaws.com".to_string(),
            host: "example.amazonaws.com".to_string(),
            path: "/".to_string(),
            query: Vec::new(),
            headers: Vec::new(),
        };
        let credentials = Credentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
        };
        let scope = Scope {
            region: "us-east-1",
            service: "service",
            time: at("20150830T123600Z"),
        };
        let (amz_date, date) = amz_dates(scope.time);
        let canonical_request = format!(
            "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:{amz_date}\n\nhost;x-amz-date\n{EMPTY_PAYLOAD_SHA256}"
        );
        assert_eq!(
            signature(&credentials, &scope, &amz_date, &date, &canonical_request),
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
        let _ = request;
    }

    #[test]
    fn presigned_urls_sign_extra_headers_and_the_session_token() {
        let mut request = example_bucket("GET", "/test.txt");
        request
            .headers
            .push(("If-Match".to_string(), "\"etag\"".to_string()));
        let credentials = Credentials {
            session_token: Some("session/token".to_string()),
            ..aws_example_credentials()
        };
        let url = presign(
            &request,
            &credentials,
            &Scope {
                region: "auto",
                service: "s3",
                time: at("20130524T000000Z"),
            },
            Duration::from_secs(60),
            true,
        )
        .unwrap();
        assert!(url.contains("X-Amz-SignedHeaders=host%3Bif-match"));
        assert!(url.contains("X-Amz-Security-Token=session%2Ftoken"));
        assert!(url.contains("X-Amz-Content-Sha256=UNSIGNED-PAYLOAD"));
        assert!(url.contains("%2Fauto%2Fs3%2Faws4_request"));
        assert!(presign(
            &request,
            &credentials,
            &Scope {
                region: "auto",
                service: "s3",
                time: at("20130524T000000Z"),
            },
            MAX_PRESIGN_EXPIRES + Duration::from_secs(1),
            true,
        )
        .is_err());
    }
}
