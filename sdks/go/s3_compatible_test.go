package beamnetworksdk

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// sigV4URIEncode is the SigV4 UriEncode: every byte except the unreserved
// characters is percent-encoded with uppercase hex; "/" is kept in paths.
func sigV4URIEncode(value string, encodeSlash bool) string {
	var builder strings.Builder
	for _, b := range []byte(value) {
		switch {
		case b >= 'A' && b <= 'Z', b >= 'a' && b <= 'z', b >= '0' && b <= '9', b == '-', b == '_', b == '.', b == '~':
			builder.WriteByte(b)
		case b == '/' && !encodeSlash:
			builder.WriteByte(b)
		default:
			fmt.Fprintf(&builder, "%%%02X", b)
		}
	}
	return builder.String()
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

// verifyS3PresignedURL recomputes the signature the way S3 does: it decodes
// the received path and re-encodes it once with UriEncode for the canonical URI.
func verifyS3PresignedURL(t *testing.T, method string, rawURL string, secretAccessKey string) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.EscapedPath() != sigV4URIEncode(parsed.Path, false) {
		t.Errorf("%s path %q is not SigV4-encoded (want %q)", method, parsed.EscapedPath(), sigV4URIEncode(parsed.Path, false))
	}
	query := parsed.Query()
	signature := query.Get("X-Amz-Signature")
	query.Del("X-Amz-Signature")
	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		for _, value := range query[key] {
			pairs = append(pairs, sigV4URIEncode(key, true)+"="+sigV4URIEncode(value, true))
		}
	}
	if query.Get("X-Amz-SignedHeaders") != "host" {
		t.Fatalf("signed headers = %q", query.Get("X-Amz-SignedHeaders"))
	}
	canonicalRequest := strings.Join([]string{
		method, sigV4URIEncode(parsed.Path, false), strings.Join(pairs, "&"),
		"host:" + parsed.Host + "\n", "host", "UNSIGNED-PAYLOAD",
	}, "\n")
	credential := strings.SplitN(query.Get("X-Amz-Credential"), "/", 2)
	scope := credential[1]
	scopeParts := strings.Split(scope, "/")
	digest := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + query.Get("X-Amz-Date") + "\n" + scope + "\n" + hex.EncodeToString(digest[:])
	signingKey := hmacSHA256([]byte("AWS4"+secretAccessKey), scopeParts[0])
	signingKey = hmacSHA256(signingKey, scopeParts[1])
	signingKey = hmacSHA256(signingKey, scopeParts[2])
	signingKey = hmacSHA256(signingKey, "aws4_request")
	if expected := hex.EncodeToString(hmacSHA256(signingKey, stringToSign)); expected != signature {
		t.Errorf("%s %s: S3 would reject the signature", method, parsed.EscapedPath())
	}
}

var s3SpecialKeys = []string{"dir/a+b=c@d,e:f g ü.bin", "plain/key.bin", "x/!$&'()*;~[]"}

// Regression: url.PathEscape left + = @ , : unescaped while the signer treated
// the path as canonical, so S3 (which re-encodes them) rejected the signature.
func TestSelfPresignedMultipartControlsMatchSigV4CanonicalURI(t *testing.T) {
	ctx := context.Background()
	for _, pathStyle := range []bool{false, true} {
		destination := S3ProviderDestination{
			Bucket: "beam-bucket", Region: "us-east-1", AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
			ForcePathStyle: aws.Bool(pathStyle),
		}
		for _, key := range s3SpecialKeys {
			complete, err := signCompleteMultipartUpload(ctx, destination, key, "upload+id=1", time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			abort, err := signAbortMultipartUpload(ctx, destination, key, "upload+id=1", time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			list, err := signListMultipartUpload(ctx, destination, key, "upload+id=1", time.Hour, 1000, 1000)
			if err != nil {
				t.Fatal(err)
			}
			verifyS3PresignedURL(t, http.MethodPost, complete, destination.SecretAccessKey)
			verifyS3PresignedURL(t, http.MethodDelete, abort, destination.SecretAccessKey)
			verifyS3PresignedURL(t, http.MethodGet, list, destination.SecretAccessKey)
		}
	}
}

// The self-presigner produces the same URL and signature as the AWS SDK
// presigner for an operation both can sign.
func TestSelfPresignerMatchesAWSPresignerForSpecialKeys(t *testing.T) {
	ctx := context.Background()
	settings := s3Settings{provider: "s3", bucket: "beam-bucket", region: "us-east-1", accessKeyID: "AKIDEXAMPLE", secretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}
	presigner := s3.NewPresignClient(s3ClientFor(settings))
	for _, key := range s3SpecialKeys {
		matched := false
		for attempt := 0; attempt < 5 && !matched; attempt++ {
			ours, err := presignS3Request(ctx, settings, http.MethodGet, key, "GetObject", time.Hour, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			reference, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(settings.bucket), Key: aws.String(key)}, func(options *s3.PresignOptions) { options.Expires = time.Hour })
			if err != nil {
				t.Fatal(err)
			}
			oursURL, _ := url.Parse(ours)
			referenceURL, _ := url.Parse(reference.URL)
			if oursURL.Query().Get("X-Amz-Date") != referenceURL.Query().Get("X-Amz-Date") {
				continue // signed across a second boundary
			}
			matched = true
			if oursURL.EscapedPath() != referenceURL.EscapedPath() || oursURL.Query().Get("X-Amz-Signature") != referenceURL.Query().Get("X-Amz-Signature") {
				t.Errorf("key %q:\n ours      %s\n aws-sdk   %s", key, ours, reference.URL)
			}
		}
		if !matched {
			t.Fatal("could not sign both URLs within one second")
		}
	}
}
