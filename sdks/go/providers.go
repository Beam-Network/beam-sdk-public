package beamnetworksdk

import (
	"fmt"
	"strings"
)

// The constructors below validate provider configs the way the TypeScript
// <Provider>Config.create helpers do and set the provider name. Configs built
// as struct literals keep working; the constructors only add validation.

// HuggingFaceRepoTypes are the repo types a Hugging Face config accepts.
var HuggingFaceRepoTypes = []string{"model", "dataset", "space", "kernel", "bucket"}

type requiredField struct {
	name  string
	value string
}

func requireFields(provider string, fields ...requiredField) error {
	for _, field := range fields {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%s config requires %s.", provider, field.name)
		}
	}
	return nil
}

func s3Fields(bucket, key, accessKeyID, secretAccessKey string) []requiredField {
	return []requiredField{{"bucket", bucket}, {"key", key}, {"access_key_id", accessKeyID}, {"secret_access_key", secretAccessKey}}
}

// NewS3ProviderSource validates an AWS S3 source.
func NewS3ProviderSource(config S3ProviderSource) (S3ProviderSource, error) {
	config.Provider = "s3"
	return config, requireFields("s3", s3Fields(config.Bucket, config.Key, config.AccessKeyID, config.SecretAccessKey)...)
}

// NewS3ProviderDestination validates an AWS S3 destination.
func NewS3ProviderDestination(config S3ProviderDestination) (S3ProviderDestination, error) {
	config.Provider = "s3"
	return config, requireFields("s3", s3Fields(config.Bucket, config.Key, config.AccessKeyID, config.SecretAccessKey)...)
}

func validateR2(bucket, key, accessKeyID, secretAccessKey, accountID, endpointURL string) error {
	if err := requireFields("r2", s3Fields(bucket, key, accessKeyID, secretAccessKey)...); err != nil {
		return err
	}
	if strings.TrimSpace(accountID) == "" && strings.TrimSpace(endpointURL) == "" {
		return fmt.Errorf("r2 config requires account_id or endpoint_url.")
	}
	return nil
}

// NewR2ProviderSource validates a Cloudflare R2 source.
func NewR2ProviderSource(config R2ProviderSource) (R2ProviderSource, error) {
	config.Provider = "r2"
	return config, validateR2(config.Bucket, config.Key, config.AccessKeyID, config.SecretAccessKey, config.AccountID, config.EndpointURL)
}

// NewR2ProviderDestination validates a Cloudflare R2 destination.
func NewR2ProviderDestination(config R2ProviderDestination) (R2ProviderDestination, error) {
	config.Provider = "r2"
	return config, validateR2(config.Bucket, config.Key, config.AccessKeyID, config.SecretAccessKey, config.AccountID, config.EndpointURL)
}

func validateS3Compatible(provider, bucket, key, accessKeyID, secretAccessKey, endpointURL, accountID string) (string, error) {
	label := strings.TrimSpace(provider)
	if label == "" {
		label = "s3-compatible"
	}
	if err := requireFields(label, append([]requiredField{{"provider", provider}}, s3Fields(bucket, key, accessKeyID, secretAccessKey)...)...); err != nil {
		return "", err
	}
	normalized := strings.ToLower(strings.TrimSpace(provider))
	if normalized == "r2" && strings.TrimSpace(endpointURL) == "" && strings.TrimSpace(accountID) == "" {
		return "", fmt.Errorf("r2 config requires account_id or endpoint_url.")
	}
	if normalized != "s3" && normalized != "r2" && strings.TrimSpace(endpointURL) == "" {
		return "", fmt.Errorf("%s config requires endpoint_url.", normalized)
	}
	return normalized, nil
}

// NewS3CompatibleProviderSource validates an S3-compatible source, normalizes
// its provider name, and sets Driver to "s3-compatible".
func NewS3CompatibleProviderSource(config S3CompatibleProviderSource) (S3CompatibleProviderSource, error) {
	provider, err := validateS3Compatible(config.Provider, config.Bucket, config.Key, config.AccessKeyID, config.SecretAccessKey, config.EndpointURL, config.AccountID)
	if err != nil {
		return config, err
	}
	config.Provider, config.Driver = provider, "s3-compatible"
	return config, nil
}

// NewS3CompatibleProviderDestination validates an S3-compatible destination,
// normalizes its provider name, and sets Driver to "s3-compatible".
func NewS3CompatibleProviderDestination(config S3CompatibleProviderDestination) (S3CompatibleProviderDestination, error) {
	provider, err := validateS3Compatible(config.Provider, config.Bucket, config.Key, config.AccessKeyID, config.SecretAccessKey, config.EndpointURL, config.AccountID)
	if err != nil {
		return config, err
	}
	config.Provider, config.Driver = provider, "s3-compatible"
	return config, nil
}

// NewHippiusProviderSource validates a Hippius source.
func NewHippiusProviderSource(config HippiusProviderSource) (HippiusProviderSource, error) {
	config.Provider = "hippius"
	return config, requireFields("hippius", requiredField{"bucket", config.Bucket}, requiredField{"key", config.Key}, requiredField{"api_token", config.APIToken})
}

// NewHippiusProviderDestination validates a Hippius destination.
func NewHippiusProviderDestination(config HippiusProviderDestination) (HippiusProviderDestination, error) {
	config.Provider = "hippius"
	return config, requireFields("hippius", requiredField{"bucket", config.Bucket}, requiredField{"key", config.Key}, requiredField{"api_token", config.APIToken})
}

func validateHuggingFace(repoID, path, token, repoType string) error {
	if err := requireFields("huggingface", requiredField{"repo_id", repoID}, requiredField{"path", path}, requiredField{"token", token}); err != nil {
		return err
	}
	if repoType != "" {
		known := false
		for _, candidate := range HuggingFaceRepoTypes {
			known = known || candidate == repoType
		}
		if !known {
			return fmt.Errorf("huggingface config repo_type must be one of %s.", strings.Join(HuggingFaceRepoTypes, ", "))
		}
	}
	if !strings.Contains(repoID, "/") {
		return fmt.Errorf("huggingface config repo_id must be `namespace/name`.")
	}
	return nil
}

// NewHuggingFaceProviderSource validates a Hugging Face Hub source.
func NewHuggingFaceProviderSource(config HuggingFaceProviderSource) (HuggingFaceProviderSource, error) {
	config.Provider = "huggingface"
	return config, validateHuggingFace(config.RepoID, config.Path, config.Token, config.RepoType)
}

// NewHuggingFaceProviderDestination validates a Hugging Face Hub destination.
func NewHuggingFaceProviderDestination(config HuggingFaceProviderDestination) (HuggingFaceProviderDestination, error) {
	config.Provider = "huggingface"
	return config, validateHuggingFace(config.RepoID, config.Path, config.Token, config.RepoType)
}
