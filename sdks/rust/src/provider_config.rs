//! Validating constructors for provider configs, mirroring the TypeScript SDK's
//! `*ProviderConfig.create()` helpers.
//!
//! Every config struct has public fields and `Default`, so it can be built directly; `create`
//! returns the config unchanged after checking it, and `validate` checks an existing value.

use crate::error::BeamApiError;
use crate::{
    AzureProviderDestination, AzureProviderSource, GCSProviderDestination, GCSProviderSource,
    HippiusProviderDestination, HippiusProviderSource, HuggingFaceProviderDestination,
    HuggingFaceProviderSource, ProviderDestinationConfig, ProviderSourceConfig,
    R2ProviderDestination, R2ProviderSource, S3CompatibleProviderDestination,
    S3CompatibleProviderSource, S3ProviderDestination, S3ProviderSource, S3_COMPATIBLE_DRIVER,
};

/// Repo types the Hugging Face Hub serves.
pub const HUGGINGFACE_REPO_TYPES: &[&str] = &["model", "dataset", "space", "kernel", "bucket"];

pub(crate) fn has_text(value: &str) -> bool {
    !value.trim().is_empty()
}

fn optional_has_text(value: Option<&str>) -> bool {
    value.is_some_and(has_text)
}

pub(crate) fn require(provider: &str, field: &str, value: &str) -> Result<(), BeamApiError> {
    if has_text(value) {
        Ok(())
    } else {
        Err(BeamApiError::InvalidArgument(format!(
            "{provider} config requires {field}."
        )))
    }
}

fn require_r2_endpoint(
    account_id: Option<&str>,
    endpoint_url: Option<&str>,
) -> Result<(), BeamApiError> {
    if optional_has_text(account_id) || optional_has_text(endpoint_url) {
        Ok(())
    } else {
        Err(BeamApiError::InvalidArgument(
            "r2 config requires account_id or endpoint_url.".to_string(),
        ))
    }
}

fn validate_huggingface(
    repo_id: &str,
    path: &str,
    token: &str,
    repo_type: Option<&str>,
) -> Result<(), BeamApiError> {
    require("huggingface", "repo_id", repo_id)?;
    require("huggingface", "path", path)?;
    require("huggingface", "token", token)?;
    if let Some(repo_type) = repo_type {
        if !HUGGINGFACE_REPO_TYPES.contains(&repo_type) {
            return Err(BeamApiError::InvalidArgument(format!(
                "huggingface config repo_type must be one of {}.",
                HUGGINGFACE_REPO_TYPES.join(", ")
            )));
        }
    }
    if !repo_id.contains('/') {
        return Err(BeamApiError::InvalidArgument(
            "huggingface config repo_id must be `namespace/name`.".to_string(),
        ));
    }
    Ok(())
}

macro_rules! validating_constructor {
    ($type:ty, |$config:ident| $body:block) => {
        impl $type {
            /// Check required fields and provider rules.
            pub fn validate(&self) -> Result<(), BeamApiError> {
                let $config = self;
                $body
            }

            /// Validate and return the config, like the TypeScript SDK's `create()`.
            pub fn create(self) -> Result<Self, BeamApiError> {
                self.validate()?;
                Ok(self)
            }
        }
    };
}

validating_constructor!(S3ProviderSource, |config| {
    require("s3", "bucket", &config.bucket)?;
    require("s3", "key", &config.key)?;
    require("s3", "access_key_id", &config.access_key_id)?;
    require("s3", "secret_access_key", &config.secret_access_key)
});

validating_constructor!(S3ProviderDestination, |config| {
    require("s3", "bucket", &config.bucket)?;
    require("s3", "key", &config.key)?;
    require("s3", "access_key_id", &config.access_key_id)?;
    require("s3", "secret_access_key", &config.secret_access_key)
});

validating_constructor!(R2ProviderSource, |config| {
    require("r2", "bucket", &config.bucket)?;
    require("r2", "key", &config.key)?;
    require("r2", "access_key_id", &config.access_key_id)?;
    require("r2", "secret_access_key", &config.secret_access_key)?;
    require_r2_endpoint(config.account_id.as_deref(), config.endpoint_url.as_deref())
});

validating_constructor!(R2ProviderDestination, |config| {
    require("r2", "bucket", &config.bucket)?;
    require("r2", "key", &config.key)?;
    require("r2", "access_key_id", &config.access_key_id)?;
    require("r2", "secret_access_key", &config.secret_access_key)?;
    require_r2_endpoint(config.account_id.as_deref(), config.endpoint_url.as_deref())
});

validating_constructor!(GCSProviderSource, |config| {
    require("gcs", "bucket", &config.bucket)?;
    require("gcs", "key", &config.key)
});

validating_constructor!(GCSProviderDestination, |config| {
    require("gcs", "bucket", &config.bucket)?;
    require("gcs", "key", &config.key)
});

validating_constructor!(AzureProviderSource, |config| {
    require("azure", "container", &config.container)?;
    require("azure", "blob", &config.blob)?;
    require("azure", "account_name", &config.account_name)
});

validating_constructor!(AzureProviderDestination, |config| {
    require("azure", "container", &config.container)?;
    require("azure", "blob", &config.blob)?;
    require("azure", "account_name", &config.account_name)
});

validating_constructor!(HippiusProviderSource, |config| {
    require("hippius", "bucket", &config.bucket)?;
    require("hippius", "key", &config.key)?;
    require("hippius", "api_token", &config.api_token)
});

validating_constructor!(HippiusProviderDestination, |config| {
    require("hippius", "bucket", &config.bucket)?;
    require("hippius", "key", &config.key)?;
    require("hippius", "api_token", &config.api_token)
});

validating_constructor!(HuggingFaceProviderSource, |config| {
    validate_huggingface(
        &config.repo_id,
        &config.path,
        &config.token,
        config.repo_type.as_deref(),
    )
});

validating_constructor!(HuggingFaceProviderDestination, |config| {
    validate_huggingface(
        &config.repo_id,
        &config.path,
        &config.token,
        config.repo_type.as_deref(),
    )
});

fn validate_s3_compatible(
    provider: &str,
    bucket: &str,
    key: &str,
    access_key_id: &str,
    secret_access_key: &str,
    account_id: Option<&str>,
    endpoint_url: Option<&str>,
) -> Result<(), BeamApiError> {
    let label = if has_text(provider) {
        provider
    } else {
        S3_COMPATIBLE_DRIVER
    };
    require(label, "provider", provider)?;
    require(label, "bucket", bucket)?;
    require(label, "key", key)?;
    require(label, "access_key_id", access_key_id)?;
    require(label, "secret_access_key", secret_access_key)?;
    let provider = provider.trim().to_ascii_lowercase();
    if provider == "r2" {
        require_r2_endpoint(account_id, endpoint_url)?;
    }
    if provider != "s3" && provider != "r2" && !optional_has_text(endpoint_url) {
        return Err(BeamApiError::InvalidArgument(format!(
            "{provider} config requires endpoint_url."
        )));
    }
    Ok(())
}

macro_rules! s3_compatible_constructor {
    ($type:ty) => {
        impl $type {
            /// Check required fields and the endpoint rules for the named provider.
            pub fn validate(&self) -> Result<(), BeamApiError> {
                validate_s3_compatible(
                    &self.provider,
                    &self.bucket,
                    &self.key,
                    &self.access_key_id,
                    &self.secret_access_key,
                    self.account_id.as_deref(),
                    self.endpoint_url.as_deref(),
                )
            }

            /// Validate, lower-case the provider name and set `driver: "s3-compatible"`, like the
            /// TypeScript SDK's `S3CompatibleProviderConfig.create()`.
            pub fn create(mut self) -> Result<Self, BeamApiError> {
                self.validate()?;
                self.provider = self.provider.trim().to_ascii_lowercase();
                self.driver = S3_COMPATIBLE_DRIVER.to_string();
                Ok(self)
            }
        }
    };
}

s3_compatible_constructor!(S3CompatibleProviderSource);
s3_compatible_constructor!(S3CompatibleProviderDestination);

impl ProviderSourceConfig {
    /// Validate the wrapped config.
    pub fn validate(&self) -> Result<(), BeamApiError> {
        match self {
            ProviderSourceConfig::S3(config) => config.validate(),
            ProviderSourceConfig::R2(config) => config.validate(),
            ProviderSourceConfig::S3Compatible(config) => config.validate(),
            ProviderSourceConfig::GCS(config) => config.validate(),
            ProviderSourceConfig::Azure(config) => config.validate(),
            ProviderSourceConfig::Hippius(config) => config.validate(),
            ProviderSourceConfig::HuggingFace(config) => config.validate(),
        }
    }
}

impl ProviderDestinationConfig {
    /// Validate the wrapped config.
    pub fn validate(&self) -> Result<(), BeamApiError> {
        match self {
            ProviderDestinationConfig::S3(config) => config.validate(),
            ProviderDestinationConfig::R2(config) => config.validate(),
            ProviderDestinationConfig::S3Compatible(config) => config.validate(),
            ProviderDestinationConfig::GCS(config) => config.validate(),
            ProviderDestinationConfig::Azure(config) => config.validate(),
            ProviderDestinationConfig::Hippius(config) => config.validate(),
            ProviderDestinationConfig::HuggingFace(config) => config.validate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(error: BeamApiError) -> String {
        error.to_string()
    }

    #[test]
    fn s3_and_r2_require_credentials_and_r2_requires_an_endpoint() {
        let s3 = S3ProviderSource {
            bucket: "bucket".into(),
            key: "key".into(),
            access_key_id: "ak".into(),
            secret_access_key: "sk".into(),
            ..Default::default()
        };
        assert!(s3.clone().create().is_ok());
        let missing = S3ProviderSource {
            secret_access_key: " ".into(),
            ..s3
        };
        assert!(message(missing.create().unwrap_err())
            .contains("s3 config requires secret_access_key."));
        let r2 = R2ProviderDestination {
            bucket: "bucket".into(),
            key: "key".into(),
            access_key_id: "ak".into(),
            secret_access_key: "sk".into(),
            ..Default::default()
        };
        assert!(message(r2.clone().create().unwrap_err()).contains("account_id or endpoint_url"));
        assert!(R2ProviderDestination {
            account_id: Some("account".into()),
            ..r2
        }
        .create()
        .is_ok());
    }

    #[test]
    fn hippius_requires_its_api_token() {
        let error = HippiusProviderDestination {
            bucket: "bucket".into(),
            key: "key".into(),
            ..Default::default()
        }
        .create()
        .unwrap_err();
        assert!(message(error).contains("hippius config requires api_token."));
    }

    #[test]
    fn huggingface_validates_repo_id_and_repo_type() {
        let valid = HuggingFaceProviderSource {
            repo_id: "org/model".into(),
            path: "weights.bin".into(),
            token: "hf_token".into(),
            ..Default::default()
        };
        assert!(valid.clone().create().is_ok());
        let bad_repo = HuggingFaceProviderSource {
            repo_id: "model".into(),
            ..valid.clone()
        };
        assert!(message(bad_repo.create().unwrap_err()).contains("namespace/name"));
        let bad_type = HuggingFaceProviderSource {
            repo_type: Some("models".into()),
            ..valid
        };
        assert!(message(bad_type.create().unwrap_err())
            .contains("repo_type must be one of model, dataset, space, kernel, bucket."));
        assert!(
            ProviderDestinationConfig::HuggingFace(HuggingFaceProviderDestination {
                repo_id: "org/model".into(),
                path: "p".into(),
                token: "".into(),
                ..Default::default()
            })
            .validate()
            .is_err()
        );
    }

    #[test]
    fn s3_compatible_configs_normalize_and_require_endpoints() {
        let config = S3CompatibleProviderDestination {
            provider: " Wasabi ".into(),
            bucket: "bucket".into(),
            key: "file.bin".into(),
            access_key_id: "ak".into(),
            secret_access_key: "sk".into(),
            endpoint_url: Some("https://s3.wasabisys.com".into()),
            ..Default::default()
        }
        .create()
        .unwrap();
        assert_eq!(config.provider, "wasabi");
        assert_eq!(config.driver, "s3-compatible");
        let missing_endpoint = S3CompatibleProviderSource {
            provider: "minio".into(),
            bucket: "bucket".into(),
            key: "file.bin".into(),
            access_key_id: "ak".into(),
            secret_access_key: "sk".into(),
            ..Default::default()
        };
        assert!(message(missing_endpoint.clone().create().unwrap_err())
            .contains("minio config requires endpoint_url."));
        let r2 = S3CompatibleProviderSource {
            provider: "r2".into(),
            ..missing_endpoint.clone()
        };
        assert!(message(r2.create().unwrap_err()).contains("account_id or endpoint_url"));
        assert!(S3CompatibleProviderSource {
            provider: "s3".into(),
            ..missing_endpoint
        }
        .create()
        .is_ok());
    }

    #[test]
    fn s3_compatible_configs_round_trip_through_the_provider_enums() {
        let source: ProviderSourceConfig = serde_json::from_value(serde_json::json!({
            "provider": "wasabi", "driver": "s3-compatible", "bucket": "b", "key": "k",
            "access_key_id": "ak", "secret_access_key": "sk",
            "endpoint_url": "https://s3.wasabisys.com", "force_path_style": false
        }))
        .unwrap();
        let ProviderSourceConfig::S3Compatible(config) = &source else {
            panic!("expected an S3-compatible source: {source:?}");
        };
        assert_eq!(config.force_path_style, Some(false));
        let value = serde_json::to_value(&source).unwrap();
        assert_eq!(value["provider"], "wasabi");
        assert_eq!(value["driver"], "s3-compatible");
        // An unknown provider carrying S3 credentials is S3-compatible, as in TypeScript.
        let destination: ProviderDestinationConfig = serde_json::from_value(serde_json::json!({
            "provider": "minio", "bucket": "b", "key": "k", "access_key_id": "ak",
            "secret_access_key": "sk", "endpoint_url": "http://127.0.0.1:9000"
        }))
        .unwrap();
        assert_eq!(destination.provider_name(), "minio");
        let typed: ProviderDestinationConfig = serde_json::from_value(serde_json::json!({
            "provider": "s3", "bucket": "b", "key": "k", "access_key_id": "ak", "secret_access_key": "sk"
        }))
        .unwrap();
        assert!(matches!(typed, ProviderDestinationConfig::S3(_)));
        assert_eq!(serde_json::to_value(&typed).unwrap()["provider"], "s3");
        assert!(
            serde_json::from_value::<ProviderSourceConfig>(serde_json::json!({
                "provider": "ftp", "host": "example"
            }))
            .is_err()
        );
    }

    #[test]
    fn typescript_id_field_is_accepted() {
        let source: ProviderSourceConfig = serde_json::from_value(serde_json::json!({
            "provider": "hippius", "id": "custom-source", "bucket": "b", "key": "k", "api_token": "t"
        }))
        .unwrap();
        match source {
            ProviderSourceConfig::Hippius(source) => {
                assert_eq!(source.source_id.as_deref(), Some("custom-source"))
            }
            other => panic!("unexpected source: {other:?}"),
        }
    }
}
