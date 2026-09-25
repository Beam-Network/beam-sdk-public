"""Python client for BEAM transfer creation and management."""

from beam_network_sdk._client import BEAM_DEV_URL, BEAM_PROD_URL, BeamSDK
from beam_network_sdk._nats_control import TransferTerminalSignalWaiter
from beam_network_sdk.exceptions import (
    BeamAPIError,
    BeamAuthError,
    BeamError,
    BeamRouteRecoveryPendingError,
    BeamTimeoutError,
)
from beam_network_sdk.models import (
    AttachSignedUrlsResponse,
    AzureProviderDestination,
    AzureProviderSource,
    CallbackConfig,
    ChunkDestinationSigningTarget,
    ChunkSigningPlanItem,
    CompactTransferPlanDescriptor,
    CompactTransferPlanDestination,
    CompactTransferPlanFormulas,
    CompactTransferPlanSource,
    DestConfig,
    DestinationStatusInfo,
    DistributeResponse,
    GCSProviderDestination,
    GCSProviderSource,
    HippiusProviderDestination,
    HippiusProviderSource,
    MultipartGroupManifest,
    PreparedDestination,
    PreparedHttpSource,
    ProviderDestinationConfig,
    ProviderSourceConfig,
    R2ProviderDestination,
    R2ProviderSource,
    S3ProviderDestination,
    S3ProviderSource,
    SignedChunkRoute,
    SourceConfig,
    SourceStatusInfo,
    TransferCancelResponse,
    TransferCreateRequest,
    TransferCreateResponse,
    TransferPlanResponse,
    TransferPrepareResponse,
    TransferStatusInfo,
    TransferTerminalEvent,
)


def _resolve_version() -> str:
    """Report the installed distribution's version.

    The version is declared once, in pyproject.toml. Reading it back from
    package metadata keeps this module from drifting out of step with it, as a
    second hardcoded literal previously did.
    """
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("beam-network-sdk")
    except PackageNotFoundError:  # running from a source tree, not installed
        return "0.0.0.dev0"


__version__ = _resolve_version()

__all__ = [
    "BeamSDK",
    "BEAM_DEV_URL",
    "BEAM_PROD_URL",
    "SourceConfig",
    "DestConfig",
    "S3ProviderSource",
    "S3ProviderDestination",
    "R2ProviderSource",
    "R2ProviderDestination",
    "GCSProviderSource",
    "GCSProviderDestination",
    "AzureProviderSource",
    "AzureProviderDestination",
    "HippiusProviderSource",
    "HippiusProviderDestination",
    "MultipartGroupManifest",
    "ProviderSourceConfig",
    "ProviderDestinationConfig",
    "PreparedHttpSource",
    "PreparedDestination",
    "ChunkDestinationSigningTarget",
    "ChunkSigningPlanItem",
    "CompactTransferPlanDescriptor",
    "CompactTransferPlanDestination",
    "CompactTransferPlanFormulas",
    "CompactTransferPlanSource",
    "SignedChunkRoute",
    "CallbackConfig",
    "TransferCreateRequest",
    "TransferCreateResponse",
    "TransferPlanResponse",
    "TransferPrepareResponse",
    "TransferCancelResponse",
    "AttachSignedUrlsResponse",
    "DestinationStatusInfo",
    "SourceStatusInfo",
    "TransferStatusInfo",
    "TransferTerminalEvent",
    "TransferTerminalSignalWaiter",
    "DistributeResponse",
    "BeamError",
    "BeamAuthError",
    "BeamAPIError",
    "BeamRouteRecoveryPendingError",
    "BeamTimeoutError",
]
