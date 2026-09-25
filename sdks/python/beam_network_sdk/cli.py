"""CLI for BeamSDK transfers and cloud storage convenience commands."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

import click
import httpx

from beam_network_sdk import BEAM_DEV_URL, BEAM_PROD_URL, BeamSDK, __version__
from beam_network_sdk.models import (
    CallbackConfig,
    DestConfig,
    R2ProviderDestination,
    R2ProviderSource,
    S3ProviderDestination,
    S3ProviderSource,
    SourceConfig,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("beam_network_sdk.cli")


def _s3v4_config() -> Any:
    from botocore.config import Config  # type: ignore

    return Config(signature_version="s3v4")


def format_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


def status_icon(status: str) -> str:
    icons = {
        "completed": "[OK]",
        "in_progress": "[~]",
        "pending": "[...]",
        "failed": "[X]",
        "timeout": "[T]",
    }
    return icons.get(status, "[?]")


def build_s3_client_kwargs(
    aws_access_key: str,
    aws_secret_key: str,
    aws_region: str,
) -> dict[str, Any]:
    client_kwargs = {
        "aws_access_key_id": aws_access_key,
        "aws_secret_access_key": aws_secret_key,
        "region_name": aws_region,
        "config": _s3v4_config(),
    }
    endpoint_url = os.getenv("AWS_ENDPOINT_URL_S3") or os.getenv("AWS_ENDPOINT_URL")
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    return client_kwargs


def build_r2_client_kwargs(
    r2_access_key: str,
    r2_secret_key: str,
    r2_account_id: str | None,
    r2_endpoint_url: str | None,
) -> dict[str, Any]:
    if not r2_account_id and not r2_endpoint_url:
        raise click.BadParameter("--r2-account-id or --r2-endpoint-url is required")

    endpoint_url = r2_endpoint_url or f"https://{r2_account_id}.r2.cloudflarestorage.com"
    return {
        "aws_access_key_id": r2_access_key,
        "aws_secret_access_key": r2_secret_key,
        "region_name": "auto",
        "endpoint_url": endpoint_url,
        "config": _s3v4_config(),
    }


def print_transfer_table(transfers: list[dict[str, Any]], start_time: float | None = None) -> None:
    """Print a status-only transfer monitor."""
    import time
    from datetime import datetime, timezone

    if not transfers:
        click.echo("No transfers to display")
        return

    if sys.stdout.isatty():
        os.system("clear" if os.name != "nt" else "cls")

    width = 80
    border = "=" * width
    now = datetime.now()
    elapsed_str = format_duration(int(time.time() - start_time)) if start_time else "--"
    title = "  BEAM Transfer Monitor"
    ts_str = f"{now.strftime('%Y-%m-%d %H:%M:%S')}  |  {elapsed_str}"
    click.echo(border)
    click.echo(title + ts_str.rjust(width - len(title)))
    click.echo(border)

    def colored(text: str, status: str) -> str:
        if status == "completed":
            return click.style(text, fg="green")
        if status in ("failed", "cancelled"):
            return click.style(text, fg="red")
        return text

    for transfer in transfers:
        transfer_id = transfer.get("transfer_id", "unknown")
        status = transfer.get("status", "unknown")
        started_at = transfer.get("started_at")
        completed_at = transfer.get("completed_at")
        error_message = transfer.get("error_message")

        started_str = "--"
        running_str = "--"
        if started_at:
            try:
                started_dt = datetime.fromisoformat(str(started_at).replace("Z", "+00:00"))
                started_str = started_dt.strftime("%H:%M:%S")
                if completed_at:
                    completed_dt = datetime.fromisoformat(str(completed_at).replace("Z", "+00:00"))
                    running_str = format_duration(int((completed_dt - started_dt).total_seconds()))
                else:
                    running_str = format_duration(
                        int((datetime.now(timezone.utc) - started_dt).total_seconds())
                    )
            except Exception:
                pass

        status_display = colored(f"{status:<14}", status)
        click.echo("")
        click.echo(f"  Transfer   {transfer_id}")
        click.echo(
            f"  Status     {status_display}  Started  {started_str}    Running  {running_str}"
        )
        if completed_at:
            click.echo(f"  Completed  {completed_at}")
        if error_message:
            click.echo(f"  Error      {error_message}")

    click.echo("")
    click.echo(border)
    click.echo("  Refreshing every 2s  |  Ctrl+C to stop")
    click.echo(border)


async def monitor_transfers(
    transfer_ids: list[str],
    server: str,
    api_key: str,
    poll_interval: float = 2.0,
    timeout: float = 600.0,
) -> dict[str, Any]:
    """Monitor transfers with a live updating table."""
    import time

    start_time = time.time()
    transfers: list[dict[str, Any]] = []

    async with BeamSDK(api_key=api_key, nats_url=server) as beam:
        while time.time() - start_time < timeout:
            transfers = []
            all_complete = True

            for transfer_id in transfer_ids:
                try:
                    status = await beam.transfers.status(transfer_id)
                    status_data = status.model_dump()
                    transfers.append(status_data)
                    if status_data.get("status") not in ("completed", "failed", "cancelled"):
                        all_complete = False
                except Exception as exc:
                    transfers.append(
                        {
                            "transfer_id": transfer_id,
                            "status": "unknown",
                            "error_message": str(exc),
                            "started_at": None,
                            "completed_at": None,
                        }
                    )
                    all_complete = False

            print_transfer_table(transfers, start_time)

            if all_complete:
                return {"transfers": transfers, "complete": True}

            await asyncio.sleep(poll_interval)

    return {"transfers": transfers, "complete": False, "timeout": True}


def _parse_storage_url(url: str, scheme: str) -> tuple[str | None, str | None]:
    prefix = f"{scheme}://"
    if not url.startswith(prefix):
        return None, None
    parts = url[len(prefix) :].split("/", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (parts[0], "")


def _parse_s3_url(url: str) -> tuple[str | None, str | None]:
    return _parse_storage_url(url, "s3")


def _parse_r2_url(url: str) -> tuple[str | None, str | None]:
    return _parse_storage_url(url, "r2")


def _parse_gcs_url(url: str) -> tuple[str | None, str | None]:
    return _parse_storage_url(url, "gs")


def _parse_hippius_url(url: str) -> tuple[str | None, str | None]:
    return _parse_storage_url(url, "hippius")


#: Plural URI prefix -> canonical repo type, mirroring ``constants.HF_URI_TYPE_PREFIXES``.
_HF_URI_TYPE_PREFIXES = {
    "models": "model",
    "datasets": "dataset",
    "spaces": "space",
    "kernels": "kernel",
}

#: Revisions that contain a slash and so take precedence when splitting off the path.
_HF_SPECIAL_REVISION = re.compile(r"^refs/(?:convert/[\w.-]+|pr/\d+)")


def _parse_hf_uri(uri: str) -> tuple[str, str, str, str]:
    """Parse ``hf://[<TYPE>/]<ID>[@<REVISION>][/<PATH>]`` into repo type, id, revision, path."""
    if not uri.startswith("hf://"):
        raise click.BadParameter(
            f"Expected a Hugging Face URI (hf://[<type>/]<org>/<repo>[@<revision>]/<path>), got {uri!r}"
        )
    rest = uri[len("hf://") :]

    repo_type = "model"
    head = rest.split("/", 1)
    if len(head) == 2 and head[0] in _HF_URI_TYPE_PREFIXES:
        repo_type = _HF_URI_TYPE_PREFIXES[head[0]]
        rest = head[1]

    revision = "main"
    if "@" in rest:
        repo_id, remainder = rest.split("@", 1)
        special = _HF_SPECIAL_REVISION.match(remainder)
        if special:
            revision = special.group(0)
            path = remainder[len(revision) :].lstrip("/")
        else:
            revision, _, path = remainder.partition("/")
    else:
        segments = rest.split("/")
        if len(segments) < 3:
            raise click.BadParameter(
                f"Hugging Face URI {uri!r} is missing a file path inside the repo"
            )
        repo_id = "/".join(segments[:2])
        path = "/".join(segments[2:])

    if repo_id.count("/") != 1 or not all(repo_id.split("/")):
        raise click.BadParameter(f"Hugging Face URI {uri!r} must name the repo as <org>/<repo>")
    if not path:
        raise click.BadParameter(f"Hugging Face URI {uri!r} is missing a file path inside the repo")
    return repo_type, repo_id, revision, path


def _build_gcs_storage_client(project_id: str | None) -> tuple[Any, str]:
    try:
        import importlib

        google_auth_default = importlib.import_module("google.auth").default
        storage = importlib.import_module("google.cloud.storage")
    except ImportError as exc:
        raise RuntimeError(
            "google-cloud-storage is required for GCS transfers. Install beam-network-sdk[gcs]."
        ) from exc

    credentials, detected_project = google_auth_default(
        scopes=["https://www.googleapis.com/auth/devstorage.read_only"]
    )
    resolved_project = (
        project_id
        or detected_project
        or getattr(credentials, "quota_project_id", None)
        or os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCLOUD_PROJECT")
    )
    client = storage.Client(project=resolved_project, credentials=credentials)
    return client, resolved_project or ""


def _load_json(value: str) -> dict[str, object]:
    payload = Path(value[1:]).read_text(encoding="utf-8") if value.startswith("@") else value
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise click.BadParameter(f"Invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise click.BadParameter("Expected a JSON object")
    return {str(key): item for key, item in data.items()}


def _parse_kv_headers(values: list[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise click.BadParameter(f"Invalid header {item!r}; expected Key=Value")
        key, value = item.split("=", 1)
        headers[key.strip()] = value.strip()
    return headers


async def _build_sdk(server: str, api_key: str | None) -> BeamSDK:
    configured_environment = os.getenv("BEAM_ENV")
    environment = configured_environment or (
        "dev" if server.rstrip("/") == BEAM_DEV_URL.rstrip("/") else "prod"
    )
    return BeamSDK(api_key=api_key, nats_url=server, environment=environment)


def _print_json(data: object) -> None:
    click.echo(json.dumps(data, indent=2, sort_keys=True))


@click.group()
@click.version_option(version=__version__, prog_name="beam-send")
def cli() -> None:
    """Create and monitor BEAM transfers."""


@cli.command("s3-transfer")
@click.argument("source")
@click.argument("destination")
@click.option(
    "--server",
    "-s",
    default=BEAM_PROD_URL,
    envvar="BEAM_NATS_URL",
    show_default=True,
    help="Beam NATS URL",
)
@click.option("--api-key", envvar="BEAM_API_KEY", help="API key")
@click.option("--aws-access-key", envvar="AWS_ACCESS_KEY_ID", help="AWS Access Key ID")
@click.option("--aws-secret-key", envvar="AWS_SECRET_ACCESS_KEY", help="AWS Secret Access Key")
@click.option(
    "--aws-region",
    envvar="AWS_DEFAULT_REGION",
    default="us-east-1",
    show_default=True,
    help="AWS region",
)
@click.option("--chunk-size", default=None, type=int, help="Chunk size in bytes (default: auto)")
@click.option("--test-mode", is_flag=True, help="Run as a test-mode transfer")
@click.option("--verbose", "-v", is_flag=True)
def s3_transfer(
    source: str,
    destination: str,
    server: str,
    api_key: str | None,
    aws_access_key: str | None,
    aws_secret_key: str | None,
    aws_region: str,
    chunk_size: int | None,
    test_mode: bool,
    verbose: bool,
) -> None:
    """Transfer a single S3 file via BeamCore."""
    if verbose:
        logging.getLogger("beam_network_sdk").setLevel(logging.DEBUG)
    if not aws_access_key or not aws_secret_key:
        click.echo(
            "Error: AWS credentials required (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)", err=True
        )
        sys.exit(1)
    if not api_key:
        click.echo("Error: --api-key or BEAM_API_KEY required", err=True)
        sys.exit(1)

    asyncio.run(
        _s3_transfer(
            source=source,
            destination=destination,
            server=server,
            api_key=api_key,
            aws_access_key=aws_access_key,
            aws_secret_key=aws_secret_key,
            aws_region=aws_region,
            chunk_size=chunk_size,
            test_mode=test_mode,
            verbose=verbose,
        )
    )


@cli.command("r2-transfer")
@click.argument("source")
@click.argument("destination")
@click.option(
    "--server",
    "-s",
    default=BEAM_PROD_URL,
    envvar="BEAM_NATS_URL",
    show_default=True,
    help="Beam NATS URL",
)
@click.option("--api-key", envvar="BEAM_API_KEY", help="API key")
@click.option("--r2-access-key", envvar="R2_ACCESS_KEY_ID", help="Cloudflare R2 Access Key ID")
@click.option(
    "--r2-secret-key", envvar="R2_SECRET_ACCESS_KEY", help="Cloudflare R2 Secret Access Key"
)
@click.option("--r2-account-id", envvar="R2_ACCOUNT_ID", help="Cloudflare account ID")
@click.option("--r2-endpoint-url", envvar="R2_ENDPOINT_URL", help="Cloudflare R2 endpoint URL")
@click.option("--chunk-size", default=None, type=int, help="Chunk size in bytes (default: auto)")
@click.option("--test-mode", is_flag=True, help="Run as a test-mode transfer")
@click.option("--verbose", "-v", is_flag=True)
def r2_transfer(
    source: str,
    destination: str,
    server: str,
    api_key: str | None,
    r2_access_key: str | None,
    r2_secret_key: str | None,
    r2_account_id: str | None,
    r2_endpoint_url: str | None,
    chunk_size: int | None,
    test_mode: bool,
    verbose: bool,
) -> None:
    """Transfer a single Cloudflare R2 object via BeamCore."""
    if verbose:
        logging.getLogger("beam_network_sdk").setLevel(logging.DEBUG)
    if not r2_access_key or not r2_secret_key:
        click.echo(
            "Error: R2 credentials required (R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY)", err=True
        )
        sys.exit(1)
    if not api_key:
        click.echo("Error: --api-key or BEAM_API_KEY required", err=True)
        sys.exit(1)

    asyncio.run(
        _r2_transfer(
            source=source,
            destination=destination,
            server=server,
            api_key=api_key,
            r2_access_key=r2_access_key,
            r2_secret_key=r2_secret_key,
            r2_account_id=r2_account_id,
            r2_endpoint_url=r2_endpoint_url,
            chunk_size=chunk_size,
            test_mode=test_mode,
            verbose=verbose,
        )
    )


@cli.command("gcs-transfer")
@click.argument("source")
@click.argument("destination")
@click.option(
    "--server",
    "-s",
    default=BEAM_PROD_URL,
    envvar="BEAM_NATS_URL",
    show_default=True,
    help="Beam NATS URL",
)
@click.option("--api-key", envvar="BEAM_API_KEY", help="API key")
@click.option(
    "--gcp-project-id", envvar="GOOGLE_CLOUD_PROJECT", default=None, help="Google Cloud project ID"
)
@click.option("--chunk-size", default=None, type=int, help="Chunk size in bytes (default: auto)")
@click.option("--test-mode", is_flag=True, help="Run as a test-mode transfer")
@click.option("--verbose", "-v", is_flag=True)
def gcs_transfer(
    source: str,
    destination: str,
    server: str,
    api_key: str | None,
    gcp_project_id: str | None,
    chunk_size: int | None,
    test_mode: bool,
    verbose: bool,
) -> None:
    """Transfer a single GCS object via BeamCore."""
    if verbose:
        logging.getLogger("beam_network_sdk").setLevel(logging.DEBUG)
    if not api_key:
        click.echo("Error: --api-key or BEAM_API_KEY required", err=True)
        sys.exit(1)

    asyncio.run(
        _gcs_transfer(
            source=source,
            destination=destination,
            server=server,
            api_key=api_key,
            project_id=gcp_project_id,
            chunk_size=chunk_size,
            test_mode=test_mode,
            verbose=verbose,
        )
    )


async def _s3_transfer(
    source: str,
    destination: str,
    server: str,
    api_key: str,
    aws_access_key: str,
    aws_secret_key: str,
    aws_region: str,
    chunk_size: int | None,
    test_mode: bool,
    verbose: bool,
) -> None:
    click.echo("=" * 60)
    click.echo("BEAM S3 Transfer")
    click.echo("=" * 60)

    src_bucket, src_key = _parse_s3_url(source)
    if not src_bucket:
        click.echo("Error: Source must be an S3 URL (s3://bucket/key)", err=True)
        sys.exit(1)

    dst_bucket, dst_key = _parse_s3_url(destination)
    if not dst_bucket:
        click.echo("Error: Destination must be an S3 URL (s3://bucket/key)", err=True)
        sys.exit(1)

    click.echo(f"Source:      s3://{src_bucket}/{src_key}")
    click.echo(f"Destination: s3://{dst_bucket}/{dst_key}")
    click.echo(f"Beam NATS:   {server}")
    click.echo("")

    async with BeamSDK(api_key=api_key, nats_url=server) as beam:
        click.echo("Preparing transfer with SDK-side signing...")
        result = await beam.transfers.prepare_provider_transfer(
            sources=[
                S3ProviderSource(
                    bucket=src_bucket,
                    key=src_key,
                    region=aws_region,
                    access_key_id=aws_access_key,
                    secret_access_key=aws_secret_key,
                )
            ],
            destinations=[
                S3ProviderDestination(
                    bucket=dst_bucket,
                    key=dst_key,
                    region=aws_region,
                    access_key_id=aws_access_key,
                    secret_access_key=aws_secret_key,
                )
            ],
            test_mode=test_mode,
            chunk_size=chunk_size,
            expires_in=3600,
        )
        if not result.success:
            click.echo(f"Error creating transfer: {result.error}", err=True)
            sys.exit(1)

        click.echo(f"  Transfer ID: {result.transfer_id}")
        click.echo(f"  Size:        {result.total_size:,} bytes")
        click.echo(
            f"  Chunks:      {result.logical_chunks} logical / {result.total_chunks} delivery tasks"
        )
        click.echo("\nDistributing...")
        distribution = await beam.transfers.distribute(result.transfer_id)
        click.echo(f"  Orchestrators: {distribution.orchestrators_assigned}")
        click.echo("\n" + "=" * 60)
        click.echo("Transfer started. Monitoring...\n")

        try:
            monitor = await monitor_transfers(
                transfer_ids=[result.transfer_id],
                server=server,
                api_key=api_key,
                poll_interval=2.0,
                timeout=600.0,
            )
        except KeyboardInterrupt:
            click.echo("\nMonitoring cancelled.")
            return

        elapsed = monitor["elapsed"]
        if monitor["completed"] > 0:
            throughput = result.total_size / elapsed / (1024 * 1024) if elapsed > 0 else 0.0
            click.echo("\n" + "=" * 60)
            click.echo("TRANSFER COMPLETE!")
            click.echo(f"  Size:       {result.total_size:,} bytes")
            click.echo(f"  Time:       {elapsed:.1f}s")
            click.echo(f"  Throughput: {throughput:.2f} MB/s")
            click.echo("=" * 60)
        else:
            click.echo("Transfer failed or timed out.", err=True)
            sys.exit(1)


async def _r2_transfer(
    source: str,
    destination: str,
    server: str,
    api_key: str,
    r2_access_key: str,
    r2_secret_key: str,
    r2_account_id: str | None,
    r2_endpoint_url: str | None,
    chunk_size: int | None,
    test_mode: bool,
    verbose: bool,
) -> None:
    click.echo("=" * 60)
    click.echo("BEAM Cloudflare R2 Transfer")
    click.echo("=" * 60)

    src_bucket, src_key = _parse_r2_url(source)
    if not src_bucket:
        click.echo("Error: Source must be an R2 URL (r2://bucket/key)", err=True)
        sys.exit(1)

    dst_bucket, dst_key = _parse_r2_url(destination)
    if not dst_bucket:
        click.echo("Error: Destination must be an R2 URL (r2://bucket/key)", err=True)
        sys.exit(1)

    endpoint_url = r2_endpoint_url or f"https://{r2_account_id}.r2.cloudflarestorage.com"
    click.echo(f"Source:      r2://{src_bucket}/{src_key}")
    click.echo(f"Destination: r2://{dst_bucket}/{dst_key}")
    click.echo(f"Endpoint:    {endpoint_url}")
    click.echo(f"Beam NATS:   {server}")
    click.echo("")

    async with BeamSDK(api_key=api_key, nats_url=server) as beam:
        click.echo("Preparing transfer with SDK-side signing...")
        result = await beam.transfers.prepare_provider_transfer(
            sources=[
                R2ProviderSource(
                    bucket=src_bucket,
                    key=src_key,
                    account_id=r2_account_id,
                    endpoint_url=r2_endpoint_url,
                    access_key_id=r2_access_key,
                    secret_access_key=r2_secret_key,
                )
            ],
            destinations=[
                R2ProviderDestination(
                    bucket=dst_bucket,
                    key=dst_key,
                    account_id=r2_account_id,
                    endpoint_url=r2_endpoint_url,
                    access_key_id=r2_access_key,
                    secret_access_key=r2_secret_key,
                )
            ],
            test_mode=test_mode,
            chunk_size=chunk_size,
            expires_in=3600,
        )
        if not result.success:
            click.echo(f"Error creating transfer: {result.error}", err=True)
            sys.exit(1)

        click.echo(f"  Transfer ID: {result.transfer_id}")
        click.echo(f"  Size:        {result.total_size:,} bytes")
        click.echo(
            f"  Chunks:      {result.logical_chunks} logical / {result.total_chunks} delivery tasks"
        )
        click.echo("\nDistributing...")
        distribution = await beam.transfers.distribute(result.transfer_id)
        click.echo(f"  Orchestrators: {distribution.orchestrators_assigned}")
        click.echo("\n" + "=" * 60)
        click.echo("Transfer started. Monitoring...\n")

        try:
            monitor = await monitor_transfers(
                transfer_ids=[result.transfer_id],
                server=server,
                api_key=api_key,
                poll_interval=2.0,
                timeout=600.0,
            )
        except KeyboardInterrupt:
            click.echo("\nMonitoring cancelled.")
            return

        elapsed = monitor["elapsed"]
        if monitor["completed"] > 0:
            throughput = result.total_size / elapsed / (1024 * 1024) if elapsed > 0 else 0.0
            click.echo("\n" + "=" * 60)
            click.echo("TRANSFER COMPLETE!")
            click.echo(f"  Size:       {result.total_size:,} bytes")
            click.echo(f"  Time:       {elapsed:.1f}s")
            click.echo(f"  Throughput: {throughput:.2f} MB/s")
            click.echo("=" * 60)
        else:
            click.echo("Transfer failed or timed out.", err=True)
            sys.exit(1)


async def _gcs_transfer(
    source: str,
    destination: str,
    server: str,
    api_key: str,
    project_id: str | None,
    chunk_size: int | None,
    test_mode: bool,
    verbose: bool,
) -> None:
    click.echo("=" * 60)
    click.echo("BEAM GCS Transfer")
    click.echo("=" * 60)

    src_bucket, src_key = _parse_gcs_url(source)
    if not src_bucket:
        click.echo("Error: Source must be a GCS URL (gs://bucket/key)", err=True)
        sys.exit(1)

    dst_bucket, dst_key = _parse_gcs_url(destination)
    if not dst_bucket:
        click.echo("Error: Destination must be a GCS URL (gs://bucket/key)", err=True)
        sys.exit(1)

    click.echo(f"Source:      gs://{src_bucket}/{src_key}")
    click.echo(f"Destination: gs://{dst_bucket}/{dst_key}")
    click.echo(f"Beam NATS:   {server}")

    try:
        gcs_client, resolved_project_id = _build_gcs_storage_client(project_id)
    except Exception as exc:
        click.echo(f"Error initializing GCS client: {exc}", err=True)
        sys.exit(1)

    click.echo(f"Project:     {resolved_project_id}")
    click.echo("")

    src_blob = gcs_client.bucket(src_bucket).blob(src_key)

    click.echo("Getting source object info...")
    try:
        src_blob.reload()
        if src_blob.size is None:
            raise RuntimeError("GCS object size is unavailable")
        total_size = int(src_blob.size)
    except Exception as exc:
        click.echo(f"Error getting source object info: {exc}", err=True)
        sys.exit(1)

    click.echo(f"  Size:   {total_size:,} bytes ({total_size / (1024 * 1024):.2f} MB)")
    click.echo("  Chunks: assigned by BeamCore")
    click.echo("")

    async with BeamSDK(api_key=api_key, nats_url=server) as beam:
        click.echo("Creating transfer...")
        result = await beam.transfers.create(
            sources=[
                SourceConfig(
                    type="gcs",
                    bucket=src_bucket,
                    key=src_key,
                    project_id=resolved_project_id,
                )
            ],
            destinations=[
                DestConfig(
                    type="gcs",
                    bucket=dst_bucket,
                    key=dst_key,
                    project_id=resolved_project_id,
                )
            ],
            total_size=total_size,
            chunk_size=chunk_size,
            test_mode=test_mode,
        )
        if not result.success:
            click.echo(f"Error creating transfer: {result.error}", err=True)
            sys.exit(1)

        click.echo(f"  Transfer ID: {result.transfer_id}")
        click.echo("\nDistributing...")
        distribution = await beam.transfers.distribute(result.transfer_id)
        click.echo(f"  Orchestrators: {distribution.orchestrators_assigned}")
        click.echo("\n" + "=" * 60)
        click.echo("Transfer started. Monitoring...\n")

        try:
            monitor = await monitor_transfers(
                transfer_ids=[result.transfer_id],
                server=server,
                api_key=api_key,
                poll_interval=2.0,
                timeout=600.0,
            )
        except KeyboardInterrupt:
            click.echo("\nMonitoring cancelled.")
            return

        elapsed = monitor["elapsed"]
        if monitor["completed"] > 0:
            throughput = total_size / elapsed / (1024 * 1024) if elapsed > 0 else 0.0
            click.echo("\n" + "=" * 60)
            click.echo("TRANSFER COMPLETE!")
            click.echo(f"  Size:       {total_size:,} bytes")
            click.echo(f"  Time:       {elapsed:.1f}s")
            click.echo(f"  Throughput: {throughput:.2f} MB/s")
            click.echo("=" * 60)
        else:
            click.echo("Transfer failed or timed out.", err=True)
            sys.exit(1)


@cli.command("hippius-transfer")
@click.argument("source")
@click.argument("destination")
@click.option(
    "--server",
    "-s",
    default=BEAM_PROD_URL,
    envvar="BEAM_NATS_URL",
    show_default=True,
    help="Beam NATS URL",
)
@click.option("--api-key", envvar="BEAM_API_KEY", help="API key")
@click.option(
    "--hippius-token", envvar="HIPPIUS_API_TOKEN", required=True, help="Hippius Bearer token"
)
@click.option(
    "--hippius-base-url",
    envvar="HIPPIUS_BASE_URL",
    default="https://api.hippius.com",
    show_default=True,
    help="Hippius API base URL",
)
@click.option("--chunk-size", default=None, type=int, help="Chunk size in bytes (default: auto)")
@click.option("--test-mode", is_flag=True, help="Run as a test-mode transfer")
@click.option("--verbose", "-v", is_flag=True)
def hippius_transfer(
    source: str,
    destination: str,
    server: str,
    api_key: str | None,
    hippius_token: str,
    hippius_base_url: str,
    chunk_size: int | None,
    test_mode: bool,
    verbose: bool,
) -> None:
    """Transfer a single Hippius object via BeamCore."""
    if verbose:
        logging.getLogger("beam_network_sdk").setLevel(logging.DEBUG)
    if not api_key:
        click.echo("Error: --api-key or BEAM_API_KEY required", err=True)
        sys.exit(1)

    asyncio.run(
        _hippius_transfer(
            source=source,
            destination=destination,
            server=server,
            api_key=api_key,
            hippius_token=hippius_token,
            hippius_base_url=hippius_base_url.rstrip("/"),
            chunk_size=chunk_size,
            test_mode=test_mode,
            verbose=verbose,
        )
    )


async def _hippius_transfer(
    source: str,
    destination: str,
    server: str,
    api_key: str,
    hippius_token: str,
    hippius_base_url: str,
    chunk_size: int | None,
    test_mode: bool,
    verbose: bool,
) -> None:
    from beam_network_sdk.models import HippiusProviderDestination, HippiusProviderSource

    click.echo("=" * 60)
    click.echo("BEAM Hippius Transfer")
    click.echo("=" * 60)

    src_bucket, src_key = _parse_hippius_url(source)
    if not src_bucket:
        click.echo("Error: Source must be a Hippius URL (hippius://bucket/key)", err=True)
        sys.exit(1)

    dst_bucket, dst_key = _parse_hippius_url(destination)
    if not dst_bucket:
        click.echo("Error: Destination must be a Hippius URL (hippius://bucket/key)", err=True)
        sys.exit(1)

    click.echo(f"Source:      hippius://{src_bucket}/{src_key}")
    click.echo(f"Destination: hippius://{dst_bucket}/{dst_key}")
    click.echo(f"Beam NATS:   {server}")
    click.echo(f"Hippius:     {hippius_base_url}")
    click.echo("")

    async with BeamSDK(api_key=api_key, nats_url=server) as beam:
        click.echo("Preparing transfer...")
        result = await beam.transfers.prepare_provider_transfer(
            sources=[
                HippiusProviderSource(
                    bucket=src_bucket,
                    key=src_key,
                    api_token=hippius_token,
                    base_url=hippius_base_url,
                )
            ],
            destinations=[
                HippiusProviderDestination(
                    bucket=dst_bucket,
                    key=dst_key,
                    api_token=hippius_token,
                    base_url=hippius_base_url,
                )
            ],
            test_mode=test_mode,
            chunk_size=chunk_size,
            distribute=True,
        )
        if not result.success:
            click.echo(f"Error preparing transfer: {result.error}", err=True)
            sys.exit(1)

        total_size = result.total_size or 0
        click.echo(f"  Transfer ID: {result.transfer_id}")
        if total_size:
            click.echo(f"  Size: {total_size:,} bytes ({total_size / (1024 * 1024):.2f} MB)")
        click.echo("\n" + "=" * 60)
        click.echo("Transfer started. Monitoring...\n")

        try:
            monitor = await monitor_transfers(
                transfer_ids=[result.transfer_id],
                server=server,
                api_key=api_key,
                poll_interval=2.0,
                timeout=600.0,
            )
        except KeyboardInterrupt:
            click.echo("\nMonitoring cancelled.")
            return

        elapsed = monitor["elapsed"]
        if monitor["completed"] > 0:
            throughput = total_size / elapsed / (1024 * 1024) if elapsed > 0 and total_size else 0.0
            click.echo("\n" + "=" * 60)
            click.echo("TRANSFER COMPLETE!")
            if total_size:
                click.echo(f"  Size:       {total_size:,} bytes")
            click.echo(f"  Time:       {elapsed:.1f}s")
            if throughput:
                click.echo(f"  Throughput: {throughput:.2f} MB/s")
            click.echo("=" * 60)
        else:
            click.echo("Transfer failed or timed out.", err=True)
            sys.exit(1)


@cli.command("hf-transfer")
@click.argument("source")
@click.argument("destination")
@click.option(
    "--server",
    "-s",
    default=BEAM_PROD_URL,
    envvar="BEAM_NATS_URL",
    show_default=True,
    help="Beam NATS URL",
)
@click.option("--api-key", envvar="BEAM_API_KEY", help="API key")
@click.option("--hf-token", envvar="HF_TOKEN", required=True, help="Hugging Face access token")
@click.option(
    "--hf-endpoint",
    envvar="HF_ENDPOINT",
    default="https://huggingface.co",
    show_default=True,
    help="Hub endpoint",
)
@click.option("--commit-message", default=None, help="Commit summary for an upload")
@click.option("--create-pr", is_flag=True, help="Open the upload as a pull request")
@click.option(
    "--allow-source-rehash",
    is_flag=True,
    help=(
        "Read the source once to compute its sha256. The Hub requires it before issuing "
        "upload URLs, so an upload from a non-Hub source needs this."
    ),
)
@click.option("--test-mode", is_flag=True, help="Run as a test-mode transfer")
@click.option("--verbose", "-v", is_flag=True)
def hf_transfer(
    source: str,
    destination: str,
    server: str,
    api_key: str | None,
    hf_token: str,
    hf_endpoint: str,
    commit_message: str | None,
    create_pr: bool,
    allow_source_rehash: bool,
    test_mode: bool,
    verbose: bool,
) -> None:
    """Transfer a single Hugging Face Hub file via BeamCore.

    SOURCE and DESTINATION are hf:// URIs, for example
    hf://datasets/acme/corpus@main/data/train.parquet
    """
    if verbose:
        logging.getLogger("beam_network_sdk").setLevel(logging.DEBUG)
    if not api_key:
        click.echo("Error: --api-key or BEAM_API_KEY required", err=True)
        sys.exit(1)

    asyncio.run(
        _hf_transfer(
            source=source,
            destination=destination,
            server=server,
            api_key=api_key,
            hf_token=hf_token,
            hf_endpoint=hf_endpoint.rstrip("/"),
            commit_message=commit_message,
            create_pr=create_pr,
            allow_source_rehash=allow_source_rehash,
            test_mode=test_mode,
        )
    )


async def _hf_transfer(
    source: str,
    destination: str,
    server: str,
    api_key: str,
    hf_token: str,
    hf_endpoint: str,
    commit_message: str | None,
    create_pr: bool,
    allow_source_rehash: bool,
    test_mode: bool,
) -> None:
    from beam_network_sdk.models import (
        HuggingFaceProviderDestination,
        HuggingFaceProviderSource,
    )

    src_type, src_repo, src_revision, src_path = _parse_hf_uri(source)
    dst_type, dst_repo, dst_revision, dst_path = _parse_hf_uri(destination)

    click.echo("=" * 60)
    click.echo("BEAM Hugging Face Transfer")
    click.echo("=" * 60)
    click.echo(f"Source:      hf://{src_repo}@{src_revision}/{src_path} ({src_type})")
    click.echo(f"Destination: hf://{dst_repo}@{dst_revision}/{dst_path} ({dst_type})")
    click.echo(f"Beam NATS:   {server}")
    click.echo(f"Hub:         {hf_endpoint}")
    click.echo("")

    async with BeamSDK(api_key=api_key, nats_url=server) as beam:
        click.echo("Preparing transfer...")
        result = await beam.transfers.prepare_provider_transfer(
            sources=[
                HuggingFaceProviderSource(
                    repo_id=src_repo,
                    path=src_path,
                    repo_type=src_type,
                    revision=src_revision,
                    token=hf_token,
                    endpoint=hf_endpoint,
                )
            ],
            destinations=[
                HuggingFaceProviderDestination(
                    repo_id=dst_repo,
                    path=dst_path,
                    repo_type=dst_type,
                    revision=dst_revision,
                    token=hf_token,
                    endpoint=hf_endpoint,
                    commit_message=commit_message,
                    create_pr=create_pr,
                    allow_source_rehash=allow_source_rehash,
                )
            ],
            test_mode=test_mode,
            distribute=True,
        )
        if not result.success:
            click.echo(f"Error preparing transfer: {result.error}", err=True)
            sys.exit(1)

        total_size = result.total_size or 0
        click.echo(f"  Transfer ID: {result.transfer_id}")
        if total_size:
            click.echo(f"  Size: {total_size:,} bytes ({total_size / (1024 * 1024):.2f} MB)")
        click.echo("\n" + "=" * 60)
        click.echo("Transfer started. Monitoring...\n")

        status = await beam.transfers.wait_complete(result.transfer_id, timeout=3600.0)
        click.echo("\n" + "=" * 60)
        click.echo(f"TRANSFER {status.status.upper()}")
        click.echo(f"  Committed to hf://{dst_repo}@{dst_revision}/{dst_path}")
        click.echo("=" * 60)


def _detect_scheme(url: str) -> str:
    for prefix in ("s3://", "gs://", "hippius://", "hf://", "http://", "https://"):
        if url.startswith(prefix):
            return prefix.rstrip(":/")
    raise click.BadParameter(
        f"Unrecognised URL scheme: {url!r}. Use s3://, gs://, hippius://, hf://, or http(s)://"
    )


async def _resolve_source(
    url: str,
    *,
    aws_access_key: str | None,
    aws_secret_key: str | None,
    aws_region: str,
    gcp_project_id: str | None,
    hippius_token: str | None,
    hippius_base_url: str,
    total_size_override: int | None,
) -> tuple:
    """Return (SourceConfig, total_size)."""
    scheme = _detect_scheme(url)

    if scheme == "s3":
        import boto3

        bucket, key = _parse_s3_url(url)
        s3 = boto3.client(
            "s3", **build_s3_client_kwargs(aws_access_key, aws_secret_key, aws_region)
        )
        head = s3.head_object(Bucket=bucket, Key=key)
        size = head["ContentLength"]
        presigned = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=3600,
        )
        return SourceConfig(type="http", url=presigned), size

    if scheme == "gs":
        gcs_client, resolved_project = _build_gcs_storage_client(gcp_project_id)
        bucket, key = _parse_gcs_url(url)
        blob = gcs_client.bucket(bucket).blob(key)
        blob.reload()
        if blob.size is None:
            raise RuntimeError(f"GCS object size unavailable for gs://{bucket}/{key}")
        return SourceConfig(type="gcs", bucket=bucket, key=key, project_id=resolved_project), int(
            blob.size
        )

    if scheme == "hippius":
        bucket, key = _parse_hippius_url(url)
        auth = {"Authorization": f"Bearer {hippius_token}"}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{hippius_base_url}/objectstore/buckets/{bucket}/objects/",
                params={"prefix": key, "max_keys": 1},
                headers=auth,
            )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Hippius object lookup failed (HTTP {resp.status_code}): {resp.text}"
            )
        contents = resp.json().get("Contents", [])
        if not contents:
            raise RuntimeError(f"Object not found: hippius://{bucket}/{key}")
        size = int(contents[0]["Size"])
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{hippius_base_url}/objectstore/buckets/{bucket}/presigned-url/",
                params={"key": key, "action": "get", "expires_in": 3600},
                headers=auth,
            )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Hippius presigned URL failed (HTTP {resp.status_code}): {resp.text}"
            )
        return SourceConfig(type="http", url=resp.json()["url"]), size

    # http / https
    if total_size_override is None:
        raise click.UsageError("--total-size is required when source is an HTTP URL")
    return SourceConfig(type="http", url=url), total_size_override


async def _resolve_destinations(
    url: str,
    src_key: str,
    *,
    aws_access_key: str | None,
    aws_secret_key: str | None,
    aws_region: str,
    gcp_project_id: str | None,
    hippius_token: str | None,
    hippius_base_url: str,
) -> list:
    """Return List[DestConfig] without SDK-side chunk planning."""
    scheme = _detect_scheme(url)

    if scheme == "s3":
        bucket, key = _parse_s3_url(url)
        return [
            DestConfig(
                type="s3",
                bucket=bucket,
                key=key.rstrip("/") if key else src_key,
                region=aws_region,
                access_key_id=aws_access_key,
                secret_access_key=aws_secret_key,
            )
        ]

    if scheme == "gs":
        bucket, key = _parse_gcs_url(url)
        resolved_project = gcp_project_id
        if resolved_project is None:
            _, resolved_project = _build_gcs_storage_client(None)
        return [DestConfig(type="gcs", bucket=bucket, key=key, project_id=resolved_project or None)]

    if scheme == "hippius":
        raise click.UsageError("hippius destinations require the provider transfer path")

    # http / https
    return [DestConfig(type="http", url=url)]


@cli.command("transfer")
@click.argument("source")
@click.argument("destination")
@click.option(
    "--server",
    "-s",
    default=BEAM_PROD_URL,
    envvar="BEAM_NATS_URL",
    show_default=True,
    help="Beam NATS URL",
)
@click.option("--api-key", envvar="BEAM_API_KEY", help="BeamCore API key")
@click.option(
    "--aws-access-key", envvar="AWS_ACCESS_KEY_ID", help="AWS Access Key ID (required for s3://)"
)
@click.option(
    "--aws-secret-key",
    envvar="AWS_SECRET_ACCESS_KEY",
    help="AWS Secret Access Key (required for s3://)",
)
@click.option("--aws-region", envvar="AWS_DEFAULT_REGION", default="us-east-1", show_default=True)
@click.option(
    "--gcp-project-id",
    envvar="GOOGLE_CLOUD_PROJECT",
    default=None,
    help="GCP project ID (optional for gs://)",
)
@click.option(
    "--hippius-token",
    envvar="HIPPIUS_API_TOKEN",
    default=None,
    help="Hippius Bearer token (required for hippius://)",
)
@click.option(
    "--hippius-base-url",
    envvar="HIPPIUS_BASE_URL",
    default="https://api.hippius.com",
    show_default=True,
)
@click.option(
    "--total-size",
    type=int,
    default=None,
    help="Total bytes â€” required when source is http(s)://",
)
@click.option("--chunk-size", default=None, type=int, help="Chunk size in bytes (default: auto)")
@click.option("--test-mode", is_flag=True, help="Run as a test-mode transfer")
@click.option("--verbose", "-v", is_flag=True)
def universal_transfer(
    source: str,
    destination: str,
    server: str,
    api_key: str | None,
    aws_access_key: str | None,
    aws_secret_key: str | None,
    aws_region: str,
    gcp_project_id: str | None,
    hippius_token: str | None,
    hippius_base_url: str,
    total_size: int | None,
    chunk_size: int | None,
    test_mode: bool,
    verbose: bool,
) -> None:
    """Transfer between any two storage connectors (s3://, gs://, hippius://, http://)."""
    if verbose:
        logging.getLogger("beam_network_sdk").setLevel(logging.DEBUG)
    if not api_key:
        click.echo("Error: --api-key or BEAM_API_KEY required", err=True)
        sys.exit(1)

    src_scheme = _detect_scheme(source)
    dst_scheme = _detect_scheme(destination)

    if (src_scheme == "s3" or dst_scheme == "s3") and (not aws_access_key or not aws_secret_key):
        click.echo("Error: --aws-access-key and --aws-secret-key required for s3:// URLs", err=True)
        sys.exit(1)
    if (src_scheme == "hippius" or dst_scheme == "hippius") and not hippius_token:
        click.echo(
            "Error: --hippius-token or HIPPIUS_API_TOKEN required for hippius:// URLs", err=True
        )
        sys.exit(1)

    asyncio.run(
        _universal_transfer(
            source=source,
            destination=destination,
            server=server,
            api_key=api_key,
            aws_access_key=aws_access_key,
            aws_secret_key=aws_secret_key,
            aws_region=aws_region,
            gcp_project_id=gcp_project_id,
            hippius_token=hippius_token,
            hippius_base_url=hippius_base_url.rstrip("/"),
            total_size_override=total_size,
            chunk_size=chunk_size,
            test_mode=test_mode,
        )
    )


async def _universal_transfer(
    source: str,
    destination: str,
    server: str,
    api_key: str,
    aws_access_key: str | None,
    aws_secret_key: str | None,
    aws_region: str,
    gcp_project_id: str | None,
    hippius_token: str | None,
    hippius_base_url: str,
    total_size_override: int | None,
    chunk_size: int | None,
    test_mode: bool,
) -> None:
    src_scheme = _detect_scheme(source)
    dst_scheme = _detect_scheme(destination)

    # Both hippius: use provider path (prepare_provider_transfer) which handles signing server-side
    if src_scheme == "hippius" and dst_scheme == "hippius":
        await _hippius_transfer(
            source=source,
            destination=destination,
            server=server,
            api_key=api_key,
            hippius_token=hippius_token or "",
            hippius_base_url=hippius_base_url,
            chunk_size=chunk_size,
            test_mode=test_mode,
            verbose=False,
        )
        return

    click.echo("=" * 60)
    click.echo("BEAM Transfer")
    click.echo("=" * 60)
    click.echo(f"Source:      {source}")
    click.echo(f"Destination: {destination}")
    click.echo(f"Beam NATS:   {server}")
    click.echo("")

    src_kwargs = dict(
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        aws_region=aws_region,
        gcp_project_id=gcp_project_id,
        hippius_token=hippius_token,
        hippius_base_url=hippius_base_url,
        total_size_override=total_size_override,
    )
    dst_kwargs = dict(
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        aws_region=aws_region,
        gcp_project_id=gcp_project_id,
        hippius_token=hippius_token,
        hippius_base_url=hippius_base_url,
    )

    click.echo("Resolving source...")
    try:
        src_config, total_size = await _resolve_source(source, **src_kwargs)
    except Exception as exc:
        click.echo(f"Error resolving source: {exc}", err=True)
        sys.exit(1)

    click.echo(f"  Size:   {total_size:,} bytes ({total_size / (1024 * 1024):.2f} MB)")
    click.echo("  Chunks: assigned by BeamCore")
    click.echo("")

    if src_scheme == "s3":
        _, src_key = _parse_s3_url(source)
    elif src_scheme == "gs":
        _, src_key = _parse_gcs_url(source)
    elif src_scheme == "hippius":
        _, src_key = _parse_hippius_url(source)
    else:
        src_key = source.rsplit("/", 1)[-1]

    click.echo("Resolving destination...")
    try:
        dest_configs = await _resolve_destinations(destination, src_key, **dst_kwargs)
    except Exception as exc:
        click.echo(f"Error resolving destination: {exc}", err=True)
        sys.exit(1)

    async with BeamSDK(api_key=api_key, nats_url=server) as beam:
        click.echo("Creating transfer...")
        result = await beam.transfers.create(
            sources=[src_config],
            destinations=dest_configs,
            total_size=total_size,
            chunk_size=chunk_size,
            test_mode=test_mode,
        )
        if not result.success:
            click.echo(f"Error creating transfer: {result.error}", err=True)
            sys.exit(1)

        click.echo(f"  Transfer ID: {result.transfer_id}")
        click.echo("\nDistributing...")
        distribution = await beam.transfers.distribute(result.transfer_id)
        click.echo(f"  Orchestrators: {distribution.orchestrators_assigned}")
        click.echo("\n" + "=" * 60)
        click.echo("Transfer started. Monitoring...\n")

        try:
            monitor = await monitor_transfers(
                transfer_ids=[result.transfer_id],
                server=server,
                api_key=api_key,
                poll_interval=2.0,
                timeout=600.0,
            )
        except KeyboardInterrupt:
            click.echo("\nMonitoring cancelled.")
            return

        elapsed = monitor["elapsed"]
        if monitor["completed"] > 0:
            throughput = total_size / elapsed / (1024 * 1024) if elapsed > 0 else 0.0
            click.echo("\n" + "=" * 60)
            click.echo("TRANSFER COMPLETE!")
            click.echo(f"  Size:       {total_size:,} bytes")
            click.echo(f"  Time:       {elapsed:.1f}s")
            click.echo(f"  Throughput: {throughput:.2f} MB/s")
            click.echo("=" * 60)
        else:
            click.echo("Transfer failed or timed out.", err=True)
            sys.exit(1)


@cli.command("multi-transfer")
@click.argument("sources", nargs=-1, required=True)
@click.option(
    "--dest",
    "-d",
    "destinations",
    multiple=True,
    required=True,
    help="Destination S3 URL - repeatable",
)
@click.option(
    "--callback",
    "-c",
    "callbacks",
    multiple=True,
    help="Webhook URL to notify on completion - repeatable",
)
@click.option(
    "--parallel", "-p", is_flag=True, help="1:1 mode: source[i] -> dest[i] (requires equal counts)"
)
@click.option(
    "--server",
    "-s",
    default=BEAM_PROD_URL,
    envvar="BEAM_NATS_URL",
    show_default=True,
    help="Beam NATS URL",
)
@click.option("--api-key", envvar="BEAM_API_KEY", help="API key")
@click.option("--aws-access-key", envvar="AWS_ACCESS_KEY_ID", help="AWS Access Key ID")
@click.option("--aws-secret-key", envvar="AWS_SECRET_ACCESS_KEY", help="AWS Secret Access Key")
@click.option(
    "--aws-region",
    envvar="AWS_DEFAULT_REGION",
    default="us-east-1",
    show_default=True,
    help="AWS region",
)
@click.option(
    "--chunk-size", default=None, type=int, help="Chunk size in bytes (default: auto per file)"
)
@click.option("--name", "-n", default=None, help="Transfer name prefix")
@click.option(
    "--create-only", is_flag=True, help="Create transfers but skip distribution and monitoring"
)
@click.option("--test-mode", is_flag=True, help="Run as a test-mode transfer")
@click.option(
    "--progressive", is_flag=True, help="Assign chunks dynamically as capacity becomes available"
)
@click.option("--verbose", "-v", is_flag=True)
def multi_transfer(
    sources: tuple[str, ...],
    destinations: tuple[str, ...],
    callbacks: tuple[str, ...],
    parallel: bool,
    server: str,
    api_key: str | None,
    aws_access_key: str | None,
    aws_secret_key: str | None,
    aws_region: str,
    chunk_size: int | None,
    name: str | None,
    create_only: bool,
    test_mode: bool,
    progressive: bool,
    verbose: bool,
) -> None:
    """Transfer S3 files to one or more destinations via BeamCore."""
    if verbose:
        logging.getLogger("beam_network_sdk").setLevel(logging.DEBUG)

    if not aws_access_key or not aws_secret_key:
        click.echo(
            "Error: AWS credentials required (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)", err=True
        )
        sys.exit(1)
    if not api_key:
        click.echo("Error: --api-key or BEAM_API_KEY required", err=True)
        sys.exit(1)
    if parallel and len(sources) != len(destinations):
        click.echo(
            f"Error: --parallel requires equal source and destination counts ({len(sources)} sources, {len(destinations)} destinations)",
            err=True,
        )
        sys.exit(1)

    asyncio.run(
        _multi_transfer(
            sources=list(sources),
            destinations=list(destinations),
            callbacks=list(callbacks),
            parallel=parallel,
            server=server,
            api_key=api_key,
            aws_access_key=aws_access_key,
            aws_secret_key=aws_secret_key,
            aws_region=aws_region,
            chunk_size=chunk_size,
            name=name,
            create_only=create_only,
            test_mode=test_mode,
            progressive=progressive,
            verbose=verbose,
        )
    )


async def _multi_transfer(
    sources: list[str],
    destinations: list[str],
    callbacks: list[str],
    parallel: bool,
    server: str,
    api_key: str,
    aws_access_key: str,
    aws_secret_key: str,
    aws_region: str,
    chunk_size: int | None,
    name: str | None,
    create_only: bool,
    test_mode: bool,
    progressive: bool,
    verbose: bool,
) -> None:
    import boto3

    click.echo("=" * 60)
    click.echo("BEAM Multi-Transfer")
    click.echo("=" * 60)

    source_files: list[dict[str, Any]] = []
    for source in sources:
        bucket, key = _parse_s3_url(source)
        if not bucket:
            click.echo(f"Error: Source must be S3 URL: {source}", err=True)
            sys.exit(1)
        source_files.append({"bucket": bucket, "key": key, "url": source})

    destination_configs: list[dict[str, Any]] = []
    for destination in destinations:
        bucket, key = _parse_s3_url(destination)
        if not bucket:
            click.echo(f"Error: Destination must be S3 URL: {destination}", err=True)
            sys.exit(1)
        destination_configs.append({"bucket": bucket, "key": key, "url": destination})

    mode_str = "parallel 1:1" if parallel else "fan-out (each source -> all destinations)"
    click.echo(f"Mode:          {mode_str}")
    click.echo(
        f"Sources ({len(source_files)}):   "
        + ", ".join(
            f"s3://{source_file['bucket']}/{source_file['key']}" for source_file in source_files
        )
    )
    click.echo(
        f"Destinations ({len(destination_configs)}): "
        + ", ".join(
            f"s3://{destination['bucket']}/{destination['key']}"
            for destination in destination_configs
        )
    )
    if callbacks:
        click.echo("Callbacks:     " + ", ".join(callbacks))
    click.echo(f"Beam NATS:     {server}")
    click.echo("")

    s3 = boto3.client("s3", **build_s3_client_kwargs(aws_access_key, aws_secret_key, aws_region))

    async with BeamSDK(api_key=api_key, nats_url=server) as beam:
        for source_file in source_files:
            click.echo(f"Getting info: s3://{source_file['bucket']}/{source_file['key']}")
            try:
                head = s3.head_object(Bucket=source_file["bucket"], Key=source_file["key"])
                file_size = head["ContentLength"]
                source_file["size"] = file_size
                click.echo(
                    f"  {file_size:,} bytes ({file_size / (1024 * 1024):.2f} MB), chunks assigned by BeamCore"
                )
            except Exception as exc:
                click.echo(f"  Error: {exc}", err=True)
                sys.exit(1)

        click.echo("")

        if parallel:
            transfer_pairs = [
                (source_file, [destination_configs[index]])
                for index, source_file in enumerate(source_files)
            ]
        else:
            transfer_pairs = [(source_file, destination_configs) for source_file in source_files]

        click.echo(f"Creating {len(transfer_pairs)} transfer(s)...")
        click.echo("")

        transfer_ids: list[str] = []
        total_transfer_bytes = 0
        for pair_index, (source_file, file_destinations) in enumerate(transfer_pairs):
            file_size = source_file["size"]
            filename = str(source_file["key"]).split("/")[-1]

            click.echo(f"[{pair_index + 1}/{len(transfer_pairs)}] {filename}")
            click.echo(f"  Source:  s3://{source_file['bucket']}/{source_file['key']}")
            click.echo(f"  Size:    {file_size:,} bytes, chunks assigned by BeamCore")
            chunk_hashes = None
            merkle_root = None

            sdk_source = SourceConfig(
                type="s3",
                bucket=source_file["bucket"],
                key=source_file["key"],
                region=aws_region,
                access_key_id=aws_access_key,
                secret_access_key=aws_secret_key,
            )

            sdk_destinations: list[DestConfig] = []
            for destination in file_destinations:
                dest_key = destination["key"]
                if dest_key.endswith("/") or not dest_key:
                    dest_key = f"{dest_key.rstrip('/')}/{filename}"
                sdk_destinations.append(
                    DestConfig(
                        type="s3",
                        bucket=destination["bucket"],
                        key=dest_key,
                        region=aws_region,
                        access_key_id=aws_access_key,
                        secret_access_key=aws_secret_key,
                    )
                )

            transfer_name = name or f"cli-{filename}"
            if len(transfer_pairs) > 1:
                transfer_name = f"{transfer_name}-{pair_index + 1}"

            sdk_callbacks = [CallbackConfig(url=url) for url in callbacks] if callbacks else None

            try:
                result = await beam.transfers.create(
                    sources=[sdk_source],
                    destinations=sdk_destinations,
                    total_size=file_size,
                    chunk_size=chunk_size,
                    name=transfer_name,
                    merkle_root=merkle_root,
                    chunk_hashes=chunk_hashes,
                    callbacks=sdk_callbacks,
                    test_mode=test_mode,
                    progressive_mode=progressive,
                )
            except Exception as exc:
                click.echo(f"  ERROR creating transfer: {exc}", err=True)
                continue

            if not result.success:
                click.echo(f"  ERROR: {result.error}", err=True)
                continue

            click.echo(f"  Transfer ID:  {result.transfer_id}")
            click.echo(f"  Destinations: {result.total_destinations}")
            transfer_ids.append(result.transfer_id)
            total_transfer_bytes += file_size * result.total_destinations

            if create_only:
                click.echo("  Distribution: SKIPPED (--create-only)")
            else:
                try:
                    distribution_result = await beam.transfers.distribute(result.transfer_id)
                    click.echo(f"  Orchestrators: {distribution_result.orchestrators_assigned}")
                except Exception as exc:
                    click.echo(f"  ERROR distributing transfer: {exc}", err=True)
                    transfer_ids.remove(result.transfer_id)
                    continue

            click.echo("")

        if not transfer_ids:
            click.echo("No transfers were created successfully.", err=True)
            sys.exit(1)

        if create_only:
            click.echo("=" * 60)
            click.echo(f"Created {len(transfer_ids)} transfer(s) (not distributed)")
            for transfer_id in transfer_ids:
                click.echo(f"  {transfer_id}")
            click.echo("=" * 60)
            return

        click.echo("=" * 60)
        click.echo(f"Created {len(transfer_ids)} transfer(s). Monitoring...\n")

        try:
            monitor = await monitor_transfers(
                transfer_ids=transfer_ids,
                server=server,
                api_key=api_key,
                poll_interval=2.0,
                timeout=600.0,
            )
            completed = monitor["completed"]
            failed = monitor["failed"]
            elapsed = monitor["elapsed"]
        except KeyboardInterrupt:
            click.echo("\nMonitoring cancelled.")
            return

        throughput = total_transfer_bytes / elapsed / (1024 * 1024) if elapsed > 0 else 0.0
        click.echo("\n" + "=" * 60)
        if failed == 0:
            click.echo("ALL TRANSFERS COMPLETE!")
        else:
            click.echo(f"TRANSFERS: {completed} completed, {failed} failed")
        click.echo(f"  Files:      {len(source_files)}")
        click.echo(
            f"  Dest slots: {total_transfer_bytes // (source_files[0].get('size', 1) if source_files else 1)}"
        )
        click.echo(f"  Total bytes:{total_transfer_bytes:,}")
        click.echo(f"  Time:       {elapsed:.1f}s")
        click.echo(f"  Throughput: {throughput:.2f} MB/s")
        click.echo("=" * 60)

        if failed > 0:
            sys.exit(1)


@cli.command("create")
@click.option(
    "--source",
    "sources",
    multiple=True,
    required=True,
    help="Source JSON object or @path/to/source.json",
)
@click.option(
    "--destination",
    "destinations",
    multiple=True,
    required=True,
    help="Destination JSON object or @path/to/destination.json",
)
@click.option("--total-size", required=True, type=int, help="Total bytes across all sources")
@click.option("--chunk-size", type=int, default=None, help="Chunk size in bytes")
@click.option("--name", default=None, help="Optional transfer name")
@click.option("--merkle-root", default=None, help="Optional merkle root")
@click.option("--chunk-hash", "chunk_hashes", multiple=True, help="Repeatable per-chunk hash value")
@click.option("--callback-url", default=None, help="Optional callback URL for transfer completion")
@click.option(
    "--callback-header",
    "callback_headers",
    multiple=True,
    help="Callback header in Key=Value format",
)
@click.option("--test-mode", is_flag=True, help="Create the transfer in test mode")
@click.option(
    "--progressive-mode",
    is_flag=True,
    help="Assign chunks dynamically as capacity becomes available",
)
@click.option(
    "--distribute/--no-distribute", default=True, help="Distribute immediately after creation"
)
@click.option(
    "--wait", "wait_for_completion", is_flag=True, help="Wait for the transfer to complete"
)
@click.option("--timeout", default=300.0, show_default=True, help="Wait timeout in seconds")
@click.option("--poll-interval", default=2.0, show_default=True, help="Polling interval in seconds")
@click.option(
    "--server",
    default=BEAM_PROD_URL,
    envvar="BEAM_NATS_URL",
    show_default=True,
    help="Beam NATS URL",
)
@click.option("--api-key", envvar="BEAM_API_KEY", help="API key")
@click.option("--json-output", is_flag=True, help="Print machine-readable JSON only")
def create_transfer(
    sources: list[str],
    destinations: list[str],
    total_size: int,
    chunk_size: int | None,
    name: str | None,
    merkle_root: str | None,
    chunk_hashes: list[str],
    callback_url: str | None,
    callback_headers: list[str],
    test_mode: bool,
    progressive_mode: bool,
    distribute: bool,
    wait_for_completion: bool,
    timeout: float,
    poll_interval: float,
    server: str,
    api_key: str | None,
    json_output: bool,
) -> None:
    """Create a transfer from JSON source and destination definitions."""

    async def run() -> None:
        callbacks = None
        if callback_url:
            callbacks = [
                CallbackConfig(url=callback_url, headers=_parse_kv_headers(list(callback_headers)))
            ]

        async with await _build_sdk(server, api_key) as beam:
            result = await beam.transfers.create(
                sources=[SourceConfig.model_validate(_load_json(item)) for item in sources],
                destinations=[DestConfig.model_validate(_load_json(item)) for item in destinations],
                total_size=total_size,
                chunk_size=chunk_size,
                name=name,
                merkle_root=merkle_root,
                chunk_hashes=list(chunk_hashes) or None,
                callbacks=callbacks,
                test_mode=test_mode,
                progressive_mode=progressive_mode,
            )

            output: dict[str, object] = {"create": result.model_dump()}
            if distribute and result.success:
                distribution = await beam.transfers.distribute(result.transfer_id)
                output["distribute"] = distribution.model_dump()
            if wait_for_completion and result.success:
                status = await beam.transfers.wait_complete(
                    result.transfer_id,
                    timeout=timeout,
                    poll_interval=poll_interval,
                )
                output["status"] = status.model_dump()

            if json_output:
                _print_json(output)
                return

            click.echo(f"Transfer: {result.transfer_id}")
            click.echo(f"Created:  {'yes' if result.success else 'no'}")
            click.echo(f"Sources:  {result.total_sources}")
            click.echo(f"Targets:  {result.total_destinations}")
            click.echo(f"Chunks:   {result.total_chunks}")
            if distribute and "distribute" in output:
                distribution = output["distribute"]
                click.echo(
                    f"Distributed: yes ({distribution['orchestrators_assigned']} orchestrators)"
                )
            if wait_for_completion and "status" in output:
                status = output["status"]
                click.echo(f"Final status: {status['status']}")

    asyncio.run(run())


@cli.command("status")
@click.argument("transfer_id")
@click.option(
    "--server",
    default=BEAM_PROD_URL,
    envvar="BEAM_NATS_URL",
    show_default=True,
    help="Beam NATS URL",
)
@click.option("--api-key", envvar="BEAM_API_KEY", help="API key")
@click.option("--json-output", is_flag=True, help="Print machine-readable JSON only")
def transfer_status(
    transfer_id: str,
    server: str,
    api_key: str | None,
    json_output: bool,
) -> None:
    """Get transfer status."""

    async def run() -> None:
        async with await _build_sdk(server, api_key) as beam:
            status = await beam.transfers.status(transfer_id)
            if json_output:
                _print_json(status.model_dump())
                return
            click.echo(f"Transfer: {status.transfer_id}")
            click.echo(f"Status:   {status.status}")
            if status.started_at:
                click.echo(f"Started:  {status.started_at}")
            if status.completed_at:
                click.echo(f"Done:     {status.completed_at}")
            if status.error_message:
                click.echo(f"Error:    {status.error_message}")

    asyncio.run(run())


@cli.command("wait")
@click.argument("transfer_id")
@click.option("--timeout", default=300.0, show_default=True, help="Wait timeout in seconds")
@click.option("--poll-interval", default=2.0, show_default=True, help="Polling interval in seconds")
@click.option(
    "--server",
    default=BEAM_PROD_URL,
    envvar="BEAM_NATS_URL",
    show_default=True,
    help="Beam NATS URL",
)
@click.option("--api-key", envvar="BEAM_API_KEY", help="API key")
@click.option("--json-output", is_flag=True, help="Print machine-readable JSON only")
def wait_for_transfer(
    transfer_id: str,
    timeout: float,
    poll_interval: float,
    server: str,
    api_key: str | None,
    json_output: bool,
) -> None:
    """Wait for a transfer to complete."""

    async def run() -> None:
        async with await _build_sdk(server, api_key) as beam:
            status = await beam.transfers.wait_complete(
                transfer_id,
                timeout=timeout,
                poll_interval=poll_interval,
            )
            if json_output:
                _print_json(status.model_dump())
                return
            click.echo(f"Transfer: {status.transfer_id}")
            click.echo(f"Final status: {status.status}")
            if status.completed_at:
                click.echo(f"Done: {status.completed_at}")

    asyncio.run(run())


def main() -> None:
    """Entry point for the CLI."""

    cli()


if __name__ == "__main__":
    main()
