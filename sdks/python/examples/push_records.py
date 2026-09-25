"""Example: create and distribute a record transfer over HTTP."""

import asyncio
import json

from beam_network_sdk import BeamSDK
from beam_network_sdk.models import DestConfig, SourceConfig


async def main():
    async with BeamSDK(api_key="b1m_test_key", environment="dev") as beam:
        records = [
            {"user_id": 1, "event": "click", "page": "/home"},
            {"user_id": 2, "event": "view", "page": "/pricing"},
            {"user_id": 3, "event": "signup"},
        ]
        ndjson_data = "\n".join(json.dumps(r) for r in records)
        data_size = len(ndjson_data.encode())

        transfer = await beam.transfers.create(
            sources=[
                SourceConfig(
                    type="http",
                    url="https://your-gateway.com/records-batch-001.ndjson",
                )
            ],
            destinations=[
                DestConfig(
                    type="http",
                    url="https://my-api.com/ingest",
                    headers={"Authorization": "Bearer my-token"},
                )
            ],
            total_size=data_size,
            name="records-batch-001",
        )
        await beam.transfers.distribute(transfer.transfer_id)
        status = await beam.transfers.wait_complete(transfer.transfer_id)

        print(f"Transfer created: {transfer.transfer_id}")
        print(f"Final status: {status.status}")


if __name__ == "__main__":
    asyncio.run(main())
