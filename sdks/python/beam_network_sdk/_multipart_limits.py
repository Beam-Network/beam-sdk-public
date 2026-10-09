"""Source-local multipart numbering shared by provider-transfer signing paths."""

MULTIPART_MAX_PART_NUMBER = 10_000
MULTIPART_ATTEMPT_SLOT_COUNT = 1
#: Backwards-compatible alias of :data:`MULTIPART_ATTEMPT_SLOT_COUNT`.
MULTIPART_ATTEMPT_SLOTS = MULTIPART_ATTEMPT_SLOT_COUNT
MULTIPART_MAX_SOURCE_CHUNKS = MULTIPART_MAX_PART_NUMBER // MULTIPART_ATTEMPT_SLOT_COUNT


def multipart_part_number(source_chunk_index: int, attempt_slot: int = 0) -> int:
    """Return the S3 part number reserved for ``attempt_slot`` of a source-local chunk.

    Under ``transfer-client-control/v7`` each source chunk owns exactly one part number,
    ``part_number = source_chunk_index + 1``, and the only attempt slot is ``0``. Any
    other slot is rejected rather than wrapped, matching the TypeScript SDK.
    """
    if (
        not isinstance(source_chunk_index, int)
        or isinstance(source_chunk_index, bool)
        or source_chunk_index < 0
    ):
        raise ValueError("source_chunk_index must be a non-negative integer")
    if source_chunk_index >= MULTIPART_MAX_SOURCE_CHUNKS:
        raise ValueError(f"source_chunk_index must be less than {MULTIPART_MAX_SOURCE_CHUNKS}")
    if (
        not isinstance(attempt_slot, int)
        or isinstance(attempt_slot, bool)
        or attempt_slot < 0
        or attempt_slot >= MULTIPART_ATTEMPT_SLOT_COUNT
    ):
        raise ValueError("attempt_slot must be 0 for consecutive multipart uploads")
    return source_chunk_index + 1
