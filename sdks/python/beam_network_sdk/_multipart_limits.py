"""Source-local multipart numbering shared by provider-transfer signing paths."""

MULTIPART_MAX_PART_NUMBER = 10_000
MULTIPART_ATTEMPT_SLOTS = 3
MULTIPART_MAX_SOURCE_CHUNKS = MULTIPART_MAX_PART_NUMBER // MULTIPART_ATTEMPT_SLOTS


def multipart_part_number(source_chunk_index: int, logical_attempt_index: int = 0) -> int:
    if (
        not isinstance(source_chunk_index, int)
        or isinstance(source_chunk_index, bool)
        or source_chunk_index < 0
    ):
        raise ValueError("source_chunk_index must be a non-negative integer")
    if (
        not isinstance(logical_attempt_index, int)
        or isinstance(logical_attempt_index, bool)
        or logical_attempt_index < 0
    ):
        raise ValueError("logical_attempt_index must be a non-negative integer")
    if source_chunk_index >= MULTIPART_MAX_SOURCE_CHUNKS:
        raise ValueError(f"source_chunk_index must be less than {MULTIPART_MAX_SOURCE_CHUNKS}")
    return (
        (source_chunk_index * MULTIPART_ATTEMPT_SLOTS)
        + (logical_attempt_index % MULTIPART_ATTEMPT_SLOTS)
        + 1
    )
