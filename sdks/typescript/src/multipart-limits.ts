export const MULTIPART_MAX_PART_NUMBER = 10_000;
export const MULTIPART_ATTEMPT_SLOT_COUNT = 1;
export const MULTIPART_MAX_SOURCE_CHUNKS = Math.floor(MULTIPART_MAX_PART_NUMBER / MULTIPART_ATTEMPT_SLOT_COUNT);

export function multipartPartNumber(chunkIndex: number, attemptSlot = 0): number {
  if (!Number.isInteger(chunkIndex) || chunkIndex < 0) {
    throw new Error("source_chunk_index must be a non-negative integer");
  }
  if (chunkIndex >= MULTIPART_MAX_SOURCE_CHUNKS) {
    throw new Error(`source_chunk_index must be less than ${MULTIPART_MAX_SOURCE_CHUNKS}`);
  }
  if (!Number.isInteger(attemptSlot) || attemptSlot < 0 || attemptSlot >= MULTIPART_ATTEMPT_SLOT_COUNT) {
    throw new Error("attempt_slot must be 0 for consecutive multipart uploads");
  }
  const partNumber = chunkIndex + 1;
  return partNumber;
}
