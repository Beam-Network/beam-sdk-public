package beamnetworksdk

import "fmt"

const (
	// MultipartMaxPartNumber is the provider limit on multipart part numbers.
	MultipartMaxPartNumber = 10_000
	// MultipartAttemptSlotCount is the number of part numbers reserved per source
	// chunk. transfer-client-control/v7 uses consecutive part numbers, so every
	// source chunk owns exactly one part number and retries reuse it.
	MultipartAttemptSlotCount = 1
	// MultipartMaxSourceChunks is the largest source chunk count a multipart
	// destination can hold.
	MultipartMaxSourceChunks = MultipartMaxPartNumber / MultipartAttemptSlotCount
)

// Unexported aliases retained for internal call sites.
const (
	multipartMaxPartNumber   = MultipartMaxPartNumber
	multipartAttemptSlots    = MultipartAttemptSlotCount
	multipartMaxSourceChunks = MultipartMaxSourceChunks
)

// MultipartPartNumber maps a source chunk to its consecutive multipart part
// number (chunkIndex + 1). attemptSlot must be 0.
func MultipartPartNumber(chunkIndex int, attemptSlot int) (int, error) {
	if chunkIndex < 0 {
		return 0, fmt.Errorf("source_chunk_index must be a non-negative integer")
	}
	if chunkIndex >= MultipartMaxSourceChunks {
		return 0, fmt.Errorf("source_chunk_index must be less than %d", MultipartMaxSourceChunks)
	}
	if attemptSlot < 0 || attemptSlot >= MultipartAttemptSlotCount {
		return 0, fmt.Errorf("attempt_slot must be 0 for consecutive multipart uploads")
	}
	return chunkIndex + 1, nil
}

// multipartPartNumber returns the consecutive part number for a source chunk.
// The optional logical attempt index is validated but does not change the part
// number: every attempt of a chunk reuses the same part.
func multipartPartNumber(sourceChunkIndex int, logicalAttemptIndex ...int) (int, error) {
	if len(logicalAttemptIndex) > 0 && logicalAttemptIndex[0] < 0 {
		return 0, fmt.Errorf("logical_attempt_index must be a non-negative integer")
	}
	return MultipartPartNumber(sourceChunkIndex, 0)
}
