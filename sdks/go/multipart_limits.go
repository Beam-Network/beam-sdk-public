package beamnetworksdk

import "fmt"

const (
	multipartMaxPartNumber   = 10_000
	multipartAttemptSlots    = 3
	multipartMaxSourceChunks = multipartMaxPartNumber / multipartAttemptSlots
)

func multipartPartNumber(sourceChunkIndex int, logicalAttemptIndex ...int) (int, error) {
	if sourceChunkIndex < 0 {
		return 0, fmt.Errorf("source_chunk_index must be a non-negative integer")
	}
	attemptIndex := 0
	if len(logicalAttemptIndex) > 0 {
		attemptIndex = logicalAttemptIndex[0]
	}
	if attemptIndex < 0 {
		return 0, fmt.Errorf("logical_attempt_index must be a non-negative integer")
	}
	if sourceChunkIndex >= multipartMaxSourceChunks {
		return 0, fmt.Errorf("source_chunk_index must be less than %d", multipartMaxSourceChunks)
	}
	return (sourceChunkIndex * multipartAttemptSlots) + (attemptIndex % multipartAttemptSlots) + 1, nil
}
