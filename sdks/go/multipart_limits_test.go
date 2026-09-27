package beamnetworksdk

import (
	"strings"
	"testing"
)

func TestMultipartPartNumber(t *testing.T) {
	tests := []struct {
		chunkIndex   int
		attemptIndex int
		partNumber   int
	}{
		{chunkIndex: 0, attemptIndex: 0, partNumber: 1},
		{chunkIndex: 0, attemptIndex: 1, partNumber: 1},
		{chunkIndex: 0, attemptIndex: 2, partNumber: 1},
		{chunkIndex: 1, attemptIndex: 0, partNumber: 2},
		{chunkIndex: 9_999, attemptIndex: 123, partNumber: 10_000},
	}
	for _, test := range tests {
		partNumber, err := multipartPartNumber(test.chunkIndex, test.attemptIndex)
		if err != nil {
			t.Fatalf("chunk %d returned error: %v", test.chunkIndex, err)
		}
		if partNumber != test.partNumber {
			t.Fatalf("chunk %d: got part %d, want %d", test.chunkIndex, partNumber, test.partNumber)
		}
	}
	if _, err := multipartPartNumber(-1); err == nil {
		t.Fatal("negative chunk index must fail")
	}
	if _, err := multipartPartNumber(10_000); err == nil {
		t.Fatal("source chunk index 10000 must fail")
	}
}

func TestExportedMultipartLimits(t *testing.T) {
	if MultipartMaxPartNumber != 10_000 || MultipartAttemptSlotCount != 1 || MultipartMaxSourceChunks != 10_000 {
		t.Fatal("multipart limits changed")
	}
	if partNumber, err := MultipartPartNumber(9_999, 0); err != nil || partNumber != 10_000 {
		t.Fatalf("MultipartPartNumber(9999, 0) = %d, %v", partNumber, err)
	}
	for _, slot := range []int{-1, 1, 2} {
		if _, err := MultipartPartNumber(0, slot); err == nil || !strings.Contains(err.Error(), "attempt_slot") {
			t.Fatalf("attempt slot %d must be rejected, got %v", slot, err)
		}
	}
	if _, err := MultipartPartNumber(10_000, 0); err == nil || !strings.Contains(err.Error(), "less than 10000") {
		t.Fatalf("chunk 10000 must be rejected, got %v", err)
	}
}
