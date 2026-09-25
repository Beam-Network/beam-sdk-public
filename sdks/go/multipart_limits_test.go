package beamnetworksdk

import "testing"

func TestMultipartPartNumber(t *testing.T) {
	tests := []struct {
		chunkIndex   int
		attemptIndex int
		partNumber   int
	}{
		{chunkIndex: 0, attemptIndex: 0, partNumber: 1},
		{chunkIndex: 0, attemptIndex: 1, partNumber: 2},
		{chunkIndex: 0, attemptIndex: 2, partNumber: 3},
		{chunkIndex: 1, attemptIndex: 0, partNumber: 4},
		{chunkIndex: 3_332, attemptIndex: 2, partNumber: 9_999},
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
	if _, err := multipartPartNumber(3_333); err == nil {
		t.Fatal("source chunk index 3333 must fail")
	}
}
